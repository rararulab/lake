// Copyright 2026 Rararulab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bounded MCAP metadata extraction through the upstream sans-I/O readers.

use std::{collections::BTreeMap, io::SeekFrom, ops::Range};

use async_trait::async_trait;
use bytes::Bytes;
use lake_common::{EpisodeManifestV1, TimelineKindV1};
use mcap::{
    McapError, Summary,
    records::Record,
    sans_io::{
        LinearReadEvent, LinearReader, LinearReaderOptions, SummaryReadEvent, SummaryReader,
        SummaryReaderOptions,
    },
};
use sha2::{Digest as _, Sha256};

use crate::{
    AdapterError, EpisodeInspectionContext, RandomAccessSource, ReadBudget, RecordingAdapter,
    neutral::{NeutralRecordingMetadata, NeutralStream, NeutralTimeline, build_manifest},
    source::BudgetedSource,
};

const FORMAT: &str = "mcap";
const LOG_TIME: &str = "log_time";

/// Extracts canonical Episode metadata from MCAP recordings.
#[derive(Clone, Copy, Debug, Default)]
pub struct McapAdapter;

struct McapStreamMetadata {
    stream_id:          String,
    media_type:         Option<String>,
    codec:              Option<String>,
    schema_fingerprint: String,
}

struct McapMetadata {
    producer:      Option<String>,
    streams:       Vec<McapStreamMetadata>,
    started_at_ns: Option<i64>,
    duration_ns:   Option<u64>,
    num_steps:     Option<u64>,
}

#[derive(Default)]
struct ReadCache {
    segments: Vec<(Range<u64>, Bytes)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SchemaMetadata {
    name:     String,
    encoding: String,
    data:     Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChannelMetadata {
    id:               u16,
    schema_id:        u16,
    topic:            String,
    message_encoding: String,
    metadata:         BTreeMap<String, String>,
}

#[derive(Default)]
struct LinearMetadata {
    header_seen: bool,
    producer:    Option<String>,
    schemas:     BTreeMap<u16, SchemaMetadata>,
    channels:    BTreeMap<u16, ChannelMetadata>,
    counts:      BTreeMap<u16, u64>,
    time_range:  Option<(u64, u64)>,
    num_steps:   u64,
}

#[async_trait]
impl RecordingAdapter for McapAdapter {
    async fn inspect(
        &self,
        source: &dyn RandomAccessSource,
        context: &EpisodeInspectionContext,
        budget: ReadBudget,
    ) -> Result<EpisodeManifestV1, AdapterError> {
        context.validate()?;
        let mut source = BudgetedSource::new(source, budget);
        let file_size = source.size_bytes().await?;
        let mut cache = ReadCache::default();
        let header = read_header(&mut source, file_size, &mut cache).await?;
        let metadata = match read_summary(&mut source, file_size, &mut cache).await? {
            Some(summary) => metadata_from_summary(&summary, &header.library)?,
            None => {
                if file_size > source.budget().max_fallback_scan_bytes() {
                    return Err(AdapterError::FallbackScanTooLarge {
                        size_bytes:  file_size,
                        limit_bytes: source.budget().max_fallback_scan_bytes(),
                    });
                }
                let bytes = cache.complete(&mut source, file_size).await?;
                scan_summaryless_mcap(&bytes, source.budget())?
            }
        };
        manifest_from_metadata(metadata, context)
    }
}

async fn read_header(
    source: &mut BudgetedSource<'_>,
    file_size: u64,
    cache: &mut ReadCache,
) -> Result<mcap::records::Header, AdapterError> {
    let record_limit = record_limit(source.budget());
    let options = LinearReaderOptions::default().with_record_length_limit(record_limit);
    let mut reader = LinearReader::new_with_options(options);
    let mut position = 0_u64;

    loop {
        let event = reader
            .next_event()
            .ok_or_else(|| format_error("MCAP ended before its Header record"))?
            .map_err(|error| map_mcap_error(error, source.budget()))?;
        match event {
            LinearReadEvent::ReadRequest(length) => {
                let bytes = read_requested(source, position, length, file_size, cache).await?;
                reader.insert(length).copy_from_slice(&bytes);
                reader.notify_read(length);
                position = position
                    .checked_add(u64::try_from(length).expect("read length fits u64"))
                    .ok_or_else(|| format_error("MCAP header read position overflows u64"))?;
            }
            LinearReadEvent::Record { data, opcode } => {
                return match mcap::parse_record(opcode, data)
                    .map_err(|error| map_mcap_error(error, source.budget()))?
                    .into_owned()
                {
                    Record::Header(header) => Ok(header),
                    _ => Err(format_error("MCAP Header is not the first record")),
                };
            }
        }
    }
}

async fn read_summary(
    source: &mut BudgetedSource<'_>,
    file_size: u64,
    cache: &mut ReadCache,
) -> Result<Option<Summary>, AdapterError> {
    let options = SummaryReaderOptions::default()
        .with_file_size(file_size)
        .with_record_length_limit(record_limit(source.budget()));
    let mut reader = SummaryReader::new_with_options(options);
    let mut position = 0_u64;

    while let Some(event) = reader.next_event() {
        match event.map_err(|error| map_mcap_error(error, source.budget()))? {
            SummaryReadEvent::SeekRequest(target) => {
                position = resolve_seek(target, position, file_size)?;
                reader.notify_seeked(position);
            }
            SummaryReadEvent::ReadRequest(length) => {
                let bytes = read_requested(source, position, length, file_size, cache).await?;
                reader.insert(length).copy_from_slice(&bytes);
                reader.notify_read(length);
                position = position
                    .checked_add(u64::try_from(length).expect("read length fits u64"))
                    .ok_or_else(|| format_error("MCAP summary read position overflows u64"))?;
            }
        }
    }
    Ok(reader.finish())
}

async fn read_requested(
    source: &mut BudgetedSource<'_>,
    start: u64,
    length: usize,
    file_size: u64,
    cache: &mut ReadCache,
) -> Result<bytes::Bytes, AdapterError> {
    if length == 0 {
        return Err(format_error("MCAP reader requested an empty range"));
    }
    let length = u64::try_from(length).map_err(|_| format_error("MCAP read length exceeds u64"))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| format_error("MCAP requested range overflows u64"))?;
    if end > file_size {
        return Err(format_error(format!(
            "MCAP requested range {start}..{end} exceeds file size {file_size}"
        )));
    }
    let bytes = source.read_range(start..end).await?;
    cache.segments.push((start..end, bytes.clone()));
    Ok(bytes)
}

impl ReadCache {
    async fn complete(
        mut self,
        source: &mut BudgetedSource<'_>,
        file_size: u64,
    ) -> Result<Vec<u8>, AdapterError> {
        let capacity = usize::try_from(file_size)
            .map_err(|_| format_error("MCAP fallback size does not fit this platform"))?;
        self.segments.sort_by_key(|(range, _)| range.start);
        let mut output = Vec::with_capacity(capacity);
        let mut position = 0_u64;
        for (range, bytes) in self.segments {
            if range.start < position {
                return Err(format_error(
                    "MCAP indexed reads overlap during fallback assembly",
                ));
            }
            if range.start > position {
                let missing = source.read_range(position..range.start).await?;
                output.extend_from_slice(&missing);
            }
            let expected = range
                .end
                .checked_sub(range.start)
                .ok_or_else(|| format_error("MCAP cached read has an invalid range"))?;
            if u64::try_from(bytes.len()).expect("cached length fits u64") != expected {
                return Err(format_error("MCAP cached read has an inconsistent length"));
            }
            output.extend_from_slice(&bytes);
            position = range.end;
        }
        if position < file_size {
            let missing = source.read_range(position..file_size).await?;
            output.extend_from_slice(&missing);
        }
        if output.len() != capacity {
            return Err(format_error(
                "MCAP fallback assembly length is inconsistent",
            ));
        }
        Ok(output)
    }
}

fn resolve_seek(target: SeekFrom, position: u64, file_size: u64) -> Result<u64, AdapterError> {
    let (base, delta) = match target {
        SeekFrom::Start(target) => {
            if target > file_size {
                return Err(format_error(format!(
                    "MCAP seek target {target} exceeds file size {file_size}"
                )));
            }
            return Ok(target);
        }
        SeekFrom::End(delta) => (i128::from(file_size), i128::from(delta)),
        SeekFrom::Current(delta) => (i128::from(position), i128::from(delta)),
    };
    let target = base
        .checked_add(delta)
        .filter(|target| *target >= 0 && *target <= i128::from(file_size))
        .ok_or_else(|| format_error("MCAP seek target is outside the file"))?;
    u64::try_from(target).map_err(|_| format_error("MCAP seek target does not fit u64"))
}

fn scan_summaryless_mcap(bytes: &[u8], budget: ReadBudget) -> Result<McapMetadata, AdapterError> {
    let options = LinearReaderOptions::default()
        .with_record_length_limit(record_limit(budget))
        .with_validate_chunk_crcs(true)
        .with_prevalidate_chunk_crcs(true)
        .with_validate_data_section_crc(true)
        .with_validate_summary_section_crc(true)
        .with_check_finishes_after_end_magic(true);
    let mut reader = LinearReader::new_with_options(options);
    let mut position = 0_usize;
    let mut metadata = LinearMetadata::default();

    while let Some(event) = reader.next_event() {
        match event.map_err(|error| map_mcap_error(error, budget))? {
            LinearReadEvent::ReadRequest(length) => {
                let available = bytes.len().saturating_sub(position);
                let read = length.min(available);
                if read > 0 {
                    reader.insert(length)[..read]
                        .copy_from_slice(&bytes[position..position + read]);
                    position += read;
                } else {
                    let _ = reader.insert(length);
                }
                reader.notify_read(read);
            }
            LinearReadEvent::Record { data, opcode } => {
                let record = mcap::parse_record(opcode, data)
                    .map_err(|error| map_mcap_error(error, budget))?;
                accumulate_record(&mut metadata, record)?;
            }
        }
    }
    metadata.finish()
}

fn accumulate_record(
    metadata: &mut LinearMetadata,
    record: Record<'_>,
) -> Result<(), AdapterError> {
    match record {
        Record::Header(header) => {
            if metadata.header_seen {
                return Err(format_error("MCAP contains more than one Header record"));
            }
            metadata.header_seen = true;
            metadata.producer = (!header.library.trim().is_empty()).then_some(header.library);
        }
        Record::Schema { header, data } => {
            let schema = SchemaMetadata {
                name:     header.name,
                encoding: header.encoding,
                data:     data.into_owned(),
            };
            insert_consistent(&mut metadata.schemas, header.id, schema, "Schema")?;
        }
        Record::Channel(channel) => {
            let channel = ChannelMetadata {
                id:               channel.id,
                schema_id:        channel.schema_id,
                topic:            channel.topic,
                message_encoding: channel.message_encoding,
                metadata:         channel.metadata,
            };
            insert_consistent(&mut metadata.channels, channel.id, channel, "Channel")?;
        }
        Record::Message { header, data: _ } => {
            if !metadata.channels.contains_key(&header.channel_id) {
                return Err(format_error(format!(
                    "MCAP message references unknown channel {}",
                    header.channel_id
                )));
            }
            let count = metadata.counts.entry(header.channel_id).or_default();
            *count = count
                .checked_add(1)
                .ok_or_else(|| format_error("MCAP channel message count overflows u64"))?;
            metadata.num_steps = metadata
                .num_steps
                .checked_add(1)
                .ok_or_else(|| format_error("MCAP message count overflows u64"))?;
            metadata.time_range = Some(
                metadata
                    .time_range
                    .map_or((header.log_time, header.log_time), |(start, end)| {
                        (start.min(header.log_time), end.max(header.log_time))
                    }),
            );
        }
        Record::Footer(_)
        | Record::Chunk { .. }
        | Record::MessageIndex(_)
        | Record::ChunkIndex(_)
        | Record::Attachment { .. }
        | Record::AttachmentIndex(_)
        | Record::Statistics(_)
        | Record::Metadata(_)
        | Record::MetadataIndex(_)
        | Record::SummaryOffset(_)
        | Record::DataEnd(_)
        | Record::Unknown { .. } => {}
    }
    Ok(())
}

fn insert_consistent<T: Eq>(
    values: &mut BTreeMap<u16, T>,
    id: u16,
    value: T,
    kind: &'static str,
) -> Result<(), AdapterError> {
    if values.get(&id).is_some_and(|existing| *existing != value) {
        return Err(format_error(format!(
            "MCAP contains conflicting {kind} records for id {id}"
        )));
    }
    values.entry(id).or_insert(value);
    Ok(())
}

impl LinearMetadata {
    fn finish(self) -> Result<McapMetadata, AdapterError> {
        if !self.header_seen {
            return Err(format_error("MCAP does not contain a Header record"));
        }
        let topic_counts = self
            .channels
            .values()
            .filter(|channel| self.counts.get(&channel.id).is_some_and(|count| *count > 0))
            .fold(BTreeMap::<&str, usize>::new(), |mut counts, channel| {
                *counts.entry(channel.topic.as_str()).or_default() += 1;
                counts
            });
        let mut streams = self
            .channels
            .values()
            .filter(|channel| self.counts.get(&channel.id).is_some_and(|count| *count > 0))
            .map(|channel| {
                let schema = if channel.schema_id == 0 {
                    None
                } else {
                    Some(self.schemas.get(&channel.schema_id).ok_or_else(|| {
                        format_error(format!(
                            "MCAP channel {} references unknown schema {}",
                            channel.id, channel.schema_id
                        ))
                    })?)
                };
                Ok(McapStreamMetadata {
                    stream_id:          if topic_counts.get(channel.topic.as_str()) == Some(&1) {
                        channel.topic.clone()
                    } else {
                        format!("{}#channel-{}", channel.topic, channel.id)
                    },
                    media_type:         channel.metadata.get("media_type").cloned(),
                    codec:              (!channel.message_encoding.is_empty())
                        .then(|| channel.message_encoding.clone()),
                    schema_fingerprint: fingerprint_channel_parts(
                        &channel.message_encoding,
                        schema,
                    ),
                })
            })
            .collect::<Result<Vec<_>, AdapterError>>()?;
        streams.sort_by(|left, right| left.stream_id.cmp(&right.stream_id));
        let (started_at_ns, duration_ns) = self
            .time_range
            .map(|(start, end)| normalize_time_range(start, end))
            .transpose()?
            .map_or((None, None), |(start, duration)| {
                (Some(start), Some(duration))
            });

        Ok(McapMetadata {
            producer: self.producer,
            streams,
            started_at_ns,
            duration_ns,
            num_steps: Some(self.num_steps),
        })
    }
}

fn normalize_time_range(start: u64, end: u64) -> Result<(i64, u64), AdapterError> {
    let start_ns = i64::try_from(start)
        .map_err(|_| format_error("MCAP start time exceeds signed nanoseconds"))?;
    let duration_ns = end
        .checked_sub(start)
        .ok_or_else(|| format_error("MCAP message time range is reversed"))?;
    Ok((start_ns, duration_ns))
}

fn metadata_from_summary(summary: &Summary, library: &str) -> Result<McapMetadata, AdapterError> {
    let stats = summary.stats.as_ref();
    let topic_counts = summary
        .channels
        .values()
        .filter(|channel| channel_has_messages(summary, channel.id))
        .fold(BTreeMap::<&str, usize>::new(), |mut counts, channel| {
            *counts.entry(channel.topic.as_str()).or_default() += 1;
            counts
        });
    let mut channels = summary
        .channels
        .values()
        .filter(|channel| channel_has_messages(summary, channel.id))
        .collect::<Vec<_>>();
    channels.sort_by_key(|channel| channel.id);
    let mut streams = channels
        .into_iter()
        .map(|channel| McapStreamMetadata {
            stream_id:          if topic_counts.get(channel.topic.as_str()) == Some(&1) {
                channel.topic.clone()
            } else {
                format!("{}#channel-{}", channel.topic, channel.id)
            },
            media_type:         channel.metadata.get("media_type").cloned(),
            codec:              (!channel.message_encoding.is_empty())
                .then(|| channel.message_encoding.clone()),
            schema_fingerprint: channel_fingerprint(channel),
        })
        .collect::<Vec<_>>();
    streams.sort_by(|left, right| left.stream_id.cmp(&right.stream_id));
    let (started_at_ns, duration_ns) = stats
        .map(|stats| normalize_time_range(stats.message_start_time, stats.message_end_time))
        .transpose()?
        .map_or((None, None), |(start, duration)| {
            (Some(start), Some(duration))
        });
    Ok(McapMetadata {
        producer: (!library.trim().is_empty()).then(|| library.to_owned()),
        streams,
        started_at_ns,
        duration_ns,
        num_steps: stats.map(|stats| stats.message_count),
    })
}

fn manifest_from_metadata(
    metadata: McapMetadata,
    context: &EpisodeInspectionContext,
) -> Result<EpisodeManifestV1, AdapterError> {
    if metadata.streams.is_empty() {
        return Err(AdapterError::NoTemporalStreams { format: FORMAT });
    }
    let schema_fingerprint = recording_fingerprint(&metadata.streams);
    build_manifest(
        NeutralRecordingMetadata {
            format: FORMAT,
            producer_version: metadata.producer.clone(),
            layer_producer: metadata.producer,
            selector: context.selector().map(str::to_owned),
            started_at_ns: metadata.started_at_ns,
            duration_ns: metadata.duration_ns,
            num_steps: metadata.num_steps,
            timelines: vec![NeutralTimeline {
                timeline_id: LOG_TIME.to_owned(),
                kind:        TimelineKindV1::Timestamp,
            }],
            streams: metadata
                .streams
                .into_iter()
                .map(|stream| NeutralStream {
                    stream_id:          stream.stream_id,
                    timeline_ids:       vec![LOG_TIME.to_owned()],
                    media_type:         stream.media_type,
                    codec:              stream.codec,
                    schema_fingerprint: stream.schema_fingerprint,
                })
                .collect(),
            schema_fingerprint,
        },
        context,
    )
}

fn channel_has_messages(summary: &Summary, channel_id: u16) -> bool {
    summary.stats.as_ref().is_none_or(|stats| {
        stats
            .channel_message_counts
            .get(&channel_id)
            .is_some_and(|count| *count > 0)
    })
}

fn channel_fingerprint(channel: &mcap::Channel<'_>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(channel.message_encoding.as_bytes());
    hasher.update([0]);
    if let Some(schema) = &channel.schema {
        hasher.update(schema.name.as_bytes());
        hasher.update([0]);
        hasher.update(schema.encoding.as_bytes());
        hasher.update([0]);
        hasher.update(&schema.data);
    }
    format!("{:x}", hasher.finalize())
}

fn fingerprint_channel_parts(message_encoding: &str, schema: Option<&SchemaMetadata>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(message_encoding.as_bytes());
    hasher.update([0]);
    if let Some(schema) = schema {
        hasher.update(schema.name.as_bytes());
        hasher.update([0]);
        hasher.update(schema.encoding.as_bytes());
        hasher.update([0]);
        hasher.update(&schema.data);
    }
    format!("{:x}", hasher.finalize())
}

fn recording_fingerprint(streams: &[McapStreamMetadata]) -> String {
    let mut hasher = Sha256::new();
    for stream in streams {
        hasher.update(stream.stream_id.as_bytes());
        hasher.update([0]);
        hasher.update(stream.schema_fingerprint.as_bytes());
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())
}

fn record_limit(budget: ReadBudget) -> usize {
    usize::try_from(budget.max_record_bytes()).unwrap_or(usize::MAX)
}

fn map_mcap_error(error: McapError, budget: ReadBudget) -> AdapterError {
    match error {
        McapError::RecordTooLarge { len, .. } | McapError::ChunkTooLarge(len) => {
            AdapterError::RecordTooLarge {
                format:      FORMAT,
                size_bytes:  len,
                limit_bytes: budget.max_record_bytes(),
            }
        }
        error => format_error(error.to_string()),
    }
}

fn format_error(message: impl Into<String>) -> AdapterError {
    AdapterError::Format {
        format:  FORMAT,
        message: message.into(),
    }
}
