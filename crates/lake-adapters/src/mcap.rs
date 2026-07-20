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

use std::{collections::BTreeMap, io::SeekFrom};

use async_trait::async_trait;
use lake_common::{
    EpisodeManifestDraftV1, EpisodeManifestV1, EpisodeSummaryV1, LayerKindV1, LayerV1,
    ManifestArtifactBindingV1, RecordingV1, StreamV1, TimelineKindV1, TimelineV1,
};
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
        let header = read_header(&mut source, file_size).await?;
        let summary = read_summary(&mut source, file_size)
            .await?
            .ok_or_else(|| format_error("MCAP summary is absent"))?;
        manifest_from_summary(&summary, &header.library, context)
    }
}

async fn read_header(
    source: &mut BudgetedSource<'_>,
    file_size: u64,
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
                let bytes = read_requested(source, position, length, file_size).await?;
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
                let bytes = read_requested(source, position, length, file_size).await?;
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
    source.read_range(start..end).await
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

fn manifest_from_summary(
    summary: &Summary,
    library: &str,
    context: &EpisodeInspectionContext,
) -> Result<EpisodeManifestV1, AdapterError> {
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
    if streams.is_empty() {
        return Err(AdapterError::NoTemporalStreams { format: FORMAT });
    }

    let started_at_ns = stats
        .map(|stats| {
            i64::try_from(stats.message_start_time)
                .map_err(|_| format_error("MCAP start time exceeds signed nanoseconds"))
        })
        .transpose()?;
    let duration_ns = stats
        .map(|stats| {
            stats
                .message_end_time
                .checked_sub(stats.message_start_time)
                .ok_or_else(|| format_error("MCAP message time range is reversed"))
        })
        .transpose()?;
    let producer = (!library.trim().is_empty()).then(|| library.to_owned());
    let stream_ids = streams
        .iter()
        .map(|stream| stream.stream_id.clone())
        .collect::<Vec<_>>();
    let schema_fingerprint = recording_fingerprint(&streams);

    EpisodeManifestV1::try_from_draft(
        EpisodeManifestDraftV1::builder()
            .summary(
                EpisodeSummaryV1::builder()
                    .episode_id(context.episode_id())
                    .maybe_started_at_ns(started_at_ns)
                    .maybe_duration_ns(duration_ns)
                    .maybe_num_steps(stats.map(|stats| stats.message_count))
                    .build(),
            )
            .recordings(vec![
                RecordingV1::builder()
                    .recording_id(context.recording_id())
                    .recording_format(FORMAT)
                    .maybe_producer_version(producer.clone())
                    .build(),
            ])
            .timelines(vec![
                TimelineV1::builder()
                    .timeline_id(LOG_TIME)
                    .kind(TimelineKindV1::Timestamp)
                    .build(),
            ])
            .streams(
                streams
                    .into_iter()
                    .map(|stream| {
                        StreamV1::builder()
                            .stream_id(stream.stream_id)
                            .recording_id(context.recording_id())
                            .timeline_ids(vec![LOG_TIME.to_owned()])
                            .maybe_media_type(stream.media_type)
                            .maybe_codec(stream.codec)
                            .schema_fingerprint(stream.schema_fingerprint)
                            .build()
                    })
                    .collect(),
            )
            .layers(vec![
                LayerV1::builder()
                    .layer_id(context.layer_id())
                    .kind(LayerKindV1::Base)
                    .maybe_producer(producer.clone())
                    .build(),
            ])
            .artifact_bindings(vec![
                ManifestArtifactBindingV1::builder()
                    .artifact_id(context.artifact_id())
                    .layer_id(context.layer_id())
                    .role("recording")
                    .recording_id(context.recording_id())
                    .maybe_selector(context.selector().map(str::to_owned))
                    .stream_ids(stream_ids)
                    .schema_fingerprint(schema_fingerprint)
                    .maybe_producer_version(producer)
                    .build(),
            ])
            .build(),
    )
    .map_err(|source| AdapterError::Manifest { source })
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
