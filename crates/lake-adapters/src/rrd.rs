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

//! Bounded RRD metadata extraction through Rerun's public format APIs.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use bytes::Bytes;
use lake_common::{EpisodeManifestV1, TimelineKindV1};
use re_log_encoding::{
    Decodable as _, DecoderApp, RawRrdManifest, RrdFooter, StreamFooter, StreamHeader,
};
use re_log_types::{LogMsg, TimeType};
use sha2::{Digest as _, Sha256};

use crate::{
    AdapterError, EpisodeInspectionContext, RandomAccessSource, ReadBudget, RecordingAdapter,
    neutral::{NeutralRecordingMetadata, NeutralStream, NeutralTimeline, build_manifest},
    source::BudgetedSource,
};

const FORMAT: &str = "rrd";

/// Extracts canonical Episode metadata from Rerun RRD recordings.
#[derive(Clone, Copy, Debug, Default)]
pub struct RrdAdapter;

#[derive(Default)]
struct StreamMetadata {
    timeline_ids: BTreeSet<String>,
    components:   BTreeSet<String>,
}

struct RrdRecordingMetadata {
    selector:       String,
    streams:        BTreeMap<String, StreamMetadata>,
    timeline_kinds: BTreeMap<String, TimelineKindV1>,
}

enum FooterProbe {
    Present(RrdFooter),
    Absent { tail_start: u64, tail: Bytes },
}

#[async_trait]
impl RecordingAdapter for RrdAdapter {
    async fn inspect(
        &self,
        source: &dyn RandomAccessSource,
        context: &EpisodeInspectionContext,
        budget: ReadBudget,
    ) -> Result<EpisodeManifestV1, AdapterError> {
        context.validate()?;
        let mut source = BudgetedSource::new(source, budget);
        let size_bytes = source.size_bytes().await?;
        let header_size =
            u64::try_from(StreamHeader::ENCODED_SIZE_BYTES).expect("RRD header size fits u64");
        if size_bytes < header_size {
            return Err(format_error("file is too small to contain an RRD header"));
        }

        let header_bytes = source.read_range(0..header_size).await?;
        let stream_header = StreamHeader::from_rrd_bytes(&header_bytes)
            .map_err(|error| format_error(error.to_string()))?;
        let (producer_version, _) = stream_header
            .to_version_and_options()
            .map_err(|error| format_error(error.to_string()))?;

        let metadata = match probe_footer(&mut source, size_bytes).await? {
            FooterProbe::Present(footer) => {
                metadata_from_manifest(select_recording(&footer, context.selector())?)?
            }
            FooterProbe::Absent { tail_start, tail } => {
                scan_footerless_rrd(
                    &mut source,
                    size_bytes,
                    header_bytes,
                    tail_start,
                    tail,
                    context.selector(),
                )
                .await?
            }
        };
        manifest_from_rrd(&metadata, context, producer_version.to_string())
    }
}

async fn probe_footer(
    source: &mut BudgetedSource<'_>,
    file_size: u64,
) -> Result<FooterProbe, AdapterError> {
    let footer_size =
        u64::try_from(StreamFooter::ENCODED_SIZE_BYTES).expect("RRD stream footer size fits u64");
    if file_size < footer_size {
        return Ok(FooterProbe::Absent {
            tail_start: file_size,
            tail:       Bytes::new(),
        });
    }
    let footer_start = file_size - footer_size;
    let footer_bytes = source.read_range(footer_start..file_size).await?;
    let Some(stream_footer) = decode_optional_stream_footer(&footer_bytes)? else {
        return Ok(FooterProbe::Absent {
            tail_start: footer_start,
            tail:       footer_bytes,
        });
    };
    let Some(entry) = stream_footer.entries.first() else {
        return Err(format_error("RRD stream footer has no manifest entry"));
    };
    if stream_footer.entries.len() != 1 {
        return Err(format_error(format!(
            "RRD stream footer has {} manifest entries; exactly one is supported",
            stream_footer.entries.len()
        )));
    }

    let span = entry.rrd_footer_byte_span_from_start_excluding_header;
    if span.len > source.budget().max_record_bytes() {
        return Err(AdapterError::RecordTooLarge {
            format:      FORMAT,
            size_bytes:  span.len,
            limit_bytes: source.budget().max_record_bytes(),
        });
    }
    let end = span
        .start
        .checked_add(span.len)
        .ok_or_else(|| format_error("RRD footer payload span overflows u64"))?;
    if span.len == 0 || span.start < StreamHeader::ENCODED_SIZE_BYTES as u64 || end > footer_start {
        return Err(format_error(format!(
            "RRD footer payload span {}..{} is outside the indexed payload region",
            span.start, end
        )));
    }

    let payload = source.read_range(span.start..end).await?;
    let actual_crc = StreamFooter::compute_crc(&payload);
    if actual_crc != entry.crc_excluding_header {
        return Err(format_error(format!(
            "RRD footer checksum mismatch: expected {}, got {}",
            entry.crc_excluding_header, actual_crc
        )));
    }

    let transport = re_protos::log_msg::v1alpha1::RrdFooter::from_rrd_bytes(&payload)
        .map_err(|error| format_error(error.to_string()))?;
    re_log_encoding::ToApplication::to_application(&transport, ())
        .map(FooterProbe::Present)
        .map_err(|error| format_error(error.to_string()))
}

fn decode_optional_stream_footer(bytes: &[u8]) -> Result<Option<StreamFooter>, AdapterError> {
    std::panic::catch_unwind(|| StreamFooter::from_rrd_bytes(bytes))
        .map_err(|_| format_error("RRD stream footer decoder rejected malformed entry metadata"))
        .map(|result| result.ok())
}

fn select_recording<'a>(
    footer: &'a RrdFooter,
    selector: Option<&str>,
) -> Result<&'a RawRrdManifest, AdapterError> {
    let mut recordings = footer
        .manifests
        .values()
        .filter(|manifest| manifest.store_id.is_recording())
        .collect::<Vec<_>>();
    recordings.sort_by_key(|manifest| native_selector(&manifest.store_id));

    if let Some(selector) = selector {
        return recordings
            .into_iter()
            .find(|manifest| native_selector(&manifest.store_id) == selector)
            .ok_or(AdapterError::RecordingNotFound { format: FORMAT });
    }
    match recordings.as_slice() {
        [] => Err(AdapterError::RecordingNotFound { format: FORMAT }),
        [manifest] => Ok(manifest),
        manifests => Err(AdapterError::AmbiguousRecording {
            format: FORMAT,
            count:  manifests.len(),
        }),
    }
}

fn metadata_from_manifest(manifest: &RawRrdManifest) -> Result<RrdRecordingMetadata, AdapterError> {
    let mut streams = BTreeMap::<String, StreamMetadata>::new();
    for (entity, components) in manifest
        .calc_static_map()
        .map_err(|error| format_error(error.to_string()))?
    {
        let stream = streams.entry(entity.to_string()).or_default();
        stream.components.extend(
            components
                .into_keys()
                .map(|component| component.as_str().to_owned()),
        );
    }

    let mut timeline_kinds = BTreeMap::<String, TimelineKindV1>::new();
    for (entity, timelines) in manifest
        .calc_temporal_map()
        .map_err(|error| format_error(error.to_string()))?
    {
        let stream = streams.entry(entity.to_string()).or_default();
        for (timeline, components) in timelines {
            let timeline_id = timeline.name().as_str().to_owned();
            let kind = match timeline.typ() {
                TimeType::Sequence => TimelineKindV1::Sequence,
                TimeType::DurationNs | TimeType::TimestampNs => TimelineKindV1::Timestamp,
            };
            if timeline_kinds
                .insert(timeline_id.clone(), kind)
                .is_some_and(|existing| existing != kind)
            {
                return Err(format_error(format!(
                    "timeline '{timeline_id}' is declared with conflicting types"
                )));
            }
            stream.timeline_ids.insert(timeline_id);
            stream.components.extend(
                components
                    .into_keys()
                    .map(|component| component.as_str().to_owned()),
            );
        }
    }
    streams.retain(|_, stream| !stream.timeline_ids.is_empty());
    if streams.is_empty() {
        return Err(AdapterError::NoTemporalStreams { format: FORMAT });
    }

    Ok(RrdRecordingMetadata {
        selector: native_selector(&manifest.store_id),
        streams,
        timeline_kinds,
    })
}

async fn scan_footerless_rrd(
    source: &mut BudgetedSource<'_>,
    file_size: u64,
    header: Bytes,
    tail_start: u64,
    tail: Bytes,
    selector: Option<&str>,
) -> Result<RrdRecordingMetadata, AdapterError> {
    if file_size > source.budget().max_fallback_scan_bytes() {
        return Err(AdapterError::FallbackScanTooLarge {
            size_bytes:  file_size,
            limit_bytes: source.budget().max_fallback_scan_bytes(),
        });
    }
    let capacity = usize::try_from(file_size)
        .map_err(|_| format_error("RRD fallback size does not fit this platform"))?;
    let header_size = u64::try_from(header.len()).expect("header length fits u64");
    if tail_start < header_size {
        return Err(format_error(
            "RRD footer probe overlaps the validated header",
        ));
    }
    let middle = if tail_start > header_size {
        source.read_range(header_size..tail_start).await?
    } else {
        Bytes::new()
    };

    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(&middle);
    bytes.extend_from_slice(&tail);
    if bytes.len() != capacity {
        return Err(format_error("RRD fallback assembly length is inconsistent"));
    }

    let mut recordings = BTreeMap::<String, RrdRecordingMetadata>::new();
    for decoded in DecoderApp::decode_lazy(bytes.as_slice()) {
        let message = decoded.map_err(|error| format_error(error.to_string()))?;
        accumulate_message(&mut recordings, message)?;
    }
    select_scanned_recording(recordings, selector)
}

fn accumulate_message(
    recordings: &mut BTreeMap<String, RrdRecordingMetadata>,
    message: LogMsg,
) -> Result<(), AdapterError> {
    let (store_id, arrow) = match message {
        LogMsg::SetStoreInfo(message) => {
            let store_id = message.info.store_id;
            if store_id.is_recording() {
                let selector = native_selector(&store_id);
                recordings
                    .entry(selector.clone())
                    .or_insert_with(|| RrdRecordingMetadata {
                        selector,
                        streams: BTreeMap::new(),
                        timeline_kinds: BTreeMap::new(),
                    });
            }
            return Ok(());
        }
        LogMsg::ArrowMsg(store_id, arrow) if store_id.is_recording() => (store_id, arrow),
        LogMsg::ArrowMsg(..) | LogMsg::BlueprintActivationCommand(_) => return Ok(()),
    };

    let selector = native_selector(&store_id);
    let recording = recordings
        .entry(selector.clone())
        .or_insert_with(|| RrdRecordingMetadata {
            selector,
            streams: BTreeMap::new(),
            timeline_kinds: BTreeMap::new(),
        });
    let chunk =
        re_chunk::Chunk::from_arrow_msg(&arrow).map_err(|error| format_error(error.to_string()))?;
    let stream = recording
        .streams
        .entry(chunk.entity_path().to_string())
        .or_default();
    stream.components.extend(
        chunk
            .components_identifiers()
            .map(|component| component.as_str().to_owned()),
    );
    for time_column in chunk.timelines().values() {
        let timeline = time_column.timeline();
        let timeline_id = timeline.name().as_str().to_owned();
        let kind = timeline_kind(timeline.typ());
        insert_timeline_kind(&mut recording.timeline_kinds, timeline_id.clone(), kind)?;
        stream.timeline_ids.insert(timeline_id);
    }
    Ok(())
}

fn select_scanned_recording(
    mut recordings: BTreeMap<String, RrdRecordingMetadata>,
    selector: Option<&str>,
) -> Result<RrdRecordingMetadata, AdapterError> {
    if let Some(selector) = selector {
        return recordings
            .remove(selector)
            .ok_or(AdapterError::RecordingNotFound { format: FORMAT });
    }
    match recordings.len() {
        0 => Err(AdapterError::RecordingNotFound { format: FORMAT }),
        1 => Ok(recordings
            .into_values()
            .next()
            .expect("one recording was counted")),
        count => Err(AdapterError::AmbiguousRecording {
            format: FORMAT,
            count,
        }),
    }
}

fn timeline_kind(time_type: TimeType) -> TimelineKindV1 {
    match time_type {
        TimeType::Sequence => TimelineKindV1::Sequence,
        TimeType::DurationNs | TimeType::TimestampNs => TimelineKindV1::Timestamp,
    }
}

fn insert_timeline_kind(
    timelines: &mut BTreeMap<String, TimelineKindV1>,
    timeline_id: String,
    kind: TimelineKindV1,
) -> Result<(), AdapterError> {
    if timelines
        .insert(timeline_id.clone(), kind)
        .is_some_and(|existing| existing != kind)
    {
        return Err(format_error(format!(
            "timeline '{timeline_id}' is declared with conflicting types"
        )));
    }
    Ok(())
}

fn manifest_from_rrd(
    metadata: &RrdRecordingMetadata,
    context: &EpisodeInspectionContext,
    producer_version: String,
) -> Result<EpisodeManifestV1, AdapterError> {
    if metadata.streams.is_empty() {
        return Err(AdapterError::NoTemporalStreams { format: FORMAT });
    }

    let timelines = metadata
        .timeline_kinds
        .iter()
        .map(|(timeline_id, kind)| NeutralTimeline {
            timeline_id: timeline_id.clone(),
            kind:        *kind,
        })
        .collect::<Vec<_>>();
    let streams = metadata
        .streams
        .iter()
        .map(|(stream_id, stream)| NeutralStream {
            stream_id:          stream_id.clone(),
            timeline_ids:       stream.timeline_ids.iter().cloned().collect(),
            media_type:         None,
            codec:              None,
            schema_fingerprint: fingerprint(&stream.components),
        })
        .collect::<Vec<_>>();
    let schema_fingerprint = recording_fingerprint(metadata);
    build_manifest(
        NeutralRecordingMetadata {
            format: FORMAT,
            producer_version: Some(producer_version),
            layer_producer: Some("rerun".to_owned()),
            selector: Some(metadata.selector.clone()),
            started_at_ns: None,
            duration_ns: None,
            num_steps: None,
            timelines,
            streams,
            schema_fingerprint,
        },
        context,
    )
}

fn native_selector(store_id: &re_log_types::StoreId) -> String {
    format!(
        "{}/{}",
        store_id.application_id().as_str(),
        store_id.recording_id().as_str()
    )
}

fn fingerprint(components: &BTreeSet<String>) -> String {
    let mut hasher = Sha256::new();
    for component in components {
        hasher.update(component.as_bytes());
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())
}

fn recording_fingerprint(metadata: &RrdRecordingMetadata) -> String {
    let mut hasher = Sha256::new();
    for (stream_id, stream) in &metadata.streams {
        hasher.update(stream_id.as_bytes());
        hasher.update([0]);
        for timeline_id in &stream.timeline_ids {
            hasher.update(timeline_id.as_bytes());
            hasher.update([0]);
        }
        for component in &stream.components {
            hasher.update(component.as_bytes());
            hasher.update([0]);
        }
    }
    format!("{:x}", hasher.finalize())
}

fn format_error(message: impl Into<String>) -> AdapterError {
    AdapterError::Format {
        format:  FORMAT,
        message: message.into(),
    }
}
