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

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use async_trait::async_trait;
use lake_common::{
    EpisodeManifestDraftV1, EpisodeManifestV1, EpisodeSummaryV1, LayerKindV1, LayerV1,
    ManifestArtifactBindingV1, RecordingV1, StreamV1, TimelineKindV1, TimelineV1,
};
use re_log_encoding::{Decodable as _, RawRrdManifest, RrdFooter, StreamFooter, StreamHeader};
use re_log_types::TimeType;
use sha2::{Digest as _, Sha256};

use crate::{
    AdapterError, EpisodeInspectionContext, RandomAccessSource, ReadBudget, RecordingAdapter,
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

        let footer = read_indexed_footer(&mut source, size_bytes).await?;
        let manifest = select_recording(&footer, context.selector())?;
        manifest_from_rrd(
            manifest,
            context,
            producer_version.to_string(),
            native_selector(&manifest.store_id),
        )
    }
}

async fn read_indexed_footer(
    source: &mut BudgetedSource<'_>,
    file_size: u64,
) -> Result<RrdFooter, AdapterError> {
    let footer_size =
        u64::try_from(StreamFooter::ENCODED_SIZE_BYTES).expect("RRD stream footer size fits u64");
    if file_size < footer_size {
        return Err(format_error("RRD footer is absent"));
    }
    let footer_start = file_size - footer_size;
    let footer_bytes = source.read_range(footer_start..file_size).await?;
    let Some(stream_footer) = decode_optional_stream_footer(&footer_bytes)? else {
        return Err(format_error("RRD footer is absent"));
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

fn manifest_from_rrd(
    manifest: &RawRrdManifest,
    context: &EpisodeInspectionContext,
    producer_version: String,
    selector: String,
) -> Result<EpisodeManifestV1, AdapterError> {
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

    let timeline_descriptors = timeline_kinds
        .into_iter()
        .map(|(timeline_id, kind)| {
            TimelineV1::builder()
                .timeline_id(timeline_id)
                .kind(kind)
                .build()
        })
        .collect::<Vec<_>>();
    let stream_descriptors = streams
        .iter()
        .map(|(stream_id, metadata)| {
            StreamV1::builder()
                .stream_id(stream_id)
                .recording_id(context.recording_id())
                .timeline_ids(metadata.timeline_ids.iter().cloned().collect())
                .schema_fingerprint(fingerprint(&metadata.components))
                .build()
        })
        .collect::<Vec<_>>();
    let stream_ids = streams.into_keys().collect::<Vec<_>>();
    let schema_fingerprint = hex_bytes(&manifest.sorbet_schema_sha256);

    EpisodeManifestV1::try_from_draft(
        EpisodeManifestDraftV1::builder()
            .summary(
                EpisodeSummaryV1::builder()
                    .episode_id(context.episode_id())
                    .build(),
            )
            .recordings(vec![
                RecordingV1::builder()
                    .recording_id(context.recording_id())
                    .recording_format(FORMAT)
                    .producer_version(producer_version.clone())
                    .build(),
            ])
            .timelines(timeline_descriptors)
            .streams(stream_descriptors)
            .layers(vec![
                LayerV1::builder()
                    .layer_id(context.layer_id())
                    .kind(LayerKindV1::Base)
                    .producer("rerun")
                    .build(),
            ])
            .artifact_bindings(vec![
                ManifestArtifactBindingV1::builder()
                    .artifact_id(context.artifact_id())
                    .layer_id(context.layer_id())
                    .role("recording")
                    .recording_id(context.recording_id())
                    .selector(selector)
                    .stream_ids(stream_ids)
                    .schema_fingerprint(schema_fingerprint)
                    .producer_version(producer_version)
                    .build(),
            ])
            .build(),
    )
    .map_err(|source| AdapterError::Manifest { source })
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

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len().saturating_mul(2)),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to a String cannot fail");
            output
        },
    )
}

fn format_error(message: impl Into<String>) -> AdapterError {
    AdapterError::Format {
        format:  FORMAT,
        message: message.into(),
    }
}
