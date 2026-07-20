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

//! Shared format-neutral mapping into Lake's canonical Episode manifest.

use lake_common::{
    EpisodeManifestDraftV1, EpisodeManifestV1, EpisodeSummaryV1, LayerKindV1, LayerV1,
    ManifestArtifactBindingV1, RecordingV1, StreamV1, TimelineKindV1, TimelineV1,
};

use crate::{AdapterError, EpisodeInspectionContext};

pub(crate) struct NeutralTimeline {
    pub(crate) timeline_id: String,
    pub(crate) kind:        TimelineKindV1,
}

pub(crate) struct NeutralStream {
    pub(crate) stream_id:          String,
    pub(crate) timeline_ids:       Vec<String>,
    pub(crate) media_type:         Option<String>,
    pub(crate) codec:              Option<String>,
    pub(crate) schema_fingerprint: String,
}

pub(crate) struct NeutralRecordingMetadata {
    pub(crate) format:             &'static str,
    pub(crate) producer_version:   Option<String>,
    pub(crate) layer_producer:     Option<String>,
    pub(crate) selector:           Option<String>,
    pub(crate) started_at_ns:      Option<i64>,
    pub(crate) duration_ns:        Option<u64>,
    pub(crate) num_steps:          Option<u64>,
    pub(crate) timelines:          Vec<NeutralTimeline>,
    pub(crate) streams:            Vec<NeutralStream>,
    pub(crate) schema_fingerprint: String,
}

pub(crate) fn build_manifest(
    metadata: NeutralRecordingMetadata,
    context: &EpisodeInspectionContext,
) -> Result<EpisodeManifestV1, AdapterError> {
    let stream_ids = metadata
        .streams
        .iter()
        .map(|stream| stream.stream_id.clone())
        .collect::<Vec<_>>();
    EpisodeManifestV1::try_from_draft(
        EpisodeManifestDraftV1::builder()
            .summary(
                EpisodeSummaryV1::builder()
                    .episode_id(context.episode_id())
                    .maybe_started_at_ns(metadata.started_at_ns)
                    .maybe_duration_ns(metadata.duration_ns)
                    .maybe_num_steps(metadata.num_steps)
                    .build(),
            )
            .recordings(vec![
                RecordingV1::builder()
                    .recording_id(context.recording_id())
                    .recording_format(metadata.format)
                    .maybe_producer_version(metadata.producer_version.clone())
                    .build(),
            ])
            .timelines(
                metadata
                    .timelines
                    .into_iter()
                    .map(|timeline| {
                        TimelineV1::builder()
                            .timeline_id(timeline.timeline_id)
                            .kind(timeline.kind)
                            .build()
                    })
                    .collect(),
            )
            .streams(
                metadata
                    .streams
                    .into_iter()
                    .map(|stream| {
                        StreamV1::builder()
                            .stream_id(stream.stream_id)
                            .recording_id(context.recording_id())
                            .timeline_ids(stream.timeline_ids)
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
                    .maybe_producer(metadata.layer_producer)
                    .build(),
            ])
            .artifact_bindings(vec![
                ManifestArtifactBindingV1::builder()
                    .artifact_id(context.artifact_id())
                    .layer_id(context.layer_id())
                    .role("recording")
                    .recording_id(context.recording_id())
                    .maybe_selector(metadata.selector)
                    .stream_ids(stream_ids)
                    .schema_fingerprint(metadata.schema_fingerprint)
                    .maybe_producer_version(metadata.producer_version)
                    .build(),
            ])
            .build(),
    )
    .map_err(|source| AdapterError::Manifest { source })
}
