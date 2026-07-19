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

//! Canonical, format-neutral metadata for one robot Episode.

use std::collections::{BTreeMap, BTreeSet};

use bon::Builder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use snafu::Snafu;

use crate::{
    ArtifactRefV1, EpisodeBundleV1, EpisodeContractError, EpisodeRecordV1, MANIFEST_ARTIFACT_ROLE,
};

/// Version of the first immutable EpisodeManifest JSON contract.
pub const EPISODE_MANIFEST_FORMAT_VERSION: u16 = 1;

/// Media type for compact EpisodeManifest v1 JSON bytes.
pub const EPISODE_MANIFEST_MEDIA_TYPE: &str =
    "application/vnd.rararulab.lake.episode-manifest.v1+json";

/// Searchable Episode values whose table projection is derived at binding time.
#[derive(Builder, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EpisodeSummaryV1 {
    #[builder(into)]
    episode_id:    String,
    #[builder(into)]
    robot_id:      Option<String>,
    #[builder(into)]
    embodiment:    Option<String>,
    #[builder(into)]
    task:          Option<String>,
    started_at_ns: Option<i64>,
    duration_ns:   Option<u64>,
    num_steps:     Option<u64>,
    success:       Option<bool>,
    quality_score: Option<f64>,
}

impl EpisodeSummaryV1 {
    /// Return the stable logical Episode identity within its Dataset.
    #[must_use]
    pub fn episode_id(&self) -> &str { &self.episode_id }

    /// Return the optional robot identity used for Dataset selection.
    #[must_use]
    pub fn robot_id(&self) -> Option<&str> { self.robot_id.as_deref() }

    /// Return the optional robot embodiment used for Dataset selection.
    #[must_use]
    pub fn embodiment(&self) -> Option<&str> { self.embodiment.as_deref() }

    /// Return the optional task label used for Dataset selection.
    #[must_use]
    pub fn task(&self) -> Option<&str> { self.task.as_deref() }

    /// Return the optional UTC Unix timestamp in nanoseconds.
    #[must_use]
    pub const fn started_at_ns(&self) -> Option<i64> { self.started_at_ns }

    /// Return the optional Episode duration in nanoseconds.
    #[must_use]
    pub const fn duration_ns(&self) -> Option<u64> { self.duration_ns }

    /// Return the optional logical sample count.
    #[must_use]
    pub const fn num_steps(&self) -> Option<u64> { self.num_steps }

    /// Return the optional task outcome.
    #[must_use]
    pub const fn success(&self) -> Option<bool> { self.success }

    /// Return the optional source-defined finite quality score.
    #[must_use]
    pub const fn quality_score(&self) -> Option<f64> { self.quality_score }
}

/// One logical recording representation, independent of physical file count.
#[derive(Builder, Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingV1 {
    #[builder(into)]
    recording_id:     String,
    #[builder(into)]
    recording_format: String,
    #[builder(into)]
    producer_version: Option<String>,
}

impl RecordingV1 {
    /// Return the stable logical Recording identity within this Episode.
    #[must_use]
    pub fn recording_id(&self) -> &str { &self.recording_id }

    /// Return the open Adapter-owned format discriminator.
    #[must_use]
    pub fn recording_format(&self) -> &str { &self.recording_format }

    /// Return the optional source-format producer version.
    #[must_use]
    pub fn producer_version(&self) -> Option<&str> { self.producer_version.as_deref() }
}

/// Stable v1 semantics for a source timeline.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimelineKindV1 {
    /// Monotonic logical sample or frame positions.
    Sequence,
    /// Time points normalized by the Adapter for temporal alignment.
    Timestamp,
}

/// One named timeline shared by one or more Episode streams.
#[derive(Builder, Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimelineV1 {
    #[builder(into)]
    timeline_id: String,
    kind:        TimelineKindV1,
}

impl TimelineV1 {
    /// Return the stable timeline identity within this Episode.
    #[must_use]
    pub fn timeline_id(&self) -> &str { &self.timeline_id }

    /// Return whether the timeline is sequence- or timestamp-based.
    #[must_use]
    pub const fn kind(&self) -> TimelineKindV1 { self.kind }
}

/// Searchable summary of one source stream without per-sample metadata.
#[derive(Builder, Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamV1 {
    #[builder(into)]
    stream_id:          String,
    #[builder(into)]
    recording_id:       String,
    timeline_ids:       Vec<String>,
    #[builder(into)]
    media_type:         Option<String>,
    #[builder(into)]
    codec:              Option<String>,
    #[builder(into)]
    schema_fingerprint: Option<String>,
}

impl StreamV1 {
    /// Return the stable stream identity within this Episode.
    #[must_use]
    pub fn stream_id(&self) -> &str { &self.stream_id }

    /// Return the Recording that owns this stream.
    #[must_use]
    pub fn recording_id(&self) -> &str { &self.recording_id }

    /// Return the canonical timeline identities associated with this stream.
    #[must_use]
    pub fn timeline_ids(&self) -> &[String] { &self.timeline_ids }

    /// Return the optional media type for stream payloads.
    #[must_use]
    pub fn media_type(&self) -> Option<&str> { self.media_type.as_deref() }

    /// Return the optional stream codec.
    #[must_use]
    pub fn codec(&self) -> Option<&str> { self.codec.as_deref() }

    /// Return the optional logical stream schema fingerprint.
    #[must_use]
    pub fn schema_fingerprint(&self) -> Option<&str> { self.schema_fingerprint.as_deref() }
}

/// Closed v1 classes of immutable Episode data Layers.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LayerKindV1 {
    /// Original captured or simulated data.
    Base,
    /// Human- or system-produced annotations.
    Annotation,
    /// Model predictions.
    Prediction,
    /// Data-quality results.
    Quality,
    /// Model- or pipeline-produced embeddings.
    Embedding,
    /// Rebuildable visualization state.
    Visualization,
}

/// One immutable semantic Layer available to the Episode.
#[derive(Builder, Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LayerV1 {
    #[builder(into)]
    layer_id: String,
    kind:     LayerKindV1,
    #[builder(into)]
    producer: Option<String>,
}

impl LayerV1 {
    /// Return the stable Layer identity within this Episode.
    #[must_use]
    pub fn layer_id(&self) -> &str { &self.layer_id }

    /// Return the semantic Layer class.
    #[must_use]
    pub const fn kind(&self) -> LayerKindV1 { self.kind }

    /// Return optional producer provenance.
    #[must_use]
    pub fn producer(&self) -> Option<&str> { self.producer.as_deref() }
}

/// Logical projection of one non-manifest top-level ArtifactRef.
#[derive(Builder, Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestArtifactBindingV1 {
    #[builder(into)]
    artifact_id:        String,
    #[builder(into)]
    layer_id:           String,
    #[builder(into)]
    role:               String,
    #[builder(into)]
    recording_id:       Option<String>,
    #[builder(into)]
    selector:           Option<String>,
    stream_ids:         Vec<String>,
    #[builder(into)]
    sidecar_of:         Option<String>,
    #[builder(into)]
    schema_fingerprint: Option<String>,
    #[builder(into)]
    producer_version:   Option<String>,
}

impl ManifestArtifactBindingV1 {
    /// Return the stable logical Artifact identity.
    #[must_use]
    pub fn artifact_id(&self) -> &str { &self.artifact_id }

    /// Return the Layer that owns this binding.
    #[must_use]
    pub fn layer_id(&self) -> &str { &self.layer_id }

    /// Return the Artifact role within the Episode.
    #[must_use]
    pub fn role(&self) -> &str { &self.role }

    /// Return the optional Recording represented by this binding.
    #[must_use]
    pub fn recording_id(&self) -> Option<&str> { self.recording_id.as_deref() }

    /// Return the optional opaque Adapter-owned selector.
    #[must_use]
    pub fn selector(&self) -> Option<&str> { self.selector.as_deref() }

    /// Return the canonical stream identities summarized by this binding.
    #[must_use]
    pub fn stream_ids(&self) -> &[String] { &self.stream_ids }

    /// Return the optional base Artifact identity indexed by this sidecar.
    #[must_use]
    pub fn sidecar_of(&self) -> Option<&str> { self.sidecar_of.as_deref() }

    /// Return the optional logical schema fingerprint.
    #[must_use]
    pub fn schema_fingerprint(&self) -> Option<&str> { self.schema_fingerprint.as_deref() }

    /// Return the optional format producer version.
    #[must_use]
    pub fn producer_version(&self) -> Option<&str> { self.producer_version.as_deref() }
}

/// Builder input that is validated and canonicalized into an EpisodeManifest.
#[derive(Builder, Clone, Debug)]
pub struct EpisodeManifestDraftV1 {
    summary:           EpisodeSummaryV1,
    recordings:        Vec<RecordingV1>,
    timelines:         Vec<TimelineV1>,
    streams:           Vec<StreamV1>,
    layers:            Vec<LayerV1>,
    artifact_bindings: Vec<ManifestArtifactBindingV1>,
}

/// Canonical immutable structured metadata for one logical Episode.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EpisodeManifestV1 {
    format_version:    u16,
    summary:           EpisodeSummaryV1,
    recordings:        Vec<RecordingV1>,
    timelines:         Vec<TimelineV1>,
    streams:           Vec<StreamV1>,
    layers:            Vec<LayerV1>,
    artifact_bindings: Vec<ManifestArtifactBindingV1>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EpisodeManifestWireV1 {
    format_version:    u16,
    summary:           EpisodeSummaryV1,
    recordings:        Vec<RecordingV1>,
    timelines:         Vec<TimelineV1>,
    streams:           Vec<StreamV1>,
    layers:            Vec<LayerV1>,
    artifact_bindings: Vec<ManifestArtifactBindingV1>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ArtifactBindingKey {
    artifact_id:        String,
    layer_id:           String,
    role:               String,
    recording_format:   Option<String>,
    selector:           Option<String>,
    schema_fingerprint: Option<String>,
    producer_version:   Option<String>,
}

/// Permanent validation and wire failures in EpisodeManifest v1.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum EpisodeManifestError {
    /// JSON bytes were corrupt or violated the strict v1 field shape.
    #[snafu(display("EpisodeManifest JSON is invalid"))]
    Json { source: serde_json::Error },

    /// The wire declared a version this reader does not understand.
    #[snafu(display(
        "EpisodeManifest format version {version} is unsupported; expected {supported}"
    ))]
    UnsupportedVersion { version: u16, supported: u16 },

    /// A required identifier or discriminator was empty.
    #[snafu(display("EpisodeManifest {kind} field '{field}' must not be empty"))]
    EmptyField {
        kind:  &'static str,
        field: &'static str,
    },

    /// A required descriptor collection was empty.
    #[snafu(display("EpisodeManifest requires at least one {kind}"))]
    EmptyCollection { kind: &'static str },

    /// A stable identity occurred more than once in one namespace.
    #[snafu(display("EpisodeManifest contains duplicate {kind} identity '{identity}'"))]
    DuplicateIdentity {
        kind:     &'static str,
        identity: String,
    },

    /// A descriptor referenced an identity absent from the aggregate.
    #[snafu(display("EpisodeManifest {owner} references missing {kind} '{identity}'"))]
    MissingReference {
        owner:    String,
        kind:     &'static str,
        identity: String,
    },

    /// A sidecar attempted to index itself.
    #[snafu(display("EpisodeManifest Artifact '{artifact_id}' cannot be its own sidecar target"))]
    SelfReferentialSidecar { artifact_id: String },

    /// The manifest did not define exactly one base Layer.
    #[snafu(display("EpisodeManifest defines {count} base Layers; expected exactly one"))]
    InvalidBaseLayerCount { count: usize },

    /// A source-defined quality score was not representable in strict JSON.
    #[snafu(display("EpisodeManifest quality_score must be finite"))]
    NonFiniteQualityScore,

    /// The manifest tried to describe its own ArtifactRef.
    #[snafu(display("EpisodeManifest must not contain a role=manifest Artifact binding"))]
    ManifestBindingForbidden,

    /// A valid wire value was not in the canonical v1 ordering.
    #[snafu(display("EpisodeManifest wire is not canonically ordered"))]
    NonCanonical,

    /// An ArtifactRef belonged to a different Episode.
    #[snafu(display(
        "ArtifactRef episode '{artifact_episode_id}' does not match manifest episode \
         '{episode_id}'"
    ))]
    EpisodeMismatch {
        episode_id:          String,
        artifact_episode_id: String,
    },

    /// The uploaded manifest identity lacked exactly one manifest ArtifactRef.
    #[snafu(display(
        "Episode '{episode_id}' has {count} manifest ArtifactRefs for '{manifest_artifact_id}'; \
         expected one"
    ))]
    MissingManifestReference {
        episode_id:           String,
        manifest_artifact_id: String,
        count:                usize,
    },

    /// The uploaded manifest object did not identify these canonical bytes.
    #[snafu(display("EpisodeManifest ArtifactRef has mismatched {field}"))]
    ManifestObjectMismatch { field: &'static str },

    /// Logical manifest bindings and top-level ArtifactRefs did not agree.
    #[snafu(display(
        "EpisodeManifest Artifact bindings do not match ArtifactRefs (expected {expected}, \
         observed {observed})"
    ))]
    ArtifactBindingMismatch { expected: usize, observed: usize },

    /// The derived Episode bundle violated the existing table contract.
    #[snafu(display("derived Episode bundle violates the table contract"))]
    EpisodeContract { source: EpisodeContractError },
}

impl EpisodeManifestV1 {
    /// Validate and canonicalize one draft without performing I/O.
    pub fn try_from_draft(mut draft: EpisodeManifestDraftV1) -> Result<Self, EpisodeManifestError> {
        require_value("summary", "episode_id", draft.summary.episode_id())?;
        for (field, value) in [
            ("robot_id", draft.summary.robot_id()),
            ("embodiment", draft.summary.embodiment()),
            ("task", draft.summary.task()),
        ] {
            require_optional_value("summary", field, value)?;
        }
        if draft
            .summary
            .quality_score()
            .is_some_and(|value| !value.is_finite())
        {
            return Err(EpisodeManifestError::NonFiniteQualityScore);
        }
        require_collection("recording", &draft.recordings)?;
        require_collection("timeline", &draft.timelines)?;
        require_collection("stream", &draft.streams)?;
        require_collection("Layer", &draft.layers)?;
        require_collection("Artifact binding", &draft.artifact_bindings)?;

        for recording in &draft.recordings {
            require_value("Recording", "recording_id", recording.recording_id())?;
            require_value(
                "Recording",
                "recording_format",
                recording.recording_format(),
            )?;
            require_optional_value(
                "Recording",
                "producer_version",
                recording.producer_version(),
            )?;
        }
        canonicalize_unique(&mut draft.recordings, "Recording", |recording| {
            recording.recording_id()
        })?;

        for timeline in &draft.timelines {
            require_value("Timeline", "timeline_id", timeline.timeline_id())?;
        }
        canonicalize_unique(&mut draft.timelines, "Timeline", |timeline| {
            timeline.timeline_id()
        })?;

        for stream in &mut draft.streams {
            require_value("Stream", "stream_id", stream.stream_id())?;
            require_value("Stream", "recording_id", stream.recording_id())?;
            require_collection("Stream timeline", &stream.timeline_ids)?;
            canonicalize_strings(&mut stream.timeline_ids, "Stream timeline")?;
            for (field, value) in [
                ("media_type", stream.media_type()),
                ("codec", stream.codec()),
                ("schema_fingerprint", stream.schema_fingerprint()),
            ] {
                require_optional_value("Stream", field, value)?;
            }
        }
        canonicalize_unique(&mut draft.streams, "Stream", |stream| stream.stream_id())?;

        for layer in &draft.layers {
            require_value("Layer", "layer_id", layer.layer_id())?;
            require_optional_value("Layer", "producer", layer.producer())?;
        }
        canonicalize_unique(&mut draft.layers, "Layer", |layer| layer.layer_id())?;
        let base_layer_count = draft
            .layers
            .iter()
            .filter(|layer| layer.kind() == LayerKindV1::Base)
            .count();
        if base_layer_count != 1 {
            return Err(EpisodeManifestError::InvalidBaseLayerCount {
                count: base_layer_count,
            });
        }

        for binding in &mut draft.artifact_bindings {
            require_value("Artifact binding", "artifact_id", binding.artifact_id())?;
            require_value("Artifact binding", "layer_id", binding.layer_id())?;
            require_value("Artifact binding", "role", binding.role())?;
            if binding.role() == MANIFEST_ARTIFACT_ROLE {
                return Err(EpisodeManifestError::ManifestBindingForbidden);
            }
            for (field, value) in [
                ("recording_id", binding.recording_id()),
                ("selector", binding.selector()),
                ("sidecar_of", binding.sidecar_of()),
                ("schema_fingerprint", binding.schema_fingerprint()),
                ("producer_version", binding.producer_version()),
            ] {
                require_optional_value("Artifact binding", field, value)?;
            }
            canonicalize_strings(&mut binding.stream_ids, "Artifact binding stream")?;
        }
        canonicalize_exact_bindings(&mut draft.artifact_bindings)?;

        let recording_formats = draft
            .recordings
            .iter()
            .map(|recording| {
                (
                    recording.recording_id().to_owned(),
                    recording.recording_format().to_owned(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let timeline_ids = id_set(&draft.timelines, |timeline| timeline.timeline_id());
        let streams = draft
            .streams
            .iter()
            .map(|stream| (stream.stream_id(), stream))
            .collect::<BTreeMap<_, _>>();
        let layer_ids = id_set(&draft.layers, |layer| layer.layer_id());
        let artifact_ids = id_set(&draft.artifact_bindings, |binding| binding.artifact_id());

        for stream in &draft.streams {
            require_reference(
                &recording_formats,
                format!("Stream '{}'", stream.stream_id()),
                "Recording",
                stream.recording_id(),
            )?;
            for timeline_id in stream.timeline_ids() {
                require_reference(
                    &timeline_ids,
                    format!("Stream '{}'", stream.stream_id()),
                    "Timeline",
                    timeline_id,
                )?;
            }
        }

        for binding in &draft.artifact_bindings {
            require_reference(
                &layer_ids,
                format!("Artifact '{}'", binding.artifact_id()),
                "Layer",
                binding.layer_id(),
            )?;
            if let Some(recording_id) = binding.recording_id() {
                require_reference(
                    &recording_formats,
                    format!("Artifact '{}'", binding.artifact_id()),
                    "Recording",
                    recording_id,
                )?;
            } else if !binding.stream_ids().is_empty() {
                return Err(EpisodeManifestError::MissingReference {
                    owner:    format!("Artifact '{}'", binding.artifact_id()),
                    kind:     "Recording",
                    identity: "<required for stream bindings>".to_owned(),
                });
            }
            for stream_id in binding.stream_ids() {
                let stream = streams.get(stream_id.as_str()).ok_or_else(|| {
                    EpisodeManifestError::MissingReference {
                        owner:    format!("Artifact '{}'", binding.artifact_id()),
                        kind:     "Stream",
                        identity: stream_id.clone(),
                    }
                })?;
                if binding.recording_id() != Some(stream.recording_id()) {
                    return Err(EpisodeManifestError::MissingReference {
                        owner:    format!("Artifact '{}'", binding.artifact_id()),
                        kind:     "Recording-compatible Stream",
                        identity: stream_id.clone(),
                    });
                }
            }
            if let Some(sidecar_of) = binding.sidecar_of() {
                if sidecar_of == binding.artifact_id() {
                    return Err(EpisodeManifestError::SelfReferentialSidecar {
                        artifact_id: binding.artifact_id().to_owned(),
                    });
                }
                require_reference(
                    &artifact_ids,
                    format!("Artifact '{}'", binding.artifact_id()),
                    "Artifact",
                    sidecar_of,
                )?;
            }
        }

        Ok(Self {
            format_version:    EPISODE_MANIFEST_FORMAT_VERSION,
            summary:           draft.summary,
            recordings:        draft.recordings,
            timelines:         draft.timelines,
            streams:           draft.streams,
            layers:            draft.layers,
            artifact_bindings: draft.artifact_bindings,
        })
    }

    /// Decode strict canonical v1 JSON and re-run every aggregate invariant.
    pub fn from_json(bytes: &[u8]) -> Result<Self, EpisodeManifestError> {
        let wire: EpisodeManifestWireV1 = serde_json::from_slice(bytes)
            .map_err(|source| EpisodeManifestError::Json { source })?;
        if wire.format_version != EPISODE_MANIFEST_FORMAT_VERSION {
            return Err(EpisodeManifestError::UnsupportedVersion {
                version:   wire.format_version,
                supported: EPISODE_MANIFEST_FORMAT_VERSION,
            });
        }
        let observed = Self {
            format_version:    wire.format_version,
            summary:           wire.summary.clone(),
            recordings:        wire.recordings.clone(),
            timelines:         wire.timelines.clone(),
            streams:           wire.streams.clone(),
            layers:            wire.layers.clone(),
            artifact_bindings: wire.artifact_bindings.clone(),
        };
        let canonical = Self::try_from_draft(EpisodeManifestDraftV1 {
            summary:           wire.summary,
            recordings:        wire.recordings,
            timelines:         wire.timelines,
            streams:           wire.streams,
            layers:            wire.layers,
            artifact_bindings: wire.artifact_bindings,
        })?;
        if canonical != observed {
            return Err(EpisodeManifestError::NonCanonical);
        }
        Ok(canonical)
    }

    /// Encode the already-canonical manifest as compact UTF-8 JSON.
    pub fn to_json(&self) -> Result<Vec<u8>, EpisodeManifestError> {
        serde_json::to_vec(self).map_err(|source| EpisodeManifestError::Json { source })
    }

    /// Derive the Episode row and prove all logical Artifact bindings are
    /// represented by top-level GC-visible ArtifactRefs.
    pub fn bind(
        &self,
        manifest_artifact_id: impl Into<String>,
        artifact_refs: Vec<ArtifactRefV1>,
    ) -> Result<EpisodeBundleV1, EpisodeManifestError> {
        let manifest_artifact_id = manifest_artifact_id.into();
        require_value("binding", "manifest_artifact_id", &manifest_artifact_id)?;
        for artifact in &artifact_refs {
            if artifact.episode_id() != self.summary.episode_id() {
                return Err(EpisodeManifestError::EpisodeMismatch {
                    episode_id:          self.summary.episode_id().to_owned(),
                    artifact_episode_id: artifact.episode_id().to_owned(),
                });
            }
        }
        let manifest_count = artifact_refs
            .iter()
            .filter(|artifact| {
                artifact.role() == MANIFEST_ARTIFACT_ROLE
                    && artifact.artifact_id() == manifest_artifact_id
            })
            .count();
        if manifest_count != 1 {
            return Err(EpisodeManifestError::MissingManifestReference {
                episode_id: self.summary.episode_id().to_owned(),
                manifest_artifact_id,
                count: manifest_count,
            });
        }
        let manifest_ref = artifact_refs
            .iter()
            .find(|artifact| {
                artifact.role() == MANIFEST_ARTIFACT_ROLE
                    && artifact.artifact_id() == manifest_artifact_id
            })
            .expect("exactly one manifest reference was counted");
        let manifest_bytes = self.to_json()?;
        let expected_size =
            u64::try_from(manifest_bytes.len()).expect("manifest byte length fits u64");
        if manifest_ref.object().content_type != EPISODE_MANIFEST_MEDIA_TYPE {
            return Err(EpisodeManifestError::ManifestObjectMismatch {
                field: "content_type",
            });
        }
        if manifest_ref.object().size_bytes != expected_size {
            return Err(EpisodeManifestError::ManifestObjectMismatch {
                field: "size_bytes",
            });
        }
        let expected_sha256 = format!("{:x}", Sha256::digest(&manifest_bytes));
        if manifest_ref.object().sha256 != expected_sha256 {
            return Err(EpisodeManifestError::ManifestObjectMismatch { field: "sha256" });
        }

        let recording_formats = self
            .recordings
            .iter()
            .map(|recording| (recording.recording_id(), recording.recording_format()))
            .collect::<BTreeMap<_, _>>();
        let expected = self
            .artifact_bindings
            .iter()
            .map(|binding| binding_key(binding, &recording_formats))
            .fold(BTreeMap::new(), count_binding_key);
        let actual_refs = artifact_refs
            .iter()
            .filter(|artifact| {
                !(artifact.role() == MANIFEST_ARTIFACT_ROLE
                    && artifact.artifact_id() == manifest_artifact_id)
            })
            .collect::<Vec<_>>();
        let actual = actual_refs
            .iter()
            .map(|artifact| artifact_ref_key(artifact))
            .fold(BTreeMap::new(), count_binding_key);
        if expected != actual {
            return Err(EpisodeManifestError::ArtifactBindingMismatch {
                expected: self.artifact_bindings.len(),
                observed: actual_refs.len(),
            });
        }

        let summary = &self.summary;
        let episode = EpisodeRecordV1::builder()
            .episode_id(summary.episode_id.clone())
            .manifest_artifact_id(manifest_artifact_id)
            .maybe_robot_id(summary.robot_id.clone())
            .maybe_embodiment(summary.embodiment.clone())
            .maybe_task(summary.task.clone())
            .maybe_started_at_ns(summary.started_at_ns)
            .maybe_duration_ns(summary.duration_ns)
            .maybe_num_steps(summary.num_steps)
            .maybe_success(summary.success)
            .maybe_quality_score(summary.quality_score)
            .build();
        EpisodeBundleV1::try_new(episode, artifact_refs)
            .map_err(|source| EpisodeManifestError::EpisodeContract { source })
    }

    /// Return the explicit wire format version.
    #[must_use]
    pub const fn format_version(&self) -> u16 { self.format_version }

    /// Return the authoritative Episode summary.
    #[must_use]
    pub const fn summary(&self) -> &EpisodeSummaryV1 { &self.summary }

    /// Return canonical Recording descriptors.
    #[must_use]
    pub fn recordings(&self) -> &[RecordingV1] { &self.recordings }

    /// Return canonical Timeline descriptors.
    #[must_use]
    pub fn timelines(&self) -> &[TimelineV1] { &self.timelines }

    /// Return canonical Stream descriptors.
    #[must_use]
    pub fn streams(&self) -> &[StreamV1] { &self.streams }

    /// Return canonical Layer descriptors.
    #[must_use]
    pub fn layers(&self) -> &[LayerV1] { &self.layers }

    /// Return canonical non-manifest Artifact bindings.
    #[must_use]
    pub fn artifact_bindings(&self) -> &[ManifestArtifactBindingV1] { &self.artifact_bindings }
}

fn require_value(
    kind: &'static str,
    field: &'static str,
    value: &str,
) -> Result<(), EpisodeManifestError> {
    if value.trim().is_empty() {
        return Err(EpisodeManifestError::EmptyField { kind, field });
    }
    Ok(())
}

fn require_optional_value(
    kind: &'static str,
    field: &'static str,
    value: Option<&str>,
) -> Result<(), EpisodeManifestError> {
    value.map_or(Ok(()), |value| require_value(kind, field, value))
}

fn require_collection<T>(kind: &'static str, values: &[T]) -> Result<(), EpisodeManifestError> {
    if values.is_empty() {
        return Err(EpisodeManifestError::EmptyCollection { kind });
    }
    Ok(())
}

fn canonicalize_unique<T>(
    values: &mut [T],
    kind: &'static str,
    identity: impl Fn(&T) -> &str,
) -> Result<(), EpisodeManifestError> {
    let mut identities = BTreeSet::new();
    for value in values.iter() {
        let identity = identity(value);
        if !identities.insert(identity.to_owned()) {
            return Err(EpisodeManifestError::DuplicateIdentity {
                kind,
                identity: identity.to_owned(),
            });
        }
    }
    values.sort_by(|left, right| identity(left).cmp(identity(right)));
    Ok(())
}

fn canonicalize_strings(
    values: &mut [String],
    kind: &'static str,
) -> Result<(), EpisodeManifestError> {
    for value in values.iter() {
        require_value(kind, "identity", value)?;
    }
    values.sort();
    if let Some(duplicate) = values.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(EpisodeManifestError::DuplicateIdentity {
            kind,
            identity: duplicate[0].clone(),
        });
    }
    Ok(())
}

fn canonicalize_exact_bindings(
    bindings: &mut [ManifestArtifactBindingV1],
) -> Result<(), EpisodeManifestError> {
    bindings.sort();
    if let Some(duplicate) = bindings.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(EpisodeManifestError::DuplicateIdentity {
            kind:     "Artifact binding",
            identity: duplicate[0].artifact_id().to_owned(),
        });
    }
    Ok(())
}

fn id_set<'a, T>(values: &'a [T], identity: impl Fn(&'a T) -> &'a str) -> BTreeSet<&'a str> {
    values.iter().map(identity).collect()
}

fn require_reference<C>(
    values: &C,
    owner: String,
    kind: &'static str,
    identity: &str,
) -> Result<(), EpisodeManifestError>
where
    C: ContainsIdentity,
{
    if !values.contains_identity(identity) {
        return Err(EpisodeManifestError::MissingReference {
            owner,
            kind,
            identity: identity.to_owned(),
        });
    }
    Ok(())
}

trait ContainsIdentity {
    fn contains_identity(&self, identity: &str) -> bool;
}

impl<V> ContainsIdentity for BTreeMap<String, V> {
    fn contains_identity(&self, identity: &str) -> bool { self.contains_key(identity) }
}

impl ContainsIdentity for BTreeSet<&str> {
    fn contains_identity(&self, identity: &str) -> bool { self.contains(identity) }
}

fn binding_key(
    binding: &ManifestArtifactBindingV1,
    recording_formats: &BTreeMap<&str, &str>,
) -> ArtifactBindingKey {
    let recording_format = binding
        .recording_id()
        .and_then(|recording_id| recording_formats.get(recording_id).copied())
        .map(str::to_owned);
    ArtifactBindingKey {
        artifact_id: binding.artifact_id().to_owned(),
        layer_id: binding.layer_id().to_owned(),
        role: binding.role().to_owned(),
        recording_format,
        selector: binding.selector().map(str::to_owned),
        schema_fingerprint: binding.schema_fingerprint().map(str::to_owned),
        producer_version: binding.producer_version().map(str::to_owned),
    }
}

fn count_binding_key(
    mut counts: BTreeMap<ArtifactBindingKey, usize>,
    key: ArtifactBindingKey,
) -> BTreeMap<ArtifactBindingKey, usize> {
    *counts.entry(key).or_default() += 1;
    counts
}

fn artifact_ref_key(artifact: &ArtifactRefV1) -> ArtifactBindingKey {
    ArtifactBindingKey {
        artifact_id:        artifact.artifact_id().to_owned(),
        layer_id:           artifact.layer_id().to_owned(),
        role:               artifact.role().to_owned(),
        recording_format:   artifact.recording_format().map(str::to_owned),
        selector:           artifact.selector().map(str::to_owned),
        schema_fingerprint: artifact.schema_fingerprint().map(str::to_owned),
        producer_version:   artifact.producer_version().map(str::to_owned),
    }
}
