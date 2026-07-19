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

//! Format-neutral robot Episode and Artifact reference values.

use bon::Builder;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use crate::DataLocation;

/// Version of the first Episode/ArtifactRef Dataset-table contract.
pub const EPISODE_TABLE_CONTRACT_VERSION: u16 = 1;

/// Discriminator stored on a selectable Episode summary row.
pub const EPISODE_RECORD_KIND: &str = "episode";

/// Discriminator stored on a GC-visible Artifact reference row.
pub const ARTIFACT_REF_RECORD_KIND: &str = "artifact_ref";

/// Artifact role binding the Episode summary to its structured manifest.
pub const MANIFEST_ARTIFACT_ROLE: &str = "manifest";

/// Searchable scalar summary for one logical robot Episode.
///
/// Episode identity is independent of object names, recording formats, and
/// shard boundaries. The referenced manifest itself is represented by a
/// separate [`ArtifactRefV1`] in the same initial bundle.
#[derive(Builder, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EpisodeRecordV1 {
    #[builder(into)]
    episode_id:           String,
    #[builder(into)]
    manifest_artifact_id: String,
    #[builder(into)]
    robot_id:             Option<String>,
    #[builder(into)]
    embodiment:           Option<String>,
    #[builder(into)]
    task:                 Option<String>,
    started_at_ns:        Option<i64>,
    duration_ns:          Option<u64>,
    num_steps:            Option<u64>,
    success:              Option<bool>,
    quality_score:        Option<f64>,
}

impl EpisodeRecordV1 {
    /// Return the stable logical identity within its Dataset.
    #[must_use]
    pub fn episode_id(&self) -> &str { &self.episode_id }

    /// Return the Artifact identity of the structured Episode manifest.
    #[must_use]
    pub fn manifest_artifact_id(&self) -> &str { &self.manifest_artifact_id }

    /// Return the optional robot identity used for Dataset selection.
    #[must_use]
    pub fn robot_id(&self) -> Option<&str> { self.robot_id.as_deref() }

    /// Return the optional embodiment used for Dataset selection.
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

    /// Return the optional source-defined quality score.
    #[must_use]
    pub const fn quality_score(&self) -> Option<f64> { self.quality_score }
}

/// One top-level managed-object reference reachable from an Episode.
///
/// Format metadata remains beside the exact [`DataLocation`] value so neither
/// the object identity nor storage-engine boundary depends on Rerun, MCAP, or
/// another recording format.
#[derive(Builder, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRefV1 {
    #[builder(into)]
    episode_id:         String,
    #[builder(into)]
    artifact_id:        String,
    #[builder(into)]
    layer_id:           String,
    #[builder(into)]
    role:               String,
    #[builder(into)]
    recording_format:   Option<String>,
    #[builder(into)]
    selector:           Option<String>,
    object:             DataLocation,
    #[builder(into)]
    schema_fingerprint: Option<String>,
    #[builder(into)]
    producer_version:   Option<String>,
}

impl ArtifactRefV1 {
    /// Return the logical Episode that owns this reference edge.
    #[must_use]
    pub fn episode_id(&self) -> &str { &self.episode_id }

    /// Return the stable logical Artifact identity.
    #[must_use]
    pub fn artifact_id(&self) -> &str { &self.artifact_id }

    /// Return the immutable Layer identity selecting this reference.
    #[must_use]
    pub fn layer_id(&self) -> &str { &self.layer_id }

    /// Return the Artifact's role within the Episode.
    #[must_use]
    pub fn role(&self) -> &str { &self.role }

    /// Return the optional recording-format discriminator.
    #[must_use]
    pub fn recording_format(&self) -> Option<&str> { self.recording_format.as_deref() }

    /// Return the optional format-specific selector within a shared Artifact.
    #[must_use]
    pub fn selector(&self) -> Option<&str> { self.selector.as_deref() }

    /// Return the complete immutable managed-object identity.
    #[must_use]
    pub const fn object(&self) -> &DataLocation { &self.object }

    /// Return the optional logical-schema fingerprint.
    #[must_use]
    pub fn schema_fingerprint(&self) -> Option<&str> { self.schema_fingerprint.as_deref() }

    /// Return the optional format producer version.
    #[must_use]
    pub fn producer_version(&self) -> Option<&str> { self.producer_version.as_deref() }
}

/// A validated initial append unit for one Episode and all of its Artifacts.
#[derive(Clone, Debug, PartialEq)]
pub struct EpisodeBundleV1 {
    episode:       EpisodeRecordV1,
    artifact_refs: Vec<ArtifactRefV1>,
}

/// Permanent validation failures in the v1 Episode table contract.
#[derive(Debug, PartialEq, Snafu)]
#[snafu(visibility(pub))]
pub enum EpisodeContractError {
    /// A required identifier or discriminator was empty.
    #[snafu(display("{record_kind} field '{field}' must not be empty"))]
    EmptyField {
        record_kind: &'static str,
        field:       &'static str,
    },

    /// An ArtifactRef pointed at another logical Episode.
    #[snafu(display(
        "ArtifactRef episode '{artifact_episode_id}' does not match bundle episode '{episode_id}'"
    ))]
    EpisodeMismatch {
        episode_id:          String,
        artifact_episode_id: String,
    },

    /// The manifest identity was not backed by a GC-visible top-level FILE.
    #[snafu(display(
        "episode '{episode_id}' has no manifest ArtifactRef for '{manifest_artifact_id}'"
    ))]
    MissingManifestReference {
        episode_id:           String,
        manifest_artifact_id: String,
    },
}

impl EpisodeBundleV1 {
    /// Validate one initial Episode append before any Arrow batch is created.
    pub fn try_new(
        episode: EpisodeRecordV1,
        artifact_refs: Vec<ArtifactRefV1>,
    ) -> Result<Self, EpisodeContractError> {
        require_value(EPISODE_RECORD_KIND, "episode_id", episode.episode_id())?;
        require_value(
            EPISODE_RECORD_KIND,
            "manifest_artifact_id",
            episode.manifest_artifact_id(),
        )?;
        for artifact in &artifact_refs {
            require_value(
                ARTIFACT_REF_RECORD_KIND,
                "episode_id",
                artifact.episode_id(),
            )?;
            require_value(
                ARTIFACT_REF_RECORD_KIND,
                "artifact_id",
                artifact.artifact_id(),
            )?;
            require_value(ARTIFACT_REF_RECORD_KIND, "layer_id", artifact.layer_id())?;
            require_value(ARTIFACT_REF_RECORD_KIND, "role", artifact.role())?;
            if artifact.episode_id() != episode.episode_id() {
                return Err(EpisodeContractError::EpisodeMismatch {
                    episode_id:          episode.episode_id().to_owned(),
                    artifact_episode_id: artifact.episode_id().to_owned(),
                });
            }
        }
        let has_manifest = artifact_refs.iter().any(|artifact| {
            artifact.role() == MANIFEST_ARTIFACT_ROLE
                && artifact.artifact_id() == episode.manifest_artifact_id()
        });
        if !has_manifest {
            return Err(EpisodeContractError::MissingManifestReference {
                episode_id:           episode.episode_id().to_owned(),
                manifest_artifact_id: episode.manifest_artifact_id().to_owned(),
            });
        }
        Ok(Self {
            episode,
            artifact_refs,
        })
    }

    /// Return the selectable Episode summary.
    #[must_use]
    pub const fn episode(&self) -> &EpisodeRecordV1 { &self.episode }

    /// Return every GC-visible Artifact reference in append order.
    #[must_use]
    pub fn artifact_refs(&self) -> &[ArtifactRefV1] { &self.artifact_refs }
}

fn require_value(
    record_kind: &'static str,
    field: &'static str,
    value: &str,
) -> Result<(), EpisodeContractError> {
    if value.trim().is_empty() {
        return Err(EpisodeContractError::EmptyField { record_kind, field });
    }
    Ok(())
}
