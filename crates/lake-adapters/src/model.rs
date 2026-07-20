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

//! Format-neutral adapter context, output seam, and errors.

use async_trait::async_trait;
use bon::Builder;
use lake_common::{EpisodeManifestError, EpisodeManifestV1};
use snafu::Snafu;

use crate::{RandomAccessSource, ReadBudget};

/// The finite resource whose pre-I/O accounting rejected a source read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BudgetResource {
    /// Bytes returned across all random-access reads.
    Bytes,
    /// Number of random-access read requests.
    Requests,
}

/// Lake-owned identities and optional source selector for one inspection.
#[derive(Builder, Clone, Debug, Eq, PartialEq)]
pub struct EpisodeInspectionContext {
    #[builder(into)]
    episode_id:   String,
    #[builder(into)]
    recording_id: String,
    #[builder(into)]
    layer_id:     String,
    #[builder(into)]
    artifact_id:  String,
    #[builder(into)]
    selector:     Option<String>,
}

impl EpisodeInspectionContext {
    /// Return the stable Lake Episode identity supplied by the caller.
    #[must_use]
    pub fn episode_id(&self) -> &str { &self.episode_id }

    /// Return the stable Lake Recording identity supplied by the caller.
    #[must_use]
    pub fn recording_id(&self) -> &str { &self.recording_id }

    /// Return the stable base Layer identity supplied by the caller.
    #[must_use]
    pub fn layer_id(&self) -> &str { &self.layer_id }

    /// Return the stable source Artifact identity supplied by the caller.
    #[must_use]
    pub fn artifact_id(&self) -> &str { &self.artifact_id }

    /// Return an optional opaque source-native recording selector.
    #[must_use]
    pub fn selector(&self) -> Option<&str> { self.selector.as_deref() }

    pub(crate) fn validate(&self) -> Result<(), AdapterError> {
        for (field, value) in [
            ("episode_id", self.episode_id()),
            ("recording_id", self.recording_id()),
            ("layer_id", self.layer_id()),
            ("artifact_id", self.artifact_id()),
        ] {
            if value.trim().is_empty() {
                return Err(AdapterError::InvalidContext { field });
            }
        }
        if self.selector().is_some_and(|value| value.trim().is_empty()) {
            return Err(AdapterError::InvalidContext { field: "selector" });
        }
        Ok(())
    }
}

/// Fail-closed errors shared by every format adapter.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum AdapterError {
    /// A caller supplied a zero-valued finite extraction limit.
    #[snafu(display("adapter read budget field '{field}' must be greater than zero"))]
    InvalidBudget { field: &'static str },

    /// A caller supplied a missing or empty Lake-owned identity.
    #[snafu(display("adapter context field '{field}' must not be empty"))]
    InvalidContext { field: &'static str },

    /// A range was empty or reversed before any source I/O.
    #[snafu(display("adapter source range {start}..{end} is invalid"))]
    InvalidRange { start: u64, end: u64 },

    /// A source range would cross a caller-provided finite limit.
    #[snafu(display(
        "adapter {resource:?} budget {limit} would be exceeded by cumulative value {attempted}"
    ))]
    BudgetExceeded {
        resource:  BudgetResource,
        limit:     u64,
        attempted: u64,
    },

    /// A source returned fewer or more bytes than its requested exact range.
    #[snafu(display(
        "adapter source returned {actual} bytes for {start}..{end}; expected {expected}"
    ))]
    ShortRead {
        start:    u64,
        end:      u64,
        expected: u64,
        actual:   u64,
    },

    /// The caller-owned source could not serve metadata or a byte range.
    #[snafu(display("adapter source operation failed"))]
    Source { source: crate::SourceError },

    /// The optional index was absent but the complete source exceeded the scan
    /// ceiling.
    #[snafu(display(
        "adapter fallback requires {size_bytes} bytes; configured ceiling is {limit_bytes}"
    ))]
    FallbackScanTooLarge { size_bytes: u64, limit_bytes: u64 },

    /// One declared source-format record exceeded the per-record decode limit.
    #[snafu(display(
        "{format} record requires {size_bytes} bytes; configured ceiling is {limit_bytes}"
    ))]
    RecordTooLarge {
        format:      &'static str,
        size_bytes:  u64,
        limit_bytes: u64,
    },

    /// An upstream format decoder rejected bytes or metadata.
    #[snafu(display("{format} metadata is invalid: {message}"))]
    Format {
        format:  &'static str,
        message: String,
    },

    /// The source contained more than one recording and no exact selector
    /// resolved it.
    #[snafu(display("{format} source contains {count} candidate recordings"))]
    AmbiguousRecording { format: &'static str, count: usize },

    /// The caller's opaque selector did not identify a recording.
    #[snafu(display("{format} recording selector did not match any candidate"))]
    RecordingNotFound { format: &'static str },

    /// Upstream metadata had no temporal stream representable by manifest v1.
    #[snafu(display("{format} recording contains no temporal streams"))]
    NoTemporalStreams { format: &'static str },

    /// Neutral metadata violated the existing Lake manifest contract.
    #[snafu(display("adapter metadata violates EpisodeManifestV1"))]
    Manifest { source: EpisodeManifestError },
}

/// Common async seam implemented by every recording-format adapter.
#[async_trait]
pub trait RecordingAdapter: Send + Sync {
    /// Extract one canonical Episode manifest within all caller-provided
    /// limits.
    async fn inspect(
        &self,
        source: &dyn RandomAccessSource,
        context: &EpisodeInspectionContext,
        budget: ReadBudget,
    ) -> Result<EpisodeManifestV1, AdapterError>;
}
