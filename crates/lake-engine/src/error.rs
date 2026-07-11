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

//! Engine error type.

use lake_common::TableLocation;
use snafu::Snafu;

/// Errors an engine implementation may surface. Kept engine-agnostic:
/// backend-specific causes (Lance, object store) are boxed into `Backend`.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum EngineError {
    #[snafu(display("table already exists at {}", location.0))]
    AlreadyExists { location: TableLocation },

    #[snafu(display("schema mismatch on append to {}", location.0))]
    SchemaMismatch { location: TableLocation },

    #[snafu(display("object reference page size must be positive; got {size}"))]
    InvalidReferencePageSize { size: usize },

    #[snafu(display("object reference lineage is unavailable for {}: {reason}", location.0))]
    ReferenceLineageUnavailable {
        location: TableLocation,
        reason:   String,
    },

    #[snafu(display("engine backend failed: {message}"))]
    Backend {
        message: String,
        source:  Box<dyn std::error::Error + Send + Sync>,
    },
}

impl EngineError {
    /// Wrap an engine-backend error (Lance, object store, …).
    pub fn backend<E>(source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Backend {
            message: source.to_string(),
            source:  Box::new(source),
        }
    }

    pub fn already_exists(location: TableLocation) -> Self { Self::AlreadyExists { location } }
}

pub type Result<T> = std::result::Result<T, EngineError>;
