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

//! Stable references to immutable large objects.

use bon::Builder;
use serde::{Deserialize, Serialize};

/// The physical representation of a complete, immutable SQL `FILE` value.
///
/// The URI is durable object identity, not an expiring download capability.
/// Callers use the SDK's object reader to resolve it into direct I/O.
#[derive(Builder, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataLocation {
    /// Managed URI naming the immutable object.
    #[builder(into)]
    pub uri:          String,
    /// IANA media type supplied by the SDK.
    #[builder(into)]
    pub content_type: String,
    /// Exact object size in bytes.
    pub size_bytes:   u64,
    /// Lowercase hex SHA-256 digest of the object bytes.
    #[builder(into)]
    pub sha256:       String,
}
