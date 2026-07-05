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

//! Where a table's data lives.

use serde::{Deserialize, Serialize};

/// The storage URI of a table's dataset (e.g. `s3://bucket/ns/tbl` or a
/// local path in dev). The engine interprets it; the registry only stores
/// it as part of a table's registration.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableLocation(pub String);

impl TableLocation {
    pub fn new(uri: impl Into<String>) -> Self { Self(uri.into()) }

    pub fn as_str(&self) -> &str { &self.0 }
}
