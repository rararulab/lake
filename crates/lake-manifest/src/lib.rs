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

//! Immutable table snapshots. A manifest lists the data files of one table
//! version and is written once, never rewritten — so every reader node can
//! cache it forever. The KV metastore only holds the current-version
//! pointer.

mod commit;
mod error;
mod model;

pub use commit::{commit, current_version, load_current};
pub use error::{ManifestError, Result};
pub use model::{Manifest, manifest_path};
