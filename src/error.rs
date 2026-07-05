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

//! Error types for the lake crate.

use std::path::PathBuf;

use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum LakeError {
    #[snafu(display("metastore operation failed on key '{key}'"))]
    Meta {
        key:    String,
        source: rocksdb::Error,
    },

    #[snafu(display("manifest I/O failed at {}", path.display()))]
    ManifestIo {
        path:   PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("manifest encode/decode failed at {}", path.display()))]
    ManifestCodec {
        path:   PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display("corrupt version pointer for table '{table}'"))]
    CorruptPointer { table: String },

    #[snafu(display(
        "manifest v{version} for table '{table}' already exists (concurrent writer?)"
    ))]
    ManifestExists { table: String, version: u64 },

    #[snafu(display("commit conflict on table '{table}': version pointer moved"))]
    CommitConflict { table: String },
}

pub type Result<T> = std::result::Result<T, LakeError>;
