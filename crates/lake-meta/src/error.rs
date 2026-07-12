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

//! Metastore error type.

use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum MetaError {
    #[snafu(display("metastore operation failed on key '{key}'"))]
    Backend {
        key:    String,
        source: rocksdb::Error,
    },

    #[snafu(display("corrupt registry entry at key '{key}'"))]
    CorruptEntry {
        key:    String,
        source: serde_json::Error,
    },

    #[snafu(display("table '{table}' already registered"))]
    AlreadyRegistered { table: String },

    #[snafu(display("registry conflict on '{table}': entry moved under us"))]
    Conflict { table: String },

    #[snafu(display("dynamodb {message}"))]
    Dynamo {
        message: String,
        source:  Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("invalid guarded mutation: guard and target keys must differ"))]
    InvalidGuardedMutation,

    #[snafu(display("metastore backend does not support atomic guarded mutations"))]
    GuardedMutationUnsupported,

    #[snafu(display("metadata mutation requires a live lease guard"))]
    MutationGuardUnavailable,

    #[snafu(display("registry scan limit must be greater than zero"))]
    InvalidScanLimit,

    #[snafu(display("invalid DynamoDB prefix cursor: {message}"))]
    InvalidScanCursor { message: String },

    #[snafu(display("invalid DynamoDB migration page size: {limit}"))]
    InvalidMigrationPageSize { limit: usize },

    #[snafu(display("DynamoDB prefix migration did not converge: {message}"))]
    MigrationConflict { message: String },
}

pub type Result<T> = std::result::Result<T, MetaError>;
