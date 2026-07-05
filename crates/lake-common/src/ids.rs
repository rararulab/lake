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

//! Table and namespace identifiers.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A database / namespace grouping related tables (LanceDB-style `db`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Namespace(pub String);

/// A table name within a [`Namespace`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TableName(pub String);

/// A fully-qualified table reference: `<namespace>.<name>`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TableRef {
    pub namespace: Namespace,
    pub name:      TableName,
}

impl TableRef {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: Namespace(namespace.into()),
            name:      TableName(name.into()),
        }
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.namespace.0, self.name.0)
    }
}

/// An opaque, engine-defined snapshot identifier for one table version.
///
/// The registry stores and compares it but never interprets it: a Lance
/// engine encodes its dataset version here, a self-built engine encodes
/// whatever its format uses. Monotonic within a table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Version(pub u64);

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "v{}", self.0) }
}
