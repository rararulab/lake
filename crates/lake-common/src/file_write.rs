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

use crate::TableRef;

/// The metadata-only Flight path for appending already-uploaded SQL `FILE`
/// rows.
///
/// The path identifies only the target table. Object URIs, credentials, and
/// payload bytes remain outside the control-plane contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileAppendRequest {
    table: TableRef,
}

impl FileAppendRequest {
    /// Create a request targeting `table`.
    #[must_use]
    pub fn new(table: TableRef) -> Self { Self { table } }

    /// Return the Flight descriptor path used by query and metadata services.
    #[must_use]
    pub fn descriptor_path(&self) -> Vec<String> {
        vec![
            "lake".to_owned(),
            "file".to_owned(),
            "append".to_owned(),
            self.table.namespace.0.clone(),
            self.table.name.0.clone(),
        ]
    }

    /// Parse a descriptor path without depending on a Flight implementation.
    #[must_use]
    pub fn from_descriptor_path(path: &[String]) -> Option<Self> {
        let [lake, file, append, namespace, table] = path else {
            return None;
        };
        if lake != "lake" || file != "file" || append != "append" {
            return None;
        }
        Some(Self::new(TableRef::new(namespace, table)))
    }

    /// Return the target table.
    #[must_use]
    pub fn table(&self) -> &TableRef { &self.table }
}

#[cfg(test)]
mod tests {
    use crate::{FileAppendRequest, TableRef};

    #[test]
    fn file_append_request_roundtrip() {
        let request = FileAppendRequest::new(TableRef::new("robots", "episodes"));

        let path = request.descriptor_path();

        assert_eq!(
            FileAppendRequest::from_descriptor_path(&path),
            Some(request)
        );
        assert!(FileAppendRequest::from_descriptor_path(&["lake".into()]).is_none());
    }
}
