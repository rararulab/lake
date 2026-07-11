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

/// Protobuf `Any` type URL used for query-mediated FILE appends.
pub const FILE_APPEND_TYPE_URL: &str = "type.googleapis.com/lake.file.Append";

/// The metadata-only Flight command for appending already-uploaded SQL
/// `FILE` rows.
///
/// The command identifies only the target table. Credentials and object
/// payload bytes remain outside the control-plane contract; object locations
/// are carried in the Arrow rows that follow it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileAppendRequest {
    table: TableRef,
}

impl FileAppendRequest {
    /// Create a request targeting `table`.
    #[must_use]
    pub fn new(table: TableRef) -> Self { Self { table } }

    /// Encode the transport-neutral command payload carried by Flight `Any`.
    #[must_use]
    pub fn command_payload(&self) -> Vec<u8> {
        format!("{}\0{}", self.table.namespace.0, self.table.name.0).into_bytes()
    }

    /// Decode a command payload without depending on Protobuf or Flight.
    #[must_use]
    pub fn from_command_payload(payload: &[u8]) -> Option<Self> {
        let payload = std::str::from_utf8(payload).ok()?;
        let (namespace, table) = payload.split_once('\0')?;
        if namespace.is_empty() || table.is_empty() || table.contains('\0') {
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

        let payload = request.command_payload();

        assert_eq!(
            FileAppendRequest::from_command_payload(&payload),
            Some(request)
        );
        assert!(FileAppendRequest::from_command_payload(b"not-a-file-command").is_none());
    }
}
