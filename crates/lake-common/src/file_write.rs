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

use std::fmt;

use crate::{TableRef, TenantId};

const FILE_APPEND_WIRE_VERSION: u8 = 1;
const SHA256_HEX_LEN: usize = 64;

/// Protobuf `Any` type URL used for query-mediated FILE appends.
pub const FILE_APPEND_TYPE_URL: &str = "type.googleapis.com/lake.file.Append";

/// A time-bearing, high-entropy identity for one logical append.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendOperationId(String);

impl AppendOperationId {
    /// Generate a time-bearing UUIDv7 identity for a new logical append.
    #[must_use]
    pub fn generate() -> Self { Self(uuid::Uuid::now_v7().to_string()) }

    /// Parse a UUIDv7 operation identity.
    #[must_use]
    pub fn parse(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        let uuid = uuid::Uuid::parse_str(&value).ok()?;
        (uuid.get_version_num() == 7).then(|| Self(uuid.to_string()))
    }

    /// Return the canonical UUID string.
    #[must_use]
    pub fn as_str(&self) -> &str { &self.0 }

    /// Return the UUIDv7 Unix timestamp in seconds.
    #[must_use]
    pub fn unix_seconds(&self) -> u64 {
        uuid::Uuid::parse_str(&self.0)
            .expect("validated append operation UUID")
            .get_timestamp()
            .expect("validated UUIDv7 carries a timestamp")
            .to_unix()
            .0
    }
}

impl fmt::Display for AppendOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A versioned SHA-256 digest over one append's Arrow metadata messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendPayloadDigest(String);

impl AppendPayloadDigest {
    /// Parse a lowercase 64-character SHA-256 digest.
    #[must_use]
    pub fn parse(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        (value.len() == SHA256_HEX_LEN
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
        .then_some(Self(value))
    }

    /// Return the lowercase hexadecimal digest.
    #[must_use]
    pub fn as_str(&self) -> &str { &self.0 }
}

/// Authenticated, engine-neutral identity attached to one append commit.
#[derive(bon::Builder, Clone, Debug, Eq, PartialEq)]
pub struct AppendOperation {
    tenant:         TenantId,
    operation_id:   AppendOperationId,
    payload_digest: AppendPayloadDigest,
}

impl AppendOperation {
    /// Return the authenticated tenant scope.
    #[must_use]
    pub fn tenant(&self) -> &TenantId { &self.tenant }

    /// Return the logical operation identity.
    #[must_use]
    pub fn operation_id(&self) -> &AppendOperationId { &self.operation_id }

    /// Return the verified digest of the Flight metadata payload.
    #[must_use]
    pub fn payload_digest(&self) -> &AppendPayloadDigest { &self.payload_digest }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileAppendWire {
    version:        u8,
    namespace:      String,
    table:          String,
    operation_id:   String,
    payload_sha256: String,
}

#[derive(bon::Builder, Clone, Debug, PartialEq, Eq)]
/// The metadata-only Flight command for appending already-uploaded SQL
/// `FILE` rows.
///
/// The command identifies the target table and logical operation. Credentials
/// and object payload bytes remain outside the control-plane contract; object
/// locations are carried in the Arrow rows that follow it.
pub struct FileAppendRequest {
    table:          TableRef,
    operation_id:   AppendOperationId,
    payload_digest: AppendPayloadDigest,
}

impl FileAppendRequest {
    /// Create a validated request targeting `table`.
    #[must_use]
    pub fn new(
        table: TableRef,
        operation_id: AppendOperationId,
        payload_digest: AppendPayloadDigest,
    ) -> Self {
        Self {
            table,
            operation_id,
            payload_digest,
        }
    }

    /// Encode the transport-neutral command payload carried by Flight `Any`.
    #[must_use]
    pub fn command_payload(&self) -> Vec<u8> {
        serde_json::to_vec(&FileAppendWire {
            version:        FILE_APPEND_WIRE_VERSION,
            namespace:      self.table.namespace.0.clone(),
            table:          self.table.name.0.clone(),
            operation_id:   self.operation_id.0.clone(),
            payload_sha256: self.payload_digest.0.clone(),
        })
        .expect("FILE append wire values are JSON serializable")
    }

    /// Decode a command payload without depending on Protobuf or Flight.
    #[must_use]
    pub fn from_command_payload(payload: &[u8]) -> Option<Self> {
        let wire: FileAppendWire = serde_json::from_slice(payload).ok()?;
        if wire.version != FILE_APPEND_WIRE_VERSION
            || wire.namespace.is_empty()
            || wire.table.is_empty()
        {
            return None;
        }
        Some(Self::new(
            TableRef::new(wire.namespace, wire.table),
            AppendOperationId::parse(wire.operation_id)?,
            AppendPayloadDigest::parse(wire.payload_sha256)?,
        ))
    }

    /// Return the target table.
    #[must_use]
    pub fn table(&self) -> &TableRef { &self.table }

    /// Return the idempotency identity for this logical append.
    #[must_use]
    pub fn operation_id(&self) -> &AppendOperationId { &self.operation_id }

    /// Return the declared payload digest.
    #[must_use]
    pub fn payload_digest(&self) -> &AppendPayloadDigest { &self.payload_digest }
}

#[cfg(test)]
mod tests {
    use crate::{AppendOperationId, AppendPayloadDigest, FileAppendRequest, TableRef};

    #[test]
    fn file_append_request_roundtrip() {
        let request = FileAppendRequest::new(
            TableRef::new("robots", "episodes"),
            AppendOperationId::parse("0197f0f4-7b2a-7000-8000-000000000001").unwrap(),
            AppendPayloadDigest::parse(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .unwrap(),
        );

        let payload = request.command_payload();

        assert_eq!(
            FileAppendRequest::from_command_payload(&payload),
            Some(request)
        );
        assert!(
            FileAppendRequest::from_command_payload(b"robots\0episodes").is_none(),
            "a descriptor without operation identity and payload digest must be rejected"
        );
        assert!(FileAppendRequest::from_command_payload(
            br#"{"version":1,"namespace":"robots","table":"episodes","operation_id":"not-a-uuid","payload_sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}"#,
        )
        .is_none());
        assert!(FileAppendRequest::from_command_payload(
            br#"{"version":1,"namespace":"robots","table":"episodes","operation_id":"0197f0f4-7b2a-7000-8000-000000000001","payload_sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","tenant":"forged"}"#,
        )
        .is_none());
        assert!(FileAppendRequest::from_command_payload(b"not-a-file-command").is_none());
    }

    #[test]
    fn append_operation_id_is_canonicalized() {
        let operation = AppendOperationId::parse("0197F0F4-7B2A-7000-8000-0000000000AB").unwrap();

        assert_eq!(operation.as_str(), "0197f0f4-7b2a-7000-8000-0000000000ab");
    }
}
