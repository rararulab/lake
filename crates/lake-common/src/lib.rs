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

//! Shared identifiers used across every lake tier.
//!
//! These are deliberately thin newtypes over `String` / `u64`: they cost
//! nothing at runtime but stop a `TableName` from being passed where a
//! `Namespace` is expected. Nothing here does I/O or pulls in a tier's
//! dependencies, so every crate can depend on `lake-common` freely.

mod data_location;
mod file_write;
mod ids;
mod location;
mod managed_stage;
mod object_reference;
mod principal;
mod robotics;

pub use data_location::DataLocation;
pub use file_write::{
    AppendOperation, AppendOperationId, AppendPayloadDigest, FILE_APPEND_TYPE_URL,
    FileAppendRequest,
};
pub use ids::{Namespace, TableName, TableRef, Version};
pub use location::TableLocation;
pub use managed_stage::{
    MANAGED_STAGE_DISCOVERY_ACTION, MANAGED_STAGE_PROTOCOL_VERSION, ManagedStageBackend,
    ManagedStageDescriptor, ManagedStageError,
};
pub use object_reference::{ObjectIdentity, ObjectReferenceDelta, ObjectReferenceError};
pub use principal::{Principal, PrincipalError, PrincipalId, PrincipalRole, TenantId};
pub use robotics::{
    ARTIFACT_REF_RECORD_KIND, ArtifactRefV1, EPISODE_RECORD_KIND, EPISODE_TABLE_CONTRACT_VERSION,
    EpisodeBundleV1, EpisodeContractError, EpisodeRecordV1, MANIFEST_ARTIFACT_ROLE,
};

#[cfg(test)]
mod managed_stage_contract_tests {
    use crate::{ManagedStageBackend, ManagedStageDescriptor, ManagedStageError};

    #[test]
    fn managed_stage_descriptors_roundtrip_without_credentials() {
        let descriptors = [
            ManagedStageDescriptor::local("/var/lib/lake/managed-objects"),
            ManagedStageDescriptor::s3(
                "embodied-data",
                "managed-objects",
                Some("us-east-1".to_owned()),
                Some("http://127.0.0.1:4566".to_owned()),
                true,
            ),
        ];
        assert!(matches!(
            descriptors[0].backend(),
            ManagedStageBackend::Local { .. }
        ));

        for expected in descriptors {
            let wire = expected.to_wire().expect("encode descriptor");
            let json = std::str::from_utf8(&wire).expect("JSON wire is UTF-8");
            for forbidden in [
                "access_key",
                "secret_key",
                "session_token",
                "signed_url",
                "object_bytes",
            ] {
                assert!(!json.contains(forbidden), "wire contains {forbidden}");
            }

            let decoded = ManagedStageDescriptor::from_wire(&wire).expect("decode descriptor");
            assert_eq!(decoded, expected);
        }
    }

    #[test]
    fn managed_stage_rejects_unsupported_protocol_version() {
        let future = br#"{"version":2,"backend":{"type":"local","root":"/tmp/objects"}}"#;

        assert!(matches!(
            ManagedStageDescriptor::from_wire(future),
            Err(ManagedStageError::UnsupportedVersion {
                version:   2,
                supported: 1,
            })
        ));
    }
}
