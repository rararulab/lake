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

//! Compact durable coordination records for idempotent FILE appends.

use lake_common::{
    AppendOperation, AppendOperationId, AppendPayloadDigest, TableRef, TenantId, Version,
};
use serde::{Deserialize, Serialize};

use crate::{MetasrvError, Result};

pub(crate) const OPERATION_PREFIX: &str = "append-operation/v1/";
const ACTIVE_PREFIX: &str = "append-active/v1/";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AppendState {
    Reserved,
    EngineCommitted,
    Committed,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub(crate) struct AppendRecord {
    pub(crate) format_version: u8,
    pub(crate) tenant:         String,
    pub(crate) namespace:      String,
    pub(crate) table:          String,
    pub(crate) operation_id:   String,
    pub(crate) payload_sha256: String,
    pub(crate) base_version:   Version,
    pub(crate) result_version: Option<Version>,
    pub(crate) state:          AppendState,
    pub(crate) created_at:     u64,
    pub(crate) updated_at:     u64,
}

impl AppendRecord {
    pub(crate) fn reserved(
        operation: &AppendOperation,
        table: &TableRef,
        base_version: Version,
        now: u64,
    ) -> Self {
        Self {
            format_version: 1,
            tenant: operation.tenant().as_str().to_owned(),
            namespace: table.namespace.0.clone(),
            table: table.name.0.clone(),
            operation_id: operation.operation_id().as_str().to_owned(),
            payload_sha256: operation.payload_digest().as_str().to_owned(),
            base_version,
            result_version: None,
            state: AppendState::Reserved,
            created_at: now,
            updated_at: now,
        }
    }

    pub(crate) fn validate(&self, operation: &AppendOperation, table: &TableRef) -> Result<()> {
        if self.format_version != 1
            || self.tenant != operation.tenant().as_str()
            || self.namespace != table.namespace.0
            || self.table != table.name.0
            || self.operation_id != operation.operation_id().as_str()
        {
            return Err(MetasrvError::CorruptOperationState {
                operation_id: operation.operation_id().to_string(),
            });
        }
        if self.payload_sha256 != operation.payload_digest().as_str() {
            return Err(MetasrvError::OperationConflict {
                operation_id: operation.operation_id().to_string(),
            });
        }
        Ok(())
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|_| MetasrvError::CorruptOperationState {
            operation_id: self.operation_id.clone(),
        })
    }

    pub(crate) fn decode(operation_id: &str, bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|_| MetasrvError::CorruptOperationState {
            operation_id: operation_id.to_owned(),
        })
    }

    pub(crate) fn identity(&self) -> Result<(TableRef, AppendOperation)> {
        let operation_id =
            AppendOperationId::parse(self.operation_id.clone()).ok_or_else(|| {
                MetasrvError::CorruptOperationState {
                    operation_id: self.operation_id.clone(),
                }
            })?;
        let payload_digest =
            AppendPayloadDigest::parse(self.payload_sha256.clone()).ok_or_else(|| {
                MetasrvError::CorruptOperationState {
                    operation_id: self.operation_id.clone(),
                }
            })?;
        let tenant = TenantId::try_new(self.tenant.clone()).map_err(|_| {
            MetasrvError::CorruptOperationState {
                operation_id: self.operation_id.clone(),
            }
        })?;
        Ok((
            TableRef::new(self.namespace.clone(), self.table.clone()),
            AppendOperation::builder()
                .tenant(tenant)
                .operation_id(operation_id)
                .payload_digest(payload_digest)
                .build(),
        ))
    }
}

pub(crate) fn operation_key(operation: &AppendOperation, table: &TableRef) -> String {
    format!(
        "{OPERATION_PREFIX}{}/{:04x}:{}/{:04x}:{}/{}",
        operation.tenant().as_str(),
        table.namespace.0.len(),
        table.namespace.0,
        table.name.0.len(),
        table.name.0,
        operation.operation_id().as_str()
    )
}

pub(crate) fn active_key(operation: &AppendOperation, table: &TableRef) -> String {
    format!(
        "{ACTIVE_PREFIX}{}/{:04x}:{}/{:04x}:{}",
        operation.tenant().as_str(),
        table.namespace.0.len(),
        table.namespace.0,
        table.name.0.len(),
        table.name.0
    )
}
