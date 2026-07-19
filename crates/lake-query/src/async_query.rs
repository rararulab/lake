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

//! Durable, CAS-friendly asynchronous query lifecycle.

use std::{
    io,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow_flight::{IpcMessage, SchemaAsIpc};
use bytes::Bytes;
use datafusion::arrow::{ipc::writer::IpcWriteOptions, record_batch::RecordBatch};
use futures::{StreamExt, stream};
use lake_catalog::TableSnapshot;
use lake_common::{
    DataLocation, Principal, PrincipalId, PrincipalRole, TableLocation, TableRef, TenantId, Version,
};
use lake_meta::MetaStoreRef;
use lake_objects::{ManagedObjectScope, ManagedObjectStore, open_verified};
use ring::digest;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader as AsyncStreamReader;

use crate::{
    QueryEngine, QueryError,
    async_ipc::{IpcPipelineLimits, PipelineProbe, encoded_batch_reader},
    ticket::{
        QueryTicketError, QueryTicketKeyRing, StatementTableSnapshot, StatementTicket,
        StatementTicketCodec,
    },
};

const MAX_QUERY_ID_BYTES: usize = 64;
const MAX_TENANT_BYTES: usize = 64;
const MAX_PRINCIPAL_BYTES: usize = 128;
const MAX_URI_BYTES: usize = 4_096;
const MAX_JOB_SPEC_BYTES: u64 = 2 * 1024 * 1024;
const MAX_FAILURE_CODE_BYTES: usize = 64;
const MAX_RESERVATION_TOKEN_BYTES: usize = 64;
const MAX_JOB_LIFETIME_SECS: u64 = 24 * 60 * 60;
const MAX_WORKER_LEASE_SECS: u64 = 5 * 60;
const MAX_RESULT_PARTS: u64 = 4_096;
const MAX_RESULT_BYTES: u64 = 1 << 40;
pub(crate) const MIN_CONFIG_RESULT_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MAX_CONFIG_RESULT_BYTES: u64 = 256 * 1024 * 1024 * 1024;
pub(crate) const DEFAULT_RESULT_BYTES: u64 = 16 * 1024 * 1024 * 1024;
pub(crate) const DEFAULT_OUTSTANDING_PER_TENANT: usize = 8;
pub(crate) const MAX_OUTSTANDING_PER_TENANT: usize = 128;
pub(crate) const MAX_RESULT_PART_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RESULT_MANIFEST_BYTES: u64 = 32 * 1024 * 1024;
const MAX_RESULT_MANIFEST_PART_JSON_BYTES: u64 = 4_269;
const MAX_RESULT_MANIFEST_ENVELOPE_BYTES: u64 = 180;
// JSON-safe URIs bound the immutable JSON manifest below its separate ceiling.
const MAX_RESULT_MANIFEST_STRUCTURE_BYTES: u64 = MAX_RESULT_PARTS
    * MAX_RESULT_MANIFEST_PART_JSON_BYTES
    + (MAX_RESULT_PARTS - 1)
    + 2
    + (MAX_RESULT_SCHEMA_BYTES as u64 * 4)
    + 1
    + MAX_RESULT_MANIFEST_ENVELOPE_BYTES;
const _: () = assert!(MAX_RESULT_MANIFEST_STRUCTURE_BYTES == 21_684_406);
const _: () = assert!(MAX_RESULT_MANIFEST_STRUCTURE_BYTES < MAX_RESULT_MANIFEST_BYTES);
const _: () = assert!(MAX_RESULT_MANIFEST_BYTES < MAX_RESULT_PART_BYTES);
const MAX_RESULT_PART_ROWS: usize = 65_536;
const MAX_RESULT_SCHEMA_BYTES: usize = 1024 * 1024;
const MAX_STATE_RECORD_BYTES: usize = 16 * 1024;
const MAX_TENANT_INDEX_BYTES: usize = 32 * 1024;
const MAX_EXECUTION_INDEX_BYTES: usize = 32 * 1024;
const TENANT_RESERVATION_GRACE_SECS: u64 = 5 * 60;
const TENANT_CAS_ATTEMPTS: usize = 8;
const EXECUTION_CAS_ATTEMPTS: usize = 8;
const MAX_CLUSTER_EXECUTIONS: usize = 64;
const SUBMISSION_RESUME_ATTEMPTS: usize = 100;
const SUBMISSION_RESUME_RETRY: Duration = Duration::from_millis(10);
const SCAN_PAGE_JOBS: usize = 256;
const STATE_KEY_PREFIX: &str = "async-query/";
const TENANT_KEY_PREFIX: &str = "async-query-tenant/";
const EXECUTION_KEY: &str = "async-query-execution/v1";
const ASYNC_JOB_CONTENT_TYPE: &str = "application/vnd.lake.async-job";
const ASYNC_PART_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";
const ASYNC_MANIFEST_CONTENT_TYPE: &str = "application/vnd.lake.async-result-manifest+json";
const ASYNC_JOB_AUDIENCE: &str = "lake-query-async-job-v1";
const ASYNC_POLL_AUDIENCE: &str = "lake-query-async-poll-v1";
const ASYNC_POLL_MAGIC: &[u8; 4] = b"LQPH";
const ASYNC_RESULT_MAGIC: &[u8; 4] = b"LQRP";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct WorkerIdentity([u8; 16]);

impl WorkerIdentity {
    pub(crate) const fn new(value: [u8; 16]) -> Self { Self(value) }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct WorkerLease {
    worker: WorkerIdentity,
    epoch:  u64,
}

impl WorkerLease {
    pub(crate) const fn epoch(&self) -> u64 { self.epoch }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "status")]
enum AsyncQueryState {
    Queued,
    Running {
        lease:         WorkerLease,
        lease_expires: u64,
    },
    Completed {
        manifest: DataLocation,
        parts:    u64,
        rows:     u64,
        bytes:    u64,
    },
    Failed {
        code:      String,
        failed_at: u64,
    },
    Cancelled {
        cancelled_at: u64,
    },
    Expired,
    Cleaning {
        started_at: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AsyncResourceLimits {
    outstanding_per_tenant: usize,
    result_bytes:           u64,
}

impl AsyncResourceLimits {
    pub(crate) fn try_new(
        outstanding_per_tenant: usize,
        result_bytes: u64,
    ) -> Result<Self, AsyncQueryTransitionError> {
        if !(1..=MAX_OUTSTANDING_PER_TENANT).contains(&outstanding_per_tenant)
            || !(MIN_CONFIG_RESULT_BYTES..=MAX_CONFIG_RESULT_BYTES).contains(&result_bytes)
        {
            return Err(AsyncQueryTransitionError::InvalidRecord);
        }
        Ok(Self {
            outstanding_per_tenant,
            result_bytes,
        })
    }

    pub(crate) const fn outstanding_per_tenant(self) -> usize { self.outstanding_per_tenant }

    pub(crate) const fn result_bytes(self) -> u64 { self.result_bytes }
}

impl Default for AsyncResourceLimits {
    fn default() -> Self {
        Self {
            outstanding_per_tenant: DEFAULT_OUTSTANDING_PER_TENANT,
            result_bytes:           DEFAULT_RESULT_BYTES,
        }
    }
}

/// Immutable shared capacity limits for durable async executions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AsyncGlobalExecutionLimits {
    max_running:            usize,
    max_running_per_tenant: usize,
}

impl AsyncGlobalExecutionLimits {
    pub(crate) fn try_new(
        max_running: usize,
        max_running_per_tenant: usize,
    ) -> Result<Self, AsyncQueryTransitionError> {
        if !(1..=MAX_CLUSTER_EXECUTIONS).contains(&max_running)
            || !(1..=max_running).contains(&max_running_per_tenant)
        {
            return Err(AsyncQueryTransitionError::InvalidRecord);
        }
        Ok(Self {
            max_running,
            max_running_per_tenant,
        })
    }

    pub(crate) const fn max_running(self) -> usize { self.max_running }

    pub(crate) const fn max_running_per_tenant(self) -> usize { self.max_running_per_tenant }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AsyncRecordResources {
    result_limit_bytes:       u64,
    tenant_reservation_token: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TenantResourceIndex {
    schema_version: u8,
    entries:        Vec<TenantReservation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TenantReservation {
    query_id:   String,
    token:      String,
    expires_at: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ExecutionLeaseIndex {
    schema_version: u8,
    entries:        Vec<ExecutionLeaseEntry>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ExecutionLeaseEntry {
    query_id:      String,
    tenant_digest: String,
    token:         String,
    expires_at:    u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AsyncQueryRecord {
    schema_version:   u8,
    query_id:         String,
    tenant_id:        String,
    principal_id:     String,
    job_spec:         DataLocation,
    created_at:       u64,
    expires_at:       u64,
    next_lease_epoch: u64,
    state:            AsyncQueryState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resources:        Option<AsyncRecordResources>,
}

#[derive(Debug, Eq, PartialEq, Snafu)]
pub(crate) enum AsyncQueryTransitionError {
    #[snafu(display("async query record is invalid"))]
    InvalidRecord,
    #[snafu(display("async query worker lease is still held"))]
    LeaseHeld,
    #[snafu(display("async query mutation came from a stale worker"))]
    StaleWorker,
    #[snafu(display("async query is terminal"))]
    Terminal,
    #[snafu(display("async query counter overflow"))]
    Overflow,
}

#[derive(Debug, Snafu)]
pub(crate) enum AsyncQueryStoreError {
    #[snafu(display("async query state backend failed"))]
    Meta { source: lake_meta::MetaError },
    #[snafu(display("async query state encoding failed"))]
    Encode { source: serde_json::Error },
    #[snafu(display("async query state decoding failed"))]
    Decode { source: serde_json::Error },
    #[snafu(display("async query state record is invalid"))]
    InvalidStateRecord,
    #[snafu(display("async query state record exceeds its byte bound"))]
    RecordTooLarge,
    #[snafu(display("async query already exists"))]
    AlreadyExists,
    #[snafu(display("async tenant outstanding query quota is exhausted"))]
    QuotaExceeded,
    #[snafu(display("async cluster execution capacity is temporarily unavailable"))]
    ExecutionCapacityHeld,
    #[snafu(display("async cluster execution lease is held"))]
    ExecutionLeaseHeld,
    #[snafu(display("async cluster execution lease is no longer owned"))]
    ExecutionLeaseLost,
    #[snafu(display("an idempotent async submission is still being created"))]
    ReservationHeld,
    #[snafu(display("async query state changed concurrently"))]
    Conflict,
    #[snafu(display("async query transition failed"))]
    Transition { source: AsyncQueryTransitionError },
}

#[derive(Clone)]
pub(crate) struct AsyncQueryStore {
    meta: MetaStoreRef,
}

pub(crate) struct AsyncQuerySubmission {
    query_id:    String,
    poll_handle: Vec<u8>,
    expires_at:  u64,
}

impl AsyncQuerySubmission {
    pub(crate) fn query_id(&self) -> &str { &self.query_id }

    pub(crate) fn poll_handle(&self) -> &[u8] { &self.poll_handle }

    pub(crate) const fn expires_at(&self) -> u64 { self.expires_at }
}

#[derive(Debug, Snafu)]
pub(crate) enum AsyncQueryCoordinatorError {
    #[snafu(display("async query state operation failed"))]
    Store { source: AsyncQueryStoreError },
    #[snafu(display("async query control-object operation failed"))]
    Object { source: lake_objects::ObjectError },
    #[snafu(display("async query capability operation failed"))]
    Ticket { source: QueryTicketError },
    #[snafu(display("async query lifetime is invalid"))]
    InvalidLifetime,
    #[snafu(display("async query job specification is invalid"))]
    InvalidJobSpec,
    #[snafu(display("system time cannot represent async query lifetime"))]
    InvalidTime,
    #[snafu(display("async submission id is already bound to another statement"))]
    SubmissionConflict,
    #[snafu(display("async submission is still being created"))]
    SubmissionPending,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AsyncResultManifest {
    schema_version: u8,
    query_id:       String,
    schema_ipc:     Vec<u8>,
    parts:          Vec<DataLocation>,
    rows:           u64,
    bytes:          u64,
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub(crate) enum AsyncQueryWorkerError {
    #[snafu(display("async query state operation failed"))]
    Store { source: AsyncQueryStoreError },
    #[snafu(display("async query control-object operation failed"))]
    Coordinator { source: AsyncQueryCoordinatorError },
    #[snafu(display("async query planning or execution failed"))]
    Query { source: QueryError },
    #[snafu(display("async query result encoding failed"))]
    Arrow {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("async query result manifest encoding failed"))]
    Manifest { source: serde_json::Error },
    #[snafu(display("async query result exceeded configured bounds"))]
    ResultBound,
    #[snafu(display("async query execution deadline exceeded"))]
    ExecutionDeadline,
    #[snafu(display("async cluster execution capacity is temporarily unavailable"))]
    ExecutionCapacityHeld,
    #[snafu(display("async query state is unavailable"))]
    Missing,
    #[snafu(display("system time is invalid"))]
    InvalidTime,
}

#[derive(Clone)]
pub(crate) struct AsyncQueryCoordinator {
    store:      AsyncQueryStore,
    objects:    Arc<dyn ManagedObjectStore>,
    poll_codec: StatementTicketCodec,
    job_codec:  StatementTicketCodec,
    lifetime:   Duration,
    poll_ttl:   Duration,
    resources:  AsyncResourceLimits,
}

impl AsyncQueryCoordinator {
    pub(crate) fn try_new(
        state: MetaStoreRef,
        objects: Arc<dyn ManagedObjectStore>,
        keys: QueryTicketKeyRing,
        job_lifetime: Duration,
        poll_ttl: Duration,
    ) -> Result<Self, AsyncQueryCoordinatorError> {
        Self::try_new_with_resources(
            state,
            objects,
            keys,
            job_lifetime,
            poll_ttl,
            AsyncResourceLimits::default(),
        )
    }

    pub(crate) fn try_new_with_resources(
        state: MetaStoreRef,
        objects: Arc<dyn ManagedObjectStore>,
        keys: QueryTicketKeyRing,
        job_lifetime: Duration,
        poll_ttl: Duration,
        resources: AsyncResourceLimits,
    ) -> Result<Self, AsyncQueryCoordinatorError> {
        if job_lifetime.is_zero() || job_lifetime.as_secs() > MAX_JOB_LIFETIME_SECS {
            return Err(AsyncQueryCoordinatorError::InvalidLifetime);
        }
        let poll_codec = StatementTicketCodec::try_new(keys.clone(), poll_ttl, ASYNC_POLL_AUDIENCE)
            .map_err(|source| AsyncQueryCoordinatorError::Ticket { source })?;
        let job_codec =
            StatementTicketCodec::try_new_durable_job(keys, job_lifetime, ASYNC_JOB_AUDIENCE)
                .map_err(|source| AsyncQueryCoordinatorError::Ticket { source })?;
        Ok(Self {
            store: AsyncQueryStore::new(state),
            objects,
            poll_codec,
            job_codec,
            lifetime: job_lifetime,
            poll_ttl,
            resources,
        })
    }

    pub(crate) async fn submit_statement(
        &self,
        statement: &StatementTicket,
        principal: &Principal,
    ) -> Result<AsyncQuerySubmission, AsyncQueryCoordinatorError> {
        let encrypted_job = self
            .job_codec
            .seal_statement(statement, principal)
            .map_err(|source| AsyncQueryCoordinatorError::Ticket { source })?;
        self.submit(encrypted_job, principal).await
    }

    pub(crate) async fn submit_statement_with_id(
        &self,
        statement: &StatementTicket,
        principal: &Principal,
        submission_id: [u8; 16],
    ) -> Result<AsyncQuerySubmission, AsyncQueryCoordinatorError> {
        let query_id = submission_query_id(principal, submission_id);
        if let Some(submission) = self
            .existing_submission(&query_id, statement, principal)
            .await?
        {
            return Ok(submission);
        }
        let encrypted_job = self
            .job_codec
            .seal_statement(statement, principal)
            .map_err(|source| AsyncQueryCoordinatorError::Ticket { source })?;
        for _ in 0..SUBMISSION_RESUME_ATTEMPTS {
            match self
                .submit_with_query_id_at(
                    &query_id,
                    encrypted_job.clone(),
                    principal,
                    SystemTime::now(),
                )
                .await
            {
                Ok(submission) => return Ok(submission),
                Err(AsyncQueryCoordinatorError::Store {
                    source:
                        AsyncQueryStoreError::AlreadyExists | AsyncQueryStoreError::ReservationHeld,
                }) => {
                    if let Some(submission) = self
                        .resume_submission_with_id(&statement.sql, principal, submission_id)
                        .await?
                    {
                        return Ok(submission);
                    }
                    tokio::time::sleep(SUBMISSION_RESUME_RETRY).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(AsyncQueryCoordinatorError::SubmissionPending)
    }

    pub(crate) async fn resume_submission_with_id(
        &self,
        sql: &str,
        principal: &Principal,
        submission_id: [u8; 16],
    ) -> Result<Option<AsyncQuerySubmission>, AsyncQueryCoordinatorError> {
        let query_id = submission_query_id(principal, submission_id);
        let Some(record) = self
            .store
            .load(&query_id)
            .await
            .map_err(|source| AsyncQueryCoordinatorError::Store { source })?
        else {
            return Ok(None);
        };
        let statement = self.open_job(&record).await?;
        if !record.belongs_to(principal) || statement.sql != sql {
            return Err(AsyncQueryCoordinatorError::SubmissionConflict);
        }
        Ok(Some(AsyncQuerySubmission {
            query_id:    query_id.clone(),
            poll_handle: self.seal_poll_handle(&query_id, principal)?,
            expires_at:  record.expires_at(),
        }))
    }

    async fn existing_submission(
        &self,
        query_id: &str,
        statement: &StatementTicket,
        principal: &Principal,
    ) -> Result<Option<AsyncQuerySubmission>, AsyncQueryCoordinatorError> {
        let Some(record) = self
            .store
            .load(query_id)
            .await
            .map_err(|source| AsyncQueryCoordinatorError::Store { source })?
        else {
            return Ok(None);
        };
        if !record.belongs_to(principal) || self.open_job(&record).await? != *statement {
            return Err(AsyncQueryCoordinatorError::SubmissionConflict);
        }
        Ok(Some(AsyncQuerySubmission {
            query_id:    query_id.to_owned(),
            poll_handle: self.seal_poll_handle(query_id, principal)?,
            expires_at:  record.expires_at(),
        }))
    }

    pub(crate) async fn submit(
        &self,
        encrypted_job: Vec<u8>,
        principal: &Principal,
    ) -> Result<AsyncQuerySubmission, AsyncQueryCoordinatorError> {
        self.submit_at(encrypted_job, principal, SystemTime::now())
            .await
    }

    async fn submit_at(
        &self,
        encrypted_job: Vec<u8>,
        principal: &Principal,
        now: SystemTime,
    ) -> Result<AsyncQuerySubmission, AsyncQueryCoordinatorError> {
        let query_id = uuid::Uuid::now_v7().to_string();
        self.submit_with_query_id_at(&query_id, encrypted_job, principal, now)
            .await
    }

    async fn submit_with_query_id_at(
        &self,
        query_id: &str,
        encrypted_job: Vec<u8>,
        principal: &Principal,
        now: SystemTime,
    ) -> Result<AsyncQuerySubmission, AsyncQueryCoordinatorError> {
        if state_key(query_id).is_none()
            || encrypted_job.is_empty()
            || encrypted_job.len() as u64 > MAX_JOB_SPEC_BYTES
        {
            return Err(AsyncQueryCoordinatorError::InvalidJobSpec);
        }
        let now = now
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AsyncQueryCoordinatorError::InvalidTime)?
            .as_secs();
        let expires_at = now
            .checked_add(self.lifetime.as_secs())
            .ok_or(AsyncQueryCoordinatorError::InvalidTime)?;
        let reservation_token = uuid::Uuid::now_v7().to_string();
        self.store
            .reserve_tenant(
                principal.tenant().as_str(),
                query_id,
                &reservation_token,
                now,
                self.resources.outstanding_per_tenant(),
            )
            .await
            .map_err(|source| AsyncQueryCoordinatorError::Store { source })?;
        let scope = ManagedObjectScope::try_new(principal.tenant().as_str(), query_id)
            .map_err(|source| AsyncQueryCoordinatorError::Object { source })?;
        let input = AsyncStreamReader::new(stream::iter([Ok::<Bytes, io::Error>(Bytes::from(
            encrypted_job,
        ))]));
        let job_spec = self
            .objects
            .put_scoped_reader(
                &scope,
                "job",
                Box::pin(input),
                ASYNC_JOB_CONTENT_TYPE.to_owned(),
            )
            .await
            .map_err(|source| AsyncQueryCoordinatorError::Object { source })?;
        let record = AsyncQueryRecord::try_new_with_resources(
            query_id,
            principal.tenant().as_str(),
            principal.subject(),
            job_spec,
            now,
            expires_at,
            &reservation_token,
            self.resources,
        )
        .map_err(|source| AsyncQueryCoordinatorError::Store {
            source: AsyncQueryStoreError::Transition { source },
        })?;
        match self.store.create(record).await {
            Ok(()) => {}
            Err(AsyncQueryStoreError::AlreadyExists) => {
                // A schema-v1 replica can win a deterministic create after this
                // v2 coordinator reserved capacity. Release only our exact
                // token before the caller resumes that legacy record.
                self.store
                    .release_tenant(principal.tenant().as_str(), query_id, &reservation_token)
                    .await
                    .map_err(|source| AsyncQueryCoordinatorError::Store { source })?;
                return Err(AsyncQueryCoordinatorError::Store {
                    source: AsyncQueryStoreError::AlreadyExists,
                });
            }
            Err(source) => return Err(AsyncQueryCoordinatorError::Store { source }),
        }
        self.store
            .confirm_tenant(
                principal.tenant().as_str(),
                query_id,
                &reservation_token,
                expires_at,
            )
            .await
            .map_err(|source| AsyncQueryCoordinatorError::Store { source })?;
        let poll_handle = self.seal_poll_handle(query_id, principal)?;
        Ok(AsyncQuerySubmission {
            query_id: query_id.to_owned(),
            poll_handle,
            expires_at,
        })
    }

    pub(crate) fn open_poll_handle(
        &self,
        handle: &[u8],
        principal: &Principal,
    ) -> Result<String, AsyncQueryCoordinatorError> {
        if !handle.starts_with(ASYNC_POLL_MAGIC) {
            return Err(AsyncQueryCoordinatorError::Ticket {
                source: QueryTicketError::Invalid,
            });
        }
        let statement = self
            .poll_codec
            .open_statement(&handle[ASYNC_POLL_MAGIC.len()..], principal)
            .map_err(|source| AsyncQueryCoordinatorError::Ticket { source })?;
        if !statement.snapshots.is_empty() || state_key(&statement.sql).is_none() {
            return Err(AsyncQueryCoordinatorError::Ticket {
                source: QueryTicketError::Invalid,
            });
        }
        Ok(statement.sql)
    }

    pub(crate) fn refresh_poll_handle(
        &self,
        query_id: &str,
        principal: &Principal,
    ) -> Result<Vec<u8>, AsyncQueryCoordinatorError> {
        if state_key(query_id).is_none() {
            return Err(AsyncQueryCoordinatorError::Ticket {
                source: QueryTicketError::Invalid,
            });
        }
        self.seal_poll_handle(query_id, principal)
    }

    pub(crate) fn is_poll_handle(handle: &[u8]) -> bool { handle.starts_with(ASYNC_POLL_MAGIC) }

    pub(crate) fn is_result_handle(handle: &[u8]) -> bool { handle.starts_with(ASYNC_RESULT_MAGIC) }

    pub(crate) fn seal_result_handle(
        &self,
        query_id: &str,
        part: usize,
        principal: &Principal,
    ) -> Result<Vec<u8>, AsyncQueryCoordinatorError> {
        if state_key(query_id).is_none() || part >= MAX_RESULT_PARTS as usize {
            return Err(AsyncQueryCoordinatorError::InvalidJobSpec);
        }
        let mut handle = Vec::from(ASYNC_RESULT_MAGIC);
        let sealed = self
            .poll_codec
            .seal_statement(
                &StatementTicket {
                    sql:               format!("{query_id}:{part}"),
                    snapshots:         Vec::new(),
                    iceberg_snapshots: Vec::new(),
                },
                principal,
            )
            .map_err(|source| AsyncQueryCoordinatorError::Ticket { source })?;
        handle.extend_from_slice(&sealed);
        Ok(handle)
    }

    pub(crate) fn open_result_handle(
        &self,
        handle: &[u8],
        principal: &Principal,
    ) -> Result<(String, usize), AsyncQueryCoordinatorError> {
        if !Self::is_result_handle(handle) {
            return Err(AsyncQueryCoordinatorError::InvalidJobSpec);
        }
        let statement = self
            .poll_codec
            .open_statement(&handle[ASYNC_RESULT_MAGIC.len()..], principal)
            .map_err(|source| AsyncQueryCoordinatorError::Ticket { source })?;
        let (query_id, part) = statement
            .sql
            .rsplit_once(':')
            .ok_or(AsyncQueryCoordinatorError::InvalidJobSpec)?;
        let part = part
            .parse::<usize>()
            .map_err(|_| AsyncQueryCoordinatorError::InvalidJobSpec)?;
        if !statement.snapshots.is_empty()
            || state_key(query_id).is_none()
            || part >= MAX_RESULT_PARTS as usize
        {
            return Err(AsyncQueryCoordinatorError::InvalidJobSpec);
        }
        Ok((query_id.to_owned(), part))
    }

    fn seal_poll_handle(
        &self,
        query_id: &str,
        principal: &Principal,
    ) -> Result<Vec<u8>, AsyncQueryCoordinatorError> {
        let mut poll_handle = Vec::from(ASYNC_POLL_MAGIC);
        let sealed = self
            .poll_codec
            .seal_statement(
                &StatementTicket {
                    sql:               query_id.to_owned(),
                    snapshots:         Vec::new(),
                    iceberg_snapshots: Vec::new(),
                },
                principal,
            )
            .map_err(|source| AsyncQueryCoordinatorError::Ticket { source })?;
        poll_handle.extend_from_slice(&sealed);
        Ok(poll_handle)
    }

    pub(crate) fn store(&self) -> &AsyncQueryStore { &self.store }

    pub(crate) fn capability_expires_at(&self) -> Result<u64, AsyncQueryCoordinatorError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AsyncQueryCoordinatorError::InvalidTime)?
            .as_secs();
        now.checked_add(self.poll_ttl.as_secs())
            .ok_or(AsyncQueryCoordinatorError::InvalidTime)
    }

    pub(crate) async fn cancel(&self, query_id: &str) -> Result<(), AsyncQueryCoordinatorError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AsyncQueryCoordinatorError::InvalidTime)?
            .as_secs();
        self.store
            .cancel(query_id, now)
            .await
            .map_err(|source| AsyncQueryCoordinatorError::Store { source })
    }

    pub(crate) async fn cleanup_if_expired(
        &self,
        query_id: &str,
        now: u64,
    ) -> Result<bool, AsyncQueryCoordinatorError> {
        let Some(record) = self
            .store
            .load(query_id)
            .await
            .map_err(|source| AsyncQueryCoordinatorError::Store { source })?
        else {
            return Ok(false);
        };
        if now < record.expires_at() {
            return Ok(false);
        }
        let Some(started_at) = record.cleaning_started_at() else {
            self.store
                .begin_cleanup(query_id, now)
                .await
                .map_err(|source| AsyncQueryCoordinatorError::Store { source })?;
            return Ok(false);
        };
        if now < started_at.saturating_add(MAX_WORKER_LEASE_SECS) {
            return Ok(false);
        }
        self.objects
            .delete_scope(&record.scope()?)
            .await
            .map_err(|source| AsyncQueryCoordinatorError::Object { source })?;
        self.store
            .delete_cleaning(query_id)
            .await
            .map_err(|source| AsyncQueryCoordinatorError::Store { source })?;
        if let Some(reservation_token) = record.tenant_reservation_token() {
            self.store
                .release_tenant(record.tenant_id(), query_id, reservation_token)
                .await
                .map_err(|source| AsyncQueryCoordinatorError::Store { source })?;
        }
        Ok(true)
    }

    pub(crate) async fn open_job(
        &self,
        record: &AsyncQueryRecord,
    ) -> Result<StatementTicket, AsyncQueryCoordinatorError> {
        let reader = open_verified(self.objects.as_ref(), record.job_spec())
            .await
            .map_err(|source| AsyncQueryCoordinatorError::Object { source })?;
        let capacity = usize::try_from(record.job_spec().size_bytes)
            .map_err(|_| AsyncQueryCoordinatorError::InvalidJobSpec)?;
        if capacity == 0 || capacity as u64 > MAX_JOB_SPEC_BYTES {
            return Err(AsyncQueryCoordinatorError::InvalidJobSpec);
        }
        let mut encrypted_job = Vec::with_capacity(capacity);
        reader
            .take(MAX_JOB_SPEC_BYTES + 1)
            .read_to_end(&mut encrypted_job)
            .await
            .map_err(|_| AsyncQueryCoordinatorError::InvalidJobSpec)?;
        if encrypted_job.len() != capacity {
            return Err(AsyncQueryCoordinatorError::InvalidJobSpec);
        }
        let principal = record.principal()?;
        self.job_codec
            .open_statement(&encrypted_job, &principal)
            .map_err(|source| AsyncQueryCoordinatorError::Ticket { source })
    }

    pub(crate) async fn load_manifest(
        &self,
        record: &AsyncQueryRecord,
    ) -> Result<AsyncResultManifest, AsyncQueryCoordinatorError> {
        let location = record
            .completed_manifest()
            .ok_or(AsyncQueryCoordinatorError::InvalidJobSpec)?;
        if !valid_manifest_location(location) {
            return Err(AsyncQueryCoordinatorError::InvalidJobSpec);
        }
        let capacity = usize::try_from(location.size_bytes)
            .map_err(|_| AsyncQueryCoordinatorError::InvalidJobSpec)?;
        let reader = open_verified(self.objects.as_ref(), location)
            .await
            .map_err(|source| AsyncQueryCoordinatorError::Object { source })?;
        let mut encoded = Vec::with_capacity(capacity);
        reader
            .take(MAX_RESULT_MANIFEST_BYTES + 1)
            .read_to_end(&mut encoded)
            .await
            .map_err(|_| AsyncQueryCoordinatorError::InvalidJobSpec)?;
        if encoded.len() != capacity {
            return Err(AsyncQueryCoordinatorError::InvalidJobSpec);
        }
        let manifest: AsyncResultManifest = serde_json::from_slice(&encoded)
            .map_err(|_| AsyncQueryCoordinatorError::InvalidJobSpec)?;
        let (parts, rows, bytes) = record
            .completed_summary()
            .ok_or(AsyncQueryCoordinatorError::InvalidJobSpec)?;
        manifest.validate(
            &record.query_id,
            parts,
            rows,
            bytes,
            record.result_limit_bytes(),
        )?;
        Ok(manifest)
    }

    pub(crate) async fn open_result_part(
        &self,
        manifest: &AsyncResultManifest,
        part: usize,
    ) -> Result<lake_objects::ObjectReader, AsyncQueryCoordinatorError> {
        let location = manifest
            .parts
            .get(part)
            .ok_or(AsyncQueryCoordinatorError::InvalidJobSpec)?;
        open_verified(self.objects.as_ref(), location)
            .await
            .map_err(|source| AsyncQueryCoordinatorError::Object { source })
    }
}

fn submission_query_id(principal: &Principal, submission_id: [u8; 16]) -> String {
    let mut context = digest::Context::new(&digest::SHA256);
    context.update(b"lake-query-async-submission-v1\0");
    context.update(principal.tenant().as_str().as_bytes());
    context.update(b"\0");
    context.update(principal.subject().as_bytes());
    context.update(b"\0");
    context.update(&submission_id);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    context.finish().as_ref().iter().fold(
        String::with_capacity(digest::SHA256_OUTPUT_LEN * 2),
        |mut output, byte| {
            output.push(char::from(HEX[usize::from(*byte >> 4)]));
            output.push(char::from(HEX[usize::from(*byte & 0x0f)]));
            output
        },
    )
}

impl AsyncResultManifest {
    pub(crate) fn part_count(&self) -> usize { self.parts.len() }

    pub(crate) fn schema_ipc(&self) -> &[u8] { &self.schema_ipc }

    pub(crate) fn part(&self, index: usize) -> Option<&DataLocation> { self.parts.get(index) }

    fn validate(
        &self,
        query_id: &str,
        parts: u64,
        rows: u64,
        bytes: u64,
        result_limit_bytes: u64,
    ) -> Result<(), AsyncQueryCoordinatorError> {
        if state_key(query_id).is_none()
            || self.schema_version != 1
            || self.query_id != query_id
            || self.schema_ipc.is_empty()
            || self.schema_ipc.len() > MAX_RESULT_SCHEMA_BYTES
            || self.parts.len() as u64 != parts
            || self.rows != rows
            || self.bytes != bytes
            || self.parts.is_empty()
            || self.parts.len() as u64 > MAX_RESULT_PARTS
            || self.bytes > result_limit_bytes
            || self
                .parts
                .iter()
                .any(|part| !valid_result_location(part, ASYNC_PART_CONTENT_TYPE))
            || self
                .parts
                .iter()
                .try_fold(0_u64, |sum, part| sum.checked_add(part.size_bytes))
                != Some(self.bytes)
        {
            return Err(AsyncQueryCoordinatorError::InvalidJobSpec);
        }
        Ok(())
    }

    fn encode_for_publication(
        &self,
        result_limit_bytes: u64,
    ) -> Result<Vec<u8>, AsyncQueryWorkerError> {
        self.validate(
            &self.query_id,
            self.parts.len() as u64,
            self.rows,
            self.bytes,
            result_limit_bytes,
        )
        .map_err(|_| AsyncQueryWorkerError::ResultBound)?;
        let encoded = serde_json::to_vec(self)
            .map_err(|source| AsyncQueryWorkerError::Manifest { source })?;
        if encoded.is_empty() || encoded.len() as u64 > MAX_RESULT_MANIFEST_BYTES {
            return Err(AsyncQueryWorkerError::ResultBound);
        }
        Ok(encoded)
    }
}

#[derive(Clone)]
pub(crate) struct AsyncQueryWorker {
    coordinator:             AsyncQueryCoordinator,
    engine:                  Arc<QueryEngine>,
    identity:                WorkerIdentity,
    lease:                   Duration,
    global_execution_limits: Option<AsyncGlobalExecutionLimits>,
}

struct ExecutionLease {
    token: String,
}

impl AsyncQueryWorker {
    pub(crate) fn try_new(
        coordinator: AsyncQueryCoordinator,
        engine: Arc<QueryEngine>,
        identity: WorkerIdentity,
        lease: Duration,
    ) -> Result<Self, AsyncQueryWorkerError> {
        if identity.0.iter().all(|byte| *byte == 0)
            || lease.is_zero()
            || lease.as_secs() == 0
            || lease.as_secs() > MAX_WORKER_LEASE_SECS
        {
            return Err(AsyncQueryWorkerError::ResultBound);
        }
        Ok(Self {
            coordinator,
            engine,
            identity,
            lease,
            global_execution_limits: None,
        })
    }

    pub(crate) fn with_global_execution_limits(
        mut self,
        limits: AsyncGlobalExecutionLimits,
    ) -> Self {
        self.global_execution_limits = Some(limits);
        self
    }

    pub(crate) async fn run(
        &self,
        query_id: &str,
        execution_time: Duration,
    ) -> Result<(), AsyncQueryWorkerError> {
        let execution_lease = self.reserve_execution(query_id).await?;
        let claimed_at = unix_now()?;
        let lease = match self
            .coordinator
            .store()
            .claim(query_id, claimed_at, self.identity, self.lease.as_secs())
            .await
        {
            Ok(lease) => lease,
            Err(source) => {
                self.release_execution(query_id, execution_lease.as_ref())
                    .await;
                return Err(AsyncQueryWorkerError::Store { source });
            }
        };
        let mut heartbeat = tokio::time::interval(self.lease / 3);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;
        let deadline = tokio::time::sleep(execution_time);
        let execution = self.run_claimed(query_id, &lease, execution_lease.as_ref());
        tokio::pin!(deadline, execution);
        let result = loop {
            tokio::select! {
                result = &mut execution => break result,
                () = &mut deadline => break Err(AsyncQueryWorkerError::ExecutionDeadline),
                _ = heartbeat.tick() => {
                    if let Err(source) = self.renew(query_id, &lease, execution_lease.as_ref()).await {
                        break Err(source);
                    }
                }
            }
        };
        if result.is_err() {
            if let Ok(now) = unix_now() {
                let code = if matches!(result, Err(AsyncQueryWorkerError::ExecutionDeadline)) {
                    "execution_timeout"
                } else {
                    "execution_failed"
                };
                let _ = self
                    .coordinator
                    .store()
                    .fail(query_id, &lease, now, code)
                    .await;
            }
        }
        self.release_execution(query_id, execution_lease.as_ref())
            .await;
        result
    }

    async fn run_claimed(
        &self,
        query_id: &str,
        lease: &WorkerLease,
        execution_lease: Option<&ExecutionLease>,
    ) -> Result<(), AsyncQueryWorkerError> {
        let record = self
            .coordinator
            .store()
            .load(query_id)
            .await
            .map_err(|source| AsyncQueryWorkerError::Store { source })?
            .ok_or(AsyncQueryWorkerError::Missing)?;
        let statement = self
            .coordinator
            .open_job(&record)
            .await
            .map_err(|source| AsyncQueryWorkerError::Coordinator { source })?;
        let mut snapshots = statement
            .snapshots
            .iter()
            .map(job_snapshot)
            .map(crate::QueryTableSnapshot::Lake)
            .collect::<Vec<_>>();
        for snapshot in &statement.iceberg_snapshots {
            snapshots.push(
                self.engine
                    .resolve_iceberg_snapshot_at(
                        &snapshot.namespace,
                        &snapshot.table,
                        snapshot.snapshot_id,
                    )
                    .await
                    .map_err(|source| AsyncQueryWorkerError::Query { source })?,
            );
        }
        let dataframe = self
            .engine
            .plan_sql_at(&statement.sql, &snapshots)
            .await
            .map_err(|source| AsyncQueryWorkerError::Query { source })?;
        let schema = Arc::new(dataframe.schema().as_arrow().clone());
        let IpcMessage(schema_ipc) = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|source| AsyncQueryWorkerError::Arrow { source })?;
        let mut batches =
            dataframe
                .execute_stream()
                .await
                .map_err(|source| AsyncQueryWorkerError::Query {
                    source: QueryError::Execute { source },
                })?;
        let scope = ManagedObjectScope::try_new(&record.tenant_id, query_id).map_err(|source| {
            AsyncQueryWorkerError::Coordinator {
                source: AsyncQueryCoordinatorError::Object { source },
            }
        })?;
        let mut parts = Vec::new();
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        let result_limit_bytes = record.result_limit_bytes();
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(|source| AsyncQueryWorkerError::Query {
                source: QueryError::Execute { source },
            })?;
            rows = rows
                .checked_add(batch.num_rows() as u64)
                .ok_or(AsyncQueryWorkerError::ResultBound)?;
            for offset in (0..batch.num_rows()).step_by(MAX_RESULT_PART_ROWS) {
                let length = (batch.num_rows() - offset).min(MAX_RESULT_PART_ROWS);
                self.write_part(
                    &scope,
                    batch.slice(offset, length),
                    &mut parts,
                    &mut bytes,
                    result_limit_bytes,
                )
                .await?;
                self.renew(query_id, lease, execution_lease).await?;
            }
        }
        if parts.is_empty() {
            let empty = RecordBatch::new_empty(schema);
            self.write_part(&scope, empty, &mut parts, &mut bytes, result_limit_bytes)
                .await?;
        }
        let manifest = AsyncResultManifest {
            schema_version: 1,
            query_id: query_id.to_owned(),
            schema_ipc: schema_ipc.to_vec(),
            parts,
            rows,
            bytes,
        };
        let encoded = manifest.encode_for_publication(result_limit_bytes)?;
        let input =
            AsyncStreamReader::new(stream::iter([Ok::<Bytes, io::Error>(Bytes::from(encoded))]));
        let manifest_location = self
            .coordinator
            .objects
            .put_scoped_reader(
                &scope,
                "manifest",
                Box::pin(input),
                ASYNC_MANIFEST_CONTENT_TYPE.to_owned(),
            )
            .await
            .map_err(|source| AsyncQueryWorkerError::Coordinator {
                source: AsyncQueryCoordinatorError::Object { source },
            })?;
        self.coordinator
            .store()
            .complete(
                query_id,
                lease,
                unix_now()?,
                manifest_location,
                manifest.parts.len() as u64,
                rows,
                bytes,
            )
            .await
            .map_err(|source| AsyncQueryWorkerError::Store { source })
    }

    async fn write_part(
        &self,
        scope: &ManagedObjectScope,
        batch: RecordBatch,
        parts: &mut Vec<DataLocation>,
        total_bytes: &mut u64,
        result_limit_bytes: u64,
    ) -> Result<(), AsyncQueryWorkerError> {
        if parts.len() as u64 >= MAX_RESULT_PARTS {
            return Err(AsyncQueryWorkerError::ResultBound);
        }
        let remaining_total = result_limit_bytes
            .checked_sub(*total_bytes)
            .ok_or(AsyncQueryWorkerError::ResultBound)?;
        let encoded_limit = MAX_RESULT_PART_BYTES.min(remaining_total);
        if encoded_limit == 0 {
            return Err(AsyncQueryWorkerError::ResultBound);
        }
        let input = encoded_batch_reader(
            batch,
            IpcPipelineLimits::production(encoded_limit),
            PipelineProbe::default(),
        );
        let location = self
            .coordinator
            .objects
            .put_scoped_reader(
                scope,
                "part",
                Box::pin(input),
                ASYNC_PART_CONTENT_TYPE.to_owned(),
            )
            .await
            .map_err(|source| AsyncQueryWorkerError::Coordinator {
                source: AsyncQueryCoordinatorError::Object { source },
            })?;
        let part_bytes = location.size_bytes;
        let next_total = total_bytes
            .checked_add(part_bytes)
            .ok_or(AsyncQueryWorkerError::ResultBound)?;
        if part_bytes == 0 || part_bytes > encoded_limit || next_total > result_limit_bytes {
            return Err(AsyncQueryWorkerError::ResultBound);
        }
        *total_bytes = next_total;
        parts.push(location);
        Ok(())
    }

    async fn renew(
        &self,
        query_id: &str,
        lease: &WorkerLease,
        execution_lease: Option<&ExecutionLease>,
    ) -> Result<(), AsyncQueryWorkerError> {
        if let Some(execution_lease) = execution_lease {
            self.coordinator
                .store()
                .renew_execution(
                    query_id,
                    &execution_lease.token,
                    unix_now()?,
                    self.lease.as_secs(),
                )
                .await
                .map_err(|source| AsyncQueryWorkerError::Store { source })?;
        }
        self.coordinator
            .store()
            .renew(query_id, lease, unix_now()?, self.lease.as_secs())
            .await
            .map_err(|source| AsyncQueryWorkerError::Store { source })
    }

    async fn reserve_execution(
        &self,
        query_id: &str,
    ) -> Result<Option<ExecutionLease>, AsyncQueryWorkerError> {
        let Some(limits) = self.global_execution_limits else {
            return Ok(None);
        };
        let record = self
            .coordinator
            .store()
            .load(query_id)
            .await
            .map_err(|source| AsyncQueryWorkerError::Store { source })?
            .ok_or(AsyncQueryWorkerError::Missing)?;
        let token = uuid::Uuid::now_v7().simple().to_string();
        match self
            .coordinator
            .store()
            .reserve_execution(
                record.tenant_id(),
                query_id,
                &token,
                unix_now()?,
                self.lease.as_secs(),
                limits,
            )
            .await
        {
            Ok(()) => {
                crate::telemetry::async_cluster_execution("reserved");
                Ok(Some(ExecutionLease { token }))
            }
            Err(
                AsyncQueryStoreError::ExecutionCapacityHeld
                | AsyncQueryStoreError::ExecutionLeaseHeld,
            ) => {
                crate::telemetry::async_cluster_execution("saturated");
                Err(AsyncQueryWorkerError::ExecutionCapacityHeld)
            }
            Err(source) => Err(AsyncQueryWorkerError::Store { source }),
        }
    }

    async fn release_execution(&self, query_id: &str, execution_lease: Option<&ExecutionLease>) {
        let Some(execution_lease) = execution_lease else {
            return;
        };
        match unix_now() {
            Ok(now) => match self
                .coordinator
                .store()
                .release_execution(query_id, &execution_lease.token, now)
                .await
            {
                Ok(()) => crate::telemetry::async_cluster_execution("released"),
                Err(error) => {
                    tracing::warn!(error = %error, "async cluster execution release failed")
                }
            },
            Err(error) => tracing::warn!(error = %error, "async cluster execution release skipped"),
        }
    }
}

fn job_snapshot(snapshot: &StatementTableSnapshot) -> TableSnapshot {
    TableSnapshot::new(
        TableRef::new(&snapshot.namespace, &snapshot.table),
        TableLocation::new(&snapshot.location),
        &snapshot.engine,
        &snapshot.incarnation_id,
        Version(snapshot.version),
    )
}

fn unix_now() -> Result<u64, AsyncQueryWorkerError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| AsyncQueryWorkerError::InvalidTime)
}

impl AsyncQueryStore {
    pub(crate) fn new(meta: MetaStoreRef) -> Self { Self { meta } }

    pub(crate) async fn create(
        &self,
        record: AsyncQueryRecord,
    ) -> Result<(), AsyncQueryStoreError> {
        record
            .validate()
            .map_err(|_| AsyncQueryStoreError::InvalidStateRecord)?;
        let key = state_key(&record.query_id).ok_or(AsyncQueryStoreError::InvalidStateRecord)?;
        let encoded = encode_record(&record)?;
        let created = self
            .meta
            .cas(&key, None, &encoded)
            .await
            .map_err(|source| AsyncQueryStoreError::Meta { source })?;
        if created {
            Ok(())
        } else {
            Err(AsyncQueryStoreError::AlreadyExists)
        }
    }

    pub(crate) async fn reserve_tenant(
        &self,
        tenant_id: &str,
        query_id: &str,
        token: &str,
        now: u64,
        limit: usize,
    ) -> Result<(), AsyncQueryStoreError> {
        if !bounded(tenant_id, MAX_TENANT_BYTES)
            || state_key(query_id).is_none()
            || !valid_reservation_token(token)
            || !(1..=MAX_OUTSTANDING_PER_TENANT).contains(&limit)
        {
            return Err(AsyncQueryStoreError::InvalidStateRecord);
        }
        let key = tenant_index_key(tenant_id).ok_or(AsyncQueryStoreError::InvalidStateRecord)?;
        for _ in 0..TENANT_CAS_ATTEMPTS {
            let expected = self
                .meta
                .get(&key)
                .await
                .map_err(|source| AsyncQueryStoreError::Meta { source })?;
            let mut index = decode_tenant_index(expected.as_deref())?;
            self.reconcile_tenant_index(tenant_id, &mut index, now)
                .await?;
            if let Some(entry) = index
                .entries
                .iter_mut()
                .find(|entry| entry.query_id == query_id)
            {
                if entry.token != token {
                    return Err(AsyncQueryStoreError::ReservationHeld);
                }
                entry.expires_at = entry
                    .expires_at
                    .max(now.saturating_add(TENANT_RESERVATION_GRACE_SECS));
            } else {
                if index.entries.len() >= limit {
                    return Err(AsyncQueryStoreError::QuotaExceeded);
                }
                index.entries.push(TenantReservation {
                    query_id:   query_id.to_owned(),
                    token:      token.to_owned(),
                    expires_at: now
                        .checked_add(TENANT_RESERVATION_GRACE_SECS)
                        .ok_or(AsyncQueryStoreError::InvalidStateRecord)?,
                });
            }
            let encoded = encode_tenant_index(&index)?;
            if self
                .meta
                .cas(&key, expected.as_deref(), &encoded)
                .await
                .map_err(|source| AsyncQueryStoreError::Meta { source })?
            {
                return Ok(());
            }
        }
        Err(AsyncQueryStoreError::Conflict)
    }

    pub(crate) async fn confirm_tenant(
        &self,
        tenant_id: &str,
        query_id: &str,
        token: &str,
        expires_at: u64,
    ) -> Result<(), AsyncQueryStoreError> {
        self.mutate_tenant_index(tenant_id, |index| {
            let entry = index
                .entries
                .iter_mut()
                .find(|entry| entry.query_id == query_id && entry.token == token)
                .ok_or(AsyncQueryStoreError::InvalidStateRecord)?;
            entry.expires_at = expires_at;
            Ok(())
        })
        .await
    }

    pub(crate) async fn release_tenant(
        &self,
        tenant_id: &str,
        query_id: &str,
        token: &str,
    ) -> Result<(), AsyncQueryStoreError> {
        self.mutate_tenant_index(tenant_id, |index| {
            index
                .entries
                .retain(|entry| entry.query_id != query_id || entry.token != token);
            Ok(())
        })
        .await
    }

    pub(crate) async fn reserve_execution(
        &self,
        tenant_id: &str,
        query_id: &str,
        token: &str,
        now: u64,
        lease_secs: u64,
        limits: AsyncGlobalExecutionLimits,
    ) -> Result<(), AsyncQueryStoreError> {
        let tenant_digest =
            execution_tenant_digest(tenant_id).ok_or(AsyncQueryStoreError::InvalidStateRecord)?;
        if state_key(query_id).is_none()
            || !valid_reservation_token(token)
            || lease_secs == 0
            || lease_secs > MAX_WORKER_LEASE_SECS
            || AsyncGlobalExecutionLimits::try_new(
                limits.max_running(),
                limits.max_running_per_tenant(),
            )
            .is_err()
        {
            return Err(AsyncQueryStoreError::InvalidStateRecord);
        }
        let expires_at = now
            .checked_add(lease_secs)
            .ok_or(AsyncQueryStoreError::InvalidStateRecord)?;
        for _ in 0..EXECUTION_CAS_ATTEMPTS {
            let expected = self
                .meta
                .get(EXECUTION_KEY)
                .await
                .map_err(|source| AsyncQueryStoreError::Meta { source })?;
            let mut index = decode_execution_index(expected.as_deref())?;
            index.entries.retain(|entry| entry.expires_at > now);
            if let Some(entry) = index
                .entries
                .iter_mut()
                .find(|entry| entry.query_id == query_id)
            {
                if entry.token != token || entry.tenant_digest != tenant_digest {
                    return Err(AsyncQueryStoreError::ExecutionLeaseHeld);
                }
                entry.expires_at = expires_at;
            } else {
                if index.entries.len() >= limits.max_running()
                    || index
                        .entries
                        .iter()
                        .filter(|entry| entry.tenant_digest == tenant_digest)
                        .count()
                        >= limits.max_running_per_tenant()
                {
                    return Err(AsyncQueryStoreError::ExecutionCapacityHeld);
                }
                index.entries.push(ExecutionLeaseEntry {
                    query_id: query_id.to_owned(),
                    tenant_digest: tenant_digest.clone(),
                    token: token.to_owned(),
                    expires_at,
                });
            }
            let encoded = encode_execution_index(&index)?;
            if self
                .meta
                .cas(EXECUTION_KEY, expected.as_deref(), &encoded)
                .await
                .map_err(|source| AsyncQueryStoreError::Meta { source })?
            {
                return Ok(());
            }
        }
        Err(AsyncQueryStoreError::Conflict)
    }

    pub(crate) async fn renew_execution(
        &self,
        query_id: &str,
        token: &str,
        now: u64,
        lease_secs: u64,
    ) -> Result<(), AsyncQueryStoreError> {
        if state_key(query_id).is_none()
            || !valid_reservation_token(token)
            || lease_secs == 0
            || lease_secs > MAX_WORKER_LEASE_SECS
        {
            return Err(AsyncQueryStoreError::InvalidStateRecord);
        }
        let expires_at = now
            .checked_add(lease_secs)
            .ok_or(AsyncQueryStoreError::InvalidStateRecord)?;
        for _ in 0..EXECUTION_CAS_ATTEMPTS {
            let expected = self
                .meta
                .get(EXECUTION_KEY)
                .await
                .map_err(|source| AsyncQueryStoreError::Meta { source })?;
            let mut index = decode_execution_index(expected.as_deref())?;
            index.entries.retain(|entry| entry.expires_at > now);
            let Some(entry) = index
                .entries
                .iter_mut()
                .find(|entry| entry.query_id == query_id && entry.token == token)
            else {
                return Err(AsyncQueryStoreError::ExecutionLeaseLost);
            };
            entry.expires_at = expires_at;
            let encoded = encode_execution_index(&index)?;
            if self
                .meta
                .cas(EXECUTION_KEY, expected.as_deref(), &encoded)
                .await
                .map_err(|source| AsyncQueryStoreError::Meta { source })?
            {
                return Ok(());
            }
        }
        Err(AsyncQueryStoreError::Conflict)
    }

    pub(crate) async fn release_execution(
        &self,
        query_id: &str,
        token: &str,
        now: u64,
    ) -> Result<(), AsyncQueryStoreError> {
        if state_key(query_id).is_none() || !valid_reservation_token(token) {
            return Err(AsyncQueryStoreError::InvalidStateRecord);
        }
        for _ in 0..EXECUTION_CAS_ATTEMPTS {
            let expected = self
                .meta
                .get(EXECUTION_KEY)
                .await
                .map_err(|source| AsyncQueryStoreError::Meta { source })?;
            let mut index = decode_execution_index(expected.as_deref())?;
            let before = index.entries.len();
            index.entries.retain(|entry| {
                entry.expires_at > now && !(entry.query_id == query_id && entry.token == token)
            });
            if index.entries.len() == before {
                return Ok(());
            }
            let encoded = encode_execution_index(&index)?;
            if self
                .meta
                .cas(EXECUTION_KEY, expected.as_deref(), &encoded)
                .await
                .map_err(|source| AsyncQueryStoreError::Meta { source })?
            {
                return Ok(());
            }
        }
        Err(AsyncQueryStoreError::Conflict)
    }

    async fn reconcile_tenant_index(
        &self,
        tenant_id: &str,
        index: &mut TenantResourceIndex,
        now: u64,
    ) -> Result<(), AsyncQueryStoreError> {
        let mut retained = Vec::with_capacity(index.entries.len());
        for mut entry in index.entries.drain(..) {
            if entry.expires_at > now {
                retained.push(entry);
                continue;
            }
            if let Some(record) = self.load(&entry.query_id).await? {
                if record.tenant_id() != tenant_id {
                    return Err(AsyncQueryStoreError::InvalidStateRecord);
                }
                if let Some(token) = record.tenant_reservation_token() {
                    if token != entry.token {
                        return Err(AsyncQueryStoreError::InvalidStateRecord);
                    }
                    entry.expires_at = record
                        .expires_at()
                        .max(now.saturating_add(TENANT_RESERVATION_GRACE_SECS));
                    retained.push(entry);
                }
            }
        }
        index.entries = retained;
        Ok(())
    }

    async fn mutate_tenant_index<F>(
        &self,
        tenant_id: &str,
        mutate: F,
    ) -> Result<(), AsyncQueryStoreError>
    where
        F: Fn(&mut TenantResourceIndex) -> Result<(), AsyncQueryStoreError>,
    {
        let key = tenant_index_key(tenant_id).ok_or(AsyncQueryStoreError::InvalidStateRecord)?;
        for _ in 0..TENANT_CAS_ATTEMPTS {
            let expected = self
                .meta
                .get(&key)
                .await
                .map_err(|source| AsyncQueryStoreError::Meta { source })?;
            let mut index = decode_tenant_index(expected.as_deref())?;
            mutate(&mut index)?;
            let encoded = encode_tenant_index(&index)?;
            if self
                .meta
                .cas(&key, expected.as_deref(), &encoded)
                .await
                .map_err(|source| AsyncQueryStoreError::Meta { source })?
            {
                return Ok(());
            }
        }
        Err(AsyncQueryStoreError::Conflict)
    }

    pub(crate) async fn load(
        &self,
        query_id: &str,
    ) -> Result<Option<AsyncQueryRecord>, AsyncQueryStoreError> {
        self.load_encoded(query_id)
            .await
            .map(|loaded| loaded.map(|(_, record)| record))
    }

    pub(crate) async fn list_records_page(
        &self,
        continuation: Option<&str>,
    ) -> Result<(Vec<AsyncQueryRecord>, usize, Option<String>), AsyncQueryStoreError> {
        let page = self
            .meta
            .scan_prefix_page(STATE_KEY_PREFIX, continuation, SCAN_PAGE_JOBS)
            .await
            .map_err(|source| AsyncQueryStoreError::Meta { source })?;
        let (entries, continuation) = page.into_parts();
        let mut records = Vec::with_capacity(entries.len());
        let mut invalid = 0;
        for (query_id, encoded) in entries {
            match decode_scanned_record(&query_id, &encoded) {
                Ok(record) => records.push(record),
                Err(_) => invalid += 1,
            }
        }
        Ok((records, invalid, continuation))
    }

    pub(crate) async fn claim(
        &self,
        query_id: &str,
        now: u64,
        worker: WorkerIdentity,
        lease_secs: u64,
    ) -> Result<WorkerLease, AsyncQueryStoreError> {
        self.transition(query_id, |record| record.claim(now, worker, lease_secs))
            .await
    }

    pub(crate) async fn renew(
        &self,
        query_id: &str,
        lease: &WorkerLease,
        now: u64,
        lease_secs: u64,
    ) -> Result<(), AsyncQueryStoreError> {
        self.transition(query_id, |record| record.renew(lease, now, lease_secs))
            .await
    }

    pub(crate) async fn complete(
        &self,
        query_id: &str,
        lease: &WorkerLease,
        now: u64,
        manifest: DataLocation,
        parts: u64,
        rows: u64,
        bytes: u64,
    ) -> Result<(), AsyncQueryStoreError> {
        self.transition(query_id, |record| {
            record.complete(lease, now, manifest, parts, rows, bytes)
        })
        .await
    }

    pub(crate) async fn fail(
        &self,
        query_id: &str,
        lease: &WorkerLease,
        now: u64,
        code: &str,
    ) -> Result<(), AsyncQueryStoreError> {
        self.transition(query_id, |record| record.fail(lease, now, code))
            .await
    }

    pub(crate) async fn cancel(
        &self,
        query_id: &str,
        now: u64,
    ) -> Result<(), AsyncQueryStoreError> {
        self.transition(query_id, |record| record.cancel(now)).await
    }

    pub(crate) async fn expire(
        &self,
        query_id: &str,
        now: u64,
    ) -> Result<(), AsyncQueryStoreError> {
        self.transition(query_id, |record| record.expire(now)).await
    }

    pub(crate) async fn begin_cleanup(
        &self,
        query_id: &str,
        now: u64,
    ) -> Result<(), AsyncQueryStoreError> {
        self.transition(query_id, |record| record.begin_cleanup(now))
            .await
    }

    pub(crate) async fn delete_cleaning(&self, query_id: &str) -> Result<(), AsyncQueryStoreError> {
        let key = state_key(query_id).ok_or(AsyncQueryStoreError::InvalidStateRecord)?;
        let Some((expected, record)) = self.load_encoded(query_id).await? else {
            return Ok(());
        };
        if !matches!(record.state, AsyncQueryState::Cleaning { .. }) {
            return Err(AsyncQueryStoreError::InvalidStateRecord);
        }
        let deleted = self
            .meta
            .delete(&key, &expected)
            .await
            .map_err(|source| AsyncQueryStoreError::Meta { source })?;
        if deleted {
            Ok(())
        } else {
            Err(AsyncQueryStoreError::Conflict)
        }
    }

    async fn transition<T, F>(&self, query_id: &str, mutate: F) -> Result<T, AsyncQueryStoreError>
    where
        F: FnOnce(&mut AsyncQueryRecord) -> Result<T, AsyncQueryTransitionError>,
    {
        let key = state_key(query_id).ok_or(AsyncQueryStoreError::InvalidStateRecord)?;
        let Some((expected, mut record)) = self.load_encoded(query_id).await? else {
            return Err(AsyncQueryStoreError::InvalidStateRecord);
        };
        let result =
            mutate(&mut record).map_err(|source| AsyncQueryStoreError::Transition { source })?;
        record
            .validate()
            .map_err(|_| AsyncQueryStoreError::InvalidStateRecord)?;
        let encoded = encode_record(&record)?;
        let installed = self
            .meta
            .cas(&key, Some(&expected), &encoded)
            .await
            .map_err(|source| AsyncQueryStoreError::Meta { source })?;
        if installed {
            Ok(result)
        } else {
            Err(AsyncQueryStoreError::Conflict)
        }
    }

    async fn load_encoded(
        &self,
        query_id: &str,
    ) -> Result<Option<(Vec<u8>, AsyncQueryRecord)>, AsyncQueryStoreError> {
        let key = state_key(query_id).ok_or(AsyncQueryStoreError::InvalidStateRecord)?;
        let Some(encoded) = self
            .meta
            .get(&key)
            .await
            .map_err(|source| AsyncQueryStoreError::Meta { source })?
        else {
            return Ok(None);
        };
        if encoded.len() > MAX_STATE_RECORD_BYTES {
            return Err(AsyncQueryStoreError::RecordTooLarge);
        }
        let record: AsyncQueryRecord = serde_json::from_slice(&encoded)
            .map_err(|source| AsyncQueryStoreError::Decode { source })?;
        record
            .validate()
            .map_err(|_| AsyncQueryStoreError::InvalidStateRecord)?;
        if record.query_id != query_id {
            return Err(AsyncQueryStoreError::InvalidStateRecord);
        }
        Ok(Some((encoded, record)))
    }
}

fn decode_scanned_record(
    query_id: &str,
    encoded: &[u8],
) -> Result<AsyncQueryRecord, AsyncQueryStoreError> {
    if encoded.len() > MAX_STATE_RECORD_BYTES {
        return Err(AsyncQueryStoreError::RecordTooLarge);
    }
    let record: AsyncQueryRecord = serde_json::from_slice(encoded)
        .map_err(|source| AsyncQueryStoreError::Decode { source })?;
    record
        .validate()
        .map_err(|_| AsyncQueryStoreError::InvalidStateRecord)?;
    if record.query_id != query_id || state_key(query_id).is_none() {
        return Err(AsyncQueryStoreError::InvalidStateRecord);
    }
    Ok(record)
}

impl AsyncQueryRecord {
    pub(crate) fn query_id(&self) -> &str { &self.query_id }

    pub(crate) fn tenant_id(&self) -> &str { &self.tenant_id }

    pub(crate) fn try_new(
        query_id: impl Into<String>,
        tenant_id: impl Into<String>,
        principal_id: impl Into<String>,
        job_spec: DataLocation,
        created_at: u64,
        expires_at: u64,
    ) -> Result<Self, AsyncQueryTransitionError> {
        let query_id = query_id.into();
        let tenant_id = tenant_id.into();
        let principal_id = principal_id.into();
        if !bounded(&query_id, MAX_QUERY_ID_BYTES)
            || !bounded(&tenant_id, MAX_TENANT_BYTES)
            || !bounded(&principal_id, MAX_PRINCIPAL_BYTES)
            || !valid_job_spec(&job_spec)
            || expires_at <= created_at
            || expires_at - created_at > MAX_JOB_LIFETIME_SECS
        {
            return Err(AsyncQueryTransitionError::InvalidRecord);
        }
        Ok(Self {
            query_id,
            schema_version: 1,
            tenant_id,
            principal_id,
            job_spec,
            created_at,
            expires_at,
            next_lease_epoch: 1,
            state: AsyncQueryState::Queued,
            resources: None,
        })
    }

    pub(crate) fn try_new_with_resources(
        query_id: impl Into<String>,
        tenant_id: impl Into<String>,
        principal_id: impl Into<String>,
        job_spec: DataLocation,
        created_at: u64,
        expires_at: u64,
        tenant_reservation_token: impl Into<String>,
        limits: AsyncResourceLimits,
    ) -> Result<Self, AsyncQueryTransitionError> {
        let mut record = Self::try_new(
            query_id,
            tenant_id,
            principal_id,
            job_spec,
            created_at,
            expires_at,
        )?;
        record.schema_version = 2;
        let tenant_reservation_token = tenant_reservation_token.into();
        if !valid_reservation_token(&tenant_reservation_token) {
            return Err(AsyncQueryTransitionError::InvalidRecord);
        }
        record.resources = Some(AsyncRecordResources {
            result_limit_bytes: limits.result_bytes(),
            tenant_reservation_token,
        });
        Ok(record)
    }

    pub(crate) fn result_limit_bytes(&self) -> u64 {
        self.resources
            .as_ref()
            .map_or(MAX_RESULT_BYTES, |resources| resources.result_limit_bytes)
    }

    pub(crate) fn has_tenant_reservation(&self) -> bool {
        self.tenant_reservation_token().is_some()
    }

    pub(crate) fn tenant_reservation_token(&self) -> Option<&str> {
        self.resources
            .as_ref()
            .map(|resources| resources.tenant_reservation_token.as_str())
    }

    pub(crate) fn claim(
        &mut self,
        now: u64,
        worker: WorkerIdentity,
        lease_secs: u64,
    ) -> Result<WorkerLease, AsyncQueryTransitionError> {
        if lease_secs == 0 || lease_secs > MAX_WORKER_LEASE_SECS {
            return Err(AsyncQueryTransitionError::InvalidRecord);
        }
        if now < self.created_at {
            return Err(AsyncQueryTransitionError::InvalidRecord);
        }
        if now >= self.expires_at {
            return Err(AsyncQueryTransitionError::Terminal);
        }
        if matches!(
            self.state,
            AsyncQueryState::Running { lease_expires, .. } if now <= lease_expires
        ) {
            return Err(AsyncQueryTransitionError::LeaseHeld);
        }
        if !matches!(
            self.state,
            AsyncQueryState::Queued | AsyncQueryState::Running { .. }
        ) {
            return Err(AsyncQueryTransitionError::Terminal);
        }
        let epoch = self.next_lease_epoch;
        self.next_lease_epoch = epoch
            .checked_add(1)
            .ok_or(AsyncQueryTransitionError::Overflow)?;
        let lease_expires = now
            .checked_add(lease_secs)
            .ok_or(AsyncQueryTransitionError::Overflow)?
            .min(self.expires_at);
        let lease = WorkerLease { worker, epoch };
        self.state = AsyncQueryState::Running {
            lease,
            lease_expires,
        };
        Ok(lease)
    }

    pub(crate) fn complete(
        &mut self,
        lease: &WorkerLease,
        now: u64,
        manifest: DataLocation,
        parts: u64,
        rows: u64,
        bytes: u64,
    ) -> Result<(), AsyncQueryTransitionError> {
        match self.state {
            AsyncQueryState::Running {
                lease: current,
                lease_expires,
            } if &current == lease
                && now >= self.created_at
                && now <= lease_expires
                && now < self.expires_at => {}
            AsyncQueryState::Running { .. } => {
                return Err(AsyncQueryTransitionError::StaleWorker);
            }
            _ => return Err(AsyncQueryTransitionError::Terminal),
        }
        if !valid_manifest_location(&manifest)
            || parts == 0
            || parts > MAX_RESULT_PARTS
            || bytes > self.result_limit_bytes()
        {
            return Err(AsyncQueryTransitionError::InvalidRecord);
        }
        self.state = AsyncQueryState::Completed {
            manifest,
            parts,
            rows,
            bytes,
        };
        Ok(())
    }

    pub(crate) fn renew(
        &mut self,
        lease: &WorkerLease,
        now: u64,
        lease_secs: u64,
    ) -> Result<(), AsyncQueryTransitionError> {
        if lease_secs == 0 || lease_secs > MAX_WORKER_LEASE_SECS {
            return Err(AsyncQueryTransitionError::InvalidRecord);
        }
        match &mut self.state {
            AsyncQueryState::Running {
                lease: current,
                lease_expires,
            } if current == lease
                && now >= self.created_at
                && now <= *lease_expires
                && now < self.expires_at =>
            {
                *lease_expires = now
                    .checked_add(lease_secs)
                    .ok_or(AsyncQueryTransitionError::Overflow)?
                    .min(self.expires_at);
                Ok(())
            }
            AsyncQueryState::Running { .. } => Err(AsyncQueryTransitionError::StaleWorker),
            _ => Err(AsyncQueryTransitionError::Terminal),
        }
    }

    pub(crate) fn fail(
        &mut self,
        lease: &WorkerLease,
        now: u64,
        code: impl Into<String>,
    ) -> Result<(), AsyncQueryTransitionError> {
        match self.state {
            AsyncQueryState::Running {
                lease: current,
                lease_expires,
            } if &current == lease
                && now >= self.created_at
                && now <= lease_expires
                && now < self.expires_at => {}
            AsyncQueryState::Running { .. } => {
                return Err(AsyncQueryTransitionError::StaleWorker);
            }
            _ => return Err(AsyncQueryTransitionError::Terminal),
        }
        let code = code.into();
        if !bounded(&code, MAX_FAILURE_CODE_BYTES) {
            return Err(AsyncQueryTransitionError::InvalidRecord);
        }
        self.state = AsyncQueryState::Failed {
            code,
            failed_at: now,
        };
        Ok(())
    }

    pub(crate) fn expire(&mut self, now: u64) -> Result<(), AsyncQueryTransitionError> {
        if now < self.expires_at {
            return Err(AsyncQueryTransitionError::InvalidRecord);
        }
        if !matches!(
            self.state,
            AsyncQueryState::Queued | AsyncQueryState::Running { .. }
        ) {
            return Err(AsyncQueryTransitionError::Terminal);
        }
        self.state = AsyncQueryState::Expired;
        Ok(())
    }

    fn begin_cleanup(&mut self, now: u64) -> Result<(), AsyncQueryTransitionError> {
        if now < self.expires_at {
            return Err(AsyncQueryTransitionError::InvalidRecord);
        }
        self.state = AsyncQueryState::Cleaning { started_at: now };
        Ok(())
    }

    pub(crate) fn cancel(&mut self, now: u64) -> Result<(), AsyncQueryTransitionError> {
        if now < self.created_at || now >= self.expires_at {
            return Err(AsyncQueryTransitionError::InvalidRecord);
        }
        if !matches!(
            self.state,
            AsyncQueryState::Queued | AsyncQueryState::Running { .. }
        ) {
            return Err(AsyncQueryTransitionError::Terminal);
        }
        self.state = AsyncQueryState::Cancelled { cancelled_at: now };
        Ok(())
    }

    pub(crate) fn is_completed(&self) -> bool {
        matches!(self.state, AsyncQueryState::Completed { .. })
    }

    pub(crate) fn is_failed(&self) -> bool { matches!(self.state, AsyncQueryState::Failed { .. }) }

    pub(crate) fn is_cancelled(&self) -> bool {
        matches!(self.state, AsyncQueryState::Cancelled { .. })
    }

    pub(crate) fn is_expired(&self) -> bool {
        matches!(
            self.state,
            AsyncQueryState::Expired | AsyncQueryState::Cleaning { .. }
        )
    }

    pub(crate) fn active_lease(&self) -> Option<WorkerLease> {
        match self.state {
            AsyncQueryState::Running { lease, .. } => Some(lease),
            _ => None,
        }
    }

    pub(crate) fn completed_manifest(&self) -> Option<&DataLocation> {
        match &self.state {
            AsyncQueryState::Completed { manifest, .. } => Some(manifest),
            _ => None,
        }
    }

    fn completed_summary(&self) -> Option<(u64, u64, u64)> {
        match self.state {
            AsyncQueryState::Completed {
                parts, rows, bytes, ..
            } => Some((parts, rows, bytes)),
            _ => None,
        }
    }

    fn cleaning_started_at(&self) -> Option<u64> {
        match self.state {
            AsyncQueryState::Cleaning { started_at } => Some(started_at),
            _ => None,
        }
    }

    pub(crate) fn job_spec(&self) -> &DataLocation { &self.job_spec }

    pub(crate) fn belongs_to(&self, principal: &Principal) -> bool {
        self.tenant_id == principal.tenant().as_str() && self.principal_id == principal.subject()
    }

    fn principal(&self) -> Result<Principal, AsyncQueryCoordinatorError> {
        Principal::try_new(
            PrincipalId::try_new(&self.principal_id)
                .map_err(|_| AsyncQueryCoordinatorError::InvalidJobSpec)?,
            TenantId::try_new(&self.tenant_id)
                .map_err(|_| AsyncQueryCoordinatorError::InvalidJobSpec)?,
            PrincipalRole::QueryService,
            std::iter::empty::<&str>(),
        )
        .map_err(|_| AsyncQueryCoordinatorError::InvalidJobSpec)
    }

    pub(crate) const fn expires_at(&self) -> u64 { self.expires_at }

    fn scope(&self) -> Result<ManagedObjectScope, AsyncQueryCoordinatorError> {
        ManagedObjectScope::try_new(&self.tenant_id, &self.query_id)
            .map_err(|source| AsyncQueryCoordinatorError::Object { source })
    }

    pub(crate) fn is_pending(&self) -> bool {
        matches!(
            self.state,
            AsyncQueryState::Queued | AsyncQueryState::Running { .. }
        )
    }

    #[cfg(test)]
    pub(crate) fn failure_code(&self) -> Option<&str> {
        match &self.state {
            AsyncQueryState::Failed { code, .. } => Some(code),
            _ => None,
        }
    }

    fn validate(&self) -> Result<(), AsyncQueryTransitionError> {
        let resources_valid = match (&self.schema_version, &self.resources) {
            (1, None) => true,
            (2, Some(resources)) => {
                valid_reservation_token(&resources.tenant_reservation_token)
                    && (MIN_CONFIG_RESULT_BYTES..=MAX_CONFIG_RESULT_BYTES)
                        .contains(&resources.result_limit_bytes)
            }
            _ => false,
        };
        if !resources_valid
            || state_key(&self.query_id).is_none()
            || !bounded(&self.tenant_id, MAX_TENANT_BYTES)
            || !bounded(&self.principal_id, MAX_PRINCIPAL_BYTES)
            || !valid_job_spec(&self.job_spec)
            || self.expires_at <= self.created_at
            || self.expires_at - self.created_at > MAX_JOB_LIFETIME_SECS
            || self.next_lease_epoch == 0
        {
            return Err(AsyncQueryTransitionError::InvalidRecord);
        }
        match &self.state {
            AsyncQueryState::Queued | AsyncQueryState::Expired => Ok(()),
            AsyncQueryState::Cleaning { started_at } if *started_at >= self.expires_at => Ok(()),
            AsyncQueryState::Running {
                lease,
                lease_expires,
            } if lease.epoch > 0
                && lease.epoch < self.next_lease_epoch
                && lease.worker.0.iter().any(|byte| *byte != 0)
                && *lease_expires > self.created_at
                && *lease_expires <= self.expires_at =>
            {
                Ok(())
            }
            AsyncQueryState::Completed {
                manifest,
                parts,
                bytes,
                ..
            } if valid_manifest_location(manifest)
                && *parts > 0
                && *parts <= MAX_RESULT_PARTS
                && *bytes <= self.result_limit_bytes() =>
            {
                Ok(())
            }
            AsyncQueryState::Failed { code, failed_at }
                if bounded(code, MAX_FAILURE_CODE_BYTES)
                    && *failed_at >= self.created_at
                    && *failed_at <= self.expires_at =>
            {
                Ok(())
            }
            AsyncQueryState::Cancelled { cancelled_at }
                if *cancelled_at >= self.created_at && *cancelled_at <= self.expires_at =>
            {
                Ok(())
            }
            _ => Err(AsyncQueryTransitionError::InvalidRecord),
        }
    }
}

fn bounded(value: &str, maximum: usize) -> bool { !value.is_empty() && value.len() <= maximum }

fn valid_job_spec(location: &DataLocation) -> bool {
    bounded(&location.uri, MAX_URI_BYTES)
        && location.content_type == ASYNC_JOB_CONTENT_TYPE
        && location.size_bytes > 0
        && location.size_bytes <= MAX_JOB_SPEC_BYTES
        && location.sha256.len() == 64
        && location
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn valid_result_location(location: &DataLocation, content_type: &str) -> bool {
    valid_async_result_uri(&location.uri)
        && location.content_type == content_type
        && location.size_bytes > 0
        && location.size_bytes <= MAX_RESULT_PART_BYTES
        && location.sha256.len() == 64
        && location
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn valid_manifest_location(location: &DataLocation) -> bool {
    valid_result_location(location, ASYNC_MANIFEST_CONTENT_TYPE)
        && location.size_bytes <= MAX_RESULT_MANIFEST_BYTES
}

fn valid_async_result_uri(uri: &str) -> bool {
    !uri.is_empty()
        && uri
            .bytes()
            .all(|byte| (0x21..=0x7e).contains(&byte) && byte != b'"' && byte != b'\\')
        && uri.len() <= MAX_URI_BYTES
}

fn valid_reservation_token(token: &str) -> bool {
    bounded(token, MAX_RESERVATION_TOKEN_BYTES)
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn state_key(query_id: &str) -> Option<String> {
    (bounded(query_id, MAX_QUERY_ID_BYTES)
        && query_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'))
    .then(|| format!("{STATE_KEY_PREFIX}{query_id}"))
}

fn tenant_index_key(tenant_id: &str) -> Option<String> {
    tenant_digest(b"lake-query-async-tenant-resource-v1\0", tenant_id)
        .map(|digest| format!("{TENANT_KEY_PREFIX}{digest}"))
}

fn execution_tenant_digest(tenant_id: &str) -> Option<String> {
    tenant_digest(b"lake-query-async-execution-v1\0", tenant_id)
}

fn tenant_digest(domain: &[u8], tenant_id: &str) -> Option<String> {
    if !bounded(tenant_id, MAX_TENANT_BYTES) {
        return None;
    }
    let mut context = digest::Context::new(&digest::SHA256);
    context.update(domain);
    context.update(tenant_id.as_bytes());
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = context.finish();
    let encoded = digest.as_ref().iter().fold(
        String::with_capacity(digest::SHA256_OUTPUT_LEN * 2),
        |mut output, byte| {
            output.push(char::from(HEX[usize::from(*byte >> 4)]));
            output.push(char::from(HEX[usize::from(*byte & 0x0f)]));
            output
        },
    );
    Some(encoded)
}

fn decode_tenant_index(
    encoded: Option<&[u8]>,
) -> Result<TenantResourceIndex, AsyncQueryStoreError> {
    let Some(encoded) = encoded else {
        return Ok(TenantResourceIndex {
            schema_version: 1,
            entries:        Vec::new(),
        });
    };
    if encoded.len() > MAX_TENANT_INDEX_BYTES {
        return Err(AsyncQueryStoreError::RecordTooLarge);
    }
    let index: TenantResourceIndex = serde_json::from_slice(encoded)
        .map_err(|source| AsyncQueryStoreError::Decode { source })?;
    if index.schema_version != 1
        || index.entries.len() > MAX_OUTSTANDING_PER_TENANT
        || index
            .entries
            .iter()
            .any(|entry| state_key(&entry.query_id).is_none() || entry.expires_at == 0)
        || index
            .entries
            .iter()
            .any(|entry| !valid_reservation_token(&entry.token))
        || index.entries.iter().enumerate().any(|(position, entry)| {
            index.entries[..position]
                .iter()
                .any(|other| other.query_id == entry.query_id)
        })
    {
        return Err(AsyncQueryStoreError::InvalidStateRecord);
    }
    Ok(index)
}

fn encode_tenant_index(index: &TenantResourceIndex) -> Result<Vec<u8>, AsyncQueryStoreError> {
    if index.schema_version != 1 || index.entries.len() > MAX_OUTSTANDING_PER_TENANT {
        return Err(AsyncQueryStoreError::InvalidStateRecord);
    }
    let encoded =
        serde_json::to_vec(index).map_err(|source| AsyncQueryStoreError::Encode { source })?;
    if encoded.len() > MAX_TENANT_INDEX_BYTES {
        return Err(AsyncQueryStoreError::RecordTooLarge);
    }
    Ok(encoded)
}

fn decode_execution_index(
    encoded: Option<&[u8]>,
) -> Result<ExecutionLeaseIndex, AsyncQueryStoreError> {
    let Some(encoded) = encoded else {
        return Ok(ExecutionLeaseIndex {
            schema_version: 1,
            entries:        Vec::new(),
        });
    };
    if encoded.len() > MAX_EXECUTION_INDEX_BYTES {
        return Err(AsyncQueryStoreError::RecordTooLarge);
    }
    let index: ExecutionLeaseIndex = serde_json::from_slice(encoded)
        .map_err(|source| AsyncQueryStoreError::Decode { source })?;
    if index.schema_version != 1
        || index.entries.len() > MAX_CLUSTER_EXECUTIONS
        || index.entries.iter().any(|entry| {
            state_key(&entry.query_id).is_none()
                || !valid_tenant_digest(&entry.tenant_digest)
                || !valid_reservation_token(&entry.token)
                || entry.expires_at == 0
        })
        || index.entries.iter().enumerate().any(|(position, entry)| {
            index.entries[..position]
                .iter()
                .any(|other| other.query_id == entry.query_id)
        })
    {
        return Err(AsyncQueryStoreError::InvalidStateRecord);
    }
    Ok(index)
}

fn encode_execution_index(index: &ExecutionLeaseIndex) -> Result<Vec<u8>, AsyncQueryStoreError> {
    if index.schema_version != 1 || index.entries.len() > MAX_CLUSTER_EXECUTIONS {
        return Err(AsyncQueryStoreError::InvalidStateRecord);
    }
    let encoded =
        serde_json::to_vec(index).map_err(|source| AsyncQueryStoreError::Encode { source })?;
    if encoded.len() > MAX_EXECUTION_INDEX_BYTES {
        return Err(AsyncQueryStoreError::RecordTooLarge);
    }
    Ok(encoded)
}

fn valid_tenant_digest(value: &str) -> bool {
    value.len() == digest::SHA256_OUTPUT_LEN * 2
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn encode_record(record: &AsyncQueryRecord) -> Result<Vec<u8>, AsyncQueryStoreError> {
    let encoded =
        serde_json::to_vec(record).map_err(|source| AsyncQueryStoreError::Encode { source })?;
    if encoded.len() > MAX_STATE_RECORD_BYTES {
        return Err(AsyncQueryStoreError::RecordTooLarge);
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use std::{
        io::Cursor,
        ops::Range,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{Duration, UNIX_EPOCH},
    };

    use async_trait::async_trait;
    use datafusion::arrow::{array::Int64Array, ipc::reader::StreamReader};
    use lake_common::{Principal, PrincipalId, PrincipalRole, TenantId};
    use lake_engine_lance::LanceEngine;
    use lake_meta::{MetaScanPage, MetaStore, MetaStoreRef, RocksMeta};
    use lake_objects::{
        LocalObjectStore, ManagedObjectStore, ObjectError, ObjectReader, Result as ObjectResult,
    };
    use tokio::io::AsyncReadExt;

    use super::{
        ASYNC_MANIFEST_CONTENT_TYPE, ASYNC_PART_CONTENT_TYPE, AsyncQueryCoordinator,
        AsyncQueryRecord, AsyncQueryState, AsyncQueryStore, AsyncQueryStoreError,
        AsyncQueryTransitionError, AsyncQueryWorker, AsyncResourceLimits, MAX_QUERY_ID_BYTES,
        MAX_RESULT_BYTES, MAX_RESULT_MANIFEST_BYTES, MAX_RESULT_MANIFEST_STRUCTURE_BYTES,
        MAX_RESULT_PART_BYTES, MAX_RESULT_PARTS, MAX_RESULT_SCHEMA_BYTES, MAX_URI_BYTES,
        MAX_WORKER_LEASE_SECS, MIN_CONFIG_RESULT_BYTES, STATE_KEY_PREFIX, WorkerIdentity,
    };
    use crate::{
        QueryEngine, QueryTicketKeyRing,
        async_scheduler::{AsyncCandidate, AsyncScheduler, AsyncSchedulerLimits},
        ticket::StatementTicket,
    };

    const TEST_RESERVATION_TOKEN: &str = "018f73b1-12b0-7d20-b8ab-8195ce8bfe01";

    struct ScanCountingMeta {
        inner: RocksMeta,
        gets:  Arc<AtomicUsize>,
    }

    /// Simulates an old replica winning the state-record create after a new
    /// replica has reserved quota, but before it can create schema-v2 state.
    struct V1CreateRaceMeta {
        inner:    RocksMeta,
        injected: AtomicBool,
    }

    struct NoReadObjectStore {
        opens: AtomicUsize,
    }

    #[async_trait]
    impl ManagedObjectStore for NoReadObjectStore {
        async fn put_reader(
            &self,
            _input: ObjectReader,
            _content_type: String,
        ) -> ObjectResult<lake_common::DataLocation> {
            panic!("manifest declaration rejection must not upload")
        }

        async fn open_reader(
            &self,
            _location: &lake_common::DataLocation,
        ) -> ObjectResult<ObjectReader> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Err(ObjectError::ScopedWriteUnsupported)
        }

        async fn open_range(
            &self,
            _location: &lake_common::DataLocation,
            _range: Range<u64>,
        ) -> ObjectResult<ObjectReader> {
            panic!("manifest declaration rejection must not open ranges")
        }
    }

    #[async_trait]
    impl MetaStore for ScanCountingMeta {
        async fn get(&self, key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.inner.get(key).await
        }

        async fn cas(
            &self,
            key: &str,
            expected: Option<&[u8]>,
            new: &[u8],
        ) -> lake_meta::Result<bool> {
            self.inner.cas(key, expected, new).await
        }

        async fn list_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<String>> {
            self.inner.list_prefix(prefix).await
        }

        async fn scan_prefix_page(
            &self,
            prefix: &str,
            continuation: Option<&str>,
            limit: usize,
        ) -> lake_meta::Result<MetaScanPage> {
            self.inner
                .scan_prefix_page(prefix, continuation, limit)
                .await
        }

        async fn delete(&self, key: &str, expected: &[u8]) -> lake_meta::Result<bool> {
            self.inner.delete(key, expected).await
        }
    }

    #[async_trait]
    impl MetaStore for V1CreateRaceMeta {
        async fn get(&self, key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
            self.inner.get(key).await
        }

        async fn cas(
            &self,
            key: &str,
            expected: Option<&[u8]>,
            new: &[u8],
        ) -> lake_meta::Result<bool> {
            if key.starts_with(STATE_KEY_PREFIX)
                && expected.is_none()
                && !self.injected.swap(true, Ordering::AcqRel)
            {
                let v2: AsyncQueryRecord =
                    serde_json::from_slice(new).expect("coordinator writes schema-v2 state");
                let v1 = AsyncQueryRecord::try_new(
                    v2.query_id,
                    v2.tenant_id,
                    v2.principal_id,
                    v2.job_spec,
                    v2.created_at,
                    v2.expires_at,
                )
                .expect("old replica writes a valid schema-v1 state record");
                let encoded = serde_json::to_vec(&v1).expect("encode schema-v1 state");
                assert!(
                    self.inner.cas(key, None, &encoded).await?,
                    "the simulated old replica wins the create race"
                );
                return Ok(false);
            }
            self.inner.cas(key, expected, new).await
        }

        async fn list_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<String>> {
            self.inner.list_prefix(prefix).await
        }

        async fn scan_prefix_page(
            &self,
            prefix: &str,
            continuation: Option<&str>,
            limit: usize,
        ) -> lake_meta::Result<MetaScanPage> {
            self.inner
                .scan_prefix_page(prefix, continuation, limit)
                .await
        }

        async fn delete(&self, key: &str, expected: &[u8]) -> lake_meta::Result<bool> {
            self.inner.delete(key, expected).await
        }
    }

    fn job_spec() -> lake_common::DataLocation {
        lake_common::DataLocation {
            uri:          "s3://async-results/tenant-a/query/job-spec".to_owned(),
            content_type: "application/vnd.lake.async-job".to_owned(),
            size_bytes:   512,
            sha256:       "ab".repeat(32),
        }
    }

    fn result_manifest() -> lake_common::DataLocation {
        lake_common::DataLocation {
            uri:          "s3://async-results/tenant-a/query/manifest/result.json".to_owned(),
            content_type: ASYNC_MANIFEST_CONTENT_TYPE.to_owned(),
            size_bytes:   512,
            sha256:       "cd".repeat(32),
        }
    }

    fn principal(id: &str) -> Principal {
        Principal::try_new(
            PrincipalId::try_new(id).unwrap(),
            TenantId::try_new("tenant-a").unwrap(),
            PrincipalRole::User,
            ["tenant-a"],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn async_result_manifest_rejects_part_sized_location_before_read() {
        let state_directory = tempfile::tempdir().unwrap();
        let state: MetaStoreRef = Arc::new(RocksMeta::open(state_directory.path()).unwrap());
        let objects = Arc::new(NoReadObjectStore {
            opens: AtomicUsize::new(0),
        });
        let coordinator = AsyncQueryCoordinator::try_new(
            state,
            objects.clone(),
            QueryTicketKeyRing::try_new(
                b"async-manifest-bound-ticket-key-00001",
                std::iter::empty(),
            )
            .unwrap(),
            Duration::from_hours(6),
            Duration::from_mins(5),
        )
        .unwrap();
        let mut record = AsyncQueryRecord::try_new(
            "0198f73b-12b0-7d20-b8ab-8195ce8bfe80",
            "tenant-a",
            "reader@example",
            job_spec(),
            1_000,
            2_000,
        )
        .unwrap();
        record.state = AsyncQueryState::Completed {
            manifest: lake_common::DataLocation {
                uri:          "s3://async-results/tenant-a/query/manifest/result.json".to_owned(),
                content_type: ASYNC_MANIFEST_CONTENT_TYPE.to_owned(),
                size_bytes:   MAX_RESULT_PART_BYTES,
                sha256:       "cd".repeat(32),
            },
            parts:    1,
            rows:     0,
            bytes:    1,
        };

        let error = coordinator
            .load_manifest(&record)
            .await
            .expect_err("part-sized manifest declaration is invalid before object I/O");

        assert!(matches!(
            error,
            super::AsyncQueryCoordinatorError::InvalidJobSpec
        ));
        assert_eq!(
            objects.opens.load(Ordering::SeqCst),
            0,
            "invalid declaration must not open the manifest object"
        );
    }

    #[test]
    fn async_result_manifest_rejects_json_escaped_uri_before_serialization() {
        let manifest = super::AsyncResultManifest {
            schema_version: 1,
            query_id:       "q".repeat(MAX_QUERY_ID_BYTES),
            schema_ipc:     vec![1],
            parts:          vec![lake_common::DataLocation {
                uri:          "\0".repeat(MAX_URI_BYTES),
                content_type: ASYNC_PART_CONTENT_TYPE.to_owned(),
                size_bytes:   1,
                sha256:       "ab".repeat(32),
            }],
            rows:           1,
            bytes:          1,
        };

        assert!(matches!(
            manifest.encode_for_publication(MAX_RESULT_BYTES),
            Err(super::AsyncQueryWorkerError::ResultBound)
        ));
    }

    #[test]
    fn async_result_manifest_maximum_json_safe_structure_fits_ceiling() {
        let part = lake_common::DataLocation {
            uri:          "x".repeat(MAX_URI_BYTES),
            content_type: ASYNC_PART_CONTENT_TYPE.to_owned(),
            size_bytes:   MAX_RESULT_PART_BYTES,
            sha256:       "ab".repeat(32),
        };
        let manifest = super::AsyncResultManifest {
            schema_version: 1,
            query_id:       "q".repeat(MAX_QUERY_ID_BYTES),
            schema_ipc:     vec![u8::MAX; MAX_RESULT_SCHEMA_BYTES],
            parts:          vec![part; MAX_RESULT_PARTS as usize],
            rows:           u64::MAX,
            bytes:          MAX_RESULT_PART_BYTES * MAX_RESULT_PARTS,
        };

        let encoded = manifest
            .encode_for_publication(MAX_RESULT_BYTES)
            .expect("maximum JSON-safe manifest structure remains publishable");

        assert!(
            manifest
                .validate(
                    &manifest.query_id,
                    MAX_RESULT_PARTS,
                    u64::MAX,
                    MAX_RESULT_PART_BYTES * MAX_RESULT_PARTS,
                    MAX_RESULT_BYTES,
                )
                .is_ok()
        );
        assert_eq!(MAX_RESULT_MANIFEST_STRUCTURE_BYTES, 21_684_406);
        assert!(encoded.len() as u64 <= MAX_RESULT_MANIFEST_STRUCTURE_BYTES);
        assert!(encoded.len() as u64 <= MAX_RESULT_MANIFEST_BYTES);
    }

    #[tokio::test]
    async fn async_resource_v1_records_remain_compatible() {
        let state_directory = tempfile::tempdir().unwrap();
        let state: MetaStoreRef = Arc::new(RocksMeta::open(state_directory.path()).unwrap());
        let catalog_directory = tempfile::tempdir().unwrap();
        let catalog: MetaStoreRef = Arc::new(RocksMeta::open(catalog_directory.path()).unwrap());
        let object_directory = tempfile::tempdir().unwrap();
        let objects = Arc::new(
            LocalObjectStore::open(object_directory.path())
                .await
                .unwrap(),
        );
        let coordinator = AsyncQueryCoordinator::try_new(
            state,
            objects,
            QueryTicketKeyRing::try_new(
                b"async-v1-compatibility-ticket-key-00001",
                std::iter::empty(),
            )
            .unwrap(),
            Duration::from_hours(6),
            Duration::from_mins(5),
        )
        .unwrap();
        let owner = principal("legacy@example");
        let job = coordinator
            .submit_statement(
                &StatementTicket {
                    sql:               "SELECT CAST(42 AS BIGINT) AS answer".to_owned(),
                    snapshots:         Vec::new(),
                    iceberg_snapshots: Vec::new(),
                },
                &owner,
            )
            .await
            .unwrap();
        let source = coordinator
            .store()
            .load(job.query_id())
            .await
            .unwrap()
            .expect("v2 job supplies a real encrypted job object");
        let query_id = "0198f73b-12b0-7d20-b8ab-8195ce8bfe70";
        let created_at = source.created_at;
        let expires_at = source.expires_at();
        let legacy = AsyncQueryRecord::try_new(
            query_id,
            "tenant-a",
            "legacy@example",
            source.job_spec().clone(),
            created_at,
            expires_at,
        )
        .expect("legacy queued record");
        let encoded = serde_json::to_vec(&legacy).expect("encode queued v1");
        let decoded: AsyncQueryRecord = serde_json::from_slice(&encoded).expect("decode queued v1");
        decoded.validate().expect("queued v1 remains valid");
        assert_eq!(decoded.result_limit_bytes(), MAX_RESULT_BYTES);
        assert!(!decoded.has_tenant_reservation());
        coordinator.store().create(decoded).await.unwrap();

        let poll_handle = coordinator.refresh_poll_handle(query_id, &owner).unwrap();
        assert_eq!(
            coordinator.open_poll_handle(&poll_handle, &owner).unwrap(),
            query_id,
            "current poll capability continues to address a v1 record"
        );
        let worker = AsyncQueryWorker::try_new(
            coordinator.clone(),
            Arc::new(QueryEngine::new(catalog, Arc::new(LanceEngine::new()))),
            WorkerIdentity::new([6; 16]),
            Duration::from_secs(30),
        )
        .unwrap();
        worker.run(query_id, Duration::from_secs(30)).await.unwrap();

        let completed = coordinator
            .store()
            .load(query_id)
            .await
            .unwrap()
            .expect("completed v1 state is loadable");
        assert!(completed.is_completed());
        let encoded = serde_json::to_vec(&completed).expect("encode completed v1");
        let decoded: AsyncQueryRecord =
            serde_json::from_slice(&encoded).expect("decode completed v1");
        decoded.validate().expect("completed v1 remains valid");
        assert_eq!(decoded.result_limit_bytes(), MAX_RESULT_BYTES);
        assert!(!decoded.has_tenant_reservation());

        assert!(
            !coordinator
                .cleanup_if_expired(query_id, expires_at)
                .await
                .unwrap(),
            "v1 cleanup first persists the normal cleanup fence"
        );
        let cleaning = coordinator
            .store()
            .load(query_id)
            .await
            .unwrap()
            .expect("cleaning v1 state is loadable");
        let encoded = serde_json::to_vec(&cleaning).expect("encode cleaning v1");
        let decoded: AsyncQueryRecord =
            serde_json::from_slice(&encoded).expect("decode cleaning v1");
        decoded.validate().expect("cleaning v1 remains valid");
        assert!(!decoded.has_tenant_reservation());
        assert!(
            coordinator
                .cleanup_if_expired(query_id, expires_at + MAX_WORKER_LEASE_SECS)
                .await
                .unwrap(),
            "current coordinator cleans a v1 record without a fabricated reservation"
        );
        assert!(coordinator.store().load(query_id).await.unwrap().is_none());
    }

    #[test]
    fn async_resource_limits_reject_values_outside_protocol_bounds() {
        assert!(AsyncResourceLimits::try_new(0, 16 << 30).is_err());
        assert!(AsyncResourceLimits::try_new(129, 16 << 30).is_err());
        assert!(AsyncResourceLimits::try_new(8, (64 << 20) - 1).is_err());
        assert!(AsyncResourceLimits::try_new(8, (256 << 30) + 1).is_err());
        assert!(AsyncResourceLimits::try_new(1, 64 << 20).is_ok());
        assert!(AsyncResourceLimits::try_new(128, 256 << 30).is_ok());
    }

    #[tokio::test]
    async fn async_tenant_quota_is_durable_and_isolated() {
        let directory = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(directory.path()).unwrap());
        let stores = [
            AsyncQueryStore::new(meta.clone()),
            AsyncQueryStore::new(meta.clone()),
            AsyncQueryStore::new(meta),
        ];
        let reservations = [
            ("0198f73b-12b0-7d20-b8ab-8195ce8bfe61", &stores[0]),
            ("0198f73b-12b0-7d20-b8ab-8195ce8bfe62", &stores[1]),
            ("0198f73b-12b0-7d20-b8ab-8195ce8bfe63", &stores[2]),
        ];
        let results = futures::future::join_all(reservations.map(|(query_id, store)| {
            store.reserve_tenant("tenant-a", query_id, TEST_RESERVATION_TOKEN, 1_000, 2)
        }))
        .await;

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 2);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(AsyncQueryStoreError::QuotaExceeded)))
                .count(),
            1
        );
        stores[0]
            .reserve_tenant(
                "tenant-b",
                "0198f73b-12b0-7d20-b8ab-8195ce8bfe64",
                TEST_RESERVATION_TOKEN,
                1_000,
                2,
            )
            .await
            .expect("another tenant has independent capacity");
    }

    #[tokio::test]
    async fn async_tenant_quota_reclaims_stale_reservations() {
        let directory = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(directory.path()).unwrap());
        let store = AsyncQueryStore::new(meta);
        let missing = "0198f73b-12b0-7d20-b8ab-8195ce8bfe65";
        store
            .reserve_tenant("tenant-a", missing, TEST_RESERVATION_TOKEN, 1_000, 1)
            .await
            .unwrap();
        store
            .reserve_tenant(
                "tenant-a",
                "0198f73b-12b0-7d20-b8ab-8195ce8bfe66",
                TEST_RESERVATION_TOKEN,
                1_301,
                1,
            )
            .await
            .expect("missing stale owner is reclaimed");

        let live = "0198f73b-12b0-7d20-b8ab-8195ce8bfe67";
        store
            .reserve_tenant("tenant-b", live, TEST_RESERVATION_TOKEN, 2_000, 1)
            .await
            .unwrap();
        store
            .create(
                AsyncQueryRecord::try_new_with_resources(
                    live,
                    "tenant-b",
                    "reader@example",
                    job_spec(),
                    2_000,
                    3_000,
                    TEST_RESERVATION_TOKEN,
                    AsyncResourceLimits::try_new(1, 64 << 20).unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let blocked = store
            .reserve_tenant(
                "tenant-b",
                "0198f73b-12b0-7d20-b8ab-8195ce8bfe68",
                TEST_RESERVATION_TOKEN,
                2_301,
                1,
            )
            .await;
        assert!(matches!(blocked, Err(AsyncQueryStoreError::QuotaExceeded)));
    }

    #[tokio::test]
    async fn cluster_execution_leases_are_bounded_and_durable() {
        let directory = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(directory.path()).unwrap());
        let stores = [
            AsyncQueryStore::new(meta.clone()),
            AsyncQueryStore::new(meta.clone()),
            AsyncQueryStore::new(meta),
        ];
        let limits = super::AsyncGlobalExecutionLimits::try_new(2, 1).unwrap();
        let reservations = [
            (
                "tenant-a",
                "0198f73b-12b0-7d20-b8ab-8195ce8bfe41",
                "018f73b1-12b0-7d20-b8ab-8195ce8bfe41",
                &stores[0],
            ),
            (
                "tenant-a",
                "0198f73b-12b0-7d20-b8ab-8195ce8bfe42",
                "018f73b1-12b0-7d20-b8ab-8195ce8bfe42",
                &stores[1],
            ),
            (
                "tenant-b",
                "0198f73b-12b0-7d20-b8ab-8195ce8bfe43",
                "018f73b1-12b0-7d20-b8ab-8195ce8bfe43",
                &stores[2],
            ),
        ];
        let results =
            futures::future::join_all(reservations.map(|(tenant, query_id, token, store)| {
                store.reserve_execution(tenant, query_id, token, 1_000, 30, limits)
            }))
            .await;

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 2);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(AsyncQueryStoreError::ExecutionCapacityHeld)))
                .count(),
            1
        );
        assert!(
            results[2].is_ok(),
            "a separate tenant remains eligible while tenant-a is at its shared limit"
        );
    }

    #[tokio::test]
    async fn cluster_execution_lease_cas_conflicts_are_bounded_and_live() {
        let directory = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(directory.path()).unwrap());
        let limits = super::AsyncGlobalExecutionLimits::try_new(64, 1).unwrap();
        let results = futures::future::join_all((0..32).map(|index| {
            let store = AsyncQueryStore::new(meta.clone());
            let tenant = format!("tenant-{index}");
            let query_id = format!("0198f73b-12b0-7d20-b8ab-{index:012x}");
            let token = format!("018f73b1-12b0-7d20-b8ab-{index:012x}");
            async move {
                store
                    .reserve_execution(&tenant, &query_id, &token, 1_000, 30, limits)
                    .await
            }
        }))
        .await;

        assert!(
            results.iter().all(Result::is_ok),
            "bounded retries must absorb one synchronized burst without dropping runnable jobs"
        );
    }

    #[tokio::test]
    async fn cluster_execution_leases_reclaim_expiry_and_fence_tokens() {
        let directory = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(directory.path()).unwrap());
        let store = AsyncQueryStore::new(meta);
        let limits = super::AsyncGlobalExecutionLimits::try_new(1, 1).unwrap();
        let stale_query = "0198f73b-12b0-7d20-b8ab-8195ce8bfe44";
        let stale_token = "018f73b1-12b0-7d20-b8ab-8195ce8bfe44";
        store
            .reserve_execution("tenant-a", stale_query, stale_token, 1_000, 30, limits)
            .await
            .unwrap();

        let successor_query = "0198f73b-12b0-7d20-b8ab-8195ce8bfe45";
        let successor_token = "018f73b1-12b0-7d20-b8ab-8195ce8bfe45";
        store
            .reserve_execution(
                "tenant-b",
                successor_query,
                successor_token,
                1_031,
                30,
                limits,
            )
            .await
            .expect("expiry reclaims a crashed owner");
        assert!(matches!(
            store
                .renew_execution(stale_query, stale_token, 1_031, 30)
                .await,
            Err(AsyncQueryStoreError::ExecutionLeaseLost)
        ));
        store
            .release_execution(stale_query, stale_token, 1_031)
            .await
            .expect("stale release is exact and cannot remove the successor");
        assert!(matches!(
            store
                .reserve_execution(
                    "tenant-c",
                    "0198f73b-12b0-7d20-b8ab-8195ce8bfe46",
                    "018f73b1-12b0-7d20-b8ab-8195ce8bfe46",
                    1_031,
                    30,
                    limits,
                )
                .await,
            Err(AsyncQueryStoreError::ExecutionCapacityHeld)
        ));
    }

    #[tokio::test]
    async fn cluster_execution_capacity_saturation_keeps_job_pending() {
        let directory = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(directory.path()).unwrap());
        let store = AsyncQueryStore::new(meta);
        let limits = super::AsyncGlobalExecutionLimits::try_new(1, 1).unwrap();
        let pending = "0198f73b-12b0-7d20-b8ab-8195ce8bfe47";
        store
            .create(
                AsyncQueryRecord::try_new(
                    pending,
                    "tenant-b",
                    "reader@example",
                    job_spec(),
                    1_000,
                    2_000,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        store
            .reserve_execution(
                "tenant-a",
                "0198f73b-12b0-7d20-b8ab-8195ce8bfe48",
                "018f73b1-12b0-7d20-b8ab-8195ce8bfe48",
                1_000,
                30,
                limits,
            )
            .await
            .unwrap();

        assert!(matches!(
            store
                .reserve_execution(
                    "tenant-b",
                    pending,
                    "018f73b1-12b0-7d20-b8ab-8195ce8bfe47",
                    1_000,
                    30,
                    limits,
                )
                .await,
            Err(AsyncQueryStoreError::ExecutionCapacityHeld)
        ));
        assert!(
            store
                .load(pending)
                .await
                .unwrap()
                .expect("record remains present")
                .is_pending(),
            "capacity pressure must not claim or terminally fail the job"
        );
    }

    #[tokio::test]
    async fn v1_record_reconciles_a_leaked_v2_reservation() {
        let directory = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(directory.path()).unwrap());
        let store = AsyncQueryStore::new(meta);
        let legacy = "0198f73b-12b0-7d20-b8ab-8195ce8bfe69";
        store
            .reserve_tenant("tenant-a", legacy, TEST_RESERVATION_TOKEN, 1_000, 1)
            .await
            .unwrap();
        store
            .create(
                AsyncQueryRecord::try_new(
                    legacy,
                    "tenant-a",
                    "legacy@example",
                    job_spec(),
                    1_000,
                    2_000,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        store
            .reserve_tenant(
                "tenant-a",
                "0198f73b-12b0-7d20-b8ab-8195ce8bfe6a",
                "018f73b1-12b0-7d20-b8ab-8195ce8bfe6a",
                1_301,
                1,
            )
            .await
            .expect("a v1 owner cannot permanently retain a v2 reservation");
    }

    #[tokio::test]
    async fn async_cleanup_releases_exact_tenant_reservation() {
        let directory = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(directory.path()).unwrap());
        let store = AsyncQueryStore::new(meta);
        let first = "0198f73b-12b0-7d20-b8ab-8195ce8bfe51";
        let second = "0198f73b-12b0-7d20-b8ab-8195ce8bfe52";
        let limits = AsyncResourceLimits::try_new(2, 64 << 20).unwrap();
        for query_id in [first, second] {
            store
                .reserve_tenant("tenant-a", query_id, TEST_RESERVATION_TOKEN, 1_000, 2)
                .await
                .unwrap();
            store
                .create(
                    AsyncQueryRecord::try_new_with_resources(
                        query_id,
                        "tenant-a",
                        "reader@example",
                        job_spec(),
                        1_000,
                        2_000,
                        TEST_RESERVATION_TOKEN,
                        limits,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            store
                .confirm_tenant("tenant-a", query_id, TEST_RESERVATION_TOKEN, 2_000)
                .await
                .unwrap();
        }
        store.begin_cleanup(first, 2_000).await.unwrap();
        store.delete_cleaning(first).await.unwrap();
        store
            .release_tenant("tenant-a", first, TEST_RESERVATION_TOKEN)
            .await
            .unwrap();
        store
            .release_tenant("tenant-a", first, TEST_RESERVATION_TOKEN)
            .await
            .unwrap();

        store
            .reserve_tenant(
                "tenant-a",
                "0198f73b-12b0-7d20-b8ab-8195ce8bfe53",
                TEST_RESERVATION_TOKEN,
                2_001,
                2,
            )
            .await
            .expect("only the cleaned reservation is released");
        let neighbor = store.load(second).await.unwrap().expect("neighbor record");
        assert!(neighbor.has_tenant_reservation());
    }

    #[tokio::test]
    async fn async_cleanup_release_token_fences_deterministic_resubmission_aba() {
        let directory = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(directory.path()).unwrap());
        let store = AsyncQueryStore::new(meta);
        let query_id = super::submission_query_id(&principal("aba@example"), [12; 16]);
        let limits = AsyncResourceLimits::try_new(1, 64 << 20).unwrap();
        let old_token = "018f73b1-12b0-7d20-b8ab-8195ce8bfe51";
        let new_token = "018f73b1-12b0-7d20-b8ab-8195ce8bfe52";

        store
            .reserve_tenant("tenant-a", &query_id, old_token, 1_000, 1)
            .await
            .unwrap();
        store
            .create(
                AsyncQueryRecord::try_new_with_resources(
                    &query_id,
                    "tenant-a",
                    "aba@example",
                    job_spec(),
                    1_000,
                    2_000,
                    old_token,
                    limits,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        store
            .confirm_tenant("tenant-a", &query_id, old_token, 2_000)
            .await
            .unwrap();
        store.begin_cleanup(&query_id, 2_000).await.unwrap();
        store.delete_cleaning(&query_id).await.unwrap();

        store
            .reserve_tenant("tenant-a", &query_id, new_token, 2_001, 1)
            .await
            .expect("state deletion permits a fresh fenced reservation");
        store
            .create(
                AsyncQueryRecord::try_new_with_resources(
                    &query_id,
                    "tenant-a",
                    "aba@example",
                    job_spec(),
                    2_001,
                    3_001,
                    new_token,
                    limits,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        store
            .confirm_tenant("tenant-a", &query_id, new_token, 3_001)
            .await
            .unwrap();

        store
            .release_tenant("tenant-a", &query_id, old_token)
            .await
            .expect("old cleanup cannot release the new reservation");
        assert!(matches!(
            store
                .reserve_tenant(
                    "tenant-a",
                    "0198f73b-12b0-7d20-b8ab-8195ce8bfe50",
                    "018f73b1-12b0-7d20-b8ab-8195ce8bfe53",
                    2_002,
                    1,
                )
                .await,
            Err(AsyncQueryStoreError::QuotaExceeded)
        ));
    }

    #[tokio::test]
    async fn async_result_limit_is_immutable_across_worker_restart() {
        let state_directory = tempfile::tempdir().unwrap();
        let state: MetaStoreRef = Arc::new(RocksMeta::open(state_directory.path()).unwrap());
        let catalog_directory = tempfile::tempdir().unwrap();
        let catalog: MetaStoreRef = Arc::new(RocksMeta::open(catalog_directory.path()).unwrap());
        let object_directory = tempfile::tempdir().unwrap();
        let objects = Arc::new(
            LocalObjectStore::open(object_directory.path())
                .await
                .unwrap(),
        );
        let keys = QueryTicketKeyRing::try_new(
            b"async-restart-result-limit-ticket-key-001",
            std::iter::empty(),
        )
        .unwrap();
        let persisted_limit = MIN_CONFIG_RESULT_BYTES;
        let first = AsyncQueryCoordinator::try_new_with_resources(
            state.clone(),
            objects.clone(),
            keys.clone(),
            Duration::from_hours(6),
            Duration::from_mins(5),
            AsyncResourceLimits::try_new(8, persisted_limit).unwrap(),
        )
        .unwrap();
        let submission = first
            .submit_statement(
                &StatementTicket {
                    sql:               format!("SELECT repeat('x', {persisted_limit}) AS payload"),
                    snapshots:         Vec::new(),
                    iceberg_snapshots: Vec::new(),
                },
                &principal("restart-limit@example"),
            )
            .await
            .unwrap();
        assert_eq!(
            first
                .store()
                .load(submission.query_id())
                .await
                .unwrap()
                .expect("persisted job")
                .result_limit_bytes(),
            persisted_limit
        );

        let restarted = AsyncQueryCoordinator::try_new_with_resources(
            state,
            objects,
            keys,
            Duration::from_hours(6),
            Duration::from_mins(5),
            AsyncResourceLimits::try_new(8, persisted_limit * 2).unwrap(),
        )
        .unwrap();
        let worker = AsyncQueryWorker::try_new(
            restarted.clone(),
            Arc::new(QueryEngine::new(catalog, Arc::new(LanceEngine::new()))),
            WorkerIdentity::new([9; 16]),
            Duration::from_secs(30),
        )
        .unwrap();
        worker
            .run(submission.query_id(), Duration::from_secs(30))
            .await
            .expect_err("the restarted worker must use the persisted smaller ceiling");

        let record = restarted
            .store()
            .load(submission.query_id())
            .await
            .unwrap()
            .expect("failed record remains durable for polling and cleanup");
        assert_eq!(record.result_limit_bytes(), persisted_limit);
        assert_eq!(record.failure_code(), Some("execution_failed"));
        assert!(
            record.completed_manifest().is_none(),
            "a bounded failure must not publish the completion pointer"
        );
        let part_directory = object_directory
            .path()
            .join("tenant-a")
            .join(submission.query_id())
            .join("part");
        assert!(
            !part_directory.exists()
                || std::fs::read_dir(part_directory)
                    .expect("part directory is readable")
                    .next()
                    .is_none(),
            "the oversized IPC part was rejected before publication"
        );
    }

    #[test]
    fn async_query_state_machine_fences_workers_and_terminal_states() {
        let mut record = AsyncQueryRecord::try_new(
            "0198f73b-12b0-7d20-b8ab-8195ce8bfe73",
            "tenant-a",
            "alice@example",
            job_spec(),
            1_000,
            2_000,
        )
        .expect("bounded queued record");
        let first = record
            .claim(1_010, WorkerIdentity::new([1; 16]), 30)
            .expect("first worker claim");
        record
            .renew(&first, 1_020, 30)
            .expect("current worker renews its lease");
        assert!(matches!(
            record.claim(1_020, WorkerIdentity::new([2; 16]), 30),
            Err(AsyncQueryTransitionError::LeaseHeld)
        ));

        let second = record
            .claim(1_051, WorkerIdentity::new([2; 16]), 30)
            .expect("expired lease is taken over");
        assert!(second.epoch() > first.epoch());
        assert!(matches!(
            record.complete(&first, 1_052, result_manifest(), 2, 100, 4_096),
            Err(AsyncQueryTransitionError::StaleWorker)
        ));
        record
            .complete(&second, 1_052, result_manifest(), 2, 100, 4_096)
            .expect("current worker completes");
        assert!(record.is_completed());
        assert!(matches!(
            record.cancel(1_050),
            Err(AsyncQueryTransitionError::Terminal)
        ));
        assert!(
            record.is_completed(),
            "terminal state must remain immutable"
        );

        let mut failed = AsyncQueryRecord::try_new(
            "0198f73b-12b0-7d20-b8ab-8195ce8bfe74",
            "tenant-a",
            "alice@example",
            job_spec(),
            1_000,
            2_000,
        )
        .unwrap();
        let lease = failed
            .claim(1_010, WorkerIdentity::new([3; 16]), 30)
            .unwrap();
        failed
            .fail(&lease, 1_020, "execution_failed")
            .expect("current worker can publish bounded failure");
        assert!(failed.is_failed());
        assert!(matches!(
            failed.cancel(1_021),
            Err(AsyncQueryTransitionError::Terminal)
        ));

        let mut expired = AsyncQueryRecord::try_new(
            "0198f73b-12b0-7d20-b8ab-8195ce8bfe75",
            "tenant-a",
            "alice@example",
            job_spec(),
            1_000,
            2_000,
        )
        .unwrap();
        expired.expire(2_000).expect("deadline expires queued job");
        assert!(expired.is_expired());
    }

    #[test]
    fn async_timeout_state_fences_stale_worker_completion() {
        let mut record = AsyncQueryRecord::try_new(
            "0198f73b-12b0-7d20-b8ab-8195ce8bfe76",
            "tenant-a",
            "alice@example",
            job_spec(),
            1_000,
            2_000,
        )
        .unwrap();
        let lease = record
            .claim(1_010, WorkerIdentity::new([4; 16]), 30)
            .unwrap();
        record.fail(&lease, 1_020, "execution_timeout").unwrap();

        assert_eq!(record.failure_code(), Some("execution_timeout"));
        assert!(matches!(
            record.renew(&lease, 1_021, 30),
            Err(AsyncQueryTransitionError::Terminal)
        ));
        assert!(matches!(
            record.complete(&lease, 1_021, result_manifest(), 1, 1, 1),
            Err(AsyncQueryTransitionError::Terminal)
        ));
        assert_eq!(record.failure_code(), Some("execution_timeout"));
    }

    #[tokio::test]
    async fn async_scheduler_uses_bounded_scan_records_without_point_reads() {
        let directory = tempfile::tempdir().unwrap();
        let gets = Arc::new(AtomicUsize::new(0));
        let meta: MetaStoreRef = Arc::new(ScanCountingMeta {
            inner: RocksMeta::open(directory.path()).unwrap(),
            gets:  gets.clone(),
        });
        let store = AsyncQueryStore::new(meta);
        for (query_id, tenant) in [
            ("0198f73b-12b0-7d20-b8ab-8195ce8bfe77", "tenant-a"),
            ("0198f73b-12b0-7d20-b8ab-8195ce8bfe78", "tenant-b"),
        ] {
            store
                .create(
                    AsyncQueryRecord::try_new(
                        query_id,
                        tenant,
                        "reader@example",
                        job_spec(),
                        1_000,
                        2_000,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        let terminal_id = "0198f73b-12b0-7d20-b8ab-8195ce8bfe78";
        let lease = store
            .claim(terminal_id, 1_010, WorkerIdentity::new([5; 16]), 30)
            .await
            .unwrap();
        store
            .fail(terminal_id, &lease, 1_020, "execution_failed")
            .await
            .unwrap();
        assert!(
            store
                .meta
                .cas("async-query/corrupt", None, b"{")
                .await
                .unwrap()
        );
        gets.store(0, Ordering::Relaxed);

        let (records, invalid, continuation) = store.list_records_page(None).await.unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(
            records.iter().filter(|record| record.is_pending()).count(),
            1
        );
        assert!(continuation.is_none());
        assert_eq!(invalid, 1);
        assert_eq!(gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn async_query_store_allows_only_one_concurrent_worker_claim() {
        let directory = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(directory.path()).unwrap());
        let store = AsyncQueryStore::new(meta);
        let query_id = "0198f73b-12b0-7d20-b8ab-8195ce8bfe73";
        store
            .create(
                AsyncQueryRecord::try_new(
                    query_id,
                    "tenant-a",
                    "alice@example",
                    job_spec(),
                    1_000,
                    2_000,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let (first, second) = tokio::join!(
            store.claim(query_id, 1_010, WorkerIdentity::new([1; 16]), 30),
            store.claim(query_id, 1_010, WorkerIdentity::new([2; 16]), 30),
        );
        let results: [_; 2] = (first, second).into();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert!(results.iter().any(|result| matches!(
            result,
            Err(AsyncQueryStoreError::Conflict)
                | Err(AsyncQueryStoreError::Transition {
                    source: AsyncQueryTransitionError::LeaseHeld,
                })
        )));
        let lease = results
            .iter()
            .find_map(|result| result.as_ref().ok().copied())
            .expect("one winning lease");
        store.renew(query_id, &lease, 1_020, 30).await.unwrap();
        store
            .complete(query_id, &lease, 1_021, result_manifest(), 2, 100, 4_096)
            .await
            .unwrap();
        let loaded = store.load(query_id).await.unwrap().expect("stored query");
        assert!(loaded.is_completed());
        assert!(matches!(
            store.cancel(query_id, 1_022).await,
            Err(AsyncQueryStoreError::Transition {
                source: AsyncQueryTransitionError::Terminal,
            })
        ));
    }

    #[tokio::test]
    async fn async_query_submission_persists_scoped_encrypted_job() {
        let state_directory = tempfile::tempdir().unwrap();
        let state: MetaStoreRef = Arc::new(RocksMeta::open(state_directory.path()).unwrap());
        let object_directory = tempfile::tempdir().unwrap();
        let objects = Arc::new(
            LocalObjectStore::open(object_directory.path())
                .await
                .unwrap(),
        );
        let coordinator = AsyncQueryCoordinator::try_new(
            state,
            objects.clone(),
            QueryTicketKeyRing::try_new(
                b"async-query-ticket-key-material-000001",
                std::iter::empty(),
            )
            .unwrap(),
            Duration::from_mins(5),
            Duration::from_mins(5),
        )
        .unwrap();
        let alice = principal("alice@example");
        let encrypted_job = b"opaque-encrypted-pinned-statement".to_vec();

        let submission = coordinator
            .submit_at(
                encrypted_job.clone(),
                &alice,
                UNIX_EPOCH + Duration::from_secs(1_000),
            )
            .await
            .unwrap();

        assert_eq!(submission.expires_at(), 1_300);
        assert_eq!(
            coordinator
                .open_poll_handle(submission.poll_handle(), &alice)
                .unwrap(),
            submission.query_id()
        );
        assert!(
            coordinator
                .open_poll_handle(submission.poll_handle(), &principal("bob@example"))
                .is_err(),
            "poll capability is bound to the submitting identity"
        );
        let record = coordinator
            .store()
            .load(submission.query_id())
            .await
            .unwrap()
            .expect("durable state record");
        assert!(record.job_spec().uri.contains("/tenant-a/"));
        assert!(record.job_spec().uri.contains(submission.query_id()));
        assert!(record.job_spec().uri.contains("/job/"));
        let mut reader = objects.open_reader(record.job_spec()).await.unwrap();
        let mut stored = Vec::new();
        reader.read_to_end(&mut stored).await.unwrap();
        assert_eq!(stored, encrypted_job);
        assert!(
            !coordinator
                .cleanup_if_expired(submission.query_id(), 1_300)
                .await
                .unwrap(),
            "cleanup first publishes a fence and waits out every worker lease"
        );
        assert!(
            coordinator
                .cleanup_if_expired(submission.query_id(), 1_600)
                .await
                .unwrap()
        );
        assert!(
            coordinator
                .store()
                .load(submission.query_id())
                .await
                .unwrap()
                .is_none(),
            "cleanup deletes state only after its scoped objects"
        );
    }

    #[tokio::test]
    async fn async_result_manifest_publishes_only_after_bounded_parts() {
        let state_directory = tempfile::tempdir().unwrap();
        let state: MetaStoreRef = Arc::new(RocksMeta::open(state_directory.path()).unwrap());
        let catalog_directory = tempfile::tempdir().unwrap();
        let catalog: MetaStoreRef = Arc::new(RocksMeta::open(catalog_directory.path()).unwrap());
        let object_directory = tempfile::tempdir().unwrap();
        let objects = Arc::new(
            LocalObjectStore::open(object_directory.path())
                .await
                .unwrap(),
        );
        let coordinator = AsyncQueryCoordinator::try_new(
            state,
            objects.clone(),
            QueryTicketKeyRing::try_new(
                b"async-worker-ticket-key-material-000001",
                std::iter::empty(),
            )
            .unwrap(),
            Duration::from_hours(6),
            Duration::from_mins(5),
        )
        .unwrap();
        let submission = coordinator
            .submit_statement(
                &StatementTicket {
                    sql:               "SELECT CAST(42 AS BIGINT) AS answer".to_owned(),
                    snapshots:         Vec::new(),
                    iceberg_snapshots: Vec::new(),
                },
                &principal("worker-test@example"),
            )
            .await
            .unwrap();
        let engine = Arc::new(QueryEngine::new(catalog, Arc::new(LanceEngine::new())));
        let worker = AsyncQueryWorker::try_new(
            coordinator.clone(),
            engine,
            WorkerIdentity::new([7; 16]),
            Duration::from_secs(30),
        )
        .unwrap();

        worker
            .run(submission.query_id(), Duration::from_secs(30))
            .await
            .unwrap();

        let record = coordinator
            .store()
            .load(submission.query_id())
            .await
            .unwrap()
            .unwrap();
        let manifest_location = record
            .completed_manifest()
            .expect("manifest is the atomic completion pointer");
        let mut manifest_bytes = Vec::new();
        objects
            .open_reader(manifest_location)
            .await
            .unwrap()
            .read_to_end(&mut manifest_bytes)
            .await
            .unwrap();
        let manifest: super::AsyncResultManifest = serde_json::from_slice(&manifest_bytes).unwrap();
        assert_eq!(manifest.query_id, submission.query_id());
        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.rows, 1);
        let mut part_bytes = Vec::new();
        objects
            .open_reader(&manifest.parts[0])
            .await
            .unwrap()
            .read_to_end(&mut part_bytes)
            .await
            .unwrap();
        let batches = StreamReader::try_new(Cursor::new(part_bytes), None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 42);
    }

    #[tokio::test]
    async fn async_worker_deadline_fails_job_and_releases_tenant_capacity() {
        let state_directory = tempfile::tempdir().unwrap();
        let state: MetaStoreRef = Arc::new(RocksMeta::open(state_directory.path()).unwrap());
        let catalog_directory = tempfile::tempdir().unwrap();
        let catalog: MetaStoreRef = Arc::new(RocksMeta::open(catalog_directory.path()).unwrap());
        let object_directory = tempfile::tempdir().unwrap();
        let objects = Arc::new(
            LocalObjectStore::open(object_directory.path())
                .await
                .unwrap(),
        );
        let coordinator = AsyncQueryCoordinator::try_new(
            state,
            objects,
            QueryTicketKeyRing::try_new(
                b"async-deadline-ticket-key-material-0001",
                std::iter::empty(),
            )
            .unwrap(),
            Duration::from_hours(6),
            Duration::from_mins(5),
        )
        .unwrap();
        let submission = coordinator
            .submit_statement(
                &StatementTicket {
                    sql:               "SELECT CAST(42 AS BIGINT) AS answer".to_owned(),
                    snapshots:         Vec::new(),
                    iceberg_snapshots: Vec::new(),
                },
                &principal("deadline@example"),
            )
            .await
            .unwrap();
        let worker = AsyncQueryWorker::try_new(
            coordinator.clone(),
            Arc::new(QueryEngine::new(catalog, Arc::new(LanceEngine::new()))),
            WorkerIdentity::new([8; 16]),
            Duration::from_secs(30),
        )
        .unwrap();

        let error = worker
            .run(submission.query_id(), Duration::from_nanos(1))
            .await
            .expect_err("absolute deadline stops execution");
        assert!(matches!(
            error,
            super::AsyncQueryWorkerError::ExecutionDeadline
        ));
        let record = coordinator
            .store()
            .load(submission.query_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.failure_code(), Some("execution_timeout"));

        let limits = AsyncSchedulerLimits::try_new(1, 1, Duration::from_secs(1)).unwrap();
        let mut scheduler = AsyncScheduler::new(limits);
        let alpha = AsyncCandidate::new(submission.query_id(), "tenant-a");
        scheduler.started(&alpha);
        scheduler.finished(&alpha);
        assert_eq!(
            scheduler.select([AsyncCandidate::new("beta-query", "tenant-b")]),
            [AsyncCandidate::new("beta-query", "tenant-b")]
        );
    }

    #[tokio::test]
    async fn coordinator_submission_id_retries_converge_on_one_job() {
        let state_directory = tempfile::tempdir().unwrap();
        let state: MetaStoreRef = Arc::new(RocksMeta::open(state_directory.path()).unwrap());
        let object_directory = tempfile::tempdir().unwrap();
        let objects = Arc::new(
            LocalObjectStore::open(object_directory.path())
                .await
                .unwrap(),
        );
        let coordinator = AsyncQueryCoordinator::try_new(
            state,
            objects,
            QueryTicketKeyRing::try_new(
                b"async-resume-ticket-key-material-000001",
                std::iter::empty(),
            )
            .unwrap(),
            Duration::from_hours(6),
            Duration::from_mins(5),
        )
        .unwrap();
        let statement = StatementTicket {
            sql:               "SELECT 1".to_owned(),
            snapshots:         Vec::new(),
            iceberg_snapshots: Vec::new(),
        };
        let alice = principal("restart-safe@example");
        let submission_id = [3_u8; 16];

        let (first, retried) = tokio::join!(
            coordinator.submit_statement_with_id(&statement, &alice, submission_id),
            coordinator.submit_statement_with_id(&statement, &alice, submission_id),
        );
        let first = first.unwrap();
        let retried = retried.unwrap();

        assert_eq!(first.query_id(), retried.query_id());
        assert_eq!(
            coordinator
                .open_poll_handle(first.poll_handle(), &alice)
                .unwrap(),
            coordinator
                .open_poll_handle(retried.poll_handle(), &alice)
                .unwrap()
        );
        let (records, invalid, continuation) =
            coordinator.store().list_records_page(None).await.unwrap();
        let query_ids = records
            .iter()
            .map(AsyncQueryRecord::query_id)
            .collect::<Vec<_>>();
        assert_eq!(query_ids, [first.query_id()]);
        assert!(continuation.is_none());
        assert_eq!(invalid, 0);
    }

    #[tokio::test]
    async fn v1_create_race_releases_new_reservation_before_idempotent_resume() {
        let state_directory = tempfile::tempdir().unwrap();
        let state = Arc::new(V1CreateRaceMeta {
            inner:    RocksMeta::open(state_directory.path()).unwrap(),
            injected: AtomicBool::new(false),
        });
        let object_directory = tempfile::tempdir().unwrap();
        let objects = Arc::new(
            LocalObjectStore::open(object_directory.path())
                .await
                .unwrap(),
        );
        let coordinator = AsyncQueryCoordinator::try_new_with_resources(
            state.clone(),
            objects,
            QueryTicketKeyRing::try_new(
                b"async-v1-create-race-ticket-key-material-01",
                std::iter::empty(),
            )
            .unwrap(),
            Duration::from_hours(6),
            Duration::from_mins(5),
            AsyncResourceLimits::try_new(1, MIN_CONFIG_RESULT_BYTES).unwrap(),
        )
        .unwrap();
        let statement = StatementTicket {
            sql:               "SELECT CAST(42 AS BIGINT) AS answer".to_owned(),
            snapshots:         Vec::new(),
            iceberg_snapshots: Vec::new(),
        };
        let owner = principal("v1-race@example");
        let submission_id = [5_u8; 16];
        let resumed = coordinator
            .submit_statement_with_id(&statement, &owner, submission_id)
            .await
            .expect("a valid v1 winner remains resumable");
        let query_id = super::submission_query_id(&owner, submission_id);
        assert!(state.injected.load(Ordering::Acquire));
        assert_eq!(resumed.query_id(), query_id);
        let legacy = coordinator
            .store()
            .load(&query_id)
            .await
            .unwrap()
            .expect("the old replica's state record persists");
        assert!(!legacy.has_tenant_reservation());

        coordinator
            .store()
            .reserve_tenant(
                "tenant-a",
                "0198f73b-12b0-7d20-b8ab-8195ce8bfe6f",
                "018f73b1-12b0-7d20-b8ab-8195ce8bfe6f",
                legacy.created_at,
                1,
            )
            .await
            .expect("the losing v2 reservation must not poison tenant capacity");
    }

    #[tokio::test]
    async fn async_submission_id_rejects_statement_alias() {
        let state_directory = tempfile::tempdir().unwrap();
        let state: MetaStoreRef = Arc::new(RocksMeta::open(state_directory.path()).unwrap());
        let object_directory = tempfile::tempdir().unwrap();
        let objects = Arc::new(
            LocalObjectStore::open(object_directory.path())
                .await
                .unwrap(),
        );
        let coordinator = AsyncQueryCoordinator::try_new(
            state,
            objects,
            QueryTicketKeyRing::try_new(
                b"async-alias-ticket-key-material-000001",
                std::iter::empty(),
            )
            .unwrap(),
            Duration::from_hours(6),
            Duration::from_mins(5),
        )
        .unwrap();
        let original = StatementTicket {
            sql:               "SELECT 1".to_owned(),
            snapshots:         Vec::new(),
            iceberg_snapshots: Vec::new(),
        };
        let replacement = StatementTicket {
            sql:               "SELECT 2".to_owned(),
            snapshots:         Vec::new(),
            iceberg_snapshots: Vec::new(),
        };
        let alice = principal("alias-safe@example");
        let submission_id = [4_u8; 16];
        let submitted = coordinator
            .submit_statement_with_id(&original, &alice, submission_id)
            .await
            .unwrap();

        assert!(matches!(
            coordinator
                .submit_statement_with_id(&replacement, &alice, submission_id)
                .await,
            Err(super::AsyncQueryCoordinatorError::SubmissionConflict)
        ));
        let record = coordinator
            .store()
            .load(submitted.query_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(coordinator.open_job(&record).await.unwrap(), original);
    }
}
