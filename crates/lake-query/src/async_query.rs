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
use datafusion::arrow::{
    ipc::writer::{IpcWriteOptions, StreamWriter},
    record_batch::RecordBatch,
};
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
const MAX_JOB_LIFETIME_SECS: u64 = 24 * 60 * 60;
const MAX_WORKER_LEASE_SECS: u64 = 5 * 60;
const MAX_RESULT_PARTS: u64 = 4_096;
const MAX_RESULT_BYTES: u64 = 1 << 40;
const MAX_RESULT_PART_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RESULT_PART_ROWS: usize = 65_536;
const MAX_RESULT_SCHEMA_BYTES: usize = 1024 * 1024;
const MAX_STATE_RECORD_BYTES: usize = 16 * 1024;
const SCAN_PAGE_JOBS: usize = 256;
const STATE_KEY_PREFIX: &str = "async-query/";
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
}

impl AsyncQueryCoordinator {
    pub(crate) fn try_new(
        state: MetaStoreRef,
        objects: Arc<dyn ManagedObjectStore>,
        keys: QueryTicketKeyRing,
        job_lifetime: Duration,
        poll_ttl: Duration,
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
        match self
            .submit_with_query_id_at(&query_id, encrypted_job, principal, SystemTime::now())
            .await
        {
            Ok(submission) => Ok(submission),
            Err(AsyncQueryCoordinatorError::Store {
                source: AsyncQueryStoreError::AlreadyExists,
            }) => self
                .resume_submission_with_id(&statement.sql, principal, submission_id)
                .await?
                .ok_or(AsyncQueryCoordinatorError::SubmissionConflict),
            Err(error) => Err(error),
        }
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
        let record = AsyncQueryRecord::try_new(
            query_id,
            principal.tenant().as_str(),
            principal.subject(),
            job_spec,
            now,
            expires_at,
        )
        .map_err(|source| AsyncQueryCoordinatorError::Store {
            source: AsyncQueryStoreError::Transition { source },
        })?;
        self.store
            .create(record)
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
                    sql:       format!("{query_id}:{part}"),
                    snapshots: Vec::new(),
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
                    sql:       query_id.to_owned(),
                    snapshots: Vec::new(),
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
        let capacity = usize::try_from(location.size_bytes)
            .map_err(|_| AsyncQueryCoordinatorError::InvalidJobSpec)?;
        if capacity == 0 || capacity as u64 > MAX_RESULT_PART_BYTES {
            return Err(AsyncQueryCoordinatorError::InvalidJobSpec);
        }
        let reader = open_verified(self.objects.as_ref(), location)
            .await
            .map_err(|source| AsyncQueryCoordinatorError::Object { source })?;
        let mut encoded = Vec::with_capacity(capacity);
        reader
            .take(MAX_RESULT_PART_BYTES + 1)
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
        manifest.validate(&record.query_id, parts, rows, bytes)?;
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
    ) -> Result<(), AsyncQueryCoordinatorError> {
        if self.schema_version != 1
            || self.query_id != query_id
            || self.schema_ipc.is_empty()
            || self.schema_ipc.len() > MAX_RESULT_SCHEMA_BYTES
            || self.parts.len() as u64 != parts
            || self.rows != rows
            || self.bytes != bytes
            || self.parts.is_empty()
            || self.parts.len() as u64 > MAX_RESULT_PARTS
            || self.bytes > MAX_RESULT_BYTES
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
}

#[derive(Clone)]
pub(crate) struct AsyncQueryWorker {
    coordinator: AsyncQueryCoordinator,
    engine:      Arc<QueryEngine>,
    identity:    WorkerIdentity,
    lease:       Duration,
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
        })
    }

    pub(crate) async fn run(&self, query_id: &str) -> Result<(), AsyncQueryWorkerError> {
        let claimed_at = unix_now()?;
        let lease = self
            .coordinator
            .store()
            .claim(query_id, claimed_at, self.identity, self.lease.as_secs())
            .await
            .map_err(|source| AsyncQueryWorkerError::Store { source })?;
        let heartbeat_stop = tokio_util::sync::CancellationToken::new();
        let lease_lost = tokio_util::sync::CancellationToken::new();
        let heartbeat = tokio::spawn({
            let worker = self.clone();
            let query_id = query_id.to_owned();
            let heartbeat_stop = heartbeat_stop.clone();
            let lease_lost = lease_lost.clone();
            async move {
                let mut interval = tokio::time::interval(worker.lease / 3);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                interval.tick().await;
                loop {
                    tokio::select! {
                        () = heartbeat_stop.cancelled() => return,
                        _ = interval.tick() => {
                            if worker.renew(&query_id, &lease).await.is_err() {
                                lease_lost.cancel();
                                return;
                            }
                        }
                    }
                }
            }
        });
        let result = tokio::select! {
            result = self.run_claimed(query_id, &lease) => result,
            () = lease_lost.cancelled() => Err(AsyncQueryWorkerError::Store {
                source: AsyncQueryStoreError::Conflict,
            }),
        };
        heartbeat_stop.cancel();
        let _ = heartbeat.await;
        if result.is_err() {
            if let Ok(now) = unix_now() {
                let _ = self
                    .coordinator
                    .store()
                    .fail(query_id, &lease, now, "execution_failed")
                    .await;
            }
        }
        result
    }

    async fn run_claimed(
        &self,
        query_id: &str,
        lease: &WorkerLease,
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
        let snapshots = statement
            .snapshots
            .iter()
            .map(job_snapshot)
            .collect::<Vec<_>>();
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
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(|source| AsyncQueryWorkerError::Query {
                source: QueryError::Execute { source },
            })?;
            rows = rows
                .checked_add(batch.num_rows() as u64)
                .ok_or(AsyncQueryWorkerError::ResultBound)?;
            for offset in (0..batch.num_rows()).step_by(MAX_RESULT_PART_ROWS) {
                let length = (batch.num_rows() - offset).min(MAX_RESULT_PART_ROWS);
                self.write_part(&scope, &batch.slice(offset, length), &mut parts, &mut bytes)
                    .await?;
                self.renew(query_id, lease).await?;
            }
        }
        if parts.is_empty() {
            let empty = RecordBatch::new_empty(schema);
            self.write_part(&scope, &empty, &mut parts, &mut bytes)
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
        let encoded = serde_json::to_vec(&manifest)
            .map_err(|source| AsyncQueryWorkerError::Manifest { source })?;
        if encoded.is_empty() || encoded.len() as u64 > MAX_RESULT_PART_BYTES {
            return Err(AsyncQueryWorkerError::ResultBound);
        }
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
        batch: &RecordBatch,
        parts: &mut Vec<DataLocation>,
        total_bytes: &mut u64,
    ) -> Result<(), AsyncQueryWorkerError> {
        if parts.len() as u64 >= MAX_RESULT_PARTS {
            return Err(AsyncQueryWorkerError::ResultBound);
        }
        let mut encoded = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut encoded, &batch.schema())
                .map_err(|source| AsyncQueryWorkerError::Arrow { source })?;
            writer
                .write(batch)
                .map_err(|source| AsyncQueryWorkerError::Arrow { source })?;
            writer
                .finish()
                .map_err(|source| AsyncQueryWorkerError::Arrow { source })?;
        }
        let part_bytes = encoded.len() as u64;
        let next_total = total_bytes
            .checked_add(part_bytes)
            .ok_or(AsyncQueryWorkerError::ResultBound)?;
        if part_bytes == 0 || part_bytes > MAX_RESULT_PART_BYTES || next_total > MAX_RESULT_BYTES {
            return Err(AsyncQueryWorkerError::ResultBound);
        }
        let input =
            AsyncStreamReader::new(stream::iter([Ok::<Bytes, io::Error>(Bytes::from(encoded))]));
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
        *total_bytes = next_total;
        parts.push(location);
        Ok(())
    }

    async fn renew(
        &self,
        query_id: &str,
        lease: &WorkerLease,
    ) -> Result<(), AsyncQueryWorkerError> {
        self.coordinator
            .store()
            .renew(query_id, lease, unix_now()?, self.lease.as_secs())
            .await
            .map_err(|source| AsyncQueryWorkerError::Store { source })
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

    pub(crate) async fn load(
        &self,
        query_id: &str,
    ) -> Result<Option<AsyncQueryRecord>, AsyncQueryStoreError> {
        self.load_encoded(query_id)
            .await
            .map(|loaded| loaded.map(|(_, record)| record))
    }

    pub(crate) async fn list_query_ids_page(
        &self,
        continuation: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), AsyncQueryStoreError> {
        let page = self
            .meta
            .scan_prefix_page(STATE_KEY_PREFIX, continuation, SCAN_PAGE_JOBS)
            .await
            .map_err(|source| AsyncQueryStoreError::Meta { source })?;
        let (entries, continuation) = page.into_parts();
        let query_ids = entries
            .into_iter()
            .map(|(query_id, _)| query_id)
            .map(|query_id| {
                state_key(&query_id)
                    .filter(|expected| expected == &format!("{STATE_KEY_PREFIX}{query_id}"))
                    .map(|_| query_id)
                    .ok_or(AsyncQueryStoreError::InvalidStateRecord)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((query_ids, continuation))
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

impl AsyncQueryRecord {
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
        })
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
        if !valid_result_location(&manifest, ASYNC_MANIFEST_CONTENT_TYPE)
            || parts == 0
            || parts > MAX_RESULT_PARTS
            || bytes > MAX_RESULT_BYTES
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

    fn validate(&self) -> Result<(), AsyncQueryTransitionError> {
        if self.schema_version != 1
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
            } if valid_result_location(manifest, ASYNC_MANIFEST_CONTENT_TYPE)
                && *parts > 0
                && *parts <= MAX_RESULT_PARTS
                && *bytes <= MAX_RESULT_BYTES =>
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
    bounded(&location.uri, MAX_URI_BYTES)
        && location.content_type == content_type
        && location.size_bytes > 0
        && location.size_bytes <= MAX_RESULT_PART_BYTES
        && location.sha256.len() == 64
        && location
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn state_key(query_id: &str) -> Option<String> {
    (bounded(query_id, MAX_QUERY_ID_BYTES)
        && query_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'))
    .then(|| format!("{STATE_KEY_PREFIX}{query_id}"))
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
        sync::Arc,
        time::{Duration, UNIX_EPOCH},
    };

    use datafusion::arrow::{array::Int64Array, ipc::reader::StreamReader};
    use lake_common::{Principal, PrincipalId, PrincipalRole, TenantId};
    use lake_engine_lance::LanceEngine;
    use lake_meta::{MetaStoreRef, RocksMeta};
    use lake_objects::LocalObjectStore;
    use tokio::io::AsyncReadExt;

    use super::{
        ASYNC_MANIFEST_CONTENT_TYPE, AsyncQueryCoordinator, AsyncQueryRecord, AsyncQueryStore,
        AsyncQueryStoreError, AsyncQueryTransitionError, AsyncQueryWorker, WorkerIdentity,
    };
    use crate::{QueryEngine, QueryTicketKeyRing, ticket::StatementTicket};

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
                    sql:       "SELECT CAST(42 AS BIGINT) AS answer".to_owned(),
                    snapshots: Vec::new(),
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

        worker.run(submission.query_id()).await.unwrap();

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
            sql:       "SELECT 1".to_owned(),
            snapshots: Vec::new(),
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
        let (query_ids, continuation) =
            coordinator.store().list_query_ids_page(None).await.unwrap();
        assert_eq!(query_ids, [first.query_id()]);
        assert!(continuation.is_none());
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
            sql:       "SELECT 1".to_owned(),
            snapshots: Vec::new(),
        };
        let replacement = StatementTicket {
            sql:       "SELECT 2".to_owned(),
            snapshots: Vec::new(),
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
