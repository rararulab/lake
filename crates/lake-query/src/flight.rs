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

//! Arrow Flight SQL wire surface over [`QueryEngine`].
//!
//! [`FlightSqlServiceImpl`] implements the two-phase Flight SQL statement path:
//! `GetFlightInfo` plans the SQL against exact table snapshots to publish its
//! Arrow schema and hands back an encrypted identity-bound capability;
//! `DoGet` reconstructs the same pinned catalog, executes the SQL, and streams
//! the resulting record batches back as Flight data. Only the statement path
//! is overridden — every other Flight SQL method keeps its trait default (an
//! `unimplemented` [`Status`]).

use std::{
    collections::{BTreeSet, HashMap},
    future::Future,
    ops::Bound::{Excluded, Unbounded},
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
};

use arrow_flight::{
    Action, ActionType, CancelFlightInfoRequest, CancelFlightInfoResult, CancelStatus, Criteria,
    Empty, FlightClient, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, Result as FlightResult, SchemaResult, Ticket,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::FlightService,
    sql::{
        Any, Command, CommandGetDbSchemas, CommandGetTables, CommandStatementQuery,
        ProstMessageExt, SqlInfo, TicketStatementQuery,
        server::{FlightSqlService, PeekableFlightDataStream},
    },
};
use datafusion::{
    arrow::{
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    common::{TableReference, config::Dialect},
    sql::{parser::Statement, sqlparser::ast::Statement as SqlStatement},
};
use futures::{Stream, StreamExt, TryStreamExt};
use lake_catalog::{CatalogGeneration, TableSnapshot};
use lake_common::{
    FILE_APPEND_TYPE_URL, FileAppendRequest, MANAGED_STAGE_DISCOVERY_ACTION,
    ManagedStageDescriptor, Namespace, Principal, TableLocation, TableName, TableRef, Version,
};
use lake_flight::{
    ClientSecurity, DELEGATED_NAMESPACE_HEADER, DELEGATED_TENANT_HEADER, TracedFlightStream,
    set_span_parent_from_request,
};
use prost::Message;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, Sleep},
};
use tonic::{Request, Response, Status, Streaming};
use tracing::{Instrument as _, Span, field};

use crate::{
    DiscoveryLimits, QueryEngine, QueryError, QueryLimits,
    async_ipc::{IpcDecodeGuard, IpcPipelineLimits, PipelineProbe, decode_ipc_reader},
    async_query::{AsyncQueryCoordinator, MAX_RESULT_PART_BYTES},
    telemetry,
    ticket::{MAX_TABLE_SNAPSHOTS, StatementTableSnapshot, StatementTicket, StatementTicketCodec},
};

const MAX_STATEMENT_TICKET_OVERHEAD: usize = 320 * 1024;

#[derive(Clone)]
pub(crate) struct QueryAdmission {
    semaphore: Arc<Semaphore>,
    tenants:   Arc<Mutex<HashMap<String, Weak<Semaphore>>>>,
    limits:    QueryLimits,
}

impl std::fmt::Debug for QueryAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryAdmission")
            .field("global", &self.semaphore)
            .field("tenants", &"<redacted>")
            .field("limits", &self.limits)
            .finish()
    }
}

impl QueryAdmission {
    pub(crate) fn new(limits: QueryLimits) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(limits.max_concurrent())),
            tenants: Arc::new(Mutex::new(HashMap::with_capacity(
                limits.max_tracked_tenants().min(limits.max_concurrent()),
            ))),
            limits,
        }
    }

    pub(crate) async fn acquire(
        &self,
        principal: &Principal,
    ) -> std::result::Result<QueryPermit, Status> {
        let deadline = Instant::now() + self.limits.queue_wait();
        let tenant = self.tenant_gate(principal)?;
        let tenant_permit = tokio::time::timeout_at(deadline, tenant.acquire_owned())
            .await
            .map_err(|_| {
                telemetry::admission("scope_saturated");
                Status::resource_exhausted("tenant query concurrency limit reached")
            })?
            .map_err(|_| {
                telemetry::admission("shutting_down");
                Status::unavailable("query admission is shutting down")
            })?;
        let global_permit =
            tokio::time::timeout_at(deadline, self.semaphore.clone().acquire_owned())
                .await
                .map_err(|_| {
                    telemetry::admission("saturated");
                    Status::resource_exhausted("query concurrency limit reached")
                })?
                .map_err(|_| {
                    telemetry::admission("shutting_down");
                    Status::unavailable("query admission is shutting down")
                })?;
        telemetry::admission("admitted");
        telemetry::inflight_increment();
        Ok(QueryPermit {
            _global_permit: global_permit,
            _tenant_permit: tenant_permit,
        })
    }

    fn tenant_gate(&self, principal: &Principal) -> std::result::Result<Arc<Semaphore>, Status> {
        let mut tenants = self.tenants.lock().map_err(|_| {
            telemetry::admission("shutting_down");
            Status::unavailable("query admission is unavailable")
        })?;
        tenants.retain(|_, gate| gate.strong_count() > 0);
        let tenant = principal.tenant().as_str();
        if let Some(existing) = tenants.get(tenant) {
            if let Some(gate) = existing.upgrade() {
                return Ok(gate);
            }
            tenants.remove(tenant);
        }
        if tenants.len() >= self.limits.max_tracked_tenants() {
            telemetry::admission("scope_tracker_saturated");
            return Err(Status::resource_exhausted(
                "tenant admission tracker capacity reached",
            ));
        }
        let gate = Arc::new(Semaphore::new(self.limits.max_concurrent_per_tenant()));
        tenants.insert(tenant.to_owned(), Arc::downgrade(&gate));
        Ok(gate)
    }

    pub(crate) fn validate_sql_size(&self, bytes: &[u8]) -> std::result::Result<(), Status> {
        if bytes.len() > self.limits.max_sql_bytes() {
            telemetry::rejection("sql_too_large");
            return Err(Status::resource_exhausted(
                "SQL or statement ticket exceeds the configured byte limit",
            ));
        }
        Ok(())
    }

    fn validate_ticket_size(&self, bytes: &[u8]) -> std::result::Result<(), Status> {
        let maximum = self
            .limits
            .max_sql_bytes()
            .saturating_add(MAX_STATEMENT_TICKET_OVERHEAD);
        if bytes.len() > maximum {
            telemetry::rejection("ticket_too_large");
            return Err(Status::resource_exhausted(
                "SQL or statement ticket exceeds the configured byte limit",
            ));
        }
        Ok(())
    }

    fn execution_deadline(&self) -> Instant { Instant::now() + self.limits.execution_time() }
}

pub(crate) struct QueryPermit {
    _global_permit: OwnedSemaphorePermit,
    _tenant_permit: OwnedSemaphorePermit,
}

impl Drop for QueryPermit {
    fn drop(&mut self) { telemetry::inflight_decrement(); }
}

struct AdmittedFlightStream {
    inner:    Option<<FlightSqlServiceImpl as FlightService>::DoGetStream>,
    deadline: Pin<Box<Sleep>>,
    permit:   Option<Arc<QueryPermit>>,
    decode:   Option<IpcDecodeGuard>,
}

fn apply_delegated_append_scope(
    metadata: &mut tonic::metadata::MetadataMap,
    principal: &Principal,
    namespace: &str,
) -> std::result::Result<(), Status> {
    metadata.insert(
        DELEGATED_NAMESPACE_HEADER,
        namespace
            .parse()
            .map_err(|_| Status::internal("authorized namespace is not valid metadata"))?,
    );
    metadata.insert(
        DELEGATED_TENANT_HEADER,
        principal
            .tenant()
            .as_str()
            .parse()
            .map_err(|_| Status::internal("authenticated tenant is not valid metadata"))?,
    );
    Ok(())
}

type DiscoveryRecordBatchStream =
    Pin<Box<dyn Stream<Item = std::result::Result<RecordBatch, FlightError>> + Send>>;

#[derive(Default)]
struct TableCursor {
    namespace:   Option<Namespace>,
    table_index: usize,
}

impl TableCursor {
    fn next(&mut self, generation: &CatalogGeneration) -> Option<(Namespace, TableName)> {
        if self.namespace.is_none() {
            self.namespace = generation
                .listings()
                .first_key_value()
                .map(|(key, _)| key.clone());
        }
        loop {
            let next_namespace = {
                let namespace = self.namespace.as_ref()?;
                let tables = generation
                    .listings()
                    .get(namespace)
                    .expect("cursor namespace comes from generation");
                if let Some(table) = tables.get(self.table_index) {
                    self.table_index += 1;
                    return Some((namespace.clone(), table.clone()));
                }
                generation
                    .listings()
                    .range((Excluded(namespace), Unbounded))
                    .next()
                    .map(|(key, _)| key.clone())
            };
            self.namespace = next_namespace;
            self.table_index = 0;
        }
    }
}

#[derive(Default)]
struct NamespaceCursor {
    last: Option<Namespace>,
}

impl NamespaceCursor {
    fn next(&mut self, generation: &CatalogGeneration) -> Option<Namespace> {
        let next = match self.last.as_ref() {
            Some(last) => generation
                .listings()
                .range((Excluded(last), Unbounded))
                .next()
                .map(|(key, _)| key.clone()),
            None => generation
                .listings()
                .first_key_value()
                .map(|(key, _)| key.clone()),
        }?;
        self.last = Some(next.clone());
        Some(next)
    }
}

struct TableDiscoveryState {
    query:               CommandGetTables,
    principal:           Principal,
    generation:          Arc<CatalogGeneration>,
    limits:              DiscoveryLimits,
    cursor:              TableCursor,
    emitted:             usize,
    pending_limit_error: bool,
    finished:            bool,
}

impl TableDiscoveryState {
    fn next_batch(&mut self) -> std::result::Result<Option<RecordBatch>, Status> {
        if self.pending_limit_error {
            return Err(Status::resource_exhausted("discovery row limit reached"));
        }
        if self.finished {
            return Ok(None);
        }
        let catalog_matches = self
            .query
            .catalog
            .as_deref()
            .map_or(true, |catalog| catalog == "lake");
        let table_type_matches = self.query.table_types.is_empty()
            || self.query.table_types.iter().any(|kind| kind == "TABLE");
        if !catalog_matches || !table_type_matches {
            self.finished = true;
            return Ok(None);
        }

        let mut builder = self.query.clone().into_builder();
        let empty_schema = Schema::empty();
        let mut batch_rows = 0;
        while batch_rows < self.limits.batch_rows() {
            let Some((namespace, table)) = self.cursor.next(&self.generation) else {
                self.finished = true;
                break;
            };
            if !self.principal.can_access_namespace(&namespace.0)
                || self
                    .query
                    .db_schema_filter_pattern
                    .as_deref()
                    .is_some_and(|pattern| !flight_sql_pattern_matches(&namespace.0, pattern))
                || self
                    .query
                    .table_name_filter_pattern
                    .as_deref()
                    .is_some_and(|pattern| !flight_sql_pattern_matches(&table.0, pattern))
            {
                continue;
            }
            if self.emitted == self.limits.max_rows() {
                if batch_rows == 0 {
                    return Err(Status::resource_exhausted("discovery row limit reached"));
                }
                self.pending_limit_error = true;
                break;
            }

            let table_ref = lake_common::TableRef::new(&namespace.0, &table.0);
            let cached_schema = self.generation.table_schema(&table_ref);
            if self.query.include_schema && cached_schema.is_none() {
                return Err(Status::failed_precondition(
                    "table schema is unavailable; migrate the legacy registration",
                ));
            }
            builder
                .append(
                    "lake",
                    &namespace.0,
                    &table.0,
                    "TABLE",
                    cached_schema.map_or(&empty_schema, AsRef::as_ref),
                )
                .map_err(Status::from)?;
            self.emitted += 1;
            batch_rows += 1;
        }

        if batch_rows == 0 {
            Ok(None)
        } else {
            builder.build().map(Some).map_err(Status::from)
        }
    }
}

struct SchemaDiscoveryState {
    query:               CommandGetDbSchemas,
    principal:           Principal,
    generation:          Arc<CatalogGeneration>,
    limits:              DiscoveryLimits,
    cursor:              NamespaceCursor,
    emitted:             usize,
    pending_limit_error: bool,
    finished:            bool,
}

impl SchemaDiscoveryState {
    fn next_batch(&mut self) -> std::result::Result<Option<RecordBatch>, Status> {
        if self.pending_limit_error {
            return Err(Status::resource_exhausted("discovery row limit reached"));
        }
        if self.finished {
            return Ok(None);
        }
        if self
            .query
            .catalog
            .as_deref()
            .is_some_and(|catalog| catalog != "lake")
        {
            self.finished = true;
            return Ok(None);
        }

        let mut builder = self.query.clone().into_builder();
        let mut batch_rows = 0;
        while batch_rows < self.limits.batch_rows() {
            let Some(namespace) = self.cursor.next(&self.generation) else {
                self.finished = true;
                break;
            };
            if !self.principal.can_access_namespace(&namespace.0)
                || self
                    .query
                    .db_schema_filter_pattern
                    .as_deref()
                    .is_some_and(|pattern| !flight_sql_pattern_matches(&namespace.0, pattern))
            {
                continue;
            }
            if self.emitted == self.limits.max_rows() {
                if batch_rows == 0 {
                    return Err(Status::resource_exhausted("discovery row limit reached"));
                }
                self.pending_limit_error = true;
                break;
            }
            builder.append("lake", &namespace.0);
            self.emitted += 1;
            batch_rows += 1;
        }

        if batch_rows == 0 {
            Ok(None)
        } else {
            builder.build().map(Some).map_err(Status::from)
        }
    }
}

fn table_discovery_batches(
    query: CommandGetTables,
    principal: Principal,
    generation: Arc<CatalogGeneration>,
    limits: DiscoveryLimits,
) -> DiscoveryRecordBatchStream {
    let state = TableDiscoveryState {
        query,
        principal,
        generation,
        limits,
        cursor: TableCursor::default(),
        emitted: 0,
        pending_limit_error: false,
        finished: false,
    };
    Box::pin(futures::stream::unfold(Some(state), |state| async move {
        let mut state = state?;
        match state.next_batch() {
            Ok(Some(batch)) => Some((Ok(batch), Some(state))),
            Ok(None) => None,
            Err(status) => Some((Err(FlightError::from(status)), None)),
        }
    }))
}

fn schema_discovery_batches(
    query: CommandGetDbSchemas,
    principal: Principal,
    generation: Arc<CatalogGeneration>,
    limits: DiscoveryLimits,
) -> DiscoveryRecordBatchStream {
    let state = SchemaDiscoveryState {
        query,
        principal,
        generation,
        limits,
        cursor: NamespaceCursor::default(),
        emitted: 0,
        pending_limit_error: false,
        finished: false,
    };
    Box::pin(futures::stream::unfold(Some(state), |state| async move {
        let mut state = state?;
        match state.next_batch() {
            Ok(Some(batch)) => Some((Ok(batch), Some(state))),
            Ok(None) => None,
            Err(status) => Some((Err(FlightError::from(status)), None)),
        }
    }))
}

#[cfg(test)]
fn build_table_discovery(
    query: CommandGetTables,
    principal: &Principal,
    generation: &Arc<CatalogGeneration>,
) -> std::result::Result<RecordBatch, Status> {
    let empty_query = query.clone();
    let rows = generation
        .listings()
        .values()
        .map(Vec::len)
        .sum::<usize>()
        .max(1);
    let mut state = TableDiscoveryState {
        query,
        principal: principal.clone(),
        generation: generation.clone(),
        limits: DiscoveryLimits::try_new(rows, rows).expect("positive test limits"),
        cursor: TableCursor::default(),
        emitted: 0,
        pending_limit_error: false,
        finished: false,
    };
    state.next_batch()?.map_or_else(
        || empty_query.into_builder().build().map_err(Status::from),
        Ok,
    )
}

fn flight_sql_pattern_matches(value: &str, pattern: &str) -> bool {
    let value = value.chars().collect::<Vec<_>>();
    let pattern = pattern.chars().collect::<Vec<_>>();
    let (mut value_index, mut pattern_index) = (0, 0);
    let (mut wildcard_index, mut retry_value_index) = (None, 0);

    while value_index < value.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == '_' || pattern[pattern_index] == value[value_index])
        {
            value_index += 1;
            pattern_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == '%' {
            wildcard_index = Some(pattern_index);
            pattern_index += 1;
            retry_value_index = value_index;
        } else if let Some(wildcard_index) = wildcard_index {
            retry_value_index += 1;
            value_index = retry_value_index;
            pattern_index = wildcard_index + 1;
        } else {
            return false;
        }
    }

    pattern[pattern_index..]
        .iter()
        .all(|character| *character == '%')
}

impl AdmittedFlightStream {
    fn new(
        inner: <FlightSqlServiceImpl as FlightService>::DoGetStream,
        deadline: Instant,
        permit: QueryPermit,
    ) -> Self {
        Self {
            inner:    Some(inner),
            deadline: Box::pin(tokio::time::sleep_until(deadline)),
            permit:   Some(Arc::new(permit)),
            decode:   None,
        }
    }

    fn new_with_decode(
        inner: <FlightSqlServiceImpl as FlightService>::DoGetStream,
        deadline: Instant,
        permit: Arc<QueryPermit>,
        decode: IpcDecodeGuard,
    ) -> Self {
        Self {
            inner:    Some(inner),
            deadline: Box::pin(tokio::time::sleep_until(deadline)),
            permit:   Some(permit),
            decode:   Some(decode),
        }
    }
}

impl Stream for AdmittedFlightStream {
    type Item = std::result::Result<arrow_flight::FlightData, Status>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.inner.is_none() {
            return Poll::Ready(None);
        }
        if self.deadline.as_mut().poll(context).is_ready() {
            self.inner.take();
            self.decode.take();
            self.permit.take();
            return Poll::Ready(Some(Err(Status::deadline_exceeded(
                "query execution deadline exceeded",
            ))));
        }
        let poll = self
            .inner
            .as_mut()
            .expect("checked above")
            .as_mut()
            .poll_next(context);
        if matches!(poll, Poll::Ready(None | Some(Err(_)))) {
            self.inner.take();
            self.decode.take();
            self.permit.take();
        }
        poll
    }
}

/// A Flight SQL service backed by a stateless [`QueryEngine`].
pub struct FlightSqlServiceImpl {
    /// The warmed query engine that plans and executes incoming SQL.
    pub engine:                  Arc<QueryEngine>,
    /// Metadata Flight address used only for stateless FILE append forwarding.
    pub metadata_addr:           Option<String>,
    /// TLS and service credential for the Query-to-Metasrv hop.
    pub metadata_security:       ClientSecurity,
    /// Immutable, credential-free stage metadata advertised to SDK clients.
    pub managed_stage:           Option<ManagedStageDescriptor>,
    /// Process-local admission shared by SQL statement RPCs.
    pub(crate) admission:        QueryAdmission,
    /// Process-local row and batch bounds for metadata discovery.
    pub(crate) discovery_limits: DiscoveryLimits,
    /// Stateless authenticated-encryption codec shared by statement RPCs.
    pub(crate) ticket_codec:     StatementTicketCodec,
}

impl FlightSqlServiceImpl {
    /// Ensure the bounded-staleness catalog and plan `sql`, returning only its
    /// Arrow schema.
    ///
    /// Used by `GetFlightInfo` to advertise the result schema without
    /// materializing any rows.
    async fn plan_schema(
        &self,
        sql: &str,
        snapshots: &[TableSnapshot],
    ) -> std::result::Result<Schema, Status> {
        let df = self
            .engine
            .plan_sql_at(sql, snapshots)
            .await
            .map_err(query_status)?;
        Ok(df.schema().as_arrow().clone())
    }

    fn principal<T>(&self, request: &Request<T>) -> std::result::Result<Principal, Status> {
        request
            .extensions()
            .get::<Principal>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("authenticated principal is missing"))
    }

    fn authorize_namespace(
        &self,
        principal: &Principal,
        namespace: &str,
    ) -> std::result::Result<(), Status> {
        if principal.can_access_namespace(namespace) {
            Ok(())
        } else {
            Err(Status::permission_denied("resource is not available"))
        }
    }

    fn authorize_sql(&self, principal: &Principal, sql: &str) -> std::result::Result<(), Status> {
        self.authorized_table_references(principal, sql).map(drop)
    }

    fn authorized_table_references(
        &self,
        principal: &Principal,
        sql: &str,
    ) -> std::result::Result<Vec<TableRef>, Status> {
        let state = self.engine.context().state();
        let statement = state
            .sql_to_statement(sql, &Dialect::Generic)
            .map_err(|_| Status::invalid_argument("invalid SQL statement"))?;
        if matches!(
            &statement,
            Statement::Statement(statement)
                if matches!(
                    statement.as_ref(),
                    SqlStatement::Insert(_)
                        | SqlStatement::Update(_)
                        | SqlStatement::Delete(_)
                        | SqlStatement::Merge(_)
                        | SqlStatement::Directory { .. }
                        | SqlStatement::Copy { .. }
                        | SqlStatement::CopyIntoSnowflake { .. }
                )
        ) || matches!(statement, Statement::CopyTo(_))
        {
            return Err(Status::invalid_argument("DML not supported"));
        }
        let references = state
            .resolve_table_references(&statement)
            .map_err(|_| Status::invalid_argument("invalid SQL statement"))?;
        let mut tables = BTreeSet::new();
        for reference in references {
            let table = match reference {
                TableReference::Full {
                    catalog,
                    schema,
                    table,
                } if catalog.as_ref() == "lake" => TableRef::new(schema.as_ref(), table.as_ref()),
                TableReference::Partial { schema, table } => {
                    TableRef::new(schema.as_ref(), table.as_ref())
                }
                TableReference::Full { .. } | TableReference::Bare { .. } => {
                    return Err(Status::permission_denied("resource is not available"));
                }
            };
            self.authorize_namespace(principal, &table.namespace.0)?;
            tables.insert(table);
            if tables.len() > MAX_TABLE_SNAPSHOTS {
                return Err(Status::resource_exhausted(
                    "statement references too many tables",
                ));
            }
        }
        Ok(tables.into_iter().collect())
    }
}

fn ticket_snapshot(snapshot: &TableSnapshot) -> StatementTableSnapshot {
    StatementTableSnapshot {
        namespace:      snapshot.table().namespace.0.clone(),
        table:          snapshot.table().name.0.clone(),
        engine:         snapshot.engine().to_owned(),
        location:       snapshot.location().0.clone(),
        incarnation_id: snapshot.incarnation_id().to_owned(),
        version:        snapshot.version().0,
    }
}

fn catalog_snapshot(snapshot: &StatementTableSnapshot) -> TableSnapshot {
    TableSnapshot::new(
        TableRef::new(&snapshot.namespace, &snapshot.table),
        TableLocation::new(&snapshot.location),
        &snapshot.engine,
        &snapshot.incarnation_id,
        Version(snapshot.version),
    )
}

fn invalid_statement_ticket() -> Status {
    telemetry::rejection("invalid_ticket");
    Status::unauthenticated("invalid statement ticket")
}

/// Collapse any displayable error into an internal [`Status`].
fn to_status<E: std::fmt::Display>(err: E) -> Status { Status::internal(err.to_string()) }

fn forwarded_append_status(status: Status) -> Status {
    Status::new(status.code(), "metadata FILE append rejected")
}

fn forwarded_append_error(error: FlightError) -> Status {
    match error {
        FlightError::Tonic(status) => forwarded_append_status(*status),
        _ => Status::unavailable("metadata FILE append is unavailable"),
    }
}

fn query_status(error: QueryError) -> Status {
    match error {
        QueryError::UnpinnableTable { .. } | QueryError::SnapshotProvider { .. } => {
            Status::failed_precondition("the pinned table snapshot is unavailable")
        }
        QueryError::SnapshotResolution { .. } => {
            Status::internal("could not resolve a table snapshot")
        }
        error => to_status(error),
    }
}

fn record_rpc_outcome<T>(span: &Span, result: &std::result::Result<T, Status>) {
    span.record("rpc.outcome", if result.is_ok() { "ok" } else { "error" });
}

fn query_server_span<T>(request: &Request<T>, method: &'static str) -> Span {
    let span = tracing::info_span!(
        target: "lake_query",
        "flight.server",
        rpc.system = "grpc",
        rpc.service = "lake.query",
        rpc.method = method,
        rpc.outcome = field::Empty,
    );
    let _ = set_span_parent_from_request(&span, request);
    span
}

type BoxStatusStream<T> =
    Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

fn finish_stream_rpc<T: Send + 'static>(
    span: &Span,
    result: std::result::Result<Response<BoxStatusStream<T>>, Status>,
) -> std::result::Result<Response<BoxStatusStream<T>>, Status> {
    match result {
        Ok(response) => {
            let stream: BoxStatusStream<T> =
                Box::pin(TracedFlightStream::new(response.into_inner(), span.clone()));
            Ok(Response::new(stream))
        }
        Err(error) => {
            span.record("rpc.outcome", "error");
            Err(error)
        }
    }
}

/// Delegates Arrow Flight SQL dispatch while overriding `ListActions`, the one
/// successful RPC whose request is not exposed by `FlightSqlService` hooks.
pub(crate) struct TracedFlightSqlService {
    inner:         FlightSqlServiceImpl,
    async_queries: Option<AsyncQueryCoordinator>,
}

impl TracedFlightSqlService {
    pub(crate) fn new(inner: FlightSqlServiceImpl) -> Self {
        Self {
            inner,
            async_queries: None,
        }
    }

    pub(crate) fn with_async_queries(
        inner: FlightSqlServiceImpl,
        async_queries: AsyncQueryCoordinator,
    ) -> Self {
        Self {
            inner,
            async_queries: Some(async_queries),
        }
    }

    async fn poll_async_query(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        let coordinator = self
            .async_queries
            .as_ref()
            .ok_or_else(|| Status::unimplemented("asynchronous queries are not configured"))?;
        let principal = self.inner.principal(&request)?;
        let descriptor = request.into_inner();
        if AsyncQueryCoordinator::is_poll_handle(&descriptor.cmd) {
            let query_id = coordinator
                .open_poll_handle(&descriptor.cmd, &principal)
                .map_err(|_| Status::unauthenticated("invalid async query handle"))?;
            let record = coordinator
                .store()
                .load(&query_id)
                .await
                .map_err(|_| Status::unavailable("async query state is unavailable"))?
                .ok_or_else(|| Status::unauthenticated("invalid async query handle"))?;
            if !record.belongs_to(&principal) {
                return Err(Status::unauthenticated("invalid async query handle"));
            }
            if record.is_completed() {
                let manifest = coordinator
                    .load_manifest(&record)
                    .await
                    .map_err(|_| Status::unavailable("async query result is unavailable"))?;
                let cancel_handle = coordinator
                    .refresh_poll_handle(&query_id, &principal)
                    .map_err(|_| Status::internal("could not issue async cancel handle"))?;
                let mut info = FlightInfo::new().with_app_metadata(cancel_handle);
                info.schema = manifest.schema_ipc().to_vec().into();
                for part in 0..manifest.part_count() {
                    let handle = coordinator
                        .seal_result_handle(&query_id, part, &principal)
                        .map_err(|_| Status::internal("could not issue async result handle"))?;
                    info = info.with_endpoint(
                        FlightEndpoint::new()
                            .with_ticket(Ticket::new(handle))
                            .with_expiration_time(poll_expiration(
                                coordinator
                                    .capability_expires_at()
                                    .map_err(|_| Status::internal("invalid capability expiry"))?,
                            )?),
                    );
                }
                let poll = PollInfo::new()
                    .with_info(info)
                    .try_with_progress(1.0)
                    .map_err(|_| Status::internal("could not encode async query progress"))?;
                return Ok(Response::new(poll));
            }
            if record.is_failed() {
                return Err(Status::internal("asynchronous query execution failed"));
            }
            if record.is_cancelled() {
                return Err(Status::cancelled("asynchronous query was cancelled"));
            }
            if record.is_expired() {
                return Err(Status::deadline_exceeded("asynchronous query expired"));
            }
            if !record.is_pending() {
                return Err(Status::failed_precondition(
                    "async query is terminal but has no published result",
                ));
            }
            let handle = coordinator
                .refresh_poll_handle(&query_id, &principal)
                .map_err(|_| Status::internal("could not refresh async query handle"))?;
            let expiration = poll_expiration(
                coordinator
                    .capability_expires_at()
                    .map_err(|_| Status::internal("invalid capability expiry"))?,
            )?;
            let poll = PollInfo::new()
                .with_descriptor(FlightDescriptor::new_cmd(handle))
                .try_with_progress(0.0)
                .map_err(|_| Status::internal("could not encode async query progress"))?
                .with_expiration_time(expiration);
            return Ok(Response::new(poll));
        }

        self.inner.admission.validate_sql_size(&descriptor.cmd)?;
        let any = Any::decode(&*descriptor.cmd)
            .map_err(|_| Status::invalid_argument("invalid Flight SQL poll descriptor"))?;
        let Command::CommandStatementQuery(query) = Command::try_from(any)
            .map_err(|_| Status::invalid_argument("invalid Flight SQL poll descriptor"))?
        else {
            return Err(Status::invalid_argument(
                "PollFlightInfo requires a statement query",
            ));
        };
        self.inner
            .admission
            .validate_sql_size(query.query.as_bytes())?;
        let submission_id = query
            .transaction_id
            .as_deref()
            .map(<[u8; 16]>::try_from)
            .transpose()
            .map_err(|_| {
                Status::invalid_argument(
                    "async statement transaction_id must be a 16-byte submission id",
                )
            })?;
        if let Some(submission_id) = submission_id {
            let existing = coordinator
                .resume_submission_with_id(&query.query, &principal, submission_id)
                .await
                .map_err(|error| match error {
                    crate::async_query::AsyncQueryCoordinatorError::SubmissionConflict => {
                        Status::failed_precondition(
                            "async submission id is already bound to another statement",
                        )
                    }
                    _ => Status::internal("could not resume async query submission"),
                })?;
            if let Some(submission) = existing {
                let poll = PollInfo::new()
                    .with_descriptor(FlightDescriptor::new_cmd(submission.poll_handle().to_vec()))
                    .try_with_progress(0.0)
                    .map_err(|_| Status::internal("could not encode async query progress"))?
                    .with_expiration_time(poll_expiration(
                        coordinator
                            .capability_expires_at()
                            .map_err(|_| Status::internal("invalid capability expiry"))?,
                    )?);
                return Ok(Response::new(poll));
            }
        }
        let references = self
            .inner
            .authorized_table_references(&principal, &query.query)?;
        let _permit = self.inner.admission.acquire(&principal).await?;
        let deadline = self.inner.admission.execution_deadline();
        let (snapshots, schema) = tokio::time::timeout_at(deadline, async {
            let snapshots = self
                .inner
                .engine
                .resolve_snapshots(&references)
                .await
                .map_err(query_status)?;
            let schema = self.inner.plan_schema(&query.query, &snapshots).await?;
            Ok::<_, Status>((snapshots, schema))
        })
        .await
        .map_err(|_| Status::deadline_exceeded("query planning deadline exceeded"))??;
        let statement = StatementTicket {
            sql:       query.query,
            snapshots: snapshots.iter().map(ticket_snapshot).collect(),
        };
        let submission = match submission_id {
            Some(submission_id) => {
                coordinator
                    .submit_statement_with_id(&statement, &principal, submission_id)
                    .await
            }
            None => coordinator.submit_statement(&statement, &principal).await,
        }
        .map_err(|error| match error {
            crate::async_query::AsyncQueryCoordinatorError::SubmissionConflict => {
                Status::failed_precondition(
                    "async submission id is already bound to another statement",
                )
            }
            crate::async_query::AsyncQueryCoordinatorError::Store {
                source: crate::async_query::AsyncQueryStoreError::QuotaExceeded,
            } => {
                crate::telemetry::async_quota_rejection("outstanding_jobs");
                Status::resource_exhausted("async tenant outstanding query limit reached")
            }
            crate::async_query::AsyncQueryCoordinatorError::SubmissionPending => {
                Status::unavailable("async query submission is in progress")
            }
            _ => Status::internal("could not persist async query"),
        })?;
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|_| Status::internal("could not encode async query schema"))?
            .with_descriptor(descriptor)
            .with_app_metadata(submission.poll_handle().to_vec());
        let poll = PollInfo::new()
            .with_info(info)
            .with_descriptor(FlightDescriptor::new_cmd(submission.poll_handle().to_vec()))
            .try_with_progress(0.0)
            .map_err(|_| Status::internal("could not encode async query progress"))?
            .with_expiration_time(poll_expiration(
                coordinator
                    .capability_expires_at()
                    .map_err(|_| Status::internal("invalid capability expiry"))?,
            )?);
        Ok(Response::new(poll))
    }

    async fn do_get_async_result(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let coordinator = self
            .async_queries
            .as_ref()
            .ok_or_else(|| Status::unimplemented("asynchronous queries are not configured"))?;
        let principal = self.inner.principal(&request)?;
        let (query_id, part) = coordinator
            .open_result_handle(&request.get_ref().ticket, &principal)
            .map_err(|_| Status::unauthenticated("invalid async result handle"))?;
        let record = coordinator
            .store()
            .load(&query_id)
            .await
            .map_err(|_| Status::unavailable("async query state is unavailable"))?
            .ok_or_else(|| Status::unauthenticated("invalid async result handle"))?;
        if !record.belongs_to(&principal) || !record.is_completed() {
            return Err(Status::unauthenticated("invalid async result handle"));
        }
        let permit = self.inner.admission.acquire(&principal).await?;
        let permit = Arc::new(permit);
        let deadline = self.inner.admission.execution_deadline();
        let manifest = coordinator
            .load_manifest(&record)
            .await
            .map_err(|_| Status::unavailable("async query result is unavailable"))?;
        let location = manifest
            .part(part)
            .ok_or_else(|| Status::unauthenticated("invalid async result handle"))?;
        let reader = coordinator
            .open_result_part(&manifest, part)
            .await
            .map_err(|_| Status::unavailable("async query result is unavailable"))?;
        let decoded = tokio::time::timeout_at(
            deadline,
            decode_ipc_reader(
                reader,
                location.size_bytes,
                IpcPipelineLimits::production(MAX_RESULT_PART_BYTES),
                PipelineProbe::default(),
                permit.clone(),
            ),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("async result decode deadline exceeded"))?
        .map_err(|_| Status::internal("async query result is invalid"))?;
        let (schema, batches, decode) = decoded.into_parts();
        let batches = batches.into_stream().map(|batch| {
            batch.map_err(|_| FlightError::from(Status::internal("async query result is invalid")))
        });
        let stream: <FlightSqlServiceImpl as FlightService>::DoGetStream = Box::pin(
            FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(batches)
                .map_err(Status::from),
        );
        Ok(Response::new(Box::pin(
            AdmittedFlightStream::new_with_decode(stream, deadline, permit, decode),
        )))
    }

    async fn cancel_async_query(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let coordinator = self
            .async_queries
            .as_ref()
            .ok_or_else(|| Status::unimplemented("asynchronous queries are not configured"))?;
        let principal = self.inner.principal(&request)?;
        let cancel = CancelFlightInfoRequest::decode(&*request.into_inner().body)
            .map_err(|_| Status::invalid_argument("invalid CancelFlightInfo request"))?;
        let info = cancel
            .info
            .ok_or_else(|| Status::invalid_argument("CancelFlightInfo is missing FlightInfo"))?;
        let query_id = coordinator
            .open_poll_handle(&info.app_metadata, &principal)
            .map_err(|_| Status::unauthenticated("invalid async cancel handle"))?;
        let record = coordinator
            .store()
            .load(&query_id)
            .await
            .map_err(|_| Status::unavailable("async query state is unavailable"))?
            .ok_or_else(|| Status::not_found("asynchronous query does not exist"))?;
        if !record.belongs_to(&principal) {
            return Err(Status::unauthenticated("invalid async cancel handle"));
        }
        let status = if record.is_pending() {
            coordinator
                .cancel(&query_id)
                .await
                .map_err(|_| Status::aborted("asynchronous query changed concurrently"))?;
            CancelStatus::Cancelled
        } else if record.is_cancelled() {
            CancelStatus::Cancelled
        } else {
            CancelStatus::NotCancellable
        };
        let body = CancelFlightInfoResult::new(status).encode_to_vec();
        let stream = futures::stream::once(async move { Ok(FlightResult { body: body.into() }) });
        Ok(Response::new(Box::pin(stream)))
    }
}

fn poll_expiration(seconds: u64) -> std::result::Result<prost_types::Timestamp, Status> {
    Ok(prost_types::Timestamp {
        seconds: i64::try_from(seconds)
            .map_err(|_| Status::internal("async query expiration is invalid"))?,
        nanos:   0,
    })
}

#[tonic::async_trait]
impl FlightService for TracedFlightSqlService {
    type DoActionStream = <FlightSqlServiceImpl as FlightService>::DoActionStream;
    type DoExchangeStream = <FlightSqlServiceImpl as FlightService>::DoExchangeStream;
    type DoGetStream = <FlightSqlServiceImpl as FlightService>::DoGetStream;
    type DoPutStream = <FlightSqlServiceImpl as FlightService>::DoPutStream;
    type HandshakeStream = <FlightSqlServiceImpl as FlightService>::HandshakeStream;
    type ListActionsStream = <FlightSqlServiceImpl as FlightService>::ListActionsStream;
    type ListFlightsStream = <FlightSqlServiceImpl as FlightService>::ListFlightsStream;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        <FlightSqlServiceImpl as FlightService>::handshake(&self.inner, request).await
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        <FlightSqlServiceImpl as FlightService>::list_flights(&self.inner, request).await
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        <FlightSqlServiceImpl as FlightService>::get_flight_info(&self.inner, request).await
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        self.poll_async_query(request).await
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        <FlightSqlServiceImpl as FlightService>::get_schema(&self.inner, request).await
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        if AsyncQueryCoordinator::is_result_handle(&request.get_ref().ticket) {
            return self.do_get_async_result(request).await;
        }
        <FlightSqlServiceImpl as FlightService>::do_get(&self.inner, request).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        <FlightSqlServiceImpl as FlightService>::do_put(&self.inner, request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        if request.get_ref().r#type == "CancelFlightInfo" {
            return self.cancel_async_query(request).await;
        }
        <FlightSqlServiceImpl as FlightService>::do_action(&self.inner, request).await
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        let span = query_server_span(&request, "list_actions");
        let result = <FlightSqlServiceImpl as FlightService>::list_actions(&self.inner, request)
            .instrument(span.clone())
            .await
            .map(|response| {
                if self.async_queries.is_none() {
                    return response;
                }
                let stream = response.into_inner().chain(futures::stream::once(async {
                    Ok(ActionType {
                        r#type:      "CancelFlightInfo".to_owned(),
                        description: "Cancel a durable asynchronous query".to_owned(),
                    })
                }));
                Response::new(Box::pin(stream) as Self::ListActionsStream)
            });
        finish_stream_rpc(&span, result)
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        <FlightSqlServiceImpl as FlightService>::do_exchange(&self.inner, request).await
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = Self;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<
        Response<
            Pin<Box<dyn Stream<Item = std::result::Result<HandshakeResponse, Status>> + Send>>,
        >,
        Status,
    > {
        let span = query_server_span(&request, "handshake");
        let result = async move {
            let response = HandshakeResponse::default();
            let stream: BoxStatusStream<HandshakeResponse> =
                Box::pin(futures::stream::once(async move { Ok(response) }));
            Ok(Response::new(stream))
        }
        .instrument(span.clone())
        .await;
        finish_stream_rpc(&span, result)
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let span = query_server_span(&request, "get_flight_info");
        let result = async move {
            let CommandStatementQuery { query: sql, .. } = query;
            self.admission.validate_sql_size(sql.as_bytes())?;
            let principal = self.principal(&request)?;
            let references = self.authorized_table_references(&principal, &sql)?;
            let _permit = self.admission.acquire(&principal).await?;
            let deadline = self.admission.execution_deadline();
            let (snapshots, schema) = tokio::time::timeout_at(deadline, async {
                let snapshots = self
                    .engine
                    .resolve_snapshots(&references)
                    .await
                    .map_err(query_status)?;
                let schema = self.plan_schema(&sql, &snapshots).await?;
                Ok::<_, Status>((snapshots, schema))
            })
            .await
            .map_err(|_| Status::deadline_exceeded("query planning deadline exceeded"))??;

            let statement = StatementTicket {
                sql,
                snapshots: snapshots.iter().map(ticket_snapshot).collect(),
            };
            let statement_handle = self
                .ticket_codec
                .seal_statement(&statement, &principal)
                .map_err(|_| Status::internal("could not issue statement ticket"))?;
            self.admission.validate_ticket_size(&statement_handle)?;
            let ticket = TicketStatementQuery {
                statement_handle: statement_handle.into(),
            };
            let endpoint =
                FlightEndpoint::new().with_ticket(Ticket::new(ticket.as_any().encode_to_vec()));

            let info = FlightInfo::new()
                .try_with_schema(&schema)
                .map_err(|e| Status::internal(format!("encode schema: {e}")))?
                .with_endpoint(endpoint)
                .with_descriptor(request.into_inner());
            Ok(Response::new(info))
        }
        .instrument(span.clone())
        .await;
        record_rpc_outcome(&span, &result);
        result
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let span = query_server_span(&request, "get_flight_info_schemas");
        let result = async move {
            let endpoint =
                FlightEndpoint::new().with_ticket(Ticket::new(query.as_any().encode_to_vec()));
            let info = FlightInfo::new()
                .try_with_schema(&query.clone().into_builder().schema())
                .map_err(|error| Status::internal(format!("encode schema discovery: {error}")))?
                .with_endpoint(endpoint)
                .with_descriptor(request.into_inner());
            Ok(Response::new(info))
        }
        .instrument(span.clone())
        .await;
        record_rpc_outcome(&span, &result);
        result
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let span = query_server_span(&request, "get_flight_info_tables");
        let result = async move {
            let endpoint =
                FlightEndpoint::new().with_ticket(Ticket::new(query.as_any().encode_to_vec()));
            let info = FlightInfo::new()
                .try_with_schema(&query.clone().into_builder().schema())
                .map_err(|error| Status::internal(format!("encode table discovery: {error}")))?
                .with_endpoint(endpoint)
                .with_descriptor(request.into_inner());
            Ok(Response::new(info))
        }
        .instrument(span.clone())
        .await;
        record_rpc_outcome(&span, &result);
        result
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let span = query_server_span(&request, "do_get");
        let result =
            async move {
                let principal = self.principal(&request)?;
                self.admission
                    .validate_ticket_size(&ticket.statement_handle)?;
                let statement = self
                    .ticket_codec
                    .open_statement(&ticket.statement_handle, &principal)
                    .map_err(|_| invalid_statement_ticket())?;
                self.admission.validate_sql_size(statement.sql.as_bytes())?;
                let references = self
                    .authorized_table_references(&principal, &statement.sql)
                    .map_err(|_| invalid_statement_ticket())?;
                let snapshots = statement
                    .snapshots
                    .iter()
                    .map(catalog_snapshot)
                    .collect::<Vec<_>>();
                if references
                    != snapshots
                        .iter()
                        .map(|snapshot| snapshot.table().clone())
                        .collect::<Vec<_>>()
                {
                    return Err(invalid_statement_ticket());
                }
                let permit = self.admission.acquire(&principal).await?;
                let deadline = self.admission.execution_deadline();

                let batches = tokio::time::timeout_at(deadline, async {
                    let df = self
                        .engine
                        .plan_sql_at(&statement.sql, &snapshots)
                        .await
                        .map_err(query_status)?;
                    df.execute_stream().await.map_err(to_status)
                })
                .await
                .map_err(|_| Status::deadline_exceeded("query planning deadline exceeded"))??;
                let schema: SchemaRef = batches.schema();
                let batches = batches.map_err(|err| FlightError::ExternalError(Box::new(err)));

                let stream = FlightDataEncoderBuilder::new()
                    .with_schema(schema)
                    .build(batches)
                    .map_err(Status::from);
                let stream: <Self as FlightService>::DoGetStream = Box::pin(
                    AdmittedFlightStream::new(Box::pin(stream), deadline, permit),
                );
                Ok(Response::new(stream))
            }
            .instrument(span.clone())
            .await;
        finish_stream_rpc(&span, result)
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let span = query_server_span(&request, "do_get_schemas");
        let result = async move {
            let principal = self.principal(&request)?;
            let permit = self.admission.acquire(&principal).await?;
            let deadline = self.admission.execution_deadline();
            let schema = query.clone().into_builder().schema();
            let generation = self.engine.cached_catalog_generation();
            let batches =
                schema_discovery_batches(query, principal, generation, self.discovery_limits);
            let stream = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(batches)
                .map_err(Status::from);
            let stream: BoxStatusStream<_> = Box::pin(AdmittedFlightStream::new(
                Box::pin(stream),
                deadline,
                permit,
            ));
            Ok(Response::new(stream))
        }
        .instrument(span.clone())
        .await;
        finish_stream_rpc(&span, result)
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let span = query_server_span(&request, "do_get_tables");
        let result = async move {
            let principal = self.principal(&request)?;
            let permit = self.admission.acquire(&principal).await?;
            let deadline = self.admission.execution_deadline();
            let schema = query.clone().into_builder().schema();
            let generation = self.engine.cached_catalog_generation();
            let batches =
                table_discovery_batches(query, principal, generation, self.discovery_limits);
            let stream = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(batches)
                .map_err(Status::from);
            let stream: BoxStatusStream<_> = Box::pin(AdmittedFlightStream::new(
                Box::pin(stream),
                deadline,
                permit,
            ));
            Ok(Response::new(stream))
        }
        .instrument(span.clone())
        .await;
        finish_stream_rpc(&span, result)
    }

    async fn do_put_fallback(
        &self,
        request: Request<PeekableFlightDataStream>,
        message: Any,
    ) -> std::result::Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        let span = query_server_span(&request, "do_put");
        let result = async move {
            if message.type_url != FILE_APPEND_TYPE_URL {
                return Err(Status::invalid_argument("invalid FILE append command"));
            }
            let append = FileAppendRequest::from_command_payload(&message.value)
                .ok_or_else(|| Status::invalid_argument("invalid FILE append command"))?;
            let principal = self.principal(&request)?;
            self.authorize_namespace(&principal, &append.table().namespace.0)?;
            let addr = self
                .metadata_addr
                .as_ref()
                .ok_or_else(|| Status::failed_precondition("FILE writes are not configured"))?;
            let channel = self
                .metadata_security
                .connect(addr.clone())
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?;
            let mut client = FlightClient::new(channel);
            self.metadata_security
                .apply_to_flight_client(&mut client)
                .map_err(|error| Status::internal(error.to_string()))?;
            apply_delegated_append_scope(
                client.metadata_mut(),
                &principal,
                &append.table().namespace.0,
            )?;
            let results = client
                .do_put(request.into_inner().map(|item| {
                    item.map_err(|error| {
                        arrow_flight::error::FlightError::protocol(error.to_string())
                    })
                }))
                .await
                .map_err(forwarded_append_error)?
                .map_err(forwarded_append_error)
                .try_collect::<Vec<_>>()
                .await?;
            self.engine.invalidate_registration(append.table()).await;
            let stream: <Self as FlightService>::DoPutStream =
                Box::pin(futures::stream::iter(results.into_iter().map(Ok)));
            Ok(Response::new(stream))
        }
        .instrument(span.clone())
        .await;
        finish_stream_rpc(&span, result)
    }

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let span = query_server_span(&request, "do_action");
        let result = async move {
            let principal = self.principal(&request)?;
            let action = request.into_inner();
            if action.r#type != MANAGED_STAGE_DISCOVERY_ACTION {
                return Err(Status::invalid_argument(format!(
                    "unknown query action '{}'",
                    action.r#type
                )));
            }
            if !action.body.is_empty() {
                return Err(Status::invalid_argument(
                    "managed-stage discovery action body must be empty",
                ));
            }
            let descriptor = self.managed_stage.as_ref().ok_or_else(|| {
                Status::failed_precondition("managed FILE stage is not configured")
            })?;
            let body = descriptor
                .scope_to_tenant(principal.tenant())
                .to_wire()
                .map_err(|error| Status::internal(error.to_string()))?;
            let stream =
                futures::stream::once(async move { Ok(FlightResult { body: body.into() }) });
            let stream: <Self as FlightService>::DoActionStream = Box::pin(stream);
            Ok(Response::new(stream))
        }
        .instrument(span.clone())
        .await;
        finish_stream_rpc(&span, result)
    }

    async fn list_custom_actions(&self) -> Option<Vec<std::result::Result<ActionType, Status>>> {
        Some(vec![Ok(ActionType {
            r#type:      MANAGED_STAGE_DISCOVERY_ACTION.to_owned(),
            description: "Return the versioned credential-free managed FILE stage".to_owned(),
        })])
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Mutex as StdMutex, RwLock,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use arrow_flight::{
        IpcMessage, SchemaAsIpc,
        decode::FlightRecordBatchStream,
        sql::{CommandGetDbSchemas, CommandGetTables},
    };
    use async_trait::async_trait;
    use datafusion::{
        arrow::{
            array::{BinaryArray, Int64Array, StringArray},
            datatypes::{DataType, Field},
            ipc::writer::{IpcWriteOptions, StreamWriter},
            record_batch::RecordBatch,
        },
        catalog::{TableProvider, streaming::StreamingTable},
        datasource::MemTable,
        error::DataFusionError,
        execution::TaskContext,
        physical_plan::{
            SendableRecordBatchStream, stream::RecordBatchStreamAdapter, streaming::PartitionStream,
        },
    };
    use futures::{StreamExt, TryStreamExt};
    use lake_common::{
        MANAGED_STAGE_DISCOVERY_ACTION, ManagedStageDescriptor, Principal, PrincipalId,
        PrincipalRole, TableLocation, TableRef, TenantId, Version,
    };
    use lake_engine::{
        EngineError, ObjectReferencePage, ObjectReferenceRequest, TableEngine, TableEngineRef,
        TableHandle, TableHandleRef,
    };
    use lake_engine_lance::LanceEngine;
    use lake_meta::{MetaStore, MetaStoreRef, RocksMeta, registry, registry::TableRegistration};
    use lake_objects::LocalObjectStore;
    use tokio::{io::AsyncWriteExt, sync::Notify};

    use super::*;
    use crate::{
        async_ipc::{IpcPipelineLimits, PipelineProbe, decode_ipc_reader},
        async_query::{
            AsyncQueryCoordinator, AsyncQueryWorker, AsyncResourceLimits, WorkerIdentity,
        },
        ticket::{QueryTicketKeyRing, StatementTicketCodec},
    };

    #[test]
    fn flight_sql_patterns_follow_percent_and_underscore_semantics() {
        for (value, pattern, expected) in [
            ("events", "events", true),
            ("events", "event", false),
            ("alpha_episodes", "alpha_%", true),
            ("alpha_episodes", "%episode_", true),
            ("episodes", "e%z", false),
            ("模型_v2", "模型__2", true),
            ("模型_v2", "模型_2", false),
            ("", "%", true),
            ("", "_", false),
        ] {
            assert_eq!(
                flight_sql_pattern_matches(value, pattern),
                expected,
                "value={value:?}, pattern={pattern:?}"
            );
        }
    }

    struct EmptyMeta;

    type TestProviders = Arc<StdMutex<HashMap<(TableLocation, u64), Arc<dyn TableProvider>>>>;

    #[derive(Clone, Default)]
    struct VersionedTestEngine {
        providers: TestProviders,
        opens:     Arc<StdMutex<Vec<TableLocation>>>,
    }

    impl VersionedTestEngine {
        fn insert(
            &self,
            location: &TableLocation,
            version: Version,
            provider: Arc<dyn TableProvider>,
        ) {
            self.providers
                .lock()
                .unwrap()
                .insert((location.clone(), version.0), provider);
        }

        fn remove_location(&self, location: &TableLocation) {
            self.providers
                .lock()
                .unwrap()
                .retain(|(candidate, _), _| candidate != location);
        }
    }

    struct VersionedTestHandle {
        location:  TableLocation,
        schema:    SchemaRef,
        providers: TestProviders,
    }

    #[async_trait]
    impl TableEngine for VersionedTestEngine {
        fn kind(&self) -> &'static str { "versioned-test" }

        async fn create(
            &self,
            _location: &TableLocation,
            _schema: SchemaRef,
        ) -> lake_engine::Result<TableHandleRef> {
            unreachable!()
        }

        async fn open(
            &self,
            location: &TableLocation,
        ) -> lake_engine::Result<Option<TableHandleRef>> {
            self.opens.lock().unwrap().push(location.clone());
            let providers = self.providers.lock().unwrap();
            let schema = providers
                .iter()
                .find(|((candidate, _), _)| candidate == location)
                .map(|(_, provider)| provider.schema());
            drop(providers);
            Ok(schema.map(|schema| {
                Arc::new(VersionedTestHandle {
                    location: location.clone(),
                    schema,
                    providers: self.providers.clone(),
                }) as TableHandleRef
            }))
        }

        async fn remove(&self, location: &TableLocation) -> lake_engine::Result<()> {
            self.remove_location(location);
            Ok(())
        }

        async fn maintain(
            &self,
            _location: &TableLocation,
            _version: Version,
        ) -> lake_engine::Result<Option<Version>> {
            Ok(None)
        }

        async fn retained_object_references(
            &self,
            _location: &TableLocation,
            _request: ObjectReferenceRequest,
        ) -> lake_engine::Result<ObjectReferencePage> {
            unreachable!()
        }
    }

    #[async_trait]
    impl TableHandle for VersionedTestHandle {
        fn schema(&self) -> SchemaRef { self.schema.clone() }

        fn current_version(&self) -> Version {
            self.providers
                .lock()
                .unwrap()
                .keys()
                .filter(|(location, _)| location == &self.location)
                .map(|(_, version)| Version(*version))
                .max()
                .unwrap_or(Version(0))
        }

        async fn table_provider(
            &self,
            version: Version,
        ) -> lake_engine::Result<Arc<dyn TableProvider>> {
            self.providers
                .lock()
                .unwrap()
                .get(&(self.location.clone(), version.0))
                .cloned()
                .ok_or_else(|| {
                    EngineError::backend(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("snapshot {version} was reclaimed"),
                    ))
                })
        }

        async fn append(
            &self,
            _operation: &lake_common::AppendOperation,
            _batches: SendableRecordBatchStream,
        ) -> lake_engine::Result<Version> {
            unreachable!()
        }

        async fn reconcile_append(
            &self,
            _operation: &lake_common::AppendOperation,
        ) -> lake_engine::Result<Option<Version>> {
            unreachable!()
        }
    }

    #[async_trait]
    impl MetaStore for EmptyMeta {
        async fn get(&self, _key: &str) -> lake_meta::Result<Option<Vec<u8>>> { Ok(None) }

        async fn cas(
            &self,
            _key: &str,
            _expected: Option<&[u8]>,
            _new: &[u8],
        ) -> lake_meta::Result<bool> {
            Ok(true)
        }

        async fn list_prefix(&self, _prefix: &str) -> lake_meta::Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn delete(&self, _key: &str, _expected: &[u8]) -> lake_meta::Result<bool> { Ok(true) }
    }

    #[derive(Default)]
    struct PlanningMeta {
        scans: AtomicUsize,
    }

    struct DiscoveryMeta;

    struct SchemaDiscoveryMeta {
        entries:    RwLock<Vec<(String, Vec<u8>)>>,
        operations: AtomicUsize,
    }

    impl SchemaDiscoveryMeta {
        fn new(entries: Vec<(String, Vec<u8>)>) -> Self {
            Self {
                entries:    RwLock::new(entries),
                operations: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl MetaStore for SchemaDiscoveryMeta {
        async fn get(&self, _key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
            self.operations.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }

        async fn cas(
            &self,
            _key: &str,
            _expected: Option<&[u8]>,
            _new: &[u8],
        ) -> lake_meta::Result<bool> {
            unreachable!()
        }

        async fn list_prefix(&self, _prefix: &str) -> lake_meta::Result<Vec<String>> {
            self.operations.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }

        async fn scan_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<(String, Vec<u8>)>> {
            self.operations.fetch_add(1, Ordering::Relaxed);
            assert_eq!(prefix, "tbl/");
            Ok(self.entries.read().unwrap().clone())
        }

        async fn delete(&self, _key: &str, _expected: &[u8]) -> lake_meta::Result<bool> {
            unreachable!()
        }
    }

    fn registration_with_schema(schema: &Schema) -> Vec<u8> {
        let IpcMessage(schema_ipc) = SchemaAsIpc::new(schema, &IpcWriteOptions::default())
            .try_into()
            .unwrap();
        serde_json::to_vec(&TableRegistration::new(
            TableLocation::new("mem://table"),
            "lance",
            Version(1),
            schema_ipc.to_vec(),
        ))
        .unwrap()
    }

    fn snapshot_registration(
        location: &TableLocation,
        version: Version,
        schema: &Schema,
    ) -> TableRegistration {
        let IpcMessage(schema_ipc) = SchemaAsIpc::new(schema, &IpcWriteOptions::default())
            .try_into()
            .unwrap();
        TableRegistration::new(
            location.clone(),
            "versioned-test",
            version,
            schema_ipc.to_vec(),
        )
    }

    fn int_provider(schema: SchemaRef, values: Vec<i64>) -> Arc<dyn TableProvider> {
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    fn snapshot_service(
        meta: MetaStoreRef,
        engine: TableEngineRef,
        ticket_codec: StatementTicketCodec,
    ) -> FlightSqlServiceImpl {
        FlightSqlServiceImpl {
            engine: Arc::new(QueryEngine::new(meta, engine)),
            metadata_addr: None,
            metadata_security: ClientSecurity::new(),
            managed_stage: None,
            admission: QueryAdmission::new(QueryLimits::default()),
            discovery_limits: DiscoveryLimits::default(),
            ticket_codec,
        }
    }

    fn robots_reader() -> Principal {
        Principal::try_new(
            PrincipalId::try_new("robots-reader").unwrap(),
            TenantId::try_new("robots-tenant").unwrap(),
            PrincipalRole::User,
            ["robots"],
        )
        .unwrap()
    }

    async fn issue_statement_ticket(
        service: &FlightSqlServiceImpl,
        sql: &str,
        principal: &Principal,
    ) -> TicketStatementQuery {
        let mut request = Request::new(FlightDescriptor::default());
        request.extensions_mut().insert(principal.clone());
        let info = service
            .get_flight_info_statement(
                CommandStatementQuery {
                    query:          sql.to_owned(),
                    transaction_id: None,
                },
                request,
            )
            .await
            .expect("issue statement ticket")
            .into_inner();
        let outer = info.endpoint[0].ticket.as_ref().expect("endpoint ticket");
        let any = Any::decode(&*outer.ticket).expect("standard Flight SQL ticket envelope");
        TicketStatementQuery::decode(&*any.value).expect("statement ticket")
    }

    async fn execute_statement_ticket(
        service: &FlightSqlServiceImpl,
        ticket: TicketStatementQuery,
        principal: &Principal,
    ) -> std::result::Result<Vec<RecordBatch>, Status> {
        let mut request = Request::new(Ticket::default());
        request.extensions_mut().insert(principal.clone());
        let stream = service
            .do_get_statement(ticket, request)
            .await?
            .into_inner();
        FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(arrow_flight::error::FlightError::from),
        )
        .try_collect::<Vec<_>>()
        .await
        .map_err(Status::from)
    }

    fn test_ticket_keys() -> QueryTicketKeyRing {
        QueryTicketKeyRing::try_new(b"query-test-ticket-key-material-000001", std::iter::empty())
            .unwrap()
    }

    fn test_ticket_codec() -> StatementTicketCodec {
        StatementTicketCodec::try_new(test_ticket_keys(), Duration::from_mins(5), "lake-query")
            .unwrap()
    }

    async fn discovery_service(
        table_keys: &[&str],
        discovery_limits: DiscoveryLimits,
    ) -> FlightSqlServiceImpl {
        let schema = Schema::new(vec![Field::new("value", DataType::Utf8, false)]);
        let entries = table_keys
            .iter()
            .map(|key| ((*key).to_owned(), registration_with_schema(&schema)))
            .collect();
        let meta: MetaStoreRef = Arc::new(SchemaDiscoveryMeta::new(entries));
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let engine = Arc::new(QueryEngine::new(meta, storage));
        engine.refresh().await.expect("warm discovery catalog");
        FlightSqlServiceImpl {
            engine,
            metadata_addr: None,
            metadata_security: ClientSecurity::new(),
            managed_stage: None,
            admission: QueryAdmission::new(QueryLimits::default()),
            discovery_limits,
            ticket_codec: test_ticket_codec(),
        }
    }

    fn admin_ticket_request() -> Request<Ticket> {
        let mut request = Request::new(Ticket::default());
        request
            .extensions_mut()
            .insert(Principal::deployment_admin());
        request
    }

    #[async_trait]
    impl MetaStore for DiscoveryMeta {
        async fn get(&self, _key: &str) -> lake_meta::Result<Option<Vec<u8>>> { Ok(None) }

        async fn cas(
            &self,
            _key: &str,
            _expected: Option<&[u8]>,
            _new: &[u8],
        ) -> lake_meta::Result<bool> {
            Ok(true)
        }

        async fn list_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<String>> {
            Ok(match prefix {
                "tbl/" => vec![
                    "alpha_episodes/events".to_owned(),
                    "beta_episodes/secrets".to_owned(),
                ],
                "tbl/alpha_episodes/" => vec!["events".to_owned()],
                "tbl/beta_episodes/" => vec!["secrets".to_owned()],
                _ => Vec::new(),
            })
        }

        async fn scan_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<(String, Vec<u8>)>> {
            assert_eq!(prefix, "tbl/");
            let registration =
                br#"{"location":"mem://table","engine":"lance","current_version":1}"#;
            Ok(vec![
                ("alpha_episodes/events".to_owned(), registration.to_vec()),
                ("beta_episodes/secrets".to_owned(), registration.to_vec()),
            ])
        }

        async fn delete(&self, _key: &str, _expected: &[u8]) -> lake_meta::Result<bool> { Ok(true) }
    }

    #[async_trait]
    impl MetaStore for PlanningMeta {
        async fn get(&self, _key: &str) -> lake_meta::Result<Option<Vec<u8>>> { Ok(None) }

        async fn cas(
            &self,
            _key: &str,
            _expected: Option<&[u8]>,
            _new: &[u8],
        ) -> lake_meta::Result<bool> {
            Ok(true)
        }

        async fn list_prefix(&self, _prefix: &str) -> lake_meta::Result<Vec<String>> {
            self.scans.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }

        async fn delete(&self, _key: &str, _expected: &[u8]) -> lake_meta::Result<bool> { Ok(true) }
    }

    #[derive(Debug)]
    struct DelayedPartition {
        schema:  SchemaRef,
        release: Arc<Notify>,
    }

    impl PartitionStream for DelayedPartition {
        fn schema(&self) -> &SchemaRef { &self.schema }

        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            let first = RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )
            .unwrap();
            let second = RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(Int64Array::from(vec![2]))],
            )
            .unwrap();
            let release = self.release.clone();
            let batches = futures::stream::once(async move { Ok(first) }).chain(
                futures::stream::once(async move {
                    release.notified().await;
                    Ok::<_, DataFusionError>(second)
                }),
            );
            Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), batches))
        }
    }

    #[tokio::test]
    async fn managed_stage_action_returns_configured_descriptor() {
        let meta: MetaStoreRef = Arc::new(EmptyMeta);
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let descriptor = ManagedStageDescriptor::s3(
            "embodied-data",
            "managed-objects",
            Some("us-east-1".to_owned()),
            None,
            false,
        );
        let service = FlightSqlServiceImpl {
            engine:            Arc::new(QueryEngine::new(meta, storage)),
            metadata_addr:     None,
            metadata_security: ClientSecurity::new(),
            managed_stage:     Some(descriptor.clone()),
            admission:         QueryAdmission::new(QueryLimits::default()),
            discovery_limits:  DiscoveryLimits::default(),
            ticket_codec:      test_ticket_codec(),
        };
        let action = arrow_flight::Action {
            r#type: MANAGED_STAGE_DISCOVERY_ACTION.to_owned(),
            body:   Vec::new().into(),
        };
        let error = service
            .do_action_fallback(Request::new(action.clone()))
            .await
            .err()
            .expect("missing principal must fail closed");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        let mut request = Request::new(action);
        request
            .extensions_mut()
            .insert(Principal::deployment_admin());

        let results = service
            .do_action_fallback(request)
            .await
            .expect("discovery action")
            .into_inner()
            .try_collect::<Vec<_>>()
            .await
            .expect("discovery results");

        assert_eq!(results.len(), 1);
        assert_eq!(
            ManagedStageDescriptor::from_wire(&results[0].body).expect("decode result"),
            descriptor.scope_to_tenant(&TenantId::try_new("deployment").unwrap())
        );
    }

    #[test]
    fn query_tenant_policy_denies_cross_namespace_before_execution() {
        let meta = Arc::new(PlanningMeta::default());
        let meta_ref: MetaStoreRef = meta.clone();
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let service = FlightSqlServiceImpl {
            engine:            Arc::new(QueryEngine::new(meta_ref, storage)),
            metadata_addr:     None,
            metadata_security: ClientSecurity::new(),
            managed_stage:     None,
            admission:         QueryAdmission::new(QueryLimits::default()),
            discovery_limits:  DiscoveryLimits::default(),
            ticket_codec:      test_ticket_codec(),
        };
        let principal = Principal::try_new(
            PrincipalId::try_new("alpha-reader").unwrap(),
            TenantId::try_new("tenant-alpha").unwrap(),
            PrincipalRole::User,
            ["alpha_episodes"],
        )
        .unwrap();

        service
            .authorize_sql(
                &principal,
                "WITH recent AS (SELECT * FROM lake.alpha_episodes.events) SELECT * FROM recent",
            )
            .expect("same-tenant CTE is authorized");
        let denied = service
            .authorize_sql(
                &principal,
                "SELECT * FROM lake.alpha_episodes.events a JOIN lake.beta_episodes.secrets b ON \
                 a.id = b.id",
            )
            .expect_err("cross-tenant join is denied");
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);
        assert_eq!(denied.message(), "resource is not available");
        assert_eq!(
            meta.scans.load(Ordering::Relaxed),
            0,
            "authorization must not consult the metastore"
        );
    }

    #[test]
    fn flight_dml_is_rejected_before_snapshot_resolution() {
        let meta = Arc::new(PlanningMeta::default());
        let meta_ref: MetaStoreRef = meta.clone();
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let service = FlightSqlServiceImpl {
            engine:            Arc::new(QueryEngine::new(meta_ref, storage)),
            metadata_addr:     None,
            metadata_security: ClientSecurity::new(),
            managed_stage:     None,
            admission:         QueryAdmission::new(QueryLimits::default()),
            discovery_limits:  DiscoveryLimits::default(),
            ticket_codec:      test_ticket_codec(),
        };

        let error = service
            .authorized_table_references(
                &Principal::deployment_admin(),
                "INSERT INTO lake.interop.missing VALUES (1)",
            )
            .expect_err("DML must fail before resolving the target table");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(error.message(), "DML not supported");
        assert_eq!(
            meta.scans.load(Ordering::Relaxed),
            0,
            "read-only rejection must not consult the catalog authority"
        );
    }

    #[test]
    fn statement_ticket_pins_joins_subqueries_and_ctes() {
        let meta: MetaStoreRef = Arc::new(EmptyMeta);
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let service = FlightSqlServiceImpl {
            engine:            Arc::new(QueryEngine::new(meta, storage)),
            metadata_addr:     None,
            metadata_security: ClientSecurity::new(),
            managed_stage:     None,
            admission:         QueryAdmission::new(QueryLimits::default()),
            discovery_limits:  DiscoveryLimits::default(),
            ticket_codec:      test_ticket_codec(),
        };
        let principal = Principal::try_new(
            PrincipalId::try_new("alpha-reader").unwrap(),
            TenantId::try_new("tenant-alpha").unwrap(),
            PrincipalRole::User,
            ["alpha"],
        )
        .unwrap();
        let sql = "WITH recent AS (SELECT * FROM lake.alpha.events) SELECT * FROM recent r JOIN \
                   lake.alpha.labels l ON r.id = l.id WHERE EXISTS (SELECT 1 FROM \
                   lake.alpha.annotations a WHERE a.id = r.id)";

        let references = service
            .authorized_table_references(&principal, sql)
            .expect("extract every authorized physical table");

        assert_eq!(
            references,
            vec![
                TableRef::new("alpha", "annotations"),
                TableRef::new("alpha", "events"),
                TableRef::new("alpha", "labels"),
            ]
        );
    }

    #[tokio::test]
    async fn statement_ticket_executes_original_snapshot_after_commit() {
        let directory = tempfile::tempdir().unwrap();
        let meta = Arc::new(RocksMeta::open(directory.path()).unwrap());
        let table = TableRef::new("robots", "episodes");
        let location = TableLocation::new("mem://robots/episodes/incarnation-a");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let registration = snapshot_registration(&location, Version(1), &schema);
        registry::register(meta.as_ref(), &table, &registration)
            .await
            .unwrap();
        let engine = VersionedTestEngine::default();
        engine.insert(&location, Version(1), int_provider(schema.clone(), vec![1]));
        engine.insert(&location, Version(2), int_provider(schema, vec![2]));
        let codec = test_ticket_codec();
        let engine_ref: TableEngineRef = Arc::new(engine.clone());
        let meta_ref: MetaStoreRef = meta.clone();
        let issuer = snapshot_service(meta_ref.clone(), engine_ref.clone(), codec.clone());
        let principal = robots_reader();

        let ticket = issue_statement_ticket(
            &issuer,
            "SELECT value FROM lake.robots.episodes",
            &principal,
        )
        .await;
        registry::set_version(meta.as_ref(), &table, &registration, Version(2))
            .await
            .unwrap();
        issuer.engine.invalidate_registration(&table).await;

        let executor = snapshot_service(meta_ref, engine_ref, codec);
        let batches = execute_statement_ticket(&executor, ticket, &principal)
            .await
            .expect("a different replica executes the issued snapshot");
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values(), &[1]);
    }

    #[tokio::test]
    async fn poll_flight_info_submits_identity_bound_pinned_job() {
        let catalog_directory = tempfile::tempdir().unwrap();
        let catalog_meta = Arc::new(RocksMeta::open(catalog_directory.path()).unwrap());
        let table = TableRef::new("robots", "episodes");
        let location = TableLocation::new("mem://robots/episodes/async-incarnation");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let registration = snapshot_registration(&location, Version(7), &schema);
        registry::register(catalog_meta.as_ref(), &table, &registration)
            .await
            .unwrap();
        let engine = VersionedTestEngine::default();
        engine.insert(&location, Version(7), int_provider(schema, vec![7]));
        let issuer = snapshot_service(catalog_meta, Arc::new(engine), test_ticket_codec());
        let worker_engine = issuer.engine.clone();
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
            test_ticket_keys(),
            Duration::from_hours(6),
            Duration::from_mins(5),
        )
        .unwrap();
        let service = TracedFlightSqlService::with_async_queries(issuer, coordinator.clone());
        let principal = robots_reader();
        let query = CommandStatementQuery {
            query:          "SELECT value FROM lake.robots.episodes".to_owned(),
            transaction_id: Some(bytes::Bytes::from_static(&[8_u8; 16])),
        };
        let initial_descriptor = FlightDescriptor::new_cmd(query.as_any().encode_to_vec());
        let mut request = Request::new(initial_descriptor.clone());
        request.extensions_mut().insert(principal.clone());

        let poll = FlightService::poll_flight_info(&service, request)
            .await
            .expect("submit async query")
            .into_inner();

        assert_eq!(poll.progress, Some(0.0));
        let next = poll.flight_descriptor.expect("retry descriptor");
        let query_id = coordinator
            .open_poll_handle(&next.cmd, &principal)
            .expect("identity-bound poll handle");
        let record = coordinator
            .store()
            .load(&query_id)
            .await
            .unwrap()
            .expect("durable queued query");
        let statement = coordinator
            .open_job(&record)
            .await
            .expect("encrypted pinned job specification");
        assert_eq!(statement.sql, query.query);
        assert_eq!(statement.snapshots.len(), 1);
        assert_eq!(statement.snapshots[0].version, 7);

        let replica = snapshot_service(
            Arc::new(EmptyMeta),
            Arc::new(LanceEngine::new()),
            test_ticket_codec(),
        );
        let replica = TracedFlightSqlService::with_async_queries(replica, coordinator.clone());
        let mut retried_submission = Request::new(initial_descriptor);
        retried_submission
            .extensions_mut()
            .insert(principal.clone());
        let retried_submission = FlightService::poll_flight_info(&replica, retried_submission)
            .await
            .expect("lost initial response retries without catalog access")
            .into_inner();
        assert_eq!(
            coordinator
                .open_poll_handle(
                    &retried_submission
                        .flight_descriptor
                        .expect("retry handle")
                        .cmd,
                    &principal,
                )
                .unwrap(),
            query_id
        );
        let mut follow_up = Request::new(next.clone());
        follow_up.extensions_mut().insert(principal.clone());
        assert!(
            FlightService::poll_flight_info(&replica, follow_up)
                .await
                .is_ok(),
            "another replica polls without catalog resolution"
        );
        let mut replay = Request::new(next.clone());
        replay.extensions_mut().insert(
            Principal::try_new(
                PrincipalId::try_new("other-reader").unwrap(),
                TenantId::try_new("robots-tenant").unwrap(),
                PrincipalRole::User,
                ["robots"],
            )
            .unwrap(),
        );
        let error = FlightService::poll_flight_info(&replica, replay)
            .await
            .expect_err("another principal cannot poll the capability");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);

        AsyncQueryWorker::try_new(
            coordinator,
            worker_engine,
            WorkerIdentity::new([9; 16]),
            Duration::from_secs(30),
        )
        .unwrap()
        .run(&query_id, Duration::from_secs(30))
        .await
        .expect("worker materializes the pinned query");
        let mut completed = Request::new(next);
        completed.extensions_mut().insert(principal.clone());
        let completed = FlightService::poll_flight_info(&replica, completed)
            .await
            .expect("completed poll returns result endpoints")
            .into_inner();
        assert_eq!(completed.progress, Some(1.0));
        assert!(completed.flight_descriptor.is_none());
        let endpoint = &completed.info.expect("completed FlightInfo").endpoint[0];
        let mut result_request = Request::new(endpoint.ticket.clone().expect("result ticket"));
        result_request.extensions_mut().insert(principal);
        let batches = FlightRecordBatchStream::new_from_flight_data(
            FlightService::do_get(&replica, result_request)
                .await
                .expect("redeem async result")
                .into_inner()
                .map_err(arrow_flight::error::FlightError::from),
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values(), &[7]);
    }

    #[tokio::test]
    async fn async_tenant_quota_is_identity_free_resource_exhausted() {
        let catalog_directory = tempfile::tempdir().unwrap();
        let catalog: MetaStoreRef = Arc::new(RocksMeta::open(catalog_directory.path()).unwrap());
        let service = snapshot_service(catalog, Arc::new(LanceEngine::new()), test_ticket_codec());
        let state_directory = tempfile::tempdir().unwrap();
        let state: MetaStoreRef = Arc::new(RocksMeta::open(state_directory.path()).unwrap());
        let object_directory = tempfile::tempdir().unwrap();
        let objects = Arc::new(
            LocalObjectStore::open(object_directory.path())
                .await
                .unwrap(),
        );
        let coordinator = AsyncQueryCoordinator::try_new_with_resources(
            state,
            objects,
            test_ticket_keys(),
            Duration::from_hours(1),
            Duration::from_mins(5),
            AsyncResourceLimits::try_new(1, 64 << 20).unwrap(),
        )
        .unwrap();
        let service = TracedFlightSqlService::with_async_queries(service, coordinator);
        let principal = robots_reader();
        for submission_id in [[1_u8; 16]] {
            let query = CommandStatementQuery {
                query:          "SELECT 1".to_owned(),
                transaction_id: Some(bytes::Bytes::copy_from_slice(&submission_id)),
            };
            let mut request =
                Request::new(FlightDescriptor::new_cmd(query.as_any().encode_to_vec()));
            request.extensions_mut().insert(principal.clone());
            FlightService::poll_flight_info(&service, request)
                .await
                .expect("first submission is admitted");
        }
        let query = CommandStatementQuery {
            query:          "SELECT 1".to_owned(),
            transaction_id: Some(bytes::Bytes::from_static(&[2_u8; 16])),
        };
        let mut request = Request::new(FlightDescriptor::new_cmd(query.as_any().encode_to_vec()));
        request.extensions_mut().insert(principal);
        let error = FlightService::poll_flight_info(&service, request)
            .await
            .expect_err("second retained job exceeds durable quota");
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            error.message(),
            "async tenant outstanding query limit reached"
        );
        assert!(!error.message().contains("robots-tenant"));
    }

    #[tokio::test]
    async fn cancel_flight_info_fences_execution_and_reaps_partial_results() {
        let catalog_directory = tempfile::tempdir().unwrap();
        let catalog: MetaStoreRef = Arc::new(RocksMeta::open(catalog_directory.path()).unwrap());
        let service = snapshot_service(catalog, Arc::new(LanceEngine::new()), test_ticket_codec());
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
            test_ticket_keys(),
            Duration::from_hours(1),
            Duration::from_mins(5),
        )
        .unwrap();
        let service = TracedFlightSqlService::with_async_queries(service, coordinator.clone());
        let principal = robots_reader();
        let query = CommandStatementQuery {
            query:          "SELECT 1".to_owned(),
            transaction_id: None,
        };
        let mut submit = Request::new(FlightDescriptor::new_cmd(query.as_any().encode_to_vec()));
        submit.extensions_mut().insert(principal.clone());
        let poll = FlightService::poll_flight_info(&service, submit)
            .await
            .unwrap()
            .into_inner();
        let retry = poll.flight_descriptor.clone().unwrap();
        let query_id = coordinator
            .open_poll_handle(&retry.cmd, &principal)
            .unwrap();
        let cancel = CancelFlightInfoRequest::new(poll.info.unwrap());
        let cancel_body = cancel.encode_to_vec();
        let mut cancel_request = Request::new(Action::new("CancelFlightInfo", cancel_body.clone()));
        cancel_request.extensions_mut().insert(principal.clone());
        let result = FlightService::do_action(&service, cancel_request)
            .await
            .unwrap()
            .into_inner()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let cancelled = CancelFlightInfoResult::decode(&*result[0].body).unwrap();
        assert_eq!(cancelled.status(), CancelStatus::Cancelled);
        let mut repeated = Request::new(Action::new("CancelFlightInfo", cancel_body));
        repeated.extensions_mut().insert(principal.clone());
        let repeated = FlightService::do_action(&service, repeated)
            .await
            .unwrap()
            .into_inner()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            CancelFlightInfoResult::decode(&*repeated[0].body)
                .unwrap()
                .status(),
            CancelStatus::Cancelled
        );
        assert!(
            coordinator
                .store()
                .load(&query_id)
                .await
                .unwrap()
                .unwrap()
                .is_cancelled()
        );
        let mut follow_up = Request::new(retry);
        follow_up.extensions_mut().insert(principal);
        let error = FlightService::poll_flight_info(&service, follow_up)
            .await
            .expect_err("cancelled job cannot be polled as pending");
        assert_eq!(error.code(), tonic::Code::Cancelled);
        let expires_at = coordinator
            .store()
            .load(&query_id)
            .await
            .unwrap()
            .unwrap()
            .expires_at();
        assert!(
            !coordinator
                .cleanup_if_expired(&query_id, expires_at)
                .await
                .unwrap()
        );
        assert!(
            coordinator
                .cleanup_if_expired(&query_id, expires_at + 300)
                .await
                .unwrap()
        );
        assert!(coordinator.store().load(&query_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn async_submission_id_retries_converge_on_one_job() {
        let catalog_directory = tempfile::tempdir().unwrap();
        let catalog: MetaStoreRef = Arc::new(RocksMeta::open(catalog_directory.path()).unwrap());
        let service = snapshot_service(catalog, Arc::new(LanceEngine::new()), test_ticket_codec());
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
            test_ticket_keys(),
            Duration::from_hours(1),
            Duration::from_mins(5),
        )
        .unwrap();
        let service = TracedFlightSqlService::with_async_queries(service, coordinator.clone());
        let principal = robots_reader();
        let descriptor = |sql: &str| {
            FlightDescriptor::new_cmd(
                CommandStatementQuery {
                    query:          sql.to_owned(),
                    transaction_id: Some(bytes::Bytes::from_static(&[7_u8; 16])),
                }
                .as_any()
                .encode_to_vec(),
            )
        };
        let poll = |descriptor: FlightDescriptor| {
            let mut request = Request::new(descriptor);
            request.extensions_mut().insert(principal.clone());
            request
        };

        let first = FlightService::poll_flight_info(&service, poll(descriptor("SELECT 1")))
            .await
            .unwrap()
            .into_inner();
        let retried = FlightService::poll_flight_info(&service, poll(descriptor("SELECT 1")))
            .await
            .unwrap()
            .into_inner();
        let first_id = coordinator
            .open_poll_handle(&first.flight_descriptor.unwrap().cmd, &principal)
            .unwrap();
        let retried_id = coordinator
            .open_poll_handle(&retried.flight_descriptor.unwrap().cmd, &principal)
            .unwrap();
        assert_eq!(first_id, retried_id);
        let (records, invalid, continuation) =
            coordinator.store().list_records_page(None).await.unwrap();
        let query_ids = records
            .iter()
            .map(|record| record.query_id())
            .collect::<Vec<_>>();
        assert_eq!(query_ids, [first_id]);
        assert!(continuation.is_none());
        assert_eq!(invalid, 0);

        let error = FlightService::poll_flight_info(&service, poll(descriptor("SELECT 2")))
            .await
            .expect_err("submission id cannot alias another statement");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn statement_ticket_never_redirects_to_recreated_table() {
        let directory = tempfile::tempdir().unwrap();
        let meta = Arc::new(RocksMeta::open(directory.path()).unwrap());
        let table = TableRef::new("robots", "episodes");
        let old_location = TableLocation::new("mem://robots/episodes/incarnation-old");
        let new_location = TableLocation::new("mem://robots/episodes/incarnation-new");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let old_registration = snapshot_registration(&old_location, Version(1), &schema);
        registry::register(meta.as_ref(), &table, &old_registration)
            .await
            .unwrap();
        let engine = VersionedTestEngine::default();
        engine.insert(
            &old_location,
            Version(1),
            int_provider(schema.clone(), vec![1]),
        );
        let codec = test_ticket_codec();
        let engine_ref: TableEngineRef = Arc::new(engine.clone());
        let meta_ref: MetaStoreRef = meta.clone();
        let issuer = snapshot_service(meta_ref.clone(), engine_ref.clone(), codec.clone());
        let principal = robots_reader();
        let ticket = issue_statement_ticket(
            &issuer,
            "SELECT value FROM lake.robots.episodes",
            &principal,
        )
        .await;

        registry::delete(meta.as_ref(), &table, &old_registration)
            .await
            .unwrap();
        let new_registration = snapshot_registration(&new_location, Version(1), &schema);
        registry::register(meta.as_ref(), &table, &new_registration)
            .await
            .unwrap();
        engine.remove_location(&old_location);
        engine.insert(&new_location, Version(1), int_provider(schema, vec![99]));
        engine.opens.lock().unwrap().clear();

        let executor = snapshot_service(meta_ref, engine_ref, codec);
        let error = execute_statement_ticket(&executor, ticket, &principal)
            .await
            .expect_err("reclaimed old incarnation must fail explicitly");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(error.message(), "the pinned table snapshot is unavailable");
        assert!(!error.message().contains("mem://"));
        assert_eq!(
            *engine.opens.lock().unwrap(),
            vec![old_location],
            "DoGet must not resolve or open the replacement location"
        );
    }

    #[tokio::test]
    async fn statement_ticket_replay_is_rejected_before_planning() {
        let meta = Arc::new(PlanningMeta::default());
        let meta_ref: MetaStoreRef = meta.clone();
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let codec = StatementTicketCodec::try_new(
            QueryTicketKeyRing::try_new(
                b"rpc-ticket-key-material-000000000001",
                std::iter::empty(),
            )
            .unwrap(),
            Duration::from_mins(5),
            "lake-query",
        )
        .unwrap();
        let service = FlightSqlServiceImpl {
            engine:            Arc::new(QueryEngine::new(meta_ref, storage)),
            metadata_addr:     None,
            metadata_security: ClientSecurity::new(),
            managed_stage:     None,
            admission:         QueryAdmission::new(QueryLimits::default()),
            discovery_limits:  DiscoveryLimits::default(),
            ticket_codec:      codec.clone(),
        };
        let principal = |id: &str| {
            Principal::try_new(
                PrincipalId::try_new(id).unwrap(),
                TenantId::try_new("tenant-alpha").unwrap(),
                PrincipalRole::User,
                ["alpha_episodes"],
            )
            .unwrap()
        };
        let alice = principal("alice");
        let handle = codec.seal("SELECT 1", &alice).unwrap();
        let ticket = || TicketStatementQuery {
            statement_handle: handle.clone().into(),
        };
        let request = |principal: Principal| {
            let mut request = Request::new(Ticket::default());
            request.extensions_mut().insert(principal);
            request
        };

        let replay = match service
            .do_get_statement(ticket(), request(principal("bob")))
            .await
        {
            Err(status) => status,
            Ok(_) => panic!("a ticket is bound to its issuing principal"),
        };

        assert_eq!(replay.code(), tonic::Code::Unauthenticated);
        assert_eq!(replay.message(), "invalid statement ticket");
        assert_eq!(meta.scans.load(Ordering::Relaxed), 0);
        assert!(
            service
                .do_get_statement(ticket(), request(alice))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn query_discovery_filters_unauthorized_namespaces() {
        let meta: MetaStoreRef = Arc::new(DiscoveryMeta);
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let engine = Arc::new(QueryEngine::new(meta, storage));
        engine.refresh().await.unwrap();
        let service = FlightSqlServiceImpl {
            engine,
            metadata_addr: None,
            metadata_security: ClientSecurity::new(),
            managed_stage: None,
            admission: QueryAdmission::new(QueryLimits::default()),
            discovery_limits: DiscoveryLimits::default(),
            ticket_codec: test_ticket_codec(),
        };
        let principal = Principal::try_new(
            PrincipalId::try_new("alpha-reader").unwrap(),
            TenantId::try_new("tenant-alpha").unwrap(),
            PrincipalRole::User,
            ["alpha_episodes"],
        )
        .unwrap();
        let request = || {
            let mut request = Request::new(Ticket::default());
            request.extensions_mut().insert(principal.clone());
            request
        };

        let schema_stream = service
            .do_get_schemas(
                CommandGetDbSchemas {
                    catalog:                  None,
                    db_schema_filter_pattern: None,
                },
                request(),
            )
            .await
            .unwrap()
            .into_inner();
        let schema_batches = FlightRecordBatchStream::new_from_flight_data(
            schema_stream.map_err(arrow_flight::error::FlightError::from),
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        let namespaces = schema_batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            namespaces.iter().collect::<Vec<_>>(),
            vec![Some("alpha_episodes")]
        );

        let table_stream = service
            .do_get_tables(
                CommandGetTables {
                    catalog:                   None,
                    db_schema_filter_pattern:  None,
                    table_name_filter_pattern: None,
                    table_types:               Vec::new(),
                    include_schema:            false,
                },
                request(),
            )
            .await
            .unwrap()
            .into_inner();
        let table_batches = FlightRecordBatchStream::new_from_flight_data(
            table_stream.map_err(arrow_flight::error::FlightError::from),
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        let table_names = table_batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(table_names.iter().collect::<Vec<_>>(), vec![Some("events")]);
    }

    #[tokio::test]
    async fn flight_table_discovery_streams_bounded_batches() {
        let service = discovery_service(
            &[
                "alpha/events_0",
                "alpha/events_1",
                "alpha/events_2",
                "alpha/events_3",
                "alpha/events_4",
            ],
            DiscoveryLimits::try_new(10, 2).expect("limits"),
        )
        .await;
        let stream = service
            .do_get_tables(
                CommandGetTables {
                    catalog:                   Some("lake".to_owned()),
                    db_schema_filter_pattern:  Some("alpha".to_owned()),
                    table_name_filter_pattern: Some("events_%".to_owned()),
                    table_types:               vec!["TABLE".to_owned()],
                    include_schema:            false,
                },
                admin_ticket_request(),
            )
            .await
            .expect("table discovery")
            .into_inner();
        let batches = FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(arrow_flight::error::FlightError::from),
        )
        .try_collect::<Vec<_>>()
        .await
        .expect("decode table discovery");

        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
        let names = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("table names")
                    .iter()
                    .flatten()
                    .map(str::to_owned)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec!["events_0", "events_1", "events_2", "events_3", "events_4"]
        );
    }

    #[tokio::test]
    async fn flight_schema_discovery_streams_bounded_batches() {
        let service = discovery_service(
            &[
                "alpha/events",
                "bravo/events",
                "charlie/events",
                "delta/events",
                "echo/events",
            ],
            DiscoveryLimits::try_new(10, 2).expect("limits"),
        )
        .await;
        let stream = service
            .do_get_schemas(
                CommandGetDbSchemas {
                    catalog:                  Some("lake".to_owned()),
                    db_schema_filter_pattern: Some("%".to_owned()),
                },
                admin_ticket_request(),
            )
            .await
            .expect("schema discovery")
            .into_inner();
        let batches = FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(arrow_flight::error::FlightError::from),
        )
        .try_collect::<Vec<_>>()
        .await
        .expect("decode schema discovery");

        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
        let namespaces = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("schema names")
                    .iter()
                    .flatten()
                    .map(str::to_owned)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            namespaces,
            vec!["alpha", "bravo", "charlie", "delta", "echo"]
        );
    }

    #[tokio::test]
    async fn flight_discovery_stops_at_configured_row_limit() {
        let service = discovery_service(
            &[
                "alpha/events_0",
                "alpha/events_1",
                "alpha/events_2",
                "alpha/events_3",
            ],
            DiscoveryLimits::try_new(3, 2).expect("limits"),
        )
        .await;
        let stream = service
            .do_get_tables(
                CommandGetTables {
                    catalog:                   None,
                    db_schema_filter_pattern:  None,
                    table_name_filter_pattern: None,
                    table_types:               Vec::new(),
                    include_schema:            false,
                },
                admin_ticket_request(),
            )
            .await
            .expect("table discovery admitted")
            .into_inner();
        let mut batches = FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(arrow_flight::error::FlightError::from),
        );
        let mut batch_rows = Vec::new();
        let failure = loop {
            match batches.next().await {
                Some(Ok(batch)) => batch_rows.push(batch.num_rows()),
                Some(Err(error)) => break error,
                None => panic!("row limit must terminate the stream"),
            }
        };

        assert_eq!(batch_rows, vec![2, 1]);
        let FlightError::Tonic(status) = failure else {
            panic!("row limit must remain a tonic status: {failure}");
        };
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(status.message(), "discovery row limit reached");
    }

    #[tokio::test]
    async fn flight_discovery_error_releases_tenant_admission_permit() {
        let mut service = discovery_service(
            &["alpha/events_0", "alpha/events_1"],
            DiscoveryLimits::try_new(1, 1).expect("discovery limits"),
        )
        .await;
        let query_limits =
            QueryLimits::try_new(1, Duration::from_millis(20), Duration::from_secs(5), 1024)
                .expect("query limits");
        service.admission = QueryAdmission::new(query_limits);
        let mut failed_stream = service
            .do_get_tables(
                CommandGetTables {
                    catalog:                   None,
                    db_schema_filter_pattern:  None,
                    table_name_filter_pattern: None,
                    table_types:               Vec::new(),
                    include_schema:            false,
                },
                admin_ticket_request(),
            )
            .await
            .expect("first discovery admitted")
            .into_inner();

        loop {
            match failed_stream.next().await {
                Some(Ok(_)) => {}
                Some(Err(status)) => {
                    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
                    break;
                }
                None => panic!("row limit must fail the stream"),
            }
        }

        assert!(
            service
                .do_get_schemas(
                    CommandGetDbSchemas {
                        catalog:                  None,
                        db_schema_filter_pattern: None,
                    },
                    admin_ticket_request(),
                )
                .await
                .is_ok(),
            "reading a terminal stream error must release admission immediately"
        );
        drop(failed_stream);
    }

    fn admission_principal(subject: &str, tenant: &str) -> Principal {
        Principal::try_new(
            PrincipalId::try_new(subject).expect("valid admission principal"),
            TenantId::try_new(tenant).expect("valid admission tenant"),
            PrincipalRole::User,
            [tenant],
        )
        .expect("valid admission principal binding")
    }

    fn tenant_limits(global: usize, per_tenant: usize, tracked_tenants: usize) -> QueryLimits {
        QueryLimits::try_new(
            global,
            Duration::from_millis(20),
            Duration::from_secs(5),
            1024,
        )
        .expect("global query limits")
        .try_with_tenant_limits(per_tenant, tracked_tenants)
        .expect("tenant query limits")
    }

    #[tokio::test]
    async fn tenant_query_admission_isolates_noisy_neighbor() {
        let admission = QueryAdmission::new(tenant_limits(2, 1, 8));
        let alpha = admission_principal("alpha-reader", "alpha");
        let beta = admission_principal("beta-reader", "beta");
        let alpha_permit = admission.acquire(&alpha).await.expect("alpha admitted");
        let queued_admission = admission.clone();
        let queued_alpha = alpha.clone();
        let queued = tokio::spawn(async move { queued_admission.acquire(&queued_alpha).await });
        tokio::time::sleep(Duration::from_millis(5)).await;

        let beta_permit = admission
            .acquire(&beta)
            .await
            .expect("beta uses the free global slot");
        let error = queued
            .await
            .expect("queued alpha joins")
            .err()
            .expect("second alpha request reaches its queue deadline");

        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(error.message(), "tenant query concurrency limit reached");
        drop((alpha_permit, beta_permit));
    }

    #[tokio::test]
    async fn tenant_query_admission_preserves_global_limit() {
        let admission = QueryAdmission::new(tenant_limits(2, 2, 8));
        let alpha = admission_principal("alpha-reader", "alpha");
        let beta = admission_principal("beta-reader", "beta");
        let gamma = admission_principal("gamma-reader", "gamma");
        let alpha_permit = admission.acquire(&alpha).await.expect("alpha admitted");
        let beta_permit = admission.acquire(&beta).await.expect("beta admitted");

        let error = admission
            .acquire(&gamma)
            .await
            .err()
            .expect("global replica ceiling remains enforced");

        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(error.message(), "query concurrency limit reached");
        drop((alpha_permit, beta_permit));
    }

    #[tokio::test]
    async fn tenant_query_admission_reclaims_inactive_trackers() {
        let admission = QueryAdmission::new(tenant_limits(2, 1, 1));
        let alpha = admission_principal("alpha-reader", "alpha");
        let beta = admission_principal("beta-reader", "beta");
        let alpha_permit = admission.acquire(&alpha).await.expect("alpha admitted");

        let error = admission
            .acquire(&beta)
            .await
            .err()
            .expect("active tenant tracker consumes the finite registry");
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(error.message(), "tenant admission tracker capacity reached");

        drop(alpha_permit);
        let beta_permit = admission
            .acquire(&beta)
            .await
            .expect("inactive alpha tracker is reclaimed");
        drop(beta_permit);
    }

    #[tokio::test]
    async fn tenant_query_admission_debug_redacts_identity() {
        let admission = QueryAdmission::new(tenant_limits(2, 1, 8));
        let principal = admission_principal("secret-subject", "secret-tenant");
        let permit = admission
            .acquire(&principal)
            .await
            .expect("principal admitted");

        let debug = format!("{admission:?}");
        assert!(!debug.contains("secret-subject"));
        assert!(!debug.contains("secret-tenant"));
        drop(permit);
    }

    #[tokio::test]
    async fn async_result_stream_drop_cancels_pipeline_and_releases_permit() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let mut ipc = StreamWriter::try_new(Vec::new(), &batch.schema()).unwrap();
        ipc.write(&batch).unwrap();
        let first_batch_end = ipc.get_ref().len();
        ipc.finish().unwrap();
        let encoded = ipc.into_inner().unwrap();
        let expected_bytes = encoded.len() as u64;
        let (mut object_writer, object_reader) = tokio::io::duplex(64 * 1024);
        tokio::time::timeout(
            Duration::from_secs(1),
            object_writer.write_all(&encoded[..first_batch_end]),
        )
        .await
        .expect("schema prefix write does not block")
        .unwrap();
        let (probe, decoder_exit) = PipelineProbe::instrumented_with_blocked_decoder();
        let admission = QueryAdmission::new(tenant_limits(1, 1, 2));
        let principal = admission_principal("async-reader", "async-tenant");
        let permit = Arc::new(admission.acquire(&principal).await.unwrap());
        let decoded = tokio::time::timeout(
            Duration::from_secs(1),
            decode_ipc_reader(
                Box::pin(object_reader),
                expected_bytes,
                IpcPipelineLimits::try_new(expected_bytes, 64, 1, 1).unwrap(),
                probe.clone(),
                permit.clone(),
            ),
        )
        .await
        .expect("schema decode does not wait for object EOF")
        .unwrap();
        let (schema, batches, decode) = decoded.into_parts();
        let batches = batches.into_stream().map(|batch| {
            batch.map_err(|_| FlightError::from(Status::internal("invalid async result")))
        });
        let inner: <FlightSqlServiceImpl as FlightService>::DoGetStream = Box::pin(
            FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(batches)
                .map_err(Status::from),
        );
        let stream = AdmittedFlightStream::new_with_decode(
            inner,
            Instant::now() + Duration::from_secs(5),
            permit,
            decode,
        );

        drop(stream);
        drop(object_writer);
        let replacement_while_decoder_runs =
            tokio::time::timeout(Duration::from_millis(20), admission.acquire(&principal)).await;
        assert!(
            !matches!(replacement_while_decoder_runs, Ok(Ok(_))),
            "admission stays held until the blocking decoder actually exits"
        );
        decoder_exit.release();
        tokio::time::timeout(Duration::from_secs(1), async {
            while probe.active_tasks() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned IPC tasks stop after Flight stream drop");
        let replacement = admission.acquire(&principal).await.unwrap();
        drop(replacement);
    }

    #[tokio::test]
    async fn flight_table_discovery_returns_cached_real_schema() {
        let alpha_schema = Schema::new(vec![Field::new("episode_id", DataType::Utf8, false)]);
        let beta_schema = Schema::new(vec![Field::new("secret", DataType::Int64, false)]);
        let meta = Arc::new(SchemaDiscoveryMeta::new(vec![
            (
                "alpha_episodes/events".to_owned(),
                registration_with_schema(&alpha_schema),
            ),
            (
                "beta_episodes/secrets".to_owned(),
                registration_with_schema(&beta_schema),
            ),
        ]));
        let meta_ref: MetaStoreRef = meta.clone();
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let engine = Arc::new(QueryEngine::new(meta_ref, storage));
        engine.refresh().await.unwrap();
        meta.operations.store(0, Ordering::Relaxed);
        let service = FlightSqlServiceImpl {
            engine,
            metadata_addr: None,
            metadata_security: ClientSecurity::new(),
            managed_stage: None,
            admission: QueryAdmission::new(QueryLimits::default()),
            discovery_limits: DiscoveryLimits::default(),
            ticket_codec: test_ticket_codec(),
        };
        let principal = Principal::try_new(
            PrincipalId::try_new("alpha-reader").unwrap(),
            TenantId::try_new("tenant-alpha").unwrap(),
            PrincipalRole::User,
            ["alpha_episodes"],
        )
        .unwrap();
        let mut request = Request::new(Ticket::default());
        request.extensions_mut().insert(principal);

        let stream = service
            .do_get_tables(
                CommandGetTables {
                    catalog:                   None,
                    db_schema_filter_pattern:  None,
                    table_name_filter_pattern: None,
                    table_types:               Vec::new(),
                    include_schema:            true,
                },
                request,
            )
            .await
            .unwrap()
            .into_inner();
        let batches = FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(arrow_flight::error::FlightError::from),
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        let names = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let schemas = batches[0]
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let actual = Schema::try_from(IpcMessage(schemas.value(0).to_vec().into())).unwrap();

        assert_eq!(names.iter().collect::<Vec<_>>(), vec![Some("events")]);
        assert_eq!(actual, alpha_schema);
        assert_eq!(meta.operations.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn flight_table_discovery_reads_one_catalog_generation() {
        let old_schema = Schema::new(vec![Field::new("episode_id", DataType::Utf8, false)]);
        let new_schema = Schema::new(vec![Field::new("run_id", DataType::Int64, false)]);
        let meta = Arc::new(SchemaDiscoveryMeta::new(vec![(
            "alpha_episodes/events".to_owned(),
            registration_with_schema(&old_schema),
        )]));
        let meta_ref: MetaStoreRef = meta.clone();
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let engine = QueryEngine::new(meta_ref, storage);
        engine.refresh().await.unwrap();
        let request_generation = engine.cached_catalog_generation();
        *meta.entries.write().unwrap() = vec![(
            "alpha_episodes/runs".to_owned(),
            registration_with_schema(&new_schema),
        )];
        engine.refresh().await.unwrap();
        let principal = Principal::try_new(
            PrincipalId::try_new("alpha-reader").unwrap(),
            TenantId::try_new("tenant-alpha").unwrap(),
            PrincipalRole::User,
            ["alpha_episodes"],
        )
        .unwrap();

        let batch = build_table_discovery(
            CommandGetTables {
                catalog:                   None,
                db_schema_filter_pattern:  None,
                table_name_filter_pattern: None,
                table_types:               Vec::new(),
                include_schema:            true,
            },
            &principal,
            &request_generation,
        )
        .unwrap();
        let names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let schemas = batch
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let actual = Schema::try_from(IpcMessage(schemas.value(0).to_vec().into())).unwrap();

        assert_eq!(names.iter().collect::<Vec<_>>(), vec![Some("events")]);
        assert_eq!(actual, old_schema);
    }

    #[tokio::test]
    async fn flight_table_discovery_prefilters_before_schema_resolution() {
        let schema = Schema::new(vec![Field::new("episode_id", DataType::Utf8, false)]);
        let meta = Arc::new(SchemaDiscoveryMeta::new(vec![
            (
                "alpha_episodes/events".to_owned(),
                registration_with_schema(&schema),
            ),
            (
                "alpha_episodes/legacy".to_owned(),
                br#"{"location":"mem://legacy","engine":"lance","current_version":1}"#.to_vec(),
            ),
        ]));
        let meta_ref: MetaStoreRef = meta;
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let engine = QueryEngine::new(meta_ref, storage);
        engine.refresh().await.unwrap();
        let generation = engine.cached_catalog_generation();
        let principal = Principal::try_new(
            PrincipalId::try_new("alpha-reader").unwrap(),
            TenantId::try_new("tenant-alpha").unwrap(),
            PrincipalRole::User,
            ["alpha_episodes"],
        )
        .unwrap();

        let batch = build_table_discovery(
            CommandGetTables {
                catalog:                   Some("lake".to_owned()),
                db_schema_filter_pattern:  Some("alpha_%".to_owned()),
                table_name_filter_pattern: Some("events".to_owned()),
                table_types:               vec!["TABLE".to_owned()],
                include_schema:            true,
            },
            &principal,
            &generation,
        )
        .expect("nonmatching legacy tables must not be resolved");
        let names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(names.iter().collect::<Vec<_>>(), vec![Some("events")]);
    }

    #[tokio::test]
    async fn flight_table_discovery_rejects_unknown_legacy_schema() {
        let meta = Arc::new(SchemaDiscoveryMeta::new(vec![(
            "alpha_episodes/legacy".to_owned(),
            br#"{"location":"mem://legacy","engine":"lance","current_version":1}"#.to_vec(),
        )]));
        let meta_ref: MetaStoreRef = meta.clone();
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let engine = Arc::new(QueryEngine::new(meta_ref, storage));
        engine.refresh().await.unwrap();
        meta.operations.store(0, Ordering::Relaxed);
        let service = FlightSqlServiceImpl {
            engine,
            metadata_addr: None,
            metadata_security: ClientSecurity::new(),
            managed_stage: None,
            admission: QueryAdmission::new(QueryLimits::default()),
            discovery_limits: DiscoveryLimits::default(),
            ticket_codec: test_ticket_codec(),
        };
        let principal = Principal::try_new(
            PrincipalId::try_new("alpha-reader").unwrap(),
            TenantId::try_new("tenant-alpha").unwrap(),
            PrincipalRole::User,
            ["alpha_episodes"],
        )
        .unwrap();
        let mut request = Request::new(Ticket::default());
        request.extensions_mut().insert(principal);

        let mut stream = service
            .do_get_tables(
                CommandGetTables {
                    catalog:                   None,
                    db_schema_filter_pattern:  None,
                    table_name_filter_pattern: None,
                    table_types:               Vec::new(),
                    include_schema:            true,
                },
                request,
            )
            .await
            .expect("discovery stream")
            .into_inner();
        let mut error = None;
        while let Some(item) = stream.next().await {
            if let Err(status) = item {
                error = Some(status);
                break;
            }
        }
        let error = error.expect("legacy schema must not be represented as empty");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(meta.operations.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn do_get_returns_before_the_input_stream_finishes() {
        let meta: MetaStoreRef = Arc::new(EmptyMeta);
        let storage = VersionedTestEngine::default();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let release = Arc::new(Notify::new());
        let table = StreamingTable::try_new(
            schema.clone(),
            vec![Arc::new(DelayedPartition {
                schema,
                release: release.clone(),
            })],
        )
        .unwrap();
        let location = TableLocation::new("mem://robots/delayed/incarnation");
        storage.insert(&location, Version(1), Arc::new(table));
        let storage: TableEngineRef = Arc::new(storage);

        let service = FlightSqlServiceImpl {
            engine:            Arc::new(QueryEngine::new(meta, storage)),
            metadata_addr:     None,
            metadata_security: ClientSecurity::new(),
            managed_stage:     None,
            admission:         QueryAdmission::new(QueryLimits::default()),
            discovery_limits:  DiscoveryLimits::default(),
            ticket_codec:      test_ticket_codec(),
        };
        let principal = robots_reader();
        let ticket = TicketStatementQuery {
            statement_handle: service
                .ticket_codec
                .seal_statement(
                    &StatementTicket {
                        sql:       "SELECT * FROM lake.robots.delayed".to_owned(),
                        snapshots: vec![StatementTableSnapshot {
                            namespace:      "robots".to_owned(),
                            table:          "delayed".to_owned(),
                            engine:         "versioned-test".to_owned(),
                            location:       location.0,
                            incarnation_id: "incarnation".to_owned(),
                            version:        1,
                        }],
                    },
                    &principal,
                )
                .unwrap()
                .into(),
        };
        let mut ticket_request = Request::new(Ticket::default());
        ticket_request.extensions_mut().insert(principal);
        let mut request =
            tokio::spawn(async move { service.do_get_statement(ticket, ticket_request).await });

        let returned_early =
            match tokio::time::timeout(Duration::from_millis(100), &mut request).await {
                Ok(result) => {
                    result
                        .expect("DoGet task")
                        .expect("DoGet must return a valid stream");
                    true
                }
                Err(_) => false,
            };
        release.notify_waiters();
        if !returned_early {
            request.await.unwrap().unwrap();
        }

        assert!(
            returned_early,
            "DoGet must return its Flight stream before the producer completes"
        );
    }

    #[tokio::test]
    async fn query_admission_rejects_when_saturated_and_releases_on_drop() {
        let meta: MetaStoreRef = Arc::new(EmptyMeta);
        let storage = VersionedTestEngine::default();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let release = Arc::new(Notify::new());
        let table = StreamingTable::try_new(
            schema.clone(),
            vec![Arc::new(DelayedPartition {
                schema,
                release: release.clone(),
            })],
        )
        .expect("streaming table");
        let location = TableLocation::new("mem://robots/admitted/incarnation");
        storage.insert(&location, Version(1), Arc::new(table));
        let storage: TableEngineRef = Arc::new(storage);
        let limits =
            QueryLimits::try_new(1, Duration::from_millis(20), Duration::from_secs(5), 1024)
                .expect("limits");
        let service = FlightSqlServiceImpl {
            engine:            Arc::new(QueryEngine::new(meta, storage)),
            metadata_addr:     None,
            metadata_security: ClientSecurity::new(),
            managed_stage:     None,
            admission:         QueryAdmission::new(limits),
            discovery_limits:  DiscoveryLimits::default(),
            ticket_codec:      test_ticket_codec(),
        };
        let principal = robots_reader();
        let statement_handle = service
            .ticket_codec
            .seal_statement(
                &StatementTicket {
                    sql:       "SELECT * FROM lake.robots.admitted".to_owned(),
                    snapshots: vec![StatementTableSnapshot {
                        namespace:      "robots".to_owned(),
                        table:          "admitted".to_owned(),
                        engine:         "versioned-test".to_owned(),
                        location:       location.0,
                        incarnation_id: "incarnation".to_owned(),
                        version:        1,
                    }],
                },
                &principal,
            )
            .unwrap();
        let ticket = || TicketStatementQuery {
            statement_handle: statement_handle.clone().into(),
        };
        let request = || {
            let mut request = Request::new(Ticket::default());
            request.extensions_mut().insert(principal.clone());
            request
        };

        let first = service
            .do_get_statement(ticket(), request())
            .await
            .expect("first query admitted");
        let Err(saturated) = service.do_get_statement(ticket(), request()).await else {
            panic!("second query must be rejected while first stream lives");
        };
        assert_eq!(saturated.code(), tonic::Code::ResourceExhausted);

        drop(first);
        let third = service.do_get_statement(ticket(), request()).await;
        assert!(third.is_ok(), "dropping the stream must release its permit");
        release.notify_waiters();
    }

    #[tokio::test]
    async fn flight_discovery_admission_releases_on_stream_drop() {
        let meta: MetaStoreRef = Arc::new(EmptyMeta);
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let limits =
            QueryLimits::try_new(1, Duration::from_millis(20), Duration::from_secs(5), 1024)
                .expect("limits");
        let service = FlightSqlServiceImpl {
            engine:            Arc::new(QueryEngine::new(meta, storage)),
            metadata_addr:     None,
            metadata_security: ClientSecurity::new(),
            managed_stage:     None,
            admission:         QueryAdmission::new(limits),
            discovery_limits:  DiscoveryLimits::default(),
            ticket_codec:      test_ticket_codec(),
        };
        let query = || CommandGetDbSchemas {
            catalog:                  None,
            db_schema_filter_pattern: None,
        };
        let request = || {
            let mut request = Request::new(Ticket::default());
            request
                .extensions_mut()
                .insert(Principal::deployment_admin());
            request
        };

        let first = service
            .do_get_schemas(query(), request())
            .await
            .expect("first discovery admitted");
        let Err(saturated) = service.do_get_schemas(query(), request()).await else {
            panic!("second discovery must be rejected while first stream lives");
        };
        assert_eq!(saturated.code(), tonic::Code::ResourceExhausted);

        drop(first);
        assert!(
            service.do_get_schemas(query(), request()).await.is_ok(),
            "dropping the discovery stream must release its permit"
        );
    }

    #[tokio::test]
    async fn query_execution_deadline_terminates_slow_stream() {
        let meta: MetaStoreRef = Arc::new(EmptyMeta);
        let storage = VersionedTestEngine::default();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let release = Arc::new(Notify::new());
        let table = StreamingTable::try_new(
            schema.clone(),
            vec![Arc::new(DelayedPartition {
                schema,
                release: release.clone(),
            })],
        )
        .expect("streaming table");
        let location = TableLocation::new("mem://robots/deadline/incarnation");
        storage.insert(&location, Version(1), Arc::new(table));
        let storage: TableEngineRef = Arc::new(storage);
        let limits = QueryLimits::try_new(
            1,
            Duration::from_millis(20),
            Duration::from_millis(50),
            1024,
        )
        .expect("limits");
        let service = FlightSqlServiceImpl {
            engine:            Arc::new(QueryEngine::new(meta, storage)),
            metadata_addr:     None,
            metadata_security: ClientSecurity::new(),
            managed_stage:     None,
            admission:         QueryAdmission::new(limits),
            discovery_limits:  DiscoveryLimits::default(),
            ticket_codec:      test_ticket_codec(),
        };
        let principal = robots_reader();
        let statement_handle = service
            .ticket_codec
            .seal_statement(
                &StatementTicket {
                    sql:       "SELECT * FROM lake.robots.deadline".to_owned(),
                    snapshots: vec![StatementTableSnapshot {
                        namespace:      "robots".to_owned(),
                        table:          "deadline".to_owned(),
                        engine:         "versioned-test".to_owned(),
                        location:       location.0,
                        incarnation_id: "incarnation".to_owned(),
                        version:        1,
                    }],
                },
                &principal,
            )
            .unwrap();
        let ticket = || TicketStatementQuery {
            statement_handle: statement_handle.clone().into(),
        };
        let mut request = Request::new(Ticket::default());
        request.extensions_mut().insert(principal.clone());
        let response = service
            .do_get_statement(ticket(), request)
            .await
            .expect("query admitted");
        let mut stream = response.into_inner();
        let mut successful_items = 0;
        let deadline_status = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match stream.next().await {
                    Some(Ok(_)) => successful_items += 1,
                    Some(Err(status)) => break status,
                    None => panic!("slow stream ended without a deadline status"),
                }
            }
        })
        .await
        .expect("deadline must terminate the stream");
        assert!(
            successful_items > 0,
            "the first batch should stream before timeout"
        );
        assert_eq!(deadline_status.code(), tonic::Code::DeadlineExceeded);

        let mut next_request = Request::new(Ticket::default());
        next_request.extensions_mut().insert(principal);
        let next = service.do_get_statement(ticket(), next_request).await;
        assert!(next.is_ok(), "deadline must release the admission permit");
        release.notify_waiters();
    }

    #[tokio::test]
    async fn oversized_sql_and_ticket_are_rejected_before_planning() {
        let meta = Arc::new(PlanningMeta::default());
        let meta_ref: MetaStoreRef = meta.clone();
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let limits = QueryLimits::try_new(1, Duration::from_millis(20), Duration::from_secs(1), 4)
            .expect("limits");
        let service = FlightSqlServiceImpl {
            engine:            Arc::new(QueryEngine::new(meta_ref, storage)),
            metadata_addr:     None,
            metadata_security: ClientSecurity::new(),
            managed_stage:     None,
            admission:         QueryAdmission::new(limits),
            discovery_limits:  DiscoveryLimits::default(),
            ticket_codec:      test_ticket_codec(),
        };
        let query = CommandStatementQuery {
            query:          "SELECT 1".to_owned(),
            transaction_id: None,
        };
        let Err(sql_error) = service
            .get_flight_info_statement(query, Request::new(FlightDescriptor::default()))
            .await
        else {
            panic!("oversized SQL must fail");
        };
        assert_eq!(sql_error.code(), tonic::Code::ResourceExhausted);

        let ticket = TicketStatementQuery {
            statement_handle: vec![0_u8; MAX_STATEMENT_TICKET_OVERHEAD + 5].into(),
        };
        let mut request = Request::new(Ticket::default());
        request
            .extensions_mut()
            .insert(Principal::deployment_admin());
        let Err(ticket_error) = service.do_get_statement(ticket, request).await else {
            panic!("oversized ticket must fail");
        };
        assert_eq!(ticket_error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(meta.scans.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn delegated_append_scope_headers_are_tenant_scoped() {
        let principal = |id: &str, tenant: &str| {
            Principal::try_new(
                PrincipalId::try_new(id).unwrap(),
                TenantId::try_new(tenant).unwrap(),
                PrincipalRole::User,
                ["robots"],
            )
            .unwrap()
        };
        let mut first = tonic::metadata::MetadataMap::new();
        let mut second = tonic::metadata::MetadataMap::new();

        apply_delegated_append_scope(&mut first, &principal("first", "tenant-a"), "robots")
            .unwrap();
        apply_delegated_append_scope(&mut second, &principal("second", "tenant-b"), "robots")
            .unwrap();

        assert_eq!(
            first.get(lake_flight::DELEGATED_TENANT_HEADER).unwrap(),
            "tenant-a"
        );
        assert_eq!(
            second.get(lake_flight::DELEGATED_TENANT_HEADER).unwrap(),
            "tenant-b"
        );
        assert_eq!(
            first.get(DELEGATED_NAMESPACE_HEADER),
            second.get(DELEGATED_NAMESPACE_HEADER)
        );
    }
}
