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

//! The metadata-layer control plane over Arrow Flight `DoAction`.
//!
//! The metadata authority is a low-fan-out registry tier, not a data path, so
//! its wire surface is a handful of RPC-style actions rather than a streaming
//! query interface. [`MetasrvFlightService`] rides the Flight `DoAction`
//! opcode: every request is an [`Action`] whose `type` names one of
//! `create_table`, `resolve`, `list_tables`, `list_namespaces`, and whose
//! `body` carries a small JSON payload. Typed `FILE` appends use `DoPut` with
//! Arrow `DataLocation` rows; the original object payload never enters this
//! service. Query-data methods such as `DoGet` remain unimplemented.
//!
//! Writes are leader-aware: a write (`create_table`, `drop_table`) that lands
//! on a follower is transparently forwarded over Flight to the current leader
//! and its result relayed, so any node accepts writes. If no leader is known
//! yet, the write fails with [`unavailable`](Status::unavailable). Reads
//! (`resolve`, `list_*`) are always served locally, matching the HA model in
//! `docs/architecture.md`.

use std::{pin::Pin, sync::Arc};

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightClient, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightResult, SchemaResult,
    Ticket, decode::FlightRecordBatchStream, flight_service_client::FlightServiceClient,
    flight_service_server::FlightService,
};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    error::DataFusionError,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use futures::{Stream, StreamExt};
use lake_common::{
    AppendOperation, FILE_APPEND_TYPE_URL, FileAppendRequest, Namespace, Principal, PrincipalRole,
    TableRef, TenantId, Version,
};
use lake_flight::{
    ClientSecurity, DELEGATED_NAMESPACE_HEADER, DELEGATED_TENANT_HEADER, TracedFlightStream,
    append_flight_payload_digest, set_span_parent_from_request,
};
use lake_objects::data_location_field;
use prost::Message;
use prost_types::Any;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Request, Response, Status, Streaming};
use tracing::{Instrument as _, Span, field};

use crate::{AppendLimits, Metasrv, TablePlacement, leadership::Leadership, telemetry};

/// The [`Status`] message returned by every unsupported Flight method.
const UNSUPPORTED: &str = "metasrv control plane only serves actions and FILE append do_put";

#[derive(Clone, Debug)]
pub(crate) struct AppendAdmission {
    concurrent: Arc<Semaphore>,
    buffered:   Arc<Semaphore>,
    limits:     AppendLimits,
}

#[derive(Debug)]
pub(crate) struct AppendPermit {
    _concurrent: OwnedSemaphorePermit,
    _buffered:   OwnedSemaphorePermit,
    bytes:       usize,
}

impl Drop for AppendPermit {
    fn drop(&mut self) { telemetry::append_released(self.bytes); }
}

impl AppendAdmission {
    pub(crate) fn new(limits: AppendLimits) -> Self {
        Self {
            concurrent: Arc::new(Semaphore::new(limits.max_concurrent())),
            buffered: Arc::new(Semaphore::new(limits.max_buffered_bytes())),
            limits,
        }
    }

    pub(crate) async fn acquire(&self) -> Result<AppendPermit, Status> {
        let stream_bytes = u32::try_from(self.limits.max_stream_bytes())
            .expect("validated append stream bytes fit u32");
        let permit = tokio::time::timeout(self.limits.queue_wait(), async {
            let concurrent = self
                .concurrent
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Status::unavailable("append admission is shutting down"))?;
            let buffered = self
                .buffered
                .clone()
                .acquire_many_owned(stream_bytes)
                .await
                .map_err(|_| Status::unavailable("append admission is shutting down"))?;
            Ok(AppendPermit {
                _concurrent: concurrent,
                _buffered:   buffered,
                bytes:       self.limits.max_stream_bytes(),
            })
        })
        .await
        .map_err(|_| {
            telemetry::append_admission("saturated");
            Status::resource_exhausted("append admission limit reached")
        })?;
        match permit {
            Ok(permit) => {
                telemetry::append_admission("admitted");
                telemetry::append_acquired(self.limits.max_stream_bytes());
                Ok(permit)
            }
            Err(status) => {
                telemetry::append_admission("shutting_down");
                Err(status)
            }
        }
    }
}

const RESOURCE_UNAVAILABLE: &str = "resource is not available";

/// A boxed server stream of `T`, the shape every Flight response stream takes.
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

fn finish_stream_rpc<T: Send + 'static>(
    span: &Span,
    result: Result<Response<BoxStream<T>>, Status>,
) -> Result<Response<BoxStream<T>>, Status> {
    match result {
        Ok(response) => {
            let stream: BoxStream<T> =
                Box::pin(TracedFlightStream::new(response.into_inner(), span.clone()));
            Ok(Response::new(stream))
        }
        Err(error) => {
            span.record("rpc.outcome", "error");
            Err(error)
        }
    }
}

/// The `DoAction` response stream: a (usually one-shot) stream of Flight
/// results carrying JSON bodies.
type ActionStream = BoxStream<FlightResult>;

fn principal<T>(request: &Request<T>) -> Result<Principal, Status> {
    request
        .extensions()
        .get::<Principal>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("authenticated principal is missing"))
}

fn delegated_namespace<T>(request: &Request<T>) -> Result<Option<String>, Status> {
    request
        .metadata()
        .get(DELEGATED_NAMESPACE_HEADER)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| Status::permission_denied(RESOURCE_UNAVAILABLE))
        })
        .transpose()
}

fn delegated_tenant<T>(request: &Request<T>) -> Result<Option<TenantId>, Status> {
    request
        .metadata()
        .get(DELEGATED_TENANT_HEADER)
        .map(|value| {
            value
                .to_str()
                .map_err(|_| Status::permission_denied(RESOURCE_UNAVAILABLE))
                .and_then(|value| {
                    TenantId::try_new(value)
                        .map_err(|_| Status::permission_denied(RESOURCE_UNAVAILABLE))
                })
        })
        .transpose()
}

fn operation_tenant(
    principal: &Principal,
    delegated: Option<TenantId>,
) -> Result<TenantId, Status> {
    match principal.role() {
        PrincipalRole::User if delegated.is_some() => {
            Err(Status::permission_denied(RESOURCE_UNAVAILABLE))
        }
        PrincipalRole::User => Ok(principal.tenant().clone()),
        PrincipalRole::Admin => Ok(delegated.unwrap_or_else(|| principal.tenant().clone())),
        PrincipalRole::QueryService | PrincipalRole::MetadataPeer => {
            delegated.ok_or_else(|| Status::permission_denied(RESOURCE_UNAVAILABLE))
        }
    }
}

fn authorize_namespace(
    principal: &Principal,
    delegated: Option<&str>,
    target: &str,
) -> Result<(), Status> {
    let authorized = match principal.role() {
        PrincipalRole::Admin => true,
        PrincipalRole::User => delegated.is_none() && principal.can_access_namespace(target),
        PrincipalRole::QueryService | PrincipalRole::MetadataPeer => delegated == Some(target),
    };
    if authorized {
        Ok(())
    } else {
        Err(Status::permission_denied(RESOURCE_UNAVAILABLE))
    }
}

fn append_request(first: &FlightData) -> Result<FileAppendRequest, Status> {
    let descriptor = first
        .flight_descriptor
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("FILE append requires a command descriptor"))?;
    let command = Any::decode(descriptor.cmd.as_ref())
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if command.type_url != FILE_APPEND_TYPE_URL {
        return Err(Status::invalid_argument("invalid FILE append command type"));
    }
    FileAppendRequest::from_command_payload(&command.value)
        .ok_or_else(|| Status::invalid_argument("invalid FILE append descriptor"))
}

async fn append_file_stream<S, E>(
    metasrv: &Metasrv,
    tenant: TenantId,
    input: S,
) -> Result<Version, Status>
where
    S: Stream<Item = std::result::Result<FlightData, E>> + Send + Unpin + 'static,
    E: std::fmt::Display + Send + 'static,
{
    append_file_stream_with_limits(metasrv, tenant, input, AppendLimits::default()).await
}

async fn append_file_stream_with_limits<S, E>(
    metasrv: &Metasrv,
    tenant: TenantId,
    input: S,
    limits: AppendLimits,
) -> Result<Version, Status>
where
    S: Stream<Item = std::result::Result<FlightData, E>> + Send + Unpin + 'static,
    E: std::fmt::Display + Send + 'static,
{
    append_file_stream_with_limit(metasrv, tenant, input, limits.max_stream_bytes()).await
}

async fn append_file_stream_with_limit<S, E>(
    metasrv: &Metasrv,
    tenant: TenantId,
    mut input: S,
    max_stream_bytes: usize,
) -> Result<Version, Status>
where
    S: Stream<Item = std::result::Result<FlightData, E>> + Send + Unpin + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let first = input
        .next()
        .await
        .ok_or_else(|| Status::invalid_argument("FILE append stream is empty"))?
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let append = append_request(&first)?;
    let mut stream_bytes = first.encoded_len();
    if stream_bytes > max_stream_bytes {
        return Err(Status::resource_exhausted(
            "FILE append control payload exceeds the server limit",
        ));
    }
    let mut messages = vec![first];
    while let Some(item) = input.next().await {
        let item = item.map_err(|error| Status::invalid_argument(error.to_string()))?;
        stream_bytes = stream_bytes
            .checked_add(item.encoded_len())
            .ok_or_else(|| {
                Status::resource_exhausted("FILE append control payload is too large")
            })?;
        if stream_bytes > max_stream_bytes {
            return Err(Status::resource_exhausted(
                "FILE append control payload exceeds the server limit",
            ));
        }
        messages.push(item);
    }
    let actual_digest = append_flight_payload_digest(&messages);
    if &actual_digest != append.payload_digest() {
        return Err(Status::invalid_argument(
            "FILE append payload digest does not match the Flight messages",
        ));
    }
    let operation = AppendOperation::builder()
        .tenant(tenant)
        .operation_id(append.operation_id().clone())
        .payload_digest(actual_digest)
        .build();
    let flight_data = futures::stream::iter(messages.into_iter().map(Ok));
    let mut decoded = FlightRecordBatchStream::new_from_flight_data(flight_data);
    let first_batch = decoded
        .next()
        .await
        .ok_or_else(|| Status::invalid_argument("FILE append contains no rows"))?
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let schema = first_batch.schema();
    let batches = futures::stream::once(async move { Ok(first_batch) }).chain(
        decoded.map(|item| item.map_err(|error| DataFusionError::External(Box::new(error)))),
    );
    let stream = Box::pin(RecordBatchStreamAdapter::new(schema, batches));
    metasrv
        .append(append.table(), &operation, stream)
        .await
        .map_err(|error| match error {
            crate::MetasrvError::OperationConflict { .. } => {
                Status::already_exists(error.to_string())
            }
            crate::MetasrvError::OperationExpired { .. } => {
                Status::failed_precondition(error.to_string())
            }
            crate::MetasrvError::OperationFromFuture { .. } => {
                Status::invalid_argument(error.to_string())
            }
            crate::MetasrvError::OperationTableRecreated { .. } => {
                Status::failed_precondition(error.to_string())
            }
            crate::MetasrvError::OperationInProgress { .. } => {
                Status::unavailable(error.to_string())
            }
            _ => Status::internal(error.to_string()),
        })
}

/// The metadata-layer control-plane Flight service.
///
/// Holds the registry [`Metasrv`] authority it dispatches to and the shared
/// [`Leadership`] flag it gates writes on.
pub(crate) struct MetasrvFlightService {
    /// The registry authority every action dispatches to.
    pub(crate) metasrv:            Arc<Metasrv>,
    /// The shared leadership state consulted before serving a write.
    pub(crate) leadership:         Arc<Leadership>,
    /// This node's own Flight address, used to tell "the leader is me" apart
    /// from "forward to another node" when the leader flag is briefly stale.
    pub(crate) own_addr:           String,
    /// TLS and service identity for forwarding writes to the elected leader.
    pub(crate) peer_security:      ClientSecurity,
    /// Trusted policy used to derive every remotely-created table location.
    pub(crate) table_placement:    Option<TablePlacement>,
    /// Process-local admission shared by direct and forwarded FILE appends.
    pub(crate) append_admission:   AppendAdmission,
    #[cfg(feature = "test")]
    pub(crate) append_result_gate: Option<Arc<crate::AppendResultGate>>,
}

/// `create_table` action body: the table to materialize and register.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTableReq {
    namespace: String,
    name:      String,
    /// Columns as `name:type` (types: `i64`, `f64`, `utf8`, `bool`).
    columns:   Vec<String>,
}

/// `resolve` action body: a fully-qualified table reference.
#[derive(serde::Deserialize)]
struct TableIdent {
    namespace: String,
    name:      String,
}

/// `list_tables` action body: the namespace to enumerate.
#[derive(serde::Deserialize)]
struct NamespaceIdent {
    namespace: String,
}

/// Decode a JSON action body, mapping any parse failure to `invalid_argument`.
fn parse_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, Status> {
    serde_json::from_slice(body)
        .map_err(|e| Status::invalid_argument(format!("invalid action body: {e}")))
}

/// Build an Arrow schema from `name:type` column specs, mirroring the CLI DSL.
fn build_schema(columns: &[String]) -> Result<SchemaRef, Status> {
    let fields = columns
        .iter()
        .map(|c| {
            let (name, ty) = c.split_once(':').ok_or_else(|| {
                Status::invalid_argument(format!("column must be name:type: {c}"))
            })?;
            let field = match ty {
                "i64" => Field::new(name, DataType::Int64, false),
                "f64" => Field::new(name, DataType::Float64, false),
                "utf8" => Field::new(name, DataType::Utf8, false),
                "bool" => Field::new(name, DataType::Boolean, false),
                "file" => data_location_field(name, false),
                other => {
                    return Err(Status::invalid_argument(format!(
                        "unknown column type '{other}' (use i64|f64|utf8|bool|file)"
                    )));
                }
            };
            Ok(field)
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Serialize `value` to a one-shot `DoAction` result stream.
fn respond_json<T: Serialize>(value: &T) -> Result<Response<ActionStream>, Status> {
    let body = serde_json::to_vec(value).map_err(|e| Status::internal(e.to_string()))?;
    let result = FlightResult { body: body.into() };
    let stream: ActionStream = Box::pin(futures::stream::once(async move { Ok(result) }));
    Ok(Response::new(stream))
}

impl MetasrvFlightService {
    /// Decide how to serve a write action given current leadership.
    ///
    /// Returns `Ok(None)` when this node should serve the write locally (it
    /// holds the lease, or the observed leader *is* this node while the flag
    /// catches up). Returns `Ok(Some(response))` when the write was forwarded
    /// to the current leader and its result should be relayed. Returns
    /// `Err(unavailable)` when no leader is known to forward to.
    async fn maybe_forward(
        &self,
        action: &Action,
        namespace: &str,
    ) -> Result<Option<Response<ActionStream>>, Status> {
        if self.leadership.is_leader() {
            return Ok(None);
        }
        match self.leadership.leader() {
            // We are the elected leader; the flag is just briefly stale.
            Some(addr) if addr == self.own_addr => Ok(None),
            Some(addr) => self.forward(&addr, action, namespace).await.map(Some),
            None => Err(Status::unavailable("no leader elected")),
        }
    }

    /// Forward `action` to the leader at `addr` over Flight `DoAction`,
    /// relaying its streamed result as this call's response.
    async fn forward(
        &self,
        addr: &str,
        action: &Action,
        namespace: &str,
    ) -> Result<Response<ActionStream>, Status> {
        let endpoint = self.peer_security.endpoint_for_authority(addr);
        let channel = self
            .peer_security
            .connect(endpoint)
            .await
            .map_err(|e| Status::unavailable(format!("cannot reach leader '{addr}': {e}")))?;
        let mut client = FlightServiceClient::new(channel);
        let mut request = self
            .peer_security
            .authorize_request(Request::new(action.clone()));
        request.metadata_mut().insert(
            DELEGATED_NAMESPACE_HEADER,
            namespace
                .parse()
                .map_err(|_| Status::internal("authorized namespace is not valid metadata"))?,
        );
        let response = client.do_action(request).await?;
        let stream: ActionStream = Box::pin(response.into_inner());
        Ok(Response::new(stream))
    }

    async fn forward_put<S, E>(
        &self,
        addr: &str,
        namespace: &str,
        tenant: &TenantId,
        input: S,
    ) -> Result<Response<BoxStream<PutResult>>, Status>
    where
        S: Stream<Item = std::result::Result<FlightData, E>> + Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        let endpoint = self.peer_security.endpoint_for_authority(addr);
        let channel = self
            .peer_security
            .connect(endpoint)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let mut client = FlightClient::new(channel);
        self.peer_security
            .apply_to_flight_client(&mut client)
            .map_err(|error| Status::internal(error.to_string()))?;
        client.metadata_mut().insert(
            DELEGATED_NAMESPACE_HEADER,
            namespace
                .parse()
                .map_err(|_| Status::internal("authorized namespace is not valid metadata"))?,
        );
        client.metadata_mut().insert(
            DELEGATED_TENANT_HEADER,
            tenant
                .as_str()
                .parse()
                .map_err(|_| Status::internal("authenticated tenant is not valid metadata"))?,
        );
        let results = client
            .do_put(input.map(|item| {
                item.map_err(|error| arrow_flight::error::FlightError::protocol(error.to_string()))
            }))
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let results = results.map(|item| item.map_err(|error| Status::internal(error.to_string())));
        Ok(Response::new(Box::pin(results)))
    }

    /// `create_table`: serve locally if leader, else forward to the leader,
    /// then materialize and register the table described by the JSON body.
    async fn action_create_table(&self, action: Action) -> Result<Response<ActionStream>, Status> {
        let req: CreateTableReq = parse_body(&action.body)?;
        if let Some(forwarded) = self.maybe_forward(&action, &req.namespace).await? {
            return Ok(forwarded);
        }
        let schema = build_schema(&req.columns)?;
        let table = TableRef::new(req.namespace, req.name);
        let location = self
            .table_placement
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("remote table placement is not configured"))?
            .place(&table)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.metasrv
            .create_table(&table, location, schema)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let stream: ActionStream = Box::pin(futures::stream::empty());
        Ok(Response::new(stream))
    }

    /// Durably tombstone, detach, and clean one table incarnation. Repeated
    /// requests converge after crashes because [`Metasrv::drop_table`] resumes
    /// any existing tombstone before inspecting the current registration.
    async fn action_drop_table(&self, action: Action) -> Result<Response<ActionStream>, Status> {
        let req: TableIdent = parse_body(&action.body)?;
        if let Some(forwarded) = self.maybe_forward(&action, &req.namespace).await? {
            return Ok(forwarded);
        }
        let table = TableRef::new(req.namespace, req.name);
        self.metasrv
            .drop_table(&table)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(Box::pin(futures::stream::empty())))
    }

    /// `resolve`: return the table's registration as one JSON result, or
    /// `not_found` if it is not registered.
    async fn action_resolve(&self, body: &[u8]) -> Result<Response<ActionStream>, Status> {
        let req: TableIdent = parse_body(body)?;
        let table = TableRef::new(req.namespace, req.name);
        let reg = self
            .metasrv
            .resolve(&table)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found(format!("table '{table}' not found")))?;
        respond_json(&reg)
    }

    /// `list_tables`: return the namespace's table names as a JSON array.
    async fn action_list_tables(&self, body: &[u8]) -> Result<Response<ActionStream>, Status> {
        let req: NamespaceIdent = parse_body(body)?;
        let names = self
            .metasrv
            .list_tables(&Namespace(req.namespace))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let names: Vec<String> = names.into_iter().map(|t| t.0).collect();
        respond_json(&names)
    }

    /// `list_namespaces`: return every namespace as a JSON array.
    async fn action_list_namespaces(
        &self,
        principal: &Principal,
        delegated: Option<&str>,
    ) -> Result<Response<ActionStream>, Status> {
        if delegated.is_some() && principal.role() == PrincipalRole::User {
            return Err(Status::permission_denied(RESOURCE_UNAVAILABLE));
        }
        let namespaces = self
            .metasrv
            .list_namespaces()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let namespaces: Vec<String> = namespaces
            .into_iter()
            .filter(|namespace| match principal.role() {
                PrincipalRole::Admin
                | PrincipalRole::QueryService
                | PrincipalRole::MetadataPeer => true,
                PrincipalRole::User => principal.can_access_namespace(&namespace.0),
            })
            .map(|n| n.0)
            .collect();
        respond_json(&namespaces)
    }
}

#[tonic::async_trait]
impl FlightService for MetasrvFlightService {
    type DoActionStream = ActionStream;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListActionsStream = BoxStream<ActionType>;
    type ListFlightsStream = BoxStream<FlightInfo>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(UNSUPPORTED))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented(UNSUPPORTED))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(UNSUPPORTED))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(UNSUPPORTED))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented(UNSUPPORTED))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented(UNSUPPORTED))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let span = tracing::info_span!(
            target: "lake_metasrv",
            "flight.server",
            rpc.system = "grpc",
            rpc.service = "lake.metasrv",
            rpc.method = "do_put",
            rpc.outcome = field::Empty,
        );
        let _ = set_span_parent_from_request(&span, &request);
        let result = async move {
            let principal = principal(&request)?;
            let delegated = delegated_namespace(&request)?;
            let tenant = operation_tenant(&principal, delegated_tenant(&request)?)?;
            let permit = self.append_admission.acquire().await?;
            let mut input = request.into_inner();
            let first = input
                .next()
                .await
                .ok_or_else(|| Status::invalid_argument("FILE append stream is empty"))??;
            let append = append_request(&first)?;
            let namespace = &append.table().namespace.0;
            authorize_namespace(&principal, delegated.as_deref(), namespace)?;
            let input = Box::pin(futures::stream::once(async move { Ok(first) }).chain(input));
            if !self.leadership.is_leader() {
                match self.leadership.leader() {
                    Some(addr) if addr != self.own_addr => {
                        let response = self.forward_put(&addr, namespace, &tenant, input).await;
                        drop(permit);
                        return response;
                    }
                    Some(_) => {}
                    None => return Err(Status::unavailable("no leader elected")),
                }
            }
            let version = append_file_stream_with_limits(
                &self.metasrv,
                tenant,
                input,
                self.append_admission.limits,
            )
            .await?;
            #[cfg(feature = "test")]
            if let Some(gate) = &self.append_result_gate
                && gate.block_first().await
            {
                return Err(Status::unavailable(
                    "injected post-commit append response loss",
                ));
            }
            let result = PutResult {
                app_metadata: serde_json::to_vec(&version)
                    .map_err(|error| Status::internal(error.to_string()))?
                    .into(),
            };
            let stream: Self::DoPutStream =
                Box::pin(futures::stream::once(async move { Ok(result) }));
            let response = Response::new(stream);
            drop(permit);
            Ok(response)
        }
        .instrument(span.clone())
        .await;
        finish_stream_rpc(&span, result)
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(UNSUPPORTED))
    }

    /// Dispatch on the action type. This is the only real Flight opcode the
    /// control plane serves; unknown types are `unimplemented`.
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let span = tracing::info_span!(
            target: "lake_metasrv",
            "flight.server",
            rpc.system = "grpc",
            rpc.service = "lake.metasrv",
            rpc.method = "do_action",
            rpc.outcome = field::Empty,
        );
        let _ = set_span_parent_from_request(&span, &request);
        let result = async move {
            let principal = principal(&request)?;
            let delegated = delegated_namespace(&request)?;
            let action = request.into_inner();
            match action.r#type.as_str() {
                "create_table" => {
                    let req: CreateTableReq = parse_body(&action.body)?;
                    authorize_namespace(&principal, delegated.as_deref(), &req.namespace)?;
                    self.action_create_table(action).await
                }
                "drop_table" => {
                    let req: TableIdent = parse_body(&action.body)?;
                    authorize_namespace(&principal, delegated.as_deref(), &req.namespace)?;
                    self.action_drop_table(action).await
                }
                "resolve" => {
                    let req: TableIdent = parse_body(&action.body)?;
                    authorize_namespace(&principal, delegated.as_deref(), &req.namespace)?;
                    self.action_resolve(&action.body).await
                }
                "list_tables" => {
                    let req: NamespaceIdent = parse_body(&action.body)?;
                    authorize_namespace(&principal, delegated.as_deref(), &req.namespace)?;
                    self.action_list_tables(&action.body).await
                }
                "list_namespaces" => {
                    self.action_list_namespaces(&principal, delegated.as_deref())
                        .await
                }
                other => Err(Status::unimplemented(format!(
                    "unknown action type '{other}'"
                ))),
            }
        }
        .instrument(span.clone())
        .await;
        finish_stream_rpc(&span, result)
    }

    /// Advertise the four control-plane actions and their descriptions.
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = [
            ActionType {
                r#type:      "create_table".to_string(),
                description: "Create and register a server-placed table (leader only). Body JSON: \
                              {namespace, name, columns:[\"name:type\"]}"
                    .to_string(),
            },
            ActionType {
                r#type:      "drop_table".to_string(),
                description: "Durably tombstone, deregister, and clean one table incarnation \
                              (leader only). Body JSON: {namespace, name}"
                    .to_string(),
            },
            ActionType {
                r#type:      "resolve".to_string(),
                description: "Resolve a table to its registration. Body JSON: {namespace, name}"
                    .to_string(),
            },
            ActionType {
                r#type:      "list_tables".to_string(),
                description: "List table names in a namespace. Body JSON: {namespace}".to_string(),
            },
            ActionType {
                r#type:      "list_namespaces".to_string(),
                description: "List all namespaces. No body.".to_string(),
            },
        ];
        let stream: Self::ListActionsStream = Box::pin(futures::stream::iter(
            actions.into_iter().map(Ok::<_, Status>),
        ));
        Ok(Response::new(stream))
    }
}

#[cfg(test)]
mod file_append_tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use arrow_flight::{FlightDescriptor, encode::FlightDataEncoderBuilder};
    use datafusion::arrow::{
        array::StringArray,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use futures::TryStreamExt;
    use lake_common::{
        AppendOperation, AppendOperationId, FILE_APPEND_TYPE_URL, FileAppendRequest, TableLocation,
        TableRef, TenantId, Version,
    };
    use lake_engine::TableEngineRef;
    use lake_engine_lance::LanceEngine;
    use lake_flight::append_flight_payload_digest;
    use lake_meta::{
        GuardedMutation, MetaError, MetaStore, MetaStoreRef, RocksMeta, SignaledMutation,
    };
    use prost::Message;
    use prost_types::Any;

    use super::{AppendAdmission, append_file_stream, append_file_stream_with_limits};
    use crate::{
        AppendLimits, Metasrv,
        election::{LeaseElection, LeaseStatus},
        leadership::Leadership,
        operation::{AppendRecord, active_key, operation_key},
    };

    struct FailOperationReservationMeta {
        inner:     MetaStoreRef,
        fail_once: AtomicBool,
    }

    struct FailOperationTransitionMeta {
        inner:     MetaStoreRef,
        needle:    &'static [u8],
        fail_once: AtomicBool,
    }

    struct TakeoverBeforeEnginePublicationMeta {
        inner:         MetaStoreRef,
        guarded_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl MetaStore for TakeoverBeforeEnginePublicationMeta {
        async fn get(&self, key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
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

        async fn signaled_mutate(&self, mutation: SignaledMutation<'_>) -> lake_meta::Result<bool> {
            self.inner.signaled_mutate(mutation).await
        }

        async fn guarded_mutate(&self, mutation: GuardedMutation<'_>) -> lake_meta::Result<bool> {
            let call = self.guarded_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == 3 {
                let takeover =
                    LeaseElection::new(self.inner.clone(), "b", Duration::from_millis(10));
                let status = takeover
                    .campaign_at(20)
                    .await
                    .expect("injected takeover succeeds");
                assert!(matches!(status, LeaseStatus::Leader { .. }));
            }
            self.inner.guarded_mutate(mutation).await
        }

        async fn list_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<String>> {
            self.inner.list_prefix(prefix).await
        }

        async fn scan_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<(String, Vec<u8>)>> {
            self.inner.scan_prefix(prefix).await
        }

        async fn delete(&self, key: &str, expected: &[u8]) -> lake_meta::Result<bool> {
            self.inner.delete(key, expected).await
        }
    }

    #[async_trait::async_trait]
    impl MetaStore for FailOperationReservationMeta {
        async fn get(&self, key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
            self.inner.get(key).await
        }

        async fn cas(
            &self,
            key: &str,
            expected: Option<&[u8]>,
            new: &[u8],
        ) -> lake_meta::Result<bool> {
            if key.starts_with(crate::operation::OPERATION_PREFIX)
                && self.fail_once.swap(false, Ordering::SeqCst)
            {
                return Err(MetaError::Dynamo {
                    message: "injected operation reservation failure".to_owned(),
                    source:  Box::new(std::io::Error::other("injected")),
                });
            }
            self.inner.cas(key, expected, new).await
        }

        async fn signaled_mutate(&self, mutation: SignaledMutation<'_>) -> lake_meta::Result<bool> {
            self.inner.signaled_mutate(mutation).await
        }

        async fn list_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<String>> {
            self.inner.list_prefix(prefix).await
        }

        async fn scan_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<(String, Vec<u8>)>> {
            self.inner.scan_prefix(prefix).await
        }

        async fn delete(&self, key: &str, expected: &[u8]) -> lake_meta::Result<bool> {
            self.inner.delete(key, expected).await
        }
    }

    #[async_trait::async_trait]
    impl MetaStore for FailOperationTransitionMeta {
        async fn get(&self, key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
            self.inner.get(key).await
        }

        async fn cas(
            &self,
            key: &str,
            expected: Option<&[u8]>,
            new: &[u8],
        ) -> lake_meta::Result<bool> {
            if key.starts_with(crate::operation::OPERATION_PREFIX)
                && new
                    .windows(self.needle.len())
                    .any(|window| window == self.needle)
                && self.fail_once.swap(false, Ordering::SeqCst)
            {
                return Err(MetaError::Dynamo {
                    message: "injected operation transition failure".to_owned(),
                    source:  Box::new(std::io::Error::other("injected")),
                });
            }
            self.inner.cas(key, expected, new).await
        }

        async fn signaled_mutate(&self, mutation: SignaledMutation<'_>) -> lake_meta::Result<bool> {
            self.inner.signaled_mutate(mutation).await
        }

        async fn list_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<String>> {
            self.inner.list_prefix(prefix).await
        }

        async fn scan_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<(String, Vec<u8>)>> {
            self.inner.scan_prefix(prefix).await
        }

        async fn delete(&self, key: &str, expected: &[u8]) -> lake_meta::Result<bool> {
            self.inner.delete(key, expected).await
        }
    }

    async fn encoded_append(
        table: TableRef,
        schema: Arc<Schema>,
        batch: RecordBatch,
        operation_id: AppendOperationId,
    ) -> (FileAppendRequest, Vec<arrow_flight::FlightData>) {
        let mut messages = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(vec![Ok(batch)]))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let append =
            FileAppendRequest::new(table, operation_id, append_flight_payload_digest(&messages));
        messages[0].flight_descriptor = Some(FlightDescriptor::new_cmd(
            Any {
                type_url: FILE_APPEND_TYPE_URL.to_owned(),
                value:    append.command_payload(),
            }
            .encode_to_vec(),
        ));
        (append, messages)
    }

    fn operation(append: &FileAppendRequest, tenant: &TenantId) -> AppendOperation {
        AppendOperation::builder()
            .tenant(tenant.clone())
            .operation_id(append.operation_id().clone())
            .payload_digest(append.payload_digest().clone())
            .build()
    }

    #[tokio::test]
    async fn append_admission_rejects_concurrency_saturation_and_releases() {
        let limits =
            AppendLimits::try_new(1, Duration::from_millis(20), 64, 128).expect("valid limits");
        let admission = AppendAdmission::new(limits);

        let first = admission.acquire().await.expect("first append admitted");
        let saturated = admission
            .acquire()
            .await
            .expect_err("second append must exceed concurrency");
        assert_eq!(saturated.code(), tonic::Code::ResourceExhausted);

        drop(first);
        assert!(admission.acquire().await.is_ok());
    }

    #[tokio::test]
    async fn append_admission_reserves_worst_case_buffer_budget() {
        let limits =
            AppendLimits::try_new(2, Duration::from_millis(20), 64, 64).expect("valid limits");
        let admission = AppendAdmission::new(limits);

        let first = admission.acquire().await.expect("first append admitted");
        let saturated = admission
            .acquire()
            .await
            .expect_err("second append must exceed buffered-byte budget");
        assert_eq!(saturated.code(), tonic::Code::ResourceExhausted);

        drop(first);
        assert!(admission.acquire().await.is_ok());
    }

    #[tokio::test]
    async fn configured_append_stream_limit_rejects_before_commit() {
        let root = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::new(meta, engine);
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.path().join("episodes.lance").to_string_lossy()),
                schema.clone(),
            )
            .await
            .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["episode-42"]))],
        )
        .unwrap();
        let (_append, messages) =
            encoded_append(table.clone(), schema, batch, AppendOperationId::generate()).await;

        let limits =
            AppendLimits::try_new(1, Duration::from_millis(20), 1, 1).expect("valid limits");
        let error = append_file_stream_with_limits(
            &metasrv,
            TenantId::try_new("tenant-a").unwrap(),
            futures::stream::iter(messages.into_iter().map(Ok::<_, String>)),
            limits,
        )
        .await
        .expect_err("metadata larger than the configured limit must fail");

        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            metasrv
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            Version(1)
        );
    }

    #[tokio::test]
    async fn operation_timestamp_far_in_the_future_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::new(meta, engine);
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.path().join("episodes.lance").to_string_lossy()),
                schema.clone(),
            )
            .await
            .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["episode-42"]))],
        )
        .unwrap();
        let future_id = AppendOperationId::parse("ffffffff-ffff-7000-8000-000000000000")
            .expect("valid far-future UUIDv7");
        let (_append, messages) = encoded_append(table, schema, batch, future_id).await;

        let error = append_file_stream(
            &metasrv,
            TenantId::try_new("tenant-a").unwrap(),
            futures::stream::iter(messages.into_iter().map(Ok::<_, String>)),
        )
        .await
        .expect_err("a far-future identity must not bypass operation expiry");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn file_append_commits_decoded_flight_batches() {
        let root = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::new(meta, engine);
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.path().join("episodes.lance").to_string_lossy()),
                schema.clone(),
            )
            .await
            .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["episode-42"]))],
        )
        .unwrap();
        let (_append, messages) =
            encoded_append(table.clone(), schema, batch, AppendOperationId::generate()).await;
        let stream = futures::stream::iter(messages.into_iter().map(Ok::<_, String>));

        let version = append_file_stream(&metasrv, TenantId::try_new("tenant-a").unwrap(), stream)
            .await
            .unwrap();

        assert_eq!(
            metasrv
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            version
        );
    }

    #[tokio::test]
    async fn same_operation_replay_returns_original_version() {
        let root = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::new(meta, engine);
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.path().join("episodes.lance").to_string_lossy()),
                schema.clone(),
            )
            .await
            .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["episode-42"]))],
        )
        .unwrap();
        let (append, messages) =
            encoded_append(table, schema, batch, AppendOperationId::generate()).await;

        let first = append_file_stream(
            &metasrv,
            TenantId::try_new("tenant-a").unwrap(),
            futures::stream::iter(messages.clone().into_iter().map(Ok::<_, String>)),
        )
        .await
        .unwrap();
        let replay = append_file_stream(
            &metasrv,
            TenantId::try_new("tenant-a").unwrap(),
            futures::stream::iter(messages.into_iter().map(Ok::<_, String>)),
        )
        .await
        .unwrap();

        assert_eq!(replay, first);
        assert_eq!(
            metasrv
                .resolve(append.table())
                .await
                .unwrap()
                .unwrap()
                .current_version,
            first
        );
    }

    #[tokio::test]
    async fn append_recovers_after_stale_leader_engine_commit() {
        let root = tempfile::tempdir().unwrap();
        let raw: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        Metasrv::new(raw.clone(), engine.clone())
            .create_table(
                &table,
                TableLocation::new(root.path().join("episodes.lance").to_string_lossy()),
                schema.clone(),
            )
            .await
            .unwrap();

        let election_a = LeaseElection::new(raw.clone(), "a", Duration::from_millis(10));
        let LeaseStatus::Leader { guard: guard_a, .. } = election_a.campaign_at(0).await.unwrap()
        else {
            panic!("a must acquire the lease");
        };
        let leadership_a = Arc::new(Leadership::new());
        leadership_a.assume_guarded_leader("a", guard_a, Duration::from_mins(1));
        let takeover_meta: MetaStoreRef = Arc::new(TakeoverBeforeEnginePublicationMeta {
            inner:         raw.clone(),
            guarded_calls: AtomicUsize::new(0),
        });
        let authority_a =
            Metasrv::new(takeover_meta, engine.clone()).fenced_for_server(leadership_a);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["episode-42"]))],
        )
        .unwrap();
        let (_append, messages) =
            encoded_append(table.clone(), schema, batch, AppendOperationId::generate()).await;
        let first = append_file_stream(
            &authority_a,
            TenantId::try_new("tenant-a").unwrap(),
            futures::stream::iter(messages.clone().into_iter().map(Ok::<_, String>)),
        )
        .await;
        assert!(first.is_err(), "stale publication must be rejected");

        let election_b = LeaseElection::new(raw.clone(), "b", Duration::from_millis(10));
        let LeaseStatus::Leader { guard: guard_b, .. } = election_b.campaign_at(21).await.unwrap()
        else {
            panic!("b must hold the takeover lease");
        };
        assert_eq!(guard_b.epoch(), 2);
        let leadership_b = Arc::new(Leadership::new());
        leadership_b.assume_guarded_leader("b", guard_b, Duration::from_mins(1));
        let authority_b = Metasrv::new(raw, engine).fenced_for_server(leadership_b);
        let recovered = append_file_stream(
            &authority_b,
            TenantId::try_new("tenant-a").unwrap(),
            futures::stream::iter(messages.into_iter().map(Ok::<_, String>)),
        )
        .await
        .unwrap();

        assert_eq!(recovered, Version(2));
        assert_eq!(
            authority_b
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            Version(2)
        );
    }

    #[tokio::test]
    async fn append_crash_window_terminal_replay_clears_stale_active_fence() {
        let root = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::new(meta.clone(), engine);
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.path().join("episodes.lance").to_string_lossy()),
                schema.clone(),
            )
            .await
            .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["episode-42"]))],
        )
        .unwrap();
        let (append, messages) =
            encoded_append(table, schema, batch, AppendOperationId::generate()).await;
        let tenant = TenantId::try_new("tenant-a").unwrap();
        append_file_stream(
            &metasrv,
            tenant.clone(),
            futures::stream::iter(messages.clone().into_iter().map(Ok::<_, String>)),
        )
        .await
        .unwrap();
        let operation = operation(&append, &tenant);
        let key = operation_key(&operation, append.table());
        let active = active_key(&operation, append.table());
        assert!(meta.cas(&active, None, key.as_bytes()).await.unwrap());

        append_file_stream(
            &metasrv,
            tenant,
            futures::stream::iter(messages.into_iter().map(Ok::<_, String>)),
        )
        .await
        .expect("terminal replay repairs crash-left active fence");

        assert_eq!(meta.get(&active).await.unwrap(), None);
    }

    #[tokio::test]
    async fn replay_after_drop_recreate_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::new(meta, engine);
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.path().join("first.lance").to_string_lossy()),
                schema.clone(),
            )
            .await
            .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["episode-42"]))],
        )
        .unwrap();
        let (_append, messages) = encoded_append(
            table.clone(),
            schema.clone(),
            batch,
            AppendOperationId::generate(),
        )
        .await;
        let tenant = TenantId::try_new("tenant-a").unwrap();
        assert_eq!(
            append_file_stream(
                &metasrv,
                tenant.clone(),
                futures::stream::iter(messages.clone().into_iter().map(Ok::<_, String>)),
            )
            .await
            .unwrap(),
            Version(2)
        );

        metasrv.drop_table(&table).await.unwrap();
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.path().join("replacement.lance").to_string_lossy()),
                schema,
            )
            .await
            .unwrap();
        let error = append_file_stream(
            &metasrv,
            tenant,
            futures::stream::iter(messages.into_iter().map(Ok::<_, String>)),
        )
        .await
        .expect_err(
            "an operation from a dropped table incarnation must not target its replacement",
        );

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            metasrv
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            Version(1)
        );
    }

    #[tokio::test]
    async fn mismatched_flight_payload_digest_is_rejected_before_commit() {
        let root = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::new(meta, engine);
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.path().join("episodes.lance").to_string_lossy()),
                schema.clone(),
            )
            .await
            .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["episode-42"]))],
        )
        .unwrap();
        let (_append, mut messages) =
            encoded_append(table.clone(), schema, batch, AppendOperationId::generate()).await;
        let mismatched = FileAppendRequest::new(
            table.clone(),
            AppendOperationId::generate(),
            lake_common::AppendPayloadDigest::parse(
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            )
            .unwrap(),
        );
        messages[0].flight_descriptor = Some(FlightDescriptor::new_cmd(
            Any {
                type_url: FILE_APPEND_TYPE_URL.to_owned(),
                value:    mismatched.command_payload(),
            }
            .encode_to_vec(),
        ));

        let error = append_file_stream(
            &metasrv,
            TenantId::try_new("tenant-a").unwrap(),
            futures::stream::iter(messages.into_iter().map(Ok::<_, String>)),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            metasrv
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            Version(1)
        );
    }

    #[tokio::test]
    async fn same_operation_with_different_payload_conflicts() {
        let root = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::new(meta, engine);
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.path().join("episodes.lance").to_string_lossy()),
                schema.clone(),
            )
            .await
            .unwrap();
        let operation_id = AppendOperationId::generate();
        let first_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["episode-42"]))],
        )
        .unwrap();
        let (_first, first_messages) = encoded_append(
            table.clone(),
            schema.clone(),
            first_batch,
            operation_id.clone(),
        )
        .await;
        append_file_stream(
            &metasrv,
            TenantId::try_new("tenant-a").unwrap(),
            futures::stream::iter(first_messages.into_iter().map(Ok::<_, String>)),
        )
        .await
        .unwrap();
        let second_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["episode-99"]))],
        )
        .unwrap();
        let (_second, second_messages) =
            encoded_append(table.clone(), schema, second_batch, operation_id).await;

        let error = append_file_stream(
            &metasrv,
            TenantId::try_new("tenant-a").unwrap(),
            futures::stream::iter(second_messages.into_iter().map(Ok::<_, String>)),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code(), tonic::Code::AlreadyExists);
        assert_eq!(
            metasrv
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            Version(2)
        );
    }

    #[tokio::test]
    async fn append_crash_windows_reconcile_without_duplicates() {
        for transition in [
            b"\"state\":\"engine_committed\"".as_slice(),
            b"\"state\":\"committed\"".as_slice(),
        ] {
            let root = tempfile::tempdir().unwrap();
            let underlying: MetaStoreRef =
                Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
            let failing: MetaStoreRef = Arc::new(FailOperationTransitionMeta {
                inner:     underlying.clone(),
                needle:    transition,
                fail_once: AtomicBool::new(true),
            });
            let engine: TableEngineRef = Arc::new(LanceEngine::new());
            let first_metasrv = Metasrv::new(failing, engine.clone());
            let table = TableRef::new("robots", "episodes");
            let schema = Arc::new(Schema::new(vec![Field::new(
                "episode_id",
                DataType::Utf8,
                false,
            )]));
            first_metasrv
                .create_table(
                    &table,
                    TableLocation::new(root.path().join("episodes.lance").to_string_lossy()),
                    schema.clone(),
                )
                .await
                .unwrap();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vec!["episode-42"]))],
            )
            .unwrap();
            let (_append, messages) =
                encoded_append(table.clone(), schema, batch, AppendOperationId::generate()).await;
            let tenant = TenantId::try_new("tenant-a").unwrap();

            assert!(
                append_file_stream(
                    &first_metasrv,
                    tenant.clone(),
                    futures::stream::iter(messages.clone().into_iter().map(Ok::<_, String>)),
                )
                .await
                .is_err(),
                "the injected transition must interrupt the first response"
            );
            let recovered = Metasrv::new(underlying, engine);
            let version = append_file_stream(
                &recovered,
                tenant,
                futures::stream::iter(messages.into_iter().map(Ok::<_, String>)),
            )
            .await
            .unwrap();

            assert_eq!(version, Version(2));
            assert_eq!(
                recovered
                    .resolve(&table)
                    .await
                    .unwrap()
                    .unwrap()
                    .current_version,
                Version(2)
            );
        }
    }

    #[tokio::test]
    async fn committed_replay_does_not_contend_with_new_active_operation() {
        let root = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::new(meta.clone(), engine);
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.path().join("episodes.lance").to_string_lossy()),
                schema.clone(),
            )
            .await
            .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["episode-42"]))],
        )
        .unwrap();
        let (committed, messages) =
            encoded_append(table.clone(), schema, batch, AppendOperationId::generate()).await;
        let tenant = TenantId::try_new("tenant-a").unwrap();
        let first = append_file_stream(
            &metasrv,
            tenant.clone(),
            futures::stream::iter(messages.clone().into_iter().map(Ok::<_, String>)),
        )
        .await
        .unwrap();
        let other = AppendOperation::builder()
            .tenant(tenant.clone())
            .operation_id(AppendOperationId::generate())
            .payload_digest(committed.payload_digest().clone())
            .build();
        let active = active_key(&other, committed.table());
        let other_key = operation_key(&other, committed.table());
        assert!(meta.cas(&active, None, other_key.as_bytes()).await.unwrap());

        let replay = append_file_stream(
            &metasrv,
            tenant,
            futures::stream::iter(messages.into_iter().map(Ok::<_, String>)),
        )
        .await
        .unwrap();

        assert_eq!(replay, first);
    }

    #[tokio::test]
    async fn append_crash_window_reservation_failure_recovers_without_orphan_fence() {
        let root = tempfile::tempdir().unwrap();
        let underlying: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let meta: MetaStoreRef = Arc::new(FailOperationReservationMeta {
            inner:     underlying.clone(),
            fail_once: AtomicBool::new(true),
        });
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::new(meta, engine.clone());
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.path().join("episodes.lance").to_string_lossy()),
                schema.clone(),
            )
            .await
            .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["episode-42"]))],
        )
        .unwrap();
        let (append, messages) =
            encoded_append(table, schema, batch, AppendOperationId::generate()).await;
        let tenant = TenantId::try_new("tenant-a").unwrap();
        let append_operation = operation(&append, &tenant);

        let result = append_file_stream(
            &metasrv,
            tenant,
            futures::stream::iter(messages.clone().into_iter().map(Ok::<_, String>)),
        )
        .await;

        assert!(
            result.is_err(),
            "the injected reservation failure must surface"
        );
        assert_eq!(
            underlying
                .get(&active_key(&append_operation, append.table()))
                .await
                .unwrap(),
            None,
            "failed reservation must not permanently fence the table"
        );
        let recovered = Metasrv::new(underlying, engine);
        assert_eq!(
            append_file_stream(
                &recovered,
                TenantId::try_new("tenant-a").unwrap(),
                futures::stream::iter(messages.into_iter().map(Ok::<_, String>)),
            )
            .await
            .unwrap(),
            Version(2)
        );
    }

    #[tokio::test]
    async fn append_crash_window_reserved_before_fencing_recovers_after_restart() {
        let root = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Metasrv::new(meta.clone(), engine.clone());
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.path().join("episodes.lance").to_string_lossy()),
                schema.clone(),
            )
            .await
            .unwrap();
        let batch = || {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vec!["episode-42"]))],
            )
            .unwrap()
        };
        let tenant = TenantId::try_new("tenant-a").unwrap();
        let (stale_append, stale_messages) = encoded_append(
            table.clone(),
            schema.clone(),
            batch(),
            AppendOperationId::generate(),
        )
        .await;
        let stale_operation = operation(&stale_append, &tenant);
        let stale_key = operation_key(&stale_operation, &table);
        let incarnation = metasrv
            .resolve(&table)
            .await
            .unwrap()
            .unwrap()
            .incarnation_id()
            .unwrap()
            .to_owned();
        let stale_record =
            AppendRecord::reserved(&stale_operation, &table, &incarnation, Version(1), 1);
        assert!(
            meta.cas(&stale_key, None, &stale_record.encode().unwrap())
                .await
                .unwrap()
        );

        let (_other, other_messages) = encoded_append(
            table.clone(),
            schema.clone(),
            batch(),
            AppendOperationId::generate(),
        )
        .await;
        assert_eq!(
            append_file_stream(
                &metasrv,
                tenant.clone(),
                futures::stream::iter(other_messages.into_iter().map(Ok::<_, String>)),
            )
            .await
            .unwrap(),
            Version(2)
        );

        let reconstructed = Metasrv::new(meta, engine);
        let recovered = append_file_stream(
            &reconstructed,
            tenant,
            futures::stream::iter(stale_messages.into_iter().map(Ok::<_, String>)),
        )
        .await
        .expect("a reservation created before fencing must recover after another commit");
        assert_eq!(recovered, Version(3));
        assert_eq!(
            reconstructed
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            Version(3)
        );
    }

    #[tokio::test]
    async fn concurrent_replays_execute_one_engine_append() {
        let root = tempfile::tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let first_metasrv = Metasrv::new(meta.clone(), engine.clone());
        // Both requests enter the same elected metadata authority. Its
        // per-table coordinator serializes engine access while the durable
        // operation record makes the second request a replay.
        let second_metasrv = first_metasrv.clone();
        let location = TableLocation::new(root.path().join("episodes.lance").to_string_lossy());
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        first_metasrv
            .create_table(&table, location.clone(), schema.clone())
            .await
            .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["episode-42"]))],
        )
        .unwrap();
        let (_append, messages) =
            encoded_append(table.clone(), schema, batch, AppendOperationId::generate()).await;
        let tenant = TenantId::try_new("tenant-a").unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let first_messages = messages.clone();
        let first_tenant = tenant.clone();
        let first_barrier = barrier.clone();
        let first_call = async {
            first_barrier.wait().await;
            append_file_stream(
                &first_metasrv,
                first_tenant,
                futures::stream::iter(first_messages.into_iter().map(Ok::<_, String>)),
            )
            .await
        };
        let second_call = async {
            barrier.wait().await;
            append_file_stream(
                &second_metasrv,
                tenant,
                futures::stream::iter(messages.into_iter().map(Ok::<_, String>)),
            )
            .await
        };

        let (first, second) = tokio::join!(first_call, second_call);

        assert_eq!(first.unwrap(), second.unwrap());
        assert_eq!(
            first_metasrv
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            Version(2)
        );
        assert_eq!(
            engine
                .open(&location)
                .await
                .unwrap()
                .unwrap()
                .current_version(),
            Version(2)
        );
    }
}

#[cfg(test)]
mod schema_tests {
    use lake_objects::data_location_field;

    use super::build_schema;

    #[test]
    fn remote_schema_dsl_accepts_file() {
        let schema = build_schema(&["video:file".to_owned()]).unwrap();

        assert_eq!(schema.field(0), &data_location_field("video", false));
    }
}

#[cfg(test)]
mod table_placement_tests {
    use std::sync::Arc;

    use arrow_flight::Action;
    use lake_common::TableRef;
    use lake_engine::TableEngineRef;
    use lake_engine_lance::LanceEngine;
    use lake_flight::ClientSecurity;
    use lake_meta::{MetaStoreRef, RocksMeta};
    use serde_json::json;
    use tonic::Code;

    use super::{AppendAdmission, MetasrvFlightService};
    use crate::{AppendLimits, Metasrv, TablePlacement, leadership::Leadership};

    fn service(root: &tempfile::TempDir) -> MetasrvFlightService {
        let meta: MetaStoreRef =
            Arc::new(RocksMeta::open(root.path().join("meta")).expect("open metastore"));
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let leadership = Arc::new(Leadership::new());
        leadership.assume_leader("127.0.0.1:50052");
        MetasrvFlightService {
            metasrv: Arc::new(Metasrv::new(meta, engine)),
            leadership,
            own_addr: "127.0.0.1:50052".to_owned(),
            peer_security: ClientSecurity::new(),
            table_placement: Some(TablePlacement::local(root.path().join("tables"))),
            append_admission: AppendAdmission::new(AppendLimits::default()),
            #[cfg(feature = "test")]
            append_result_gate: None,
        }
    }

    fn create_action(body: serde_json::Value) -> Action {
        Action {
            r#type: "create_table".to_owned(),
            body:   serde_json::to_vec(&body).expect("serialize action").into(),
        }
    }

    fn drop_action(namespace: &str, name: &str) -> Action {
        Action {
            r#type: "drop_table".to_owned(),
            body:   serde_json::to_vec(&json!({
                "namespace": namespace,
                "name": name,
            }))
            .expect("serialize action")
            .into(),
        }
    }

    #[tokio::test]
    async fn remote_drop_removes_dataset_and_is_idempotent() {
        let root = tempfile::tempdir().expect("temporary root");
        let service = service(&root);
        service
            .action_create_table(create_action(json!({
                "namespace": "robots",
                "name": "episodes",
                "columns": ["episode_id:utf8"]
            })))
            .await
            .expect("create table");
        let table = TableRef::new("robots", "episodes");
        let registration = service
            .metasrv
            .resolve(&table)
            .await
            .unwrap()
            .expect("created registration");

        service
            .action_drop_table(drop_action("robots", "episodes"))
            .await
            .expect("durable remote drop");
        service
            .action_drop_table(drop_action("robots", "episodes"))
            .await
            .expect("idempotent repeated drop");
        assert!(
            service.metasrv.resolve(&table).await.unwrap().is_none(),
            "drop must detach the registry"
        );
        assert!(
            service
                .metasrv
                .engine()
                .open(&registration.location)
                .await
                .unwrap()
                .is_none(),
            "drop must remove the old dataset"
        );
    }

    #[tokio::test]
    async fn remote_create_uses_server_table_placement() {
        let root = tempfile::tempdir().expect("temporary root");
        let service = service(&root);
        service
            .action_create_table(create_action(json!({
                "namespace": "robots",
                "name": "episodes",
                "columns": ["episode_id:utf8"]
            })))
            .await
            .expect("server-derived create succeeds");

        let registration = service
            .metasrv
            .resolve(&TableRef::new("robots", "episodes"))
            .await
            .expect("resolve table")
            .expect("table is registered");
        let location = std::path::Path::new(registration.location.as_str());
        assert_eq!(
            location.parent(),
            Some(root.path().join("tables/robots/episodes").as_path())
        );
        assert_eq!(
            location.extension().and_then(std::ffi::OsStr::to_str),
            Some("lance")
        );
    }

    #[tokio::test]
    async fn remote_create_rejects_caller_location() {
        let root = tempfile::tempdir().expect("temporary root");
        let service = service(&root);
        let result = service
            .action_create_table(create_action(json!({
                "namespace": "robots",
                "name": "episodes",
                "columns": ["episode_id:utf8"],
                "location": root.path().join("caller-selected.lance")
            })))
            .await;
        let error = match result {
            Ok(_) => panic!("legacy caller-selected locations must fail closed"),
            Err(error) => error,
        };

        assert_eq!(error.code(), Code::InvalidArgument);
        assert!(
            service
                .metasrv
                .resolve(&TableRef::new("robots", "episodes"))
                .await
                .expect("resolve table")
                .is_none()
        );
    }

    #[tokio::test]
    async fn remote_create_rejects_overlong_dataset_segment_before_mutation() {
        let root = tempfile::tempdir().expect("temporary root");
        let service = service(&root);
        let name = "x".repeat(256);
        let result = service
            .action_create_table(create_action(json!({
                "namespace": "bounds",
                "name": name,
                "columns": ["episode_id:utf8"]
            })))
            .await;
        let error = match result {
            Ok(_) => panic!("overlong table directory segment must fail before storage"),
            Err(error) => error,
        };

        assert_eq!(error.code(), Code::InvalidArgument);
        assert!(
            service
                .metasrv
                .resolve(&TableRef::new("bounds", name))
                .await
                .expect("resolve table")
                .is_none()
        );
        assert!(
            !root.path().join("tables/bounds").exists(),
            "placement rejection must happen before engine filesystem mutation"
        );
    }
}

#[cfg(test)]
mod authorization_tests {
    use lake_common::{Principal, PrincipalId, PrincipalRole, TenantId};
    use tonic::Code;

    use super::{authorize_namespace, operation_tenant};

    fn principal(role: PrincipalRole, namespaces: &[&str]) -> Principal {
        Principal::try_new(
            PrincipalId::try_new("caller").unwrap(),
            TenantId::try_new("tenant-a").unwrap(),
            role,
            namespaces,
        )
        .unwrap()
    }

    #[test]
    fn metasrv_rejects_cross_tenant_mutations() {
        let user = principal(PrincipalRole::User, &["alpha"]);
        assert!(authorize_namespace(&user, None, "alpha").is_ok());
        assert_eq!(
            authorize_namespace(&user, None, "beta").unwrap_err().code(),
            Code::PermissionDenied
        );
        assert_eq!(
            authorize_namespace(&user, Some("alpha"), "alpha")
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );

        let query = principal(PrincipalRole::QueryService, &[]);
        assert!(authorize_namespace(&query, Some("alpha"), "alpha").is_ok());
        assert_eq!(
            authorize_namespace(&query, None, "alpha")
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            authorize_namespace(&query, Some("beta"), "alpha")
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );

        assert_eq!(
            operation_tenant(&user, Some(TenantId::try_new("tenant-b").unwrap()))
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            operation_tenant(&query, Some(TenantId::try_new("tenant-b").unwrap()))
                .unwrap()
                .as_str(),
            "tenant-b"
        );
    }
}
