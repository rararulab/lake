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
    FILE_APPEND_TYPE_URL, FileAppendRequest, Namespace, TableLocation, TableRef, Version,
};
use lake_flight::ClientSecurity;
use lake_objects::data_location_field;
use prost::Message;
use prost_types::Any;
use serde::{Serialize, de::DeserializeOwned};
use tonic::{Request, Response, Status, Streaming};

use crate::{Metasrv, leadership::Leadership};

/// The [`Status`] message returned by every unsupported Flight method.
const UNSUPPORTED: &str = "metasrv control plane only serves actions and FILE append do_put";

/// A boxed server stream of `T`, the shape every Flight response stream takes.
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// The `DoAction` response stream: a (usually one-shot) stream of Flight
/// results carrying JSON bodies.
type ActionStream = BoxStream<FlightResult>;

async fn append_file_stream<S, E>(metasrv: &Metasrv, mut input: S) -> Result<Version, Status>
where
    S: Stream<Item = std::result::Result<FlightData, E>> + Send + Unpin + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let first = input
        .next()
        .await
        .ok_or_else(|| Status::invalid_argument("FILE append stream is empty"))?
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let descriptor = first
        .flight_descriptor
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("FILE append requires a command descriptor"))?;
    let command = Any::decode(descriptor.cmd.as_ref())
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if command.type_url != FILE_APPEND_TYPE_URL {
        return Err(Status::invalid_argument("invalid FILE append command type"));
    }
    let append = FileAppendRequest::from_command_payload(&command.value)
        .ok_or_else(|| Status::invalid_argument("invalid FILE append descriptor"))?;
    let flight_data = futures::stream::once(async move { Ok(first) }).chain(input.map(|item| {
        item.map_err(|error| arrow_flight::error::FlightError::protocol(error.to_string()))
    }));
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
        .append(append.table(), stream)
        .await
        .map_err(|error| Status::internal(error.to_string()))
}

/// The metadata-layer control-plane Flight service.
///
/// Holds the registry [`Metasrv`] authority it dispatches to and the shared
/// [`Leadership`] flag it gates writes on.
pub(crate) struct MetasrvFlightService {
    /// The registry authority every action dispatches to.
    pub(crate) metasrv:       Arc<Metasrv>,
    /// The shared leadership state consulted before serving a write.
    pub(crate) leadership:    Arc<Leadership>,
    /// This node's own Flight address, used to tell "the leader is me" apart
    /// from "forward to another node" when the leader flag is briefly stale.
    pub(crate) own_addr:      String,
    /// TLS and service identity for forwarding writes to the elected leader.
    pub(crate) peer_security: ClientSecurity,
}

/// `create_table` action body: the table to materialize and register.
#[derive(serde::Deserialize)]
struct CreateTableReq {
    namespace: String,
    name:      String,
    /// Columns as `name:type` (types: `i64`, `f64`, `utf8`, `bool`).
    columns:   Vec<String>,
    /// Storage URI for the dataset. Required: the metasrv does not own the
    /// data-dir layout the way the CLI does, so the caller must supply it.
    location:  Option<String>,
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
    ) -> Result<Option<Response<ActionStream>>, Status> {
        if self.leadership.is_leader() {
            return Ok(None);
        }
        match self.leadership.leader() {
            // We are the elected leader; the flag is just briefly stale.
            Some(addr) if addr == self.own_addr => Ok(None),
            Some(addr) => self.forward(&addr, action).await.map(Some),
            None => Err(Status::unavailable("no leader elected")),
        }
    }

    /// Forward `action` to the leader at `addr` over Flight `DoAction`,
    /// relaying its streamed result as this call's response.
    async fn forward(&self, addr: &str, action: &Action) -> Result<Response<ActionStream>, Status> {
        let endpoint = self.peer_security.endpoint_for_authority(addr);
        let channel = self
            .peer_security
            .connect(endpoint)
            .await
            .map_err(|e| Status::unavailable(format!("cannot reach leader '{addr}': {e}")))?;
        let mut client = FlightServiceClient::new(channel);
        let request = self
            .peer_security
            .authorize_request(Request::new(action.clone()));
        let response = client.do_action(request).await?;
        let stream: ActionStream = Box::pin(response.into_inner());
        Ok(Response::new(stream))
    }

    async fn forward_put(
        &self,
        addr: &str,
        input: Streaming<FlightData>,
    ) -> Result<Response<BoxStream<PutResult>>, Status> {
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
        if let Some(forwarded) = self.maybe_forward(&action).await? {
            return Ok(forwarded);
        }
        let req: CreateTableReq = parse_body(&action.body)?;
        let location = req
            .location
            .ok_or_else(|| Status::invalid_argument("create_table requires a 'location' field"))?;
        let schema = build_schema(&req.columns)?;
        let table = TableRef::new(req.namespace, req.name);
        self.metasrv
            .create_table(&table, TableLocation::new(location), schema)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let stream: ActionStream = Box::pin(futures::stream::empty());
        Ok(Response::new(stream))
    }

    /// `drop_table`: serve locally if leader, else forward to the leader, then
    /// delete the table's data and deregister it. Idempotent.
    async fn action_drop_table(&self, action: Action) -> Result<Response<ActionStream>, Status> {
        if let Some(forwarded) = self.maybe_forward(&action).await? {
            return Ok(forwarded);
        }
        let req: TableIdent = parse_body(&action.body)?;
        let table = TableRef::new(req.namespace, req.name);
        self.metasrv
            .drop_table(&table)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let stream: ActionStream = Box::pin(futures::stream::empty());
        Ok(Response::new(stream))
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
    async fn action_list_namespaces(&self) -> Result<Response<ActionStream>, Status> {
        let namespaces = self
            .metasrv
            .list_namespaces()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let namespaces: Vec<String> = namespaces.into_iter().map(|n| n.0).collect();
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
        if !self.leadership.is_leader() {
            match self.leadership.leader() {
                Some(addr) if addr != self.own_addr => {
                    return self.forward_put(&addr, request.into_inner()).await;
                }
                Some(_) => {}
                None => return Err(Status::unavailable("no leader elected")),
            }
        }
        let version = append_file_stream(&self.metasrv, request.into_inner()).await?;
        let result = PutResult {
            app_metadata: serde_json::to_vec(&version)
                .map_err(|error| Status::internal(error.to_string()))?
                .into(),
        };
        Ok(Response::new(Box::pin(futures::stream::once(async move {
            Ok(result)
        }))))
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
        let action = request.into_inner();
        match action.r#type.as_str() {
            "create_table" => self.action_create_table(action).await,
            "drop_table" => self.action_drop_table(action).await,
            "resolve" => self.action_resolve(&action.body).await,
            "list_tables" => self.action_list_tables(&action.body).await,
            "list_namespaces" => self.action_list_namespaces().await,
            other => Err(Status::unimplemented(format!(
                "unknown action type '{other}'"
            ))),
        }
    }

    /// Advertise the four control-plane actions and their descriptions.
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = [
            ActionType {
                r#type:      "create_table".to_string(),
                description: "Create and register a table (leader only). Body JSON: {namespace, \
                              name, columns:[\"name:type\"], location}"
                    .to_string(),
            },
            ActionType {
                r#type:      "drop_table".to_string(),
                description: "Delete a table's data and deregister it (leader only). Body JSON: \
                              {namespace, name}"
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
    use std::sync::Arc;

    use arrow_flight::{FlightDescriptor, encode::FlightDataEncoderBuilder};
    use datafusion::arrow::{
        array::StringArray,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use lake_common::{FILE_APPEND_TYPE_URL, FileAppendRequest, TableLocation, TableRef};
    use lake_engine::TableEngineRef;
    use lake_engine_lance::LanceEngine;
    use lake_meta::{MetaStoreRef, RocksMeta};
    use prost::Message;
    use prost_types::Any;

    use super::append_file_stream;
    use crate::Metasrv;

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
        let append = FileAppendRequest::new(table.clone());
        let descriptor = FlightDescriptor::new_cmd(
            Any {
                type_url: FILE_APPEND_TYPE_URL.to_owned(),
                value:    append.command_payload(),
            }
            .encode_to_vec(),
        );
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_flight_descriptor(Some(descriptor))
            .build(futures::stream::iter(vec![Ok(batch)]));

        let version = append_file_stream(&metasrv, stream).await.unwrap();

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
