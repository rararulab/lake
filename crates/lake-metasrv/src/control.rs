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
//! `body` carries a small JSON payload. Every other Flight method is
//! unimplemented — this service never serves `DoGet`/`DoPut` data.
//!
//! Writes gate on leadership: `create_table` returns
//! [`failed_precondition`](Status::failed_precondition) unless this node holds
//! the lease. Reads (`resolve`, `list_*`) are served regardless, matching the
//! HA model in `docs/architecture.md`.

use std::{pin::Pin, sync::Arc};

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightResult, SchemaResult,
    Ticket, flight_service_server::FlightService,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::Stream;
use lake_common::{Namespace, TableLocation, TableRef};
use serde::{Serialize, de::DeserializeOwned};
use tonic::{Request, Response, Status, Streaming};

use crate::{Metasrv, leadership::Leadership};

/// The [`Status`] message returned by every unsupported Flight method.
const UNSUPPORTED: &str = "metasrv control plane only serves do_action";

/// A boxed server stream of `T`, the shape every Flight response stream takes.
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// The `DoAction` response stream: a (usually one-shot) stream of Flight
/// results carrying JSON bodies.
type ActionStream = BoxStream<FlightResult>;

/// The metadata-layer control-plane Flight service.
///
/// Holds the registry [`Metasrv`] authority it dispatches to and the shared
/// [`Leadership`] flag it gates writes on.
pub(crate) struct MetasrvFlightService {
    /// The registry authority every action dispatches to.
    pub(crate) metasrv:    Arc<Metasrv>,
    /// The shared leader flag consulted before serving a write.
    pub(crate) leadership: Arc<Leadership>,
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
            let dt = match ty {
                "i64" => DataType::Int64,
                "f64" => DataType::Float64,
                "utf8" => DataType::Utf8,
                "bool" => DataType::Boolean,
                other => {
                    return Err(Status::invalid_argument(format!(
                        "unknown column type '{other}' (use i64|f64|utf8|bool)"
                    )));
                }
            };
            Ok(Field::new(name, dt, false))
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
    /// `create_table`: gate on leadership, then materialize and register the
    /// table described by the JSON body.
    async fn action_create_table(&self, body: &[u8]) -> Result<Response<ActionStream>, Status> {
        if !self.leadership.is_leader() {
            return Err(Status::failed_precondition("not the leader"));
        }
        let req: CreateTableReq = parse_body(body)?;
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

    /// `drop_table`: gate on leadership, then delete the table's data and
    /// deregister it. Idempotent.
    async fn action_drop_table(&self, body: &[u8]) -> Result<Response<ActionStream>, Status> {
        if !self.leadership.is_leader() {
            return Err(Status::failed_precondition("not the leader"));
        }
        let req: TableIdent = parse_body(body)?;
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
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(UNSUPPORTED))
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
        let body = action.body;
        match action.r#type.as_str() {
            "create_table" => self.action_create_table(&body).await,
            "drop_table" => self.action_drop_table(&body).await,
            "resolve" => self.action_resolve(&body).await,
            "list_tables" => self.action_list_tables(&body).await,
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
