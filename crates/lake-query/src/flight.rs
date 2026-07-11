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
//! `GetFlightInfo` plans the SQL to publish its Arrow schema and hands back a
//! ticket carrying the query text; `DoGet` decodes that ticket, executes the
//! SQL on the engine, and streams the resulting record batches back as Flight
//! data. Only the statement path is overridden — every other Flight SQL method
//! keeps its trait default (an `unimplemented` [`Status`]).

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_flight::{
    Action, ActionType, FlightClient, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, Result as FlightResult, Ticket,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::FlightService,
    sql::{
        Any, CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
        server::{FlightSqlService, PeekableFlightDataStream},
    },
};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use futures::{Stream, StreamExt, TryStreamExt};
use lake_common::{
    FILE_APPEND_TYPE_URL, FileAppendRequest, MANAGED_STAGE_DISCOVERY_ACTION, ManagedStageDescriptor,
};
use lake_flight::ClientSecurity;
use prost::Message;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, Sleep},
};
use tonic::{Request, Response, Status, Streaming};

use crate::{QueryEngine, QueryLimits};

#[derive(Clone, Debug)]
pub(crate) struct QueryAdmission {
    semaphore: Arc<Semaphore>,
    limits:    QueryLimits,
}

impl QueryAdmission {
    pub(crate) fn new(limits: QueryLimits) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(limits.max_concurrent())),
            limits,
        }
    }

    async fn acquire(&self) -> std::result::Result<OwnedSemaphorePermit, Status> {
        tokio::time::timeout(
            self.limits.queue_wait(),
            self.semaphore.clone().acquire_owned(),
        )
        .await
        .map_err(|_| Status::resource_exhausted("query concurrency limit reached"))?
        .map_err(|_| Status::unavailable("query admission is shutting down"))
    }

    fn validate_sql_size(&self, bytes: &[u8]) -> std::result::Result<(), Status> {
        if bytes.len() > self.limits.max_sql_bytes() {
            return Err(Status::resource_exhausted(
                "SQL or statement ticket exceeds the configured byte limit",
            ));
        }
        Ok(())
    }

    fn execution_deadline(&self) -> Instant { Instant::now() + self.limits.execution_time() }
}

struct AdmittedFlightStream {
    inner:    Option<<FlightSqlServiceImpl as FlightService>::DoGetStream>,
    deadline: Pin<Box<Sleep>>,
    permit:   Option<OwnedSemaphorePermit>,
}

impl AdmittedFlightStream {
    fn new(
        inner: <FlightSqlServiceImpl as FlightService>::DoGetStream,
        deadline: Instant,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            inner:    Some(inner),
            deadline: Box::pin(tokio::time::sleep_until(deadline)),
            permit:   Some(permit),
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
        if matches!(poll, Poll::Ready(None)) {
            self.inner.take();
            self.permit.take();
        }
        poll
    }
}

/// A Flight SQL service backed by a stateless [`QueryEngine`].
pub struct FlightSqlServiceImpl {
    /// The warmed query engine that plans and executes incoming SQL.
    pub engine:            Arc<QueryEngine>,
    /// Metadata Flight address used only for stateless FILE append forwarding.
    pub metadata_addr:     Option<String>,
    /// TLS and service credential for the Query-to-Metasrv hop.
    pub metadata_security: ClientSecurity,
    /// Immutable, credential-free stage metadata advertised to SDK clients.
    pub managed_stage:     Option<ManagedStageDescriptor>,
    /// Process-local admission shared by SQL statement RPCs.
    pub(crate) admission:  QueryAdmission,
}

impl FlightSqlServiceImpl {
    /// Ensure the bounded-staleness catalog and plan `sql`, returning only its
    /// Arrow schema.
    ///
    /// Used by `GetFlightInfo` to advertise the result schema without
    /// materializing any rows.
    async fn plan_schema(&self, sql: &str) -> std::result::Result<Schema, Status> {
        let df = self.engine.plan_sql(sql).await.map_err(to_status)?;
        Ok(df.schema().as_arrow().clone())
    }
}

/// Collapse any displayable error into an internal [`Status`].
fn to_status<E: std::fmt::Display>(err: E) -> Status { Status::internal(err.to_string()) }

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = Self;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<
        Response<
            Pin<Box<dyn Stream<Item = std::result::Result<HandshakeResponse, Status>> + Send>>,
        >,
        Status,
    > {
        let response = HandshakeResponse::default();
        let stream = futures::stream::once(async move { Ok(response) });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let CommandStatementQuery { query: sql, .. } = query;
        self.admission.validate_sql_size(sql.as_bytes())?;
        let _permit = self.admission.acquire().await?;
        let schema =
            tokio::time::timeout_at(self.admission.execution_deadline(), self.plan_schema(&sql))
                .await
                .map_err(|_| Status::deadline_exceeded("query planning deadline exceeded"))??;

        // The ticket carries the raw SQL so `DoGet` can re-plan and execute it.
        let ticket = TicketStatementQuery {
            statement_handle: sql.into_bytes().into(),
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

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.admission.validate_sql_size(&ticket.statement_handle)?;
        let permit = self.admission.acquire().await?;
        let deadline = self.admission.execution_deadline();
        let sql = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("ticket is not utf-8: {e}")))?;

        let batches = tokio::time::timeout_at(deadline, async {
            let df = self.engine.plan_sql(&sql).await.map_err(to_status)?;
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
        Ok(Response::new(Box::pin(AdmittedFlightStream::new(
            Box::pin(stream),
            deadline,
            permit,
        ))))
    }

    async fn do_put_fallback(
        &self,
        request: Request<PeekableFlightDataStream>,
        message: Any,
    ) -> std::result::Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        if message.type_url != FILE_APPEND_TYPE_URL {
            return Err(Status::invalid_argument("invalid FILE append command"));
        }
        let append = FileAppendRequest::from_command_payload(&message.value)
            .ok_or_else(|| Status::invalid_argument("invalid FILE append command"))?;
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
        let results = client
            .do_put(request.into_inner().map(|item| {
                item.map_err(|error| arrow_flight::error::FlightError::protocol(error.to_string()))
            }))
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?
            .map_err(|error| Status::internal(error.to_string()))
            .try_collect::<Vec<_>>()
            .await?;
        self.engine.invalidate_registration(append.table()).await;
        Ok(Response::new(Box::pin(futures::stream::iter(
            results.into_iter().map(Ok),
        ))))
    }

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoActionStream>, Status> {
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
        let descriptor = self
            .managed_stage
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("managed FILE stage is not configured"))?;
        let body = descriptor
            .to_wire()
            .map_err(|error| Status::internal(error.to_string()))?;
        let stream = futures::stream::once(async move { Ok(FlightResult { body: body.into() }) });
        Ok(Response::new(Box::pin(stream)))
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
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use async_trait::async_trait;
    use datafusion::{
        arrow::{
            array::Int64Array,
            datatypes::{DataType, Field},
            record_batch::RecordBatch,
        },
        catalog::streaming::StreamingTable,
        error::DataFusionError,
        execution::TaskContext,
        physical_plan::{
            SendableRecordBatchStream, stream::RecordBatchStreamAdapter, streaming::PartitionStream,
        },
    };
    use futures::{StreamExt, TryStreamExt};
    use lake_common::{MANAGED_STAGE_DISCOVERY_ACTION, ManagedStageDescriptor};
    use lake_engine::TableEngineRef;
    use lake_engine_lance::LanceEngine;
    use lake_meta::{MetaStore, MetaStoreRef};
    use tokio::sync::Notify;

    use super::*;

    struct EmptyMeta;

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
        };
        let request = Request::new(arrow_flight::Action {
            r#type: MANAGED_STAGE_DISCOVERY_ACTION.to_owned(),
            body:   Vec::new().into(),
        });

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
            descriptor
        );
    }

    #[tokio::test]
    async fn do_get_returns_before_the_input_stream_finishes() {
        let meta: MetaStoreRef = Arc::new(EmptyMeta);
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let engine = Arc::new(QueryEngine::new(meta, storage));
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
        engine
            .context()
            .register_table("delayed", Arc::new(table))
            .unwrap();

        let service = FlightSqlServiceImpl {
            engine,
            metadata_addr: None,
            metadata_security: ClientSecurity::new(),
            managed_stage: None,
            admission: QueryAdmission::new(QueryLimits::default()),
        };
        let ticket = TicketStatementQuery {
            statement_handle: b"SELECT * FROM delayed".to_vec().into(),
        };
        let mut request = tokio::spawn(async move {
            service
                .do_get_statement(ticket, Request::new(Ticket::default()))
                .await
        });

        let returned_early = tokio::time::timeout(Duration::from_millis(100), &mut request)
            .await
            .is_ok();
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
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let engine = Arc::new(QueryEngine::new(meta, storage));
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
        engine
            .context()
            .register_table("admitted", Arc::new(table))
            .expect("register table");
        let limits =
            QueryLimits::try_new(1, Duration::from_millis(20), Duration::from_secs(5), 1024)
                .expect("limits");
        let service = FlightSqlServiceImpl {
            engine,
            metadata_addr: None,
            metadata_security: ClientSecurity::new(),
            managed_stage: None,
            admission: QueryAdmission::new(limits),
        };
        let ticket = || TicketStatementQuery {
            statement_handle: b"SELECT * FROM admitted".to_vec().into(),
        };

        let first = service
            .do_get_statement(ticket(), Request::new(Ticket::default()))
            .await
            .expect("first query admitted");
        let Err(saturated) = service
            .do_get_statement(ticket(), Request::new(Ticket::default()))
            .await
        else {
            panic!("second query must be rejected while first stream lives");
        };
        assert_eq!(saturated.code(), tonic::Code::ResourceExhausted);

        drop(first);
        let third = service
            .do_get_statement(ticket(), Request::new(Ticket::default()))
            .await;
        assert!(third.is_ok(), "dropping the stream must release its permit");
        release.notify_waiters();
    }

    #[tokio::test]
    async fn query_execution_deadline_terminates_slow_stream() {
        let meta: MetaStoreRef = Arc::new(EmptyMeta);
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let engine = Arc::new(QueryEngine::new(meta, storage));
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
        engine
            .context()
            .register_table("deadline", Arc::new(table))
            .expect("register table");
        let limits = QueryLimits::try_new(
            1,
            Duration::from_millis(20),
            Duration::from_millis(50),
            1024,
        )
        .expect("limits");
        let service = FlightSqlServiceImpl {
            engine,
            metadata_addr: None,
            metadata_security: ClientSecurity::new(),
            managed_stage: None,
            admission: QueryAdmission::new(limits),
        };
        let ticket = || TicketStatementQuery {
            statement_handle: b"SELECT * FROM deadline".to_vec().into(),
        };
        let response = service
            .do_get_statement(ticket(), Request::new(Ticket::default()))
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

        let next = service
            .do_get_statement(ticket(), Request::new(Ticket::default()))
            .await;
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
            statement_handle: b"SELECT 1".to_vec().into(),
        };
        let Err(ticket_error) = service
            .do_get_statement(ticket, Request::new(Ticket::default()))
            .await
        else {
            panic!("oversized ticket must fail");
        };
        assert_eq!(ticket_error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(meta.scans.load(Ordering::Relaxed), 0);
    }
}
