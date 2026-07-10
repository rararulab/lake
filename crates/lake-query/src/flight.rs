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

use std::{pin::Pin, sync::Arc};

use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::FlightService,
    sql::{
        CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
        server::FlightSqlService,
    },
};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use futures::{Stream, TryStreamExt};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

use crate::QueryEngine;

/// A Flight SQL service backed by a stateless [`QueryEngine`].
pub struct FlightSqlServiceImpl {
    /// The warmed query engine that plans and executes incoming SQL.
    pub engine: Arc<QueryEngine>,
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
        let schema = self.plan_schema(&sql).await?;

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
        let sql = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("ticket is not utf-8: {e}")))?;

        let df = self.engine.plan_sql(&sql).await.map_err(to_status)?;
        let batches = df.execute_stream().await.map_err(to_status)?;
        let schema: SchemaRef = batches.schema();
        let batches = batches.map_err(|err| FlightError::ExternalError(Box::new(err)));

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batches)
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
    use futures::StreamExt;
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

        let service = FlightSqlServiceImpl { engine };
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
}
