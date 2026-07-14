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

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use arrow_flight::{
    Empty, FlightClient, FlightDescriptor, encode::FlightDataEncoderBuilder, error::FlightError,
    flight_service_client::FlightServiceClient, sql::client::FlightSqlServiceClient,
};
use datafusion::arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    ipc::{CompressionType, writer::IpcWriteOptions},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use lake_common::{
    AppendOperationId, FILE_APPEND_TYPE_URL, FileAppendRequest, Principal, PrincipalId,
    PrincipalRole, TableLocation, TableRef, TenantId, Version,
};
use lake_engine::TableEngineRef;
use lake_engine_lance::LanceEngine;
use lake_flight::{
    BearerPrincipalBinding, ClientSecurity, ServerSecurity, append_flight_payload_digest,
};
use lake_meta::{MetaStoreRef, RocksMeta};
use lake_metasrv::{Metasrv, MetasrvServerConfig};
use lake_query::{QueryEngine, QueryServerConfig};
use opentelemetry::{Value, trace::TracerProvider as _};
use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{SdkTracerProvider, SpanData, SpanExporter},
};
use prost::Message;
use prost_types::Any;
use tonic::{Code, Request, transport::Channel};
use tracing::Instrument as _;
use tracing_subscriber::layer::SubscriberExt as _;

#[derive(Clone, Debug, Default)]
struct RecordingExporter(Arc<Mutex<Vec<SpanData>>>);

impl SpanExporter for RecordingExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.0.lock().expect("span recorder lock").extend(batch);
        Ok(())
    }
}

fn span_attribute<'a>(span: &'a SpanData, key: &str) -> Option<&'a str> {
    span.attributes.iter().find_map(|attribute| {
        if attribute.key.as_str() != key {
            return None;
        }
        match &attribute.value {
            Value::String(value) => Some(value.as_str()),
            _ => None,
        }
    })
}

fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("read local addr").to_string()
}

#[tokio::test]
async fn query_trace_context_reaches_metasrv_without_data_attributes() {
    let exporter = RecordingExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("lake-query-test"))
            .with_location(false)
            .with_threads(false)
            .with_target(false)
            .with_tracked_inactivity(false),
    );
    tracing::subscriber::set_global_default(subscriber).expect("install tracing subscriber");

    let root = tempfile::tempdir().unwrap();
    let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
    let engine: TableEngineRef = Arc::new(LanceEngine::new());
    let metasrv = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
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
    let meta_addr = free_addr();
    let query_addr = free_addr();
    let query_service = Principal::try_new(
        PrincipalId::try_new("query-service").unwrap(),
        TenantId::try_new("service").unwrap(),
        PrincipalRole::QueryService,
        std::iter::empty::<&str>(),
    )
    .unwrap();
    let meta_security = ServerSecurity::with_bearer_principals([BearerPrincipalBinding::new(
        "query-token",
        query_service,
    )
    .unwrap()])
    .unwrap();
    let metadata_client = ClientSecurity::new()
        .with_bearer_token("query-token")
        .unwrap();
    tokio::spawn({
        let metasrv = metasrv.clone();
        let addr = meta_addr.clone();
        let config = MetasrvServerConfig::new().with_server_security(meta_security);
        async move { lake_metasrv::serve_with_config(metasrv, &addr, config).await }
    });
    let alpha = Principal::try_new(
        PrincipalId::try_new("alpha-user").unwrap(),
        TenantId::try_new("tenant-a").unwrap(),
        PrincipalRole::User,
        ["robots"],
    )
    .unwrap();
    let beta = Principal::try_new(
        PrincipalId::try_new("beta-user").unwrap(),
        TenantId::try_new("tenant-b").unwrap(),
        PrincipalRole::User,
        ["robots"],
    )
    .unwrap();
    let query_security = ServerSecurity::with_bearer_principals([
        BearerPrincipalBinding::new("alpha-token", alpha).unwrap(),
        BearerPrincipalBinding::new("beta-token", beta).unwrap(),
    ])
    .unwrap();
    tokio::spawn({
        let query = Arc::new(QueryEngine::new(meta.clone(), engine));
        let addr = query_addr.clone();
        let metadata = format!("http://{meta_addr}");
        let config = QueryServerConfig::new()
            .with_metadata(metadata, metadata_client)
            .with_server_security(query_security);
        async move { lake_query::serve_with_config(query, &addr, config).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["episode-42"]))],
    )
    .unwrap();
    let mut messages = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(futures::stream::iter(vec![Ok(batch)]))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let append = FileAppendRequest::new(
        table.clone(),
        AppendOperationId::generate(),
        append_flight_payload_digest(&messages),
    );
    messages[0].flight_descriptor = Some(FlightDescriptor::new_cmd(
        Any {
            type_url: FILE_APPEND_TYPE_URL.to_owned(),
            value:    append.command_payload(),
        }
        .encode_to_vec(),
    ));
    let channel = Channel::from_shared(format!("http://{query_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let send = |token: &'static str, messages: Vec<arrow_flight::FlightData>| {
        let channel = channel.clone();
        async move {
            let security = ClientSecurity::new().with_bearer_token(token).unwrap();
            let mut client = FlightClient::new(channel);
            security.apply_to_flight_client(&mut client).unwrap();
            let results = client
                .do_put(futures::stream::iter(messages.into_iter().map(Ok)))
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            serde_json::from_slice::<Version>(&results[0].app_metadata).unwrap()
        }
    };

    let client_span = tracing::info_span!("test.client");
    let alpha_version = send("alpha-token", messages.clone())
        .instrument(client_span.clone())
        .await;
    let beta_version = send("beta-token", messages.clone()).await;
    let alpha_replay = send("alpha-token", messages).await;

    let mut sql_client = FlightSqlServiceClient::new(channel.clone());
    let sql_span = tracing::info_span!("test.client.sql");
    async {
        ClientSecurity::new()
            .with_bearer_token("alpha-token")
            .unwrap()
            .apply_to_sql_client(&mut sql_client);
        let info = sql_client
            .execute(
                "SELECT episode_id FROM lake.robots.episodes".to_owned(),
                None,
            )
            .await
            .expect("plan SQL");
        let ticket = info.endpoint[0].ticket.clone().expect("query ticket");
        sql_client
            .do_get(ticket)
            .await
            .expect("execute SQL")
            .try_collect::<Vec<_>>()
            .await
            .expect("collect SQL response");
    }
    .instrument(sql_span.clone())
    .await;

    let actions_span = tracing::info_span!("test.client.actions");
    async {
        let security = ClientSecurity::new()
            .with_bearer_token("alpha-token")
            .unwrap();
        let mut client = FlightServiceClient::new(channel.clone());
        client
            .list_actions(security.authorize_request(Request::new(Empty {})))
            .await
            .expect("list actions")
            .into_inner()
            .try_collect::<Vec<_>>()
            .await
            .expect("collect actions");
    }
    .instrument(actions_span.clone())
    .await;

    let anonymous_actions_span = tracing::info_span!("test.client.actions.anonymous");
    async {
        let mut client = FlightServiceClient::new(channel.clone());
        let error = client
            .list_actions(ClientSecurity::new().authorize_request(Request::new(Empty {})))
            .await
            .expect_err("anonymous ListActions rejected");
        assert_eq!(error.code(), Code::Unauthenticated);
    }
    .instrument(anonymous_actions_span.clone())
    .await;

    assert_eq!(alpha_version, Version(2));
    assert_eq!(beta_version, Version(3));
    assert_eq!(alpha_replay, alpha_version);
    assert_eq!(
        metasrv
            .resolve(&table)
            .await
            .unwrap()
            .unwrap()
            .current_version,
        beta_version
    );

    drop(client_span);
    drop(sql_span);
    drop(actions_span);
    drop(anonymous_actions_span);
    provider.force_flush().expect("flush test spans");
    let spans = exporter.0.lock().expect("span recorder lock");
    let client_trace = spans
        .iter()
        .find(|span| span.name == "test.client")
        .expect("client span")
        .span_context
        .trace_id();
    let query_span = spans
        .iter()
        .find(|span| {
            span.span_context.trace_id() == client_trace
                && span_attribute(span, "rpc.service") == Some("lake.query")
        })
        .expect("Query server span");
    let metasrv_span = spans
        .iter()
        .find(|span| {
            span.span_context.trace_id() == client_trace
                && span_attribute(span, "rpc.service") == Some("lake.metasrv")
        })
        .expect("Metasrv server span");
    assert_eq!(query_span.span_context.trace_id(), client_trace);
    assert_eq!(metasrv_span.span_context.trace_id(), client_trace);
    let sql_trace = spans
        .iter()
        .find(|span| span.name == "test.client.sql")
        .expect("SQL client span")
        .span_context
        .trace_id();
    let sql_methods = spans
        .iter()
        .filter(|span| span.span_context.trace_id() == sql_trace)
        .filter_map(|span| span_attribute(span, "rpc.method"))
        .collect::<Vec<_>>();
    assert_eq!(sql_methods, ["get_flight_info", "do_get"]);
    let actions_trace = spans
        .iter()
        .find(|span| span.name == "test.client.actions")
        .expect("ListActions client span")
        .span_context
        .trace_id();
    assert!(spans.iter().any(|span| {
        span.span_context.trace_id() == actions_trace
            && span_attribute(span, "rpc.method") == Some("list_actions")
    }));
    let anonymous_actions_trace = spans
        .iter()
        .find(|span| span.name == "test.client.actions.anonymous")
        .expect("anonymous ListActions client span")
        .span_context
        .trace_id();
    assert!(!spans.iter().any(|span| {
        span.span_context.trace_id() == anonymous_actions_trace
            && span_attribute(span, "rpc.method") == Some("list_actions")
    }));
    for span in spans.iter().filter(|span| span.name == "flight.server") {
        let keys = span
            .attributes
            .iter()
            .map(|attribute| attribute.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            ["rpc.system", "rpc.service", "rpc.method", "rpc.outcome"]
        );
    }
}

#[tokio::test]
async fn query_forwarded_file_append_rejects_compressed_ipc() {
    let root = tempfile::tempdir().unwrap();
    let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
    let engine: TableEngineRef = Arc::new(LanceEngine::new());
    let metasrv = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
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
    let initial_version = metasrv
        .resolve(&table)
        .await
        .unwrap()
        .unwrap()
        .current_version;
    let meta_addr = free_addr();
    let query_addr = free_addr();
    let query_service = Principal::try_new(
        PrincipalId::try_new("query-service").unwrap(),
        TenantId::try_new("service").unwrap(),
        PrincipalRole::QueryService,
        std::iter::empty::<&str>(),
    )
    .unwrap();
    let meta_security = ServerSecurity::with_bearer_principals([BearerPrincipalBinding::new(
        "query-token",
        query_service,
    )
    .unwrap()])
    .unwrap();
    let metadata_client = ClientSecurity::new()
        .with_bearer_token("query-token")
        .unwrap();
    tokio::spawn({
        let metasrv = metasrv.clone();
        let addr = meta_addr.clone();
        let config = MetasrvServerConfig::new().with_server_security(meta_security);
        async move { lake_metasrv::serve_with_config(metasrv, &addr, config).await }
    });
    let user = Principal::try_new(
        PrincipalId::try_new("alpha-user").unwrap(),
        TenantId::try_new("tenant-a").unwrap(),
        PrincipalRole::User,
        ["robots"],
    )
    .unwrap();
    let query_security =
        ServerSecurity::with_bearer_principals([
            BearerPrincipalBinding::new("alpha-token", user).unwrap()
        ])
        .unwrap();
    tokio::spawn({
        let query = Arc::new(QueryEngine::new(meta, engine));
        let addr = query_addr.clone();
        let metadata = format!("http://{meta_addr}");
        let config = QueryServerConfig::new()
            .with_metadata(metadata, metadata_client)
            .with_server_security(query_security);
        async move { lake_query::serve_with_config(query, &addr, config).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            "episode-42".repeat(1_024),
        ]))],
    )
    .unwrap();
    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))
        .unwrap();
    let mut messages = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_options(options)
        .build(futures::stream::iter(vec![Ok(batch)]))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let append = FileAppendRequest::new(
        table.clone(),
        AppendOperationId::generate(),
        append_flight_payload_digest(&messages),
    );
    messages[0].flight_descriptor = Some(FlightDescriptor::new_cmd(
        Any {
            type_url: FILE_APPEND_TYPE_URL.to_owned(),
            value:    append.command_payload(),
        }
        .encode_to_vec(),
    ));
    let channel = Channel::from_shared(format!("http://{query_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightClient::new(channel);
    ClientSecurity::new()
        .with_bearer_token("alpha-token")
        .unwrap()
        .apply_to_flight_client(&mut client)
        .unwrap();

    let error = match client
        .do_put(futures::stream::iter(messages.into_iter().map(Ok)))
        .await
    {
        Err(FlightError::Tonic(status)) => status,
        Err(error) => panic!("Query must return a tonic status: {error}"),
        Ok(_) => panic!("Query must relay Metasrv's compressed-IPC rejection"),
    };

    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(
        metasrv
            .resolve(&table)
            .await
            .unwrap()
            .unwrap()
            .current_version,
        initial_version
    );
}
