// Copyright 2026 Rararulab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Black-box protocol checks against Apache Arrow's official ADBC driver.

use std::{
    io,
    path::PathBuf,
    process::{Command, Output, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use datafusion::{
    arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    },
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use lake_common::{
    AppendOperation, AppendOperationId, AppendPayloadDigest, TableLocation, TableRef, TenantId,
};
use lake_engine::TableEngineRef;
use lake_engine_lance::LanceEngine;
use lake_flight::ServerSecurity;
use lake_meta::{MetaStoreRef, RocksMeta, registry};
use lake_query::{QueryEngine, QueryServerConfig, serve_with_config_and_shutdown};
use tempfile::TempDir;
use tokio::{net::TcpStream, task::JoinHandle};
use tokio_util::sync::CancellationToken;

const ADBC_TIMEOUT: Duration = Duration::from_secs(45);

struct QueryFixture {
    _data:    TempDir,
    addr:     std::net::SocketAddr,
    shutdown: CancellationToken,
    server:   JoinHandle<lake_query::Result<()>>,
}

impl QueryFixture {
    async fn start(security: ServerSecurity) -> Self {
        let data = tempfile::tempdir().expect("temporary Query state");
        let meta: MetaStoreRef = Arc::new(
            RocksMeta::open(data.path().join("meta")).expect("open temporary Query metastore"),
        );
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        install_rows(&meta, &storage, data.path()).await;
        let engine = Arc::new(QueryEngine::new(meta, storage));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
        let addr = listener.local_addr().expect("read loopback address");
        drop(listener);
        let shutdown = CancellationToken::new();
        let stopped = shutdown.clone();
        let server = tokio::spawn(async move {
            serve_with_config_and_shutdown(
                engine,
                &addr.to_string(),
                QueryServerConfig::new().with_server_security(security),
                stopped.cancelled_owned(),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if TcpStream::connect(addr).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Query listener starts");
        Self {
            _data: data,
            addr,
            shutdown,
            server,
        }
    }

    fn uri(&self) -> String { format!("grpc://{}", self.addr) }

    async fn stop(self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), self.server)
            .await
            .expect("Query shutdown is bounded")
            .expect("Query task joins")
            .expect("Query shuts down cleanly");
    }
}

async fn install_rows(meta: &MetaStoreRef, storage: &TableEngineRef, root: &std::path::Path) {
    let table = TableRef::new("interop", "rows");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let location = TableLocation::new(root.join("rows.lance").display().to_string());
    lake_catalog::create_table(meta, storage, &table, location.clone(), schema.clone())
        .await
        .expect("create interop table");
    let handle = storage
        .open(&location)
        .await
        .expect("open interop table")
        .expect("interop table exists");
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(1..=20_000))],
    )
    .expect("interop batch");
    let batches = futures::stream::once(async move { Ok::<_, DataFusionError>(batch) });
    let stream: SendableRecordBatchStream =
        Box::pin(RecordBatchStreamAdapter::new(schema, batches));
    let operation = AppendOperation::builder()
        .tenant(TenantId::try_new("interop").expect("valid interop tenant"))
        .operation_id(AppendOperationId::generate())
        .payload_digest(
            AppendPayloadDigest::parse(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .expect("valid fixed digest"),
        )
        .build();
    let version = handle
        .append(&operation, stream)
        .await
        .expect("append interop rows");
    let registration = registry::get(meta.as_ref(), &table)
        .await
        .expect("read registration")
        .expect("registration exists");
    registry::set_version(meta.as_ref(), &table, &registration, version)
        .await
        .expect("publish interop version");
}

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("lake-query lives under crates/")
        .to_owned()
}

async fn run_adbc(uri: String, mode: &'static str, token: Option<&'static str>) {
    let root = repository_root();
    let output = tokio::task::spawn_blocking(move || {
        let mut command = Command::new("uv");
        command.current_dir(&root).args([
            "run",
            "--project",
            "interop/adbc",
            "--frozen",
            "python",
            "interop/adbc/check.py",
            "--uri",
            &uri,
            "--mode",
            mode,
        ]);
        if let Some(token) = token {
            command.args(["--token", token]);
        }
        command_with_timeout(&mut command, ADBC_TIMEOUT)
    })
    .await
    .expect("ADBC subprocess task")
    .expect("execute uv ADBC runner");
    assert!(
        output.status.success(),
        "ADBC {mode} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn command_with_timeout(command: &mut Command, timeout: Duration) -> io::Result<Output> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let deadline = Instant::now() + timeout;
    loop {
        if child.try_wait()?.is_some() {
            return child.wait_with_output();
        }
        if Instant::now() >= deadline {
            child.kill()?;
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "ADBC interoperability process exceeded its deadline",
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run with mise run test-adbc"]
async fn upstream_adbc_reads_typed_multibatch_result() {
    let fixture = QueryFixture::start(ServerSecurity::insecure()).await;
    run_adbc(fixture.uri(), "query", None).await;
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run with mise run test-adbc"]
async fn upstream_adbc_observes_stable_write_rejection() {
    let fixture = QueryFixture::start(ServerSecurity::insecure()).await;
    run_adbc(fixture.uri(), "reject-write", None).await;
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run with mise run test-adbc"]
async fn upstream_adbc_bearer_authentication_fails_closed() {
    const TOKEN: &str = "lake-adbc-conformance-token";
    let security = ServerSecurity::with_bearer_token(TOKEN).expect("test bearer");
    let fixture = QueryFixture::start(security).await;
    run_adbc(fixture.uri(), "query", Some(TOKEN)).await;
    run_adbc(
        fixture.uri(),
        "expect-auth-failure",
        Some("incorrect-token"),
    )
    .await;
    run_adbc(fixture.uri(), "expect-auth-failure", None).await;
    fixture.stop().await;
}

#[test]
fn adbc_interop_harness_is_pinned_and_bounded() {
    let root = repository_root();
    let project = std::fs::read_to_string(root.join("interop/adbc/pyproject.toml")).unwrap();
    assert!(project.contains("adbc-driver-flightsql==1.11.0"));
    assert!(project.contains("pyarrow==24.0.0"));
    assert!(root.join("interop/adbc/uv.lock").is_file());
    assert!(ADBC_TIMEOUT <= Duration::from_mins(1));
}
