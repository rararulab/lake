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

//! Live two-node test that the write path forwards a follower's write to the
//! current leader.
//!
//! Two `serve` instances share one [`RocksMeta`] registry and one local-FS
//! [`LanceEngine`], so they campaign for the same lease and see the same
//! tables. Once a leader is elected, we send a `create_table` action to the
//! *follower's* Flight address and assert it succeeds — which can only happen
//! if the follower forwarded the write to the leader (a write served locally on
//! a follower would fail `unavailable`). We then confirm the table is really
//! registered, and that remote destructive drop is forwarded through the
//! durable tombstone protocol.
//!
//! Hermetic: no external services, only two loopback ports and two tempdirs.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arrow_flight::{
    Action, FlightDescriptor, encode::FlightDataEncoderBuilder,
    flight_service_client::FlightServiceClient,
};
use datafusion::arrow::{
    array::{Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use lake_common::{AppendOperationId, FILE_APPEND_TYPE_URL, FileAppendRequest, TableRef, Version};
use lake_engine::TableEngineRef;
use lake_engine_lance::LanceEngine;
use lake_flight::{ClientSecurity, ServerSecurity, append_flight_payload_digest};
use lake_meta::{MetaStoreRef, RocksMeta, registry};
use lake_metasrv::{
    Metasrv, MetasrvServerConfig, TablePlacement,
    election::{LEASE_KEY, LeaseValue},
    serve_with_config, serve_with_config_and_shutdown,
};
use prost::Message;
use prost_types::Any;
use rcgen::generate_simple_self_signed;
use serde_json::json;
use tokio::sync::oneshot;
use tonic::{Code, Request, Status, transport::Channel};

/// Grab a currently-free loopback address by binding an ephemeral port and
/// immediately releasing it; `serve` re-binds it a moment later.
fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("read local addr").to_string()
}

/// Current wall clock in milliseconds since the Unix epoch.
fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis(),
    )
    .expect("millis fit u64")
}

/// Open a fresh Flight client to `addr`.
async fn client(addr: &str) -> Result<FlightServiceClient<Channel>, Status> {
    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("valid uri")
        .connect()
        .await
        .map_err(|e| Status::unavailable(e.to_string()))?;
    Ok(FlightServiceClient::new(channel))
}

/// Issue one `do_action` against `addr` and drain its result stream.
async fn do_action(addr: &str, r#type: &str, body: serde_json::Value) -> Result<(), Status> {
    let mut client = client(addr).await?;
    let action = Action {
        r#type: r#type.to_owned(),
        body:   serde_json::to_vec(&body).expect("encode body").into(),
    };
    let response = client.do_action(Request::new(action)).await?;
    response.into_inner().try_collect::<Vec<_>>().await?;
    Ok(())
}

async fn file_append_messages(
    table: TableRef,
    operation_id: AppendOperationId,
) -> Vec<arrow_flight::FlightData> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("reward", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(Float64Array::from(vec![0.8])),
        ],
    )
    .expect("build append batch");
    let mut messages = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(futures::stream::iter(vec![Ok(batch)]))
        .try_collect::<Vec<_>>()
        .await
        .expect("encode FlightData");
    let append =
        FileAppendRequest::new(table, operation_id, append_flight_payload_digest(&messages));
    messages[0].flight_descriptor = Some(FlightDescriptor::new_cmd(
        Any {
            type_url: FILE_APPEND_TYPE_URL.to_owned(),
            value:    append.command_payload(),
        }
        .encode_to_vec(),
    ));
    messages
}

async fn do_file_append_messages(
    addr: &str,
    messages: Vec<arrow_flight::FlightData>,
) -> Result<Version, Status> {
    let mut client = client(addr).await?;
    let stream = futures::stream::iter(messages);
    let response = client.do_put(Request::new(stream)).await?;
    let results = response.into_inner().try_collect::<Vec<_>>().await?;
    let result = results
        .first()
        .ok_or_else(|| Status::internal("append returned no result"))?;
    serde_json::from_slice(&result.app_metadata)
        .map_err(|error| Status::internal(error.to_string()))
}

async fn do_file_append(addr: &str, table: TableRef) -> Result<(), Status> {
    let messages = file_append_messages(table, AppendOperationId::generate()).await;
    do_file_append_messages(addr, messages).await.map(|_| ())
}

/// Block until `addr` answers a read (`list_namespaces` is served locally on
/// any node regardless of leadership), i.e. the server is accepting requests.
async fn wait_serving(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if do_action(addr, "list_namespaces", json!(null))
            .await
            .is_ok()
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "server {addr} never started serving within 10s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll the shared lease until a valid leader is elected, returning its
/// address. Panics if leadership cannot be determined within the timeout —
/// that would itself be a forwarding blocker.
async fn wait_for_leader(meta: &MetaStoreRef, candidates: &[&str]) -> String {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(bytes) = meta.get(LEASE_KEY).await.expect("read lease") {
            let lease: LeaseValue = serde_json::from_slice(&bytes).expect("decode lease");
            if lease.expires_at_ms > now_ms() && candidates.contains(&lease.holder.as_str()) {
                return lease.holder;
            }
        }
        assert!(
            Instant::now() < deadline,
            "no leader elected within 15s (candidates: {candidates:?})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Run `action` against `addr`, retrying while the node reports `unavailable`
/// (the follower may not have observed leadership on its first campaign round
/// yet). Any other status is returned immediately — that is a genuine
/// forwarding failure, not a settling delay.
async fn forward_with_retry(
    addr: &str,
    r#type: &str,
    body: serde_json::Value,
) -> Result<(), Status> {
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        match do_action(addr, r#type, body.clone()).await {
            Ok(()) => return Ok(()),
            Err(status) if status.code() == Code::Unavailable && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            Err(status) => return Err(status),
        }
    }
}

#[tokio::test]
async fn follower_forwards_write_to_leader() {
    // One registry + one local-FS engine, shared by both nodes: they campaign
    // for the same lease and resolve the same tables.
    let meta_dir = tempfile::tempdir().expect("meta tempdir");
    let table_dir = tempfile::tempdir().expect("table tempdir");
    let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).expect("open RocksMeta"));
    let engine: TableEngineRef = Arc::new(LanceEngine::with_manifest_store(meta.clone()));

    let addr_a = free_addr();
    let addr_b = free_addr();
    assert_ne!(addr_a, addr_b, "picked two distinct ports");

    let node_a = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
    let node_b = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
    let placement = TablePlacement::local(table_dir.path().to_path_buf());

    tokio::spawn({
        let addr = addr_a.clone();
        let config = MetasrvServerConfig::new().with_table_placement(placement.clone());
        async move { serve_with_config(node_a, &addr, config).await }
    });
    tokio::spawn({
        let addr = addr_b.clone();
        let config = MetasrvServerConfig::new().with_table_placement(placement);
        async move { serve_with_config(node_b, &addr, config).await }
    });

    wait_serving(&addr_a).await;
    wait_serving(&addr_b).await;

    // Determine leader vs follower from the shared lease record.
    let leader = wait_for_leader(&meta, &[&addr_a, &addr_b]).await;
    let follower = if leader == addr_a {
        addr_b.clone()
    } else {
        addr_a.clone()
    };

    // THE ASSERTION: a write sent to the FOLLOWER must succeed, which is only
    // possible if the follower forwards it to the leader.
    let create_body = json!({
        "namespace": "robots",
        "name": "arm",
        "columns": ["ts:i64", "reward:f64"],
    });
    let created = forward_with_retry(&follower, "create_table", create_body).await;
    assert!(
        created.is_ok(),
        "follower {follower} (leader {leader}) did not forward create_table to the leader: \
         {created:?}"
    );

    // The forwarded write really landed: the shared registry now has the table.
    let table = TableRef::new("robots", "arm");
    let reg = registry::get(meta.as_ref(), &table)
        .await
        .expect("registry get");
    assert!(
        reg.is_some(),
        "robots.arm is not registered after a forwarded create_table"
    );

    let before_append = reg.expect("registered table").current_version;
    let appended = do_file_append(&follower, table.clone()).await;
    assert!(
        appended.is_ok(),
        "follower {follower} did not forward FILE append to leader {leader}: {appended:?}"
    );
    let after_append = registry::get(meta.as_ref(), &table)
        .await
        .expect("registry get after append")
        .expect("table remains registered");
    assert!(after_append.current_version > before_append);

    // Destructive drop follows the same follower-to-leader path and converges
    // only after the durable tombstone cleanup finishes.
    let drop_body = json!({ "namespace": "robots", "name": "arm" });
    let dropped = forward_with_retry(&follower, "drop_table", drop_body).await;
    dropped.expect("remote drop through follower");
    let reg_after = registry::get(meta.as_ref(), &table)
        .await
        .expect("registry get after drop");
    assert!(reg_after.is_none(), "durable drop must detach robots.arm");
}

#[tokio::test]
async fn remote_drop_is_idempotent_across_leader_handoff() {
    let meta_dir = tempfile::tempdir().expect("meta tempdir");
    let table_dir = tempfile::tempdir().expect("table tempdir");
    let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).expect("open RocksMeta"));
    let engine: TableEngineRef = Arc::new(LanceEngine::with_manifest_store(meta.clone()));
    let addr_a = free_addr();
    let addr_b = free_addr();
    let (shutdown_a_tx, shutdown_a_rx) = oneshot::channel();
    let (shutdown_b_tx, shutdown_b_rx) = oneshot::channel();
    let mut shutdown_a_tx = Some(shutdown_a_tx);
    let mut shutdown_b_tx = Some(shutdown_b_tx);
    let placement = TablePlacement::local(table_dir.path().to_path_buf());

    let server_a = tokio::spawn({
        let node = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
        let addr = addr_a.clone();
        let placement = placement.clone();
        async move {
            serve_with_config_and_shutdown(
                node,
                &addr,
                MetasrvServerConfig::new()
                    .with_table_placement(placement)
                    .with_shutdown_grace(Duration::from_secs(1)),
                async move {
                    let _ = shutdown_a_rx.await;
                },
            )
            .await
        }
    });
    let server_b = tokio::spawn({
        let node = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
        let addr = addr_b.clone();
        async move {
            serve_with_config_and_shutdown(
                node,
                &addr,
                MetasrvServerConfig::new()
                    .with_table_placement(placement)
                    .with_shutdown_grace(Duration::from_secs(1)),
                async move {
                    let _ = shutdown_b_rx.await;
                },
            )
            .await
        }
    });
    let mut server_a = Some(server_a);
    let mut server_b = Some(server_b);

    wait_serving(&addr_a).await;
    wait_serving(&addr_b).await;
    let leader = wait_for_leader(&meta, &[&addr_a, &addr_b]).await;
    let standby = if leader == addr_a {
        addr_b.clone()
    } else {
        addr_a.clone()
    };
    let table = TableRef::new("robots", "drop_handoff");
    forward_with_retry(
        &leader,
        "create_table",
        json!({
            "namespace": "robots",
            "name": "drop_handoff",
            "columns": ["ts:i64"],
        }),
    )
    .await
    .expect("leader creates table");
    let location = registry::get(meta.as_ref(), &table)
        .await
        .expect("registry get")
        .expect("created registration")
        .location;
    forward_with_retry(
        &leader,
        "drop_table",
        json!({ "namespace": "robots", "name": "drop_handoff" }),
    )
    .await
    .expect("first remote drop");

    if leader == addr_a {
        shutdown_a_tx.take().unwrap().send(()).unwrap();
        server_a.take().unwrap().await.unwrap().unwrap();
    } else {
        shutdown_b_tx.take().unwrap().send(()).unwrap();
        server_b.take().unwrap().await.unwrap().unwrap();
    }
    assert_eq!(wait_for_leader(&meta, &[&standby]).await, standby);
    forward_with_retry(
        &standby,
        "drop_table",
        json!({ "namespace": "robots", "name": "drop_handoff" }),
    )
    .await
    .expect("repeated drop through successor");

    assert!(
        registry::get(meta.as_ref(), &table)
            .await
            .unwrap()
            .is_none()
    );
    assert!(engine.open(&location).await.unwrap().is_none());
    assert!(meta.list_prefix("drop/").await.unwrap().is_empty());

    if leader == addr_a {
        shutdown_b_tx.take().unwrap().send(()).unwrap();
        server_b.take().unwrap().await.unwrap().unwrap();
    } else {
        shutdown_a_tx.take().unwrap().send(()).unwrap();
        server_a.take().unwrap().await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn committed_replay_survives_graceful_leader_handoff() {
    let meta_dir = tempfile::tempdir().expect("meta tempdir");
    let table_dir = tempfile::tempdir().expect("table tempdir");
    let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).expect("open RocksMeta"));
    let engine: TableEngineRef = Arc::new(LanceEngine::with_manifest_store(meta.clone()));
    let addr_a = free_addr();
    let addr_b = free_addr();
    let (shutdown_a_tx, shutdown_a_rx) = oneshot::channel();
    let (shutdown_b_tx, shutdown_b_rx) = oneshot::channel();
    let mut shutdown_a_tx = Some(shutdown_a_tx);
    let mut shutdown_b_tx = Some(shutdown_b_tx);
    let placement = TablePlacement::local(table_dir.path().to_path_buf());
    let placement_a = placement.clone();

    let server_a = tokio::spawn({
        let node = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
        let addr = addr_a.clone();
        async move {
            serve_with_config_and_shutdown(
                node,
                &addr,
                MetasrvServerConfig::new()
                    .with_table_placement(placement_a)
                    .with_shutdown_grace(Duration::from_secs(1)),
                async move {
                    let _ = shutdown_a_rx.await;
                },
            )
            .await
        }
    });
    let server_b = tokio::spawn({
        let node = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
        let addr = addr_b.clone();
        async move {
            serve_with_config_and_shutdown(
                node,
                &addr,
                MetasrvServerConfig::new()
                    .with_table_placement(placement)
                    .with_shutdown_grace(Duration::from_secs(1)),
                async move {
                    let _ = shutdown_b_rx.await;
                },
            )
            .await
        }
    });
    let mut server_a = Some(server_a);
    let mut server_b = Some(server_b);

    wait_serving(&addr_a).await;
    wait_serving(&addr_b).await;
    let leader = wait_for_leader(&meta, &[&addr_a, &addr_b]).await;
    let standby = if leader == addr_a {
        addr_b.clone()
    } else {
        addr_a.clone()
    };
    forward_with_retry(
        &leader,
        "create_table",
        json!({
            "namespace": "robots",
            "name": "failover_arm",
            "columns": ["ts:i64", "reward:f64"],
        }),
    )
    .await
    .expect("leader creates table");

    let table = TableRef::new("robots", "failover_arm");
    let location = registry::get(meta.as_ref(), &table)
        .await
        .expect("registry get")
        .expect("created registration")
        .location;
    let messages = file_append_messages(table.clone(), AppendOperationId::generate()).await;
    let committed = do_file_append_messages(&leader, messages.clone())
        .await
        .expect("leader commits append");
    assert_eq!(committed, Version(2));

    if leader == addr_a {
        shutdown_a_tx
            .take()
            .expect("leader A sender")
            .send(())
            .expect("stop leader A");
        tokio::time::timeout(
            Duration::from_secs(3),
            server_a.take().expect("leader A task"),
        )
        .await
        .expect("leader A stops")
        .expect("leader A task joins")
        .expect("leader A shutdown succeeds");
    } else {
        shutdown_b_tx
            .take()
            .expect("leader B sender")
            .send(())
            .expect("stop leader B");
        tokio::time::timeout(
            Duration::from_secs(3),
            server_b.take().expect("leader B task"),
        )
        .await
        .expect("leader B stops")
        .expect("leader B task joins")
        .expect("leader B shutdown succeeds");
    }
    assert_eq!(wait_for_leader(&meta, &[&standby]).await, standby);

    let replayed = do_file_append_messages(&standby, messages)
        .await
        .expect("new leader reconciles replay");
    assert_eq!(replayed, committed, "replay must return the first version");
    let registered = registry::get(meta.as_ref(), &table)
        .await
        .expect("registry get")
        .expect("table remains registered");
    assert_eq!(registered.current_version, committed);
    assert_eq!(
        engine
            .open(&location)
            .await
            .expect("open table")
            .expect("table exists")
            .current_version(),
        committed
    );

    if leader == addr_a {
        shutdown_b_tx
            .take()
            .expect("standby B sender")
            .send(())
            .expect("stop standby B");
        server_b
            .take()
            .expect("standby B task")
            .await
            .expect("standby B task joins")
            .expect("standby B shutdown succeeds");
    } else {
        shutdown_a_tx
            .take()
            .expect("standby A sender")
            .send(())
            .expect("stop standby A");
        server_a
            .take()
            .expect("standby A task")
            .await
            .expect("standby A task joins")
            .expect("standby A shutdown succeeds");
    }
}

async fn secure_do_action(
    addr: &str,
    security: &ClientSecurity,
    r#type: &str,
    body: serde_json::Value,
) -> Result<(), Status> {
    let endpoint = security.endpoint_for_authority(addr);
    let channel = security
        .connect(endpoint)
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let mut client = FlightServiceClient::new(channel);
    let action = Action {
        r#type: r#type.to_owned(),
        body:   serde_json::to_vec(&body).expect("encode body").into(),
    };
    let response = client
        .do_action(security.authorize_request(Request::new(action)))
        .await?;
    response.into_inner().try_collect::<Vec<_>>().await?;
    Ok(())
}

async fn wait_secure_serving(addr: &str, security: &ClientSecurity) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if secure_do_action(addr, security, "list_namespaces", json!(null))
            .await
            .is_ok()
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "secured server {addr} did not start"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn secured_follower_forwards_with_peer_identity() {
    let credential = "metasrv-peer-credential";
    let certified =
        generate_simple_self_signed(vec!["localhost".to_owned()]).expect("test identity");
    let certificate = certified.cert.pem();
    let private_key = certified.key_pair.serialize_pem();
    let server_security = ServerSecurity::with_bearer_token(credential)
        .expect("bearer")
        .with_tls_identity_pem(certificate.as_bytes(), private_key.as_bytes());
    let client_security = ClientSecurity::new()
        .with_ca_certificate_pem(certificate.as_bytes().to_vec())
        .with_server_name("localhost")
        .with_bearer_token(credential)
        .expect("client bearer");

    let meta_dir = tempfile::tempdir().expect("meta tempdir");
    let table_dir = tempfile::tempdir().expect("table tempdir");
    let meta: MetaStoreRef = Arc::new(RocksMeta::open(meta_dir.path()).expect("open RocksMeta"));
    let engine: TableEngineRef = Arc::new(LanceEngine::with_manifest_store(meta.clone()));
    let addr_a = free_addr();
    let addr_b = free_addr();
    let placement = TablePlacement::local(table_dir.path().to_path_buf());

    for (node, addr) in [
        (
            Arc::new(Metasrv::new(meta.clone(), engine.clone())),
            addr_a.clone(),
        ),
        (
            Arc::new(Metasrv::new(meta.clone(), engine.clone())),
            addr_b.clone(),
        ),
    ] {
        let config = MetasrvServerConfig::new()
            .with_table_placement(placement.clone())
            .with_server_security(server_security.clone())
            .with_peer_security(client_security.clone());
        tokio::spawn(async move { serve_with_config(node, &addr, config).await });
    }

    wait_secure_serving(&addr_a, &client_security).await;
    wait_secure_serving(&addr_b, &client_security).await;
    let leader = wait_for_leader(&meta, &[&addr_a, &addr_b]).await;
    let follower = if leader == addr_a { &addr_b } else { &addr_a };
    let body = json!({
        "namespace": "robots",
        "name": "secure_arm",
        "columns": ["ts:i64", "reward:f64"],
    });

    secure_do_action(follower, &client_security, "create_table", body)
        .await
        .expect("secured follower forwards to leader");
    assert!(
        registry::get(meta.as_ref(), &TableRef::new("robots", "secure_arm"))
            .await
            .expect("registry")
            .is_some()
    );
}
