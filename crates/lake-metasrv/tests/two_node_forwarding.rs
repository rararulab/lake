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
//! registered, and that a follower-issued `drop_table` forwards too.
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
use futures::{StreamExt, TryStreamExt};
use lake_common::{FILE_APPEND_TYPE_URL, FileAppendRequest, TableRef};
use lake_engine::TableEngineRef;
use lake_engine_lance::LanceEngine;
use lake_meta::{MetaStoreRef, RocksMeta, registry};
use lake_metasrv::{
    Metasrv,
    election::{LEASE_KEY, LeaseValue},
    serve,
};
use prost::Message;
use prost_types::Any;
use serde_json::json;
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

async fn do_file_append(addr: &str, table: TableRef) -> Result<(), Status> {
    let mut client = client(addr).await?;
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
    let append = FileAppendRequest::new(table);
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
    let stream = stream.map(|item| item.expect("encode FlightData"));
    let response = client.do_put(Request::new(stream)).await?;
    response.into_inner().try_collect::<Vec<_>>().await?;
    Ok(())
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

    tokio::spawn({
        let addr = addr_a.clone();
        async move { serve(node_a, &addr).await }
    });
    tokio::spawn({
        let addr = addr_b.clone();
        async move { serve(node_b, &addr).await }
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
    let location = format!("{}/robots/arm.lance", table_dir.path().display());
    let create_body = json!({
        "namespace": "robots",
        "name": "arm",
        "columns": ["ts:i64", "reward:f64"],
        "location": location,
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

    // A follower-issued drop forwards too: after it, the table is gone.
    let drop_body = json!({ "namespace": "robots", "name": "arm" });
    let dropped = forward_with_retry(&follower, "drop_table", drop_body).await;
    assert!(
        dropped.is_ok(),
        "follower {follower} did not forward drop_table to the leader: {dropped:?}"
    );
    let reg_after = registry::get(meta.as_ref(), &table)
        .await
        .expect("registry get after drop");
    assert!(
        reg_after.is_none(),
        "robots.arm still registered after a forwarded drop_table"
    );
}
