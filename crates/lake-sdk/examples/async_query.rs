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

//! Run durable SQL through Flight PollFlightInfo and consume its Arrow parts.

use std::{error::Error, sync::Arc, time::Duration};

use arrow::array::Int64Array;
use futures::TryStreamExt;
use lake_engine_lance::LanceEngine;
use lake_meta::{MetaStoreRef, RocksMeta};
use lake_objects::LocalObjectStore;
use lake_query::{AsyncQueryConfig, QueryEngine, QueryServerConfig};
use lake_sdk::{AsyncQueryHandle, LakeClient};
use tempfile::tempdir;

fn free_addr() -> Result<String, Box<dyn Error>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let root = tempdir()?;
    let catalog: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("catalog"))?);
    let state: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("async-state"))?);
    let results = Arc::new(LocalObjectStore::open(root.path().join("async-results")).await?);
    let engine = Arc::new(QueryEngine::new(catalog, Arc::new(LanceEngine::new())));
    let address = free_addr()?;
    let server = tokio::spawn({
        let address = address.clone();
        async move {
            lake_query::serve_with_config(
                engine,
                &address,
                QueryServerConfig::new().with_async_queries(
                    AsyncQueryConfig::new(state, results)
                        .with_scan_interval(Duration::from_millis(10)),
                ),
            )
            .await
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The injected store is only the normal FILE stage. Async job state and
    // result parts remain server-owned; the SDK still connects only to Query.
    let stage = LocalObjectStore::open(root.path().join("managed-files")).await?;
    let client = LakeClient::connect_with_store(format!("http://{address}"), stage).await?;
    let handle = client
        .submit_async(
            "SELECT CAST(value AS BIGINT) AS value FROM (VALUES (1), (2)) AS rows(value) ORDER BY \
             value",
        )
        .await?;
    let checkpoint = handle.to_json()?;
    drop(client); // Simulate a caller restart after durable submission.

    let stage = LocalObjectStore::open(root.path().join("managed-files")).await?;
    let restarted = LakeClient::connect_with_store(format!("http://{address}"), stage).await?;
    let mut batches = restarted
        .resume_async(AsyncQueryHandle::from_json(&checkpoint)?)
        .await?;
    let mut values = Vec::new();
    while let Some(batch) = batches.try_next().await? {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("value is not Int64")?;
        values.extend(column.values().iter().copied());
    }
    assert_eq!(values, [1, 2]);
    eprintln!("restart-safe PollFlightInfo query completed: {values:?}");
    server.abort();
    Ok(())
}
