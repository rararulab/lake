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

//! Upload and directly read a SQL `FILE` through the local Rust SDK.

use std::{error::Error, sync::Arc, time::Duration};

use arrow::{
    array::{Array, StringArray},
    datatypes::{DataType, Field, Schema},
};
use futures::TryStreamExt;
use lake_common::{ManagedStageDescriptor, TableLocation, TableRef};
use lake_engine::TableEngineRef;
use lake_engine_lance::LanceEngine;
use lake_meta::{MetaStoreRef, RocksMeta};
use lake_metasrv::Metasrv;
use lake_objects::data_location_field;
use lake_query::QueryEngine;
use lake_sdk::{FileUpload, InsertValue, LakeClient, data_location};
use tempfile::tempdir;
use tokio::io::AsyncReadExt;

fn free_addr() -> Result<String, Box<dyn Error>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let root = tempdir()?;
    let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta"))?);
    let engine: TableEngineRef = Arc::new(LanceEngine::new());
    let metasrv = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
    let table = TableRef::new("robots", "episodes");
    metasrv
        .create_table(
            &table,
            TableLocation::new(root.path().join("tables/episodes.lance").to_string_lossy()),
            Arc::new(Schema::new(vec![
                Field::new("episode_id", DataType::Utf8, false),
                data_location_field("video", false),
            ])),
        )
        .await?;

    let expected = [
        b"first object streamed directly into the Lake-managed stage".as_slice(),
        b"second object streamed directly into the Lake-managed stage".as_slice(),
    ];
    let sources = [
        root.path().join("episode-1.mp4"),
        root.path().join("episode-2.mp4"),
    ];
    tokio::fs::write(&sources[0], expected[0]).await?;
    tokio::fs::write(&sources[1], expected[1]).await?;
    let metadata_addr = free_addr()?;
    let query_addr = free_addr()?;
    let managed_stage = ManagedStageDescriptor::local(
        root.path()
            .join("managed-objects")
            .to_string_lossy()
            .into_owned(),
    );
    tokio::spawn({
        let metasrv = metasrv.clone();
        let addr = metadata_addr.clone();
        async move { lake_metasrv::serve(metasrv, &addr).await }
    });
    tokio::spawn({
        let query = Arc::new(QueryEngine::new(meta.clone(), engine.clone()));
        let addr = query_addr.clone();
        let metadata = format!("http://{metadata_addr}");
        async move {
            lake_query::serve_with_metadata_and_stage(query, &addr, &metadata, managed_stage).await
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let client = LakeClient::builder(format!("http://{query_addr}"))
        .with_upload_checkpoint_dir(root.path().join("upload-checkpoints"))
        .connect()
        .await?;
    client
        .insert_many(
            "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
            vec![
                vec![
                    InsertValue::Utf8("episode-1".to_owned()),
                    InsertValue::File(FileUpload::from_path(&sources[0], "video/mp4")),
                ],
                vec![
                    InsertValue::Utf8("episode-2".to_owned()),
                    InsertValue::File(FileUpload::from_path(&sources[1], "video/mp4")),
                ],
            ],
        )
        .await?;

    let mut results = client
        .query("SELECT episode_id, video FROM lake.robots.episodes ORDER BY episode_id")
        .await?;
    let batch = results.try_next().await?.ok_or("query returned no rows")?;
    let episode_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("episode_id is not UTF-8")?;
    assert_eq!(episode_ids.value(0), "episode-1");
    assert_eq!(episode_ids.value(1), "episode-2");
    assert!(results.try_next().await?.is_none());

    for (row, expected) in expected.into_iter().enumerate() {
        let location = data_location(&batch, "video", row)?;
        let mut reader = client.open(&location).await?;
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await?;
        assert_eq!(actual, expected);
        eprintln!("FILE upload and direct read succeeded: {}", location.uri);
    }
    Ok(())
}
