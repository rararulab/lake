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
    array::{Array, StringArray, StructArray},
    datatypes::{DataType, Field, Schema},
};
use lake_common::{TableLocation, TableRef};
use lake_engine::TableEngineRef;
use lake_engine_lance::LanceEngine;
use lake_meta::{MetaStoreRef, RocksMeta};
use lake_metasrv::Metasrv;
use lake_objects::{LocalObjectStore, data_location_field, data_location_from_array};
use lake_query::QueryEngine;
use lake_sdk::{FileUpload, InsertValue, LakeClient};
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

    let expected = b"streamed directly into the Lake-managed stage";
    let source = root.path().join("episode.mp4");
    tokio::fs::write(&source, expected).await?;
    let metadata_addr = free_addr()?;
    let query_addr = free_addr()?;
    tokio::spawn({
        let metasrv = metasrv.clone();
        let addr = metadata_addr.clone();
        async move { lake_metasrv::serve(metasrv, &addr).await }
    });
    tokio::spawn({
        let query = Arc::new(QueryEngine::new(meta.clone(), engine.clone()));
        let addr = query_addr.clone();
        let metadata = format!("http://{metadata_addr}");
        async move { lake_query::serve_with_metadata(query, &addr, &metadata).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let client = LakeClient::connect(
        format!("http://{query_addr}"),
        LocalObjectStore::open(root.path().join("objects")).await?,
    )
    .await?;
    client
        .insert(
            "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
            vec![
                InsertValue::Utf8("episode-42".to_owned()),
                InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
            ],
        )
        .await?;

    let batches = QueryEngine::new(meta, engine)
        .execute_sql("SELECT episode_id, video FROM lake.robots.episodes")
        .await?;
    let episode_ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("episode_id is not UTF-8")?;
    assert_eq!(episode_ids.value(0), "episode-42");
    let files = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or("video is not a FILE/DataLocation struct")?;
    let location = data_location_from_array(files, 0)?;

    let mut reader = client.open(&location).await?;
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).await?;
    assert_eq!(actual, expected);
    eprintln!("FILE upload and direct read succeeded: {}", location.uri);
    Ok(())
}
