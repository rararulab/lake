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

use std::{sync::Arc, time::Duration};

use arrow_flight::{FlightClient, FlightDescriptor, encode::FlightDataEncoderBuilder};
use datafusion::arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use lake_common::{FILE_APPEND_TYPE_URL, FileAppendRequest, TableLocation, TableRef};
use lake_engine::TableEngineRef;
use lake_engine_lance::LanceEngine;
use lake_meta::{MetaStoreRef, RocksMeta};
use lake_metasrv::Metasrv;
use lake_query::{QueryEngine, serve_with_metadata};
use prost::Message;
use prost_types::Any;
use tonic::transport::Channel;

fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("read local addr").to_string()
}

#[tokio::test]
async fn file_append_is_forwarded_without_payload_proxying() {
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
    tokio::spawn({
        let metasrv = metasrv.clone();
        let addr = meta_addr.clone();
        async move { lake_metasrv::serve(metasrv, &addr).await }
    });
    tokio::spawn({
        let query = Arc::new(QueryEngine::new(meta.clone(), engine));
        let addr = query_addr.clone();
        let metadata = format!("http://{meta_addr}");
        async move { serve_with_metadata(query, &addr, &metadata).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let append = FileAppendRequest::new(table.clone());
    let descriptor = FlightDescriptor::new_cmd(
        Any {
            type_url: FILE_APPEND_TYPE_URL.to_owned(),
            value:    append.command_payload(),
        }
        .encode_to_vec(),
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["episode-42"]))],
    )
    .unwrap();
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_flight_descriptor(Some(descriptor))
        .build(futures::stream::iter(vec![Ok(batch)]));
    let channel = Channel::from_shared(format!("http://{query_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightClient::new(channel);
    client
        .do_put(stream)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert!(
        metasrv
            .resolve(&table)
            .await
            .unwrap()
            .unwrap()
            .current_version
            .0
            > 1
    );
}
