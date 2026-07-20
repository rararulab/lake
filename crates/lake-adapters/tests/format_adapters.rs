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

mod support;

use bytes::Bytes;
use lake_adapters::{AdapterError, McapAdapter, RandomAccessSource, RecordingAdapter, RrdAdapter};
use lake_common::EpisodeManifestV1;
use re_log_encoding::{Decodable as _, StreamFooter};

use self::support::{MemorySource, budget, context, mcap_fixture, rrd_fixture};

fn assert_send_sync<T: Send + Sync>() {}

fn assert_send<T: Send>(value: T) -> T { value }

#[tokio::test]
async fn format_adapters_emit_valid_episode_manifest_v1() {
    assert_send_sync::<RrdAdapter>();
    assert_send_sync::<McapAdapter>();
    assert_send_sync::<MemorySource>();

    let rrd_bytes = rrd_fixture();
    let rrd_source = MemorySource::new(rrd_bytes.clone());
    let rrd_adapter = RrdAdapter;
    let rrd_adapter: &dyn RecordingAdapter = &rrd_adapter;
    let _: &(dyn RandomAccessSource + Send + Sync) = &rrd_source;
    let rrd_context = context("artifact-contract-1", Some("robot-app/native-recording"));
    let rrd = assert_send(rrd_adapter.inspect(&rrd_source, &rrd_context, budget(rrd_bytes.len())))
        .await
        .expect("RRD contract fixture succeeds");

    let mcap_bytes = mcap_fixture();
    let mcap_source = MemorySource::new(mcap_bytes.clone());
    let mcap_adapter = McapAdapter;
    let mcap_adapter: &dyn RecordingAdapter = &mcap_adapter;
    let _: &(dyn RandomAccessSource + Send + Sync) = &mcap_source;
    let mcap_context = context("artifact-contract-1", None);
    let mcap =
        assert_send(mcap_adapter.inspect(&mcap_source, &mcap_context, budget(mcap_bytes.len())))
            .await
            .expect("MCAP contract fixture succeeds");

    for manifest in [&rrd, &mcap] {
        let _: &EpisodeManifestV1 = manifest;
        assert_eq!(manifest.summary().episode_id(), "episode-contract-1");
        assert_eq!(
            manifest.recordings()[0].recording_id(),
            "recording-contract-1"
        );
        assert_eq!(manifest.layers()[0].layer_id(), "layer-contract-base");
        assert_eq!(
            manifest.artifact_bindings()[0].artifact_id(),
            "artifact-contract-1"
        );
        let json = manifest.to_json().expect("encode canonical manifest");
        assert_eq!(
            EpisodeManifestV1::from_json(&json).expect("round-trip canonical manifest"),
            *manifest
        );
        let json = std::str::from_utf8(&json).expect("manifest JSON is UTF-8");
        for forbidden in [
            "StoreId",
            "RawRrdManifest",
            "SummaryReader",
            "RandomAccessSource",
            "signed_url",
            "object_bytes",
        ] {
            assert!(!json.contains(forbidden), "manifest leaked {forbidden}");
        }
    }
    assert_eq!(rrd.recordings()[0].recording_format(), "rrd");
    assert_eq!(mcap.recordings()[0].recording_format(), "mcap");
}

#[tokio::test]
async fn corrupt_format_index_fails_closed_without_fallback() {
    let mut rrd_bytes = rrd_fixture().to_vec();
    let footer_start = rrd_bytes.len() - StreamFooter::ENCODED_SIZE_BYTES;
    let footer = StreamFooter::from_rrd_bytes(&rrd_bytes[footer_start..])
        .expect("fixture stream footer decodes");
    let payload_start = usize::try_from(
        footer.entries[0]
            .rrd_footer_byte_span_from_start_excluding_header
            .start,
    )
    .expect("fixture footer offset fits usize");
    rrd_bytes[payload_start] ^= 0x01;
    let rrd_len = rrd_bytes.len();
    let rrd_source = MemorySource::new(Bytes::from(rrd_bytes));
    let error = RrdAdapter
        .inspect(
            &rrd_source,
            &context("artifact-corrupt-rrd", Some("robot-app/native-recording")),
            budget(rrd_len),
        )
        .await
        .expect_err("corrupt RRD footer must fail");
    assert!(matches!(error, AdapterError::Format { format: "rrd", .. }));
    assert!(rrd_source.total_bytes() < u64::try_from(rrd_len).expect("length fits u64"));

    let mut mcap_bytes = mcap_fixture().to_vec();
    let footer = mcap::read::footer(&mcap_bytes).expect("fixture MCAP footer decodes");
    assert!(footer.summary_start > 0);
    let summary_start = usize::try_from(footer.summary_start).expect("summary offset fits usize");
    mcap_bytes[summary_start] = 0xFF;
    let mcap_len = mcap_bytes.len();
    let mcap_source = MemorySource::new(Bytes::from(mcap_bytes));
    let error = McapAdapter
        .inspect(
            &mcap_source,
            &context("artifact-corrupt-mcap", None),
            budget(mcap_len),
        )
        .await
        .expect_err("corrupt MCAP summary must fail");
    assert!(matches!(error, AdapterError::Format { format: "mcap", .. }));
    assert!(mcap_source.total_bytes() < u64::try_from(mcap_len).expect("length fits u64"));
}
