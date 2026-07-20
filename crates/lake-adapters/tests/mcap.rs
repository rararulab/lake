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

use std::{borrow::Cow, collections::BTreeMap, io::Cursor, ops::Range, sync::Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use lake_adapters::{
    AdapterError, BudgetResource, EpisodeInspectionContext, McapAdapter, RandomAccessSource,
    ReadBudget, RecordingAdapter, SourceError,
};
use lake_common::{LayerKindV1, TimelineKindV1};
use mcap::{Attachment, WriteOptions, records::MessageHeader};

#[derive(Debug)]
struct InstrumentedSource {
    bytes:  Bytes,
    ranges: Mutex<Vec<Range<u64>>>,
}

impl InstrumentedSource {
    fn new(bytes: Bytes) -> Self {
        Self {
            bytes,
            ranges: Mutex::new(Vec::new()),
        }
    }

    fn ranges(&self) -> Vec<Range<u64>> { self.ranges.lock().expect("range log lock").clone() }

    fn total_bytes(&self) -> u64 {
        self.ranges()
            .into_iter()
            .map(|range| range.end - range.start)
            .sum()
    }
}

#[async_trait]
impl RandomAccessSource for InstrumentedSource {
    async fn size_bytes(&self) -> Result<u64, SourceError> {
        Ok(u64::try_from(self.bytes.len()).expect("fixture length fits u64"))
    }

    async fn read_range(&self, range: Range<u64>) -> Result<Bytes, SourceError> {
        let start = usize::try_from(range.start)
            .map_err(|_| SourceError::new("fixture range start exceeds usize"))?;
        let end = usize::try_from(range.end)
            .map_err(|_| SourceError::new("fixture range end exceeds usize"))?;
        let bytes = self
            .bytes
            .get(start..end)
            .ok_or_else(|| SourceError::new("fixture range is out of bounds"))?;
        self.ranges.lock().expect("range log lock").push(range);
        Ok(Bytes::copy_from_slice(bytes))
    }
}

fn context() -> EpisodeInspectionContext {
    EpisodeInspectionContext::builder()
        .episode_id("episode-lake-1")
        .recording_id("recording-lake-1")
        .layer_id("layer-base-1")
        .artifact_id("artifact-mcap-1")
        .build()
}

fn fixture(emit_summary: bool) -> (Bytes, Vec<Range<u64>>) {
    let cursor = Cursor::new(Vec::new());
    let mut writer = WriteOptions::new()
        .compression(None)
        .profile("ros2")
        .library("robot-collector/1.2.3")
        .chunk_size(Some(64 * 1024))
        .emit_summary_records(emit_summary)
        .emit_summary_offsets(emit_summary)
        .create(cursor)
        .expect("create MCAP writer");
    let joint_schema = writer
        .add_schema(
            "sensor_msgs/msg/JointState",
            "ros2msg",
            b"string[] name\nfloat64[] position",
        )
        .expect("add joint schema");
    let image_schema = writer
        .add_schema(
            "sensor_msgs/msg/CompressedImage",
            "ros2msg",
            b"std_msgs/Header header\nstring format\nuint8[] data",
        )
        .expect("add image schema");
    let joint_channel = writer
        .add_channel(joint_schema, "/joint_states", "cdr", &BTreeMap::new())
        .expect("add joint channel");
    let image_channel = writer
        .add_channel(
            image_schema,
            "/camera/front/compressed",
            "cdr",
            &BTreeMap::from([("media_type".to_owned(), "image/jpeg".to_owned())]),
        )
        .expect("add image channel");
    writer
        .write_to_known_channel(
            &MessageHeader {
                channel_id:   joint_channel,
                sequence:     1,
                log_time:     1_000,
                publish_time: 900,
            },
            &vec![0xA5; 96 * 1024],
        )
        .expect("write joint message");
    writer
        .write_to_known_channel(
            &MessageHeader {
                channel_id:   image_channel,
                sequence:     2,
                log_time:     3_000,
                publish_time: 2_900,
            },
            &vec![0x5A; 128 * 1024],
        )
        .expect("write image message");
    writer
        .attach(&Attachment {
            log_time:    2_000,
            create_time: 1_500,
            name:        "calibration.json".to_owned(),
            media_type:  "application/json".to_owned(),
            data:        Cow::Owned(vec![0xCC; 64 * 1024]),
        })
        .expect("write attachment");
    let summary = writer.finish().expect("finish MCAP fixture");
    let payload_ranges = summary
        .chunk_indexes
        .iter()
        .map(|index| index.chunk_start_offset..index.chunk_start_offset + index.chunk_length)
        .chain(
            summary
                .attachment_indexes
                .iter()
                .map(|index| index.offset..index.offset + index.length),
        )
        .collect();
    let bytes = writer.into_inner().into_inner();
    (Bytes::from(bytes), payload_ranges)
}

fn fixture_with_summary() -> (Bytes, Vec<Range<u64>>) { fixture(true) }

fn fixture_without_summary() -> Bytes { fixture(false).0 }

fn generous_budget(file_len: u64) -> ReadBudget {
    ReadBudget::try_new(file_len * 2, 128, file_len, file_len).expect("valid test budget")
}

fn intersects(left: &Range<u64>, right: &Range<u64>) -> bool {
    left.start < right.end && right.start < left.end
}

#[tokio::test]
async fn mcap_summary_metadata_stays_within_read_budget() {
    let (bytes, payload_ranges) = fixture_with_summary();
    let file_len = u64::try_from(bytes.len()).expect("fixture length fits u64");
    let source = InstrumentedSource::new(bytes.clone());
    let adapter = McapAdapter;

    let manifest = adapter
        .inspect(&source, &context(), generous_budget(file_len))
        .await
        .expect("indexed MCAP inspection succeeds");

    assert_eq!(manifest.summary().episode_id(), "episode-lake-1");
    assert_eq!(manifest.summary().started_at_ns(), Some(1_000));
    assert_eq!(manifest.summary().duration_ns(), Some(2_000));
    assert_eq!(manifest.summary().num_steps(), Some(2));
    assert_eq!(manifest.recordings().len(), 1);
    assert_eq!(manifest.recordings()[0].recording_id(), "recording-lake-1");
    assert_eq!(manifest.recordings()[0].recording_format(), "mcap");
    assert_eq!(
        manifest.recordings()[0].producer_version(),
        Some("robot-collector/1.2.3")
    );
    assert_eq!(manifest.timelines().len(), 1);
    assert_eq!(manifest.timelines()[0].timeline_id(), "log_time");
    assert_eq!(manifest.timelines()[0].kind(), TimelineKindV1::Timestamp);
    assert_eq!(manifest.streams().len(), 2);
    assert_eq!(
        manifest.streams()[0].stream_id(),
        "/camera/front/compressed"
    );
    assert_eq!(manifest.streams()[0].media_type(), Some("image/jpeg"));
    assert_eq!(manifest.streams()[0].codec(), Some("cdr"));
    assert!(manifest.streams()[0].schema_fingerprint().is_some());
    assert_eq!(manifest.streams()[1].stream_id(), "/joint_states");
    assert_eq!(manifest.layers()[0].layer_id(), "layer-base-1");
    assert_eq!(manifest.layers()[0].kind(), LayerKindV1::Base);
    assert_eq!(
        manifest.artifact_bindings()[0].artifact_id(),
        "artifact-mcap-1"
    );

    let successful_ranges = source.ranges();
    let successful_bytes = source.total_bytes();
    assert!(
        successful_bytes < file_len,
        "indexed path must not scan file"
    );
    assert!(successful_ranges.iter().all(|read| {
        payload_ranges
            .iter()
            .all(|payload| !intersects(read, payload))
    }));

    let request_count = u64::try_from(successful_ranges.len()).expect("range count fits u64");
    adapter
        .inspect(
            &InstrumentedSource::new(bytes.clone()),
            &context(),
            ReadBudget::try_new(successful_bytes, request_count, file_len, file_len)
                .expect("exact budget"),
        )
        .await
        .expect("exact observed budget succeeds");

    let byte_short = InstrumentedSource::new(bytes.clone());
    let error = adapter
        .inspect(
            &byte_short,
            &context(),
            ReadBudget::try_new(successful_bytes - 1, request_count, file_len, file_len)
                .expect("one-byte-short budget"),
        )
        .await
        .expect_err("one-byte-short budget must fail");
    assert!(matches!(
        error,
        AdapterError::BudgetExceeded {
            resource: BudgetResource::Bytes,
            ..
        }
    ));
    assert!(byte_short.total_bytes() < successful_bytes);

    let request_short = InstrumentedSource::new(bytes);
    let error = adapter
        .inspect(
            &request_short,
            &context(),
            ReadBudget::try_new(successful_bytes, request_count - 1, file_len, file_len)
                .expect("one-request-short budget"),
        )
        .await
        .expect_err("one-request-short budget must fail");
    assert!(matches!(
        error,
        AdapterError::BudgetExceeded {
            resource: BudgetResource::Requests,
            ..
        }
    ));
    assert!(
        u64::try_from(request_short.ranges().len()).expect("range count fits u64") < request_count
    );
}

#[tokio::test]
async fn mcap_missing_summary_uses_bounded_linear_fallback() {
    let indexed_bytes = fixture_with_summary().0;
    let summaryless_bytes = fixture_without_summary();
    let file_len = u64::try_from(summaryless_bytes.len()).expect("fixture length fits u64");
    let adapter = McapAdapter;

    let indexed = adapter
        .inspect(
            &InstrumentedSource::new(indexed_bytes.clone()),
            &context(),
            generous_budget(u64::try_from(indexed_bytes.len()).expect("fixture length fits u64")),
        )
        .await
        .expect("indexed comparison fixture succeeds");
    let source = InstrumentedSource::new(summaryless_bytes.clone());
    let fallback = adapter
        .inspect(
            &source,
            &context(),
            ReadBudget::try_new(file_len, 128, file_len, file_len).expect("exact scan budget"),
        )
        .await
        .expect("bounded summaryless inspection succeeds");

    assert_eq!(fallback, indexed);
    assert_eq!(source.total_bytes(), file_len);
    let request_count = u64::try_from(source.ranges().len()).expect("range count fits u64");
    assert!(request_count > 1);
    let canonical = fallback.to_json().expect("encode canonical manifest");
    assert_eq!(
        lake_common::EpisodeManifestV1::from_json(&canonical)
            .expect("fallback output is canonical"),
        fallback
    );

    adapter
        .inspect(
            &InstrumentedSource::new(summaryless_bytes.clone()),
            &context(),
            ReadBudget::try_new(file_len, request_count, file_len, file_len)
                .expect("exact observed budget"),
        )
        .await
        .expect("exact observed fallback budget succeeds");

    let scan_short = InstrumentedSource::new(summaryless_bytes.clone());
    let error = adapter
        .inspect(
            &scan_short,
            &context(),
            ReadBudget::try_new(file_len, request_count, file_len - 1, file_len)
                .expect("one-byte-short scan ceiling"),
        )
        .await
        .expect_err("fallback scan ceiling must fail");
    assert!(matches!(
        error,
        AdapterError::FallbackScanTooLarge {
            size_bytes,
            limit_bytes,
        } if size_bytes == file_len && limit_bytes == file_len - 1
    ));
    assert!(scan_short.total_bytes() < file_len);

    let byte_short = InstrumentedSource::new(summaryless_bytes.clone());
    let error = adapter
        .inspect(
            &byte_short,
            &context(),
            ReadBudget::try_new(file_len - 1, request_count, file_len, file_len)
                .expect("one-byte-short I/O budget"),
        )
        .await
        .expect_err("fallback byte budget must fail");
    assert!(matches!(
        error,
        AdapterError::BudgetExceeded {
            resource: BudgetResource::Bytes,
            ..
        }
    ));
    assert!(byte_short.total_bytes() < file_len);

    let record_limited = InstrumentedSource::new(summaryless_bytes.clone());
    let error = adapter
        .inspect(
            &record_limited,
            &context(),
            ReadBudget::try_new(file_len, request_count, file_len, 1_024)
                .expect("bounded record budget"),
        )
        .await
        .expect_err("oversized chunk must fail structurally");
    assert!(matches!(
        error,
        AdapterError::RecordTooLarge {
            format: "mcap",
            limit_bytes: 1_024,
            ..
        }
    ));
    assert_eq!(record_limited.total_bytes(), file_len);

    let request_short = InstrumentedSource::new(summaryless_bytes);
    let error = adapter
        .inspect(
            &request_short,
            &context(),
            ReadBudget::try_new(file_len, request_count - 1, file_len, file_len)
                .expect("one-request-short I/O budget"),
        )
        .await
        .expect_err("fallback request budget must fail");
    assert!(matches!(
        error,
        AdapterError::BudgetExceeded {
            resource: BudgetResource::Requests,
            ..
        }
    ));
    assert!(
        u64::try_from(request_short.ranges().len()).expect("range count fits u64") < request_count
    );
}
