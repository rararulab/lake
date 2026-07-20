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

use std::{io::Write as _, ops::Range, sync::Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use lake_adapters::{
    AdapterError, BudgetResource, EpisodeInspectionContext, RandomAccessSource, ReadBudget,
    RecordingAdapter, RrdAdapter, SourceError,
};
use lake_common::{LayerKindV1, TimelineKindV1};
use re_chunk::{Chunk, RowId, TimePoint, Timeline};
use re_log_encoding::{Encoder, read_rrd_footer};
use re_log_types::{LogMsg, SetStoreInfo, StoreId, StoreInfo, StoreSource};
use re_sdk_types::archetypes::Points3D;

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
        .artifact_id("artifact-rrd-1")
        .selector("robot-app/native-recording")
        .build()
}

fn fixture_with_footer() -> (Bytes, Vec<Range<u64>>) {
    let store_id = StoreId::recording("robot-app", "native-recording");
    let positions = (0..8_192).map(|value| {
        let value = value as f32;
        [value, value.mul_add(0.5, 3.0), value.sin()]
    });
    let chunk = Chunk::builder("/robot/camera/front")
        .with_archetype(
            RowId::new(),
            TimePoint::default().with(Timeline::new_sequence("frame"), 7),
            &Points3D::new(positions),
        )
        .build()
        .expect("valid Rerun chunk");
    let messages = [
        LogMsg::SetStoreInfo(SetStoreInfo {
            row_id: *RowId::ZERO,
            info:   StoreInfo::new(store_id.clone(), StoreSource::Unknown),
        }),
        LogMsg::ArrowMsg(
            store_id.clone(),
            chunk.to_arrow_msg().expect("chunk encodes as ArrowMsg"),
        ),
    ];
    let bytes = Encoder::encode(messages.iter().cloned().map(Ok)).expect("valid RRD fixture");

    let mut file = tempfile::NamedTempFile::new().expect("temporary RRD file");
    file.write_all(&bytes).expect("write RRD fixture");
    file.flush().expect("flush RRD fixture");
    let footer = read_rrd_footer(file.as_file_mut())
        .expect("fixture footer decodes")
        .expect("fixture has footer");
    let manifest = footer
        .manifests
        .get(&store_id)
        .expect("fixture store has manifest");
    let offsets = manifest
        .col_chunk_byte_offset()
        .expect("chunk offsets")
        .collect::<Vec<_>>();
    let sizes = manifest
        .col_chunk_byte_size()
        .expect("chunk sizes")
        .collect::<Vec<_>>();
    let payloads = offsets
        .into_iter()
        .zip(sizes)
        .map(|(start, len)| start..start + len)
        .collect();

    (Bytes::from(bytes), payloads)
}

fn generous_budget(file_len: u64) -> ReadBudget {
    ReadBudget::try_new(file_len * 2, 64, file_len, file_len).expect("valid test budget")
}

fn intersects(left: &Range<u64>, right: &Range<u64>) -> bool {
    left.start < right.end && right.start < left.end
}

#[tokio::test]
async fn rrd_footer_metadata_stays_within_read_budget() {
    let (bytes, payload_ranges) = fixture_with_footer();
    let file_len = u64::try_from(bytes.len()).expect("fixture length fits u64");
    let source = InstrumentedSource::new(bytes.clone());
    let adapter = RrdAdapter;

    let manifest = adapter
        .inspect(&source, &context(), generous_budget(file_len))
        .await
        .expect("indexed RRD inspection succeeds");

    assert_eq!(manifest.summary().episode_id(), "episode-lake-1");
    assert_eq!(manifest.recordings().len(), 1);
    assert_eq!(manifest.recordings()[0].recording_id(), "recording-lake-1");
    assert_eq!(manifest.recordings()[0].recording_format(), "rrd");
    assert!(manifest.recordings()[0].producer_version().is_some());
    assert_eq!(manifest.timelines().len(), 1);
    assert_eq!(manifest.timelines()[0].timeline_id(), "frame");
    assert_eq!(manifest.timelines()[0].kind(), TimelineKindV1::Sequence);
    assert_eq!(manifest.streams().len(), 1);
    assert_eq!(manifest.streams()[0].stream_id(), "/robot/camera/front");
    assert_eq!(manifest.streams()[0].timeline_ids(), &["frame"]);
    assert!(manifest.streams()[0].schema_fingerprint().is_some());
    assert_eq!(manifest.layers().len(), 1);
    assert_eq!(manifest.layers()[0].layer_id(), "layer-base-1");
    assert_eq!(manifest.layers()[0].kind(), LayerKindV1::Base);
    assert_eq!(manifest.artifact_bindings().len(), 1);
    assert_eq!(
        manifest.artifact_bindings()[0].artifact_id(),
        "artifact-rrd-1"
    );
    assert_eq!(
        manifest.artifact_bindings()[0].selector(),
        Some("robot-app/native-recording")
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

    let exact = InstrumentedSource::new(bytes.clone());
    adapter
        .inspect(
            &exact,
            &context(),
            ReadBudget::try_new(
                successful_bytes,
                u64::try_from(successful_ranges.len()).expect("range count fits u64"),
                file_len,
                file_len,
            )
            .expect("exact budget"),
        )
        .await
        .expect("exact observed budget succeeds");

    let byte_short = InstrumentedSource::new(bytes.clone());
    let error = adapter
        .inspect(
            &byte_short,
            &context(),
            ReadBudget::try_new(
                successful_bytes - 1,
                u64::try_from(successful_ranges.len()).expect("range count fits u64"),
                file_len,
                file_len,
            )
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
    let request_limit = u64::try_from(successful_ranges.len() - 1).expect("range count fits u64");
    let error = adapter
        .inspect(
            &request_short,
            &context(),
            ReadBudget::try_new(successful_bytes, request_limit, file_len, file_len)
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
        u64::try_from(request_short.ranges().len()).expect("range count fits u64") <= request_limit
    );
}
