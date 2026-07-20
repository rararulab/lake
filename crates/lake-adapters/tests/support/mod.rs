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

use std::{collections::BTreeMap, io::Cursor, ops::Range, sync::Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use lake_adapters::{EpisodeInspectionContext, RandomAccessSource, ReadBudget, SourceError};
use mcap::{WriteOptions, records::MessageHeader};
use re_chunk::{Chunk, RowId, TimePoint, Timeline};
use re_log_encoding::Encoder;
use re_log_types::{LogMsg, SetStoreInfo, StoreId, StoreInfo, StoreSource};
use re_sdk_types::archetypes::Points3D;

pub struct MemorySource {
    bytes:  Bytes,
    ranges: Mutex<Vec<Range<u64>>>,
}

impl MemorySource {
    pub fn new(bytes: Bytes) -> Self {
        Self {
            bytes,
            ranges: Mutex::new(Vec::new()),
        }
    }

    pub fn total_bytes(&self) -> u64 {
        self.ranges
            .lock()
            .expect("range log lock")
            .iter()
            .map(|range| range.end - range.start)
            .sum()
    }
}

#[async_trait]
impl RandomAccessSource for MemorySource {
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

pub fn context(artifact_id: &str, selector: Option<&str>) -> EpisodeInspectionContext {
    EpisodeInspectionContext::builder()
        .episode_id("episode-contract-1")
        .recording_id("recording-contract-1")
        .layer_id("layer-contract-base")
        .artifact_id(artifact_id)
        .maybe_selector(selector.map(str::to_owned))
        .build()
}

pub fn budget(size: usize) -> ReadBudget {
    let size = u64::try_from(size).expect("fixture length fits u64");
    ReadBudget::try_new(size * 2, 128, size, size).expect("fixture budget is valid")
}

pub fn rrd_fixture() -> Bytes {
    let store_id = StoreId::recording("robot-app", "native-recording");
    let chunk = Chunk::builder("/robot/camera/front")
        .with_archetype(
            RowId::new(),
            TimePoint::default().with(Timeline::new_sequence("frame"), 1),
            &Points3D::new((0..2_048).map(|value| [value as f32, 1.0, 2.0])),
        )
        .build()
        .expect("valid Rerun chunk");
    let messages = [
        LogMsg::SetStoreInfo(SetStoreInfo {
            row_id: *RowId::ZERO,
            info:   StoreInfo::new(store_id.clone(), StoreSource::Unknown),
        }),
        LogMsg::ArrowMsg(
            store_id,
            chunk.to_arrow_msg().expect("chunk encodes as ArrowMsg"),
        ),
    ];
    Bytes::from(Encoder::encode(messages.into_iter().map(Ok)).expect("valid RRD fixture"))
}

pub fn mcap_fixture() -> Bytes {
    let mut writer = WriteOptions::new()
        .compression(None)
        .profile("ros2")
        .library("robot-collector/1.2.3")
        .create(Cursor::new(Vec::new()))
        .expect("create MCAP writer");
    let schema = writer
        .add_schema(
            "sensor_msgs/msg/PointCloud2",
            "ros2msg",
            b"std_msgs/Header header\nuint32 height\nuint32 width\nuint8[] data",
        )
        .expect("add MCAP schema");
    let channel = writer
        .add_channel(schema, "/robot/camera/front", "cdr", &BTreeMap::new())
        .expect("add MCAP channel");
    writer
        .write_to_known_channel(
            &MessageHeader {
                channel_id:   channel,
                sequence:     1,
                log_time:     1_000,
                publish_time: 900,
            },
            &vec![0xA5; 64 * 1024],
        )
        .expect("write MCAP message");
    writer.finish().expect("finish MCAP fixture");
    Bytes::from(writer.into_inner().into_inner())
}
