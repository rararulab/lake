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

//! Arrow encoding for the versioned Episode/ArtifactRef Dataset table.

use std::{collections::HashMap, sync::Arc};

use datafusion::arrow::{
    array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, StructArray,
        UInt64Array,
    },
    buffer::NullBuffer,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use lake_common::{
    ARTIFACT_REF_RECORD_KIND, DataLocation, EPISODE_RECORD_KIND, EPISODE_TABLE_CONTRACT_VERSION,
    EpisodeBundleV1,
};
use snafu::ResultExt;

use crate::{EpisodeTableSnafu, Result, data_location_field, data_location_fields};

const CONTRACT_METADATA_KEY: &str = "lake.robotics.episode_artifact.version";

/// Return the exact flat Arrow schema for v1 Episode and ArtifactRef rows.
///
/// The `object` field is nullable because Episode summary rows do not identify
/// a physical object. Every ArtifactRef row produced by
/// [`episode_artifact_table_v1`] carries a non-null top-level `DataLocation`.
#[must_use]
pub fn episode_artifact_table_schema_v1() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("record_kind", DataType::Utf8, false),
            Field::new("episode_id", DataType::Utf8, false),
            Field::new("manifest_artifact_id", DataType::Utf8, true),
            Field::new("robot_id", DataType::Utf8, true),
            Field::new("embodiment", DataType::Utf8, true),
            Field::new("task", DataType::Utf8, true),
            Field::new("started_at_ns", DataType::Int64, true),
            Field::new("duration_ns", DataType::UInt64, true),
            Field::new("num_steps", DataType::UInt64, true),
            Field::new("success", DataType::Boolean, true),
            Field::new("quality_score", DataType::Float64, true),
            Field::new("artifact_id", DataType::Utf8, true),
            Field::new("layer_id", DataType::Utf8, true),
            Field::new("role", DataType::Utf8, true),
            Field::new("recording_format", DataType::Utf8, true),
            Field::new("selector", DataType::Utf8, true),
            data_location_field("object", true),
            Field::new("schema_fingerprint", DataType::Utf8, true),
            Field::new("producer_version", DataType::Utf8, true),
        ],
        HashMap::from([(
            CONTRACT_METADATA_KEY.to_owned(),
            EPISODE_TABLE_CONTRACT_VERSION.to_string(),
        )]),
    ))
}

/// Encode one validated Episode bundle as an append-ready Arrow batch.
///
/// Row zero is the logical Episode summary. Remaining rows are its physical
/// Artifact references, each with a top-level `object FILE`. Encoding allocates
/// metadata only; immutable Artifact bytes remain in object storage.
pub fn episode_artifact_table_v1(bundle: &EpisodeBundleV1) -> Result<RecordBatch> {
    let episode = bundle.episode();
    let artifacts = bundle.artifact_refs();
    let rows = artifacts.len() + 1;
    let record_kinds = std::iter::once(EPISODE_RECORD_KIND).chain(std::iter::repeat_n(
        ARTIFACT_REF_RECORD_KIND,
        artifacts.len(),
    ));
    let episode_ids = std::iter::once(episode.episode_id())
        .chain(artifacts.iter().map(|artifact| artifact.episode_id()));
    let object_values = std::iter::once(None)
        .chain(artifacts.iter().map(|artifact| Some(artifact.object())))
        .collect::<Vec<_>>();
    let columns = vec![
        Arc::new(StringArray::from_iter_values(record_kinds)) as ArrayRef,
        Arc::new(StringArray::from_iter_values(episode_ids)),
        Arc::new(StringArray::from(
            std::iter::once(Some(episode.manifest_artifact_id()))
                .chain(std::iter::repeat_n(None, artifacts.len()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(episode_string_values(episode.robot_id(), artifacts.len())),
        Arc::new(episode_string_values(episode.embodiment(), artifacts.len())),
        Arc::new(episode_string_values(episode.task(), artifacts.len())),
        Arc::new(Int64Array::from(
            std::iter::once(episode.started_at_ns())
                .chain(std::iter::repeat_n(None, artifacts.len()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            std::iter::once(episode.duration_ns())
                .chain(std::iter::repeat_n(None, artifacts.len()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            std::iter::once(episode.num_steps())
                .chain(std::iter::repeat_n(None, artifacts.len()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(
            std::iter::once(episode.success())
                .chain(std::iter::repeat_n(None, artifacts.len()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            std::iter::once(episode.quality_score())
                .chain(std::iter::repeat_n(None, artifacts.len()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(artifact_string_values(artifacts, |artifact| {
            Some(artifact.artifact_id())
        })),
        Arc::new(artifact_string_values(artifacts, |artifact| {
            Some(artifact.layer_id())
        })),
        Arc::new(artifact_string_values(artifacts, |artifact| {
            Some(artifact.role())
        })),
        Arc::new(artifact_string_values(artifacts, |artifact| {
            artifact.recording_format()
        })),
        Arc::new(artifact_string_values(artifacts, |artifact| {
            artifact.selector()
        })),
        Arc::new(nullable_data_location_array(&object_values)),
        Arc::new(artifact_string_values(artifacts, |artifact| {
            artifact.schema_fingerprint()
        })),
        Arc::new(artifact_string_values(artifacts, |artifact| {
            artifact.producer_version()
        })),
    ];
    debug_assert!(columns.iter().all(|column| column.len() == rows));
    RecordBatch::try_new(episode_artifact_table_schema_v1(), columns).context(EpisodeTableSnafu)
}

fn episode_string_values(value: Option<&str>, artifact_count: usize) -> StringArray {
    StringArray::from(
        std::iter::once(value)
            .chain(std::iter::repeat_n(None, artifact_count))
            .collect::<Vec<_>>(),
    )
}

fn artifact_string_values<'a>(
    artifacts: &'a [lake_common::ArtifactRefV1],
    value: impl Fn(&'a lake_common::ArtifactRefV1) -> Option<&'a str>,
) -> StringArray {
    StringArray::from(
        std::iter::once(None)
            .chain(artifacts.iter().map(value))
            .collect::<Vec<_>>(),
    )
}

fn nullable_data_location_array(locations: &[Option<&DataLocation>]) -> StructArray {
    let uri = StringArray::from_iter_values(
        locations
            .iter()
            .map(|location| location.map_or("", |value| value.uri.as_str())),
    );
    let content_type = StringArray::from_iter_values(
        locations
            .iter()
            .map(|location| location.map_or("", |value| value.content_type.as_str())),
    );
    let size_bytes = UInt64Array::from_iter_values(
        locations
            .iter()
            .map(|location| location.map_or(0, |value| value.size_bytes)),
    );
    let sha256 = StringArray::from_iter_values(
        locations
            .iter()
            .map(|location| location.map_or("", |value| value.sha256.as_str())),
    );
    StructArray::new(
        data_location_fields(),
        vec![
            Arc::new(uri) as ArrayRef,
            Arc::new(content_type),
            Arc::new(size_bytes),
            Arc::new(sha256),
        ],
        Some(NullBuffer::from(
            locations.iter().map(Option::is_some).collect::<Vec<_>>(),
        )),
    )
}
