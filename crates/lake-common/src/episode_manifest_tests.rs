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

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    ArtifactRefV1, DataLocation, MANIFEST_ARTIFACT_ROLE,
    episode_manifest::{
        EPISODE_MANIFEST_MEDIA_TYPE, EpisodeManifestDraftV1, EpisodeManifestError,
        EpisodeManifestV1, EpisodeSummaryV1, LayerKindV1, LayerV1, ManifestArtifactBindingV1,
        RecordingV1, StreamV1, TimelineKindV1, TimelineV1,
    },
};

fn summary() -> EpisodeSummaryV1 {
    EpisodeSummaryV1::builder()
        .episode_id("episode-001")
        .robot_id("robot-7")
        .embodiment("dual-arm")
        .task("fold-towel")
        .started_at_ns(1_750_000_000_000_000_000)
        .duration_ns(8_000_000_000)
        .num_steps(240)
        .success(true)
        .quality_score(0.95)
        .build()
}

fn manifest() -> EpisodeManifestV1 {
    EpisodeManifestV1::try_from_draft(
        EpisodeManifestDraftV1::builder()
            .summary(summary())
            .recordings(vec![
                RecordingV1::builder()
                    .recording_id("visualization")
                    .recording_format("rrd")
                    .producer_version("0.28.1")
                    .build(),
                RecordingV1::builder()
                    .recording_id("capture")
                    .recording_format("mcap")
                    .producer_version("1.3.0")
                    .build(),
            ])
            .timelines(vec![
                TimelineV1::builder()
                    .timeline_id("frame")
                    .kind(TimelineKindV1::Sequence)
                    .build(),
                TimelineV1::builder()
                    .timeline_id("log_time")
                    .kind(TimelineKindV1::Timestamp)
                    .build(),
            ])
            .streams(vec![
                StreamV1::builder()
                    .stream_id("camera.front")
                    .recording_id("visualization")
                    .timeline_ids(vec!["log_time".to_owned(), "frame".to_owned()])
                    .media_type("image/jpeg")
                    .codec("jpeg")
                    .schema_fingerprint("camera-schema-v1")
                    .build(),
                StreamV1::builder()
                    .stream_id("joint_states")
                    .recording_id("capture")
                    .timeline_ids(vec!["log_time".to_owned()])
                    .schema_fingerprint("ros-joint-state-v1")
                    .build(),
            ])
            .layers(vec![
                LayerV1::builder()
                    .layer_id("base")
                    .kind(LayerKindV1::Base)
                    .producer("collector@abc123")
                    .build(),
            ])
            .artifact_bindings(vec![
                ManifestArtifactBindingV1::builder()
                    .artifact_id("rrd-1")
                    .layer_id("base")
                    .role("recording")
                    .recording_id("visualization")
                    .selector("recording_id=episode-001")
                    .stream_ids(vec!["camera.front".to_owned()])
                    .schema_fingerprint("camera-schema-v1")
                    .producer_version("0.28.1")
                    .build(),
                ManifestArtifactBindingV1::builder()
                    .artifact_id("mcap-1")
                    .layer_id("base")
                    .role("recording")
                    .recording_id("capture")
                    .selector("log_time=[0,8000000000]")
                    .stream_ids(vec!["joint_states".to_owned()])
                    .schema_fingerprint("ros-joint-state-v1")
                    .producer_version("1.3.0")
                    .build(),
                ManifestArtifactBindingV1::builder()
                    .artifact_id("rrd-index-1")
                    .layer_id("base")
                    .role("index")
                    .recording_id("visualization")
                    .stream_ids(vec!["camera.front".to_owned()])
                    .sidecar_of("rrd-1")
                    .producer_version("0.28.1")
                    .build(),
            ])
            .build(),
    )
    .expect("valid two-format manifest")
}

fn location(name: &str, content_type: &str, sha: char) -> DataLocation {
    DataLocation {
        uri:          format!("s3://lake/objects/{name}"),
        content_type: content_type.to_owned(),
        size_bytes:   42,
        sha256:       sha.to_string().repeat(64),
    }
}

fn artifact_ref(
    episode_id: &str,
    artifact_id: &str,
    layer_id: &str,
    role: &str,
    recording_format: Option<&str>,
    selector: Option<&str>,
    schema_fingerprint: Option<&str>,
    producer_version: Option<&str>,
    sha: char,
) -> ArtifactRefV1 {
    ArtifactRefV1::builder()
        .episode_id(episode_id)
        .artifact_id(artifact_id)
        .layer_id(layer_id)
        .role(role)
        .maybe_recording_format(recording_format.map(str::to_owned))
        .maybe_selector(selector.map(str::to_owned))
        .object(location(artifact_id, "application/octet-stream", sha))
        .maybe_schema_fingerprint(schema_fingerprint.map(str::to_owned))
        .maybe_producer_version(producer_version.map(str::to_owned))
        .build()
}

fn artifact_refs(manifest: &EpisodeManifestV1) -> Vec<ArtifactRefV1> {
    let manifest_bytes = manifest.to_json().unwrap();
    let manifest_location = DataLocation {
        uri:          "s3://lake/objects/manifest-1".to_owned(),
        content_type: EPISODE_MANIFEST_MEDIA_TYPE.to_owned(),
        size_bytes:   u64::try_from(manifest_bytes.len()).unwrap(),
        sha256:       format!("{:x}", Sha256::digest(&manifest_bytes)),
    };
    vec![
        ArtifactRefV1::builder()
            .episode_id("episode-001")
            .artifact_id("manifest-1")
            .layer_id("base")
            .role(MANIFEST_ARTIFACT_ROLE)
            .object(manifest_location)
            .producer_version("lake-1.0")
            .build(),
        artifact_ref(
            "episode-001",
            "rrd-1",
            "base",
            "recording",
            Some("rrd"),
            Some("recording_id=episode-001"),
            Some("camera-schema-v1"),
            Some("0.28.1"),
            'b',
        ),
        artifact_ref(
            "episode-001",
            "mcap-1",
            "base",
            "recording",
            Some("mcap"),
            Some("log_time=[0,8000000000]"),
            Some("ros-joint-state-v1"),
            Some("1.3.0"),
            'c',
        ),
        artifact_ref(
            "episode-001",
            "rrd-index-1",
            "base",
            "index",
            Some("rrd"),
            None,
            None,
            Some("0.28.1"),
            'd',
        ),
    ]
}

#[test]
fn episode_manifest_v1_roundtrips_two_recording_formats() {
    let manifest = manifest();

    let encoded = manifest.to_json().expect("encode canonical manifest");
    let decoded = EpisodeManifestV1::from_json(&encoded).expect("decode canonical manifest");

    assert_eq!(decoded, manifest);
    assert_eq!(decoded.format_version(), 1);
    assert_eq!(decoded.recordings().len(), 2);
    assert_eq!(decoded.to_json().unwrap(), encoded);
    let json = std::str::from_utf8(&encoded).unwrap();
    for forbidden in [
        "\"uri\"",
        "\"sha256\"",
        "\"object\"",
        "access_key",
        "secret_key",
        "signed_url",
        "object_bytes",
    ] {
        assert!(!json.contains(forbidden), "manifest contains {forbidden}");
    }
}

#[test]
fn episode_manifest_v1_binds_complete_artifact_refs() {
    let manifest = manifest();

    let bundle = manifest
        .bind("manifest-1", artifact_refs(&manifest))
        .expect("bind complete artifact references");

    assert_eq!(
        bundle.episode().episode_id(),
        manifest.summary().episode_id()
    );
    assert_eq!(bundle.episode().robot_id(), Some("robot-7"));
    assert_eq!(bundle.episode().task(), Some("fold-towel"));
    assert_eq!(bundle.episode().duration_ns(), Some(8_000_000_000));
    assert_eq!(bundle.episode().manifest_artifact_id(), "manifest-1");
    assert_eq!(bundle.artifact_refs().len(), 4);
    assert_eq!(manifest.artifact_bindings().len(), 3);
}

#[test]
fn episode_manifest_v1_rejects_artifact_binding_mismatch() {
    let manifest = manifest();

    let mut stale_manifest = artifact_refs(&manifest);
    stale_manifest[0] = artifact_ref(
        "episode-001",
        "manifest-1",
        "base",
        MANIFEST_ARTIFACT_ROLE,
        None,
        None,
        None,
        Some("lake-1.0"),
        'a',
    );
    assert!(matches!(
        manifest.bind("manifest-1", stale_manifest),
        Err(EpisodeManifestError::ManifestObjectMismatch { .. })
    ));

    let mut missing = artifact_refs(&manifest);
    missing.pop();
    assert!(matches!(
        manifest.bind("manifest-1", missing),
        Err(EpisodeManifestError::ArtifactBindingMismatch { .. })
    ));

    let mut extra = artifact_refs(&manifest);
    extra.push(artifact_ref(
        "episode-001",
        "extra-1",
        "base",
        "attachment",
        None,
        None,
        None,
        None,
        'e',
    ));
    assert!(matches!(
        manifest.bind("manifest-1", extra),
        Err(EpisodeManifestError::ArtifactBindingMismatch { .. })
    ));

    let mut duplicate = artifact_refs(&manifest);
    duplicate.push(duplicate[1].clone());
    assert!(matches!(
        manifest.bind("manifest-1", duplicate),
        Err(EpisodeManifestError::ArtifactBindingMismatch { .. })
    ));

    for replacement in [
        artifact_ref(
            "episode-001",
            "rrd-1",
            "derived",
            "recording",
            Some("rrd"),
            Some("recording_id=episode-001"),
            Some("camera-schema-v1"),
            Some("0.28.1"),
            'b',
        ),
        artifact_ref(
            "episode-001",
            "rrd-1",
            "base",
            "recording",
            Some("mcap"),
            Some("recording_id=episode-001"),
            Some("camera-schema-v1"),
            Some("0.28.1"),
            'b',
        ),
        artifact_ref(
            "episode-001",
            "rrd-1",
            "base",
            "recording",
            Some("rrd"),
            Some("different-selector"),
            Some("camera-schema-v1"),
            Some("0.28.1"),
            'b',
        ),
    ] {
        let mut mismatched = artifact_refs(&manifest);
        mismatched[1] = replacement;
        assert!(matches!(
            manifest.bind("manifest-1", mismatched),
            Err(EpisodeManifestError::ArtifactBindingMismatch { .. })
        ));
    }

    let mut wrong_episode = artifact_refs(&manifest);
    wrong_episode[1] = artifact_ref(
        "episode-002",
        "rrd-1",
        "base",
        "recording",
        Some("rrd"),
        Some("recording_id=episode-001"),
        Some("camera-schema-v1"),
        Some("0.28.1"),
        'b',
    );
    assert!(matches!(
        manifest.bind("manifest-1", wrong_episode),
        Err(EpisodeManifestError::EpisodeMismatch { .. })
    ));
}

#[test]
fn episode_manifest_v1_rejects_invalid_wire() {
    assert!(matches!(
        EpisodeManifestV1::from_json(b"not-json"),
        Err(EpisodeManifestError::Json { .. })
    ));

    let encoded = manifest().to_json().unwrap();
    let canonical: Value = serde_json::from_slice(&encoded).unwrap();

    let mut future = canonical.clone();
    future["format_version"] = Value::from(2);
    assert!(matches!(
        EpisodeManifestV1::from_json(&serde_json::to_vec(&future).unwrap()),
        Err(EpisodeManifestError::UnsupportedVersion { .. })
    ));

    let mut unknown = canonical.clone();
    unknown["credential"] = Value::from("secret");
    assert!(matches!(
        EpisodeManifestV1::from_json(&serde_json::to_vec(&unknown).unwrap()),
        Err(EpisodeManifestError::Json { .. })
    ));

    let mut duplicate = canonical.clone();
    let first_recording = duplicate["recordings"][0].clone();
    duplicate["recordings"]
        .as_array_mut()
        .unwrap()
        .push(first_recording);
    assert!(matches!(
        EpisodeManifestV1::from_json(&serde_json::to_vec(&duplicate).unwrap()),
        Err(EpisodeManifestError::DuplicateIdentity { .. })
    ));

    let mut dangling = canonical.clone();
    dangling["streams"][0]["recording_id"] = Value::from("missing");
    assert!(matches!(
        EpisodeManifestV1::from_json(&serde_json::to_vec(&dangling).unwrap()),
        Err(EpisodeManifestError::MissingReference { .. })
    ));

    let mut unsorted = canonical;
    unsorted["recordings"].as_array_mut().unwrap().swap(0, 1);
    assert!(matches!(
        EpisodeManifestV1::from_json(&serde_json::to_vec(&unsorted).unwrap()),
        Err(EpisodeManifestError::NonCanonical)
    ));
}

#[test]
fn episode_manifest_v1_rejects_invalid_wire_noncanonical_json_bytes() {
    let encoded = manifest().to_json().unwrap();
    let value: Value = serde_json::from_slice(&encoded).unwrap();
    let pretty = serde_json::to_vec_pretty(&value).unwrap();
    let reordered = serde_json::to_vec(&value).unwrap();
    let mut trailing = encoded.clone();
    trailing.push(b'\n');
    let alternate_number = String::from_utf8(encoded.clone())
        .unwrap()
        .replace("\"quality_score\":0.95", "\"quality_score\":9.5e-1")
        .into_bytes();

    for noncanonical in [pretty, reordered, trailing, alternate_number] {
        assert_ne!(noncanonical, encoded);
        assert!(matches!(
            EpisodeManifestV1::from_json(&noncanonical),
            Err(EpisodeManifestError::NonCanonical)
        ));
    }
}

#[test]
fn episode_manifest_v1_rejects_invalid_wire_duplicate_artifact_identity() {
    let encoded = manifest().to_json().unwrap();
    let mut duplicate: Value = serde_json::from_slice(&encoded).unwrap();
    let bindings = duplicate["artifact_bindings"].as_array_mut().unwrap();
    bindings[2]["artifact_id"] = Value::from("rrd-1");
    bindings[2]["sidecar_of"] = Value::Null;
    bindings.swap(1, 2);

    assert!(matches!(
        EpisodeManifestV1::from_json(&serde_json::to_vec(&duplicate).unwrap()),
        Err(EpisodeManifestError::DuplicateIdentity {
            kind: "Artifact binding",
            ..
        })
    ));
}
