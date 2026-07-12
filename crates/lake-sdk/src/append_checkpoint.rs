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

use std::path::{Path, PathBuf};

use arrow_flight::{FlightData, FlightDescriptor};
use lake_common::{AppendOperationId, FILE_APPEND_TYPE_URL, FileAppendRequest};
use lake_flight::append_flight_payload_digest;
use prost::Message;
use prost_types::Any;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{PendingAppend, Result, SdkError, validate_flight_payload_size};

const FORMAT_VERSION: u32 = 1;
const CHECKPOINT_SUFFIX: &str = ".append.pb";
const MAX_CHECKPOINT_OVERHEAD: u64 = 1024 * 1024;
const MAX_FLIGHT_MESSAGES: usize = 4_096;

#[derive(Clone, PartialEq, Message)]
struct AppendCheckpointV1 {
    #[prost(uint32, tag = "1")]
    format_version:   u32,
    #[prost(string, tag = "2")]
    operation_id:     String,
    #[prost(string, tag = "3")]
    stage_identity:   String,
    #[prost(bytes = "vec", tag = "4")]
    flight_payload:   Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    integrity_sha256: Vec<u8>,
}

pub(super) async fn save(
    directory: Option<&Path>,
    pending: &PendingAppend,
    stage_identity: &str,
    max_flight_bytes: usize,
) -> Result<Option<PathBuf>> {
    let Some(directory) = directory else {
        return Ok(None);
    };
    validate_flight_payload_size(&pending.messages, max_flight_bytes)?;
    validate_append_contract(
        pending.operation_id(),
        &pending.messages,
        Path::new("<prepared append>"),
    )?;
    let path = checkpoint_path(directory, pending.operation_id());
    match tokio::fs::try_exists(&path).await {
        Ok(true) => {
            let existing = load(
                Some(directory),
                pending.operation_id(),
                stage_identity,
                max_flight_bytes,
            )
            .await?;
            if !same_messages(&existing.messages, &pending.messages) {
                return Err(invalid(
                    &path,
                    "existing checkpoint payload differs for the same operation ID",
                ));
            }
            return Ok(Some(path));
        }
        Ok(false) => {}
        Err(source) => return Err(checkpoint_io("inspecting", &path, source)),
    }
    let mut checkpoint = AppendCheckpointV1 {
        format_version:   FORMAT_VERSION,
        operation_id:     pending.operation_id().as_str().to_owned(),
        stage_identity:   stage_identity.to_owned(),
        flight_payload:   encode_messages(&pending.messages)?,
        integrity_sha256: Vec::new(),
    };
    checkpoint.integrity_sha256 = integrity_digest(&checkpoint);
    let bytes = checkpoint.encode_to_vec();
    enforce_checkpoint_size(
        &path,
        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        max_flight_bytes,
    )?;
    save_atomic(&path, &bytes, pending).await?;
    Ok(Some(path))
}

pub(super) async fn list(
    directory: Option<&Path>,
    maximum: usize,
) -> Result<Vec<AppendOperationId>> {
    let Some(directory) = directory else {
        return Ok(Vec::new());
    };
    let mut entries = match tokio::fs::read_dir(directory).await {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => return Err(checkpoint_io("listing", directory, source)),
    };
    let mut operations = Vec::new();
    let mut inspected = 0usize;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|source| checkpoint_io("listing", directory, source))?
    {
        inspected = inspected.saturating_add(1);
        if inspected > maximum {
            return Err(SdkError::TooManyPendingAppendCheckpoints { maximum });
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(operation) = name.strip_suffix(CHECKPOINT_SUFFIX) else {
            continue;
        };
        if !entry
            .file_type()
            .await
            .map_err(|source| checkpoint_io("inspecting", &entry.path(), source))?
            .is_file()
        {
            return Err(invalid(
                &entry.path(),
                "checkpoint entry is not a regular file",
            ));
        }
        let operation = AppendOperationId::parse(operation.to_owned()).ok_or_else(|| {
            invalid(
                &entry.path(),
                "filename is not a canonical UUIDv7 operation",
            )
        })?;
        if operation.as_str()
            != name
                .strip_suffix(CHECKPOINT_SUFFIX)
                .expect("suffix checked")
        {
            return Err(invalid(
                &entry.path(),
                "filename is not a canonical UUIDv7 operation",
            ));
        }
        operations.push(operation);
        if operations.len() > maximum {
            return Err(SdkError::TooManyPendingAppendCheckpoints { maximum });
        }
    }
    operations.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
    Ok(operations)
}

pub(super) async fn load(
    directory: Option<&Path>,
    operation_id: &AppendOperationId,
    expected_stage_identity: &str,
    max_flight_bytes: usize,
) -> Result<PendingAppend> {
    let directory = directory.ok_or(SdkError::AppendCheckpointingDisabled)?;
    let path = checkpoint_path(directory, operation_id);
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    #[cfg(windows)]
    options.custom_flags(0x0020_0000); // FILE_FLAG_OPEN_REPARSE_POINT
    let file = options
        .open(&path)
        .await
        .map_err(|source| checkpoint_io("opening", &path, source))?;
    let metadata = file
        .metadata()
        .await
        .map_err(|source| checkpoint_io("inspecting", &path, source))?;
    if !metadata.file_type().is_file() {
        return Err(invalid(&path, "checkpoint is not a regular file"));
    }
    enforce_checkpoint_size(&path, metadata.len(), max_flight_bytes)?;
    let maximum = checkpoint_max_size(max_flight_bytes);
    let capacity = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
    let mut bytes = Vec::with_capacity(capacity.min(max_flight_bytes));
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)
        .await
        .map_err(|source| checkpoint_io("reading", &path, source))?;
    enforce_checkpoint_size(
        &path,
        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        max_flight_bytes,
    )?;
    let checkpoint = AppendCheckpointV1::decode(bytes.as_slice())
        .map_err(|source| invalid(&path, format!("protobuf decode failed: {source}")))?;
    if checkpoint.format_version != FORMAT_VERSION {
        return Err(invalid(&path, "unsupported format version"));
    }
    if checkpoint.operation_id != operation_id.as_str() {
        return Err(invalid(
            &path,
            "content operation ID does not match filename",
        ));
    }
    if checkpoint.stage_identity != expected_stage_identity {
        return Err(invalid(
            &path,
            "managed stage identity does not match this client",
        ));
    }
    if checkpoint.integrity_sha256 != integrity_digest(&checkpoint) {
        return Err(invalid(&path, "checkpoint integrity digest does not match"));
    }
    let messages = decode_messages(&checkpoint.flight_payload, &path)?;
    validate_flight_payload_size(&messages, max_flight_bytes)?;
    validate_append_contract(operation_id, &messages, &path)?;
    Ok(PendingAppend {
        operation_id: operation_id.clone(),
        messages,
        checkpoint: Some(path),
    })
}

pub(super) async fn remove(path: Option<&Path>) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    match tokio::fs::remove_file(path).await {
        Ok(()) => sync_parent(path).await,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(checkpoint_io("removing", path, source)),
    }
}

fn checkpoint_path(directory: &Path, operation_id: &AppendOperationId) -> PathBuf {
    directory.join(format!("{}{CHECKPOINT_SUFFIX}", operation_id.as_str()))
}

fn enforce_checkpoint_size(path: &Path, actual: u64, max_flight_bytes: usize) -> Result<()> {
    let maximum = checkpoint_max_size(max_flight_bytes);
    if actual > maximum {
        return Err(SdkError::AppendCheckpointTooLarge {
            path: path.to_path_buf(),
            actual,
            maximum,
        });
    }
    Ok(())
}

fn checkpoint_max_size(max_flight_bytes: usize) -> u64 {
    u64::try_from(max_flight_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(MAX_CHECKPOINT_OVERHEAD)
}

fn validate_append_contract(
    operation_id: &AppendOperationId,
    messages: &[FlightData],
    path: &Path,
) -> Result<()> {
    let descriptor = messages
        .first()
        .and_then(|message| message.flight_descriptor.as_ref())
        .ok_or_else(|| invalid(path, "first Flight message has no append descriptor"))?;
    let request = append_request(descriptor)
        .ok_or_else(|| invalid(path, "first Flight descriptor is not a FILE append command"))?;
    if request.operation_id() != operation_id {
        return Err(invalid(
            path,
            "descriptor operation ID does not match checkpoint",
        ));
    }
    if request.payload_digest() != &append_flight_payload_digest(messages) {
        return Err(invalid(
            path,
            "descriptor payload digest does not match Flight messages",
        ));
    }
    Ok(())
}

fn append_request(descriptor: &FlightDescriptor) -> Option<FileAppendRequest> {
    let command = Any::decode(descriptor.cmd.as_ref()).ok()?;
    if command.type_url != FILE_APPEND_TYPE_URL {
        return None;
    }
    FileAppendRequest::from_command_payload(&command.value)
}

fn integrity_digest(checkpoint: &AppendCheckpointV1) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(checkpoint.format_version.to_be_bytes());
    update_len_prefixed(&mut hasher, checkpoint.operation_id.as_bytes());
    update_len_prefixed(&mut hasher, checkpoint.stage_identity.as_bytes());
    update_len_prefixed(&mut hasher, &checkpoint.flight_payload);
    hasher.finalize().to_vec()
}

fn encode_messages(messages: &[FlightData]) -> Result<Vec<u8>> {
    if messages.len() > MAX_FLIGHT_MESSAGES {
        return Err(invalid(
            Path::new("<prepared append>"),
            "Flight message count exceeds checkpoint limit",
        ));
    }
    let capacity = messages.iter().fold(4usize, |total, message| {
        total
            .saturating_add(4)
            .saturating_add(message.encoded_len())
    });
    let mut payload = Vec::with_capacity(capacity);
    payload.extend_from_slice(
        &u32::try_from(messages.len())
            .map_err(|_| {
                invalid(
                    Path::new("<prepared append>"),
                    "Flight message count overflow",
                )
            })?
            .to_be_bytes(),
    );
    for message in messages {
        let encoded = message.encode_to_vec();
        payload.extend_from_slice(
            &u32::try_from(encoded.len())
                .map_err(|_| invalid(Path::new("<prepared append>"), "Flight message too large"))?
                .to_be_bytes(),
        );
        payload.extend_from_slice(&encoded);
    }
    Ok(payload)
}

fn decode_messages(payload: &[u8], path: &Path) -> Result<Vec<FlightData>> {
    let mut cursor = 0usize;
    let count = usize::try_from(read_u32(payload, &mut cursor, path)?)
        .expect("u32 always fits supported usize");
    if count > MAX_FLIGHT_MESSAGES {
        return Err(invalid(
            path,
            "Flight message count exceeds checkpoint limit",
        ));
    }
    let mut messages = Vec::with_capacity(count);
    for _ in 0..count {
        let length = usize::try_from(read_u32(payload, &mut cursor, path)?)
            .expect("u32 always fits supported usize");
        let end = cursor
            .checked_add(length)
            .filter(|end| *end <= payload.len())
            .ok_or_else(|| invalid(path, "Flight message length exceeds checkpoint payload"))?;
        messages.push(
            FlightData::decode(&payload[cursor..end]).map_err(|source| {
                invalid(path, format!("Flight message decode failed: {source}"))
            })?,
        );
        cursor = end;
    }
    if cursor != payload.len() {
        return Err(invalid(path, "checkpoint payload has trailing bytes"));
    }
    Ok(messages)
}

fn read_u32(payload: &[u8], cursor: &mut usize, path: &Path) -> Result<u32> {
    let end = cursor
        .checked_add(4)
        .filter(|end| *end <= payload.len())
        .ok_or_else(|| invalid(path, "checkpoint payload ended before a length field"))?;
    let value = u32::from_be_bytes(
        payload[*cursor..end]
            .try_into()
            .expect("validated four-byte length field"),
    );
    *cursor = end;
    Ok(value)
}

fn update_len_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

fn same_messages(left: &[FlightData], right: &[FlightData]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.encode_to_vec() == right.encode_to_vec())
}

async fn save_atomic(path: &Path, bytes: &[u8], pending: &PendingAppend) -> Result<()> {
    save_atomic_inner(path, bytes, pending, false).await
}

async fn save_atomic_inner(
    path: &Path,
    bytes: &[u8],
    pending: &PendingAppend,
    inject_post_publish_sync_failure: bool,
) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        checkpoint_io(
            "resolving parent of",
            path,
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "checkpoint has no parent"),
        )
    })?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|source| checkpoint_io("creating directory for", path, source))?;
    let temporary = temporary_path(path);
    let result = async {
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options
            .open(&temporary)
            .await
            .map_err(|source| checkpoint_io("creating", &temporary, source))?;
        file.write_all(bytes)
            .await
            .map_err(|source| checkpoint_io("writing", &temporary, source))?;
        file.sync_all()
            .await
            .map_err(|source| checkpoint_io("syncing", &temporary, source))?;
        tokio::fs::rename(&temporary, path)
            .await
            .map_err(|source| checkpoint_io("publishing", path, source))?;
        let sync_result = if inject_post_publish_sync_failure {
            Err(std::io::Error::other(
                "injected post-publish directory sync failure",
            ))
        } else {
            sync_parent_io(path).await
        };
        if let Err(source) = sync_result {
            let mut recoverable = pending.clone();
            recoverable.checkpoint = Some(path.to_path_buf());
            return Err(SdkError::AppendCheckpointPublishUncertain {
                path: path.to_path_buf(),
                pending: recoverable,
                source,
            });
        }
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}

async fn sync_parent(path: &Path) -> Result<()> {
    sync_parent_io(path)
        .await
        .map_err(|source| checkpoint_io("syncing directory for", path, source))
}

async fn sync_parent_io(path: &Path) -> std::io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "checkpoint has no parent")
    })?;
    let directory = tokio::fs::File::open(parent).await?;
    directory.sync_all().await
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map_or_else(|| "append-checkpoint".into(), std::ffi::OsString::from);
    name.push(format!(".{}.tmp", std::process::id()));
    path.with_file_name(name)
}

fn checkpoint_io(action: &'static str, path: &Path, source: std::io::Error) -> SdkError {
    SdkError::AppendCheckpointIo {
        action,
        path: path.to_path_buf(),
        source,
    }
}

fn invalid(path: &Path, message: impl Into<String>) -> SdkError {
    SdkError::InvalidAppendCheckpoint {
        path:    path.to_path_buf(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use arrow_flight::{FlightData, FlightDescriptor};
    use lake_common::{AppendOperationId, FILE_APPEND_TYPE_URL, FileAppendRequest, TableRef};
    use prost::Message;
    use prost_types::Any;

    use super::{
        AppendCheckpointV1, decode_messages, encode_messages, integrity_digest, save_atomic_inner,
    };
    use crate::{MAX_INSERT_FLIGHT_BYTES, PendingAppend, SdkError, append_checkpoint};

    fn pending() -> PendingAppend {
        let operation_id = AppendOperationId::generate();
        let mut messages = vec![FlightData::default(), FlightData::default()];
        messages[1].data_body = b"bounded Arrow payload".to_vec().into();
        let request = FileAppendRequest::new(
            TableRef::new("robots", "episodes"),
            operation_id.clone(),
            lake_flight::append_flight_payload_digest(&messages),
        );
        messages[0].flight_descriptor = Some(FlightDescriptor::new_cmd(
            Any {
                type_url: FILE_APPEND_TYPE_URL.to_owned(),
                value:    request.command_payload(),
            }
            .encode_to_vec(),
        ));
        PendingAppend {
            operation_id,
            messages,
            checkpoint: None,
        }
    }

    #[tokio::test]
    async fn durable_append_checkpoint_rejects_invalid_state() {
        let root = tempfile::tempdir().unwrap();
        let mut pending = pending();
        pending.checkpoint = append_checkpoint::save(
            Some(root.path()),
            &pending,
            "stage-a",
            MAX_INSERT_FLIGHT_BYTES,
        )
        .await
        .unwrap();
        let path = pending.checkpoint.clone().unwrap();

        tokio::fs::write(&path, b"not protobuf").await.unwrap();
        assert!(matches!(
            append_checkpoint::load(
                Some(root.path()),
                pending.operation_id(),
                "stage-a",
                MAX_INSERT_FLIGHT_BYTES,
            )
            .await,
            Err(SdkError::InvalidAppendCheckpoint { .. })
        ));

        let file = tokio::fs::File::create(&path).await.unwrap();
        file.set_len(u64::try_from(MAX_INSERT_FLIGHT_BYTES).unwrap() + 1024 * 1024 + 1)
            .await
            .unwrap();
        assert!(matches!(
            append_checkpoint::load(
                Some(root.path()),
                pending.operation_id(),
                "stage-a",
                MAX_INSERT_FLIGHT_BYTES,
            )
            .await,
            Err(SdkError::AppendCheckpointTooLarge { .. })
        ));

        tokio::fs::remove_file(&path).await.unwrap();
        pending.checkpoint = append_checkpoint::save(
            Some(root.path()),
            &pending,
            "stage-a",
            MAX_INSERT_FLIGHT_BYTES,
        )
        .await
        .unwrap();
        assert!(matches!(
            append_checkpoint::load(
                Some(root.path()),
                pending.operation_id(),
                "stage-b",
                MAX_INSERT_FLIGHT_BYTES,
            )
            .await,
            Err(SdkError::InvalidAppendCheckpoint { .. })
        ));

        let bytes = tokio::fs::read(&path).await.unwrap();
        let mut wire = AppendCheckpointV1::decode(bytes.as_slice()).unwrap();
        let mut messages = decode_messages(&wire.flight_payload, &path).unwrap();
        let mismatched = FileAppendRequest::new(
            TableRef::new("robots", "episodes"),
            AppendOperationId::generate(),
            lake_flight::append_flight_payload_digest(&messages),
        );
        messages[0].flight_descriptor = Some(FlightDescriptor::new_cmd(
            Any {
                type_url: FILE_APPEND_TYPE_URL.to_owned(),
                value:    mismatched.command_payload(),
            }
            .encode_to_vec(),
        ));
        wire.flight_payload = encode_messages(&messages).unwrap();
        wire.integrity_sha256 = integrity_digest(&wire);
        tokio::fs::write(&path, wire.encode_to_vec()).await.unwrap();
        assert!(matches!(
            append_checkpoint::load(
                Some(root.path()),
                pending.operation_id(),
                "stage-a",
                MAX_INSERT_FLIGHT_BYTES,
            )
            .await,
            Err(SdkError::InvalidAppendCheckpoint { .. })
        ));

        tokio::fs::remove_file(&path).await.unwrap();
        tokio::fs::write(root.path().join("unrelated-a"), b"ignored")
            .await
            .unwrap();
        tokio::fs::write(root.path().join("unrelated-b"), b"ignored")
            .await
            .unwrap();
        assert!(matches!(
            append_checkpoint::list(Some(root.path()), 1).await,
            Err(SdkError::TooManyPendingAppendCheckpoints { maximum: 1 })
        ));
        tokio::fs::remove_file(root.path().join("unrelated-a"))
            .await
            .unwrap();
        tokio::fs::remove_file(root.path().join("unrelated-b"))
            .await
            .unwrap();
        tokio::fs::write(root.path().join("not-a-uuid.append.pb"), b"invalid")
            .await
            .unwrap();
        assert!(matches!(
            append_checkpoint::list(Some(root.path()), 1).await,
            Err(SdkError::InvalidAppendCheckpoint { .. })
        ));
    }

    #[tokio::test]
    async fn published_checkpoint_sync_failure_returns_recoverable_operation() {
        let root = tempfile::tempdir().unwrap();
        let pending = pending();
        let path = root
            .path()
            .join(format!("{}.append.pb", pending.operation_id().as_str()));
        let mut wire = AppendCheckpointV1 {
            format_version:   1,
            operation_id:     pending.operation_id().as_str().to_owned(),
            stage_identity:   "stage-a".to_owned(),
            flight_payload:   encode_messages(&pending.messages).unwrap(),
            integrity_sha256: Vec::new(),
        };
        wire.integrity_sha256 = integrity_digest(&wire);
        let error = save_atomic_inner(&path, &wire.encode_to_vec(), &pending, true)
            .await
            .unwrap_err();

        assert!(path.exists(), "rename published the final checkpoint");
        let recovered = error
            .into_pending_append()
            .expect("post-publish uncertainty returns the exact operation");
        assert_eq!(recovered.operation_id(), pending.operation_id());
        assert_eq!(recovered.checkpoint.as_deref(), Some(path.as_path()));
        assert_eq!(
            append_checkpoint::load(
                Some(root.path()),
                pending.operation_id(),
                "stage-a",
                MAX_INSERT_FLIGHT_BYTES,
            )
            .await
            .unwrap()
            .operation_id(),
            pending.operation_id()
        );
    }

    #[tokio::test]
    async fn checkpoint_listing_rejects_noncanonical_uuid_filename() {
        let root = tempfile::tempdir().unwrap();
        let pending = pending();
        let uppercase = pending.operation_id().as_str().to_ascii_uppercase();
        tokio::fs::write(
            root.path().join(format!("{uppercase}.append.pb")),
            b"invalid",
        )
        .await
        .unwrap();

        assert!(matches!(
            append_checkpoint::list(Some(root.path()), 1).await,
            Err(SdkError::InvalidAppendCheckpoint { .. })
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn checkpoint_load_never_follows_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let pending = pending();
        let checkpoint = append_checkpoint::save(
            Some(root.path()),
            &pending,
            "stage-a",
            MAX_INSERT_FLIGHT_BYTES,
        )
        .await
        .unwrap()
        .unwrap();
        let target = root.path().join("target.append.pb");
        tokio::fs::rename(&checkpoint, &target).await.unwrap();
        std::os::unix::fs::symlink(&target, &checkpoint).unwrap();

        assert!(
            append_checkpoint::load(
                Some(root.path()),
                pending.operation_id(),
                "stage-a",
                MAX_INSERT_FLIGHT_BYTES,
            )
            .await
            .is_err()
        );
    }
}
