// Copyright 2026 Rararulab
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! Fixed-window bridges between Arrow's synchronous IPC implementation and
//! Lake's asynchronous object/result streams.

use std::{
    io::{self, Write},
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use bytes::Bytes;
use datafusion::arrow::{
    buffer::Buffer,
    datatypes::SchemaRef,
    ipc::{MessageHeader, reader::StreamDecoder, root_as_message, writer::StreamWriter},
    record_batch::RecordBatch,
};
use futures::{Stream, stream};
use lake_objects::ObjectReader;
use tokio::{
    io::{AsyncRead, AsyncReadExt, ReadBuf},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::io::StreamReader as AsyncStreamReader;

const MAX_CHANNEL_CAPACITY: usize = 64;
const MAX_CHUNK_BYTES: usize = 1024 * 1024;
const DEFAULT_CHUNK_BYTES: usize = 64 * 1024;
const DEFAULT_BYTE_CHANNEL_CAPACITY: usize = 4;
const DEFAULT_BATCH_CHANNEL_CAPACITY: usize = 2;
const MAX_IPC_MESSAGE_BYTES: usize = 1024 * 1024;
const IPC_CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

#[derive(Clone, Copy, Debug)]
pub(crate) struct IpcPipelineLimits {
    max_encoded_bytes:      u64,
    byte_chunk_bytes:       usize,
    byte_channel_capacity:  usize,
    batch_channel_capacity: usize,
}

impl IpcPipelineLimits {
    pub(crate) fn production(max_encoded_bytes: u64) -> Self {
        Self::try_new(
            max_encoded_bytes,
            DEFAULT_CHUNK_BYTES,
            DEFAULT_BYTE_CHANNEL_CAPACITY,
            DEFAULT_BATCH_CHANNEL_CAPACITY,
        )
        .expect("production IPC pipeline limits are finite")
    }

    pub(crate) fn try_new(
        max_encoded_bytes: u64,
        byte_chunk_bytes: usize,
        byte_channel_capacity: usize,
        batch_channel_capacity: usize,
    ) -> Result<Self, &'static str> {
        if max_encoded_bytes == 0
            || byte_chunk_bytes == 0
            || byte_chunk_bytes > MAX_CHUNK_BYTES
            || !(1..=MAX_CHANNEL_CAPACITY).contains(&byte_channel_capacity)
            || !(1..=MAX_CHANNEL_CAPACITY).contains(&batch_channel_capacity)
        {
            return Err("invalid IPC pipeline limits");
        }
        Ok(Self {
            max_encoded_bytes,
            byte_chunk_bytes,
            byte_channel_capacity,
            batch_channel_capacity,
        })
    }

    pub(crate) const fn encode_window_bytes(&self) -> usize {
        self.byte_chunk_bytes
            .saturating_mul(self.byte_channel_capacity.saturating_add(1))
    }

    #[cfg(test)]
    pub(crate) const fn decode_window_bytes(&self) -> usize {
        self.byte_chunk_bytes
            // The bounded channel, one sender-held chunk waiting for capacity,
            // and one decoder-owned chunk after it frees that capacity can all
            // be live concurrently.
            .saturating_mul(self.byte_channel_capacity.saturating_add(2))
    }

    #[cfg(test)]
    pub(crate) const fn decode_window_batches(&self) -> usize {
        self.batch_channel_capacity.saturating_add(1)
    }
}

#[derive(Clone, Default)]
pub(crate) struct PipelineProbe {
    inner: Option<Arc<PipelineProbeInner>>,
}

#[derive(Default)]
struct PipelineProbeInner {
    encoded_bytes:        AtomicUsize,
    peak_encoded_bytes:   AtomicUsize,
    input_bytes:          AtomicUsize,
    peak_input_bytes:     AtomicUsize,
    decoded_batches:      AtomicUsize,
    peak_decoded_batches: AtomicUsize,
    active_tasks:         AtomicUsize,
    decoder_exit_gate:    Option<Arc<DecoderExitGate>>,
}

#[derive(Default)]
struct DecoderExitGate {
    released: Mutex<bool>,
    changed:  Condvar,
}

#[cfg(test)]
pub(crate) struct DecoderExitRelease(Arc<DecoderExitGate>);

#[cfg(test)]
impl DecoderExitRelease {
    pub(crate) fn release(self) {
        *self.0.released.lock().expect("decoder exit gate") = true;
        self.0.changed.notify_all();
    }
}

impl PipelineProbe {
    #[cfg(test)]
    pub(crate) fn instrumented() -> Self {
        Self {
            inner: Some(Arc::new(PipelineProbeInner::default())),
        }
    }

    #[cfg(test)]
    pub(crate) fn instrumented_with_blocked_decoder() -> (Self, DecoderExitRelease) {
        let gate = Arc::new(DecoderExitGate::default());
        let probe = Self {
            inner: Some(Arc::new(PipelineProbeInner {
                decoder_exit_gate: Some(gate.clone()),
                ..PipelineProbeInner::default()
            })),
        };
        (probe, DecoderExitRelease(gate))
    }

    fn wait_for_decoder_exit(&self) {
        let Some(gate) = self
            .inner
            .as_ref()
            .and_then(|inner| inner.decoder_exit_gate.as_ref())
        else {
            return;
        };
        let mut released = gate.released.lock().expect("decoder exit gate");
        while !*released {
            released = gate.changed.wait(released).expect("decoder exit gate");
        }
    }

    fn encoded_enqueued(&self, bytes: usize) {
        let Some(inner) = &self.inner else { return };
        let live = inner.encoded_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        inner.peak_encoded_bytes.fetch_max(live, Ordering::Relaxed);
    }

    fn encoded_dequeued(&self, bytes: usize) {
        if let Some(inner) = &self.inner {
            inner.encoded_bytes.fetch_sub(bytes, Ordering::Relaxed);
        }
    }

    fn input_enqueued(&self, bytes: usize) {
        let Some(inner) = &self.inner else { return };
        let live = inner.input_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        inner.peak_input_bytes.fetch_max(live, Ordering::Relaxed);
    }

    fn input_dequeued(&self, bytes: usize) {
        if let Some(inner) = &self.inner {
            inner.input_bytes.fetch_sub(bytes, Ordering::Relaxed);
        }
    }

    fn batch_enqueued(&self) {
        let Some(inner) = &self.inner else { return };
        let live = inner.decoded_batches.fetch_add(1, Ordering::Relaxed) + 1;
        inner
            .peak_decoded_batches
            .fetch_max(live, Ordering::Relaxed);
    }

    fn batch_dequeued(&self) {
        if let Some(inner) = &self.inner {
            inner.decoded_batches.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn task_guard(&self) -> ActiveTaskGuard {
        if let Some(inner) = &self.inner {
            inner.active_tasks.fetch_add(1, Ordering::Relaxed);
        }
        ActiveTaskGuard(self.clone())
    }

    #[cfg(test)]
    pub(crate) fn current_encoded_bytes(&self) -> usize {
        self.inner
            .as_ref()
            .expect("instrumented probe")
            .encoded_bytes
            .load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn peak_encoded_bytes(&self) -> usize {
        self.inner
            .as_ref()
            .expect("instrumented probe")
            .peak_encoded_bytes
            .load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn peak_input_bytes(&self) -> usize {
        self.inner
            .as_ref()
            .expect("instrumented probe")
            .peak_input_bytes
            .load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn peak_decoded_batches(&self) -> usize {
        self.inner
            .as_ref()
            .expect("instrumented probe")
            .peak_decoded_batches
            .load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn active_tasks(&self) -> usize {
        self.inner
            .as_ref()
            .expect("instrumented probe")
            .active_tasks
            .load(Ordering::Relaxed)
    }
}

struct ActiveTaskGuard(PipelineProbe);

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        if let Some(inner) = &self.0.inner {
            inner.active_tasks.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

struct ChannelWriter {
    sender:  mpsc::Sender<Bytes>,
    limits:  IpcPipelineLimits,
    written: u64,
    probe:   PipelineProbe,
}

impl Write for ChannelWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let remaining = self.limits.max_encoded_bytes.saturating_sub(self.written);
        if remaining == 0 {
            return Err(io::Error::other("encoded IPC part exceeds byte limit"));
        }
        let length = buffer
            .len()
            .min(self.limits.byte_chunk_bytes)
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let chunk = Bytes::copy_from_slice(&buffer[..length]);
        self.probe.encoded_enqueued(length);
        if self.sender.blocking_send(chunk).is_err() {
            self.probe.encoded_dequeued(length);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "IPC upload reader was dropped",
            ));
        }
        self.written += length as u64;
        Ok(length)
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

struct EncodedState {
    receiver: mpsc::Receiver<Bytes>,
    encoder:  Option<JoinHandle<io::Result<()>>>,
    probe:    PipelineProbe,
    terminal: bool,
}

struct PinnedAsyncReader<R> {
    inner: Pin<Box<R>>,
}

impl<R: AsyncRead> AsyncRead for PinnedAsyncReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.inner.as_mut().poll_read(context, buffer)
    }
}

impl Drop for EncodedState {
    fn drop(&mut self) {
        self.receiver.close();
        if let Some(encoder) = &self.encoder {
            encoder.abort();
        }
    }
}

pub(crate) fn encoded_batch_reader(
    batch: RecordBatch,
    limits: IpcPipelineLimits,
    probe: PipelineProbe,
) -> ObjectReader {
    let (sender, receiver) = mpsc::channel(limits.byte_channel_capacity);
    let writer_probe = probe.clone();
    let encoder = tokio::task::spawn_blocking(move || {
        let writer = ChannelWriter {
            sender,
            limits,
            written: 0,
            probe: writer_probe,
        };
        let mut ipc = StreamWriter::try_new(writer, &batch.schema()).map_err(ipc_error)?;
        ipc.write(&batch).map_err(ipc_error)?;
        ipc.finish().map_err(ipc_error)
    });
    let state = EncodedState {
        receiver,
        encoder: Some(encoder),
        probe,
        terminal: false,
    };
    let chunks = stream::unfold(state, |mut state| async move {
        if state.terminal {
            return None;
        }
        if let Some(chunk) = state.receiver.recv().await {
            state.probe.encoded_dequeued(chunk.len());
            return Some((Ok::<Bytes, io::Error>(chunk), state));
        }
        state.terminal = true;
        let result = state
            .encoder
            .take()
            .expect("encoder exists until channel termination")
            .await;
        match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some((Err(error), state)),
            Err(_) => Some((Err(io::Error::other("IPC encoder task failed")), state)),
        }
    });
    Box::pin(PinnedAsyncReader {
        inner: Box::pin(AsyncStreamReader::new(chunks)),
    })
}

fn ipc_error(error: datafusion::arrow::error::ArrowError) -> io::Error {
    io::Error::other(format!("IPC encoding failed: {error}"))
}

pub(crate) struct DecodedBatchReceiver {
    receiver: mpsc::Receiver<io::Result<RecordBatch>>,
    probe:    PipelineProbe,
}

struct InputReceiver {
    receiver: mpsc::Receiver<io::Result<Bytes>>,
    probe:    PipelineProbe,
}

impl InputReceiver {
    fn blocking_recv(&mut self) -> Option<io::Result<Bytes>> { self.receiver.blocking_recv() }
}

impl Drop for InputReceiver {
    fn drop(&mut self) {
        while let Ok(item) = self.receiver.try_recv() {
            if let Ok(chunk) = item {
                self.probe.input_dequeued(chunk.len());
            }
        }
        self.receiver.close();
    }
}

impl DecodedBatchReceiver {
    pub(crate) async fn recv(&mut self) -> Option<io::Result<RecordBatch>> {
        let item = self.receiver.recv().await;
        if item.as_ref().is_some_and(Result::is_ok) {
            self.probe.batch_dequeued();
        }
        item
    }

    pub(crate) fn into_stream(self) -> impl Stream<Item = io::Result<RecordBatch>> + Send {
        stream::unfold(self, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        })
    }
}

impl Drop for DecodedBatchReceiver {
    fn drop(&mut self) {
        while let Ok(item) = self.receiver.try_recv() {
            if item.is_ok() {
                self.probe.batch_dequeued();
            }
        }
        self.receiver.close();
    }
}

pub(crate) struct IpcDecodeGuard {
    pump:    JoinHandle<()>,
    decoder: JoinHandle<()>,
}

impl Drop for IpcDecodeGuard {
    fn drop(&mut self) {
        self.pump.abort();
        self.decoder.abort();
    }
}

pub(crate) struct DecodedIpc {
    schema:  SchemaRef,
    batches: Option<DecodedBatchReceiver>,
    guard:   Option<IpcDecodeGuard>,
}

impl DecodedIpc {
    #[cfg(test)]
    pub(crate) fn batches_mut(&mut self) -> &mut DecodedBatchReceiver {
        self.batches.as_mut().expect("decoded batches exist")
    }

    pub(crate) fn into_parts(mut self) -> (SchemaRef, DecodedBatchReceiver, IpcDecodeGuard) {
        (
            self.schema.clone(),
            self.batches.take().expect("decoded batches exist"),
            self.guard.take().expect("decode guard exists"),
        )
    }
}

pub(crate) async fn decode_ipc_reader<K>(
    reader: ObjectReader,
    expected_bytes: u64,
    limits: IpcPipelineLimits,
    probe: PipelineProbe,
    decoder_keepalive: K,
) -> io::Result<DecodedIpc>
where
    K: Send + 'static,
{
    if expected_bytes == 0 || expected_bytes > limits.max_encoded_bytes {
        return Err(decode_error());
    }
    let (input_sender, input_receiver) = mpsc::channel(limits.byte_channel_capacity);
    let (batch_sender, batch_receiver) = mpsc::channel(limits.batch_channel_capacity);
    let (schema_sender, schema_receiver) = oneshot::channel();
    let pump_probe = probe.clone();
    let pump = tokio::spawn(async move {
        let _active = pump_probe.task_guard();
        pump_input(reader, expected_bytes, limits, input_sender, pump_probe).await;
    });
    let decoder_probe = probe.clone();
    let decoder = tokio::task::spawn_blocking(move || {
        let _active = decoder_probe.task_guard();
        decode_batches(
            input_receiver,
            schema_sender,
            batch_sender,
            limits,
            decoder_probe.clone(),
        );
        decoder_probe.wait_for_decoder_exit();
        drop(decoder_keepalive);
    });
    let guard = IpcDecodeGuard { pump, decoder };
    let schema = match schema_receiver.await {
        Ok(Ok(schema)) => schema,
        Ok(Err(error)) => return Err(error),
        Err(_) => return Err(decode_error()),
    };
    Ok(DecodedIpc {
        schema,
        batches: Some(DecodedBatchReceiver {
            receiver: batch_receiver,
            probe,
        }),
        guard: Some(guard),
    })
}

async fn pump_input(
    mut reader: ObjectReader,
    expected_bytes: u64,
    limits: IpcPipelineLimits,
    sender: mpsc::Sender<io::Result<Bytes>>,
    probe: PipelineProbe,
) {
    let mut buffer = vec![0_u8; limits.byte_chunk_bytes];
    let mut total = 0_u64;
    loop {
        let read = match reader.read(&mut buffer).await {
            Ok(read) => read,
            Err(_) => {
                let _ = sender.send(Err(decode_error())).await;
                return;
            }
        };
        if read == 0 {
            if total != expected_bytes {
                let _ = sender.send(Err(decode_error())).await;
            }
            return;
        }
        total = match total.checked_add(read as u64) {
            Some(total) if total <= expected_bytes => total,
            _ => {
                let _ = sender.send(Err(decode_error())).await;
                return;
            }
        };
        let chunk = Bytes::copy_from_slice(&buffer[..read]);
        probe.input_enqueued(read);
        if sender.send(Ok(chunk)).await.is_err() {
            probe.input_dequeued(read);
            return;
        }
    }
}

fn decode_batches(
    input: mpsc::Receiver<io::Result<Bytes>>,
    schema_sender: oneshot::Sender<io::Result<SchemaRef>>,
    batch_sender: mpsc::Sender<io::Result<RecordBatch>>,
    limits: IpcPipelineLimits,
    probe: PipelineProbe,
) {
    let mut input = InputReceiver {
        receiver: input,
        probe:    probe.clone(),
    };
    let mut decoder = StreamDecoder::new();
    let mut validator = IpcSafetyValidator::new(limits.max_encoded_bytes);
    let mut schema_sender = Some(schema_sender);
    while let Some(item) = input.blocking_recv() {
        let chunk = match item {
            Ok(chunk) => chunk,
            Err(_) => {
                send_decode_error(&mut schema_sender, &batch_sender);
                return;
            }
        };
        probe.input_dequeued(chunk.len());
        if validator.validate(&chunk).is_err() {
            send_decode_error(&mut schema_sender, &batch_sender);
            return;
        }
        let mut buffer = Buffer::from(chunk);
        while !buffer.is_empty() {
            let batch = match decoder.decode(&mut buffer) {
                Ok(batch) => batch,
                Err(_) => {
                    send_decode_error(&mut schema_sender, &batch_sender);
                    return;
                }
            };
            if let Some(sender) = schema_sender.take() {
                if let Some(schema) = decoder.schema() {
                    if sender.send(Ok(schema)).is_err() {
                        return;
                    }
                } else {
                    schema_sender = Some(sender);
                }
            }
            if let Some(batch) = batch {
                probe.batch_enqueued();
                if batch_sender.blocking_send(Ok(batch)).is_err() {
                    probe.batch_dequeued();
                    return;
                }
            }
        }
    }
    if decoder.finish().is_err() || schema_sender.is_some() {
        send_decode_error(&mut schema_sender, &batch_sender);
    }
}

enum IpcFramingState {
    Header {
        bytes:        [u8; 4],
        read:         usize,
        continuation: bool,
    },
    Message {
        size:  usize,
        bytes: Vec<u8>,
    },
    Body {
        remaining: usize,
    },
    Finished,
}

impl Default for IpcFramingState {
    fn default() -> Self {
        Self::Header {
            bytes:        [0; 4],
            read:         0,
            continuation: false,
        }
    }
}

struct IpcSafetyValidator {
    state:          IpcFramingState,
    max_body_bytes: u64,
}

impl IpcSafetyValidator {
    fn new(max_body_bytes: u64) -> Self {
        Self {
            state: IpcFramingState::default(),
            max_body_bytes,
        }
    }

    fn validate(&mut self, mut input: &[u8]) -> io::Result<()> {
        while !input.is_empty() {
            match &mut self.state {
                IpcFramingState::Header {
                    bytes,
                    read,
                    continuation,
                } => {
                    let take = input.len().min(bytes.len() - *read);
                    bytes[*read..*read + take].copy_from_slice(&input[..take]);
                    *read += take;
                    input = &input[take..];
                    if *read != bytes.len() {
                        continue;
                    }
                    if !*continuation && *bytes == IPC_CONTINUATION_MARKER {
                        *read = 0;
                        *continuation = true;
                        continue;
                    }
                    let size = u32::from_le_bytes(*bytes) as usize;
                    if size == 0 {
                        self.state = IpcFramingState::Finished;
                    } else if size > MAX_IPC_MESSAGE_BYTES {
                        return Err(decode_error());
                    } else {
                        self.state = IpcFramingState::Message {
                            size,
                            bytes: Vec::with_capacity(size),
                        };
                    }
                }
                IpcFramingState::Message { size, bytes } => {
                    let take = input.len().min(*size - bytes.len());
                    bytes.extend_from_slice(&input[..take]);
                    input = &input[take..];
                    if bytes.len() != *size {
                        continue;
                    }
                    let message = root_as_message(bytes).map_err(|_| decode_error())?;
                    let body_length =
                        u64::try_from(message.bodyLength()).map_err(|_| decode_error())?;
                    if body_length > self.max_body_bytes || message_uses_compression(&message)? {
                        return Err(decode_error());
                    }
                    self.state = if body_length == 0 {
                        IpcFramingState::default()
                    } else {
                        IpcFramingState::Body {
                            remaining: body_length as usize,
                        }
                    };
                }
                IpcFramingState::Body { remaining } => {
                    let take = input.len().min(*remaining);
                    *remaining -= take;
                    input = &input[take..];
                    if *remaining == 0 {
                        self.state = IpcFramingState::default();
                    }
                }
                IpcFramingState::Finished => return Err(decode_error()),
            }
        }
        Ok(())
    }
}

fn message_uses_compression(message: &datafusion::arrow::ipc::Message<'_>) -> io::Result<bool> {
    match message.header_type() {
        MessageHeader::RecordBatch => message
            .header_as_record_batch()
            .map(|batch| batch.compression().is_some())
            .ok_or_else(decode_error),
        MessageHeader::DictionaryBatch => message
            .header_as_dictionary_batch()
            .and_then(|dictionary| dictionary.data())
            .map(|batch| batch.compression().is_some())
            .ok_or_else(decode_error),
        _ => Ok(false),
    }
}

fn send_decode_error(
    schema_sender: &mut Option<oneshot::Sender<io::Result<SchemaRef>>>,
    batch_sender: &mpsc::Sender<io::Result<RecordBatch>>,
) {
    if let Some(sender) = schema_sender.take() {
        let _ = sender.send(Err(decode_error()));
    } else {
        let _ = batch_sender.blocking_send(Err(decode_error()));
    }
}

fn decode_error() -> io::Error { io::Error::other("IPC decoding failed") }

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc, time::Duration};

    use datafusion::arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        ipc::{
            CompressionType,
            writer::{IpcWriteOptions, StreamWriter},
        },
        record_batch::RecordBatch,
    };
    use lake_objects::{LocalObjectStore, ManagedObjectScope, ManagedObjectStore};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{IpcPipelineLimits, PipelineProbe, decode_ipc_reader, encoded_batch_reader};

    fn string_batch(value: &str) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec![value])) as ArrayRef],
        )
        .unwrap()
    }

    fn integer_batch(value: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef],
        )
        .unwrap()
    }

    fn encoded_batches(values: &[i64]) -> (Vec<u8>, usize) {
        let first = integer_batch(values[0]);
        let mut writer = StreamWriter::try_new(Vec::new(), &first.schema()).unwrap();
        writer.write(&first).unwrap();
        let first_end = writer.get_ref().len();
        for value in &values[1..] {
            writer.write(&integer_batch(*value)).unwrap();
        }
        writer.finish().unwrap();
        (writer.into_inner().unwrap(), first_end)
    }

    #[tokio::test]
    async fn async_part_encoder_rejects_encoded_overflow_without_publication() {
        let limits = IpcPipelineLimits::try_new(256, 64, 2, 2).unwrap();
        let probe = PipelineProbe::instrumented();
        let reader = encoded_batch_reader(string_batch(&"x".repeat(4_096)), limits, probe);
        let root = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(root.path()).await.unwrap();
        let scope = ManagedObjectScope::try_new("tenant-a", "query-a").unwrap();

        let result = store
            .put_scoped_reader(
                &scope,
                "part",
                reader,
                "application/vnd.apache.arrow.stream".to_owned(),
            )
            .await;

        assert!(result.is_err());
        assert!(regular_files(root.path()).is_empty());
    }

    #[tokio::test]
    async fn async_part_encoder_backpressure_bounds_live_bytes() {
        let limits = IpcPipelineLimits::try_new(64 * 1024, 128, 2, 2).unwrap();
        let probe = PipelineProbe::instrumented();
        let mut reader =
            encoded_batch_reader(string_batch(&"x".repeat(16 * 1024)), limits, probe.clone());
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        assert!(probe.peak_encoded_bytes() > 0);
        assert!(probe.peak_encoded_bytes() <= limits.encode_window_bytes());

        let mut received = Vec::new();
        reader.read_to_end(&mut received).await.unwrap();
        assert!(!received.is_empty());
        assert_eq!(probe.current_encoded_bytes(), 0);
    }

    #[tokio::test]
    async fn async_result_decoder_streams_before_object_eof() {
        let (encoded, first_end) = encoded_batches(&[1, 2]);
        let expected_bytes = encoded.len() as u64;
        let (mut writer, reader) = tokio::io::duplex(64 * 1024);
        let release = Arc::new(tokio::sync::Notify::new());
        let released = release.clone();
        tokio::spawn(async move {
            writer.write_all(&encoded[..first_end]).await.unwrap();
            released.notified().await;
            writer.write_all(&encoded[first_end..]).await.unwrap();
        });
        let limits = IpcPipelineLimits::try_new(expected_bytes, 128, 2, 2).unwrap();
        let mut decoded = decode_ipc_reader(
            Box::pin(reader),
            expected_bytes,
            limits,
            PipelineProbe::instrumented(),
            (),
        )
        .await
        .unwrap();

        let first = tokio::time::timeout(Duration::from_secs(1), decoded.batches_mut().recv())
            .await
            .expect("first batch arrives before object EOF")
            .expect("batch channel remains open")
            .unwrap();
        assert_eq!(
            first
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        release.notify_one();
        assert_eq!(
            decoded
                .batches_mut()
                .recv()
                .await
                .unwrap()
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
    }

    #[tokio::test]
    async fn async_result_decoder_backpressure_bounds_live_data() {
        let (encoded, _) = encoded_batches(&(0..16).collect::<Vec<_>>());
        let expected_bytes = encoded.len() as u64;
        let limits = IpcPipelineLimits::try_new(expected_bytes, 128, 2, 2).unwrap();
        let probe = PipelineProbe::instrumented();
        let mut decoded = decode_ipc_reader(
            Box::pin(Cursor::new(encoded)),
            expected_bytes,
            limits,
            probe.clone(),
            (),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(probe.peak_decoded_batches() <= limits.decode_window_batches());
        assert!(
            probe.peak_input_bytes() <= limits.decode_window_bytes(),
            "input peak {} exceeded window {}",
            probe.peak_input_bytes(),
            limits.decode_window_bytes()
        );

        let mut batches = 0;
        while let Some(batch) = decoded.batches_mut().recv().await {
            batch.unwrap();
            batches += 1;
        }
        assert_eq!(batches, 16);
    }

    #[tokio::test]
    async fn async_result_pipeline_drop_cancels_owned_tasks() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let (prefix, _) = encoded_batches(&[1]);
        writer.write_all(&prefix[..4]).await.unwrap();
        let probe = PipelineProbe::instrumented();
        let decode = decode_ipc_reader(
            Box::pin(reader),
            prefix.len() as u64,
            IpcPipelineLimits::try_new(prefix.len() as u64, 64, 1, 1).unwrap(),
            probe.clone(),
            (),
        );
        let mut decode = Box::pin(decode);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), decode.as_mut())
                .await
                .is_err()
        );
        drop(decode);
        drop(writer);

        tokio::time::timeout(Duration::from_secs(1), async {
            while probe.active_tasks() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reader pump and decoder terminate after drop");
    }

    #[tokio::test]
    async fn async_result_invalid_ipc_fails_bounded_and_redacted() {
        let error = decode_ipc_reader(
            Box::pin(Cursor::new(b"not-arrow-ipc".to_vec())),
            13,
            IpcPipelineLimits::try_new(13, 8, 1, 1).unwrap(),
            PipelineProbe::instrumented(),
            (),
        )
        .await
        .err()
        .expect("invalid IPC must fail before returning a stream");

        let message = error.to_string();
        assert!(message.contains("IPC decoding failed"));
        assert!(!message.contains("tenant"));
        assert!(!message.contains("query"));
        assert!(!message.contains("uri"));

        let batch = string_batch(&"compressible".repeat(1_024));
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))
            .unwrap();
        let mut writer =
            StreamWriter::try_new_with_options(Vec::new(), &batch.schema(), options).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        let compressed = writer.into_inner().unwrap();
        let compressed_len = compressed.len() as u64;
        let mut decoded = decode_ipc_reader(
            Box::pin(Cursor::new(compressed)),
            compressed_len,
            IpcPipelineLimits::try_new(compressed_len, 64, 1, 1).unwrap(),
            PipelineProbe::instrumented(),
            (),
        )
        .await
        .expect("the schema precedes the compressed batch");
        let error = decoded
            .batches_mut()
            .recv()
            .await
            .expect("compressed IPC emits a terminal error")
            .expect_err("compressed IPC must be rejected before decompression");
        assert_eq!(error.to_string(), "IPC decoding failed");
    }

    fn regular_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut pending = vec![root.to_path_buf()];
        let mut files = Vec::new();
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    files.push(path);
                }
            }
        }
        files
    }
}
