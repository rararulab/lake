use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use lake_common::DataLocation;
use sha2::{Digest, Sha256};
use snafu::Snafu;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf, Take};

use crate::{ManagedObjectStore, ObjectError, ObjectReader, Result};

/// A typed reason why streamed bytes did not match their immutable identity.
#[derive(Clone, Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ObjectIntegrityError {
    #[snafu(display("DataLocation SHA-256 is not exactly 64 hexadecimal characters"))]
    InvalidSha256,

    #[snafu(display("object ended at {actual} bytes; DataLocation declares {expected}"))]
    PrematureEof { expected: u64, actual: u64 },

    #[snafu(display("object exceeds the {expected}-byte size declared by DataLocation"))]
    SizeExceeded { expected: u64 },

    #[snafu(display("object SHA-256 {actual} does not match DataLocation SHA-256 {expected}"))]
    Sha256Mismatch { expected: String, actual: String },
}

struct ExpectedIntegrity {
    size_bytes: u64,
    sha256:     String,
}

impl TryFrom<&DataLocation> for ExpectedIntegrity {
    type Error = ObjectIntegrityError;

    fn try_from(location: &DataLocation) -> std::result::Result<Self, Self::Error> {
        if location.sha256.len() != 64
            || !location.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(ObjectIntegrityError::InvalidSha256);
        }
        Ok(Self {
            size_bytes: location.size_bytes,
            sha256:     location.sha256.to_ascii_lowercase(),
        })
    }
}

enum TerminalState {
    Reading,
    Verified,
    Failed(ObjectIntegrityError),
}

struct IntegrityReader {
    inner:      Take<ObjectReader>,
    expected:   ExpectedIntegrity,
    hasher:     Sha256,
    bytes_read: u64,
    terminal:   TerminalState,
}

impl IntegrityReader {
    fn new(inner: ObjectReader, expected: ExpectedIntegrity) -> Self {
        let size_bytes = expected.size_bytes;
        Self {
            inner: inner.take(size_bytes),
            expected,
            hasher: Sha256::new(),
            bytes_read: 0,
            terminal: TerminalState::Reading,
        }
    }

    fn fail(&mut self, error: ObjectIntegrityError) -> Poll<io::Result<()>> {
        self.terminal = TerminalState::Failed(error.clone());
        Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, error)))
    }

    fn verify_hash(&mut self) -> Poll<io::Result<()>> {
        let actual = format!("{:x}", self.hasher.clone().finalize());
        if actual != self.expected.sha256 {
            return self.fail(ObjectIntegrityError::Sha256Mismatch {
                expected: self.expected.sha256.clone(),
                actual,
            });
        }
        self.terminal = TerminalState::Verified;
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for IntegrityReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match &this.terminal {
            TerminalState::Verified => return Poll::Ready(Ok(())),
            TerminalState::Failed(error) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    error.clone(),
                )));
            }
            TerminalState::Reading => {}
        }

        if this.inner.limit() == 0 {
            let mut probe = [0_u8; 1];
            let mut probe_buf = ReadBuf::new(&mut probe);
            match Pin::new(&mut this.inner)
                .get_pin_mut()
                .poll_read(cx, &mut probe_buf)
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) if !probe_buf.filled().is_empty() => {
                    return this.fail(ObjectIntegrityError::SizeExceeded {
                        expected: this.expected.size_bytes,
                    });
                }
                Poll::Ready(Ok(())) => return this.verify_hash(),
            }
        }

        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let before = output.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, output) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {
                let bytes = &output.filled()[before..];
                if bytes.is_empty() {
                    return this.fail(ObjectIntegrityError::PrematureEof {
                        expected: this.expected.size_bytes,
                        actual:   this.bytes_read,
                    });
                }
                this.hasher.update(bytes);
                this.bytes_read = this
                    .bytes_read
                    .checked_add(bytes.len() as u64)
                    .expect("Tokio Take caps bytes at the u64 DataLocation size");
                Poll::Ready(Ok(()))
            }
        }
    }
}

/// Open a managed object and verify its declared size and SHA-256 at EOF.
///
/// The expected identity is validated before storage I/O. The wrapper keeps
/// constant memory and reports success only when the caller drains it to EOF.
pub async fn open_verified(
    store: &dyn ManagedObjectStore,
    location: &DataLocation,
) -> Result<ObjectReader> {
    let expected = ExpectedIntegrity::try_from(location)
        .map_err(|source| ObjectError::Integrity { source })?;
    let inner = store.open_reader(location).await?;
    Ok(Box::pin(IntegrityReader::new(inner, expected)))
}
