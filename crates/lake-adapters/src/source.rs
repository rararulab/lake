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

//! Random-access source and pre-I/O budget enforcement.

use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use snafu::Snafu;

use crate::{AdapterError, BudgetResource};

/// A transport-neutral failure from a caller-owned random-access source.
#[derive(Debug, Snafu)]
#[snafu(display("{message}"))]
pub struct SourceError {
    message: String,
}

impl SourceError {
    /// Create a redaction-safe source error from a caller-controlled message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Caller-owned source capable of serving exact byte ranges.
#[async_trait]
pub trait RandomAccessSource: Send + Sync {
    /// Return the immutable source length in bytes.
    async fn size_bytes(&self) -> Result<u64, SourceError>;

    /// Return exactly the requested half-open byte range.
    async fn read_range(&self, range: Range<u64>) -> Result<Bytes, SourceError>;
}

/// Finite limits for one metadata extraction attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadBudget {
    bytes:               u64,
    requests:            u64,
    fallback_scan_bytes: u64,
    record_bytes:        u64,
}

impl ReadBudget {
    /// Validate and construct explicit finite limits.
    pub fn try_new(
        max_bytes: u64,
        max_requests: u64,
        max_fallback_scan_bytes: u64,
        max_record_bytes: u64,
    ) -> Result<Self, AdapterError> {
        for (field, value) in [
            ("max_bytes", max_bytes),
            ("max_requests", max_requests),
            ("max_fallback_scan_bytes", max_fallback_scan_bytes),
            ("max_record_bytes", max_record_bytes),
        ] {
            if value == 0 {
                return Err(AdapterError::InvalidBudget { field });
            }
        }
        Ok(Self {
            bytes:               max_bytes,
            requests:            max_requests,
            fallback_scan_bytes: max_fallback_scan_bytes,
            record_bytes:        max_record_bytes,
        })
    }

    /// Return the cumulative byte ceiling for source range reads.
    #[must_use]
    pub const fn max_bytes(self) -> u64 { self.bytes }

    /// Return the cumulative random-access request ceiling.
    #[must_use]
    pub const fn max_requests(self) -> u64 { self.requests }

    /// Return the largest source eligible for an index-free linear fallback.
    #[must_use]
    pub const fn max_fallback_scan_bytes(self) -> u64 { self.fallback_scan_bytes }

    /// Return the largest single upstream record or index payload accepted.
    #[must_use]
    pub const fn max_record_bytes(self) -> u64 { self.record_bytes }
}

pub(crate) struct BudgetedSource<'a> {
    source:        &'a dyn RandomAccessSource,
    budget:        ReadBudget,
    used_bytes:    u64,
    used_requests: u64,
}

impl<'a> BudgetedSource<'a> {
    pub(crate) const fn new(source: &'a dyn RandomAccessSource, budget: ReadBudget) -> Self {
        Self {
            source,
            budget,
            used_bytes: 0,
            used_requests: 0,
        }
    }

    pub(crate) async fn size_bytes(&self) -> Result<u64, AdapterError> {
        self.source
            .size_bytes()
            .await
            .map_err(|source| AdapterError::Source { source })
    }

    pub(crate) async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes, AdapterError> {
        if range.start >= range.end {
            return Err(AdapterError::InvalidRange {
                start: range.start,
                end:   range.end,
            });
        }
        let requested = range
            .end
            .checked_sub(range.start)
            .ok_or(AdapterError::InvalidRange {
                start: range.start,
                end:   range.end,
            })?;
        let attempted_requests =
            self.used_requests
                .checked_add(1)
                .ok_or(AdapterError::BudgetExceeded {
                    resource:  BudgetResource::Requests,
                    limit:     self.budget.max_requests(),
                    attempted: u64::MAX,
                })?;
        if attempted_requests > self.budget.max_requests() {
            return Err(AdapterError::BudgetExceeded {
                resource:  BudgetResource::Requests,
                limit:     self.budget.max_requests(),
                attempted: attempted_requests,
            });
        }
        let attempted_bytes =
            self.used_bytes
                .checked_add(requested)
                .ok_or(AdapterError::BudgetExceeded {
                    resource:  BudgetResource::Bytes,
                    limit:     self.budget.max_bytes(),
                    attempted: u64::MAX,
                })?;
        if attempted_bytes > self.budget.max_bytes() {
            return Err(AdapterError::BudgetExceeded {
                resource:  BudgetResource::Bytes,
                limit:     self.budget.max_bytes(),
                attempted: attempted_bytes,
            });
        }

        self.used_requests = attempted_requests;
        self.used_bytes = attempted_bytes;
        let bytes = self
            .source
            .read_range(range.clone())
            .await
            .map_err(|source| AdapterError::Source { source })?;
        let actual = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if actual != requested {
            return Err(AdapterError::ShortRead {
                start: range.start,
                end: range.end,
                expected: requested,
                actual,
            });
        }
        Ok(bytes)
    }

    pub(crate) const fn budget(&self) -> ReadBudget { self.budget }
}

#[cfg(test)]
mod tests {
    use std::{
        ops::Range,
        sync::atomic::{AtomicU64, Ordering},
    };

    use async_trait::async_trait;
    use bytes::Bytes;

    use super::BudgetedSource;
    use crate::{AdapterError, BudgetResource, RandomAccessSource, ReadBudget, SourceError};

    #[derive(Debug)]
    struct CountingSource {
        bytes:        Bytes,
        requests:     AtomicU64,
        returned:     AtomicU64,
        truncate_one: bool,
    }

    impl CountingSource {
        fn new(bytes: impl Into<Bytes>) -> Self {
            Self {
                bytes:        bytes.into(),
                requests:     AtomicU64::new(0),
                returned:     AtomicU64::new(0),
                truncate_one: false,
            }
        }

        fn truncating(bytes: impl Into<Bytes>) -> Self {
            Self {
                truncate_one: true,
                ..Self::new(bytes)
            }
        }
    }

    #[async_trait]
    impl RandomAccessSource for CountingSource {
        async fn size_bytes(&self) -> Result<u64, SourceError> {
            Ok(u64::try_from(self.bytes.len()).expect("fixture length fits u64"))
        }

        async fn read_range(&self, range: Range<u64>) -> Result<Bytes, SourceError> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            let start = usize::try_from(range.start).expect("test range start fits usize");
            let mut end = usize::try_from(range.end).expect("test range end fits usize");
            if self.truncate_one && end > start {
                end -= 1;
            }
            let bytes = self.bytes.slice(start..end);
            self.returned.fetch_add(
                u64::try_from(bytes.len()).expect("fixture length fits u64"),
                Ordering::SeqCst,
            );
            Ok(bytes)
        }
    }

    fn budget(max_bytes: u64, max_requests: u64) -> ReadBudget {
        ReadBudget::try_new(max_bytes, max_requests, max_bytes, max_bytes)
            .expect("test budget is valid")
    }

    fn range(start: u64, end: u64) -> Range<u64> { start..end }

    #[tokio::test]
    async fn exact_budget_passes_and_next_request_is_rejected_before_io() {
        let source = CountingSource::new(Bytes::from_static(b"abcdefgh"));
        let mut bounded = BudgetedSource::new(&source, budget(4, 1));

        assert_eq!(
            bounded.read_range(2..6).await.expect("exact budget"),
            Bytes::from_static(b"cdef")
        );
        let error = bounded
            .read_range(6..7)
            .await
            .expect_err("second request must exceed request budget");

        assert!(matches!(
            error,
            AdapterError::BudgetExceeded {
                resource:  BudgetResource::Requests,
                limit:     1,
                attempted: 2,
            }
        ));
        assert_eq!(source.requests.load(Ordering::SeqCst), 1);
        assert_eq!(source.returned.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn one_byte_short_budget_never_delegates_the_read() {
        let source = CountingSource::new(Bytes::from_static(b"abcdefgh"));
        let mut bounded = BudgetedSource::new(&source, budget(3, 1));

        let error = bounded
            .read_range(2..6)
            .await
            .expect_err("four-byte range must exceed three-byte budget");

        assert!(matches!(
            error,
            AdapterError::BudgetExceeded {
                resource:  BudgetResource::Bytes,
                limit:     3,
                attempted: 4,
            }
        ));
        assert_eq!(source.requests.load(Ordering::SeqCst), 0);
        assert_eq!(source.returned.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn short_reads_fail_after_charging_the_exact_request() {
        let source = CountingSource::truncating(Bytes::from_static(b"abcdefgh"));
        let mut bounded = BudgetedSource::new(&source, budget(4, 1));

        let error = bounded
            .read_range(2..6)
            .await
            .expect_err("short source response must fail closed");

        assert!(matches!(
            error,
            AdapterError::ShortRead {
                start:    2,
                end:      6,
                expected: 4,
                actual:   3,
            }
        ));
        assert_eq!(source.requests.load(Ordering::SeqCst), 1);
        assert_eq!(source.returned.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn invalid_ranges_fail_before_source_io() {
        let source = CountingSource::new(Bytes::from_static(b"abcdefgh"));
        let mut bounded = BudgetedSource::new(&source, budget(8, 2));

        let reversed = bounded
            .read_range(range(6, 2))
            .await
            .expect_err("reversed range must fail");
        let empty = bounded
            .read_range(2..2)
            .await
            .expect_err("empty range must fail");

        assert!(matches!(
            reversed,
            AdapterError::InvalidRange { start: 6, end: 2 }
        ));
        assert!(matches!(
            empty,
            AdapterError::InvalidRange { start: 2, end: 2 }
        ));
        assert_eq!(source.requests.load(Ordering::SeqCst), 0);
    }
}
