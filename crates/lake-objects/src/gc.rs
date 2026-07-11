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

//! Bounded merge planning for managed-object garbage collection.

use std::iter::Peekable;

use lake_common::ObjectIdentity;
use serde::{Deserialize, Serialize};

use crate::{ObjectError, Result};

const MAX_PLAN_PAGE_SIZE: usize = 1_024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectCandidate {
    pub uri:              String,
    pub size_bytes:       u64,
    pub last_modified_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcPlanPage {
    candidates: Vec<ObjectCandidate>,
}

impl GcPlanPage {
    #[must_use]
    pub fn candidates(&self) -> &[ObjectCandidate] { &self.candidates }
}

#[derive(Clone, Debug)]
pub struct GcPlanner {
    managed_prefix: String,
    cutoff_ms:      u64,
    page_size:      usize,
}

impl GcPlanner {
    pub fn try_new(
        managed_prefix: impl Into<String>,
        cutoff_ms: u64,
        page_size: usize,
        lineage_complete: bool,
    ) -> Result<Self> {
        if !lineage_complete {
            return Err(ObjectError::GcLineageIncomplete);
        }
        let managed_prefix = managed_prefix.into();
        if !managed_prefix.ends_with('/') {
            return Err(ObjectError::InvalidGcConfig {
                message: "managed URI prefix must end with '/'".to_owned(),
            });
        }
        if page_size == 0 || page_size > MAX_PLAN_PAGE_SIZE {
            return Err(ObjectError::InvalidGcConfig {
                message: format!("page size must be within 1..={MAX_PLAN_PAGE_SIZE}"),
            });
        }
        Ok(Self {
            managed_prefix,
            cutoff_ms,
            page_size,
        })
    }

    pub fn plan<C, L>(&self, candidates: C, live: L) -> GcPlanPages<C::IntoIter, L::IntoIter>
    where
        C: IntoIterator<Item = ObjectCandidate>,
        L: IntoIterator<Item = ObjectIdentity>,
    {
        GcPlanPages {
            candidates:     candidates.into_iter().peekable(),
            live:           live.into_iter().peekable(),
            managed_prefix: self.managed_prefix.clone(),
            cutoff_ms:      self.cutoff_ms,
            page_size:      self.page_size,
            last_candidate: None,
            last_live:      None,
            done:           false,
        }
    }
}

pub struct GcPlanPages<C, L>
where
    C: Iterator<Item = ObjectCandidate>,
    L: Iterator<Item = ObjectIdentity>,
{
    candidates:     Peekable<C>,
    live:           Peekable<L>,
    managed_prefix: String,
    cutoff_ms:      u64,
    page_size:      usize,
    last_candidate: Option<String>,
    last_live:      Option<String>,
    done:           bool,
}

impl<C, L> GcPlanPages<C, L>
where
    C: Iterator<Item = ObjectCandidate>,
    L: Iterator<Item = ObjectIdentity>,
{
    fn pop_live(&mut self) -> Result<Option<ObjectIdentity>> {
        let Some(identity) = self.live.next() else {
            return Ok(None);
        };
        if self
            .last_live
            .as_deref()
            .is_some_and(|previous| previous >= identity.uri.as_str())
        {
            return Err(ObjectError::GcInputUnsorted { input: "live" });
        }
        self.last_live = Some(identity.uri.clone());
        Ok(Some(identity))
    }

    fn finish_with_error(&mut self, error: ObjectError) -> Option<Result<GcPlanPage>> {
        self.done = true;
        Some(Err(error))
    }
}

impl<C, L> Iterator for GcPlanPages<C, L>
where
    C: Iterator<Item = ObjectCandidate>,
    L: Iterator<Item = ObjectIdentity>,
{
    type Item = Result<GcPlanPage>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut planned = Vec::with_capacity(self.page_size);
        while planned.len() < self.page_size {
            let Some(candidate) = self.candidates.next() else {
                while self.live.peek().is_some() {
                    if let Err(error) = self.pop_live() {
                        return self.finish_with_error(error);
                    }
                }
                self.done = true;
                return (!planned.is_empty()).then_some(Ok(GcPlanPage {
                    candidates: planned,
                }));
            };
            if self
                .last_candidate
                .as_deref()
                .is_some_and(|previous| previous >= candidate.uri.as_str())
            {
                return self.finish_with_error(ObjectError::GcInputUnsorted { input: "inventory" });
            }
            self.last_candidate = Some(candidate.uri.clone());
            if !candidate.uri.starts_with(&self.managed_prefix) {
                return self.finish_with_error(ObjectError::GcCandidateOutsideStage {
                    uri:   candidate.uri,
                    stage: self.managed_prefix.clone(),
                });
            }

            while self
                .live
                .peek()
                .is_some_and(|identity| identity.uri.as_str() < candidate.uri.as_str())
            {
                if let Err(error) = self.pop_live() {
                    return self.finish_with_error(error);
                }
            }
            let is_live = self
                .live
                .peek()
                .is_some_and(|identity| identity.uri == candidate.uri);
            if is_live {
                if let Err(error) = self.pop_live() {
                    return self.finish_with_error(error);
                }
            } else if candidate.last_modified_ms <= self.cutoff_ms {
                planned.push(candidate);
            }
        }
        Some(Ok(GcPlanPage {
            candidates: planned,
        }))
    }
}

#[cfg(test)]
mod tests {
    use lake_common::ObjectIdentity;

    use super::*;

    fn candidate(uri: &str, modified_ms: u64) -> ObjectCandidate {
        ObjectCandidate {
            uri:              uri.to_owned(),
            size_bytes:       42,
            last_modified_ms: modified_ms,
        }
    }

    fn live(uri: &str) -> ObjectIdentity {
        ObjectIdentity {
            uri:          uri.to_owned(),
            content_type: "video/mp4".to_owned(),
            size_bytes:   42,
            sha256:       "aa".to_owned(),
        }
    }

    #[test]
    fn gc_plan_marks_only_old_unreferenced_managed_objects() {
        assert!(matches!(
            GcPlanner::try_new("s3://lake/objects/", 1_000, 2, false),
            Err(ObjectError::GcLineageIncomplete)
        ));
        let planner = GcPlanner::try_new("s3://lake/objects/", 1_000, 1, true).unwrap();
        let candidates = vec![
            candidate("s3://lake/objects/a", 10),
            candidate("s3://lake/objects/b", 20),
            candidate("s3://lake/objects/c", 2_000),
        ];
        let pages = planner
            .plan(candidates, vec![live("s3://lake/objects/b")])
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(pages.len(), 1);
        assert_eq!(
            pages[0].candidates(),
            &[candidate("s3://lake/objects/a", 10)]
        );

        let outside = planner
            .plan(
                vec![candidate("s3://somebody-else/objects/a", 10)],
                Vec::<ObjectIdentity>::new(),
            )
            .collect::<Result<Vec<_>>>();
        assert!(matches!(
            outside,
            Err(ObjectError::GcCandidateOutsideStage { .. })
        ));

        let unsorted = planner
            .plan(
                vec![
                    candidate("s3://lake/objects/b", 10),
                    candidate("s3://lake/objects/a", 10),
                ],
                Vec::<ObjectIdentity>::new(),
            )
            .collect::<Result<Vec<_>>>();
        assert!(matches!(
            unsorted,
            Err(ObjectError::GcInputUnsorted { input: "inventory" })
        ));
    }
}
