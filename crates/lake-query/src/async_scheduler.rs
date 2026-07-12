// Copyright 2026 Rararulab
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! Bounded process-local selection for durable asynchronous Query workers.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

const MAX_ASYNC_WORKERS: usize = 64;
const MAX_ASYNC_EXECUTION: Duration = Duration::from_hours(24);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AsyncSchedulerLimits {
    max_running:            usize,
    max_running_per_tenant: usize,
    execution_time:         Duration,
}

impl AsyncSchedulerLimits {
    pub(crate) fn try_new(
        max_running: usize,
        max_running_per_tenant: usize,
        execution_time: Duration,
    ) -> Result<Self, &'static str> {
        if !(1..=MAX_ASYNC_WORKERS).contains(&max_running) {
            return Err("max async workers must be within 1..=64");
        }
        if max_running_per_tenant == 0 || max_running_per_tenant > max_running {
            return Err("per-tenant async workers must be within 1..=max workers");
        }
        if execution_time.is_zero() || execution_time > MAX_ASYNC_EXECUTION {
            return Err("async execution time must be within 1ns..=24h");
        }
        Ok(Self {
            max_running,
            max_running_per_tenant,
            execution_time,
        })
    }

    pub(crate) const fn max_running(&self) -> usize { self.max_running }

    pub(crate) const fn max_running_per_tenant(&self) -> usize { self.max_running_per_tenant }

    pub(crate) const fn execution_time(&self) -> Duration { self.execution_time }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AsyncCandidate {
    query_id: String,
    tenant:   String,
}

impl AsyncCandidate {
    pub(crate) fn new(query_id: impl Into<String>, tenant: impl Into<String>) -> Self {
        Self {
            query_id: query_id.into(),
            tenant:   tenant.into(),
        }
    }

    pub(crate) fn query_id(&self) -> &str { &self.query_id }

    pub(crate) fn tenant(&self) -> &str { &self.tenant }
}

pub(crate) struct AsyncScheduler {
    limits:         AsyncSchedulerLimits,
    active_queries: HashSet<String>,
    active_tenants: HashMap<String, usize>,
}

impl AsyncScheduler {
    pub(crate) fn new(limits: AsyncSchedulerLimits) -> Self {
        Self {
            limits,
            active_queries: HashSet::with_capacity(limits.max_running()),
            active_tenants: HashMap::with_capacity(limits.max_running()),
        }
    }

    pub(crate) fn active(&self) -> usize { self.active_queries.len() }

    pub(crate) fn available(&self) -> usize {
        self.limits.max_running().saturating_sub(self.active())
    }

    pub(crate) fn tenant_saturated(&self, tenant: &str) -> bool {
        self.active_tenants.get(tenant).copied().unwrap_or_default()
            >= self.limits.max_running_per_tenant()
    }

    pub(crate) fn select(
        &self,
        candidates: impl IntoIterator<Item = AsyncCandidate>,
    ) -> Vec<AsyncCandidate> {
        let mut tenants = VecDeque::<(String, VecDeque<AsyncCandidate>)>::new();
        let mut indexes = HashMap::<String, usize>::new();
        for candidate in candidates {
            if self.active_queries.contains(candidate.query_id())
                || self.tenant_saturated(candidate.tenant())
            {
                continue;
            }
            if let Some(index) = indexes.get(candidate.tenant()).copied() {
                tenants[index].1.push_back(candidate);
            } else {
                indexes.insert(candidate.tenant().to_owned(), tenants.len());
                tenants.push_back((candidate.tenant().to_owned(), VecDeque::from([candidate])));
            }
        }

        let mut selected = Vec::with_capacity(self.available());
        let mut selected_per_tenant = HashMap::<String, usize>::new();
        while selected.len() < self.available() && !tenants.is_empty() {
            let round = tenants.len();
            let mut progressed = false;
            for _ in 0..round {
                let Some((tenant, mut queue)) = tenants.pop_front() else {
                    break;
                };
                let already_active = self
                    .active_tenants
                    .get(&tenant)
                    .copied()
                    .unwrap_or_default();
                let newly_selected = selected_per_tenant
                    .get(&tenant)
                    .copied()
                    .unwrap_or_default();
                if already_active + newly_selected < self.limits.max_running_per_tenant() {
                    if let Some(candidate) = queue.pop_front() {
                        selected.push(candidate);
                        *selected_per_tenant.entry(tenant.clone()).or_default() += 1;
                        progressed = true;
                    }
                }
                if !queue.is_empty() {
                    tenants.push_back((tenant, queue));
                }
            }
            if !progressed {
                break;
            }
        }
        selected
    }

    pub(crate) fn started(&mut self, candidate: &AsyncCandidate) {
        if self.active_queries.insert(candidate.query_id().to_owned()) {
            *self
                .active_tenants
                .entry(candidate.tenant().to_owned())
                .or_default() += 1;
        }
    }

    pub(crate) fn finished(&mut self, candidate: &AsyncCandidate) {
        if !self.active_queries.remove(candidate.query_id()) {
            return;
        }
        if let Some(active) = self.active_tenants.get_mut(candidate.tenant()) {
            *active -= 1;
            if *active == 0 {
                self.active_tenants.remove(candidate.tenant());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{AsyncCandidate, AsyncScheduler, AsyncSchedulerLimits};

    fn candidate(query_id: &str, tenant: &str) -> AsyncCandidate {
        AsyncCandidate::new(query_id, tenant)
    }

    #[test]
    fn async_scheduler_skips_saturated_tenant_for_eligible_neighbor() {
        let limits = AsyncSchedulerLimits::try_new(2, 1, Duration::from_secs(30)).unwrap();
        let mut scheduler = AsyncScheduler::new(limits);
        scheduler.started(&candidate("alpha-running", "alpha"));

        let selected = scheduler.select([
            candidate("alpha-queued-1", "alpha"),
            candidate("alpha-queued-2", "alpha"),
            candidate("beta-queued", "beta"),
        ]);

        assert_eq!(selected, vec![candidate("beta-queued", "beta")]);
        assert_eq!(scheduler.active(), 1);
    }

    #[test]
    fn async_scheduler_round_robins_eligible_tenants_within_a_page() {
        let limits = AsyncSchedulerLimits::try_new(4, 2, Duration::from_secs(30)).unwrap();
        let scheduler = AsyncScheduler::new(limits);

        let selected = scheduler.select([
            candidate("alpha-1", "alpha"),
            candidate("alpha-2", "alpha"),
            candidate("alpha-3", "alpha"),
            candidate("beta-1", "beta"),
            candidate("beta-2", "beta"),
        ]);

        assert_eq!(
            selected,
            [
                candidate("alpha-1", "alpha"),
                candidate("beta-1", "beta"),
                candidate("alpha-2", "alpha"),
                candidate("beta-2", "beta"),
            ]
        );
    }

    #[test]
    fn async_scheduler_limit_values_are_bounded() {
        assert!(AsyncSchedulerLimits::try_new(0, 1, Duration::from_secs(1)).is_err());
        assert!(AsyncSchedulerLimits::try_new(2, 0, Duration::from_secs(1)).is_err());
        assert!(AsyncSchedulerLimits::try_new(2, 3, Duration::from_secs(1)).is_err());
        assert!(AsyncSchedulerLimits::try_new(2, 1, Duration::ZERO).is_err());
        assert!(AsyncSchedulerLimits::try_new(65, 1, Duration::from_secs(1)).is_err());
    }
}
