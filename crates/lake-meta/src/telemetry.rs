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

//! Bounded-cardinality DynamoDB layout and migration telemetry.

#[derive(Clone, Copy)]
pub(crate) enum PrefixLayout {
    V1,
    V2,
}

#[derive(Clone, Copy)]
pub(crate) enum PrefixApi {
    Scan,
    Query,
}

#[derive(Clone, Copy)]
pub(crate) enum RequestOutcome {
    Success,
    Error,
}

impl PrefixLayout {
    const fn label(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }
}

impl PrefixApi {
    const fn label(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Query => "query",
        }
    }
}

impl RequestOutcome {
    const fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
        }
    }
}

pub(crate) fn describe() {
    metrics::describe_gauge!(
        "lake_dynamo_v2_authoritative",
        "Whether this process reads from the Dynamo v2 prefix layout"
    );
    metrics::describe_gauge!(
        "lake_dynamo_finalize_barrier_held",
        "Whether the durable Dynamo migration write barrier is held"
    );
    metrics::describe_counter!(
        "lake_dynamo_prefix_requests_total",
        "Physical Dynamo prefix requests by bounded layout, API, and outcome"
    );
    metrics::describe_counter!(
        "lake_dynamo_prefix_items_total",
        "Dynamo prefix items evaluated and returned by bounded layout and API"
    );
}

pub(crate) fn authority(authoritative: bool) {
    metrics::gauge!("lake_dynamo_v2_authoritative").set(f64::from(authoritative));
}

pub(crate) fn barrier(held: bool) {
    metrics::gauge!("lake_dynamo_finalize_barrier_held").set(f64::from(held));
}

pub(crate) fn prefix_request(
    layout: PrefixLayout,
    api: PrefixApi,
    outcome: RequestOutcome,
    evaluated: usize,
    returned: usize,
) {
    let layout = layout.label();
    let api = api.label();
    metrics::counter!(
        "lake_dynamo_prefix_requests_total",
        "layout" => layout,
        "api" => api,
        "outcome" => outcome.label(),
    )
    .increment(1);
    metrics::counter!(
        "lake_dynamo_prefix_items_total",
        "layout" => layout,
        "api" => api,
        "kind" => "evaluated",
    )
    .increment(evaluated as u64);
    metrics::counter!(
        "lake_dynamo_prefix_items_total",
        "layout" => layout,
        "api" => api,
        "kind" => "returned",
    )
    .increment(returned as u64);
}

#[cfg(test)]
mod tests {
    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::{
        PrefixApi, PrefixLayout, RequestOutcome, authority, barrier, describe, prefix_request,
    };

    fn with_recorder(test: impl FnOnce(metrics_exporter_prometheus::PrometheusHandle)) {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        describe();
        test(handle);
    }

    #[test]
    fn dynamo_prefix_metrics_record_bounded_work() {
        with_recorder(|handle| {
            prefix_request(
                PrefixLayout::V1,
                PrefixApi::Scan,
                RequestOutcome::Success,
                17,
                2,
            );
            prefix_request(
                PrefixLayout::V2,
                PrefixApi::Query,
                RequestOutcome::Error,
                0,
                0,
            );
            let rendered = handle.render();
            for expected in [
                "lake_dynamo_prefix_requests_total{layout=\"v1\",api=\"scan\",outcome=\"success\"\
                 } 1",
                "lake_dynamo_prefix_requests_total{layout=\"v2\",api=\"query\",outcome=\"error\"} \
                 1",
                "lake_dynamo_prefix_items_total{layout=\"v1\",api=\"scan\",kind=\"evaluated\"} 17",
                "lake_dynamo_prefix_items_total{layout=\"v1\",api=\"scan\",kind=\"returned\"} 2",
            ] {
                assert!(
                    rendered.contains(expected),
                    "missing {expected}:\n{rendered}"
                );
            }
        });
    }

    #[test]
    fn dynamo_prefix_metrics_never_export_logical_keys() {
        with_recorder(|handle| {
            prefix_request(
                PrefixLayout::V2,
                PrefixApi::Query,
                RequestOutcome::Success,
                1,
                1,
            );
            let rendered = handle.render();
            for forbidden in [
                "tbl/private-tenant/secret-table",
                "https://credentials.example",
                "forged-cursor",
                "operation_id",
                "prefix=",
                "key=",
            ] {
                assert!(
                    !rendered.contains(forbidden),
                    "leaked {forbidden}:\n{rendered}"
                );
            }
        });
    }

    #[test]
    fn dynamo_authority_metric_tracks_monotonic_switch() {
        with_recorder(|handle| {
            authority(false);
            assert!(handle.render().contains("lake_dynamo_v2_authoritative 0"));
            authority(true);
            assert!(handle.render().contains("lake_dynamo_v2_authoritative 1"));
        });
    }

    #[test]
    fn dynamo_migration_barrier_metric_is_identity_free() {
        with_recorder(|handle| {
            barrier(true);
            let rendered = handle.render();
            assert!(rendered.contains("lake_dynamo_finalize_barrier_held 1"));
            assert!(!rendered.contains("cursor"));
        });
    }
}
