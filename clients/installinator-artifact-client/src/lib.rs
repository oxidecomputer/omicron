// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to installinator-artifactd.

progenitor::generate_api!(
    spec = "../../openapi/installinator-artifactd.json",
    inner_type = slog::Logger,
    pre_hook = (|log: &slog::Logger, request: &reqwest::Request| {
        slog::debug!(log, "client request";
            "method" => %request.method(),
            "uri" => %request.url(),
            "body" => ?&request.body(),
        );
    }),
    post_hook = (|log: &slog::Logger, result: &Result<_, _>| {
        slog::debug!(log, "client response"; "result" => ?result);
    }),
    derives = [schemars::JsonSchema],
    replace = {
        EventReportForInstallinatorSpec = installinator_common::EventReport,
        StepEventForInstallinatorSpec = installinator_common::StepEvent,
        ProgressEventForInstallinatorSpec = installinator_common::ProgressEvent,
        StepEventForGenericSpec = installinator_common::StepEvent<update_engine::NestedSpec>,
        ProgressEventForGenericSpec = installinator_common::ProgressEvent<update_engine::NestedSpec>,
        M2Slot = installinator_common::M2Slot,
        Duration = std::time::Duration,
    }
);

/// A type alias for errors returned by this crate.
pub type ClientError = crate::Error<crate::types::Error>;
