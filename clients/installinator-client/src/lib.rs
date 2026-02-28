// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for installinator to make API requests.

progenitor::generate_api!(
    spec = "../../openapi/installinator/installinator-1.0.0-c0ed87.json",
    interface = Positional,
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
    crates = {
        "omicron-uuid-kinds" = "*",
    },
    replace = {
        Duration = std::time::Duration,
        EventReportForGenericSpec = oxide_update_engine_types::events::EventReport<oxide_update_engine_types::spec::GenericSpec>,
        M2Slot = omicron_common::disk::M2Slot,
        ProgressEventForGenericSpec = oxide_update_engine_types::events::ProgressEvent<oxide_update_engine_types::spec::GenericSpec>,
        StepEventForGenericSpec = oxide_update_engine_types::events::StepEvent<oxide_update_engine_types::spec::GenericSpec>,
    }
);

/// A type alias for errors returned by this crate.
pub type ClientError = crate::Error<crate::types::Error>;
