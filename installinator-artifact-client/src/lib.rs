// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to installinator-artifactd.

progenitor::generate_api!(
    spec = "../openapi/installinator-artifactd.json",
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
        ProgressReport = installinator_common::ProgressReport,
        CompletionEvent = installinator_common::CompletionEvent,
        CompletionEventKind = installinator_common::CompletionEventKind,
        ProgressEvent = installinator_common::ProgressEvent,
        ProgressEventKind = installinator_common::ProgressEventKind,
        Duration = std::time::Duration,
    }
);

/// A type alias for errors returned by this crate.
pub type ClientError = crate::Error<crate::types::Error>;
