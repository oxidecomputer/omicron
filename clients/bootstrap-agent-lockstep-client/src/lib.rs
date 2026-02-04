// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to the Bootstrap Agent Lockstep API

progenitor::generate_api!(
    spec = "../../openapi/bootstrap-agent-lockstep.json",
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
        "oxnet" = "0.1.0",
    },
    replace = {
        AllowedSourceIps = omicron_common::api::external::AllowedSourceIps,
        Baseboard = sled_hardware_types::Baseboard,
        ImportExportPolicy = omicron_common::api::external::ImportExportPolicy,
    },
);

impl omicron_common::api::external::ClientError for types::Error {
    fn message(&self) -> String {
        self.message.clone()
    }
}
