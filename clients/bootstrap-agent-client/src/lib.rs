// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to a Bootstrap Agent

progenitor::generate_api!(
    spec = "../../openapi/bootstrap-agent/bootstrap-agent-1.0.0-127591.json",
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
        ImportExportPolicy = omicron_common::api::external::ImportExportPolicy,
    },
);

impl omicron_common::api::external::ClientError for types::Error {
    fn message(&self) -> String {
        self.message.clone()
    }
}

impl From<types::Baseboard> for sled_hardware_types::Baseboard {
    fn from(value: types::Baseboard) -> Self {
        match value {
            types::Baseboard::Gimlet { identifier, model, revision } => {
                sled_hardware_types::Baseboard::new_gimlet(
                    identifier, model, revision,
                )
            }
            types::Baseboard::Unknown => {
                sled_hardware_types::Baseboard::unknown()
            }
            types::Baseboard::Pc { identifier, model } => {
                sled_hardware_types::Baseboard::new_pc(identifier, model)
            }
        }
    }
}

impl From<sled_hardware_types::Baseboard> for types::Baseboard {
    fn from(value: sled_hardware_types::Baseboard) -> Self {
        match value {
            sled_hardware_types::Baseboard::Gimlet {
                identifier,
                model,
                revision,
            } => types::Baseboard::Gimlet { identifier, model, revision },
            sled_hardware_types::Baseboard::Unknown => {
                types::Baseboard::Unknown
            }
            sled_hardware_types::Baseboard::Pc { identifier, model } => {
                types::Baseboard::Pc { identifier, model }
            }
        }
    }
}
