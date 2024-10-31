// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2021 Oxide Computer Company

//! Interface for API requests to an ereporter.

progenitor::generate_api!(
    spec = "../../openapi/ereporter.json",
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

    replace = {
        Entry = ereporter_types::Entry,
        EntryKind = ereporter_types::EntryKind,
        Ereport = ereporter_types::Ereport,
        ReporterId = ereporter_types::ReporterId,
        Generation = omicron_common::api::external::Generation,
    },
);

pub use ereporter_types::UnregisteredReporterError;

impl omicron_common::api::external::ClientError for types::Error {
    fn message(&self) -> String {
        self.message.clone()
    }
}

impl types::Error {
    pub fn unregistered_reporter(&self) -> Option<UnregisteredReporterError> {
        let code = self.error_code.as_ref()?;
        serde_json::from_str::<UnregisteredReporterError>(&code).ok()
    }
}
