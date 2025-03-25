// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

progenitor::generate_api!(
    spec = "../../openapi/ereport/ereport-latest.json",
    inner_type = slog::Logger,
    derives = [schemars::JsonSchema, Clone, Eq, PartialEq],
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
        Ena = ereport_types::Ena,
        TypedUuidForEreporterGenerationKind = omicron_uuid_kinds::EreporterGenerationUuid,
    }
);
