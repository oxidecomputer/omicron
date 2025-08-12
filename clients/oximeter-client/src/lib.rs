// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2025 Oxide Computer Company

//! Interface for API requests to an Oximeter metric collection server

progenitor::generate_api!(
    spec = "../../openapi/oximeter/oximeter-latest.json",
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
);

impl omicron_common::api::external::ClientError for types::Error {
    fn message(&self) -> String {
        self.message.clone()
    }
}

impl From<std::time::Duration> for types::Duration {
    fn from(s: std::time::Duration) -> Self {
        Self { nanos: s.subsec_nanos(), secs: s.as_secs() }
    }
}

impl From<omicron_common::api::internal::nexus::ProducerKind>
    for types::ProducerKind
{
    fn from(kind: omicron_common::api::internal::nexus::ProducerKind) -> Self {
        use omicron_common::api::internal::nexus;
        match kind {
            nexus::ProducerKind::ManagementGateway => Self::ManagementGateway,
            nexus::ProducerKind::Service => Self::Service,
            nexus::ProducerKind::SledAgent => Self::SledAgent,
            nexus::ProducerKind::Instance => Self::Instance,
        }
    }
}

impl From<&omicron_common::api::internal::nexus::ProducerEndpoint>
    for types::ProducerEndpoint
{
    fn from(
        s: &omicron_common::api::internal::nexus::ProducerEndpoint,
    ) -> Self {
        Self {
            address: s.address.to_string(),
            id: s.id,
            kind: s.kind.into(),
            interval: s.interval.into(),
        }
    }
}
