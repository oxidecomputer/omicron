// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for API requests to an Oximeter metric collection server

progenitor::generate_api!(
    spec = "../../openapi/oximeter/oximeter-latest.json",
    interface = Positional,
    inner_type = slog::Logger,
    hooks = Expected,
);

progenitor_extras::slog_hooks::impl_slog_client_hooks!(Client);

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
