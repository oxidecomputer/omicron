// Copyright 2021 Oxide Computer Company

//! Interface for API requests to an Oximeter metric collection server

omicron_common::generate_logging_api!("../openapi/oximeter.json");

impl From<std::time::Duration> for types::Duration {
    fn from(s: std::time::Duration) -> Self {
        Self { nanos: s.subsec_nanos(), secs: s.as_secs() }
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
            base_route: s.base_route.clone(),
            id: s.id,
            interval: s.interval.into(),
        }
    }
}
