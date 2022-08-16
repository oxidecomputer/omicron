// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::SqlU16;
use crate::schema::metric_producer;
use db_macros::Asset;
use nexus_types::identity::Asset;
use omicron_common::api::internal;
use uuid::Uuid;

/// Information announced by a metric server, used so that clients can contact it and collect
/// available metric data from it.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = metric_producer)]
pub struct ProducerEndpoint {
    #[diesel(embed)]
    identity: ProducerEndpointIdentity,

    pub ip: ipnetwork::IpNetwork,
    pub port: SqlU16,
    pub interval: f64,
    pub base_route: String,
    pub oximeter_id: Uuid,
}

impl ProducerEndpoint {
    /// Create a new endpoint, with the data announced by the producer and a chosen Oximeter
    /// instance to act as its collector.
    pub fn new(
        endpoint: &internal::nexus::ProducerEndpoint,
        oximeter_id: Uuid,
    ) -> Self {
        Self {
            identity: ProducerEndpointIdentity::new(endpoint.id),
            ip: endpoint.address.ip().into(),
            port: endpoint.address.port().into(),
            base_route: endpoint.base_route.clone(),
            interval: endpoint.interval.as_secs_f64(),
            oximeter_id,
        }
    }

    /// Return the route that can be used to request metric data.
    pub fn collection_route(&self) -> String {
        format!("{}/{}", &self.base_route, self.id())
    }
}
