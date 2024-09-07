// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::SocketAddr;
use std::time::Duration;

use super::SqlU16;
use crate::impl_enum_type;
use crate::schema::metric_producer;
use db_macros::Asset;
use nexus_types::identity::Asset;
use omicron_common::api::internal;
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Copy, Clone, Debug, QueryId)]
    #[diesel(postgres_type(name = "producer_kind", schema = "public"))]
    pub struct ProducerKindEnum;

    #[derive(AsExpression, Copy, Clone, Debug, FromSqlRow, PartialEq)]
    #[diesel(sql_type = ProducerKindEnum)]
    pub enum ProducerKind;

    ManagementGateway => b"management_gateway"
    SledAgent => b"sled_agent"
    Service => b"service"
    Instance => b"instance"
);

impl From<internal::nexus::ProducerKind> for ProducerKind {
    fn from(kind: internal::nexus::ProducerKind) -> Self {
        match kind {
            internal::nexus::ProducerKind::ManagementGateway => {
                ProducerKind::ManagementGateway
            }
            internal::nexus::ProducerKind::SledAgent => ProducerKind::SledAgent,
            internal::nexus::ProducerKind::Service => ProducerKind::Service,
            internal::nexus::ProducerKind::Instance => ProducerKind::Instance,
        }
    }
}

impl From<ProducerKind> for internal::nexus::ProducerKind {
    fn from(kind: ProducerKind) -> Self {
        match kind {
            ProducerKind::ManagementGateway => {
                internal::nexus::ProducerKind::ManagementGateway
            }
            ProducerKind::SledAgent => internal::nexus::ProducerKind::SledAgent,
            ProducerKind::Service => internal::nexus::ProducerKind::Service,
            ProducerKind::Instance => internal::nexus::ProducerKind::Instance,
        }
    }
}

impl From<ProducerEndpoint> for internal::nexus::ProducerEndpoint {
    fn from(ep: ProducerEndpoint) -> Self {
        internal::nexus::ProducerEndpoint {
            id: ep.id(),
            kind: ep.kind.into(),
            address: SocketAddr::new(ep.ip.ip(), *ep.port),
            interval: Duration::from_secs_f64(ep.interval),
        }
    }
}

/// Information announced by a metric server, used so that clients can contact it and collect
/// available metric data from it.
#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset, PartialEq)]
#[diesel(table_name = metric_producer)]
pub struct ProducerEndpoint {
    #[diesel(embed)]
    identity: ProducerEndpointIdentity,

    pub kind: ProducerKind,
    pub ip: ipnetwork::IpNetwork,
    pub port: SqlU16,
    pub interval: f64,
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
            kind: endpoint.kind.into(),
            ip: endpoint.address.ip().into(),
            port: endpoint.address.port().into(),
            interval: endpoint.interval.as_secs_f64(),
            oximeter_id,
        }
    }
}
