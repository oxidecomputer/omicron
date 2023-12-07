// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Model types for external IPs, both for instances and externally-facing
//! services.

use crate::impl_enum_type;
use crate::schema::external_ip;
use crate::schema::floating_ip;
use crate::Name;
use crate::SqlU16;
use chrono::DateTime;
use chrono::Utc;
use db_macros::Resource;
use diesel::Queryable;
use diesel::Selectable;
use ipnetwork::IpNetwork;
use nexus_types::external_api::shared;
use nexus_types::external_api::views;
use omicron_common::address::NUM_SOURCE_NAT_PORTS;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadata;
use serde::Deserialize;
use serde::Serialize;
use std::convert::TryFrom;
use std::net::IpAddr;
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, Clone, Copy, QueryId)]
    #[diesel(postgres_type(name = "ip_kind"))]
     pub struct IpKindEnum;

     #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
     #[diesel(sql_type = IpKindEnum)]
     pub enum IpKind;

     SNat => b"snat"
     Ephemeral => b"ephemeral"
     Floating => b"floating"
);

/// The main model type for external IP addresses for instances
/// and externally-facing services.
///
/// This encompasses the three flavors of external IPs: automatic source NAT
/// IPs, Ephemeral IPs, and Floating IPs. The first two are similar in that they
/// are anonymous, lacking a name or description, which a Floating IP is a named
/// API resource. The second two are similar in that they are externally-visible
/// addresses and port ranges, while source NAT IPs are not discoverable in the
/// API at all, and only provide outbound connectivity to instances, not
/// inbound.
#[derive(Debug, Clone, Selectable, Queryable, Insertable)]
#[diesel(table_name = external_ip)]
pub struct ExternalIp {
    pub id: Uuid,
    // Only Some(_) for Floating IPs
    pub name: Option<Name>,
    // Only Some(_) for Floating IPs
    pub description: Option<String>,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    pub ip_pool_id: Uuid,
    pub ip_pool_range_id: Uuid,
    pub is_service: bool,
    // This is Some(_) for:
    //  - all instance/service SNAT IPs
    //  - all ephemeral IPs
    //  - a floating IP attached to an instance or service.
    pub parent_id: Option<Uuid>,
    pub kind: IpKind,
    pub ip: IpNetwork,
    pub first_port: SqlU16,
    pub last_port: SqlU16,
    // Only Some(_) for instance Floating IPs
    pub project_id: Option<Uuid>,
}

/// A view type constructed from `ExternalIp` used to represent Floating IP
/// objects in user-facing APIs.
///
/// This View type fills a similar niche to `ProjectImage` etc.: we need to
/// represent identity as non-nullable (ditto for parent project) so as to
/// play nicely with authz and resource APIs.
#[derive(
    Queryable, Selectable, Clone, Debug, Resource, Serialize, Deserialize,
)]
#[diesel(table_name = floating_ip)]
pub struct FloatingIp {
    #[diesel(embed)]
    pub identity: FloatingIpIdentity,

    pub ip_pool_id: Uuid,
    pub ip_pool_range_id: Uuid,
    pub is_service: bool,
    pub parent_id: Option<Uuid>,
    pub ip: IpNetwork,
    pub project_id: Uuid,
}

impl From<ExternalIp> for sled_agent_client::types::SourceNatConfig {
    fn from(eip: ExternalIp) -> Self {
        Self {
            ip: eip.ip.ip(),
            first_port: eip.first_port.0,
            last_port: eip.last_port.0,
        }
    }
}

/// An incomplete external IP, used to store state required for issuing the
/// database query that selects an available IP and stores the resulting record.
#[derive(Debug, Clone)]
pub struct IncompleteExternalIp {
    id: Uuid,
    name: Option<Name>,
    description: Option<String>,
    time_created: DateTime<Utc>,
    kind: IpKind,
    is_service: bool,
    parent_id: Option<Uuid>,
    pool_id: Uuid,
    project_id: Option<Uuid>,
    // Optional address requesting that a specific IP address be allocated.
    explicit_ip: Option<IpNetwork>,
    // Optional range when requesting a specific SNAT range be allocated.
    explicit_port_range: Option<(i32, i32)>,
}

impl IncompleteExternalIp {
    pub fn for_instance_source_nat(
        id: Uuid,
        instance_id: Uuid,
        pool_id: Uuid,
    ) -> Self {
        Self {
            id,
            name: None,
            description: None,
            time_created: Utc::now(),
            kind: IpKind::SNat,
            is_service: false,
            parent_id: Some(instance_id),
            pool_id,
            project_id: None,
            explicit_ip: None,
            explicit_port_range: None,
        }
    }

    pub fn for_ephemeral(id: Uuid, instance_id: Uuid, pool_id: Uuid) -> Self {
        Self {
            id,
            name: None,
            description: None,
            time_created: Utc::now(),
            kind: IpKind::Ephemeral,
            is_service: false,
            parent_id: Some(instance_id),
            pool_id,
            project_id: None,
            explicit_ip: None,
            explicit_port_range: None,
        }
    }

    pub fn for_floating(
        id: Uuid,
        name: &Name,
        description: &str,
        project_id: Uuid,
        pool_id: Uuid,
    ) -> Self {
        Self {
            id,
            name: Some(name.clone()),
            description: Some(description.to_string()),
            time_created: Utc::now(),
            kind: IpKind::Floating,
            is_service: false,
            parent_id: None,
            pool_id,
            project_id: Some(project_id),
            explicit_ip: None,
            explicit_port_range: None,
        }
    }

    pub fn for_floating_explicit(
        id: Uuid,
        name: &Name,
        description: &str,
        project_id: Uuid,
        explicit_ip: IpAddr,
        pool_id: Uuid,
    ) -> Self {
        Self {
            id,
            name: Some(name.clone()),
            description: Some(description.to_string()),
            time_created: Utc::now(),
            kind: IpKind::Floating,
            is_service: false,
            parent_id: None,
            pool_id,
            project_id: Some(project_id),
            explicit_ip: Some(explicit_ip.into()),
            explicit_port_range: None,
        }
    }

    pub fn for_service_explicit(
        id: Uuid,
        name: &Name,
        description: &str,
        service_id: Uuid,
        pool_id: Uuid,
        address: IpAddr,
    ) -> Self {
        Self {
            id,
            name: Some(name.clone()),
            description: Some(description.to_string()),
            time_created: Utc::now(),
            kind: IpKind::Floating,
            is_service: true,
            parent_id: Some(service_id),
            pool_id,
            project_id: None,
            explicit_ip: Some(IpNetwork::from(address)),
            explicit_port_range: None,
        }
    }

    pub fn for_service_explicit_snat(
        id: Uuid,
        service_id: Uuid,
        pool_id: Uuid,
        address: IpAddr,
        (first_port, last_port): (u16, u16),
    ) -> Self {
        assert!(
            (first_port % NUM_SOURCE_NAT_PORTS == 0)
                && (last_port - first_port + 1) == NUM_SOURCE_NAT_PORTS,
            "explicit port range must be aligned to {}",
            NUM_SOURCE_NAT_PORTS,
        );
        let explicit_port_range = Some((first_port.into(), last_port.into()));
        Self {
            id,
            name: None,
            description: None,
            time_created: Utc::now(),
            kind: IpKind::SNat,
            is_service: true,
            parent_id: Some(service_id),
            pool_id,
            project_id: None,
            explicit_ip: Some(IpNetwork::from(address)),
            explicit_port_range,
        }
    }

    pub fn for_service(
        id: Uuid,
        name: &Name,
        description: &str,
        service_id: Uuid,
        pool_id: Uuid,
    ) -> Self {
        Self {
            id,
            name: Some(name.clone()),
            description: Some(description.to_string()),
            time_created: Utc::now(),
            kind: IpKind::Floating,
            is_service: true,
            parent_id: Some(service_id),
            pool_id,
            project_id: None,
            explicit_ip: None,
            explicit_port_range: None,
        }
    }

    pub fn for_service_snat(id: Uuid, service_id: Uuid, pool_id: Uuid) -> Self {
        Self {
            id,
            name: None,
            description: None,
            time_created: Utc::now(),
            kind: IpKind::SNat,
            is_service: true,
            parent_id: Some(service_id),
            pool_id,
            project_id: None,
            explicit_ip: None,
            explicit_port_range: None,
        }
    }

    pub fn id(&self) -> &Uuid {
        &self.id
    }

    pub fn name(&self) -> &Option<Name> {
        &self.name
    }

    pub fn description(&self) -> &Option<String> {
        &self.description
    }

    pub fn time_created(&self) -> &DateTime<Utc> {
        &self.time_created
    }

    pub fn kind(&self) -> &IpKind {
        &self.kind
    }

    pub fn is_service(&self) -> &bool {
        &self.is_service
    }

    pub fn parent_id(&self) -> &Option<Uuid> {
        &self.parent_id
    }

    pub fn pool_id(&self) -> &Uuid {
        &self.pool_id
    }

    pub fn project_id(&self) -> &Option<Uuid> {
        &self.project_id
    }

    pub fn explicit_ip(&self) -> &Option<IpNetwork> {
        &self.explicit_ip
    }

    pub fn explicit_port_range(&self) -> &Option<(i32, i32)> {
        &self.explicit_port_range
    }
}

impl TryFrom<IpKind> for shared::IpKind {
    type Error = Error;

    fn try_from(kind: IpKind) -> Result<Self, Self::Error> {
        match kind {
            IpKind::Ephemeral => Ok(shared::IpKind::Ephemeral),
            IpKind::Floating => Ok(shared::IpKind::Floating),
            _ => Err(Error::internal_error(
                "SNAT IP addresses should not be exposed in the API",
            )),
        }
    }
}

impl TryFrom<ExternalIp> for views::ExternalIp {
    type Error = Error;

    fn try_from(ip: ExternalIp) -> Result<Self, Self::Error> {
        if ip.is_service {
            return Err(Error::internal_error(
                "Service IPs should not be exposed in the API",
            ));
        }
        let kind = ip.kind.try_into()?;
        Ok(views::ExternalIp { kind, ip: ip.ip.ip() })
    }
}

impl TryFrom<ExternalIp> for FloatingIp {
    type Error = Error;

    fn try_from(ip: ExternalIp) -> Result<Self, Self::Error> {
        if ip.kind != IpKind::Floating {
            return Err(Error::internal_error(
                "attempted to convert non-floating external IP to floating",
            ));
        }
        if ip.is_service {
            return Err(Error::internal_error(
                "Service IPs should not be exposed in the API",
            ));
        }

        let project_id = ip.project_id.ok_or(Error::internal_error(
            "database schema guarantees parent project for non-service FIP",
        ))?;

        let name = ip.name.ok_or(Error::internal_error(
            "database schema guarantees ID metadata for non-service FIP",
        ))?;

        let description = ip.description.ok_or(Error::internal_error(
            "database schema guarantees ID metadata for non-service FIP",
        ))?;

        let identity = FloatingIpIdentity {
            id: ip.id,
            name,
            description,
            time_created: ip.time_created,
            time_modified: ip.time_modified,
            time_deleted: ip.time_deleted,
        };

        Ok(FloatingIp {
            ip: ip.ip,
            identity,
            project_id,
            ip_pool_id: ip.ip_pool_id,
            ip_pool_range_id: ip.ip_pool_range_id,
            is_service: ip.is_service,
            parent_id: ip.parent_id,
        })
    }
}

impl TryFrom<ExternalIp> for views::FloatingIp {
    type Error = Error;

    fn try_from(ip: ExternalIp) -> Result<Self, Self::Error> {
        FloatingIp::try_from(ip).map(Into::into)
    }
}

impl From<FloatingIp> for views::FloatingIp {
    fn from(ip: FloatingIp) -> Self {
        let identity = IdentityMetadata {
            id: ip.identity.id,
            name: ip.identity.name.into(),
            description: ip.identity.description,
            time_created: ip.identity.time_created,
            time_modified: ip.identity.time_modified,
        };

        views::FloatingIp {
            ip: ip.ip.ip(),
            identity,
            project_id: ip.project_id,
            instance_id: ip.parent_id,
        }
    }
}
