// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Model types for external IPs, both for instances and externally-facing
//! services.

use crate::impl_enum_type;
use crate::schema::instance_external_ip;
use crate::Name;
use crate::SqlU16;
use chrono::DateTime;
use chrono::Utc;
use diesel::Queryable;
use diesel::Selectable;
use ipnetwork::IpNetwork;
use nexus_types::external_api::shared;
use nexus_types::external_api::views;
use omicron_common::api::external::Error;
use std::convert::TryFrom;
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, Clone, Copy)]
    #[diesel(postgres_type(name = "ip_kind"))]
     pub struct IpKindEnum;

     #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
     #[diesel(sql_type = IpKindEnum)]
     pub enum IpKind;

     SNat => b"snat"
     Ephemeral => b"ephemeral"
     Floating => b"floating"
     Service => b"service"
);

/// The main model type for external IP addresses for instances.
///
/// This encompasses the three flavors of external IPs: automatic source NAT
/// IPs, Ephemeral IPs, and Floating IPs. The first two are similar in that they
/// are anonymous, lacking a name or description, which a Floating IP is a named
/// API resource. The second two are similar in that they are externally-visible
/// addresses and port ranges, while source NAT IPs are not discoverable in the
/// API at all, and only provide outbound connectivity to instances, not
/// inbound.
#[derive(Debug, Clone, Selectable, Queryable, Insertable)]
#[diesel(table_name = instance_external_ip)]
pub struct InstanceExternalIp {
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
    pub project_id: Option<Uuid>,
    // This is Some(_) for:
    //  - all instance SNAT IPs
    //  - all ephemeral IPs
    //  - a floating IP attached to an instance.
    pub instance_id: Option<Uuid>,
    pub kind: IpKind,
    pub ip: IpNetwork,
    pub first_port: SqlU16,
    pub last_port: SqlU16,
}

impl From<InstanceExternalIp> for sled_agent_client::types::SourceNatConfig {
    fn from(eip: InstanceExternalIp) -> Self {
        Self {
            ip: eip.ip.ip(),
            first_port: eip.first_port.0,
            last_port: eip.last_port.0,
        }
    }
}

/// Describes where the IP candidates for allocation come from: either
/// from an IP pool, or from a project.
///
/// This ensures that a source is always specified, and a caller cannot
/// request an external IP allocation without providing at least one of
/// these options.
#[derive(Debug, Clone, Copy)]
pub enum IpSource {
    Pool(Uuid),
    Project(Uuid),
}

/// An incomplete external IP, used to store state required for issuing the
/// database query that selects an available IP and stores the resulting record.
#[derive(Debug, Clone)]
pub struct IncompleteInstanceExternalIp {
    id: Uuid,
    name: Option<Name>,
    description: Option<String>,
    time_created: DateTime<Utc>,
    kind: IpKind,
    project_id: Option<Uuid>,
    instance_id: Option<Uuid>,
    source: IpSource,
}

impl IncompleteInstanceExternalIp {
    pub fn for_instance_source_nat(
        id: Uuid,
        project_id: Uuid,
        instance_id: Uuid,
        pool_id: Option<Uuid>,
    ) -> Self {
        let source = pool_id
            .map(|id| IpSource::Pool(id))
            .unwrap_or_else(|| IpSource::Project(project_id));
        Self {
            id,
            name: None,
            description: None,
            time_created: Utc::now(),
            kind: IpKind::SNat,
            project_id: Some(project_id),
            instance_id: Some(instance_id),
            source,
        }
    }

    pub fn for_ephemeral(
        id: Uuid,
        project_id: Uuid,
        instance_id: Uuid,
        pool_id: Option<Uuid>,
    ) -> Self {
        let source = pool_id
            .map(|id| IpSource::Pool(id))
            .unwrap_or_else(|| IpSource::Project(project_id));
        Self {
            id,
            name: None,
            description: None,
            time_created: Utc::now(),
            kind: IpKind::Ephemeral,
            project_id: Some(project_id),
            instance_id: Some(instance_id),
            source,
        }
    }

    pub fn for_floating(
        id: Uuid,
        name: &Name,
        description: &str,
        project_id: Uuid,
        pool_id: Option<Uuid>,
    ) -> Self {
        let source = pool_id
            .map(|id| IpSource::Pool(id))
            .unwrap_or_else(|| IpSource::Project(project_id));
        Self {
            id,
            name: Some(name.clone()),
            description: Some(description.to_string()),
            time_created: Utc::now(),
            kind: IpKind::Floating,
            project_id: Some(project_id),
            instance_id: None,
            source,
        }
    }

    pub fn for_service(id: Uuid, pool_id: Uuid) -> Self {
        Self {
            id,
            name: None,
            description: None,
            time_created: Utc::now(),
            kind: IpKind::Service,
            project_id: None,
            instance_id: None,
            source: IpSource::Pool(pool_id),
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

    pub fn project_id(&self) -> &Option<Uuid> {
        &self.project_id
    }

    pub fn instance_id(&self) -> &Option<Uuid> {
        &self.instance_id
    }

    pub fn source(&self) -> &IpSource {
        &self.source
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

impl TryFrom<InstanceExternalIp> for views::ExternalIp {
    type Error = Error;

    fn try_from(ip: InstanceExternalIp) -> Result<Self, Self::Error> {
        let kind = ip.kind.try_into()?;
        Ok(views::ExternalIp { kind, ip: ip.ip.ip() })
    }
}
