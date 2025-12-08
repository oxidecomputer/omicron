// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Model types for external IPs, both for instances and externally-facing
//! services.

use crate::Name;
use crate::ServiceNetworkInterface;
use crate::SqlU16;
use crate::impl_enum_type;
use chrono::DateTime;
use chrono::Utc;
use db_macros::Resource;
use diesel::Queryable;
use diesel::Selectable;
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::external_ip;
use nexus_db_schema::schema::floating_ip;
use nexus_types::deployment::OmicronZoneExternalFloatingIp;
use nexus_types::deployment::OmicronZoneExternalIp;
use nexus_types::deployment::OmicronZoneExternalSnatIp;
use nexus_types::external_api::params;
use nexus_types::external_api::shared;
use nexus_types::external_api::shared::ProbeExternalIp;
use nexus_types::external_api::shared::ProbeExternalIpKind;
use nexus_types::external_api::views;
use nexus_types::inventory::SourceNatConfig;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadata;
use omicron_common::api::internal::shared::SourceNatConfigError;
use omicron_uuid_kinds::ExternalIpUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::InstanceExternalIpBody;
use sled_agent_types_migrations::latest::inventory::ZoneKind;
use slog_error_chain::SlogInlineError;
use std::convert::TryFrom;
use std::net::IpAddr;
use uuid::Uuid;

impl_enum_type!(
    IpKindEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
    pub enum IpKind;

    SNat => b"snat"
    Ephemeral => b"ephemeral"
    Floating => b"floating"
);

impl_enum_type!(
    IpAttachStateEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
    pub enum IpAttachState;

    Detached => b"detached"
    Attached => b"attached"
    Detaching => b"detaching"
    Attaching => b"attaching"
);

impl std::fmt::Display for IpAttachState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            IpAttachState::Detached => "Detached",
            IpAttachState::Attached => "Attached",
            IpAttachState::Detaching => "Detaching",
            IpAttachState::Attaching => "Attaching",
        })
    }
}

impl std::fmt::Display for IpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            IpKind::Floating => "floating",
            IpKind::Ephemeral => "ephemeral",
            IpKind::SNat => "SNAT",
        })
    }
}

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
#[derive(
    Debug,
    Clone,
    Selectable,
    Queryable,
    Insertable,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    JsonSchema,
)]
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
    pub state: IpAttachState,
    pub is_probe: bool,
}

#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum OmicronZoneExternalIpError {
    #[error("database IP is for an instance")]
    IpIsForInstance,
    #[error("invalid SNAT configuration")]
    InvalidSnatConfig(#[from] SourceNatConfigError),
    #[error(
        "Omicron zone has a range of IPs ({0}); only a single IP is supported"
    )]
    NotSingleIp(IpNetwork),
    #[error(
        "database IP is ephemeral; currently unsupported for Omicron zones"
    )]
    EphemeralIp,
}

impl TryFrom<&'_ ExternalIp> for OmicronZoneExternalIp {
    type Error = OmicronZoneExternalIpError;

    fn try_from(row: &ExternalIp) -> Result<Self, Self::Error> {
        if !row.is_service {
            return Err(OmicronZoneExternalIpError::IpIsForInstance);
        }
        let size = match row.ip.size() {
            ipnetwork::NetworkSize::V4(n) => u128::from(n),
            ipnetwork::NetworkSize::V6(n) => n,
        };
        if size != 1 {
            return Err(OmicronZoneExternalIpError::NotSingleIp(row.ip));
        }

        match row.kind {
            IpKind::SNat => Ok(Self::Snat(OmicronZoneExternalSnatIp {
                id: ExternalIpUuid::from_untyped_uuid(row.id),
                snat_cfg: SourceNatConfig::new(
                    row.ip.ip(),
                    row.first_port.0,
                    row.last_port.0,
                )?,
            })),
            IpKind::Floating => {
                Ok(Self::Floating(OmicronZoneExternalFloatingIp {
                    id: ExternalIpUuid::from_untyped_uuid(row.id),
                    ip: row.ip.ip(),
                }))
            }
            IpKind::Ephemeral => Err(OmicronZoneExternalIpError::EphemeralIp),
        }
    }
}

impl From<ExternalIp> for ProbeExternalIp {
    fn from(value: ExternalIp) -> Self {
        Self {
            ip: value.ip.ip(),
            first_port: value.first_port.0,
            last_port: value.last_port.0,
            kind: value.kind.into(),
        }
    }
}

impl From<IpKind> for ProbeExternalIpKind {
    fn from(value: IpKind) -> Self {
        match value {
            IpKind::SNat => ProbeExternalIpKind::Snat,
            IpKind::Ephemeral => ProbeExternalIpKind::Ephemeral,
            IpKind::Floating => ProbeExternalIpKind::Floating,
        }
    }
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

impl TryFrom<ExternalIp>
    for omicron_common::api::internal::shared::SourceNatConfig
{
    type Error = SourceNatConfigError;

    fn try_from(eip: ExternalIp) -> Result<Self, Self::Error> {
        Self::new(eip.ip.ip(), eip.first_port.0, eip.last_port.0)
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
    is_probe: bool,
    parent_id: Option<Uuid>,
    pool_id: Uuid,
    project_id: Option<Uuid>,
    state: IpAttachState,
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
        let kind = IpKind::SNat;
        Self {
            id,
            name: None,
            description: None,
            time_created: Utc::now(),
            kind,
            is_service: false,
            is_probe: false,
            parent_id: Some(instance_id),
            pool_id,
            project_id: None,
            explicit_ip: None,
            explicit_port_range: None,
            state: kind.initial_state(),
        }
    }

    pub fn for_ephemeral(id: Uuid, pool_id: Uuid) -> Self {
        let kind = IpKind::Ephemeral;
        Self {
            id,
            name: None,
            description: None,
            time_created: Utc::now(),
            kind,
            is_service: false,
            parent_id: None,
            is_probe: false,
            pool_id,
            project_id: None,
            explicit_ip: None,
            explicit_port_range: None,
            state: kind.initial_state(),
        }
    }

    pub fn for_ephemeral_probe(
        id: Uuid,
        instance_id: Uuid,
        pool_id: Uuid,
    ) -> Self {
        let kind = IpKind::Ephemeral;
        Self {
            id,
            name: None,
            description: None,
            time_created: Utc::now(),
            kind: IpKind::Ephemeral,
            is_service: false,
            is_probe: true,
            parent_id: Some(instance_id),
            pool_id,
            project_id: None,
            explicit_ip: None,
            explicit_port_range: None,
            state: kind.initial_state(),
        }
    }

    pub fn for_floating(
        id: Uuid,
        name: &Name,
        description: &str,
        project_id: Uuid,
        pool_id: Uuid,
    ) -> Self {
        let kind = IpKind::Floating;
        Self {
            id,
            name: Some(name.clone()),
            description: Some(description.to_string()),
            time_created: Utc::now(),
            kind,
            is_service: false,
            is_probe: false,
            parent_id: None,
            pool_id,
            project_id: Some(project_id),
            explicit_ip: None,
            explicit_port_range: None,
            state: kind.initial_state(),
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
        let kind = IpKind::Floating;
        Self {
            id,
            name: Some(name.clone()),
            description: Some(description.to_string()),
            time_created: Utc::now(),
            kind,
            is_service: false,
            is_probe: false,
            parent_id: None,
            pool_id,
            project_id: Some(project_id),
            explicit_ip: Some(explicit_ip.into()),
            explicit_port_range: Some((0, u16::MAX.into())),
            state: kind.initial_state(),
        }
    }

    pub fn for_omicron_zone(
        pool_id: Uuid,
        external_ip: OmicronZoneExternalIp,
        zone_id: OmicronZoneUuid,
        zone_kind: ZoneKind,
    ) -> Self {
        let (kind, port_range, name, description, state) = match external_ip {
            OmicronZoneExternalIp::Floating(_) => {
                // We'll name this external IP the same as we'll name the NIC
                // associated with this zone.
                let name = ServiceNetworkInterface::name(zone_id, zone_kind);

                // Using `IpAttachState::Attached` preserves existing behavior,
                // `IpKind::Floating.initial_state()` is `::Detached`. If/when
                // we do more to unify IPs between services and instances, this
                // probably needs to be addressed.
                let state = IpAttachState::Attached;

                (
                    IpKind::Floating,
                    Some((0, u16::MAX.into())),
                    Some(name),
                    Some(zone_kind.report_str().to_string()),
                    state,
                )
            }
            OmicronZoneExternalIp::Snat(OmicronZoneExternalSnatIp {
                snat_cfg,
                ..
            }) => {
                let (first_port, last_port) = snat_cfg.port_range_raw();
                let kind = IpKind::SNat;
                (
                    kind,
                    Some((first_port.into(), last_port.into())),
                    // Only floating IPs are allowed to have names and
                    // descriptions.
                    None,
                    None,
                    kind.initial_state(),
                )
            }
        };

        Self {
            id: external_ip.id().into_untyped_uuid(),
            name,
            description,
            time_created: Utc::now(),
            kind,
            is_service: true,
            is_probe: false,
            parent_id: Some(zone_id.into_untyped_uuid()),
            pool_id,
            project_id: None,
            explicit_ip: Some(IpNetwork::from(external_ip.ip())),
            explicit_port_range: port_range,
            state,
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

    pub fn is_probe(&self) -> &bool {
        &self.is_probe
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

    pub fn state(&self) -> &IpAttachState {
        &self.state
    }

    pub fn explicit_ip(&self) -> &Option<IpNetwork> {
        &self.explicit_ip
    }

    pub fn explicit_port_range(&self) -> &Option<(i32, i32)> {
        &self.explicit_port_range
    }
}

impl IpKind {
    /// The initial state which a new non-service IP should
    /// be allocated in.
    pub fn initial_state(&self) -> IpAttachState {
        match &self {
            IpKind::SNat => IpAttachState::Attached,
            IpKind::Ephemeral => IpAttachState::Detached,
            IpKind::Floating => IpAttachState::Detached,
        }
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
        match ip.kind {
            IpKind::Floating => Ok(views::ExternalIp::Floating(ip.try_into()?)),
            IpKind::Ephemeral => Ok(views::ExternalIp::Ephemeral {
                ip: ip.ip.ip(),
                ip_pool_id: ip.ip_pool_id,
            }),
            IpKind::SNat => Ok(views::ExternalIp::SNat(views::SNatIp {
                ip: ip.ip.ip(),
                first_port: u16::from(ip.first_port),
                last_port: u16::from(ip.last_port),
                ip_pool_id: ip.ip_pool_id,
            })),
        }
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
            id: ip.id.into_untyped_uuid(),
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
            ip_pool_id: ip.ip_pool_id,
            identity,
            project_id: ip.project_id,
            instance_id: ip.parent_id,
        }
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = external_ip)]
pub struct FloatingIpUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
}

impl From<params::FloatingIpUpdate> for FloatingIpUpdate {
    fn from(params: params::FloatingIpUpdate) -> Self {
        Self {
            name: params.identity.name.map(Name),
            description: params.identity.description,
            time_modified: Utc::now(),
        }
    }
}

impl TryFrom<ExternalIp> for InstanceExternalIpBody {
    type Error = Error;

    fn try_from(value: ExternalIp) -> Result<Self, Self::Error> {
        let ip = value.ip.ip();
        match value.kind {
            IpKind::Ephemeral => Ok(InstanceExternalIpBody::Ephemeral(ip)),
            IpKind::Floating => Ok(InstanceExternalIpBody::Floating(ip)),
            IpKind::SNat => Err(Error::invalid_request(
                "cannot dynamically add/remove SNAT allocation",
            )),
        }
    }
}
