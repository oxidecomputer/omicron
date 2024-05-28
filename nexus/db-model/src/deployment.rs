// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for representing the deployed software and configuration in the
//! database

use crate::inventory::ZoneType;
use crate::omicron_zone_config::{OmicronZone, OmicronZoneNic};
use crate::schema::{
    blueprint, bp_omicron_zone, bp_omicron_zone_nic,
    bp_omicron_zones_not_in_service, bp_sled_omicron_zones, bp_target,
};
use crate::{ipv6, Generation, MacAddr, Name, SqlU16, SqlU32, SqlU8};
use chrono::{DateTime, Utc};
use ipnetwork::IpNetwork;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZonesConfig;
use omicron_common::api::internal::shared::NetworkInterface;
use uuid::Uuid;

/// See [`nexus_types::deployment::Blueprint`].
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = blueprint)]
pub struct Blueprint {
    pub id: Uuid,
    pub parent_blueprint_id: Option<Uuid>,
    pub internal_dns_version: Generation,
    pub external_dns_version: Generation,
    pub time_created: DateTime<Utc>,
    pub creator: String,
    pub comment: String,
}

impl From<&'_ nexus_types::deployment::Blueprint> for Blueprint {
    fn from(bp: &'_ nexus_types::deployment::Blueprint) -> Self {
        Self {
            id: bp.id,
            parent_blueprint_id: bp.parent_blueprint_id,
            internal_dns_version: Generation(bp.internal_dns_version),
            external_dns_version: Generation(bp.external_dns_version),
            time_created: bp.time_created,
            creator: bp.creator.clone(),
            comment: bp.comment.clone(),
        }
    }
}

impl From<Blueprint> for nexus_types::deployment::BlueprintMetadata {
    fn from(value: Blueprint) -> Self {
        Self {
            id: value.id,
            parent_blueprint_id: value.parent_blueprint_id,
            internal_dns_version: *value.internal_dns_version,
            external_dns_version: *value.external_dns_version,
            time_created: value.time_created,
            creator: value.creator,
            comment: value.comment,
        }
    }
}

/// See [`nexus_types::deployment::BlueprintTarget`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_target)]
pub struct BpTarget {
    pub version: SqlU32,
    pub blueprint_id: Uuid,
    pub enabled: bool,
    pub time_made_target: DateTime<Utc>,
}

impl BpTarget {
    pub fn new(version: u32, target: BlueprintTarget) -> Self {
        Self {
            version: version.into(),
            blueprint_id: target.target_id,
            enabled: target.enabled,
            time_made_target: target.time_made_target,
        }
    }
}

impl From<BpTarget> for nexus_types::deployment::BlueprintTarget {
    fn from(value: BpTarget) -> Self {
        Self {
            target_id: value.blueprint_id,
            enabled: value.enabled,
            time_made_target: value.time_made_target,
        }
    }
}

/// See [`nexus_types::deployment::OmicronZonesConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_sled_omicron_zones)]
pub struct BpSledOmicronZones {
    pub blueprint_id: Uuid,
    pub sled_id: Uuid,
    pub generation: Generation,
}

impl BpSledOmicronZones {
    pub fn new(
        blueprint_id: Uuid,
        sled_id: Uuid,
        zones_config: &BlueprintZonesConfig,
    ) -> Self {
        Self {
            blueprint_id,
            sled_id,
            generation: Generation(zones_config.generation),
        }
    }
}

/// See [`nexus_types::deployment::OmicronZoneConfig`].
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_omicron_zone)]
pub struct BpOmicronZone {
    pub blueprint_id: Uuid,
    pub sled_id: Uuid,
    pub id: Uuid,
    pub underlay_address: ipv6::Ipv6Addr,
    pub zone_type: ZoneType,
    pub primary_service_ip: ipv6::Ipv6Addr,
    pub primary_service_port: SqlU16,
    pub second_service_ip: Option<IpNetwork>,
    pub second_service_port: Option<SqlU16>,
    pub dataset_zpool_name: Option<String>,
    pub bp_nic_id: Option<Uuid>,
    pub dns_gz_address: Option<ipv6::Ipv6Addr>,
    pub dns_gz_address_index: Option<SqlU32>,
    pub ntp_ntp_servers: Option<Vec<String>>,
    pub ntp_dns_servers: Option<Vec<IpNetwork>>,
    pub ntp_domain: Option<String>,
    pub nexus_external_tls: Option<bool>,
    pub nexus_external_dns_servers: Option<Vec<IpNetwork>>,
    pub snat_ip: Option<IpNetwork>,
    pub snat_first_port: Option<SqlU16>,
    pub snat_last_port: Option<SqlU16>,
}

impl BpOmicronZone {
    pub fn new(
        blueprint_id: Uuid,
        sled_id: Uuid,
        zone: &BlueprintZoneConfig,
    ) -> Result<Self, anyhow::Error> {
        let zone = OmicronZone::new(sled_id, &zone.config)?;
        Ok(Self {
            blueprint_id,
            sled_id: zone.sled_id,
            id: zone.id,
            underlay_address: zone.underlay_address,
            zone_type: zone.zone_type,
            primary_service_ip: zone.primary_service_ip,
            primary_service_port: zone.primary_service_port,
            second_service_ip: zone.second_service_ip,
            second_service_port: zone.second_service_port,
            dataset_zpool_name: zone.dataset_zpool_name,
            bp_nic_id: zone.nic_id,
            dns_gz_address: zone.dns_gz_address,
            dns_gz_address_index: zone.dns_gz_address_index,
            ntp_ntp_servers: zone.ntp_ntp_servers,
            ntp_dns_servers: zone.ntp_dns_servers,
            ntp_domain: zone.ntp_domain,
            nexus_external_tls: zone.nexus_external_tls,
            nexus_external_dns_servers: zone.nexus_external_dns_servers,
            snat_ip: zone.snat_ip,
            snat_first_port: zone.snat_first_port,
            snat_last_port: zone.snat_last_port,
        })
    }

    pub fn into_blueprint_zone_config(
        self,
        nic_row: Option<BpOmicronZoneNic>,
        disposition: BlueprintZoneDisposition,
    ) -> Result<BlueprintZoneConfig, anyhow::Error> {
        let zone = OmicronZone {
            sled_id: self.sled_id,
            id: self.id,
            underlay_address: self.underlay_address,
            zone_type: self.zone_type,
            primary_service_ip: self.primary_service_ip,
            primary_service_port: self.primary_service_port,
            second_service_ip: self.second_service_ip,
            second_service_port: self.second_service_port,
            dataset_zpool_name: self.dataset_zpool_name,
            nic_id: self.bp_nic_id,
            dns_gz_address: self.dns_gz_address,
            dns_gz_address_index: self.dns_gz_address_index,
            ntp_ntp_servers: self.ntp_ntp_servers,
            ntp_dns_servers: self.ntp_dns_servers,
            ntp_domain: self.ntp_domain,
            nexus_external_tls: self.nexus_external_tls,
            nexus_external_dns_servers: self.nexus_external_dns_servers,
            snat_ip: self.snat_ip,
            snat_first_port: self.snat_first_port,
            snat_last_port: self.snat_last_port,
        };
        let config =
            zone.into_omicron_zone_config(nic_row.map(OmicronZoneNic::from))?;
        Ok(BlueprintZoneConfig { config, disposition })
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_omicron_zone_nic)]
pub struct BpOmicronZoneNic {
    blueprint_id: Uuid,
    pub id: Uuid,
    name: Name,
    ip: IpNetwork,
    mac: MacAddr,
    subnet: IpNetwork,
    vni: SqlU32,
    is_primary: bool,
    slot: SqlU8,
}

impl From<BpOmicronZoneNic> for OmicronZoneNic {
    fn from(value: BpOmicronZoneNic) -> Self {
        OmicronZoneNic {
            id: value.id,
            name: value.name,
            ip: value.ip,
            mac: value.mac,
            subnet: value.subnet,
            vni: value.vni,
            is_primary: value.is_primary,
            slot: value.slot,
        }
    }
}

impl BpOmicronZoneNic {
    pub fn new(
        blueprint_id: Uuid,
        zone: &BlueprintZoneConfig,
    ) -> Result<Option<BpOmicronZoneNic>, anyhow::Error> {
        let zone_nic = OmicronZoneNic::new(&zone.config)?;
        Ok(zone_nic.map(|nic| Self {
            blueprint_id,
            id: nic.id,
            name: nic.name,
            ip: nic.ip,
            mac: nic.mac,
            subnet: nic.subnet,
            vni: nic.vni,
            is_primary: nic.is_primary,
            slot: nic.slot,
        }))
    }

    pub fn into_network_interface_for_zone(
        self,
        zone_id: Uuid,
    ) -> Result<NetworkInterface, anyhow::Error> {
        let zone_nic = OmicronZoneNic::from(self);
        zone_nic.into_network_interface_for_zone(zone_id)
    }
}

/// Nexus wants to think in terms of "zones in service", but since most zones of
/// most blueprints are in service, we store the zones NOT in service in the
/// database. We handle that inversion internally in the db-queries layer.
#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = bp_omicron_zones_not_in_service)]
pub struct BpOmicronZoneNotInService {
    pub blueprint_id: Uuid,
    pub bp_omicron_zone_id: Uuid,
}
