// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages allocation and deallocation of external networking resources
//! required for blueprint realization

use anyhow::bail;
use anyhow::Context;
use nexus_db_model::IncompleteNetworkInterface;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::fixed_data::vpc_subnet::DNS_VPC_SUBNET;
use nexus_db_queries::db::fixed_data::vpc_subnet::NEXUS_VPC_SUBNET;
use nexus_db_queries::db::fixed_data::vpc_subnet::NTP_VPC_SUBNET;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::OmicronZoneExternalIp;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use sled_agent_client::ZoneKind;
use slog::error;
use slog::info;
use slog::warn;

pub(crate) async fn ensure_zone_external_networking_allocated(
    opctx: &OpContext,
    datastore: &DataStore,
    all_omicron_zones: impl Iterator<Item = &BlueprintZoneConfig>,
) -> anyhow::Result<()> {
    for z in all_omicron_zones {
        let Some((external_ip, nic)) = z.zone_type.external_networking() else {
            continue;
        };

        let kind = z.zone_type.kind();
        ensure_external_service_ip(opctx, datastore, kind, z.id, external_ip)
            .await?;
        ensure_service_nic(opctx, datastore, kind, z.id, nic).await?;
    }

    Ok(())
}

// Helper function to determine whether a given external IP address is
// already allocated to a specific service zone.
async fn is_external_ip_already_allocated(
    opctx: &OpContext,
    datastore: &DataStore,
    zone_kind: ZoneKind,
    zone_id: OmicronZoneUuid,
    external_ip: OmicronZoneExternalIp,
) -> anyhow::Result<bool> {
    // localhost is used by many components in the test suite.  We can't use
    // the normal path because normally a given external IP must only be
    // used once.  Just treat localhost in the test suite as though it's
    // already allocated.  We do the same in is_nic_already_allocated().
    if cfg!(test) && external_ip.ip().is_loopback() {
        return Ok(true);
    }

    let allocated_ips = datastore
        .external_ip_list_service(opctx, zone_id.into_untyped_uuid())
        .await
        .with_context(|| {
            format!("failed to look up external IPs for {zone_kind} {zone_id}")
        })?;

    // We expect to find either 0 or exactly 1 IP for any given zone. If 0,
    // we know the IP isn't allocated; if 1, we'll check that it matches
    // below.
    let existing_ip = match allocated_ips.as_slice() {
        [] => {
            info!(
                opctx.log, "external IP allocation required for zone";
                "zone_kind" => %zone_kind,
                "zone_id" => %zone_id,
                "ip" => ?external_ip,
            );

            return Ok(false);
        }
        [ip] => ip,
        _ => {
            warn!(
                opctx.log, "zone has multiple IPs allocated";
                "zone_kind" => %zone_kind,
                "zone_id" => %zone_id,
                "want_ip" => ?external_ip,
                "allocated_ips" => ?allocated_ips,
            );
            bail!(
                "zone {zone_id} already has {} IPs allocated (expected 1)",
                allocated_ips.len()
            );
        }
    };

    // We expect this to always succeed; a failure here means we've stored
    // an Omicron zone IP in the database that can't be converted back to an
    // Omicron zone IP!
    let existing_ip = match OmicronZoneExternalIp::try_from(existing_ip) {
        Ok(existing_ip) => existing_ip,
        Err(err) => {
            error!(
                opctx.log, "invalid IP in database for zone";
                "zone_kind" => %zone_kind,
                "zone_id" => %zone_id,
                "ip" => ?existing_ip,
                &err,
            );
            bail!("zone {zone_id} has invalid IP database record: {err}");
        }
    };

    if existing_ip == external_ip {
        info!(
            opctx.log, "found already-allocated external IP";
            "zone_kind" => %zone_kind,
            "zone_id" => %zone_id,
            "ip" => ?external_ip,
        );
        Ok(true)
    } else {
        warn!(
            opctx.log, "zone has unexpected IP allocated";
            "zone_kind" => %zone_kind,
            "zone_id" => %zone_id,
            "want_ip" => ?external_ip,
            "allocated_ip" => ?existing_ip,
        );
        bail!("zone {zone_id} has a different IP allocated ({existing_ip:?})",);
    }
}

// Helper function to determine whether a given NIC is already allocated to
// a specific service zone.
async fn is_nic_already_allocated(
    opctx: &OpContext,
    datastore: &DataStore,
    zone_kind: ZoneKind,
    zone_id: OmicronZoneUuid,
    nic: &NetworkInterface,
) -> anyhow::Result<bool> {
    // See the comment in is_external_ip_already_allocated().
    if cfg!(test) && nic.ip.is_loopback() {
        return Ok(true);
    }

    let allocated_nics = datastore
        .service_list_network_interfaces(opctx, zone_id.into_untyped_uuid())
        .await
        .with_context(|| {
            format!("failed to look up NICs for {zone_kind} {zone_id}")
        })?;

    if !allocated_nics.is_empty() {
        // All the service zones that want NICs only expect to have a single
        // one. Bail out here if this zone already has one or more allocated
        // NICs but not the one we think it needs.
        //
        // This doesn't check the allocated NIC's subnet against our NICs,
        // because that would require an extra DB lookup. We'll assume if
        // these main properties are correct, the subnet is too.
        for allocated_nic in &allocated_nics {
            if allocated_nic.ip.ip() == nic.ip
                && *allocated_nic.mac == nic.mac
                && *allocated_nic.slot == nic.slot
                && allocated_nic.primary == nic.primary
            {
                info!(
                    opctx.log, "found already-allocated NIC";
                    "zone_kind" => %zone_kind,
                    "zone_id" => %zone_id,
                    "nic" => ?allocated_nic,
                );
                return Ok(true);
            }
        }

        warn!(
            opctx.log, "zone has unexpected NICs allocated";
            "zone_kind" => %zone_kind,
            "zone_id" => %zone_id,
            "want_nic" => ?nic,
            "allocated_nics" => ?allocated_nics,
        );

        bail!(
            "zone {zone_id} already has {} non-matching NIC(s) allocated",
            allocated_nics.len()
        );
    }

    info!(
        opctx.log, "NIC allocation required for zone";
        "zone_kind" => %zone_kind,
        "zone_id" => %zone_id,
        "nid" => ?nic,
    );

    Ok(false)
}

async fn ensure_external_service_ip(
    opctx: &OpContext,
    datastore: &DataStore,
    zone_kind: ZoneKind,
    zone_id: OmicronZoneUuid,
    external_ip: OmicronZoneExternalIp,
) -> anyhow::Result<()> {
    // Only attempt to allocate `external_ip` if it isn't already assigned
    // to this zone.
    //
    // Checking for the existing of the external IP and then creating it
    // if not found inserts a classic TOCTOU race: what if another Nexus
    // is running concurrently, we both check and see that the IP is not
    // allocated, then both attempt to create it? We believe this is
    // okay: the loser of the race (i.e., the one whose create tries to
    // commit second) will fail to allocate the IP, which will bubble
    // out and prevent realization of the current blueprint. That's
    // exactly what we want if two Nexuses try to realize the same
    // blueprint at the same time.
    if is_external_ip_already_allocated(
        opctx,
        datastore,
        zone_kind,
        zone_id,
        external_ip,
    )
    .await?
    {
        return Ok(());
    }
    datastore
        .external_ip_allocate_omicron_zone(
            opctx,
            zone_id,
            zone_kind,
            external_ip,
        )
        .await
        .with_context(|| {
            format!(
                "failed to allocate IP to {zone_kind} {zone_id}: \
                     {external_ip:?}"
            )
        })?;

    info!(
        opctx.log, "successfully allocated external IP";
        "zone_kind" => %zone_kind,
        "zone_id" => %zone_id,
        "ip" => ?external_ip,
    );

    Ok(())
}

// All service zones with external connectivity get service vNICs.
async fn ensure_service_nic(
    opctx: &OpContext,
    datastore: &DataStore,
    zone_kind: ZoneKind,
    service_id: OmicronZoneUuid,
    nic: &NetworkInterface,
) -> anyhow::Result<()> {
    // We don't pass `nic.kind` into the database below, but instead
    // explicitly call `service_create_network_interface`. Ensure this is
    // indeed a service NIC.
    match &nic.kind {
        NetworkInterfaceKind::Instance { .. } => {
            bail!("invalid NIC kind (expected service, got instance)")
        }
        NetworkInterfaceKind::Probe { .. } => {
            bail!("invalid NIC kind (expected service, got probe)")
        }
        NetworkInterfaceKind::Service { .. } => (),
    }

    let nic_subnet = match zone_kind {
        ZoneKind::BoundaryNtp => &*NTP_VPC_SUBNET,
        ZoneKind::ExternalDns => &*DNS_VPC_SUBNET,
        ZoneKind::Nexus => &*NEXUS_VPC_SUBNET,
        ZoneKind::Clickhouse
        | ZoneKind::ClickhouseKeeper
        | ZoneKind::CockroachDb
        | ZoneKind::Crucible
        | ZoneKind::CruciblePantry
        | ZoneKind::InternalDns
        | ZoneKind::InternalNtp
        | ZoneKind::Oximeter => {
            bail!("no VPC subnet available for {zone_kind} zone")
        }
    };

    // Only attempt to allocate `nic` if it isn't already assigned to this
    // zone.
    //
    // This is subject to the same kind of TOCTOU race as described for IP
    // allocation in `ensure_external_service_ip`, and we believe it's okay
    // for the same reasons as described there.
    if is_nic_already_allocated(opctx, datastore, zone_kind, service_id, nic)
        .await?
    {
        return Ok(());
    }
    let nic_arg = IncompleteNetworkInterface::new_service(
        nic.id,
        service_id.into_untyped_uuid(),
        nic_subnet.clone(),
        IdentityMetadataCreateParams {
            name: nic.name.clone(),
            description: format!("{zone_kind} service vNIC"),
        },
        nic.ip,
        nic.mac,
        nic.slot,
    )
    .with_context(|| {
        format!(
            "failed to convert NIC into IncompleteNetworkInterface: {nic:?}"
        )
    })?;
    let created_nic = datastore
        .service_create_network_interface(opctx, nic_arg)
        .await
        .map_err(|err| err.into_external())
        .with_context(|| {
            format!(
                "failed to allocate NIC to {zone_kind} {service_id}: \
                     {nic:?}"
            )
        })?;

    // We don't pass all the properties of `nic` into the create request
    // above. Double-check that the properties the DB assigned match
    // what we expect.
    //
    // We do not check `nic.vni`, because it's not stored in the
    // database. (All services are given the constant vni
    // `Vni::SERVICES_VNI`.)
    if created_nic.primary != nic.primary || *created_nic.slot != nic.slot {
        warn!(
            opctx.log, "unexpected property on allocated NIC";
            "db_primary" => created_nic.primary,
            "expected_primary" => nic.primary,
            "db_slot" => *created_nic.slot,
            "expected_slot" => nic.slot,
        );

        // Now what? We've allocated a NIC in the database but it's
        // incorrect. Should we try to delete it? That would be best
        // effort (we could fail to delete, or we could crash between
        // creation and deletion).
        //
        // We only expect services to have one NIC, so the only way it
        // should be possible to get a different primary/slot value is
        // if somehow this same service got a _different_ NIC allocated
        // to it in the TOCTOU race window above. That should be
        // impossible with the way we generate blueprints, so we'll just
        // return a scary error here and expect to never see it.
        bail!(
            "database cleanup required: \
                 unexpected NIC ({created_nic:?}) \
                 allocated for {zone_kind} {service_id}"
        );
    }

    info!(
        opctx.log, "successfully allocated service vNIC";
        "zone_kind" => %zone_kind,
        "zone_id" => %service_id,
        "nic" => ?nic,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
    use nexus_db_model::SqlU16;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::blueprint_zone_type;
    use nexus_types::deployment::BlueprintZoneType;
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::OmicronZoneDataset;
    use nexus_types::deployment::OmicronZoneExternalFloatingAddr;
    use nexus_types::deployment::OmicronZoneExternalFloatingIp;
    use nexus_types::deployment::OmicronZoneExternalSnatIp;
    use nexus_types::identity::Resource;
    use nexus_types::inventory::SourceNatConfig;
    use omicron_common::address::IpRange;
    use omicron_common::address::DNS_OPTE_IPV4_SUBNET;
    use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
    use omicron_common::address::NTP_OPTE_IPV4_SUBNET;
    use omicron_common::address::NUM_SOURCE_NAT_PORTS;
    use omicron_common::api::external::IpNet;
    use omicron_common::api::external::MacAddr;
    use omicron_common::api::external::Vni;
    use omicron_uuid_kinds::ExternalIpUuid;
    use std::net::IpAddr;
    use std::net::Ipv6Addr;
    use std::net::SocketAddr;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

    #[nexus_test]
    async fn test_allocate_external_networking(
        cptestctx: &ControlPlaneTestContext,
    ) {
        // Set up.
        let nexus = &cptestctx.server.apictx().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Create an external IP range we can use for our services.
        let external_ip_range = IpRange::try_from((
            "192.0.2.1".parse::<IpAddr>().unwrap(),
            "192.0.2.100".parse::<IpAddr>().unwrap(),
        ))
        .expect("bad IP range");
        let mut external_ips = external_ip_range.iter();

        // Add the external IP range to the services IP pool.
        let (ip_pool, _) = datastore
            .ip_pools_service_lookup(&opctx)
            .await
            .expect("failed to find service IP pool");
        datastore
            .ip_pool_add_range(&opctx, &ip_pool, &external_ip_range)
            .await
            .expect("failed to expand service IP pool");

        // Generate the values we care about. (Other required zone config params
        // that we don't care about will be filled in below arbitrarily.)

        // Nexus:
        let nexus_id = OmicronZoneUuid::new_v4();
        let nexus_external_ip = OmicronZoneExternalFloatingIp {
            id: ExternalIpUuid::new_v4(),
            ip: external_ips.next().expect("exhausted external_ips"),
        };
        let nexus_nic = NetworkInterface {
            id: Uuid::new_v4(),
            kind: NetworkInterfaceKind::Service {
                id: nexus_id.into_untyped_uuid(),
            },
            name: "test-nexus".parse().expect("bad name"),
            ip: NEXUS_OPTE_IPV4_SUBNET
                .iter()
                .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES)
                .unwrap()
                .into(),
            mac: MacAddr::random_system(),
            subnet: IpNet::from(*NEXUS_OPTE_IPV4_SUBNET),
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
        };

        // External DNS:
        let dns_id = OmicronZoneUuid::new_v4();
        let dns_external_addr = OmicronZoneExternalFloatingAddr {
            id: ExternalIpUuid::new_v4(),
            addr: SocketAddr::new(
                external_ips.next().expect("exhausted external_ips"),
                0,
            ),
        };
        let dns_nic = NetworkInterface {
            id: Uuid::new_v4(),
            kind: NetworkInterfaceKind::Service {
                id: dns_id.into_untyped_uuid(),
            },
            name: "test-external-dns".parse().expect("bad name"),
            ip: DNS_OPTE_IPV4_SUBNET
                .iter()
                .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES)
                .unwrap()
                .into(),
            mac: MacAddr::random_system(),
            subnet: IpNet::from(*DNS_OPTE_IPV4_SUBNET),
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
        };

        // Boundary NTP:
        let ntp_id = OmicronZoneUuid::new_v4();
        let ntp_external_ip = OmicronZoneExternalSnatIp {
            id: ExternalIpUuid::new_v4(),
            snat_cfg: SourceNatConfig::new(
                external_ips.next().expect("exhausted external_ips"),
                NUM_SOURCE_NAT_PORTS,
                2 * NUM_SOURCE_NAT_PORTS - 1,
            )
            .unwrap(),
        };
        let ntp_nic = NetworkInterface {
            id: Uuid::new_v4(),
            kind: NetworkInterfaceKind::Service {
                id: ntp_id.into_untyped_uuid(),
            },
            name: "test-external-ntp".parse().expect("bad name"),
            ip: NTP_OPTE_IPV4_SUBNET
                .iter()
                .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES)
                .unwrap()
                .into(),
            mac: MacAddr::random_system(),
            subnet: IpNet::from(*NTP_OPTE_IPV4_SUBNET),
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
        };

        // Build the `zones` map needed by `ensure_zone_resources_allocated`,
        // with an arbitrary sled_id.
        let zones = vec![
            BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: nexus_id,
                underlay_address: Ipv6Addr::LOCALHOST,
                zone_type: BlueprintZoneType::Nexus(
                    blueprint_zone_type::Nexus {
                        internal_address: "[::1]:0".parse().unwrap(),
                        external_ip: nexus_external_ip,
                        nic: nexus_nic.clone(),
                        external_tls: false,
                        external_dns_servers: Vec::new(),
                    },
                ),
            },
            BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: dns_id,
                underlay_address: Ipv6Addr::LOCALHOST,
                zone_type: BlueprintZoneType::ExternalDns(
                    blueprint_zone_type::ExternalDns {
                        dataset: OmicronZoneDataset {
                            pool_name: format!("oxp_{}", Uuid::new_v4())
                                .parse()
                                .expect("bad name"),
                        },
                        http_address: "[::1]:0".parse().unwrap(),
                        dns_address: dns_external_addr,
                        nic: dns_nic.clone(),
                    },
                ),
            },
            BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: ntp_id,
                underlay_address: Ipv6Addr::LOCALHOST,
                zone_type: BlueprintZoneType::BoundaryNtp(
                    blueprint_zone_type::BoundaryNtp {
                        address: "[::1]:0".parse().unwrap(),
                        ntp_servers: Vec::new(),
                        dns_servers: Vec::new(),
                        domain: None,
                        nic: ntp_nic.clone(),
                        external_ip: ntp_external_ip,
                    },
                ),
            },
        ];

        // Initialize resource allocation: this should succeed and create all
        // the relevant db records.
        ensure_zone_external_networking_allocated(
            &opctx,
            datastore,
            zones.iter(),
        )
        .await
        .with_context(|| format!("{zones:#?}"))
        .unwrap();

        // Check that the external IP records were created.
        let db_nexus_ips = datastore
            .external_ip_list_service(&opctx, nexus_id.into_untyped_uuid())
            .await
            .expect("failed to get external IPs");
        assert_eq!(db_nexus_ips.len(), 1);
        assert!(db_nexus_ips[0].is_service);
        assert_eq!(
            db_nexus_ips[0].parent_id,
            Some(nexus_id.into_untyped_uuid())
        );
        assert_eq!(
            db_nexus_ips[0].id,
            nexus_external_ip.id.into_untyped_uuid()
        );
        assert_eq!(db_nexus_ips[0].ip, nexus_external_ip.ip.into());
        assert_eq!(db_nexus_ips[0].first_port, SqlU16(0));
        assert_eq!(db_nexus_ips[0].last_port, SqlU16(65535));

        let db_dns_ips = datastore
            .external_ip_list_service(&opctx, dns_id.into_untyped_uuid())
            .await
            .expect("failed to get external IPs");
        assert_eq!(db_dns_ips.len(), 1);
        assert!(db_dns_ips[0].is_service);
        assert_eq!(db_dns_ips[0].parent_id, Some(dns_id.into_untyped_uuid()));
        assert_eq!(db_dns_ips[0].id, dns_external_addr.id.into_untyped_uuid());
        assert_eq!(db_dns_ips[0].ip, dns_external_addr.addr.ip().into());
        assert_eq!(db_dns_ips[0].first_port, SqlU16(0));
        assert_eq!(db_dns_ips[0].last_port, SqlU16(65535));

        let db_ntp_ips = datastore
            .external_ip_list_service(&opctx, ntp_id.into_untyped_uuid())
            .await
            .expect("failed to get external IPs");
        assert_eq!(db_ntp_ips.len(), 1);
        assert!(db_ntp_ips[0].is_service);
        assert_eq!(db_ntp_ips[0].parent_id, Some(ntp_id.into_untyped_uuid()));
        assert_eq!(db_ntp_ips[0].id, ntp_external_ip.id.into_untyped_uuid());
        assert_eq!(db_ntp_ips[0].ip, ntp_external_ip.snat_cfg.ip.into());
        assert_eq!(
            db_ntp_ips[0].first_port.0..=db_ntp_ips[0].last_port.0,
            ntp_external_ip.snat_cfg.port_range()
        );

        // Check that the NIC records were created.
        let db_nexus_nics = datastore
            .service_list_network_interfaces(
                &opctx,
                nexus_id.into_untyped_uuid(),
            )
            .await
            .expect("failed to get NICs");
        assert_eq!(db_nexus_nics.len(), 1);
        assert_eq!(db_nexus_nics[0].id(), nexus_nic.id);
        assert_eq!(db_nexus_nics[0].service_id, nexus_id.into_untyped_uuid());
        assert_eq!(db_nexus_nics[0].vpc_id, NEXUS_VPC_SUBNET.vpc_id);
        assert_eq!(db_nexus_nics[0].subnet_id, NEXUS_VPC_SUBNET.id());
        assert_eq!(*db_nexus_nics[0].mac, nexus_nic.mac);
        assert_eq!(db_nexus_nics[0].ip, nexus_nic.ip.into());
        assert_eq!(*db_nexus_nics[0].slot, nexus_nic.slot);
        assert_eq!(db_nexus_nics[0].primary, nexus_nic.primary);

        let db_dns_nics = datastore
            .service_list_network_interfaces(&opctx, dns_id.into_untyped_uuid())
            .await
            .expect("failed to get NICs");
        assert_eq!(db_dns_nics.len(), 1);
        assert_eq!(db_dns_nics[0].id(), dns_nic.id);
        assert_eq!(db_dns_nics[0].service_id, dns_id.into_untyped_uuid());
        assert_eq!(db_dns_nics[0].vpc_id, DNS_VPC_SUBNET.vpc_id);
        assert_eq!(db_dns_nics[0].subnet_id, DNS_VPC_SUBNET.id());
        assert_eq!(*db_dns_nics[0].mac, dns_nic.mac);
        assert_eq!(db_dns_nics[0].ip, dns_nic.ip.into());
        assert_eq!(*db_dns_nics[0].slot, dns_nic.slot);
        assert_eq!(db_dns_nics[0].primary, dns_nic.primary);

        let db_ntp_nics = datastore
            .service_list_network_interfaces(&opctx, ntp_id.into_untyped_uuid())
            .await
            .expect("failed to get NICs");
        assert_eq!(db_ntp_nics.len(), 1);
        assert_eq!(db_ntp_nics[0].id(), ntp_nic.id);
        assert_eq!(db_ntp_nics[0].service_id, ntp_id.into_untyped_uuid());
        assert_eq!(db_ntp_nics[0].vpc_id, NTP_VPC_SUBNET.vpc_id);
        assert_eq!(db_ntp_nics[0].subnet_id, NTP_VPC_SUBNET.id());
        assert_eq!(*db_ntp_nics[0].mac, ntp_nic.mac);
        assert_eq!(db_ntp_nics[0].ip, ntp_nic.ip.into());
        assert_eq!(*db_ntp_nics[0].slot, ntp_nic.slot);
        assert_eq!(db_ntp_nics[0].primary, ntp_nic.primary);

        // We should be able to run the function again with the same inputs, and
        // it should succeed without inserting any new records.
        ensure_zone_external_networking_allocated(
            &opctx,
            datastore,
            zones.iter(),
        )
        .await
        .with_context(|| format!("{zones:#?}"))
        .unwrap();
        assert_eq!(
            db_nexus_ips,
            datastore
                .external_ip_list_service(&opctx, nexus_id.into_untyped_uuid())
                .await
                .expect("failed to get external IPs")
        );
        assert_eq!(
            db_dns_ips,
            datastore
                .external_ip_list_service(&opctx, dns_id.into_untyped_uuid())
                .await
                .expect("failed to get external IPs")
        );
        assert_eq!(
            db_ntp_ips,
            datastore
                .external_ip_list_service(&opctx, ntp_id.into_untyped_uuid())
                .await
                .expect("failed to get external IPs")
        );
        assert_eq!(
            db_nexus_nics,
            datastore
                .service_list_network_interfaces(
                    &opctx,
                    nexus_id.into_untyped_uuid()
                )
                .await
                .expect("failed to get NICs")
        );
        assert_eq!(
            db_dns_nics,
            datastore
                .service_list_network_interfaces(
                    &opctx,
                    dns_id.into_untyped_uuid()
                )
                .await
                .expect("failed to get NICs")
        );
        assert_eq!(
            db_ntp_nics,
            datastore
                .service_list_network_interfaces(
                    &opctx,
                    ntp_id.into_untyped_uuid()
                )
                .await
                .expect("failed to get NICs")
        );

        // Now that we've tested the happy path, try some requests that ought to
        // fail because the request includes an external IP that doesn't match
        // the already-allocated external IPs from above.
        let bogus_ip = external_ips.next().expect("exhausted external_ips");
        for mutate_zones_fn in [
            // non-matching IP on Nexus
            (&|zones: &mut [BlueprintZoneConfig]| {
                for zone in zones {
                    if let BlueprintZoneType::Nexus(
                        blueprint_zone_type::Nexus {
                            ref mut external_ip, ..
                        },
                    ) = &mut zone.zone_type
                    {
                        external_ip.ip = bogus_ip;
                        return format!(
                            "zone {} has a different IP allocated",
                            zone.id
                        );
                    }
                }

                panic!("didn't find expected zone");
            }) as &dyn Fn(&mut [BlueprintZoneConfig]) -> String,
            // non-matching IP on External DNS
            &|zones| {
                for zone in zones {
                    if let BlueprintZoneType::ExternalDns(
                        blueprint_zone_type::ExternalDns {
                            ref mut dns_address,
                            ..
                        },
                    ) = &mut zone.zone_type
                    {
                        dns_address.addr.set_ip(bogus_ip);
                        return format!(
                            "zone {} has a different IP allocated",
                            zone.id
                        );
                    }
                }
                panic!("didn't find expected zone");
            },
            // non-matching SNAT port range on Boundary NTP
            &|zones| {
                for zone in zones {
                    if let BlueprintZoneType::BoundaryNtp(
                        blueprint_zone_type::BoundaryNtp {
                            ref mut external_ip,
                            ..
                        },
                    ) = &mut zone.zone_type
                    {
                        let (mut first, mut last) =
                            external_ip.snat_cfg.port_range_raw();
                        first += NUM_SOURCE_NAT_PORTS;
                        last += NUM_SOURCE_NAT_PORTS;
                        external_ip.snat_cfg = SourceNatConfig::new(
                            external_ip.snat_cfg.ip,
                            first,
                            last,
                        )
                        .unwrap();
                        return format!(
                            "zone {} has a different IP allocated",
                            zone.id
                        );
                    }
                }
                panic!("didn't find expected zone");
            },
        ] {
            // Run `mutate_zones_fn` on our config...
            let (mutated_zones, expected_error) = {
                let mut zones = zones.clone();
                let expected_error = mutate_zones_fn(&mut zones);
                (zones, expected_error)
            };

            // and check that we get the error we expect.
            let err = ensure_zone_external_networking_allocated(
                &opctx,
                datastore,
                mutated_zones.iter(),
            )
            .await
            .expect_err("unexpected success");
            assert!(
                err.to_string().contains(&expected_error),
                "expected {expected_error:?}, got {err:#}"
            );
        }

        // Also try some requests that ought to fail because the request
        // includes a NIC that doesn't match the already-allocated NICs from
        // above.
        //
        // All three zone types have a `nic` property, so here our mutating
        // function only modifies that, and the body of our loop tries it on all
        // three to ensure we get the errors we expect no matter the zone type.
        for mutate_nic_fn in [
            // switch kind from Service to Instance
            (&|_: OmicronZoneUuid, nic: &mut NetworkInterface| {
                match &nic.kind {
                    NetworkInterfaceKind::Instance { .. } => {
                        panic!(
                            "invalid NIC kind (expected service, got instance)"
                        )
                    }
                    NetworkInterfaceKind::Probe { .. } => {
                        panic!(
                            "invalid NIC kind (expected service, got instance)"
                        )
                    }
                    NetworkInterfaceKind::Service { id } => {
                        let id = *id;
                        nic.kind = NetworkInterfaceKind::Instance { id };
                    }
                }
                "invalid NIC kind".to_string()
            })
                as &dyn Fn(OmicronZoneUuid, &mut NetworkInterface) -> String,
            // non-matching IP
            &|zone_id, nic| {
                nic.ip = bogus_ip;
                format!("zone {zone_id} already has 1 non-matching NIC")
            },
        ] {
            // Try this NIC mutation on Nexus...
            let mut mutated_zones = zones.clone();
            for zone in &mut mutated_zones {
                if let BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                    ref mut nic,
                    ..
                }) = &mut zone.zone_type
                {
                    let expected_error = mutate_nic_fn(zone.id, nic);

                    let err = ensure_zone_external_networking_allocated(
                        &opctx,
                        datastore,
                        mutated_zones.iter(),
                    )
                    .await
                    .expect_err("unexpected success");

                    assert!(
                        err.to_string().contains(&expected_error),
                        "expected {expected_error:?}, got {err:#}"
                    );

                    break;
                }
            }

            // ... and again on ExternalDns
            let mut mutated_zones = zones.clone();
            for zone in &mut mutated_zones {
                if let BlueprintZoneType::ExternalDns(
                    blueprint_zone_type::ExternalDns { ref mut nic, .. },
                ) = &mut zone.zone_type
                {
                    let expected_error = mutate_nic_fn(zone.id, nic);

                    let err = ensure_zone_external_networking_allocated(
                        &opctx,
                        datastore,
                        mutated_zones.iter(),
                    )
                    .await
                    .expect_err("unexpected success");

                    assert!(
                        err.to_string().contains(&expected_error),
                        "expected {expected_error:?}, got {err:#}"
                    );

                    break;
                }
            }

            // ... and again on BoundaryNtp
            let mut mutated_zones = zones.clone();
            for zone in &mut mutated_zones {
                if let BlueprintZoneType::BoundaryNtp(
                    blueprint_zone_type::BoundaryNtp { ref mut nic, .. },
                ) = &mut zone.zone_type
                {
                    let expected_error = mutate_nic_fn(zone.id, nic);

                    let err = ensure_zone_external_networking_allocated(
                        &opctx,
                        datastore,
                        mutated_zones.iter(),
                    )
                    .await
                    .expect_err("unexpected success");

                    assert!(
                        err.to_string().contains(&expected_error),
                        "expected {expected_error:?}, got {err:#}"
                    );

                    break;
                }
            }
        }
    }
}
