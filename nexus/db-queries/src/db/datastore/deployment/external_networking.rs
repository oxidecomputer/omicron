// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages allocation and deallocation of external networking resources
//! required for blueprint realization

use crate::context::OpContext;
use crate::db::fixed_data::vpc_subnet::DNS_VPC_SUBNET;
use crate::db::fixed_data::vpc_subnet::NEXUS_VPC_SUBNET;
use crate::db::fixed_data::vpc_subnet::NTP_VPC_SUBNET;
use crate::db::DataStore;
use crate::db::DbConnection;
use nexus_db_model::IncompleteNetworkInterface;
use nexus_db_model::IpPool;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::OmicronZoneExternalIp;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use slog::debug;
use slog::error;
use slog::info;
use slog::warn;
use slog::Logger;
use slog_error_chain::InlineErrorChain;

impl DataStore {
    pub(super) async fn ensure_zone_external_networking_allocated_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        opctx: &OpContext,
        zones_to_allocate: impl Iterator<Item = &BlueprintZoneConfig>,
    ) -> Result<(), Error> {
        // Looking up the service pool ID requires an opctx; we'll do this once
        // up front and reuse the pool ID (which never changes) in the loop
        // below.
        let (_, pool) = self.ip_pools_service_lookup(opctx).await?;

        for z in zones_to_allocate {
            let Some((external_ip, nic)) = z.zone_type.external_networking()
            else {
                continue;
            };

            let log = opctx.log.new(slog::o!(
                "action" => "allocate-external-networking",
                "zone_kind" => z.zone_type.kind().report_str(),
                "zone_id" => z.id.to_string(),
                "ip" => format!("{external_ip:?}"),
                "nic" => format!("{nic:?}"),
            ));

            let kind = z.zone_type.kind();
            self.ensure_external_service_ip(
                conn,
                &pool,
                kind,
                z.id,
                external_ip,
                &log,
            )
            .await?;
            self.ensure_service_nic(conn, kind, z.id, nic, &log).await?;
        }

        Ok(())
    }

    pub(super) async fn ensure_zone_external_networking_deallocated_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        log: &Logger,
        zones_to_deallocate: impl Iterator<Item = &BlueprintZoneConfig>,
    ) -> Result<(), Error> {
        for z in zones_to_deallocate {
            let Some((external_ip, nic)) = z.zone_type.external_networking()
            else {
                continue;
            };

            let kind = z.zone_type.kind();
            let log = log.new(slog::o!(
                "action" => "deallocate-external-networking",
                "zone_kind" => kind.report_str(),
                "zone_id" => z.id.to_string(),
                "ip" => format!("{external_ip:?}"),
                "nic" => format!("{nic:?}"),
            ));

            let deleted_ip = self
                .deallocate_external_ip_on_connection(
                    conn,
                    external_ip.id().into_untyped_uuid(),
                )
                .await?;
            if deleted_ip {
                info!(log, "successfully deleted Omicron zone external IP");
            } else {
                debug!(log, "Omicron zone external IP already deleted");
            }

            let deleted_nic = self
                .service_delete_network_interface_on_connection(
                    conn,
                    z.id.into_untyped_uuid(),
                    nic.id,
                )
                .await
                .map_err(|err| err.into_external())?;
            if deleted_nic {
                info!(log, "successfully deleted Omicron zone vNIC");
            } else {
                debug!(log, "Omicron zone vNIC already deleted");
            }
        }

        Ok(())
    }

    // Helper function to determine whether a given external IP address is
    // already allocated to a specific service zone.
    async fn is_external_ip_already_allocated(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        zone_id: OmicronZoneUuid,
        external_ip: OmicronZoneExternalIp,
        log: &Logger,
    ) -> Result<bool, Error> {
        // localhost is used by many components in the test suite.  We can't use
        // the normal path because normally a given external IP must only be
        // used once.  Just treat localhost in the test suite as though it's
        // already allocated.  We do the same in is_nic_already_allocated().
        if cfg!(any(test, feature = "testing"))
            && external_ip.ip().is_loopback()
        {
            return Ok(true);
        }

        let allocated_ips = self
            .external_ip_list_service_on_connection(
                conn,
                zone_id.into_untyped_uuid(),
            )
            .await?;

        // We expect to find either 0 or exactly 1 IP for any given zone. If 0,
        // we know the IP isn't allocated; if 1, we'll check that it matches
        // below.
        let existing_ip = match allocated_ips.as_slice() {
            [] => {
                info!(log, "external IP allocation required for zone");

                return Ok(false);
            }
            [ip] => ip,
            _ => {
                warn!(
                    log, "zone has multiple IPs allocated";
                    "allocated_ips" => ?allocated_ips,
                );
                return Err(Error::invalid_request(format!(
                    "zone {zone_id} already has {} IPs allocated (expected 1)",
                    allocated_ips.len()
                )));
            }
        };

        // We expect this to always succeed; a failure here means we've stored
        // an Omicron zone IP in the database that can't be converted back to an
        // Omicron zone IP!
        let existing_ip = match OmicronZoneExternalIp::try_from(existing_ip) {
            Ok(existing_ip) => existing_ip,
            Err(err) => {
                error!(log, "invalid IP in database for zone"; &err);
                return Err(Error::invalid_request(format!(
                    "zone {zone_id} has invalid IP database record: {}",
                    InlineErrorChain::new(&err)
                )));
            }
        };

        if existing_ip == external_ip {
            info!(log, "found already-allocated external IP");
            Ok(true)
        } else {
            warn!(
                log, "zone has unexpected IP allocated";
                "allocated_ip" => ?existing_ip,
            );
            return Err(Error::invalid_request(format!(
                "zone {zone_id} has a different IP allocated ({existing_ip:?})",
            )));
        }
    }

    // Helper function to determine whether a given NIC is already allocated to
    // a specific service zone.
    async fn is_nic_already_allocated(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        zone_id: OmicronZoneUuid,
        nic: &NetworkInterface,
        log: &Logger,
    ) -> Result<bool, Error> {
        // See the comment in is_external_ip_already_allocated().
        if cfg!(any(test, feature = "testing")) && nic.ip.is_loopback() {
            return Ok(true);
        }

        let allocated_nics = self
            .service_list_network_interfaces_on_connection(
                conn,
                zone_id.into_untyped_uuid(),
            )
            .await?;

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
                    info!(log, "found already-allocated NIC");
                    return Ok(true);
                }
            }

            warn!(
                log, "zone has unexpected NICs allocated";
                "allocated_nics" => ?allocated_nics,
            );

            return Err(Error::invalid_request(format!(
                "zone {zone_id} already has {} non-matching NIC(s) allocated",
                allocated_nics.len()
            )));
        }

        info!(log, "NIC allocation required for zone");

        Ok(false)
    }

    async fn ensure_external_service_ip(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        pool: &IpPool,
        zone_kind: ZoneKind,
        zone_id: OmicronZoneUuid,
        external_ip: OmicronZoneExternalIp,
        log: &Logger,
    ) -> Result<(), Error> {
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
        if self
            .is_external_ip_already_allocated(conn, zone_id, external_ip, log)
            .await?
        {
            return Ok(());
        }
        self.external_ip_allocate_omicron_zone_on_connection(
            conn,
            pool,
            zone_id,
            zone_kind,
            external_ip,
        )
        .await?;

        info!(log, "successfully allocated external IP");

        Ok(())
    }

    // All service zones with external connectivity get service vNICs.
    async fn ensure_service_nic(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        zone_kind: ZoneKind,
        service_id: OmicronZoneUuid,
        nic: &NetworkInterface,
        log: &Logger,
    ) -> Result<(), Error> {
        // We don't pass `nic.kind` into the database below, but instead
        // explicitly call `service_create_network_interface`. Ensure this is
        // indeed a service NIC.
        match &nic.kind {
            NetworkInterfaceKind::Instance { .. } => {
                return Err(Error::invalid_request(
                    "invalid NIC kind (expected service, got instance)",
                ));
            }
            NetworkInterfaceKind::Probe { .. } => {
                return Err(Error::invalid_request(
                    "invalid NIC kind (expected service, got probe)",
                ));
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
                return Err(Error::invalid_request(format!(
                    "no VPC subnet available for {} zone",
                    zone_kind.report_str()
                )));
            }
        };

        // Only attempt to allocate `nic` if it isn't already assigned to this
        // zone.
        //
        // This is subject to the same kind of TOCTOU race as described for IP
        // allocation in `ensure_external_service_ip`, and we believe it's okay
        // for the same reasons as described there.
        if self.is_nic_already_allocated(conn, service_id, nic, log).await? {
            return Ok(());
        }
        let nic_arg = IncompleteNetworkInterface::new_service(
            nic.id,
            service_id.into_untyped_uuid(),
            nic_subnet.clone(),
            IdentityMetadataCreateParams {
                name: nic.name.clone(),
                description: format!("{} service vNIC", zone_kind.report_str()),
            },
            nic.ip,
            nic.mac,
            nic.slot,
        )?;
        let created_nic = self
            .create_network_interface_raw_conn(conn, nic_arg)
            .await
            .map_err(|err| err.into_external())?;

        // We don't pass all the properties of `nic` into the create request
        // above. Double-check that the properties the DB assigned match
        // what we expect.
        //
        // We do not check `nic.vni`, because it's not stored in the
        // database. (All services are given the constant vni
        // `Vni::SERVICES_VNI`.)
        if created_nic.primary != nic.primary || *created_nic.slot != nic.slot {
            warn!(
                log, "unexpected property on allocated NIC";
                "allocated_primary" => created_nic.primary,
                "allocated_slot" => *created_nic.slot,
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
            return Err(Error::invalid_request(format!(
                "database cleanup required: unexpected NIC ({created_nic:?}) \
                 allocated for {} {service_id}",
                zone_kind.report_str(),
            )));
        }

        info!(log, "successfully allocated service vNIC");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::datastore::test_utils::datastore_test;
    use crate::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
    use anyhow::Context as _;
    use async_bb8_diesel::AsyncSimpleConnection;
    use chrono::DateTime;
    use chrono::Utc;
    use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
    use nexus_db_model::SqlU16;
    use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::deployment::blueprint_zone_type;
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneType;
    use nexus_types::deployment::OmicronZoneExternalFloatingAddr;
    use nexus_types::deployment::OmicronZoneExternalFloatingIp;
    use nexus_types::deployment::OmicronZoneExternalSnatIp;
    use nexus_types::identity::Resource;
    use nexus_types::inventory::SourceNatConfig;
    use omicron_common::address::IpRange;
    use omicron_common::address::IpRangeIter;
    use omicron_common::address::DNS_OPTE_IPV4_SUBNET;
    use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
    use omicron_common::address::NTP_OPTE_IPV4_SUBNET;
    use omicron_common::address::NUM_SOURCE_NAT_PORTS;
    use omicron_common::api::external::MacAddr;
    use omicron_common::api::external::Vni;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ExternalIpUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use oxnet::IpNet;
    use std::net::IpAddr;
    use std::net::Ipv6Addr;
    use std::net::SocketAddr;
    use uuid::Uuid;

    struct Harness {
        external_ips_range: IpRange,
        external_ips: IpRangeIter,

        nexus_id: OmicronZoneUuid,
        nexus_external_ip: OmicronZoneExternalFloatingIp,
        nexus_nic: NetworkInterface,
        dns_id: OmicronZoneUuid,
        dns_external_addr: OmicronZoneExternalFloatingAddr,
        dns_nic: NetworkInterface,
        ntp_id: OmicronZoneUuid,
        ntp_external_ip: OmicronZoneExternalSnatIp,
        ntp_nic: NetworkInterface,
    }

    impl Harness {
        fn new() -> Self {
            let external_ips_range = IpRange::try_from((
                "192.0.2.1".parse::<IpAddr>().unwrap(),
                "192.0.2.100".parse::<IpAddr>().unwrap(),
            ))
            .expect("bad IP range");
            let mut external_ips = external_ips_range.iter();

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
                    .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES)
                    .unwrap()
                    .into(),
                mac: MacAddr::random_system(),
                subnet: IpNet::from(*NEXUS_OPTE_IPV4_SUBNET),
                vni: Vni::SERVICES_VNI,
                primary: true,
                slot: 0,
                transit_ips: vec![],
            };

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
                    .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES)
                    .unwrap()
                    .into(),
                mac: MacAddr::random_system(),
                subnet: IpNet::from(*DNS_OPTE_IPV4_SUBNET),
                vni: Vni::SERVICES_VNI,
                primary: true,
                slot: 0,
                transit_ips: vec![],
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
                    .nth(NUM_INITIAL_RESERVED_IP_ADDRESSES)
                    .unwrap()
                    .into(),
                mac: MacAddr::random_system(),
                subnet: IpNet::from(*NTP_OPTE_IPV4_SUBNET),
                vni: Vni::SERVICES_VNI,
                primary: true,
                slot: 0,
                transit_ips: vec![],
            };

            Self {
                external_ips_range,
                external_ips,
                nexus_id,
                nexus_external_ip,
                nexus_nic,
                dns_id,
                dns_external_addr,
                dns_nic,
                ntp_id,
                ntp_external_ip,
                ntp_nic,
            }
        }

        async fn set_up_service_ip_pool(
            &self,
            opctx: &OpContext,
            datastore: &DataStore,
        ) {
            let (ip_pool, _) = datastore
                .ip_pools_service_lookup(&opctx)
                .await
                .expect("failed to find service IP pool");
            datastore
                .ip_pool_add_range(&opctx, &ip_pool, &self.external_ips_range)
                .await
                .expect("failed to expand service IP pool");
        }

        fn zone_configs(&self) -> Vec<BlueprintZoneConfig> {
            vec![
                BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: self.nexus_id,
                    underlay_address: Ipv6Addr::LOCALHOST,
                    filesystem_pool: Some(ZpoolName::new_external(
                        ZpoolUuid::new_v4(),
                    )),
                    zone_type: BlueprintZoneType::Nexus(
                        blueprint_zone_type::Nexus {
                            internal_address: "[::1]:0".parse().unwrap(),
                            external_ip: self.nexus_external_ip,
                            nic: self.nexus_nic.clone(),
                            external_tls: false,
                            external_dns_servers: Vec::new(),
                        },
                    ),
                },
                BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: self.dns_id,
                    underlay_address: Ipv6Addr::LOCALHOST,
                    filesystem_pool: Some(ZpoolName::new_external(
                        ZpoolUuid::new_v4(),
                    )),
                    zone_type: BlueprintZoneType::ExternalDns(
                        blueprint_zone_type::ExternalDns {
                            dataset: OmicronZoneDataset {
                                pool_name: format!("oxp_{}", Uuid::new_v4())
                                    .parse()
                                    .expect("bad name"),
                            },
                            http_address: "[::1]:0".parse().unwrap(),
                            dns_address: self.dns_external_addr,
                            nic: self.dns_nic.clone(),
                        },
                    ),
                },
                BlueprintZoneConfig {
                    disposition: BlueprintZoneDisposition::InService,
                    id: self.ntp_id,
                    underlay_address: Ipv6Addr::LOCALHOST,
                    filesystem_pool: Some(ZpoolName::new_external(
                        ZpoolUuid::new_v4(),
                    )),
                    zone_type: BlueprintZoneType::BoundaryNtp(
                        blueprint_zone_type::BoundaryNtp {
                            address: "[::1]:0".parse().unwrap(),
                            ntp_servers: Vec::new(),
                            dns_servers: Vec::new(),
                            domain: None,
                            nic: self.ntp_nic.clone(),
                            external_ip: self.ntp_external_ip,
                        },
                    ),
                },
            ]
        }

        async fn assert_ips_exist_in_datastore(&self, datastore: &DataStore) {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            let db_nexus_ips = datastore
                .external_ip_list_service_on_connection(
                    &conn,
                    self.nexus_id.into_untyped_uuid(),
                )
                .await
                .expect("failed to get external IPs");
            assert_eq!(db_nexus_ips.len(), 1);
            assert!(db_nexus_ips[0].is_service);
            assert_eq!(
                db_nexus_ips[0].parent_id,
                Some(self.nexus_id.into_untyped_uuid())
            );
            assert_eq!(
                db_nexus_ips[0].id,
                self.nexus_external_ip.id.into_untyped_uuid()
            );
            assert_eq!(db_nexus_ips[0].ip, self.nexus_external_ip.ip.into());
            assert_eq!(db_nexus_ips[0].first_port, SqlU16(0));
            assert_eq!(db_nexus_ips[0].last_port, SqlU16(65535));

            let db_dns_ips = datastore
                .external_ip_list_service_on_connection(
                    &conn,
                    self.dns_id.into_untyped_uuid(),
                )
                .await
                .expect("failed to get external IPs");
            assert_eq!(db_dns_ips.len(), 1);
            assert!(db_dns_ips[0].is_service);
            assert_eq!(
                db_dns_ips[0].parent_id,
                Some(self.dns_id.into_untyped_uuid())
            );
            assert_eq!(
                db_dns_ips[0].id,
                self.dns_external_addr.id.into_untyped_uuid()
            );
            assert_eq!(
                db_dns_ips[0].ip,
                self.dns_external_addr.addr.ip().into()
            );
            assert_eq!(db_dns_ips[0].first_port, SqlU16(0));
            assert_eq!(db_dns_ips[0].last_port, SqlU16(65535));

            let db_ntp_ips = datastore
                .external_ip_list_service_on_connection(
                    &conn,
                    self.ntp_id.into_untyped_uuid(),
                )
                .await
                .expect("failed to get external IPs");
            assert_eq!(db_ntp_ips.len(), 1);
            assert!(db_ntp_ips[0].is_service);
            assert_eq!(
                db_ntp_ips[0].parent_id,
                Some(self.ntp_id.into_untyped_uuid())
            );
            assert_eq!(
                db_ntp_ips[0].id,
                self.ntp_external_ip.id.into_untyped_uuid()
            );
            assert_eq!(
                db_ntp_ips[0].ip,
                self.ntp_external_ip.snat_cfg.ip.into()
            );
            assert_eq!(
                db_ntp_ips[0].first_port.0..=db_ntp_ips[0].last_port.0,
                self.ntp_external_ip.snat_cfg.port_range()
            );
        }

        async fn assert_nics_exist_in_datastore(&self, datastore: &DataStore) {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            let db_nexus_nics = datastore
                .service_list_network_interfaces_on_connection(
                    &conn,
                    self.nexus_id.into_untyped_uuid(),
                )
                .await
                .expect("failed to get NICs");
            assert_eq!(db_nexus_nics.len(), 1);
            assert_eq!(db_nexus_nics[0].id(), self.nexus_nic.id);
            assert_eq!(
                db_nexus_nics[0].service_id,
                self.nexus_id.into_untyped_uuid()
            );
            assert_eq!(db_nexus_nics[0].vpc_id, NEXUS_VPC_SUBNET.vpc_id);
            assert_eq!(db_nexus_nics[0].subnet_id, NEXUS_VPC_SUBNET.id());
            assert_eq!(*db_nexus_nics[0].mac, self.nexus_nic.mac);
            assert_eq!(db_nexus_nics[0].ip, self.nexus_nic.ip.into());
            assert_eq!(*db_nexus_nics[0].slot, self.nexus_nic.slot);
            assert_eq!(db_nexus_nics[0].primary, self.nexus_nic.primary);

            let db_dns_nics = datastore
                .service_list_network_interfaces_on_connection(
                    &conn,
                    self.dns_id.into_untyped_uuid(),
                )
                .await
                .expect("failed to get NICs");
            assert_eq!(db_dns_nics.len(), 1);
            assert_eq!(db_dns_nics[0].id(), self.dns_nic.id);
            assert_eq!(
                db_dns_nics[0].service_id,
                self.dns_id.into_untyped_uuid()
            );
            assert_eq!(db_dns_nics[0].vpc_id, DNS_VPC_SUBNET.vpc_id);
            assert_eq!(db_dns_nics[0].subnet_id, DNS_VPC_SUBNET.id());
            assert_eq!(*db_dns_nics[0].mac, self.dns_nic.mac);
            assert_eq!(db_dns_nics[0].ip, self.dns_nic.ip.into());
            assert_eq!(*db_dns_nics[0].slot, self.dns_nic.slot);
            assert_eq!(db_dns_nics[0].primary, self.dns_nic.primary);

            let db_ntp_nics = datastore
                .service_list_network_interfaces_on_connection(
                    &conn,
                    self.ntp_id.into_untyped_uuid(),
                )
                .await
                .expect("failed to get NICs");
            assert_eq!(db_ntp_nics.len(), 1);
            assert_eq!(db_ntp_nics[0].id(), self.ntp_nic.id);
            assert_eq!(
                db_ntp_nics[0].service_id,
                self.ntp_id.into_untyped_uuid()
            );
            assert_eq!(db_ntp_nics[0].vpc_id, NTP_VPC_SUBNET.vpc_id);
            assert_eq!(db_ntp_nics[0].subnet_id, NTP_VPC_SUBNET.id());
            assert_eq!(*db_ntp_nics[0].mac, self.ntp_nic.mac);
            assert_eq!(db_ntp_nics[0].ip, self.ntp_nic.ip.into());
            assert_eq!(*db_ntp_nics[0].slot, self.ntp_nic.slot);
            assert_eq!(db_ntp_nics[0].primary, self.ntp_nic.primary);
        }

        async fn assert_ips_are_deleted_in_datastore(
            &self,
            datastore: &DataStore,
        ) {
            use async_bb8_diesel::AsyncRunQueryDsl;
            use diesel::prelude::*;
            use nexus_db_model::schema::external_ip::dsl;

            let conn = datastore.pool_connection_for_tests().await.unwrap();
            let ips: Vec<(Uuid, Option<DateTime<Utc>>)> = datastore
                .transaction_retry_wrapper("read_external_ips")
                .transaction(&conn, |conn| async move {
                    conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL)
                        .await
                        .unwrap();
                    Ok(dsl::external_ip
                        .filter(dsl::parent_id.eq_any([
                            self.nexus_id.into_untyped_uuid(),
                            self.dns_id.into_untyped_uuid(),
                            self.ntp_id.into_untyped_uuid(),
                        ]))
                        .select((dsl::id, dsl::time_deleted))
                        .get_results_async(&conn)
                        .await
                        .unwrap())
                })
                .await
                .unwrap();

            for (id, time_deleted) in &ips {
                eprintln!("{id} {time_deleted:?}");
            }

            // We should have found records for all three zone IPs.
            assert_eq!(ips.len(), 3);
            assert!(ips.iter().any(
                |(id, _)| id == self.nexus_external_ip.id.as_untyped_uuid()
            ));
            assert!(ips.iter().any(
                |(id, _)| id == self.dns_external_addr.id.as_untyped_uuid()
            ));
            assert!(
                ips.iter()
                    .any(|(id, _)| id
                        == self.ntp_external_ip.id.as_untyped_uuid())
            );

            // All rows should indicate deleted records.
            assert!(ips.iter().all(|(_, time_deleted)| time_deleted.is_some()));
        }

        async fn assert_nics_are_deleted_in_datastore(
            &self,
            datastore: &DataStore,
        ) {
            use async_bb8_diesel::AsyncRunQueryDsl;
            use diesel::prelude::*;
            use nexus_db_model::schema::service_network_interface::dsl;

            let conn = datastore.pool_connection_for_tests().await.unwrap();
            let nics: Vec<(Uuid, Option<DateTime<Utc>>)> = datastore
                .transaction_retry_wrapper("read_external_ips")
                .transaction(&conn, |conn| async move {
                    conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL)
                        .await
                        .unwrap();
                    Ok(dsl::service_network_interface
                        .filter(dsl::service_id.eq_any([
                            self.nexus_id.into_untyped_uuid(),
                            self.dns_id.into_untyped_uuid(),
                            self.ntp_id.into_untyped_uuid(),
                        ]))
                        .select((dsl::id, dsl::time_deleted))
                        .get_results_async(&conn)
                        .await
                        .unwrap())
                })
                .await
                .unwrap();

            for (id, time_deleted) in &nics {
                eprintln!("{id} {time_deleted:?}");
            }

            // We should have found records for all three zone NICs.
            assert_eq!(nics.len(), 3);
            assert!(nics.iter().any(|(id, _)| *id == self.nexus_nic.id));
            assert!(nics.iter().any(|(id, _)| *id == self.dns_nic.id));
            assert!(nics.iter().any(|(id, _)| *id == self.ntp_nic.id));

            // All rows should indicate deleted records.
            assert!(nics
                .iter()
                .all(|(_, time_deleted)| time_deleted.is_some()));
        }
    }

    #[tokio::test]
    async fn test_allocate_external_networking() {
        // Set up.
        usdt::register_probes().unwrap();
        let logctx = dev::test_setup_log("test_service_ip_list");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Generate the test values we care about.
        let mut harness = Harness::new();
        harness.set_up_service_ip_pool(&opctx, &datastore).await;

        // Build the `zones` map needed by `ensure_zone_resources_allocated`,
        // with an arbitrary sled_id.
        let zones = harness.zone_configs();

        // Initialize resource allocation: this should succeed and create all
        // the relevant db records.
        datastore
            .ensure_zone_external_networking_allocated_on_connection(
                &datastore.pool_connection_for_tests().await.unwrap(),
                &opctx,
                zones.iter(),
            )
            .await
            .with_context(|| format!("{zones:#?}"))
            .unwrap();

        // Check that the external IP and NIC records were created.
        harness.assert_ips_exist_in_datastore(&datastore).await;
        harness.assert_nics_exist_in_datastore(&datastore).await;

        // We should be able to run the function again with the same inputs, and
        // it should succeed without inserting any new records.
        datastore
            .ensure_zone_external_networking_allocated_on_connection(
                &datastore.pool_connection_for_tests().await.unwrap(),
                &opctx,
                zones.iter(),
            )
            .await
            .with_context(|| format!("{zones:#?}"))
            .unwrap();
        harness.assert_ips_exist_in_datastore(&datastore).await;
        harness.assert_nics_exist_in_datastore(&datastore).await;

        // Now that we've tested the happy path, try some requests that ought to
        // fail because the request includes an external IP that doesn't match
        // the already-allocated external IPs from above.
        let bogus_ip =
            harness.external_ips.next().expect("exhausted external_ips");
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
            let err = datastore
                .ensure_zone_external_networking_allocated_on_connection(
                    &datastore.pool_connection_for_tests().await.unwrap(),
                    &opctx,
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

                    let err = datastore.ensure_zone_external_networking_allocated_on_connection(
                        &datastore.pool_connection_for_tests().await.unwrap(),
                        &opctx,
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

                    let err = datastore.ensure_zone_external_networking_allocated_on_connection(
                        &datastore.pool_connection_for_tests().await.unwrap(),
                        &opctx,
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

                    let err = datastore.ensure_zone_external_networking_allocated_on_connection(
                        &datastore.pool_connection_for_tests().await.unwrap(),
                        &opctx,
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

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_deallocate_external_networking() {
        // Set up.
        usdt::register_probes().unwrap();
        let logctx = dev::test_setup_log("test_service_ip_list");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Generate the test values we care about.
        let harness = Harness::new();
        harness.set_up_service_ip_pool(&opctx, &datastore).await;

        // Build the `zones` map needed by `ensure_zone_resources_allocated`,
        // with an arbitrary sled_id.
        let zones = harness.zone_configs();

        // Initialize resource allocation: this should succeed and create all
        // the relevant db records.
        datastore
            .ensure_zone_external_networking_allocated_on_connection(
                &datastore.pool_connection_for_tests().await.unwrap(),
                &opctx,
                zones.iter(),
            )
            .await
            .with_context(|| format!("{zones:#?}"))
            .unwrap();

        // Check that the external IP and NIC records were created.
        harness.assert_ips_exist_in_datastore(&datastore).await;
        harness.assert_nics_exist_in_datastore(&datastore).await;

        // Deallocate resources: this should succeed and mark all relevant db
        // records deleted.
        datastore
            .ensure_zone_external_networking_deallocated_on_connection(
                &datastore.pool_connection_for_tests().await.unwrap(),
                &logctx.log,
                zones.iter(),
            )
            .await
            .with_context(|| format!("{zones:#?}"))
            .unwrap();

        harness.assert_ips_are_deleted_in_datastore(&datastore).await;
        harness.assert_nics_are_deleted_in_datastore(&datastore).await;

        // This operation should be idempotent: we can run it again, and the
        // records remain deleted.
        datastore
            .ensure_zone_external_networking_deallocated_on_connection(
                &datastore.pool_connection_for_tests().await.unwrap(),
                &logctx.log,
                zones.iter(),
            )
            .await
            .with_context(|| format!("{zones:#?}"))
            .unwrap();

        harness.assert_ips_are_deleted_in_datastore(&datastore).await;
        harness.assert_nics_are_deleted_in_datastore(&datastore).await;

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
