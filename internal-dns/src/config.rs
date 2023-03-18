// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Control plane DNS zone configuration
//!
//! RFD 206 defines service discovery:
//!
//! > Service discovery is the mechanism by which a service, S, locates backend
//! > instances of a given service, D, that are suppposed to be in service
//! > currently, so that S may utilize `D’s functionality.
//!
//! RFD 248 describes how components of Omicron (the control plane) will
//! discover each other using internal DNS, using the DNS zone
//! "control-plane.oxide.internal".  The RFD goes on to describe how we
//! structure the DNS names.
//!
//! In networking, a **host** is just a component sitting on a network.  For
//! us, each sled's global zone would be a host.  Each non-global zone that
//! makes up the control plane would also be a host.  Each host might have
//! several things running there.  For example, our DNS servers have both an
//! HTTP server that's used to configure the DNS server as well as an actual
//! DNS \[protocol\] server.
//!
//! **Services**, **instances**, and **backends** are specific terms, too.
//! From RFD 248:
//!
//! > Consider the case where Nexus depends on CockroachDB. We say that Nexus
//! > is a service. It may have one or more instances, each being a different
//! > Unix process, typically running on a different host.
//! >
//! > CockroachDB is also a service. It too may have many instances.
//! >
//! > In the context of a service, we refer to instances of a dependent
//! > service as backends. Different backends of a service are
//! > interchangeable. So for Nexus, there’s only one service containing all
//! > the backends.
//! >
//! > In the end, we might have:
//! >
//! > * Service Nexus instance N1 at 192.168.0.10 port 12220
//! > * Service Nexus instance N2 at 192.168.0.11 port 12220
//! > * Service CockroachDB instance D1 at 192.168.0.6 port 26257
//! > * Service CockroachDB instance D2 at 192.168.0.7 port 26257
//! > * Service CockroachDB instance D3 at 192.168.0.8 port 26257
//! >
//! > Nexus thus has two backends. CockroachDB has three backends.
//! >
//! > For something like Sled Agent, each Sled Agent would be its own service
//! > with exactly one backend, since the Sled Agents cannot be treated as
//! > interchangeable with each other.
//!
//! DNS allows us to express services as SRV records.  These point at components
//! running on specific hosts.  Those hosts are described using AAAA records.
//!
//! If you take the full set of DNS records for all DNS zones operated by one of
//! our DNS servers, we call that the **DNS data** or **configuration** for that
//! server.  This data is assembled by RSS (before the rack is set up) and Nexus
//! (after the rack is set up) and propagated to the DNS servers.
//!
//! This module provides types used to assemble that configuration.

use crate::names::{BackendName, ServiceName, DNS_ZONE};
use anyhow::{anyhow, ensure};
use dns_service_client::types::{
    DnsConfigParams, DnsConfigZone, DnsKv, DnsRecord, DnsRecordKey,
};
use std::collections::BTreeMap;
use std::net::Ipv6Addr;
use uuid::Uuid;

/// Describes the DNS name that will be used for a control plane service
///
/// This does not describe the SRV record itself.
#[derive(Clone, Debug, Hash, Eq, Ord, PartialEq, PartialOrd)]
pub enum SRV {
    /// A service identified and accessed by name, such as "nexus", "CRDB", etc.
    ///
    /// This is used in cases where services are interchangeable.
    Service(ServiceName),

    /// A service identified by name and a unique identifier.
    ///
    /// This is used in cases where services are not interchangeable, such as
    /// for the Sled agent.
    Backend(BackendName, Uuid),
}

impl SRV {
    /// Returns the DNS name for this service, ignoring the zone part of the DNS
    /// name
    pub(crate) fn dns_name(&self) -> String {
        match &self {
            SRV::Service(name) => format!("_{}._tcp", name),
            SRV::Backend(name, id) => format!("_{}._tcp.{}", name, id),
        }
    }
}

/// Describes the DNS name used for a control plane host
///
/// This does not describe the AAA record itself.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum AAAA {
    /// Identifies an AAAA record for a sled.
    Sled(Uuid),

    /// Identifies an AAAA record for a zone within a sled.
    Zone(Uuid),
}

impl AAAA {
    /// Returns the DNS name for this host, ignoring the zone part of the DNS
    /// name
    pub(crate) fn dns_name(&self) -> String {
        match &self {
            AAAA::Sled(id) => format!("{}.sled", id),
            AAAA::Zone(id) => format!("{}.host", id),
        }
    }
}

/// Builder for assembling DNS data for the control plane's DNS zone
///
/// `DnsConfigBuilder` provides a much simpler interface for constructing DNS
/// zone data than using `DnsConfig` directly.  That's because it makes a number
/// of assumptions that are true of the control plane DNS zone (all described in
/// RFD 248), but not true in general about DNS zones:
///
/// - We assume that there are only two kinds of hosts: a "sled" (an illumos
///   global zone) or a "zone (an illumos non-global zone).  (Both of these are
///   unrelated to DNS zones -- an unfortunate overlap in terminology.) It might
///   seem arbitrary to draw a line between these at all, but they play such
///   different roles in the control plane that it's useful to know when looking
///   at a DNS name if it's referring to a sled or some other zone.
/// - We assume that the DNS names for each kind of host are assembled in a
///   predictable way.  Different hosts of the same kind differ only in their
///   first DNS label, which is their uuid.
/// - We assume that each host has exactly one IP address, and that it's an IPv6
///   address.
/// - We assume that each backend for each service is a host defined elsewhere
///   in the DNS zone.
///
/// This builder ensures that the constructed DNS data satisfies these
/// assumptions.
pub struct DnsConfigBuilder {
    /// set of hosts of type "sled" that have been configured so far, mapping
    /// each sled's unique uuid to its sole IPv6 address on the control plane
    /// network
    sleds: BTreeMap<Uuid, Ipv6Addr>,

    /// set of hosts of type "zone" that have been configured so far, mapping
    /// each zone's unique uuid to its sole IPv6 address on the control plane
    /// network
    zones: BTreeMap<Uuid, Ipv6Addr>,

    /// set of services (see module-level comment) that have been configured so
    /// far, mapping the name of the service (encapsulated in an [`SRV`]) to the
    /// backends configured for that service.  The set of backends is
    /// represented as a mapping from the zone's uuid to the port on which it's
    /// running the service.
    service_instances_zones: BTreeMap<SRV, BTreeMap<Uuid, u16>>,

    /// similar to service_instances_zones, but for services that run on sleds
    service_instances_sleds: BTreeMap<SRV, BTreeMap<Uuid, u16>>,
}

/// Describes a host of type "sled" in the control plane DNS zone
#[derive(Debug)]
pub struct Sled(Uuid);

/// Describes a host of type "zone" (an illumos zone) in the control plane DNS
/// zone
#[derive(Debug)]
pub struct Zone(Uuid);

impl DnsConfigBuilder {
    pub fn new() -> Self {
        DnsConfigBuilder {
            sleds: BTreeMap::new(),
            zones: BTreeMap::new(),
            service_instances_zones: BTreeMap::new(),
            service_instances_sleds: BTreeMap::new(),
        }
    }

    /// Add a new host of type "sled" to the configuration
    ///
    /// Returns a [`Sled`] that can be used with [`Self::service_backend_sled()`] to
    /// specify that this sled is a backend for some higher-level service.
    ///
    /// # Errors
    ///
    /// This function fails only if the given sled has already been added to the
    /// configuration.
    pub fn host_sled(
        &mut self,
        sled_id: Uuid,
        addr: Ipv6Addr,
    ) -> anyhow::Result<Sled> {
        match self.sleds.insert(sled_id, addr) {
            None => Ok(Sled(sled_id)),
            Some(existing) => Err(anyhow!(
                "multiple definitions for sled {} (previously {}, now {})",
                sled_id,
                existing,
                addr,
            )),
        }
    }

    /// Add a new host of type "zone" to the configuration
    ///
    /// Returns a [`Zone`] that can be used with [`Self::service_backend_zone()`] to
    /// specify that this zone is a backend for some higher-level service.
    ///
    /// # Errors
    ///
    /// This function fails only if the given zone has already been added to the
    /// configuration.
    pub fn host_zone(
        &mut self,
        zone_id: Uuid,
        addr: Ipv6Addr,
    ) -> anyhow::Result<Zone> {
        match self.zones.insert(zone_id, addr) {
            None => Ok(Zone(zone_id)),
            Some(existing) => Err(anyhow!(
                "multiple definitions for zone {} (previously {}, now {})",
                zone_id,
                existing,
                addr
            )),
        }
    }

    /// Specify that service `service` has a backend instance running in the
    /// given (zone) host.
    ///
    /// # Errors
    ///
    /// This function fails only if the given host has already been added as a
    /// backend for this service.
    pub fn service_backend_zone(
        &mut self,
        service: SRV,
        zone: &Zone,
        port: u16,
    ) -> anyhow::Result<()> {
        // Although one can only get a `Zone` by adding it to a
        // `DnsConfigBuilder`, it's possible that it was added to a different
        // DnsBuilder.
        ensure!(
            self.zones.contains_key(&zone.0),
            "zone {:?} has not been defined",
            zone.0
        );

        let set = self
            .service_instances_zones
            .entry(service.clone())
            .or_insert_with(BTreeMap::new);
        let zone_id = zone.0;
        match set.insert(zone_id, port) {
            None => Ok(()),
            Some(existing) => Err(anyhow!(
                "service {}: zone {}: registered twice \
                (previously port {}, now {})",
                service.dns_name(),
                zone_id,
                existing,
                port
            )),
        }
    }

    /// Specify that service `service` has a backend instance running directly
    /// on the given (sled) host (generally in the sled's global zone)
    ///
    /// # Errors
    ///
    /// This function fails only if the given host has already been added as a
    /// backend for this service.
    pub fn service_backend_sled(
        &mut self,
        service: SRV,
        sled: &Sled,
        port: u16,
    ) -> anyhow::Result<()> {
        // Although one can only get a `Sled` by adding it to a
        // `DnsConfigBuilder`, it's possible that it was added to a different
        // DnsBuilder.
        ensure!(
            self.sleds.contains_key(&sled.0),
            "sled {:?} has not been defined",
            sled.0
        );

        let set = self
            .service_instances_sleds
            .entry(service.clone())
            .or_insert_with(BTreeMap::new);
        let sled_id = sled.0;
        match set.insert(sled_id, port) {
            None => Ok(()),
            Some(existing) => Err(anyhow!(
                "service {}: sled {}: registered twice \
                (previously port {}, now {})",
                service.dns_name(),
                sled_id,
                existing,
                port
            )),
        }
    }

    /// Construct a complete [`DnsConfigParams`] (suitable for propagating to
    /// our DNS servers) for the control plane DNS zone described up to this
    /// point
    pub fn build(self) -> DnsConfigParams {
        // Assemble the set of "AAAA" records for sleds.
        let sled_records = self.sleds.into_iter().map(|(sled_id, sled_ip)| {
            let name = AAAA::Sled(sled_id).dns_name();
            DnsKv {
                key: DnsRecordKey { name },
                records: vec![DnsRecord::Aaaa(sled_ip)],
            }
        });

        // Assemble the set of AAAA records for zones.
        let zone_records = self.zones.into_iter().map(|(zone_id, zone_ip)| {
            let name = AAAA::Zone(zone_id).dns_name();
            DnsKv {
                key: DnsRecordKey { name },
                records: vec![DnsRecord::Aaaa(zone_ip)],
            }
        });

        // Assemble the set of SRV records, which implicitly point back at
        // zones' AAAA records.
        let srv_records_zones = self.service_instances_zones.into_iter().map(
            |(service_name, zone2port)| {
                let name = service_name.dns_name();
                let records = zone2port
                    .into_iter()
                    .map(|(zone_id, port)| {
                        DnsRecord::Srv(dns_service_client::types::Srv {
                            prio: 0,
                            weight: 0,
                            port,
                            target: format!(
                                "{}.{}",
                                AAAA::Zone(zone_id).dns_name(),
                                DNS_ZONE
                            ),
                        })
                    })
                    .collect();

                DnsKv { key: DnsRecordKey { name }, records }
            },
        );

        let srv_records_sleds = self.service_instances_sleds.into_iter().map(
            |(service_name, sled2port)| {
                let name = service_name.dns_name();
                let records = sled2port
                    .into_iter()
                    .map(|(sled_id, port)| {
                        DnsRecord::Srv(dns_service_client::types::Srv {
                            prio: 0,
                            weight: 0,
                            port,
                            target: format!(
                                "{}.{}",
                                AAAA::Sled(sled_id).dns_name(),
                                DNS_ZONE
                            ),
                        })
                    })
                    .collect();

                DnsKv { key: DnsRecordKey { name }, records }
            },
        );

        let all_records = sled_records
            .chain(zone_records)
            .chain(srv_records_sleds)
            .chain(srv_records_zones)
            .collect();

        DnsConfigParams {
            generation: 1,
            time_created: chrono::Utc::now(),
            zones: vec![DnsConfigZone {
                zone_name: DNS_ZONE.to_owned(),
                records: all_records,
            }],
        }
    }
}

#[cfg(test)]
mod test {
    use super::{BackendName, DnsConfigBuilder, ServiceName, AAAA, SRV};
    use crate::DNS_ZONE;
    use std::{io::Write, net::Ipv6Addr};
    use uuid::Uuid;

    #[test]
    fn display_srv_service() {
        assert_eq!(
            SRV::Service(ServiceName::Clickhouse).dns_name(),
            "_clickhouse._tcp",
        );
        assert_eq!(
            SRV::Service(ServiceName::Cockroach).dns_name(),
            "_cockroach._tcp",
        );
        assert_eq!(
            SRV::Service(ServiceName::InternalDNS).dns_name(),
            "_internalDNS._tcp",
        );
        assert_eq!(SRV::Service(ServiceName::Nexus).dns_name(), "_nexus._tcp",);
        assert_eq!(
            SRV::Service(ServiceName::Oximeter).dns_name(),
            "_oximeter._tcp",
        );
        assert_eq!(
            SRV::Service(ServiceName::Dendrite).dns_name(),
            "_dendrite._tcp",
        );
        assert_eq!(
            SRV::Service(ServiceName::CruciblePantry).dns_name(),
            "_crucible-pantry._tcp",
        );
    }

    #[test]
    fn display_srv_backend() {
        let uuid = Uuid::nil();
        assert_eq!(
            SRV::Backend(BackendName::Crucible, uuid).dns_name(),
            "_crucible._tcp.00000000-0000-0000-0000-000000000000",
        );
        assert_eq!(
            SRV::Backend(BackendName::SledAgent, uuid).dns_name(),
            "_sledagent._tcp.00000000-0000-0000-0000-000000000000",
        );
    }

    #[test]
    fn display_aaaa() {
        let uuid = Uuid::nil();
        assert_eq!(
            AAAA::Sled(uuid).dns_name(),
            "00000000-0000-0000-0000-000000000000.sled",
        );
        assert_eq!(
            AAAA::Zone(uuid).dns_name(),
            "00000000-0000-0000-0000-000000000000.host",
        );
    }

    // DnsConfigBuilder tests

    const SLED1_UUID: &'static str = "001de000-51ed-4000-8000-000000000001";
    const SLED2_UUID: &'static str = "001de000-51ed-4000-8000-000000000002";
    const ZONE1_UUID: &'static str = "001de000-c04e-4000-8000-000000000001";
    const ZONE2_UUID: &'static str = "001de000-c04e-4000-8000-000000000002";
    const ZONE3_UUID: &'static str = "001de000-c04e-4000-8000-000000000003";
    const ZONE4_UUID: &'static str = "001de000-c04e-4000-8000-000000000004";
    const SLED1_IP: Ipv6Addr = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1);
    const SLED2_IP: Ipv6Addr = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 2);
    const ZONE1_IP: Ipv6Addr = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 1, 1);
    const ZONE2_IP: Ipv6Addr = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 1, 2);
    const ZONE3_IP: Ipv6Addr = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 1, 3);
    const ZONE4_IP: Ipv6Addr = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 1, 4);

    #[test]
    fn test_builder_output() {
        let mut output = std::io::Cursor::new(Vec::new());

        let sled1_uuid: Uuid = SLED1_UUID.parse().unwrap();
        let sled2_uuid: Uuid = SLED2_UUID.parse().unwrap();
        let zone1_uuid: Uuid = ZONE1_UUID.parse().unwrap();
        let zone2_uuid: Uuid = ZONE2_UUID.parse().unwrap();
        let zone3_uuid: Uuid = ZONE3_UUID.parse().unwrap();
        let zone4_uuid: Uuid = ZONE4_UUID.parse().unwrap();

        let builder_empty = DnsConfigBuilder::new();

        let builder_hosts_only = {
            let mut b = DnsConfigBuilder::new();
            b.host_sled(sled1_uuid, SLED1_IP).unwrap();
            b.host_sled(sled2_uuid, SLED2_IP).unwrap();
            b
        };

        let builder_zones_only = {
            let mut b = DnsConfigBuilder::new();
            b.host_zone(zone1_uuid, ZONE1_IP).unwrap();
            b.host_zone(zone2_uuid, ZONE2_IP).unwrap();
            b
        };

        let builder_non_trivial = {
            let mut b = DnsConfigBuilder::new();

            // Some hosts
            let sled1 = b.host_sled(sled1_uuid, SLED1_IP).unwrap();
            b.host_sled(sled2_uuid, SLED2_IP).unwrap();

            // Some zones (including some not used by services)
            let zone1 = b.host_zone(zone1_uuid, ZONE1_IP).unwrap();
            let zone2 = b.host_zone(zone2_uuid, ZONE2_IP).unwrap();
            let zone3 = b.host_zone(zone3_uuid, ZONE3_IP).unwrap();
            let _ = b.host_zone(zone4_uuid, ZONE4_IP).unwrap();

            // A service with two backends on two zones using two different
            // ports
            b.service_backend_zone(
                SRV::Service(ServiceName::Nexus),
                &zone1,
                123,
            )
            .unwrap();
            b.service_backend_zone(
                SRV::Service(ServiceName::Nexus),
                &zone2,
                124,
            )
            .unwrap();

            // Another service, using one of the same zones (so the same zone is
            // used in two services)
            b.service_backend_zone(
                SRV::Service(ServiceName::Oximeter),
                &zone2,
                125,
            )
            .unwrap();
            b.service_backend_zone(
                SRV::Service(ServiceName::Oximeter),
                &zone3,
                126,
            )
            .unwrap();

            // A sharded service
            b.service_backend_sled(
                SRV::Backend(BackendName::SledAgent, sled1_uuid),
                &sled1,
                123,
            )
            .unwrap();

            b
        };

        for (label, builder) in [
            ("empty", builder_empty),
            ("hosts_only", builder_hosts_only),
            ("zones_only", builder_zones_only),
            ("non_trivial", builder_non_trivial),
        ] {
            let config = builder.build();
            assert_eq!(config.generation, 1);
            assert_eq!(config.zones.len(), 1);
            assert_eq!(config.zones[0].zone_name, DNS_ZONE);
            write!(&mut output, "builder: {:?}\n", label).unwrap();
            serde_json::to_writer_pretty(&mut output, &config.zones[0].records)
                .unwrap();
            write!(&mut output, "\n").unwrap();
        }

        expectorate::assert_contents(
            "tests/output/internal-dns-zone.txt",
            std::str::from_utf8(&output.into_inner()).unwrap(),
        );
    }

    #[test]
    fn test_builder_errors() {
        let sled1_uuid: Uuid = SLED1_UUID.parse().unwrap();
        let zone1_uuid: Uuid = ZONE1_UUID.parse().unwrap();

        // Duplicate sled, with both the same IP and a different one
        let mut builder = DnsConfigBuilder::new();
        builder.host_sled(sled1_uuid, SLED1_IP).unwrap();
        let error = builder.host_sled(sled1_uuid, SLED1_IP).unwrap_err();
        assert_eq!(
            error.to_string(),
            "multiple definitions for sled \
            001de000-51ed-4000-8000-000000000001 (previously ::1, now ::1)"
        );
        let error = builder.host_sled(sled1_uuid, SLED2_IP).unwrap_err();
        assert_eq!(
            error.to_string(),
            "multiple definitions for sled \
            001de000-51ed-4000-8000-000000000001 (previously ::1, \
            now ::0.0.0.2)"
        );

        // Duplicate zone, with both the same IP and a different one.
        let mut builder = DnsConfigBuilder::new();
        builder.host_zone(zone1_uuid, ZONE1_IP).unwrap();
        let error = builder.host_zone(zone1_uuid, ZONE1_IP).unwrap_err();
        assert_eq!(
            error.to_string(),
            "multiple definitions for zone \
            001de000-c04e-4000-8000-000000000001 (previously ::0.1.0.1, \
            now ::0.1.0.1)"
        );
        let error = builder.host_zone(zone1_uuid, ZONE2_IP).unwrap_err();
        assert_eq!(
            error.to_string(),
            "multiple definitions for zone \
            001de000-c04e-4000-8000-000000000001 (previously ::0.1.0.1, \
            now ::0.1.0.2)"
        );

        // Specify an undefined zone or sled.  (This requires a second builder.)
        let mut builder1 = DnsConfigBuilder::new();
        let zone = builder1.host_zone(zone1_uuid, ZONE1_IP).unwrap();
        let sled = builder1.host_sled(sled1_uuid, SLED1_IP).unwrap();
        let mut builder2 = DnsConfigBuilder::new();
        let error = builder2
            .service_backend_zone(
                SRV::Service(ServiceName::Oximeter),
                &zone,
                123,
            )
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "zone 001de000-c04e-4000-8000-000000000001 has not been defined"
        );
        let error = builder2
            .service_backend_sled(
                SRV::Service(ServiceName::Oximeter),
                &sled,
                123,
            )
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "sled 001de000-51ed-4000-8000-000000000001 has not been defined"
        );

        // Duplicate service backend, with both the same port and a different
        // one
        let mut builder = DnsConfigBuilder::new();
        let zone = builder.host_zone(zone1_uuid, ZONE1_IP).unwrap();
        builder
            .service_backend_zone(
                SRV::Service(ServiceName::Oximeter),
                &zone,
                123,
            )
            .unwrap();
        let error = builder
            .service_backend_zone(
                SRV::Service(ServiceName::Oximeter),
                &zone,
                123,
            )
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "service _oximeter._tcp: zone \
            001de000-c04e-4000-8000-000000000001: registered twice \
            (previously port 123, now 123)"
        );
        let error = builder
            .service_backend_zone(
                SRV::Service(ServiceName::Oximeter),
                &zone,
                456,
            )
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "service _oximeter._tcp: zone \
            001de000-c04e-4000-8000-000000000001: registered twice \
            (previously port 123, now 456)"
        );
    }
}
