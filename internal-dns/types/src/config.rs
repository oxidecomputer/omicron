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

use crate::names::{BOUNDARY_NTP_DNS_NAME, DNS_ZONE, ServiceName};
use anyhow::{anyhow, ensure};
use core::fmt;
use omicron_common::address::{CLICKHOUSE_ADMIN_PORT, CLICKHOUSE_TCP_PORT};
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::{OmicronZoneUuid, SledUuid};
use std::collections::BTreeMap;
use std::net::{Ipv6Addr, SocketAddrV6};

// "v2" types are the most recent, so we re-export them here for dependents that
// just want "latest".
pub use crate::v2::config::*;

/// Used to construct the DNS name for a control plane host
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Host {
    /// Used to construct an AAAA record for a sled.
    Sled(SledUuid),

    /// Used to construct an AAAA record for a zone on a sled.
    Zone(Zone),
}

impl Host {
    pub fn for_zone(zone: Zone) -> Host {
        Host::Zone(zone)
    }

    /// Returns the DNS name for this host, ignoring the zone part of the DNS
    /// name
    pub(crate) fn dns_name(&self) -> String {
        match &self {
            Host::Sled(id) => format!("{}.sled", id),
            Host::Zone(Zone::Dendrite(id)) => {
                format!("dendrite-{}.host", id)
            }
            Host::Zone(Zone::Other(id)) => {
                format!("{}.host", id)
            }
        }
    }

    /// Returns the full-qualified DNS name, including the zone name of the
    /// control plane's internal DNS zone
    pub fn fqdn(&self) -> String {
        format!("{}.{}", self.dns_name(), DNS_ZONE)
    }
}

/// Builder for assembling DNS data for the control plane's DNS zone
///
/// `DnsConfigBuilder` provides a much simpler interface for constructing DNS
/// zone data than using `DnsConfig` directly.  That's because it makes a number
/// of assumptions that are true of the control plane DNS zones (all described
/// in RFD 248), but not true in general about DNS zones:
///
/// - We assume that there are only two kinds of hosts: a "sled" (an illumos
///   global zone) or a "zone" (an illumos non-global zone).  (Both of these are
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
#[derive(Clone)]
pub struct DnsConfigBuilder {
    /// set of hosts of type "sled" that have been configured so far, mapping
    /// each sled's unique uuid to its sole IPv6 address on the control plane
    /// network
    sleds: BTreeMap<Sled, Ipv6Addr>,

    /// set of hosts of type "zone" that have been configured so far, mapping
    /// each zone's unique uuid to its sole IPv6 address on the control plane
    /// network
    zones: BTreeMap<Zone, Ipv6Addr>,

    /// a map of internal DNS service zone UUIDs to the address that service
    /// answers DNS queries on.  Each internal DNS server is present on the
    /// control plane network with its HTTP interface on an IPv6 address in the
    /// map above.  In practice the addresses here are the same as the addresses
    /// associated with this same UUID in `zones` above, but they are tracked
    /// separately here to avoid surprise if they differ in the future.
    internal_dns_addresses: BTreeMap<Zone, Ipv6Addr>,

    /// set of services (see module-level comment) that have been configured so
    /// far, mapping the name of the service (encapsulated in a [`ServiceName`])
    /// to the backends configured for that service.  The set of backends is
    /// represented as a mapping from the zone's uuid to the port on which it's
    /// running the service.
    service_instances_zones: BTreeMap<ServiceName, BTreeMap<Zone, u16>>,

    /// similar to service_instances_zones, but for services that run on sleds
    service_instances_sleds: BTreeMap<ServiceName, BTreeMap<Sled, u16>>,
}

/// Describes a host of type "sled" in the control plane DNS zone
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Sled(SledUuid);

/// Describes a host of type "zone" (an illumos zone) in the control plane DNS
/// zone
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Zone {
    /// This non-global zone runs an instance of Dendrite.
    ///
    /// This implies that the Sled is a scrimlet.
    // When this variant is used, the UUID in the record should match the sled
    // itself.
    Dendrite(SledUuid),
    /// All other non-global zones.
    Other(OmicronZoneUuid),
}

impl Zone {
    pub(crate) fn to_host(&self) -> Host {
        Host::Zone(self.clone())
    }

    pub fn dns_name(&self) -> String {
        self.to_host().dns_name()
    }
}

impl fmt::Display for Zone {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Zone::Dendrite(sled_uuid) => {
                write!(f, "{} (dendrite)", sled_uuid)
            }
            Zone::Other(zone_uuid) => write!(f, "{} (other)", zone_uuid),
        }
    }
}

impl DnsConfigBuilder {
    pub fn new() -> Self {
        DnsConfigBuilder {
            sleds: BTreeMap::new(),
            zones: BTreeMap::new(),
            internal_dns_addresses: BTreeMap::new(),
            service_instances_zones: BTreeMap::new(),
            service_instances_sleds: BTreeMap::new(),
        }
    }

    /// Add a new host of type "sled" to the configuration
    ///
    /// Returns a [`Sled`] that can be used with
    /// [`Self::service_backend_sled()`] to specify that this sled is a backend
    /// for some higher-level service.
    ///
    /// # Errors
    ///
    /// This function fails only if the given sled has already been added to the
    /// configuration.
    pub fn host_sled(
        &mut self,
        sled_id: SledUuid,
        addr: Ipv6Addr,
    ) -> anyhow::Result<Sled> {
        match self.sleds.insert(Sled(sled_id), addr) {
            None => Ok(Sled(sled_id)),
            Some(existing) => Err(anyhow!(
                "multiple definitions for sled {} (previously {}, now {})",
                sled_id,
                existing,
                addr,
            )),
        }
    }

    /// Add a new dendrite host of type "zone" to the configuration
    ///
    /// Returns a [`Zone`] that can be used with
    /// [`Self::service_backend_zone()`] to specify that this zone is a backend
    /// for some higher-level service.
    ///
    /// # Errors
    ///
    /// This function fails only if the given zone has already been added to the
    /// configuration.
    pub fn host_dendrite(
        &mut self,
        sled_id: SledUuid,
        addr: Ipv6Addr,
    ) -> anyhow::Result<Zone> {
        self.host_zone_internal(Zone::Dendrite(sled_id), addr)
    }

    /// Add a new host of type "zone" to the configuration
    ///
    /// Returns a [`Zone`] that can be used with
    /// [`Self::service_backend_zone()`] to specify that this zone is a backend
    /// for some higher-level service.
    ///
    /// # Errors
    ///
    /// This function fails only if the given zone has already been added to the
    /// configuration.
    pub fn host_zone(
        &mut self,
        zone_id: OmicronZoneUuid,
        addr: Ipv6Addr,
    ) -> anyhow::Result<Zone> {
        self.host_zone_internal(Zone::Other(zone_id), addr)
    }

    fn host_zone_internal(
        &mut self,
        zone: Zone,
        addr: Ipv6Addr,
    ) -> anyhow::Result<Zone> {
        match self.zones.insert(zone.clone(), addr) {
            None => Ok(zone),
            Some(existing) => Err(anyhow!(
                "multiple definitions for zone {} (previously {}, now {})",
                zone,
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
        service: ServiceName,
        zone: &Zone,
        port: u16,
    ) -> anyhow::Result<()> {
        // Although one can only get a `Zone` by adding it to a
        // `DnsConfigBuilder`, it's possible that it was added to a different
        // DnsBuilder.
        ensure!(
            self.zones.contains_key(&zone),
            "zone {zone} has not been defined",
        );

        let set = self
            .service_instances_zones
            .entry(service)
            .or_insert_with(BTreeMap::new);
        match set.insert(zone.clone(), port) {
            None => Ok(()),
            Some(existing) => Err(anyhow!(
                "service {}: zone {}: registered twice \
                (previously port {}, now {})",
                service.dns_name(),
                zone,
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
        service: ServiceName,
        sled: &Sled,
        port: u16,
    ) -> anyhow::Result<()> {
        // Although one can only get a `Sled` by adding it to a
        // `DnsConfigBuilder`, it's possible that it was added to a different
        // DnsBuilder.
        ensure!(
            self.sleds.contains_key(&sled),
            "sled {} has not been defined",
            sled.0
        );

        let set = self
            .service_instances_sleds
            .entry(service)
            .or_insert_with(BTreeMap::new);
        let sled_id = sled.0;
        match set.insert(sled.clone(), port) {
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

    /// Higher-level shorthand for adding a zone with a single backend service
    ///
    /// # Errors
    ///
    /// This function fails only if the given zone has already been added to the
    /// configuration.
    pub fn host_zone_with_one_backend(
        &mut self,
        zone_id: OmicronZoneUuid,
        service: ServiceName,
        addr: SocketAddrV6,
    ) -> anyhow::Result<()> {
        let zone = self.host_zone(zone_id, *addr.ip())?;
        self.service_backend_zone(service, &zone, addr.port())
    }

    /// Higher-level shorthand for adding a "switch" zone with its usual set of
    /// backend services
    ///
    /// # Errors
    ///
    /// This function fails only if the given zone has already been added to the
    /// configuration.
    pub fn host_zone_switch(
        &mut self,
        sled_id: SledUuid,
        switch_zone_ip: Ipv6Addr,
        dendrite_port: u16,
        mgs_port: u16,
        mgd_port: u16,
    ) -> anyhow::Result<()> {
        let zone = self.host_dendrite(sled_id, switch_zone_ip)?;
        self.service_backend_zone(ServiceName::Dendrite, &zone, dendrite_port)?;
        self.service_backend_zone(
            ServiceName::ManagementGatewayService,
            &zone,
            mgs_port,
        )?;
        self.service_backend_zone(ServiceName::Mgd, &zone, mgd_port)
    }

    /// Higher-level shorthand for adding a ClickHouse single node zone with
    /// several services.
    ///
    /// The ClickHouse single node server exposes several interfaces on the
    /// network. We use both a simple HTTP interface as well as a lower-level
    /// protocol over TCP, called the "Native protocol". This method inserts a
    /// zone and the related records for both of these services.
    ///
    /// `http_service` is the `ServiceName` for the HTTP service that belongs in
    /// this zone, and `http_port` is the associated port for that service. The
    /// native service is added automatically, using its default port.
    ///
    /// We also add a `ClickhouseAdminSingleServer` service.
    ///
    /// # Errors
    ///
    /// This function fails if the given zone has already been added to the
    /// configuration.
    pub fn host_zone_clickhouse_single_node(
        &mut self,
        zone_id: OmicronZoneUuid,
        http_address: SocketAddrV6,
        read_policy_enabled: bool,
    ) -> anyhow::Result<()> {
        let zone = self.host_zone(zone_id, *http_address.ip())?;
        self.service_backend_zone(
            ServiceName::Clickhouse,
            &zone,
            http_address.port(),
        )?;
        if read_policy_enabled {
            self.service_backend_zone(
                ServiceName::OximeterReader,
                &zone,
                CLICKHOUSE_TCP_PORT,
            )?;
        };
        self.service_backend_zone(
            ServiceName::ClickhouseNative,
            &zone,
            CLICKHOUSE_TCP_PORT,
        )?;
        self.service_backend_zone(
            ServiceName::ClickhouseAdminSingleServer,
            &zone,
            CLICKHOUSE_ADMIN_PORT,
        )
    }

    /// Higher-level shorthand for adding a ClickHouse cluster zone with several
    /// services.
    ///
    /// ClickHouse servers expose several interfaces on the network. We use both
    /// a simple HTTP interface as well as a lower-level protocol over TCP,
    /// called the "Native protocol". This method inserts a zone and the related
    /// records for both of these services.
    /// (TODO-<https://github.com/oxidecomputer/omicron/issues/7419:> Add Native protocol
    /// interface)
    ///
    /// `http_service` is the `ServiceName` for the HTTP service that belongs in
    /// this zone, and `http_port` is the associated port for that service. The
    /// native service is added automatically, using its default port.
    ///
    /// We also add a `ClickhouseAdminServer` service.
    ///
    /// # Errors
    ///
    /// This function fails if the given zone has already been added to the
    /// configuration.
    pub fn host_zone_clickhouse_cluster(
        &mut self,
        zone_id: OmicronZoneUuid,
        http_address: SocketAddrV6,
        read_policy_enabled: bool,
    ) -> anyhow::Result<()> {
        let zone = self.host_zone(zone_id, *http_address.ip())?;
        self.service_backend_zone(
            ServiceName::ClickhouseServer,
            &zone,
            http_address.port(),
        )?;
        if read_policy_enabled {
            self.service_backend_zone(
                ServiceName::OximeterReader,
                &zone,
                CLICKHOUSE_TCP_PORT,
            )?;
        };
        self.service_backend_zone(
            ServiceName::ClickhouseClusterNative,
            &zone,
            CLICKHOUSE_TCP_PORT,
        )?;
        self.service_backend_zone(
            ServiceName::ClickhouseAdminServer,
            &zone,
            CLICKHOUSE_ADMIN_PORT,
        )
    }

    /// Higher-level shorthand for adding a ClickhouseKeeper zone with several
    /// services.
    ///
    /// # Errors
    ///
    /// This function fails if the given zone has already been added to the
    /// configuration.
    pub fn host_zone_clickhouse_keeper(
        &mut self,
        zone_id: OmicronZoneUuid,
        address: SocketAddrV6,
    ) -> anyhow::Result<()> {
        let zone = self.host_zone(zone_id, *address.ip())?;
        self.service_backend_zone(
            ServiceName::ClickhouseKeeper,
            &zone,
            address.port(),
        )?;
        self.service_backend_zone(
            ServiceName::ClickhouseAdminKeeper,
            &zone,
            CLICKHOUSE_ADMIN_PORT,
        )
    }

    /// Higher-level shorthand for adding an internal DNS zone, including
    /// records for both its HTTP and DNS interfaces.
    ///
    /// # Errors
    ///
    /// This function fails if the given zone has already been added to the
    /// configuration.
    pub fn host_zone_internal_dns(
        &mut self,
        zone_id: OmicronZoneUuid,
        http_address: SocketAddrV6,
        dns_address: SocketAddrV6,
    ) -> anyhow::Result<()> {
        let zone = self.host_zone(zone_id, *http_address.ip())?;
        self.service_backend_zone(
            ServiceName::InternalDns,
            &zone,
            http_address.port(),
        )?;
        let prior_address =
            self.internal_dns_addresses.insert(zone.clone(), *dns_address.ip());
        if let Some(addr) = prior_address {
            anyhow::bail!("zone {} already had a DNS address: {}", zone, addr);
        }
        Ok(())
    }

    /// Construct a `DnsConfigZone` describing the control plane DNS zone
    /// described up to this point
    pub fn build_zone(self) -> DnsConfigZone {
        // Assemble the set of "AAAA" records for sleds.
        let sled_records = self.sleds.into_iter().map(|(sled, sled_ip)| {
            let name = Host::Sled(sled.0).dns_name();
            (name, vec![DnsRecord::Aaaa(sled_ip)])
        });

        // Assemble the special boundary NTP name to support chrony on internal
        // NTP zones.
        //
        // We leave this as `None` if there are no `BoundaryNtp` service zones,
        // which omits it from the final set of records.
        let boundary_ntp_records = self
            .service_instances_zones
            .get(&ServiceName::BoundaryNtp)
            .map(|zone2port| {
                let records = zone2port
                    .iter()
                    .map(|(zone, _port)| {
                        let zone_ip = self.zones.get(&zone).expect(
                            "service_backend_zone() ensures zones are defined",
                        );
                        DnsRecord::Aaaa(*zone_ip)
                    })
                    .collect::<Vec<DnsRecord>>();
                (BOUNDARY_NTP_DNS_NAME.to_string(), records)
            });

        // Assemble the set of AAAA records for zones.
        let zone_records = self.zones.into_iter().map(|(zone, zone_ip)| {
            (zone.dns_name(), vec![DnsRecord::Aaaa(zone_ip)])
        });

        // DNS nameservers can be added in arbitrary order to this builder.
        // Before assembling DNS records, sort the addresses to have stability
        // in which IPs are for which nameserver records.
        let mut internal_dns_addresses =
            self.internal_dns_addresses.values().collect::<Vec<_>>();
        internal_dns_addresses.sort();
        let mut internal_nameservers = Vec::new();
        let mut internal_dns_records = internal_dns_addresses
            .iter()
            .enumerate()
            .map(|(idx, ip)| {
                // Enumeration starts at zero, but name server names start from one.
                let name = format!("ns{}", idx + 1);
                internal_nameservers.push(DnsRecord::Ns(format!(
                    "{}.{}",
                    name,
                    crate::names::DNS_ZONE
                )));
                (name, vec![DnsRecord::Aaaa(**ip)])
            })
            .collect::<Vec<_>>();
        if !internal_nameservers.is_empty() {
            internal_dns_records.push(("@".to_string(), internal_nameservers));
        }

        // Assemble the set of SRV records, which implicitly point back at
        // zones' AAAA records.
        let srv_records_zones = self.service_instances_zones.into_iter().map(
            |(service_name, zone2port)| {
                let name = service_name.dns_name();
                let records = zone2port
                    .into_iter()
                    .map(|(zone, port)| {
                        DnsRecord::Srv(Srv {
                            prio: 0,
                            weight: 0,
                            port,
                            target: zone.to_host().fqdn(),
                        })
                    })
                    .collect();

                (name, records)
            },
        );

        let srv_records_sleds = self.service_instances_sleds.into_iter().map(
            |(service_name, sled2port)| {
                let name = service_name.dns_name();
                let records = sled2port
                    .into_iter()
                    .map(|(sled, port)| {
                        DnsRecord::Srv(Srv {
                            prio: 0,
                            weight: 0,
                            port,
                            target: Host::Sled(sled.0).fqdn(),
                        })
                    })
                    .collect();

                (name, records)
            },
        );

        let all_records = sled_records
            .chain(zone_records)
            .chain(boundary_ntp_records)
            .chain(internal_dns_records)
            .chain(srv_records_sleds)
            .chain(srv_records_zones)
            .collect();

        DnsConfigZone { zone_name: DNS_ZONE.to_owned(), records: all_records }
    }

    /// Construct a complete [`DnsConfigParams`] (suitable for propagating to
    /// our DNS servers) for the control plane DNS zone described up to this
    /// point
    pub fn build_full_config_for_initial_generation(self) -> DnsConfigParams {
        let zone = self.build_zone();
        let generation = Generation::new();
        DnsConfigParams {
            generation,
            serial: generation
                .as_u64()
                .try_into()
                .expect("initial generation fits into u32"),
            time_created: chrono::Utc::now(),
            zones: vec![zone],
        }
    }
}

#[cfg(test)]
mod test {
    use super::{DnsConfigBuilder, Host, ServiceName};
    use crate::{config::Zone, names::DNS_ZONE};
    use omicron_common::api::external::Generation;
    use omicron_uuid_kinds::{OmicronZoneUuid, SledUuid};
    use std::{
        collections::BTreeMap, io::Write, net::Ipv6Addr, net::SocketAddrV6,
    };

    #[test]
    fn display_srv_service() {
        assert_eq!(ServiceName::Clickhouse.dns_name(), "_clickhouse._tcp",);
        assert_eq!(
            ServiceName::ClickhouseAdminKeeper.dns_name(),
            "_clickhouse-admin-keeper._tcp",
        );
        assert_eq!(
            ServiceName::ClickhouseAdminServer.dns_name(),
            "_clickhouse-admin-server._tcp",
        );
        assert_eq!(
            ServiceName::ClickhouseAdminSingleServer.dns_name(),
            "_clickhouse-admin-single-server._tcp",
        );
        assert_eq!(
            ServiceName::ClickhouseKeeper.dns_name(),
            "_clickhouse-keeper._tcp",
        );
        assert_eq!(
            ServiceName::ClickhouseNative.dns_name(),
            "_clickhouse-native._tcp",
        );
        assert_eq!(
            ServiceName::ClickhouseServer.dns_name(),
            "_clickhouse-server._tcp",
        );
        assert_eq!(ServiceName::Cockroach.dns_name(), "_cockroach._tcp",);
        assert_eq!(ServiceName::InternalDns.dns_name(), "_nameservice._tcp",);
        assert_eq!(ServiceName::Nexus.dns_name(), "_nexus._tcp",);
        assert_eq!(ServiceName::Oximeter.dns_name(), "_oximeter._tcp",);
        assert_eq!(
            ServiceName::OximeterReader.dns_name(),
            "_oximeter-reader._tcp",
        );
        assert_eq!(ServiceName::Dendrite.dns_name(), "_dendrite._tcp",);
        assert_eq!(
            ServiceName::CruciblePantry.dns_name(),
            "_crucible-pantry._tcp",
        );

        let sled_uuid = SledUuid::nil();
        let zone_uuid = OmicronZoneUuid::nil();
        assert_eq!(
            ServiceName::SledAgent(sled_uuid).dns_name(),
            "_sledagent._tcp.00000000-0000-0000-0000-000000000000",
        );
        assert_eq!(
            ServiceName::Crucible(zone_uuid).dns_name(),
            "_crucible._tcp.00000000-0000-0000-0000-000000000000",
        );
    }

    #[test]
    fn display_hosts() {
        let sled_uuid = SledUuid::nil();
        let zone_uuid = OmicronZoneUuid::nil();
        assert_eq!(
            Host::Sled(sled_uuid).dns_name(),
            "00000000-0000-0000-0000-000000000000.sled",
        );
        assert_eq!(
            Host::Zone(Zone::Other(zone_uuid)).dns_name(),
            "00000000-0000-0000-0000-000000000000.host",
        );
        assert_eq!(
            Host::Zone(Zone::Dendrite(sled_uuid)).dns_name(),
            "dendrite-00000000-0000-0000-0000-000000000000.host",
        );
    }

    // DnsConfigBuilder tests

    const SLED1_UUID: &'static str = "001de000-51ed-4000-8000-000000000001";
    const SLED2_UUID: &'static str = "001de000-51ed-4000-8000-000000000002";
    const ZONE1_UUID: &'static str = "001de000-c04e-4000-8000-000000000001";
    const ZONE2_UUID: &'static str = "001de000-c04e-4000-8000-000000000002";
    const ZONE3_UUID: &'static str = "001de000-c04e-4000-8000-000000000003";
    const ZONE4_UUID: &'static str = "001de000-c04e-4000-8000-000000000004";
    const ZONE_CLICKHOUSE_UUID: &'static str =
        "001de000-c04e-4000-8000-000000000005";
    const ZONE_CLICKHOUSE_SERVER_UUID: &'static str =
        "001de000-c04e-4000-8000-000000000006";
    const SLED1_IP: Ipv6Addr = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1);
    const SLED2_IP: Ipv6Addr = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 2);
    const ZONE1_IP: Ipv6Addr = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 1, 1);
    const ZONE2_IP: Ipv6Addr = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 1, 2);
    const ZONE3_IP: Ipv6Addr = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 1, 3);
    const ZONE4_IP: Ipv6Addr = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 1, 4);
    const ZONE_CLICKHOUSE_IP: Ipv6Addr = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 1, 5);
    const ZONE_CLICKHOUSE_SERVER_IP: Ipv6Addr =
        Ipv6Addr::new(0, 0, 0, 0, 0, 0, 1, 6);

    #[test]
    fn test_builder_output() {
        let mut output = std::io::Cursor::new(Vec::new());

        let sled1_uuid: SledUuid = SLED1_UUID.parse().unwrap();
        let sled2_uuid: SledUuid = SLED2_UUID.parse().unwrap();
        let zone1_uuid: OmicronZoneUuid = ZONE1_UUID.parse().unwrap();
        let zone2_uuid: OmicronZoneUuid = ZONE2_UUID.parse().unwrap();
        let zone3_uuid: OmicronZoneUuid = ZONE3_UUID.parse().unwrap();
        let zone4_uuid: OmicronZoneUuid = ZONE4_UUID.parse().unwrap();
        let zone_clickhouse_uuid: OmicronZoneUuid =
            ZONE_CLICKHOUSE_UUID.parse().unwrap();
        let zone_clickhouse_server_uuid: OmicronZoneUuid =
            ZONE_CLICKHOUSE_SERVER_UUID.parse().unwrap();

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
            b.service_backend_zone(ServiceName::Nexus, &zone1, 123).unwrap();
            b.service_backend_zone(ServiceName::Nexus, &zone2, 124).unwrap();

            // Another service, using one of the same zones (so the same zone is
            // used in two services)
            b.service_backend_zone(ServiceName::Oximeter, &zone2, 125).unwrap();
            b.service_backend_zone(ServiceName::Oximeter, &zone3, 126).unwrap();

            // Add a boundary NTP service to one of the zones; this will also
            // populate the special `BOUNDARY_NTP_DNS_NAME`.
            b.service_backend_zone(ServiceName::BoundaryNtp, &zone2, 127)
                .unwrap();

            // Add clickhouse and clickhouse server zones, which have serveral services each
            b.host_zone_clickhouse_single_node(
                zone_clickhouse_uuid,
                SocketAddrV6::new(ZONE_CLICKHOUSE_IP, 0, 0, 0),
                true,
            )
            .unwrap();
            b.host_zone_clickhouse_cluster(
                zone_clickhouse_server_uuid,
                SocketAddrV6::new(ZONE_CLICKHOUSE_SERVER_IP, 0, 0, 0),
                false,
            )
            .unwrap();

            // A sharded service
            b.service_backend_sled(
                ServiceName::SledAgent(sled1_uuid),
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
            let config = builder.build_full_config_for_initial_generation();
            assert_eq!(config.generation, Generation::from(1));
            assert_eq!(config.zones.len(), 1);
            assert_eq!(config.zones[0].zone_name, DNS_ZONE);
            write!(&mut output, "builder: {:?}\n", label).unwrap();
            // Sort the records for stability.
            let records: BTreeMap<_, _> =
                config.zones[0].records.iter().collect();
            serde_json::to_writer_pretty(&mut output, &records).unwrap();
            write!(&mut output, "\n").unwrap();
        }

        expectorate::assert_contents(
            "tests/output/internal-dns-zone.txt",
            std::str::from_utf8(&output.into_inner()).unwrap(),
        );
    }

    #[test]
    fn test_builder_errors() {
        let sled1_uuid: SledUuid = SLED1_UUID.parse().unwrap();
        let zone1_uuid: OmicronZoneUuid = ZONE1_UUID.parse().unwrap();

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
            now ::2)"
        );

        // Duplicate zone, with both the same IP and a different one.
        let mut builder = DnsConfigBuilder::new();
        builder.host_zone(zone1_uuid, ZONE1_IP).unwrap();
        let error = builder.host_zone(zone1_uuid, ZONE1_IP).unwrap_err();
        assert_eq!(
            error.to_string(),
            "multiple definitions for zone \
            001de000-c04e-4000-8000-000000000001 (other) \
            (previously ::1:1, now ::1:1)"
        );
        let error = builder.host_zone(zone1_uuid, ZONE2_IP).unwrap_err();
        assert_eq!(
            error.to_string(),
            "multiple definitions for zone \
            001de000-c04e-4000-8000-000000000001 (other) \
            (previously ::1:1, now ::1:2)"
        );

        // Specify an undefined zone or sled.  (This requires a second builder.)
        let mut builder1 = DnsConfigBuilder::new();
        let zone = builder1.host_zone(zone1_uuid, ZONE1_IP).unwrap();
        let sled = builder1.host_sled(sled1_uuid, SLED1_IP).unwrap();
        let mut builder2 = DnsConfigBuilder::new();
        let error = builder2
            .service_backend_zone(ServiceName::Oximeter, &zone, 123)
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "zone 001de000-c04e-4000-8000-000000000001 (other) \
            has not been defined"
        );
        let error = builder2
            .service_backend_sled(ServiceName::Oximeter, &sled, 123)
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
            .service_backend_zone(ServiceName::Oximeter, &zone, 123)
            .unwrap();
        let error = builder
            .service_backend_zone(ServiceName::Oximeter, &zone, 123)
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "service _oximeter._tcp: zone \
            001de000-c04e-4000-8000-000000000001 (other): registered twice \
            (previously port 123, now 123)"
        );
        let error = builder
            .service_backend_zone(ServiceName::Oximeter, &zone, 456)
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "service _oximeter._tcp: zone \
            001de000-c04e-4000-8000-000000000001 (other): registered twice \
            (previously port 123, now 456)"
        );
    }
}
