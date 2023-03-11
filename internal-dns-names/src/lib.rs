// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Naming scheme for Internal DNS names (RFD 248).

// XXX-dap rename crate to "internal-dns"

use anyhow::anyhow;
use dns_service_client::types::{
    DnsConfig, DnsConfigZone, DnsKv, DnsRecord, DnsRecordKey,
};
use std::collections::BTreeMap;
use std::fmt;
use std::net::Ipv6Addr;
use uuid::Uuid;

pub mod multiclient;

pub const DNS_ZONE: &str = "control-plane.oxide.internal";

/// Names for services where backends are interchangeable.
#[derive(Clone, Debug, Hash, Eq, Ord, PartialEq, PartialOrd)]
pub enum ServiceName {
    Clickhouse,
    Cockroach,
    InternalDNS,
    Nexus,
    Oximeter,
    ManagementGatewayService,
    Wicketd,
    Dendrite,
    Tfport,
    CruciblePantry,
}

impl fmt::Display for ServiceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ServiceName::Clickhouse => write!(f, "clickhouse"),
            ServiceName::Cockroach => write!(f, "cockroach"),
            ServiceName::InternalDNS => write!(f, "internalDNS"),
            ServiceName::Nexus => write!(f, "nexus"),
            ServiceName::Oximeter => write!(f, "oximeter"),
            ServiceName::ManagementGatewayService => write!(f, "mgs"),
            ServiceName::Wicketd => write!(f, "wicketd"),
            ServiceName::Dendrite => write!(f, "dendrite"),
            ServiceName::Tfport => write!(f, "tfport"),
            ServiceName::CruciblePantry => write!(f, "crucible-pantry"),
        }
    }
}

/// Names for services where backends are not interchangeable.
#[derive(Clone, Debug, Hash, Eq, PartialEq, PartialOrd)]
pub enum BackendName {
    Crucible,
    SledAgent,
}

impl fmt::Display for BackendName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            BackendName::Crucible => write!(f, "crucible"),
            BackendName::SledAgent => write!(f, "sledagent"),
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, PartialOrd)]
pub enum SRV {
    /// A service identified and accessed by name, such as "nexus", "CRDB", etc.
    ///
    /// This is used in cases where services are interchangeable.
    Service(ServiceName),

    /// A service identified by name and a unique identifier.
    ///
    /// This is used in cases where services are not interchangeable, such as
    /// for the Sled agent.
    // XXX-dap the naming here is misleading
    Backend(BackendName, Uuid),
}

impl fmt::Display for SRV {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            SRV::Service(name) => {
                write!(f, "_{}._tcp.{}", name, DNS_ZONE)
            }
            SRV::Backend(name, id) => {
                write!(f, "_{}._tcp.{}.{}", name, id, DNS_ZONE)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum AAAA {
    /// Identifies an AAAA record for a sled.
    Sled(Uuid),

    /// Identifies an AAAA record for a zone within a sled.
    Zone(Uuid),
}

impl fmt::Display for AAAA {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            AAAA::Sled(id) => {
                write!(f, "{}.sled.{}", id, DNS_ZONE)
            }
            AAAA::Zone(id) => {
                write!(f, "{}.host.{}", id, DNS_ZONE)
            }
        }
    }
}

/// Helper for building an initial DNS configuration
// XXX-dap I wonder if we can make the stuff above non-pub once we flesh this
// out.  (Is it used anywhere aside from constructing DNS configuration?)
// XXX-dap TODO-doc design note: this is pretty simple because it makes a lot of
// assumptions: only zones are backends for services; there is only ever one
// address for zones or sleds
// XXX-dap TODO-doc this could use better documentation
// XXX-dap this crate could use better file organization now

pub struct DnsConfigBuilder {
    sleds: BTreeMap<Uuid, Ipv6Addr>,
    zones: BTreeMap<Uuid, Ipv6Addr>,
    service_instances: BTreeMap<ServiceName, BTreeMap<Uuid, u16>>,
}

pub struct Zone(Uuid);

impl DnsConfigBuilder {
    pub fn new() -> Self {
        DnsConfigBuilder {
            sleds: BTreeMap::new(),
            zones: BTreeMap::new(),
            service_instances: BTreeMap::new(),
        }
    }

    pub fn host_sled(
        &mut self,
        sled_id: Uuid,
        addr: Ipv6Addr,
    ) -> anyhow::Result<()> {
        match self.sleds.insert(sled_id, addr) {
            None => Ok(()),
            Some(existing) => Err(anyhow!(
                "multiple definitions for sled {} (previously {}, now {})",
                sled_id,
                existing,
                addr,
            )),
        }
    }

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

    pub fn service_backend(
        &mut self,
        service: ServiceName,
        zone: &Zone,
        port: u16,
    ) -> anyhow::Result<()> {
        let set = self
            .service_instances
            .entry(service.clone())
            .or_insert_with(BTreeMap::new);
        let zone_id = zone.0;
        match set.insert(zone_id, port) {
            None => Ok(()),
            Some(existing) => Err(anyhow!(
                "service {}: zone {}: registered twice\
                (previously port {}, now {})",
                service,
                zone_id,
                existing,
                port
            )),
        }
    }

    pub fn build(self) -> DnsConfig {
        // Assemble the set of "AAAA" records for sleds.
        let sled_records = self.sleds.into_iter().map(|(sled_id, sled_ip)| {
            let name = AAAA::Sled(sled_id).to_string();
            DnsKv {
                key: DnsRecordKey { name },
                // XXX-dap fix the case of these
                records: vec![DnsRecord::Aaaa(sled_ip)],
            }
        });

        // Assemble the set of AAAA records for zones.
        let zone_records = self.zones.into_iter().map(|(zone_id, zone_ip)| {
            let name = AAAA::Zone(zone_id).to_string();
            DnsKv {
                key: DnsRecordKey { name },
                records: vec![DnsRecord::Aaaa(zone_ip)],
            }
        });

        // Assemble the set of SRV records, which implicitly point back at
        // zones' AAAA records.
        let srv_records = self.service_instances.into_iter().map(
            |(service_name, zone2port)| {
                let name = SRV::Service(service_name).to_string();
                let records = zone2port
                    .into_iter()
                    .map(|(zone_id, port)| {
                        DnsRecord::Srv(dns_service_client::types::Srv {
                            prio: 0,
                            weight: 0,
                            port,
                            target: AAAA::Zone(zone_id).to_string(),
                        })
                    })
                    .collect();

                DnsKv { key: DnsRecordKey { name }, records }
            },
        );

        let all_records =
            sled_records.chain(zone_records).chain(srv_records).collect();

        DnsConfig {
            generation: 1,
            zones: vec![DnsConfigZone {
                zone_name: DNS_ZONE.to_owned(),
                records: all_records,
            }],
        }
    }
}

// XXX-dap TODO-coverage test coverage for DnsConfigBuilder

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn display_srv_service() {
        assert_eq!(
            SRV::Service(ServiceName::Clickhouse).to_string(),
            "_clickhouse._tcp.control-plane.oxide.internal",
        );
        assert_eq!(
            SRV::Service(ServiceName::Cockroach).to_string(),
            "_cockroach._tcp.control-plane.oxide.internal",
        );
        assert_eq!(
            SRV::Service(ServiceName::InternalDNS).to_string(),
            "_internalDNS._tcp.control-plane.oxide.internal",
        );
        assert_eq!(
            SRV::Service(ServiceName::Nexus).to_string(),
            "_nexus._tcp.control-plane.oxide.internal",
        );
        assert_eq!(
            SRV::Service(ServiceName::Oximeter).to_string(),
            "_oximeter._tcp.control-plane.oxide.internal",
        );
        assert_eq!(
            SRV::Service(ServiceName::Dendrite).to_string(),
            "_dendrite._tcp.control-plane.oxide.internal",
        );
        assert_eq!(
            SRV::Service(ServiceName::CruciblePantry).to_string(),
            "_crucible-pantry._tcp.control-plane.oxide.internal",
        );
    }

    #[test]
    fn display_srv_backend() {
        let uuid = Uuid::nil();
        assert_eq!(
            SRV::Backend(BackendName::Crucible, uuid).to_string(),
            "_crucible._tcp.00000000-0000-0000-0000-000000000000.control-plane.oxide.internal",
        );
        assert_eq!(
            SRV::Backend(BackendName::SledAgent, uuid).to_string(),
            "_sledagent._tcp.00000000-0000-0000-0000-000000000000.control-plane.oxide.internal",
        );
    }

    #[test]
    fn display_aaaa() {
        let uuid = Uuid::nil();
        assert_eq!(
            AAAA::Sled(uuid).to_string(),
            "00000000-0000-0000-0000-000000000000.sled.control-plane.oxide.internal",
        );
        assert_eq!(
            AAAA::Zone(uuid).to_string(),
            "00000000-0000-0000-0000-000000000000.host.control-plane.oxide.internal",
        );
    }
}
