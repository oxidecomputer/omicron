// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! High-level interface for putting together the contents of the internal DNS
//! zone

use crate::names::{BackendName, ServiceName, DNS_ZONE};
use anyhow::anyhow;
use dns_service_client::types::{
    DnsConfigParams, DnsConfigZone, DnsKv, DnsRecord, DnsRecordKey,
};
use std::collections::BTreeMap;
use std::net::Ipv6Addr;
use uuid::Uuid;

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
    // XXX-dap the naming here is misleading
    Backend(BackendName, Uuid),
}

impl SRV {
    pub(crate) fn dns_name(&self) -> String {
        match &self {
            SRV::Service(name) => format!("_{}._tcp", name),
            SRV::Backend(name, id) => format!("_{}._tcp.{}", name, id),
        }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum AAAA {
    /// Identifies an AAAA record for a sled.
    Sled(Uuid),

    /// Identifies an AAAA record for a zone within a sled.
    Zone(Uuid),
}

impl AAAA {
    pub(crate) fn dns_name(&self) -> String {
        match &self {
            AAAA::Sled(id) => format!("{}.sled", id),
            AAAA::Zone(id) => format!("{}.host", id),
        }
    }
}

/// Helper for building an initial DNS configuration
// XXX-dap TODO-doc design note: this is pretty simple because it makes a lot of
// assumptions: only zones are backends for services; there is only ever one
// address for zones or sleds
// XXX-dap TODO-doc this could use better documentation

pub struct DnsConfigBuilder {
    sleds: BTreeMap<Uuid, Ipv6Addr>,
    zones: BTreeMap<Uuid, Ipv6Addr>,
    service_instances: BTreeMap<SRV, BTreeMap<Uuid, u16>>,
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
        service: SRV,
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
                service.dns_name(),
                zone_id,
                existing,
                port
            )),
        }
    }

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
        let srv_records = self.service_instances.into_iter().map(
            |(service_name, zone2port)| {
                // XXX-dap For internal DNS should we create two sets of SRV
                // records?  Should the caller be responsible for that?
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

        let all_records =
            sled_records.chain(zone_records).chain(srv_records).collect();

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

// XXX-dap TODO-coverage test coverage for DnsConfigBuilder

#[cfg(test)]
mod test {
    use super::{BackendName, ServiceName, AAAA, SRV};
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
}
