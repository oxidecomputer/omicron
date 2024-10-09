// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    collections::{BTreeMap, HashMap},
    net::IpAddr,
};

use internal_dns_types::{config::DnsConfigBuilder, names::ServiceName};
use omicron_common::api::external::Name;
use omicron_uuid_kinds::SledUuid;

use crate::{
    deployment::{
        blueprint_zone_type, Blueprint, BlueprintZoneFilter, BlueprintZoneType,
    },
    internal_api::params::{DnsConfigZone, DnsRecord},
    silo::{default_silo_name, silo_dns_name},
};

use super::{blueprint_nexus_external_ips, Overridables, Sled};

/// Returns the expected contents of internal DNS based on the given blueprint
pub fn blueprint_internal_dns_config(
    blueprint: &Blueprint,
    sleds_by_id: &BTreeMap<SledUuid, Sled>,
    overrides: &Overridables,
) -> anyhow::Result<DnsConfigZone> {
    // The DNS names configured here should match what RSS configures for the
    // same zones.  It's tricky to have RSS share the same code because it uses
    // Sled Agent's _internal_ `OmicronZoneConfig` (and friends), whereas we're
    // using `sled-agent-client`'s version of that type.  However, the
    // DnsConfigBuilder's interface is high-level enough that it handles most of
    // the details.
    let mut dns_builder = DnsConfigBuilder::new();

    'all_zones: for (_, zone) in
        blueprint.all_omicron_zones(BlueprintZoneFilter::ShouldBeInInternalDns)
    {
        let (service_name, port) = match &zone.zone_type {
            BlueprintZoneType::BoundaryNtp(
                blueprint_zone_type::BoundaryNtp { address, .. },
            ) => (ServiceName::BoundaryNtp, address.port()),
            BlueprintZoneType::InternalNtp(
                blueprint_zone_type::InternalNtp { address, .. },
            ) => (ServiceName::InternalNtp, address.port()),
            BlueprintZoneType::Clickhouse(
                blueprint_zone_type::Clickhouse { address, .. },
            )
            | BlueprintZoneType::ClickhouseServer(
                blueprint_zone_type::ClickhouseServer { address, .. },
            ) => {
                // Add the HTTP and native TCP interfaces for ClickHouse data
                // replicas. This adds the zone itself, so we need to continue
                // back up to the loop over all the Omicron zones, rather than
                // falling through to call `host_zone_with_one_backend()`.
                let http_service = if matches!(
                    &zone.zone_type,
                    BlueprintZoneType::Clickhouse(_)
                ) {
                    ServiceName::Clickhouse
                } else {
                    ServiceName::ClickhouseServer
                };
                dns_builder.host_zone_clickhouse(
                    zone.id,
                    zone.underlay_address,
                    http_service,
                    address.port(),
                )?;
                continue 'all_zones;
            }
            BlueprintZoneType::ClickhouseKeeper(
                blueprint_zone_type::ClickhouseKeeper { address, .. },
            ) => {
                // Add the Clickhouse keeper service and `clickhouse-admin`
                // service used for managing the keeper. We continue below so we
                // don't fall through and call `host_zone_with_one_backend`.
                dns_builder.host_zone_clickhouse_keeper(
                    zone.id,
                    zone.underlay_address,
                    ServiceName::ClickhouseKeeper,
                    address.port(),
                )?;
                continue 'all_zones;
            }
            BlueprintZoneType::CockroachDb(
                blueprint_zone_type::CockroachDb { address, .. },
            ) => (ServiceName::Cockroach, address.port()),
            BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                internal_address,
                ..
            }) => (ServiceName::Nexus, internal_address.port()),
            BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                address,
                ..
            }) => (ServiceName::Crucible(zone.id), address.port()),
            BlueprintZoneType::CruciblePantry(
                blueprint_zone_type::CruciblePantry { address },
            ) => (ServiceName::CruciblePantry, address.port()),
            BlueprintZoneType::Oximeter(blueprint_zone_type::Oximeter {
                address,
            }) => (ServiceName::Oximeter, address.port()),
            BlueprintZoneType::ExternalDns(
                blueprint_zone_type::ExternalDns { http_address, .. },
            ) => (ServiceName::ExternalDns, http_address.port()),
            BlueprintZoneType::InternalDns(
                blueprint_zone_type::InternalDns { http_address, .. },
            ) => (ServiceName::InternalDns, http_address.port()),
        };
        dns_builder.host_zone_with_one_backend(
            zone.id,
            zone.underlay_address,
            service_name,
            port,
        )?;
    }

    let scrimlets = sleds_by_id.values().filter(|sled| sled.is_scrimlet());
    for scrimlet in scrimlets {
        let sled_subnet = scrimlet.subnet();
        let switch_zone_ip =
            overrides.switch_zone_ip(scrimlet.id(), sled_subnet);
        dns_builder.host_zone_switch(
            scrimlet.id(),
            switch_zone_ip,
            overrides.dendrite_port(scrimlet.id()),
            overrides.mgs_port(scrimlet.id()),
            overrides.mgd_port(scrimlet.id()),
        )?;
    }

    Ok(dns_builder.build_zone())
}

pub fn blueprint_external_dns_config(
    blueprint: &Blueprint,
    silos: &[Name],
    external_dns_zone_name: String,
) -> DnsConfigZone {
    let nexus_external_ips = blueprint_nexus_external_ips(blueprint);

    let dns_records: Vec<DnsRecord> = nexus_external_ips
        .into_iter()
        .map(|addr| match addr {
            IpAddr::V4(addr) => DnsRecord::A(addr),
            IpAddr::V6(addr) => DnsRecord::AAAA(addr),
        })
        .collect();

    let records = silos
        .into_iter()
        // We do not generate a DNS name for the "default" Silo.
        //
        // We use the name here rather than the id.  It shouldn't really matter
        // since every system will have this silo and so no other Silo could
        // have this name.  But callers (particularly the test suite and
        // reconfigurator-cli) specify silos by name, not id, so if we used the
        // id here then they'd have to apply this filter themselves (and this
        // abstraction, such as it is, would be leakier).
        .filter_map(|silo_name| {
            (silo_name != default_silo_name())
                .then(|| (silo_dns_name(&silo_name), dns_records.clone()))
        })
        .collect::<HashMap<String, Vec<DnsRecord>>>();

    DnsConfigZone {
        zone_name: external_dns_zone_name,
        records: records.clone(),
    }
}
