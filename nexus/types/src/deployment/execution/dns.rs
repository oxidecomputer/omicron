// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    collections::{BTreeMap, HashMap},
    net::IpAddr,
};

use internal_dns_types::{
    config::DnsConfigBuilder,
    names::{ServiceName, ZONE_APEX_NAME},
};
use omicron_common::api::external::Name;
use omicron_uuid_kinds::SledUuid;

use crate::{
    deployment::{
        Blueprint, BlueprintZoneDisposition, BlueprintZoneType, SledFilter,
        blueprint_zone_type,
    },
    internal_api::params::{DnsConfigZone, DnsRecord},
    silo::{default_silo_name, silo_dns_name},
};

use super::{
    Overridables, Sled, blueprint_external_dns_nameserver_ips,
    blueprint_nexus_external_ips,
};

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
        blueprint.all_omicron_zones(BlueprintZoneDisposition::is_in_service)
    {
        let (service_name, &address) = match &zone.zone_type {
            BlueprintZoneType::BoundaryNtp(
                blueprint_zone_type::BoundaryNtp { address, .. },
            ) => (ServiceName::BoundaryNtp, address),
            BlueprintZoneType::InternalNtp(
                blueprint_zone_type::InternalNtp { address, .. },
            ) => (ServiceName::InternalNtp, address),
            BlueprintZoneType::Clickhouse(
                blueprint_zone_type::Clickhouse { address, .. },
            ) => {
                // Add the HTTP and native TCP interfaces for ClickHouse data
                // replicas. This adds the zone itself, so we need to continue
                // back up to the loop over all the Omicron zones, rather than
                // falling through to call `host_zone_with_one_backend()`.
                dns_builder.host_zone_clickhouse_single_node(
                    zone.id,
                    ServiceName::Clickhouse,
                    *address,
                    blueprint.oximeter_read_mode.single_node_enabled(),
                )?;
                continue 'all_zones;
            }
            BlueprintZoneType::ClickhouseServer(
                blueprint_zone_type::ClickhouseServer { address, .. },
            ) => {
                // Add the HTTP and native TCP interfaces for ClickHouse data
                // replicas. This adds the zone itself, so we need to continue
                // back up to the loop over all the Omicron zones, rather than
                // falling through to call `host_zone_with_one_backend()`.
                dns_builder.host_zone_clickhouse_cluster(
                    zone.id,
                    ServiceName::ClickhouseServer,
                    *address,
                    blueprint.oximeter_read_mode.cluster_enabled(),
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
                    ServiceName::ClickhouseKeeper,
                    *address,
                )?;
                continue 'all_zones;
            }
            BlueprintZoneType::CockroachDb(
                blueprint_zone_type::CockroachDb { address, .. },
            ) => (ServiceName::Cockroach, address),
            BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                internal_address,
                ..
            }) => (ServiceName::Nexus, internal_address),
            BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                address,
                ..
            }) => (ServiceName::Crucible(zone.id), address),
            BlueprintZoneType::CruciblePantry(
                blueprint_zone_type::CruciblePantry { address },
            ) => (ServiceName::CruciblePantry, address),
            BlueprintZoneType::Oximeter(blueprint_zone_type::Oximeter {
                address,
            }) => (ServiceName::Oximeter, address),
            BlueprintZoneType::ExternalDns(
                blueprint_zone_type::ExternalDns { http_address, .. },
            ) => (ServiceName::ExternalDns, http_address),
            BlueprintZoneType::InternalDns(
                blueprint_zone_type::InternalDns {
                    http_address,
                    dns_address,
                    ..
                },
            ) => {
                dns_builder.host_zone_internal_dns(
                    zone.id,
                    ServiceName::InternalDns,
                    *http_address,
                    dns_address.to_owned(),
                )?;
                continue 'all_zones;
            }
        };
        dns_builder.host_zone_with_one_backend(
            zone.id,
            service_name,
            address,
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

    // For each sled to which we are supposed to be replicating artifacts,
    // define DNS entries for the repo depot service.
    //
    // Consumers need to be careful in using these names since artifacts are not
    // replicated synchronously or atomically to all instances.  That is: a
    // consumer should be careful when fetching an artifact about whether they
    // really can just pick any backend of this service or not.
    for (sled_id, sled) in sleds_by_id {
        if !sled.policy().matches(SledFilter::TufArtifactReplication) {
            continue;
        }

        let dns_sled =
            dns_builder.host_sled(*sled_id, *sled.sled_agent_address().ip())?;
        dns_builder.service_backend_sled(
            ServiceName::RepoDepot,
            &dns_sled,
            sled.repo_depot_address().port(),
        )?;
    }

    Ok(dns_builder.build_zone())
}

pub fn blueprint_external_dns_config<'a>(
    blueprint: &Blueprint,
    silos: impl IntoIterator<Item = &'a Name>,
    external_dns_zone_name: String,
) -> DnsConfigZone {
    let nexus_external_ips = blueprint_nexus_external_ips(blueprint);
    let mut dns_external_ips = blueprint_external_dns_nameserver_ips(blueprint);

    let nexus_dns_records: Vec<DnsRecord> = nexus_external_ips
        .into_iter()
        .map(|addr| match addr {
            IpAddr::V4(addr) => DnsRecord::A(addr),
            IpAddr::V6(addr) => DnsRecord::Aaaa(addr),
        })
        .collect();

    let mut zone_records: Vec<DnsRecord> = Vec::new();
    // Sort DNS IPs for determinism about which `ns<N>` records have which IPs.
    // This avoids the risk that a permutation of the DNS nameserver IPs
    // produces different records and a DNS configuration change even though the
    // data is logically equivalent.
    dns_external_ips.sort();
    let external_dns_records: Vec<(String, Vec<DnsRecord>)> = dns_external_ips
        .into_iter()
        .enumerate()
        .map(|(idx, dns_ip)| {
            let record = match dns_ip {
                IpAddr::V4(addr) => DnsRecord::A(addr),
                IpAddr::V6(addr) => DnsRecord::Aaaa(addr),
            };
            // `idx` is 0-based, but nameservers start at `ns1` (1-based).
            let name = format!("ns{}", idx + 1);
            zone_records.push(DnsRecord::Ns(format!(
                "{}.{}",
                &name, external_dns_zone_name
            )));
            (name, vec![record])
        })
        .collect();

    let mut records = silos
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
                .then(|| (silo_dns_name(&silo_name), nexus_dns_records.clone()))
        })
        .chain(external_dns_records)
        .collect::<HashMap<String, Vec<DnsRecord>>>();

    if !zone_records.is_empty() {
        records
            .entry(ZONE_APEX_NAME.to_string())
            .or_insert(Vec::new())
            .extend(zone_records);
    }

    DnsConfigZone {
        zone_name: external_dns_zone_name,
        records: records.clone(),
    }
}
