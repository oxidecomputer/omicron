// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Propagates internal DNS changes in a given blueprint

use crate::Sled;
use dns_service_client::DnsDiff;
use internal_dns::DnsConfigBuilder;
use internal_dns::ServiceName;
use nexus_db_model::DnsGroup;
use nexus_db_model::Silo;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::DnsVersionUpdateBuilder;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::OmicronZoneType;
use nexus_types::identity::Resource;
use nexus_types::internal_api::params::DnsConfigParams;
use nexus_types::internal_api::params::DnsConfigZone;
use nexus_types::internal_api::params::DnsRecord;
use omicron_common::address::get_switch_zone_address;
use omicron_common::address::CLICKHOUSE_KEEPER_PORT;
use omicron_common::address::CLICKHOUSE_PORT;
use omicron_common::address::COCKROACH_PORT;
use omicron_common::address::CRUCIBLE_PANTRY_PORT;
use omicron_common::address::CRUCIBLE_PORT;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::DNS_HTTP_PORT;
use omicron_common::address::MGD_PORT;
use omicron_common::address::MGS_PORT;
use omicron_common::address::NEXUS_INTERNAL_PORT;
use omicron_common::address::NTP_PORT;
use omicron_common::address::OXIMETER_PORT;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::InternalContext;
use slog::{debug, info, o};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::IpAddr;
use uuid::Uuid;

pub(crate) async fn deploy_dns(
    opctx: &OpContext,
    datastore: &DataStore,
    creator: String,
    blueprint: &Blueprint,
    sleds_by_id: &BTreeMap<Uuid, Sled>,
) -> Result<(), Error> {
    // First, fetch the current DNS configs.
    let internal_dns_config_current = datastore
        .dns_config_read(opctx, DnsGroup::Internal)
        .await
        .internal_context("reading current DNS (internal)")?;
    let external_dns_config_current = datastore
        .dns_config_read(opctx, DnsGroup::External)
        .await
        .internal_context("reading current DNS (external)")?;

    // We could check here that the DNS version we found isn't newer than when
    // the blueprint was generated.  But we have to check later when we try to
    // update the database anyway.  And we're not wasting much effort allowing
    // this proceed for now.  This way, we have only one code path for this and
    // we know it's being hit when we exercise this condition.

    // Next, construct the DNS config represented by the blueprint.
    let internal_dns_config_blueprint =
        blueprint_internal_dns_config(blueprint, sleds_by_id);
    let silos = datastore
        .silo_list_all_batched(opctx)
        .await
        .internal_context("listing Silos (for configuring external DNS)")?;

    let (nexus_external_ips, nexus_external_dns_zones) =
        datastore.nexus_external_addresses(opctx).await?;
    let nexus_external_dns_zone_names = nexus_external_dns_zones
        .into_iter()
        .map(|z| z.zone_name)
        .collect::<Vec<_>>();
    let external_dns_config_blueprint = blueprint_external_dns_config(
        blueprint,
        &silos,
        &nexus_external_ips,
        &nexus_external_dns_zone_names,
    );

    // Deploy the changes.
    deploy_dns_one(
        opctx,
        datastore,
        creator.clone(),
        blueprint,
        &internal_dns_config_current,
        &internal_dns_config_blueprint,
        DnsGroup::Internal,
    )
    .await?;
    deploy_dns_one(
        opctx,
        datastore,
        creator,
        blueprint,
        &external_dns_config_current,
        &external_dns_config_blueprint,
        DnsGroup::External,
    )
    .await?;
    Ok(())
}

pub(crate) async fn deploy_dns_one(
    opctx: &OpContext,
    datastore: &DataStore,
    creator: String,
    blueprint: &Blueprint,
    dns_config_current: &DnsConfigParams,
    dns_config_blueprint: &DnsConfigParams,
    dns_group: DnsGroup,
) -> Result<(), Error> {
    let log = opctx
        .log
        .new(o!("blueprint_execution" => format!("dns {:?}", dns_group)));

    // Looking at the current contents of DNS, prepare an update that will make
    // it match what it should be.
    let comment = format!("blueprint {} ({})", blueprint.id, blueprint.comment);
    let maybe_update = dns_compute_update(
        &log,
        dns_group,
        comment,
        creator,
        dns_config_current,
        dns_config_blueprint,
    )?;
    let Some(update) = maybe_update else {
        // Nothing to do.
        return Ok(());
    };

    // Our goal here is to update the DNS configuration stored in the database
    // to match the blueprint.  But it's always possible that we're executing a
    // blueprint that's no longer the current target.  In that case, we want to
    // fail without making any changes.  We definitely don't want to
    // accidentally clobber changes that have been made by another instance
    // executing a newer target blueprint.
    //
    // To avoid this problem, before generating a blueprint, Nexus fetches the
    // current DNS generation and stores that into the blueprint itself.  Here,
    // when we execute the blueprint, we make our database update conditional on
    // that still being the current DNS generation.  If some other instance has
    // already come along and updated the database, whether for this same
    // blueprint or a newer one, our attempt to update the database will fail.
    //
    // Let's look at a tricky example.  Suppose:
    //
    // 1. The system starts with some initial blueprint B1 with DNS version 3.
    //    The blueprint has been fully executed and all is well.
    //
    // 2. Blueprint B2 gets generated.  It stores DNS version 3.  It's made the
    //    current target.  Execution has not started yet.
    //
    // 3. Blueprint B3 gets generated.  It also stores DNS version 3 because
    //    that's still the current version in DNS.  B3 is made the current
    //    target.
    //
    //    Assume B2 and B3 specify different DNS contents (e.g., have a
    //    different set of Omicron zones in them).
    //
    // 4. Nexus instance N1 finds B2 to be the current target and starts
    //    executing it.  (Assume it found this between 2 and 3 above.)
    //
    // 5. Nexus instance N2 finds B3 to be the current target and starts
    //    executing it.
    //
    // During execution:
    //
    // * N1 will assemble a new version of DNS called version 4, generate a diff
    //   between version 3 (which is fixed) and version 4, and attempt to apply
    //   this to the database conditional on the current version being version
    //   3.
    //
    // * N2 will do the same, but its version 4 will look different.
    //
    // Now, one of two things could happen:
    //
    // 1. N1 wins.  Its database update applies successfully.  In the database,
    //    the DNS version becomes version 4.  In this case, N2 loses.  Its
    //    database operation fails altogether.  At this point, any subsequent
    //    attempt to execute blueprint B3 will fail because any DNS update will
    //    be conditional on the database having version 3.  The only way out of
    //    this is for the planner to generate a new blueprint B4 that's exactly
    //    equivalent to B3 except that the stored DNS version is 4.  Then we'll
    //    be able to execute that.
    //
    // 2. N2 wins.  Its database update applies successfully.  In the database,
    //    the DNS version becomes version 4.  In this case, N1 loses.  Its
    //    database operation fails altogether.  At this point, any subsequent
    //    attempt to execute blueprint B3 will fail because any DNS update will
    //    be conditional on the databae having version 3.  No further action is
    //    needed, though, because we've successfully executed the latest target
    //    blueprint.
    //
    // In both cases, the system will (1) converge to having successfully
    // executed the target blueprint, and (2) never have rolled any changes back
    // -- DNS only ever moves forward, closer to the latest desired state.
    info!(
        log,
        "attempting to update from generation {} to generation {}",
        dns_config_current.generation,
        dns_config_blueprint.generation,
    );
    let generation_u32 =
        u32::try_from(dns_config_current.generation).map_err(|e| {
            Error::internal_error(&format!(
                "DNS generation got too large: {}",
                e,
            ))
        })?;
    let generation =
        nexus_db_model::Generation::from(Generation::from(generation_u32));
    datastore.dns_update_from_version(opctx, update, generation).await
}

/// Returns the expected contents of internal DNS based on the given blueprint
pub fn blueprint_internal_dns_config(
    blueprint: &Blueprint,
    sleds_by_id: &BTreeMap<Uuid, Sled>,
) -> DnsConfigParams {
    // The DNS names configured here should match what RSS configures for the
    // same zones.  It's tricky to have RSS share the same code because it uses
    // Sled Agent's _internal_ `OmicronZoneConfig` (and friends), whereas we're
    // using `sled-agent-client`'s version of that type.  However, the
    // DnsConfigBuilder's interface is high-level enough that it handles most of
    // the details.
    let mut dns_builder = DnsConfigBuilder::new();

    // The code below assumes that all zones are using the default port numbers.
    // That should be true, as those are the only ports ever used today.
    // In an ideal world, the correct port would be pulled out of the
    // `OmicronZoneType` variant instead.  Although that information is present,
    // it's irritatingly non-trivial to do right now because SocketAddrs are
    // represented as strings, so we'd need to parse all of them and handle all
    // the errors, even though they should never happen.
    // See oxidecomputer/omicron#4988.
    for (_, omicron_zone) in blueprint.all_omicron_zones() {
        if !blueprint.zones_in_service.contains(&omicron_zone.id) {
            continue;
        }

        let (service_name, port) = match omicron_zone.zone_type {
            OmicronZoneType::BoundaryNtp { .. } => {
                (ServiceName::BoundaryNtp, NTP_PORT)
            }
            OmicronZoneType::InternalNtp { .. } => {
                (ServiceName::InternalNtp, NTP_PORT)
            }
            OmicronZoneType::Clickhouse { .. } => {
                (ServiceName::Clickhouse, CLICKHOUSE_PORT)
            }
            OmicronZoneType::ClickhouseKeeper { .. } => {
                (ServiceName::ClickhouseKeeper, CLICKHOUSE_KEEPER_PORT)
            }
            OmicronZoneType::CockroachDb { .. } => {
                (ServiceName::Cockroach, COCKROACH_PORT)
            }
            OmicronZoneType::Nexus { .. } => {
                (ServiceName::Nexus, NEXUS_INTERNAL_PORT)
            }
            OmicronZoneType::Crucible { .. } => {
                (ServiceName::Crucible(omicron_zone.id), CRUCIBLE_PORT)
            }
            OmicronZoneType::CruciblePantry { .. } => {
                (ServiceName::CruciblePantry, CRUCIBLE_PANTRY_PORT)
            }
            OmicronZoneType::Oximeter { .. } => {
                (ServiceName::Oximeter, OXIMETER_PORT)
            }
            OmicronZoneType::ExternalDns { .. } => {
                (ServiceName::ExternalDns, DNS_HTTP_PORT)
            }
            OmicronZoneType::InternalDns { .. } => {
                (ServiceName::InternalDns, DNS_HTTP_PORT)
            }
        };

        // This unwrap is safe because this function only fails if we provide
        // the same zone id twice, which should not be possible here.
        dns_builder
            .host_zone_with_one_backend(
                omicron_zone.id,
                omicron_zone.underlay_address,
                service_name,
                port,
            )
            .unwrap();
    }

    let scrimlets = sleds_by_id.values().filter(|sled| sled.is_scrimlet);
    for scrimlet in scrimlets {
        let sled_subnet = scrimlet.subnet();
        let switch_zone_ip = get_switch_zone_address(sled_subnet);
        // unwrap(): see above.
        dns_builder
            .host_zone_switch(
                scrimlet.id,
                switch_zone_ip,
                DENDRITE_PORT,
                MGS_PORT,
                MGD_PORT,
            )
            .unwrap();
    }

    // We set the generation number for the internal DNS to be newer than
    // whatever it was when this blueprint was generated.  This will only be
    // used if the generated DNS contents are different from what's current.
    dns_builder.generation(blueprint.internal_dns_version.next());
    dns_builder.build()
}

pub fn blueprint_external_dns_config(
    blueprint: &Blueprint,
    silos: &[Silo],
    nexus_external_ips: &[IpAddr],
    external_dns_zone_names: &[String],
) -> DnsConfigParams {
    let dns_records: Vec<DnsRecord> = nexus_external_ips
        .into_iter()
        .map(|addr| match addr {
            IpAddr::V4(addr) => DnsRecord::A(*addr),
            IpAddr::V6(addr) => DnsRecord::Aaaa(*addr),
        })
        .collect();

    let records = silos
        .into_iter()
        .map(|silo| (silo_dns_name(&silo.name()), dns_records.clone()))
        .collect::<HashMap<String, Vec<DnsRecord>>>();

    let zones = external_dns_zone_names
        .into_iter()
        .map(|zone_name| DnsConfigZone {
            zone_name: zone_name.to_owned(),
            records: records.clone(),
        })
        .collect();

    DnsConfigParams {
        generation: u64::from(blueprint.external_dns_version.next()),
        time_created: chrono::Utc::now(),
        zones,
    }
}

fn dns_compute_update(
    log: &slog::Logger,
    dns_group: DnsGroup,
    comment: String,
    creator: String,
    current_config: &DnsConfigParams,
    new_config: &DnsConfigParams,
) -> Result<Option<DnsVersionUpdateBuilder>, Error> {
    let mut update = DnsVersionUpdateBuilder::new(dns_group, comment, creator);

    let diff = DnsDiff::new(&current_config, &new_config)
        .map_err(|e| Error::internal_error(&format!("{:#}", e)))?;
    if diff.is_empty() {
        info!(log, "no changes");
        return Ok(None);
    }

    for (name, new_records) in diff.names_added() {
        debug!(
            log,
            "adding name";
            "name" => name,
            "new_records" => ?new_records,
        );
        update.add_name(
            name.to_string(),
            new_records.into_iter().cloned().collect(),
        )?;
    }

    for (name, old_records) in diff.names_removed() {
        debug!(
            log,
            "removing name";
            "name" => name,
            "old_records" => ?old_records,
        );
        update.remove_name(name.to_string())?;
    }

    for (name, old_records, new_records) in diff.names_changed() {
        debug!(
            log,
            "updating name";
            "name" => name,
            "old_records" => ?old_records,
            "new_records" => ?new_records,
        );
        update.remove_name(name.to_string())?;
        update.add_name(
            name.to_string(),
            new_records.into_iter().cloned().collect(),
        )?;
    }

    Ok(Some(update))
}

#[cfg(test)]
mod test {
    use super::blueprint_internal_dns_config;
    use super::dns_compute_update;
    use crate::Sled;
    use internal_dns::ServiceName;
    use internal_dns::DNS_ZONE;
    use nexus_db_model::DnsGroup;
    use nexus_inventory::CollectionBuilder;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_types::deployment::Blueprint;
    use nexus_types::deployment::OmicronZoneConfig;
    use nexus_types::deployment::OmicronZoneType;
    use nexus_types::deployment::Policy;
    use nexus_types::deployment::SledResources;
    use nexus_types::deployment::ZpoolName;
    use nexus_types::external_api::views::SledPolicy;
    use nexus_types::external_api::views::SledState;
    use nexus_types::internal_api::params::DnsConfigParams;
    use nexus_types::internal_api::params::DnsConfigZone;
    use nexus_types::internal_api::params::DnsRecord;
    use omicron_common::address::get_sled_address;
    use omicron_common::address::get_switch_zone_address;
    use omicron_common::address::Ipv6Subnet;
    use omicron_common::address::RACK_PREFIX;
    use omicron_common::address::SLED_PREFIX;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev::test_setup_log;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
    use std::str::FromStr;
    use uuid::Uuid;

    fn blueprint_empty() -> Blueprint {
        let builder = CollectionBuilder::new("test-suite");
        let collection = builder.build();
        let policy = Policy {
            sleds: BTreeMap::new(),
            service_ip_pool_ranges: vec![],
            target_nexus_zone_count: 3,
        };
        BlueprintBuilder::build_initial_from_collection(
            &collection,
            Generation::new(),
            Generation::new(),
            &policy,
            "test-suite",
        )
        .expect("failed to generate empty blueprint")
    }

    fn dns_config_empty() -> DnsConfigParams {
        DnsConfigParams {
            generation: 1,
            time_created: chrono::Utc::now(),
            zones: vec![DnsConfigZone {
                zone_name: String::from("internal"),
                records: HashMap::new(),
            }],
        }
    }

    /// test blueprint_dns_config(): trivial case of an empty blueprint
    #[test]
    fn test_blueprint_dns_empty() {
        let blueprint = blueprint_empty();
        let blueprint_dns =
            blueprint_internal_dns_config(&blueprint, &BTreeMap::new());
        assert!(blueprint_dns.sole_zone().unwrap().records.is_empty());
    }

    /// test blueprint_dns_config(): exercise various different conditions
    /// - one of each type of zone in service
    /// - some zones not in service
    #[test]
    fn test_blueprint_dns_basic() {
        // We'll use the standard representative inventory collection to build a
        // blueprint.  The main thing we care about here is that we have at
        // least one zone of each type.  Later, we'll mark a couple of the sleds
        // as Scrimlets to exercise that case.
        let representative = nexus_inventory::examples::representative();
        let collection = representative.builder.build();
        let rack_subnet_base: Ipv6Addr =
            "fd00:1122:3344:0100::".parse().unwrap();
        let rack_subnet =
            ipnet::Ipv6Net::new(rack_subnet_base, RACK_PREFIX).unwrap();
        let possible_sled_subnets = rack_subnet.subnets(SLED_PREFIX).unwrap();
        // Ignore sleds with no associated zones in the inventory.
        // This is included in the "representative" collection, but it's
        // not allowed by BlueprintBuilder::build_initial_from_collection().
        let policy_sleds = collection
            .omicron_zones
            .keys()
            .zip(possible_sled_subnets)
            .map(|(sled_id, subnet)| {
                let sled_resources = SledResources {
                    policy: SledPolicy::provisionable(),
                    state: SledState::Active,
                    zpools: BTreeSet::from([ZpoolName::from_str(&format!(
                        "oxp_{}",
                        Uuid::new_v4()
                    ))
                    .unwrap()]),
                    subnet: Ipv6Subnet::new(subnet.network()),
                };
                (*sled_id, sled_resources)
            })
            .collect();

        let policy = Policy {
            sleds: policy_sleds,
            service_ip_pool_ranges: vec![],
            target_nexus_zone_count: 3,
        };
        let dns_empty = dns_config_empty();
        let initial_dns_generation =
            Generation::from(u32::try_from(dns_empty.generation).unwrap());
        let mut blueprint = BlueprintBuilder::build_initial_from_collection(
            &collection,
            initial_dns_generation,
            Generation::new(),
            &policy,
            "test-suite",
        )
        .expect("failed to build initial blueprint");

        // To make things slightly more interesting, let's add a zone that's
        // not currently in service.
        let out_of_service_id = Uuid::new_v4();
        let out_of_service_addr = Ipv6Addr::LOCALHOST;
        blueprint.omicron_zones.values_mut().next().unwrap().zones.push(
            OmicronZoneConfig {
                id: out_of_service_id,
                underlay_address: out_of_service_addr,
                zone_type: OmicronZoneType::Oximeter {
                    address: SocketAddrV6::new(
                        out_of_service_addr,
                        12345,
                        0,
                        0,
                    )
                    .to_string(),
                },
            },
        );

        // To generate the blueprint's DNS config, we need to make up a
        // different set of information about the sleds in our fake system.
        let sleds_by_id = policy
            .sleds
            .iter()
            .enumerate()
            .map(|(i, (sled_id, sled_resources))| {
                let sled_info = Sled {
                    id: *sled_id,
                    sled_agent_address: get_sled_address(sled_resources.subnet),
                    // The first two of these (arbitrarily) will be marked
                    // Scrimlets.
                    is_scrimlet: i < 2,
                };
                (*sled_id, sled_info)
            })
            .collect();

        let dns_config_blueprint =
            blueprint_internal_dns_config(&blueprint, &sleds_by_id);
        assert_eq!(
            dns_config_blueprint.generation,
            u64::from(initial_dns_generation.next())
        );
        let blueprint_dns_zone = dns_config_blueprint.sole_zone().unwrap();
        assert_eq!(blueprint_dns_zone.zone_name, DNS_ZONE);

        // Now, verify a few different properties about the generated DNS
        // configuration:
        //
        // 1. Every zone (except for the one that we added not-in-service)
        //    should have some DNS name with a AAAA record that points at the
        //    zone's underlay IP.  (i.e., every Omiron zone is _in_ DNS)
        //
        // 2. Every SRV record that we find should have a "target" that points
        //    to another name within the DNS configuration, and that name should
        //    be one of the ones with a AAAA record pointing to an Omicron zone.
        //
        // 3. There is at least one SRV record for each service that we expect
        //    to appear in the representative system that we're working with.
        //
        // 4. Our out-of-service zone does *not* appear in the DNS config,
        //    neither with an AAAA record nor in an SRV record.
        //
        // Together, this tells us that we have SRV records for all services,
        // that those SRV records all point to at least one of the Omicron zones
        // for that service, and that we correctly ignored zones that were not
        // in service.

        // To start, we need a mapping from underlay IP to the corresponding
        // Omicron zone.
        let mut omicron_zones_by_ip: BTreeMap<_, _> = blueprint
            .all_omicron_zones()
            .filter(|(_, zone)| zone.id != out_of_service_id)
            .map(|(_, zone)| (zone.underlay_address, zone.id))
            .collect();
        println!("omicron zones by IP: {:#?}", omicron_zones_by_ip);

        // We also want a mapping from underlay IP to the corresponding switch
        // zone.  In this case, the value is the Scrimlet's sled id.
        let mut switch_sleds_by_ip: BTreeMap<_, _> = sleds_by_id
            .iter()
            .filter_map(|(sled_id, sled)| {
                if sled.is_scrimlet {
                    let sled_subnet = policy.sleds.get(sled_id).unwrap().subnet;
                    let switch_zone_ip = get_switch_zone_address(sled_subnet);
                    Some((switch_zone_ip, *sled_id))
                } else {
                    None
                }
            })
            .collect();

        // Now go through all the DNS names that have AAAA records and remove
        // any corresponding Omicron zone.  While doing this, construct a set of
        // the fully-qualified DNS names (i.e., with the zone name suffix
        // appended) that had AAAA records.  We'll use this later to make sure
        // all the SRV records' targets that we find are valid.
        let mut expected_srv_targets: BTreeSet<_> = BTreeSet::new();
        for (name, records) in &blueprint_dns_zone.records {
            let addrs: Vec<_> = records
                .iter()
                .filter_map(|dns_record| match dns_record {
                    DnsRecord::Aaaa(addr) => Some(addr),
                    _ => None,
                })
                .collect();
            for addr in addrs {
                if let Some(zone_id) = omicron_zones_by_ip.remove(addr) {
                    println!(
                        "IP {} found in DNS corresponds with zone {}",
                        addr, zone_id
                    );
                    expected_srv_targets.insert(format!(
                        "{}.{}",
                        name, blueprint_dns_zone.zone_name
                    ));
                    continue;
                }

                if let Some(scrimlet_id) = switch_sleds_by_ip.remove(addr) {
                    println!(
                        "IP {} found in DNS corresponds with switch zone \
                        for Scrimlet {}",
                        addr, scrimlet_id
                    );
                    expected_srv_targets.insert(format!(
                        "{}.{}",
                        name, blueprint_dns_zone.zone_name
                    ));
                    continue;
                }

                println!(
                    "note: found IP ({}) not corresponding to any \
                    Omicron zone or switch zone (name {:?})",
                    addr, name
                );
            }
        }

        println!(
            "Omicron zones whose IPs were not found in DNS: {:?}",
            omicron_zones_by_ip,
        );
        assert!(
            omicron_zones_by_ip.is_empty(),
            "some Omicron zones' IPs were not found in DNS"
        );

        println!(
            "Scrimlets whose switch zone IPs were not found in DNS: {:?}",
            switch_sleds_by_ip,
        );
        assert!(
            switch_sleds_by_ip.is_empty(),
            "some switch zones' IPs were not found in DNS"
        );

        // Now go through all DNS names that have SRV records.  For each one,
        //
        // 1. If its name corresponds to the name of one of the SRV services
        //    that we expect the system to have, record that fact.  At the end
        //    we'll verify that we found at least one SRV record for each such
        //    service.
        //
        // 2. Make sure that the SRV record points at a name that we found in
        //    the previous pass (i.e., that corresponds to an Omicron zone).
        //
        // There are some ServiceNames missing here because they are not part of
        // our representative config (e.g., ClickhouseKeeper) or they don't
        // currently have DNS record at all (e.g., SledAgent, Maghemite, Mgd,
        // Tfport).
        let mut srv_kinds_expected = BTreeSet::from([
            ServiceName::Clickhouse,
            ServiceName::Cockroach,
            ServiceName::InternalDns,
            ServiceName::ExternalDns,
            ServiceName::Nexus,
            ServiceName::Oximeter,
            ServiceName::Dendrite,
            ServiceName::CruciblePantry,
            ServiceName::BoundaryNtp,
            ServiceName::InternalNtp,
        ]);

        for (name, records) in &blueprint_dns_zone.records {
            let srvs: Vec<_> = records
                .iter()
                .filter_map(|dns_record| match dns_record {
                    DnsRecord::Srv(srv) => Some(srv),
                    _ => None,
                })
                .collect();
            for srv in srvs {
                assert!(
                    expected_srv_targets.contains(&srv.target),
                    "found SRV record with target {:?} that does not \
                    correspond to a name that points to any Omicron zone",
                    srv.target
                );
            }

            let kinds_left: Vec<_> =
                srv_kinds_expected.iter().copied().collect();
            for kind in kinds_left {
                if kind.dns_name() == *name {
                    srv_kinds_expected.remove(&kind);
                }
            }
        }

        println!("SRV kinds with no records found: {:?}", srv_kinds_expected);
        assert!(srv_kinds_expected.is_empty());
    }

    #[test]
    fn test_dns_compute_update() {
        let logctx = test_setup_log("dns_compute_update");

        // Start with an empty DNS config.  There's no database update needed
        // when updating the DNS config to itself.
        let dns_empty = dns_config_empty();
        match dns_compute_update(
            &logctx.log,
            DnsGroup::Internal,
            "test-suite".to_string(),
            "test-suite".to_string(),
            &dns_empty,
            &dns_empty,
        ) {
            Ok(None) => (),
            Err(error) => {
                panic!("unexpected error generating update: {:?}", error)
            }
            Ok(Some(diff)) => panic!("unexpected delta: {:?}", diff),
        };

        // Now let's do something a little less trivial.  Set up two slightly
        // different DNS configurations, compute the database update, and make
        // sure it matches what we expect.
        let dns_config1 = DnsConfigParams {
            generation: 4,
            time_created: chrono::Utc::now(),
            zones: vec![DnsConfigZone {
                zone_name: "my-zone".to_string(),
                records: HashMap::from([
                    (
                        "ex1".to_string(),
                        vec![DnsRecord::A(Ipv4Addr::LOCALHOST)],
                    ),
                    (
                        "ex2".to_string(),
                        vec![DnsRecord::A("192.168.1.3".parse().unwrap())],
                    ),
                ]),
            }],
        };

        let dns_config2 = DnsConfigParams {
            generation: 4,
            time_created: chrono::Utc::now(),
            zones: vec![DnsConfigZone {
                zone_name: "my-zone".to_string(),
                records: HashMap::from([
                    (
                        "ex2".to_string(),
                        vec![DnsRecord::A("192.168.1.4".parse().unwrap())],
                    ),
                    (
                        "ex3".to_string(),
                        vec![DnsRecord::A(Ipv4Addr::LOCALHOST)],
                    ),
                ]),
            }],
        };

        let update = dns_compute_update(
            &logctx.log,
            DnsGroup::Internal,
            "test-suite".to_string(),
            "test-suite".to_string(),
            &dns_config1,
            &dns_config2,
        )
        .expect("failed to compute update")
        .expect("unexpectedly produced no update");

        let mut removed: Vec<_> = update.names_removed().collect();
        removed.sort();
        assert_eq!(removed, vec!["ex1", "ex2"]);

        let mut added: Vec<_> = update.names_added().collect();
        added.sort_by_key(|n| n.0);
        assert_eq!(
            added,
            vec![
                (
                    "ex2",
                    [DnsRecord::A("192.168.1.4".parse().unwrap())].as_ref()
                ),
                ("ex3", [DnsRecord::A(Ipv4Addr::LOCALHOST)].as_ref()),
            ]
        );

        logctx.cleanup_successful();
    }
}

// XXX-dap duplicated -- figure out where to put this
/// Returns the (relative) DNS name for this Silo's API and console endpoints
/// _within_ the external DNS zone (i.e., without that zone's suffix)
///
/// This specific naming scheme is determined under RFD 357.
pub(crate) fn silo_dns_name(
    name: &omicron_common::api::external::Name,
) -> String {
    // RFD 4 constrains resource names (including Silo names) to DNS-safe
    // strings, which is why it's safe to directly put the name of the
    // resource into the DNS name rather than doing any kind of escaping.
    format!("{}.sys", name)
}
