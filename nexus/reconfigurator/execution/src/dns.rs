// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Propagates DNS changes in a given blueprint

use crate::overridables::Overridables;
use crate::Sled;
use anyhow::Context;
use dns_service_client::DnsDiff;
use internal_dns::DnsConfigBuilder;
use internal_dns::ServiceName;
use nexus_db_model::DnsGroup;
use nexus_db_model::Silo;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::Discoverability;
use nexus_db_queries::db::datastore::DnsVersionUpdateBuilder;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO_ID;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::OmicronZoneType;
use nexus_types::identity::Resource;
use nexus_types::internal_api::params::DnsConfigParams;
use nexus_types::internal_api::params::DnsConfigZone;
use nexus_types::internal_api::params::DnsRecord;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::InternalContext;
use slog::{debug, info, o};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::IpAddr;
use std::net::SocketAddrV6;
use uuid::Uuid;

pub(crate) async fn deploy_dns(
    opctx: &OpContext,
    datastore: &DataStore,
    creator: String,
    blueprint: &Blueprint,
    sleds_by_id: &BTreeMap<Uuid, Sled>,
    overrides: &Overridables,
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
        blueprint_internal_dns_config(blueprint, sleds_by_id, overrides)
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error constructing internal DNS config: {:#}",
                    e
                ))
            })?;
    let silos = datastore
        .silo_list_all_batched(opctx, Discoverability::All)
        .await
        .internal_context("listing Silos (for configuring external DNS)")?
        .into_iter()
        // We do not generate a DNS name for the "default" Silo.
        .filter(|silo| silo.id() != *DEFAULT_SILO_ID)
        .collect::<Vec<_>>();

    let (nexus_external_ips, nexus_external_dns_zones) =
        datastore.nexus_external_addresses(opctx, Some(blueprint)).await?;
    let nexus_external_dns_zone_names = nexus_external_dns_zones
        .into_iter()
        .map(|z| z.zone_name)
        .collect::<Vec<_>>();
    let external_dns_config_blueprint = blueprint_external_dns_config(
        blueprint,
        &nexus_external_ips,
        &silos,
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
    overrides: &Overridables,
) -> Result<DnsConfigParams, anyhow::Error> {
    // The DNS names configured here should match what RSS configures for the
    // same zones.  It's tricky to have RSS share the same code because it uses
    // Sled Agent's _internal_ `OmicronZoneConfig` (and friends), whereas we're
    // using `sled-agent-client`'s version of that type.  However, the
    // DnsConfigBuilder's interface is high-level enough that it handles most of
    // the details.
    let mut dns_builder = DnsConfigBuilder::new();

    // It's annoying that we have to parse this because it really should be
    // valid already.  See oxidecomputer/omicron#4988.
    fn parse_port(address: &str) -> Result<u16, anyhow::Error> {
        address
            .parse::<SocketAddrV6>()
            .with_context(|| format!("parsing socket address {:?}", address))
            .map(|addr| addr.port())
    }

    for (_, zone) in
        blueprint.all_blueprint_zones(BlueprintZoneFilter::InternalDns)
    {
        let context = || {
            format!(
                "parsing {} zone with id {}",
                zone.config.zone_type.label(),
                zone.config.id
            )
        };

        let (service_name, port) = match &zone.config.zone_type {
            OmicronZoneType::BoundaryNtp { address, .. } => {
                let port = parse_port(&address).with_context(context)?;
                (ServiceName::BoundaryNtp, port)
            }
            OmicronZoneType::InternalNtp { address, .. } => {
                let port = parse_port(&address).with_context(context)?;
                (ServiceName::InternalNtp, port)
            }
            OmicronZoneType::Clickhouse { address, .. } => {
                let port = parse_port(&address).with_context(context)?;
                (ServiceName::Clickhouse, port)
            }
            OmicronZoneType::ClickhouseKeeper { address, .. } => {
                let port = parse_port(&address).with_context(context)?;
                (ServiceName::ClickhouseKeeper, port)
            }
            OmicronZoneType::CockroachDb { address, .. } => {
                let port = parse_port(&address).with_context(context)?;
                (ServiceName::Cockroach, port)
            }
            OmicronZoneType::Nexus { internal_address, .. } => {
                let port =
                    parse_port(internal_address).with_context(context)?;
                (ServiceName::Nexus, port)
            }
            OmicronZoneType::Crucible { address, .. } => {
                let port = parse_port(address).with_context(context)?;
                (ServiceName::Crucible(zone.config.id), port)
            }
            OmicronZoneType::CruciblePantry { address } => {
                let port = parse_port(address).with_context(context)?;
                (ServiceName::CruciblePantry, port)
            }
            OmicronZoneType::Oximeter { address } => {
                let port = parse_port(address).with_context(context)?;
                (ServiceName::Oximeter, port)
            }
            OmicronZoneType::ExternalDns { http_address, .. } => {
                let port = parse_port(http_address).with_context(context)?;
                (ServiceName::ExternalDns, port)
            }
            OmicronZoneType::InternalDns { http_address, .. } => {
                let port = parse_port(http_address).with_context(context)?;
                (ServiceName::InternalDns, port)
            }
        };

        // This unwrap is safe because this function only fails if we provide
        // the same zone id twice, which should not be possible here.
        dns_builder
            .host_zone_with_one_backend(
                zone.config.id,
                zone.config.underlay_address,
                service_name,
                port,
            )
            .unwrap();
    }

    let scrimlets = sleds_by_id.values().filter(|sled| sled.is_scrimlet);
    for scrimlet in scrimlets {
        let sled_subnet = scrimlet.subnet();
        let switch_zone_ip = overrides.switch_zone_ip(scrimlet.id, sled_subnet);
        // unwrap(): see above.
        dns_builder
            .host_zone_switch(
                scrimlet.id,
                switch_zone_ip,
                overrides.dendrite_port(scrimlet.id),
                overrides.mgs_port(scrimlet.id),
                overrides.mgd_port(scrimlet.id),
            )
            .unwrap();
    }

    // We set the generation number for the internal DNS to be newer than
    // whatever it was when this blueprint was generated.  This will only be
    // used if the generated DNS contents are different from what's current.
    dns_builder.generation(blueprint.internal_dns_version.next());
    Ok(dns_builder.build())
}

pub fn blueprint_external_dns_config(
    blueprint: &Blueprint,
    nexus_external_ips: &[IpAddr],
    silos: &[Silo],
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
            "dns_name" => name,
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
            "dns_name" => name,
            "old_records" => ?old_records,
        );
        update.remove_name(name.to_string())?;
    }

    for (name, old_records, new_records) in diff.names_changed() {
        debug!(
            log,
            "updating name";
            "dns_name" => name,
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

/// Returns the (relative) DNS name for this Silo's API and console endpoints
/// _within_ the external DNS zone (i.e., without that zone's suffix)
///
/// This specific naming scheme is determined under RFD 357.
pub fn silo_dns_name(name: &omicron_common::api::external::Name) -> String {
    // RFD 4 constrains resource names (including Silo names) to DNS-safe
    // strings, which is why it's safe to directly put the name of the
    // resource into the DNS name rather than doing any kind of escaping.
    format!("{}.sys", name)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::overridables::Overridables;
    use crate::Sled;
    use dns_service_client::DnsDiff;
    use internal_dns::ServiceName;
    use internal_dns::DNS_ZONE;
    use nexus_db_model::DnsGroup;
    use nexus_db_model::Silo;
    use nexus_db_queries::authn;
    use nexus_db_queries::authz;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::DataStore;
    use nexus_inventory::CollectionBuilder;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_reconfigurator_planning::blueprint_builder::EnsureMultiple;
    use nexus_reconfigurator_planning::example::example;
    use nexus_reconfigurator_preparation::policy_from_db;
    use nexus_test_utils::resource_helpers::create_silo;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::Blueprint;
    use nexus_types::deployment::BlueprintTarget;
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::OmicronZoneConfig;
    use nexus_types::deployment::OmicronZoneType;
    use nexus_types::deployment::Policy;
    use nexus_types::deployment::SledResources;
    use nexus_types::deployment::ZpoolName;
    use nexus_types::external_api::params;
    use nexus_types::external_api::shared;
    use nexus_types::external_api::views::SledPolicy;
    use nexus_types::external_api::views::SledState;
    use nexus_types::identity::Resource;
    use nexus_types::internal_api::params::DnsConfigParams;
    use nexus_types::internal_api::params::DnsConfigZone;
    use nexus_types::internal_api::params::DnsRecord;
    use nexus_types::internal_api::params::Srv;
    use omicron_common::address::get_sled_address;
    use omicron_common::address::get_switch_zone_address;
    use omicron_common::address::IpRange;
    use omicron_common::address::Ipv6Subnet;
    use omicron_common::address::NEXUS_REDUNDANCY;
    use omicron_common::address::RACK_PREFIX;
    use omicron_common::address::SLED_PREFIX;
    use omicron_common::api::external::Generation;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev::test_setup_log;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
    use std::str::FromStr;
    use std::sync::Arc;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

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

    /// test blueprint_internal_dns_config(): trivial case of an empty blueprint
    #[test]
    fn test_blueprint_internal_dns_empty() {
        let blueprint = blueprint_empty();
        let blueprint_dns = blueprint_internal_dns_config(
            &blueprint,
            &BTreeMap::new(),
            &Default::default(),
        )
        .unwrap();
        assert!(blueprint_dns.sole_zone().unwrap().records.is_empty());
    }

    /// test blueprint_dns_config(): exercise various different conditions
    /// - one of each type of zone in service
    /// - some zones not in service
    #[test]
    fn test_blueprint_internal_dns_basic() {
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
        blueprint.blueprint_zones.values_mut().next().unwrap().zones.push(
            BlueprintZoneConfig {
                config: OmicronZoneConfig {
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
                disposition: BlueprintZoneDisposition::Quiesced,
            },
        );

        // To generate the blueprint's DNS config, we need to make up a
        // different set of information about the Quiesced fake system.
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

        let dns_config_blueprint = blueprint_internal_dns_config(
            &blueprint,
            &sleds_by_id,
            &Default::default(),
        )
        .unwrap();
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

    #[tokio::test]
    async fn test_blueprint_external_dns_basic() {
        static TEST_NAME: &str = "test_blueprint_external_dns_basic";
        let logctx = test_setup_log(TEST_NAME);
        let (collection, policy) = example(&logctx.log, TEST_NAME, 5);
        let initial_external_dns_generation = Generation::new();
        let blueprint = BlueprintBuilder::build_initial_from_collection(
            &collection,
            Generation::new(),
            initial_external_dns_generation,
            &policy,
            "test suite",
        )
        .expect("failed to generate initial blueprint");

        let my_silo = Silo::new(params::SiloCreate {
            identity: IdentityMetadataCreateParams {
                name: "my-silo".parse().unwrap(),
                description: String::new(),
            },
            quotas: params::SiloQuotasCreate::empty(),
            discoverable: false,
            identity_mode: shared::SiloIdentityMode::SamlJit,
            admin_group_name: None,
            tls_certificates: vec![],
            mapped_fleet_roles: Default::default(),
        })
        .unwrap();

        let nexus_external_ips: Vec<_> = blueprint
            .all_omicron_zones()
            .filter_map(|(_, z)| match &z.zone_type {
                OmicronZoneType::Nexus { external_ip, .. } => {
                    Some(*external_ip)
                }
                _ => None,
            })
            .collect();

        // It shouldn't ever be possible to have no Silos at all, but at least
        // make sure we don't panic.
        let external_dns_config = blueprint_external_dns_config(
            &blueprint,
            &nexus_external_ips,
            &[],
            &[String::from("oxide.test")],
        );
        assert_eq!(
            external_dns_config.generation,
            u64::from(initial_external_dns_generation.next())
        );
        assert_eq!(external_dns_config.zones.len(), 1);
        assert_eq!(external_dns_config.zones[0].zone_name, "oxide.test");
        assert!(external_dns_config.zones[0].records.is_empty());

        // Same with external DNS zones.
        let external_dns_config = blueprint_external_dns_config(
            &blueprint,
            &nexus_external_ips,
            std::slice::from_ref(&my_silo),
            &[],
        );
        assert_eq!(
            external_dns_config.generation,
            u64::from(initial_external_dns_generation.next())
        );
        assert!(external_dns_config.zones.is_empty());

        // Same with external IPs.
        let external_dns_config = blueprint_external_dns_config(
            &blueprint,
            &[],
            std::slice::from_ref(&my_silo),
            &[String::from("oxide.test")],
        );
        assert_eq!(
            external_dns_config.generation,
            u64::from(initial_external_dns_generation.next())
        );

        // Now check a more typical case.  (Although we wouldn't normally have
        // more than one external DNS zone, it's a more general case and pretty
        // easy to test.)
        let external_dns_config = blueprint_external_dns_config(
            &blueprint,
            &nexus_external_ips,
            std::slice::from_ref(&my_silo),
            &[String::from("oxide1.test"), String::from("oxide2.test")],
        );
        assert_eq!(
            external_dns_config.generation,
            u64::from(initial_external_dns_generation.next())
        );
        assert_eq!(external_dns_config.zones.len(), 2);
        assert_eq!(
            external_dns_config.zones[0].records,
            external_dns_config.zones[1].records
        );
        assert_eq!(
            external_dns_config.zones[0].zone_name,
            String::from("oxide1.test"),
        );
        assert_eq!(
            external_dns_config.zones[1].zone_name,
            String::from("oxide2.test"),
        );
        let records = &external_dns_config.zones[0].records;
        assert_eq!(records.len(), 1);
        let silo_records = records
            .get(&silo_dns_name(my_silo.name()))
            .expect("missing silo DNS records");

        // Here we're hardcoding the contents of the example blueprint.  It
        // currently puts one Nexus zone on each sled.  If we change the example
        // blueprint, change the expected set of IPs here.
        let mut silo_record_ips: Vec<_> = silo_records
            .into_iter()
            .map(|record| match record {
                DnsRecord::A(v) => IpAddr::V4(*v),
                DnsRecord::Aaaa(v) => IpAddr::V6(*v),
                DnsRecord::Srv(_) => panic!("unexpected SRV record"),
            })
            .collect();
        silo_record_ips.sort();
        assert_eq!(
            silo_record_ips,
            &[
                "192.0.2.2".parse::<IpAddr>().unwrap(),
                "192.0.2.3".parse::<IpAddr>().unwrap(),
                "192.0.2.4".parse::<IpAddr>().unwrap(),
                "192.0.2.5".parse::<IpAddr>().unwrap(),
                "192.0.2.6".parse::<IpAddr>().unwrap(),
            ]
        );
        logctx.cleanup_successful();
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

        // Test the difference between two configs whose SRV records differ.
        let mut dns_config1 = dns_config1.clone();
        dns_config1.zones[0].records.insert(
            String::from("_nexus._tcp"),
            vec![
                DnsRecord::Srv(Srv {
                    port: 123,
                    prio: 1,
                    target: String::from("ex1.my-zone"),
                    weight: 2,
                }),
                DnsRecord::Srv(Srv {
                    port: 123,
                    prio: 1,
                    target: String::from("ex2.my-zone"),
                    weight: 2,
                }),
            ],
        );
        // A clone of the same one should of course be the same as the original.
        let mut dns_config2 = dns_config1.clone();
        let update = dns_compute_update(
            &logctx.log,
            DnsGroup::Internal,
            "test-suite".to_string(),
            "test-suite".to_string(),
            &dns_config1,
            &dns_config2,
        )
        .expect("failed to compute update");
        assert!(update.is_none());

        // If we shift the order of the items, it should still reflect no
        // changes.
        let records =
            dns_config2.zones[0].records.get_mut("_nexus._tcp").unwrap();
        records.rotate_left(1);
        assert!(
            records != dns_config1.zones[0].records.get("_nexus._tcp").unwrap()
        );
        let update = dns_compute_update(
            &logctx.log,
            DnsGroup::Internal,
            "test-suite".to_string(),
            "test-suite".to_string(),
            &dns_config1,
            &dns_config2,
        )
        .expect("failed to compute update");
        assert!(update.is_none());

        // If we add another record, there should indeed be a new update.
        let records =
            dns_config2.zones[0].records.get_mut("_nexus._tcp").unwrap();
        records.push(DnsRecord::Srv(Srv {
            port: 123,
            prio: 1,
            target: String::from("ex3.my-zone"),
            weight: 2,
        }));
        let final_records = records.clone();

        let update = dns_compute_update(
            &logctx.log,
            DnsGroup::Internal,
            "test-suite".to_string(),
            "test-suite".to_string(),
            &dns_config1,
            &dns_config2,
        )
        .expect("failed to compute update")
        .expect("expected an update");

        assert_eq!(
            update.names_removed().collect::<Vec<_>>(),
            &["_nexus._tcp"]
        );
        assert_eq!(
            update.names_added().collect::<Vec<_>>(),
            &[("_nexus._tcp", final_records.as_slice())]
        );

        logctx.cleanup_successful();
    }

    // Tests end-to-end DNS behavior:
    //
    // - If we create a blueprint matching the current system, and then apply
    //   it, there are no changes to either internal or external DNS
    //
    // - If we create a Silo, DNS will be updated.  If we then re-execute the
    //   previous blueprint, again, there will be no new changes to DNS.
    //
    // - If we then generate a blueprint with a Nexus zone and execute the DNS
    //   part of that, then:
    //
    //   - internal DNS SRV record for _nexus._tcp is added
    //   - internal DNS AAAA record for the new zone is added
    //   - external DNS gets a A record for the new zone's external IP
    //
    // - If we subsequently create a new Silo, the new Silo's DNS record
    //   reflects the Nexus zone that was added.
    #[nexus_test]
    async fn test_silos_external_dns_end_to_end(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.apictx().nexus;
        let datastore = nexus.datastore();
        let log = &cptestctx.logctx.log;
        let opctx = OpContext::for_background(
            log.clone(),
            Arc::new(authz::Authz::new(log)),
            authn::Context::internal_api(),
            datastore.clone(),
        );

        // Fetch the initial contents of internal and external DNS.
        let dns_initial_internal = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .expect("fetching initial internal DNS");
        let dns_initial_external = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .expect("fetching initial external DNS");

        // Fetch the initial blueprint installed during rack initialization.
        let (_blueprint_target, blueprint) = datastore
            .blueprint_target_get_current_full(&opctx)
            .await
            .expect("failed to read current target blueprint")
            .expect("no target blueprint set");
        eprintln!("blueprint: {:?}", blueprint);

        // Now, execute the initial blueprint.
        let overrides = Overridables::for_test(cptestctx);
        crate::realize_blueprint_with_overrides(
            &opctx,
            datastore,
            &blueprint,
            "test-suite",
            &overrides,
        )
        .await
        .expect("failed to execute initial blueprint");

        // DNS ought not to have changed.
        verify_dns_unchanged(
            &opctx,
            datastore,
            &dns_initial_internal,
            &dns_initial_external,
        )
        .await;

        // Create a Silo.  Make sure that external DNS is updated (and that
        // internal DNS is not).  Then make sure that if we execute the same
        // blueprint again, DNS does not change again (i.e., that it does not
        // revert somehow).
        let dns_latest_external = create_silo_and_verify_dns(
            cptestctx,
            &opctx,
            datastore,
            &blueprint,
            &overrides,
            "squidport",
            &dns_initial_internal,
            &dns_initial_external,
        )
        .await;

        // Now, go through the motions of provisioning a new Nexus zone.
        // We do this directly with BlueprintBuilder to avoid the planner
        // deciding to make other unrelated changes.
        let sled_rows = datastore.sled_list_all_batched(&opctx).await.unwrap();
        let zpool_rows =
            datastore.zpool_list_all_external_batched(&opctx).await.unwrap();
        let ip_pool_range_rows = {
            let (authz_service_ip_pool, _) =
                datastore.ip_pools_service_lookup(&opctx).await.unwrap();
            datastore
                .ip_pool_list_ranges_batched(&opctx, &authz_service_ip_pool)
                .await
                .unwrap()
        };
        let mut policy = policy_from_db(
            &sled_rows,
            &zpool_rows,
            &ip_pool_range_rows,
            // This is not used because we're not actually going through the
            // planner.
            NEXUS_REDUNDANCY,
        )
        .unwrap();
        // We'll need another (fake) external IP for this new Nexus.
        policy
            .service_ip_pool_ranges
            .push(IpRange::from(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        let mut builder = BlueprintBuilder::new_based_on(
            &log,
            &blueprint,
            Generation::from(
                u32::try_from(dns_initial_internal.generation).unwrap(),
            ),
            Generation::from(
                u32::try_from(dns_latest_external.generation).unwrap(),
            ),
            &policy,
            "test suite",
        )
        .unwrap();
        let sled_id =
            blueprint.sleds().next().expect("expected at least one sled");
        let nalready = builder.sled_num_nexus_zones(sled_id);
        let rv = builder
            .sled_ensure_zone_multiple_nexus(sled_id, nalready + 1)
            .unwrap();
        assert_eq!(rv, EnsureMultiple::Added(1));
        let blueprint2 = builder.build();
        eprintln!("blueprint2: {:?}", blueprint2);
        // Figure out the id of the new zone.
        let zones_before = blueprint
            .all_omicron_zones()
            .filter_map(|(_, z)| z.zone_type.is_nexus().then_some(z.id))
            .collect::<BTreeSet<_>>();
        let zones_after = blueprint2
            .all_omicron_zones()
            .filter_map(|(_, z)| z.zone_type.is_nexus().then_some(z.id))
            .collect::<BTreeSet<_>>();
        let new_zones: Vec<_> = zones_after.difference(&zones_before).collect();
        assert_eq!(new_zones.len(), 1);
        let new_zone_id = *new_zones[0];

        // Set this blueprint as the current target.  We set it to disabled
        // because we're controlling the execution directly here.  But we need
        // to do this so that silo creation sees the change.
        datastore
            .blueprint_insert(&opctx, &blueprint2)
            .await
            .expect("failed to save blueprint to database");
        datastore
            .blueprint_target_set_current(
                &opctx,
                BlueprintTarget {
                    target_id: blueprint2.id,
                    enabled: false,
                    time_made_target: chrono::Utc::now(),
                },
            )
            .await
            .expect("failed to set blueprint as target");

        crate::realize_blueprint_with_overrides(
            &opctx,
            datastore,
            &blueprint2,
            "test-suite",
            &overrides,
        )
        .await
        .expect("failed to execute second blueprint");

        // Now fetch DNS again.  Both should have changed this time.
        let dns_latest_internal = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .expect("fetching latest internal DNS");

        assert_eq!(
            dns_latest_internal.generation,
            dns_initial_internal.generation + 1,
        );

        let diff =
            DnsDiff::new(&dns_initial_internal, &dns_latest_internal).unwrap();
        // There should be one new AAAA record for the zone itself.
        let new_records: Vec<_> = diff.names_added().collect();
        let (new_name, &[DnsRecord::Aaaa(_)]) = new_records[0] else {
            panic!("did not find expected AAAA record for new Nexus zone");
        };
        let new_zone_host = internal_dns::config::Host::for_zone(
            new_zone_id,
            internal_dns::config::ZoneVariant::Other,
        );
        assert!(new_zone_host.fqdn().starts_with(new_name));

        // Nothing was removed.
        assert!(diff.names_removed().next().is_none());

        // The SRV record for Nexus itself ought to have changed, growing one
        // more record -- for the new AAAA record above.
        let changed: Vec<_> = diff.names_changed().collect();
        assert_eq!(changed.len(), 1);
        let (name, old_records, new_records) = changed[0];
        assert_eq!(name, ServiceName::Nexus.dns_name());
        let new_srv = subset_plus_one(old_records, new_records);
        let DnsRecord::Srv(new_srv) = new_srv else {
            panic!("expected SRV record, found {:?}", new_srv);
        };
        assert_eq!(new_srv.target, new_zone_host.fqdn());

        // As for external DNS: all existing names ought to have been changed,
        // gaining a new A record for the new host.
        let dns_previous_external = dns_latest_external;
        let dns_latest_external = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .expect("fetching latest external DNS");
        assert_eq!(
            dns_latest_external.generation,
            dns_previous_external.generation + 1,
        );
        let diff =
            DnsDiff::new(&dns_previous_external, &dns_latest_external).unwrap();
        assert!(diff.names_added().next().is_none());
        assert!(diff.names_removed().next().is_none());
        let changed: Vec<_> = diff.names_changed().collect();
        for (name, old_records, new_records) in changed {
            // These are Silo names and end with ".sys".
            assert!(name.ends_with(".sys"));
            // We can't really tell which one points to what, especially in the
            // test suite where all Nexus zones use localhost for their external
            // IP.  All we can tell is that there's one new one.
            assert_eq!(old_records.len() + 1, new_records.len());
        }

        // If we execute it again, we should see no more changes.
        crate::realize_blueprint_with_overrides(
            &opctx,
            datastore,
            &blueprint2,
            "test-suite",
            &overrides,
        )
        .await
        .expect("failed to execute second blueprint again");
        verify_dns_unchanged(
            &opctx,
            datastore,
            &dns_latest_internal,
            &dns_latest_external,
        )
        .await;

        // Now create another Silo and verify the changes to DNS.
        // This ensures that the "create Silo" path picks up Nexus instances
        // that exist only in Reconfigurator, not the services table.
        let dns_latest_external = create_silo_and_verify_dns(
            &cptestctx,
            &opctx,
            datastore,
            &blueprint2,
            &overrides,
            "tickety-boo",
            &dns_latest_internal,
            &dns_latest_external,
        )
        .await;

        // One more time, make sure that executing the blueprint does not do
        // anything.
        crate::realize_blueprint_with_overrides(
            &opctx,
            datastore,
            &blueprint2,
            "test-suite",
            &overrides,
        )
        .await
        .expect("failed to execute second blueprint again");
        verify_dns_unchanged(
            &opctx,
            datastore,
            &dns_latest_internal,
            &dns_latest_external,
        )
        .await;
    }

    fn subset_plus_one<'a, T: std::fmt::Debug + Ord + Eq>(
        list1: &'a [T],
        list2: &'a [T],
    ) -> &'a T {
        let set: BTreeSet<_> = list1.into_iter().collect();
        let mut extra = Vec::with_capacity(1);
        for item in list2 {
            if !set.contains(&item) {
                extra.push(item);
            }
        }

        if extra.len() != 1 {
            panic!(
                "expected list2 to have one extra element:\n\
                list1: {:?}\n\
                list2: {:?}\n
                extra: {:?}\n",
                list1, list2, extra
            );
        }

        extra.into_iter().next().unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_silo_and_verify_dns(
        cptestctx: &ControlPlaneTestContext,
        opctx: &OpContext,
        datastore: &DataStore,
        blueprint: &Blueprint,
        overrides: &Overridables,
        silo_name: &str,
        old_internal: &DnsConfigParams,
        old_external: &DnsConfigParams,
    ) -> DnsConfigParams {
        // Create a Silo.  Make sure that external DNS is updated (and that
        // internal DNS is not).  This is tested elsewhere already but really we
        // want to make sure that if we then execute the blueprint again, DNS
        // does not change _again_ (i.e., does not somehow revert).
        let silo = create_silo(
            &cptestctx.external_client,
            silo_name,
            false,
            shared::SiloIdentityMode::SamlJit,
        )
        .await;

        let dns_latest_internal = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .expect("fetching latest internal DNS");
        assert_eq!(old_internal.generation, dns_latest_internal.generation);
        let dns_latest_external = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .expect("fetching latest external DNS");
        assert_eq!(old_external.generation + 1, dns_latest_external.generation);

        // Specifically, there should be one new name (for the new Silo).
        let diff = DnsDiff::new(&old_external, &dns_latest_external).unwrap();
        assert!(diff.names_removed().next().is_none());
        assert!(diff.names_changed().next().is_none());
        let added = diff.names_added().collect::<Vec<_>>();
        assert_eq!(added.len(), 1);
        let (new_name, new_records) = added[0];
        assert_eq!(new_name, silo_dns_name(&silo.identity.name));
        // And it should have the same IP addresses as all of the other Silos.
        assert_eq!(
            new_records,
            old_external.zones[0].records.values().next().unwrap()
        );

        // If we execute the blueprint, DNS should not be changed.
        crate::realize_blueprint_with_overrides(
            &opctx,
            datastore,
            &blueprint,
            "test-suite",
            &overrides,
        )
        .await
        .expect("failed to execute blueprint");
        let dns_latest_internal = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .expect("fetching latest internal DNS");
        let dns_latest_external = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .expect("fetching latest external DNS");
        assert_eq!(old_internal.generation, dns_latest_internal.generation);
        assert_eq!(old_external.generation + 1, dns_latest_external.generation);

        dns_latest_external
    }

    async fn verify_dns_unchanged(
        opctx: &OpContext,
        datastore: &DataStore,
        old_internal: &DnsConfigParams,
        old_external: &DnsConfigParams,
    ) {
        let dns_latest_internal = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .expect("fetching latest internal DNS");
        let dns_latest_external = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .expect("fetching latest external DNS");

        assert_eq!(old_internal.generation, dns_latest_internal.generation);
        assert_eq!(old_external.generation, dns_latest_external.generation);
    }
}
