// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Propagates DNS changes in a given blueprint

use crate::Sled;
use internal_dns_types::diff::DnsDiff;
use nexus_db_model::DnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::Discoverability;
use nexus_db_queries::db::datastore::DnsVersionUpdateBuilder;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::execution::Overridables;
use nexus_types::deployment::execution::blueprint_external_dns_config;
use nexus_types::deployment::execution::blueprint_internal_dns_config;
use nexus_types::identity::Resource;
use nexus_types::internal_api::params::DnsConfigParams;
use nexus_types::internal_api::params::DnsConfigZone;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::bail_unless;
use omicron_uuid_kinds::SledUuid;
use slog::{debug, info, o};
use std::collections::BTreeMap;

pub(crate) async fn deploy_dns(
    opctx: &OpContext,
    datastore: &DataStore,
    creator: String,
    blueprint: &Blueprint,
    sleds_by_id: &BTreeMap<SledUuid, Sled>,
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
    let internal_dns_zone_blueprint =
        blueprint_internal_dns_config(blueprint, sleds_by_id, overrides)
            .map_err(|e| Error::InternalError {
                internal_message: e.to_string(),
            })?;
    let silos = datastore
        .silo_list_all_batched(opctx, Discoverability::All)
        .await
        .internal_context("listing Silos (for configuring external DNS)")?
        .into_iter()
        .map(|silo| silo.name().clone())
        .collect::<Vec<_>>();

    let nexus_external_dns_zone_names = datastore
        .dns_zones_list_all(opctx, DnsGroup::External)
        .await
        .internal_context("listing DNS zones")?
        .into_iter()
        .map(|z| z.zone_name)
        .collect::<Vec<_>>();
    // Other parts of the system support multiple external DNS zone names.  We
    // do not here.  If we decide to support this in the future, this mechanism
    // will need to be updated.
    bail_unless!(
        nexus_external_dns_zone_names.len() == 1,
        "expected exactly one external DNS zone"
    );
    // unwrap: we just checked the length.
    let external_dns_zone_name =
        nexus_external_dns_zone_names.into_iter().next().unwrap();
    let external_dns_zone_blueprint = blueprint_external_dns_config(
        blueprint,
        &silos,
        external_dns_zone_name,
    );

    // Deploy the changes.
    deploy_dns_one(
        opctx,
        datastore,
        creator.clone(),
        blueprint,
        &internal_dns_config_current,
        internal_dns_zone_blueprint,
        DnsGroup::Internal,
    )
    .await?;
    deploy_dns_one(
        opctx,
        datastore,
        creator,
        blueprint,
        &external_dns_config_current,
        external_dns_zone_blueprint,
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
    dns_zone_blueprint: DnsConfigZone,
    dns_group: DnsGroup,
) -> Result<(), Error> {
    let log = opctx
        .log
        .new(o!("blueprint_execution" => format!("dns {:?}", dns_group)));

    // Other parts of the system support multiple external DNS zones.  We do not
    // do so here.
    let dns_zone_current = dns_config_current
        .sole_zone()
        .map_err(|e| Error::internal_error(&format!("{:#}", e)))?;

    // Looking at the current contents of DNS, prepare an update that will make
    // it match what it should be.
    let comment = format!("blueprint {} ({})", blueprint.id, blueprint.comment);
    let maybe_update = dns_compute_update(
        &log,
        dns_group,
        comment,
        creator,
        dns_zone_current,
        &dns_zone_blueprint,
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
    let blueprint_generation = match dns_group {
        DnsGroup::Internal => blueprint.internal_dns_version,
        DnsGroup::External => blueprint.external_dns_version,
    };
    let dns_config_blueprint = DnsConfigParams {
        zones: vec![dns_zone_blueprint],
        time_created: chrono::Utc::now(),
        generation: blueprint_generation.next(),
    };

    info!(
        log,
        "attempting to update from generation {} to generation {}",
        dns_config_current.generation,
        dns_config_blueprint.generation,
    );
    datastore
        .dns_update_from_version(
            opctx,
            update,
            dns_config_current.generation.into(),
        )
        .await
}

fn dns_compute_update(
    log: &slog::Logger,
    dns_group: DnsGroup,
    comment: String,
    creator: String,
    current_zone: &DnsConfigZone,
    new_zone: &DnsConfigZone,
) -> Result<Option<DnsVersionUpdateBuilder>, Error> {
    let mut update = DnsVersionUpdateBuilder::new(dns_group, comment, creator);

    let diff = DnsDiff::new(&current_zone, &new_zone)
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::Sled;
    use crate::test_utils::overridables_for_test;
    use crate::test_utils::realize_blueprint_and_expect;
    use id_map::IdMap;
    use internal_dns_resolver::Resolver;
    use internal_dns_types::config::Host;
    use internal_dns_types::config::Zone;
    use internal_dns_types::names::BOUNDARY_NTP_DNS_NAME;
    use internal_dns_types::names::DNS_ZONE;
    use internal_dns_types::names::ServiceName;
    use nexus_db_model::DnsGroup;
    use nexus_db_model::Silo;
    use nexus_db_queries::authn;
    use nexus_db_queries::authz;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::DataStore;
    use nexus_inventory::CollectionBuilder;
    use nexus_inventory::now_db_precision;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
    use nexus_reconfigurator_preparation::PlanningInputFromDb;
    use nexus_sled_agent_shared::inventory::OmicronZoneConfig;
    use nexus_sled_agent_shared::inventory::OmicronZoneImageSource;
    use nexus_sled_agent_shared::inventory::OmicronZoneType;
    use nexus_sled_agent_shared::inventory::SledRole;
    use nexus_sled_agent_shared::inventory::ZoneKind;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils::resource_helpers::create_silo;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::Blueprint;
    use nexus_types::deployment::BlueprintSledConfig;
    use nexus_types::deployment::BlueprintTarget;
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneImageSource;
    use nexus_types::deployment::BlueprintZoneType;
    use nexus_types::deployment::CockroachDbClusterVersion;
    use nexus_types::deployment::CockroachDbPreserveDowngrade;
    use nexus_types::deployment::CockroachDbSettings;
    pub use nexus_types::deployment::OmicronZoneExternalFloatingAddr;
    pub use nexus_types::deployment::OmicronZoneExternalFloatingIp;
    pub use nexus_types::deployment::OmicronZoneExternalSnatIp;
    use nexus_types::deployment::OximeterReadMode;
    use nexus_types::deployment::OximeterReadPolicy;
    use nexus_types::deployment::PendingMgsUpdates;
    use nexus_types::deployment::SledFilter;
    use nexus_types::deployment::blueprint_zone_type;
    use nexus_types::external_api::params;
    use nexus_types::external_api::shared;
    use nexus_types::external_api::views::SledPolicy;
    use nexus_types::external_api::views::SledProvisionPolicy;
    use nexus_types::external_api::views::SledState;
    use nexus_types::identity::Resource;
    use nexus_types::internal_api::params::DnsConfigParams;
    use nexus_types::internal_api::params::DnsConfigZone;
    use nexus_types::internal_api::params::DnsRecord;
    use nexus_types::internal_api::params::Srv;
    use nexus_types::silo::silo_dns_name;
    use omicron_common::address::IpRange;
    use omicron_common::address::Ipv6Subnet;
    use omicron_common::address::RACK_PREFIX;
    use omicron_common::address::REPO_DEPOT_PORT;
    use omicron_common::address::SLED_PREFIX;
    use omicron_common::address::get_sled_address;
    use omicron_common::address::get_switch_zone_address;
    use omicron_common::api::external::Generation;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::policy::BOUNDARY_NTP_REDUNDANCY;
    use omicron_common::policy::COCKROACHDB_REDUNDANCY;
    use omicron_common::policy::CRUCIBLE_PANTRY_REDUNDANCY;
    use omicron_common::policy::INTERNAL_DNS_REDUNDANCY;
    use omicron_common::policy::NEXUS_REDUNDANCY;
    use omicron_common::policy::OXIMETER_REDUNDANCY;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::BlueprintUuid;
    use omicron_uuid_kinds::ExternalIpUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::mem;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
    use std::sync::Arc;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

    fn dns_config_empty() -> DnsConfigParams {
        DnsConfigParams {
            generation: Generation::new(),
            time_created: chrono::Utc::now(),
            zones: vec![DnsConfigZone {
                zone_name: String::from("internal"),
                records: HashMap::new(),
            }],
        }
    }

    /// **********************************************************************
    /// DEPRECATION WARNING:
    ///
    /// Remove when `deprecated_omicron_zone_config_to_blueprint_zone_config`
    /// is deleted.
    /// **********************************************************************
    ///
    /// Errors from converting an [`OmicronZoneType`] into a [`BlueprintZoneType`].
    #[derive(Debug, Clone)]
    pub enum InvalidOmicronZoneType {
        #[allow(unused)]
        ExternalIpIdRequired { kind: ZoneKind },
    }

    /// **********************************************************************
    /// DEPRECATION WARNING: Do not call this function in new code !!!
    /// **********************************************************************
    ///
    /// Convert an [`OmicronZoneConfig`] to a [`BlueprintZoneConfig`].
    ///
    /// A `BlueprintZoneConfig` is a superset of `OmicronZoneConfig` and
    /// contains auxiliary information not present in an `OmicronZoneConfig`.
    /// Therefore, the only valid direction for a real system to take is a
    /// lossy conversion from `BlueprintZoneConfig` to `OmicronZoneConfig`.
    /// This function, however, does the opposite. We therefore have to inject
    /// fake information to fill in the unknown fields in the generated
    /// `OmicronZoneConfig`.
    ///
    /// This is bad, and we should generally feel bad for doing it :). At
    /// the time this was done we were backporting the blueprint system into
    /// RSS while trying not to change too much code. This was a judicious
    /// shortcut used right before a release for stability reasons. As the
    /// number of zones managed by the reconfigurator has grown, the use
    /// of this function has become more egregious, and so it was removed
    /// from the production code path and into this test module. This move
    /// itself is a judicious shortcut. We have a test in this module,
    /// `test_blueprint_internal_dns_basic`, that is the last caller of this
    /// function, and so we have moved this function into this module.
    ///
    /// Ideally, we would get rid of this function altogether and use another
    /// method for generating `BlueprintZoneConfig` structures. Unfortunately,
    /// there are still a few remaining zones that need to be implemented in the
    /// `BlueprintBuilder`, and some of them require custom code. Until that is
    /// done, we don't have a good way of generating a test representation of
    /// the real system that would properly serve this test. We could generate
    /// a `BlueprintZoneConfig` by hand for each zone type in this test, on
    /// top of the more modern `SystemDescription` setup, but that isn't much
    /// different than what we do in this test. We'd also eventually remove it
    /// for better test setup when our `BlueprintBuilder` is capable of properly
    /// constructing all zone types. Instead, we do the simple thing, and reuse
    /// what we alreaady have.
    ///
    /// # Errors
    ///
    /// If `config.zone_type` is a zone that has an external IP address (Nexus,
    /// boundary NTP, external DNS), `external_ip_id` must be `Some(_)` or this
    /// method will return an error.
    pub fn deprecated_omicron_zone_config_to_blueprint_zone_config(
        config: OmicronZoneConfig,
        disposition: BlueprintZoneDisposition,
        external_ip_id: Option<ExternalIpUuid>,
    ) -> Result<BlueprintZoneConfig, InvalidOmicronZoneType> {
        let kind = config.zone_type.kind();
        let zone_type = match config.zone_type {
            OmicronZoneType::BoundaryNtp {
                address,
                dns_servers,
                domain,
                nic,
                ntp_servers,
                snat_cfg,
            } => {
                let external_ip_id = external_ip_id.ok_or(
                    InvalidOmicronZoneType::ExternalIpIdRequired { kind },
                )?;
                BlueprintZoneType::BoundaryNtp(
                    blueprint_zone_type::BoundaryNtp {
                        address,
                        ntp_servers,
                        dns_servers,
                        domain,
                        nic,
                        external_ip: OmicronZoneExternalSnatIp {
                            id: external_ip_id,
                            snat_cfg,
                        },
                    },
                )
            }
            OmicronZoneType::Clickhouse { address, dataset } => {
                BlueprintZoneType::Clickhouse(blueprint_zone_type::Clickhouse {
                    address,
                    dataset,
                })
            }
            OmicronZoneType::ClickhouseKeeper { address, dataset } => {
                BlueprintZoneType::ClickhouseKeeper(
                    blueprint_zone_type::ClickhouseKeeper { address, dataset },
                )
            }
            OmicronZoneType::ClickhouseServer { address, dataset } => {
                BlueprintZoneType::ClickhouseServer(
                    blueprint_zone_type::ClickhouseServer { address, dataset },
                )
            }
            OmicronZoneType::CockroachDb { address, dataset } => {
                BlueprintZoneType::CockroachDb(
                    blueprint_zone_type::CockroachDb { address, dataset },
                )
            }
            OmicronZoneType::Crucible { address, dataset } => {
                BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                    address,
                    dataset,
                })
            }
            OmicronZoneType::CruciblePantry { address } => {
                BlueprintZoneType::CruciblePantry(
                    blueprint_zone_type::CruciblePantry { address },
                )
            }
            OmicronZoneType::ExternalDns {
                dataset,
                dns_address,
                http_address,
                nic,
            } => {
                let external_ip_id = external_ip_id.ok_or(
                    InvalidOmicronZoneType::ExternalIpIdRequired { kind },
                )?;
                BlueprintZoneType::ExternalDns(
                    blueprint_zone_type::ExternalDns {
                        dataset,
                        http_address,
                        dns_address: OmicronZoneExternalFloatingAddr {
                            id: external_ip_id,
                            addr: dns_address,
                        },
                        nic,
                    },
                )
            }
            OmicronZoneType::InternalDns {
                dataset,
                dns_address,
                gz_address,
                gz_address_index,
                http_address,
            } => BlueprintZoneType::InternalDns(
                blueprint_zone_type::InternalDns {
                    dataset,
                    http_address,
                    dns_address,
                    gz_address,
                    gz_address_index,
                },
            ),
            OmicronZoneType::InternalNtp { address } => {
                BlueprintZoneType::InternalNtp(
                    blueprint_zone_type::InternalNtp { address },
                )
            }
            OmicronZoneType::Nexus {
                external_dns_servers,
                external_ip,
                external_tls,
                internal_address,
                nic,
            } => {
                let external_ip_id = external_ip_id.ok_or(
                    InvalidOmicronZoneType::ExternalIpIdRequired { kind },
                )?;
                BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                    internal_address,
                    external_ip: OmicronZoneExternalFloatingIp {
                        id: external_ip_id,
                        ip: external_ip,
                    },
                    nic,
                    external_tls,
                    external_dns_servers,
                })
            }
            OmicronZoneType::Oximeter { address } => {
                BlueprintZoneType::Oximeter(blueprint_zone_type::Oximeter {
                    address,
                })
            }
        };
        let image_source = match config.image_source {
            OmicronZoneImageSource::InstallDataset => {
                BlueprintZoneImageSource::InstallDataset
            }
            OmicronZoneImageSource::Artifact { .. } => {
                // BlueprintZoneImageSource::Artifact has both a version and a
                // hash in it, while OmicronZoneImageSource::Artifact only has a
                // hash field. Rather than conjuring up a fake version, we
                // simply panic here.
                unreachable!(
                    "this test does not use OmicronZoneImageSource::Artifact"
                )
            }
        };
        Ok(BlueprintZoneConfig {
            disposition,
            id: config.id,
            // This is *VERY* incorrect, in terms of a real system, but is not
            // harmful to our DNS tests below. We should not introduce any new
            // callers of
            // `deprecated_omicron_zone_config_to_blueprint_zone_config`, so
            // hopefully this doesn't cause us too much pain in the future.
            filesystem_pool: config.filesystem_pool.unwrap_or_else(|| {
                ZpoolName::new_external(ZpoolUuid::new_v4())
            }),
            zone_type,
            image_source,
        })
    }

    /// test blueprint_internal_dns_config(): trivial case of an empty blueprint
    #[test]
    fn test_blueprint_internal_dns_empty() {
        let blueprint = BlueprintBuilder::build_empty_with_sleds(
            std::iter::empty(),
            "test-suite",
        );
        let blueprint_dns = blueprint_internal_dns_config(
            &blueprint,
            &BTreeMap::new(),
            &Default::default(),
        )
        .unwrap();
        assert!(blueprint_dns.records.is_empty());
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

        let mut blueprint_sleds = BTreeMap::new();

        for (sled_id, sa) in collection.sled_agents {
            let ledgered_sled_config =
                sa.ledgered_sled_config.unwrap_or_default();

            // Convert the inventory `OmicronZonesConfig`s into
            // `BlueprintZoneConfig`s. This is going to get more painful over
            // time as we add to blueprints, but for now we can make this work.
            let zones = ledgered_sled_config
                .zones
                .into_iter()
                .map(|config| -> BlueprintZoneConfig {
                    deprecated_omicron_zone_config_to_blueprint_zone_config(
                        config,
                        BlueprintZoneDisposition::InService,
                        // We don't get external IP IDs in inventory
                        // collections. We'll just make one up for every
                        // zone that needs one here. This is gross.
                        Some(ExternalIpUuid::new_v4()),
                    )
                    .expect("failed to convert zone config")
                })
                .collect();
            blueprint_sleds.insert(
                sled_id,
                BlueprintSledConfig {
                    state: SledState::Active,
                    sled_agent_generation: ledgered_sled_config.generation,
                    disks: IdMap::new(),
                    datasets: IdMap::new(),
                    zones,
                    remove_mupdate_override: None,
                },
            );
        }

        let dns_empty = dns_config_empty();
        let initial_dns_generation = dns_empty.generation;
        let mut blueprint = Blueprint {
            id: BlueprintUuid::new_v4(),
            sleds: blueprint_sleds,
            pending_mgs_updates: PendingMgsUpdates::new(),
            cockroachdb_setting_preserve_downgrade:
                CockroachDbPreserveDowngrade::DoNotModify,
            parent_blueprint_id: None,
            internal_dns_version: initial_dns_generation,
            external_dns_version: Generation::new(),
            target_release_minimum_generation: None,
            cockroachdb_fingerprint: String::new(),
            clickhouse_cluster_config: None,
            oximeter_read_version: Generation::new(),
            oximeter_read_mode: OximeterReadMode::SingleNode,
            time_created: now_db_precision(),
            creator: "test-suite".to_string(),
            comment: "test blueprint".to_string(),
        };

        // To make things slightly more interesting, let's add a zone that's
        // not currently in service.
        let out_of_service_id = OmicronZoneUuid::new_v4();
        let out_of_service_addr = Ipv6Addr::LOCALHOST;
        blueprint.sleds.values_mut().next().unwrap().zones.insert(
            BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::Expunged {
                    as_of_generation: Generation::new(),
                    ready_for_cleanup: false,
                },
                id: out_of_service_id,
                filesystem_pool: ZpoolName::new_external(ZpoolUuid::new_v4()),
                zone_type: BlueprintZoneType::Oximeter(
                    blueprint_zone_type::Oximeter {
                        address: SocketAddrV6::new(
                            out_of_service_addr,
                            12345,
                            0,
                            0,
                        ),
                    },
                ),
                image_source: BlueprintZoneImageSource::InstallDataset,
            },
        );

        // To generate the blueprint's DNS config, we need to make up a
        // different set of information about the Quiesced fake system.
        let sleds_by_id = blueprint
            .sleds
            .keys()
            .zip(possible_sled_subnets)
            .enumerate()
            .map(|(i, (sled_id, subnet))| {
                let sled_info = Sled::new(
                    *sled_id,
                    SledPolicy::InService {
                        provision_policy: SledProvisionPolicy::Provisionable,
                    },
                    get_sled_address(Ipv6Subnet::new(subnet.network())),
                    REPO_DEPOT_PORT,
                    // The first two of these (arbitrarily) will be marked
                    // Scrimlets.
                    if i < 2 { SledRole::Scrimlet } else { SledRole::Gimlet },
                );
                (*sled_id, sled_info)
            })
            .collect();

        let mut blueprint_dns_zone = blueprint_internal_dns_config(
            &blueprint,
            &sleds_by_id,
            &Default::default(),
        )
        .unwrap();
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
        //    be one of the ones with a AAAA record pointing to either an
        //    Omicron zone or a global zone.
        //
        // 3. There is at least one SRV record for each service that we expect
        //    to appear in the representative system that we're working with.
        //
        // 4. Our out-of-service zone does *not* appear in the DNS config,
        //    neither with an AAAA record nor in an SRV record.
        //
        // 5. The boundary NTP zones' IP addresses are mapped to AAAA records in
        //    the special boundary DNS name (in addition to having their normal
        //    zone DNS name -> AAAA record from 1).
        //
        // Together, this tells us that we have SRV records for all services,
        // that those SRV records all point to at least one of the Omicron zones
        // for that service, and that we correctly ignored zones that were not
        // in service.

        // To start, we need a mapping from underlay IP to the corresponding
        // Omicron zone.
        let mut omicron_zones_by_ip: BTreeMap<_, _> = blueprint
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .map(|(_, zone)| (zone.underlay_ip(), zone.id))
            .collect();
        println!("omicron zones by IP: {:#?}", omicron_zones_by_ip);

        // Check to see that the out-of-service zone was actually excluded.
        assert!(
            omicron_zones_by_ip.values().all(|id| *id != out_of_service_id)
        );

        // We also want a mapping from underlay IP to the corresponding switch
        // zone.  In this case, the value is the Scrimlet's sled id.
        let mut switch_sleds_by_ip: BTreeMap<_, _> = sleds_by_id
            .iter()
            .filter_map(|(sled_id, sled)| {
                if sled.is_scrimlet() {
                    let sled_subnet =
                        sleds_by_id.get(sled_id).unwrap().subnet();
                    let switch_zone_ip = get_switch_zone_address(sled_subnet);
                    Some((switch_zone_ip, *sled_id))
                } else {
                    None
                }
            })
            .collect();

        // We also want a mapping from underlay IP to each sled global zone.
        // In this case, the value is the sled id.
        let mut all_sleds_by_ip: BTreeMap<_, _> = sleds_by_id
            .keys()
            .map(|sled_id| {
                let sled_subnet = sleds_by_id.get(sled_id).unwrap().subnet();
                let global_zone_ip = *get_sled_address(sled_subnet).ip();
                (global_zone_ip, *sled_id)
            })
            .collect();

        // Prune the special boundary NTP DNS name out, collecting their IP
        // addresses, and build a list of expected SRV targets to ensure these
        // IPs show up both in the special boundary NTP DNS name and as their
        // normal SRV records.
        let boundary_ntp_ips = blueprint_dns_zone
            .records
            .remove(BOUNDARY_NTP_DNS_NAME)
            .expect("missing boundary NTP DNS name")
            .into_iter()
            .map(|record| match record {
                DnsRecord::Aaaa(ip) => ip,
                _ => panic!("expected AAAA record; got {record:?}"),
            });
        let mut expected_boundary_ntp_srv_targets = boundary_ntp_ips
            .map(|ip| {
                let Some(zone_id) = omicron_zones_by_ip.get(&ip) else {
                    panic!("did not find zone ID for boundary NTP IP {ip}");
                };
                let name = Host::Zone(Zone::Other(*zone_id)).fqdn();
                println!(
                    "Boundary NTP IP {ip} maps to expected \
                     SRV record target {name}"
                );
                name
            })
            .collect::<BTreeSet<_>>();

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

                if let Some(sled_id) = all_sleds_by_ip.remove(addr) {
                    println!(
                        "IP {} found in DNS corresponds with global zone \
                        for sled {}",
                        addr, sled_id
                    );
                    expected_srv_targets.insert(format!(
                        "{}.{}",
                        name, blueprint_dns_zone.zone_name
                    ));
                    continue;
                }

                println!(
                    "note: found IP ({}) not corresponding to any \
                    Omicron zone, switch zone, or global zone \
                    (name {:?})",
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
            ServiceName::ClickhouseNative,
            ServiceName::Cockroach,
            ServiceName::InternalDns,
            ServiceName::ExternalDns,
            ServiceName::Nexus,
            ServiceName::Oximeter,
            ServiceName::Dendrite,
            ServiceName::CruciblePantry,
            ServiceName::BoundaryNtp,
            ServiceName::InternalNtp,
            ServiceName::RepoDepot,
        ]);

        for (name, records) in &blueprint_dns_zone.records {
            let mut this_kind = None;
            let kinds_left: Vec<_> =
                srv_kinds_expected.iter().copied().collect();
            for kind in kinds_left {
                if kind.dns_name() == *name {
                    srv_kinds_expected.remove(&kind);
                    this_kind = Some(kind);
                }
            }

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
                if this_kind == Some(ServiceName::BoundaryNtp) {
                    assert!(
                        expected_boundary_ntp_srv_targets.contains(&srv.target),
                        "found boundary NTP SRV record with target {:?} \
                        that does not correspond to an expected boundary \
                        NTP zone",
                        srv.target,
                    );
                    expected_boundary_ntp_srv_targets.remove(&srv.target);
                }
            }
        }

        println!("SRV kinds with no records found: {:?}", srv_kinds_expected);
        assert!(srv_kinds_expected.is_empty());

        println!(
            "Boundary NTP SRV targets not found: {:?}",
            expected_boundary_ntp_srv_targets
        );
        assert!(expected_boundary_ntp_srv_targets.is_empty());
    }

    #[tokio::test]
    async fn test_blueprint_external_dns_basic() {
        static TEST_NAME: &str = "test_blueprint_external_dns_basic";
        let logctx = test_setup_log(TEST_NAME);
        let (_, mut blueprint) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME).nsleds(5).build();
        blueprint.internal_dns_version = Generation::new();
        blueprint.external_dns_version = Generation::new();

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

        // It shouldn't ever be possible to have no Silos at all, but at least
        // make sure we don't panic.
        let external_dns_zone = blueprint_external_dns_config(
            &blueprint,
            &[],
            String::from("oxide.test"),
        );
        assert_eq!(external_dns_zone.zone_name, "oxide.test");
        assert!(external_dns_zone.records.is_empty());

        // Now check a more typical case.
        let external_dns_zone = blueprint_external_dns_config(
            &blueprint,
            std::slice::from_ref(my_silo.name()),
            String::from("oxide.test"),
        );
        assert_eq!(external_dns_zone.zone_name, String::from("oxide.test"));
        let records = &external_dns_zone.records;
        assert_eq!(records.len(), 1);
        let silo_records = records
            .get(&silo_dns_name(my_silo.name()))
            .expect("missing silo DNS records");

        // Helper for converting dns records for a given silo to IpAddrs
        let records_to_ips = |silo_records: &Vec<_>| {
            let mut ips: Vec<_> = silo_records
                .into_iter()
                .map(|record| match record {
                    DnsRecord::A(v) => IpAddr::V4(*v),
                    DnsRecord::Aaaa(v) => IpAddr::V6(*v),
                    DnsRecord::Srv(_) => panic!("unexpected SRV record"),
                })
                .collect();
            ips.sort();
            ips
        };

        // Here we're hardcoding the contents of the example blueprint.  It
        // currently puts one Nexus zone on each sled.  If we change the example
        // blueprint, change the expected set of IPs here.
        let silo_record_ips: Vec<_> = records_to_ips(silo_records);
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

        // Change the zone disposition to expunged for the nexus zone on the
        // first sled. This should ensure we don't get an external DNS record
        // back for that sled.
        let bp_sled_config = &mut blueprint.sleds.values_mut().next().unwrap();
        let mut nexus_zone = bp_sled_config
            .zones
            .iter_mut()
            .find(|z| z.zone_type.is_nexus())
            .unwrap();
        nexus_zone.disposition = BlueprintZoneDisposition::Expunged {
            as_of_generation: Generation::new(),
            ready_for_cleanup: false,
        };
        mem::drop(nexus_zone);

        // Retrieve the DNS config based on the modified blueprint
        let external_dns_zone = blueprint_external_dns_config(
            &blueprint,
            std::slice::from_ref(my_silo.name()),
            String::from("oxide.test"),
        );
        let silo_records = &external_dns_zone
            .records
            .get(&silo_dns_name(my_silo.name()))
            .expect("missing silo DNS records");
        let silo_record_ips: Vec<_> = records_to_ips(silo_records);

        // We shouldn't see the excluded Nexus address
        assert_eq!(
            silo_record_ips,
            &[
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
        let dns_empty = &dns_config_empty().zones[0];
        match dns_compute_update(
            &logctx.log,
            DnsGroup::Internal,
            "test-suite".to_string(),
            "test-suite".to_string(),
            dns_empty,
            dns_empty,
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
        let dns_zone1 = DnsConfigZone {
            zone_name: "my-zone".to_string(),
            records: HashMap::from([
                ("ex1".to_string(), vec![DnsRecord::A(Ipv4Addr::LOCALHOST)]),
                (
                    "ex2".to_string(),
                    vec![DnsRecord::A("192.168.1.3".parse().unwrap())],
                ),
            ]),
        };

        let dns_zone2 = DnsConfigZone {
            zone_name: "my-zone".to_string(),
            records: HashMap::from([
                (
                    "ex2".to_string(),
                    vec![DnsRecord::A("192.168.1.4".parse().unwrap())],
                ),
                ("ex3".to_string(), vec![DnsRecord::A(Ipv4Addr::LOCALHOST)]),
            ]),
        };

        let update = dns_compute_update(
            &logctx.log,
            DnsGroup::Internal,
            "test-suite".to_string(),
            "test-suite".to_string(),
            &dns_zone1,
            &dns_zone2,
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
        let mut dns_zone1 = dns_zone1.clone();
        dns_zone1.records.insert(
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
        let mut dns_zone2 = dns_zone1.clone();
        let update = dns_compute_update(
            &logctx.log,
            DnsGroup::Internal,
            "test-suite".to_string(),
            "test-suite".to_string(),
            &dns_zone1,
            &dns_zone2,
        )
        .expect("failed to compute update");
        assert!(update.is_none());

        // If we shift the order of the items, it should still reflect no
        // changes.
        let records = dns_zone2.records.get_mut("_nexus._tcp").unwrap();
        records.rotate_left(1);
        assert!(records != dns_zone1.records.get("_nexus._tcp").unwrap());
        let update = dns_compute_update(
            &logctx.log,
            DnsGroup::Internal,
            "test-suite".to_string(),
            "test-suite".to_string(),
            &dns_zone1,
            &dns_zone2,
        )
        .expect("failed to compute update");
        assert!(update.is_none());

        // If we add another record, there should indeed be a new update.
        let records = dns_zone2.records.get_mut("_nexus._tcp").unwrap();
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
            &dns_zone1,
            &dns_zone2,
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

    fn diff_sole_zones<'a>(
        left: &'a DnsConfigParams,
        right: &'a DnsConfigParams,
    ) -> DnsDiff<'a> {
        let left_zone = left.sole_zone().unwrap();
        let right_zone = right.sole_zone().unwrap();
        DnsDiff::new(left_zone, right_zone).unwrap()
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
    #[nexus_test(extra_sled_agents = 1)]
    async fn test_silos_external_dns_end_to_end(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let resolver = nexus.resolver();
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
        let (_blueprint_target, mut blueprint) = datastore
            .blueprint_target_get_current_full(&opctx)
            .await
            .expect("failed to read current target blueprint");
        eprintln!("blueprint: {}", blueprint.display());
        // Override the CockroachDB settings so that we don't try to set them.
        blueprint.cockroachdb_setting_preserve_downgrade =
            CockroachDbPreserveDowngrade::DoNotModify;

        // Record the zpools so we don't fail to ensure datasets (unrelated to
        // DNS) during blueprint execution.
        let mut disk_test = DiskTest::new(&cptestctx).await;
        disk_test.add_blueprint_disks(&blueprint).await;

        // Now, execute the initial blueprint.
        let overrides = overridables_for_test(cptestctx);
        _ = realize_blueprint_and_expect(
            &opctx, datastore, resolver, &blueprint, &overrides,
        )
        .await;

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
            resolver,
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
        let sled_rows = datastore
            .sled_list_all_batched(&opctx, SledFilter::Commissioned)
            .await
            .unwrap();
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
        let planning_input = {
            let mut builder = PlanningInputFromDb {
                sled_rows: &sled_rows,
                zpool_rows: &zpool_rows,
                ip_pool_range_rows: &ip_pool_range_rows,
                internal_dns_version: dns_initial_internal.generation.into(),
                external_dns_version: dns_latest_external.generation.into(),
                // These are not used because we're not actually going through
                // the planner.
                cockroachdb_settings: &CockroachDbSettings::empty(),
                external_ip_rows: &[],
                service_nic_rows: &[],
                target_boundary_ntp_zone_count: BOUNDARY_NTP_REDUNDANCY,
                target_nexus_zone_count: NEXUS_REDUNDANCY,
                target_internal_dns_zone_count: INTERNAL_DNS_REDUNDANCY,
                target_oximeter_zone_count: OXIMETER_REDUNDANCY,
                target_cockroachdb_zone_count: COCKROACHDB_REDUNDANCY,
                target_cockroachdb_cluster_version:
                    CockroachDbClusterVersion::POLICY,
                target_crucible_pantry_zone_count: CRUCIBLE_PANTRY_REDUNDANCY,
                clickhouse_policy: None,
                oximeter_read_policy: OximeterReadPolicy::new(1),
                log,
            }
            .build()
            .unwrap()
            .into_builder();

            // We'll need another (fake) external IP for this new Nexus.
            builder
                .policy_mut()
                .service_ip_pool_ranges
                .push(IpRange::from(IpAddr::V4(Ipv4Addr::LOCALHOST)));

            builder.build()
        };
        let collection = CollectionBuilder::new("test").build();
        let mut builder = BlueprintBuilder::new_based_on(
            &log,
            &blueprint,
            &planning_input,
            &collection,
            "test suite",
        )
        .unwrap();
        let sled_id =
            blueprint.sleds().next().expect("expected at least one sled");
        builder.sled_add_zone_nexus(sled_id).unwrap();
        let blueprint2 = builder.build();
        eprintln!("blueprint2: {}", blueprint2.display());
        // Figure out the id of the new zone.
        let zones_before = blueprint
            .all_omicron_zones(BlueprintZoneDisposition::any)
            .filter_map(|(_, z)| z.zone_type.is_nexus().then_some(z.id))
            .collect::<BTreeSet<_>>();
        let zones_after = blueprint2
            .all_omicron_zones(BlueprintZoneDisposition::any)
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

        _ = realize_blueprint_and_expect(
            &opctx,
            datastore,
            resolver,
            &blueprint2,
            &overrides,
        )
        .await;

        // Now fetch DNS again.  Both should have changed this time.
        let dns_latest_internal = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .expect("fetching latest internal DNS");

        assert_eq!(
            dns_latest_internal.generation,
            dns_initial_internal.generation.next(),
        );

        let diff = diff_sole_zones(&dns_initial_internal, &dns_latest_internal);
        // There should be one new AAAA record for the zone itself.
        let new_records: Vec<_> = diff.names_added().collect();
        let (new_name, &[DnsRecord::Aaaa(_)]) = new_records[0] else {
            panic!("did not find expected AAAA record for new Nexus zone");
        };
        let new_zone_host = internal_dns_types::config::Host::for_zone(
            internal_dns_types::config::Zone::Other(new_zone_id),
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
            dns_previous_external.generation.next(),
        );
        let diff =
            diff_sole_zones(&dns_previous_external, &dns_latest_external);
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
        _ = realize_blueprint_and_expect(
            &opctx,
            datastore,
            resolver,
            &blueprint2,
            &overrides,
        )
        .await;
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
            resolver,
            &blueprint2,
            &overrides,
            "tickety-boo",
            &dns_latest_internal,
            &dns_latest_external,
        )
        .await;

        // One more time, make sure that executing the blueprint does not do
        // anything.
        _ = realize_blueprint_and_expect(
            &opctx,
            datastore,
            resolver,
            &blueprint2,
            &overrides,
        )
        .await;
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
        resolver: &Resolver,
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
        assert_eq!(
            old_external.generation.next(),
            dns_latest_external.generation
        );

        // Specifically, there should be one new name (for the new Silo).
        let diff = diff_sole_zones(&old_external, &dns_latest_external);
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
        _ = realize_blueprint_and_expect(
            &opctx, datastore, resolver, &blueprint, &overrides,
        )
        .await;
        let dns_latest_internal = datastore
            .dns_config_read(&opctx, DnsGroup::Internal)
            .await
            .expect("fetching latest internal DNS");
        let dns_latest_external = datastore
            .dns_config_read(&opctx, DnsGroup::External)
            .await
            .expect("fetching latest external DNS");
        assert_eq!(old_internal.generation, dns_latest_internal.generation);
        assert_eq!(
            old_external.generation.next(),
            dns_latest_external.generation
        );

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
