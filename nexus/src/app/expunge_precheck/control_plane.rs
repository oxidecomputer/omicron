// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Checks whether the control plane has finished recovering from previous
//! expungements
//!
//! When a sled or physical disk is expunged, the blueprint planner expunges the
//! control plane zones on it and deploys replacements elsewhere.  The system
//! does not have its usual redundancy again until those replacements are
//! running and their services are healthy.  This check passes only if all of
//! these are true:
//!
//! 1. The planner has accounted for every expungement.  The most recent
//!    blueprint produced by this Nexus's planner has no in-service zones on
//!    expunged sleds and no in-service disks that are expunged.  If this is not
//!    true, the other checks would be evaluating a stale plan, so we skip them.
// <review covers="4 lines above">
// XXX-dap TODO how do we know that we're not looking at a blueprint so old that it never knew about these disks/sleds?  Right now the blueprint will show them as expunged but eventually we want to prune expunged things.
// </review>
//! 2. The planner's report for that blueprint shows that it is not still
//!    placing zones.  The planner ran its step for adding zones, placed none,
//!    and found nothing missing.  Being in the middle of an upgrade or waiting
//!    for MUPdate resolution does not by itself fail this check.
//! 3. Every in-service zone in that blueprint appears in the latest inventory
//!    collection without a reconciliation error.
//! 4. Using that same inventory collection, the planner's zone safety checks
//!    pass.  These check that CockroachDB, boundary NTP, and internal DNS have
//!    the redundancy that they need.
//!
//! The planner's report reflects what the planner saw when it last ran, and
//! the latest inventory collection may predate an expungement.  These checks
//! still do not report success too early.  (1) means that the planner ran after
//! every expungement.  Any replacement zones that it deployed keep (2) or (3)
//! failing until they appear in inventory.  So by the time those pass, the
//! inventory collection used for (4) was collected after the replacements were
//! deployed.  It's possible that services became unhealthy again after that
//! (not due to an expungement), but we're just looking to verify that
//! expungement repairs are complete, not that everything is healthy.

use nexus_db_model::DnsGroup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::model::PhysicalDiskPolicy;
use nexus_reconfigurator_planning::planner::ZonePropagationStatus;
use nexus_reconfigurator_planning::planner::ZoneSafetyChecks;
use nexus_reconfigurator_planning::planner::ZoneSafetyTargets;
use nexus_reconfigurator_planning::planner::zones_not_yet_propagated_to_inventory;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::DiskFilter;
use nexus_types::deployment::PlanningAddStepReport;
use nexus_types::deployment::PlanningReport;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::ZoneUnsafeToShutdown;
use nexus_types::deployment::planning_report::AddUpdateBlockedReason;
use nexus_types::deployment::planning_report::PlanningAddOutOfEligibleSleds;
use nexus_types::deployment::planning_report::ZoneAddWaitingOn;
use nexus_types::external_api::expunge_precheck::ControlPlanePrecheck;
use nexus_types::external_api::sled::SledPolicy;
use nexus_types::identity::Asset;
use nexus_types::internal_api::background::BlueprintPlannerLatest;
use nexus_types::internal_api::background::PlannerFailure;
use nexus_types::inventory::Collection;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::policy::BOUNDARY_NTP_REDUNDANCY;
use omicron_common::policy::COCKROACHDB_REDUNDANCY;
use omicron_common::policy::INTERNAL_DNS_REDUNDANCY;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use sled_agent_types::inventory::ZoneKind;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

/// Checks whether the control plane has finished recovering from previous
/// expungements
///
/// `planner_latest` is the outcome of this Nexus's most recent planner
/// activation.  `target_blueprint` and `inventory` are the target blueprint and
/// inventory collection that this Nexus has most recently loaded.
pub(super) async fn check(
    opctx: &OpContext,
    datastore: &DataStore,
    planner_latest: BlueprintPlannerLatest,
    target_blueprint: Option<Arc<Blueprint>>,
    inventory: Option<Arc<Collection>>,
) -> Result<ControlPlanePrecheck, Error> {
    let input = ControlPlaneInput::load(
        opctx,
        datastore,
        planner_latest,
        target_blueprint,
        inventory,
    )
    .await?;
    Ok(ControlPlanePrecheck {
        risks: input.risks().iter().map(|r| r.to_string()).collect(),
    })
}

/// Everything that the control plane check looks at
///
/// This is separate from [`check()`] so that the logic can be tested without a
/// database.
struct ControlPlaneInput {
    /// what this Nexus's planner most recently produced
    plan: LatestPlan,
    /// sleds and disks that are expunged in the database
    expunged: ExpungedHardware,
    /// latest inventory collection, if any
    inventory: Option<Arc<Collection>>,
    /// targets used for the zone safety checks
    targets: ZoneSafetyTargets,
}

/// What this Nexus's planner most recently produced
enum LatestPlan {
    /// the planner has not run since Nexus started
    NotYetRun,
    /// the planner's most recent activation failed
    Failed(PlannerFailure),
    /// the planner's most recent blueprint no longer exists
    BlueprintMissing(BlueprintUuid),
    /// the planner produced this blueprint and report
    Planned { blueprint: Arc<Blueprint>, report: Arc<PlanningReport> },
}

impl ControlPlaneInput {
    /// Loads the inputs to the control plane check
    async fn load(
        opctx: &OpContext,
        datastore: &DataStore,
        planner_latest: BlueprintPlannerLatest,
        target_blueprint: Option<Arc<Blueprint>>,
        inventory: Option<Arc<Collection>>,
    ) -> Result<ControlPlaneInput, Error> {
        let plan = match planner_latest {
            BlueprintPlannerLatest::NotYetRun { time_created: _ } => {
                LatestPlan::NotYetRun
            }
            BlueprintPlannerLatest::Failed { time_planned: _, reason } => {
                LatestPlan::Failed(reason)
            }
            BlueprintPlannerLatest::Succeeded {
                time_planned: _,
                blueprint_id,
                report,
            } => {
                match planned_blueprint(
                    opctx,
                    datastore,
                    blueprint_id,
                    target_blueprint,
                )
                .await?
                {
                    Some(blueprint) => {
                        LatestPlan::Planned { blueprint, report }
                    }
                    None => LatestPlan::BlueprintMissing(blueprint_id),
                }
            }
        };

        let expunged = ExpungedHardware::load(opctx, datastore).await?;

        // These targets must match the ones that the planner uses.  See
        // `PlanningInputFromDb::assemble()`.
        let internal_dns_version = datastore
            .dns_group_latest_version(opctx, DnsGroup::Internal)
            .await?
            .version
            .into();
        let targets = ZoneSafetyTargets {
            target_boundary_ntp_zone_count: BOUNDARY_NTP_REDUNDANCY,
            target_internal_dns_zone_count: INTERNAL_DNS_REDUNDANCY,
            target_cockroachdb_zone_count: COCKROACHDB_REDUNDANCY,
            internal_dns_version,
        };

        Ok(ControlPlaneInput { plan, expunged, inventory, targets })
    }

    /// Returns the risks found by the control plane check
    ///
    /// See the module-level documentation for details.
    fn risks(&self) -> Vec<ControlPlaneRisk> {
        let (blueprint, report) = match &self.plan {
            LatestPlan::NotYetRun => {
                return vec![ControlPlaneRisk::PlannerNotYetRun];
            }
            LatestPlan::Failed(reason) => {
                return vec![ControlPlaneRisk::PlannerFailed(reason.clone())];
            }
            LatestPlan::BlueprintMissing(blueprint_id) => {
                return vec![ControlPlaneRisk::PlannedBlueprintMissing {
                    blueprint_id: *blueprint_id,
                }];
            }
            LatestPlan::Planned { blueprint, report } => (blueprint, report),
        };

        // (1) Check that the planner has accounted for every expungement.  If
        // it hasn't, nothing else that it told us is relevant.
        let unaccounted = unaccounted_expungements(blueprint, &self.expunged);
        if !unaccounted.is_empty() {
            return unaccounted;
        }

        // (2) Check whether the planner is still placing zones.
        let mut risks = zone_add_risks(&report.add);

        // (3) Check that the planned zones have been deployed.
        let Some(inventory) = &self.inventory else {
            risks.push(ControlPlaneRisk::InventoryMissing);
            return risks;
        };
        risks.extend(zone_propagation_risks(blueprint, inventory));

        // (4) Check that services have the redundancy that they need.
        risks.extend(zone_safety_risks(blueprint, inventory, &self.targets));

        risks
    }
}

/// Returns the blueprint with the given id, or `None` if it no longer exists
///
/// This is normally the current target blueprint, which Nexus has already
/// loaded.
async fn planned_blueprint(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint_id: BlueprintUuid,
    target_blueprint: Option<Arc<Blueprint>>,
) -> Result<Option<Arc<Blueprint>>, Error> {
    if let Some(target) = target_blueprint {
        if target.id == blueprint_id {
            return Ok(Some(target));
        }
    }

    let authz_blueprint = authz::Blueprint::new(
        authz::FLEET,
        blueprint_id.into_untyped_uuid(),
        LookupType::ById(blueprint_id.into_untyped_uuid()),
    );
    match datastore.blueprint_read(opctx, &authz_blueprint).await {
        Ok(blueprint) => Ok(Some(Arc::new(blueprint))),
        Err(Error::ObjectNotFound { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Sleds and physical disks whose policy in the database is `expunged`
struct ExpungedHardware {
    sleds: BTreeSet<SledUuid>,
    disks: BTreeSet<PhysicalDiskUuid>,
}

impl ExpungedHardware {
    async fn load(
        opctx: &OpContext,
        datastore: &DataStore,
    ) -> Result<ExpungedHardware, Error> {
        let sleds = datastore
            .sled_list_all_batched(opctx, SledFilter::All)
            .await?
            .into_iter()
            .filter(|sled| match sled.policy() {
                SledPolicy::Expunged => true,
                SledPolicy::InService { .. } => false,
            })
            .map(|sled| sled.id())
            .collect();
        let disks = datastore
            .physical_disk_list_all_batched(opctx, DiskFilter::All)
            .await?
            .into_iter()
            .filter(|disk| match disk.disk_policy {
                PhysicalDiskPolicy::Expunged => true,
                PhysicalDiskPolicy::InService => false,
            })
            .map(|disk| disk.id())
            .collect();
        Ok(ExpungedHardware { sleds, disks })
    }
}

/// Returns risks for expunged sleds and disks that the blueprint still treats
/// as in service
fn unaccounted_expungements(
    blueprint: &Blueprint,
    expunged: &ExpungedHardware,
) -> Vec<ControlPlaneRisk> {
    let mut risks = Vec::new();
    for (sled_id, sled_config) in &blueprint.sleds {
        // When the planner sees an expunged sled, it expunges all of the
        // sled's zones.
        if expunged.sleds.contains(sled_id)
            && sled_config.zones.iter().any(|z| z.disposition.is_in_service())
        {
            risks.push(ControlPlaneRisk::ExpungedSledNotAccountedFor {
                sled_id: *sled_id,
            });
        }

        // Likewise, when it sees an expunged disk, it expunges the disk (and
        // the zones that use it).
        for disk in &sled_config.disks {
            if disk.disposition.is_in_service()
                && expunged.disks.contains(&disk.id)
            {
                risks.push(ControlPlaneRisk::ExpungedDiskNotAccountedFor {
                    sled_id: *sled_id,
                    disk_id: disk.id,
                });
            }
        }
    }
    risks
}

/// Returns risks indicating that the planner is still placing zones
fn zone_add_risks(add: &PlanningAddStepReport) -> Vec<ControlPlaneRisk> {
    // Destructure the report so that adding a field causes a compile error
    // here.  Whoever adds it should decide whether it indicates that the
    // planner is still placing zones.
    let PlanningAddStepReport {
        waiting_on,
        // This can be non-empty even when the planner added zones (in target
        // release generation 1).  `waiting_on` reflects whether the planner
        // actually skipped adding zones.
        add_update_blocked_reasons,
        target_release_generation_is_one: _,
        sleds_without_ntp_zones_in_inventory,
        sleds_without_zpools_for_ntp_zones,
        sleds_waiting_for_ntp_zone,
        sleds_getting_ntp_and_discretionary_zones,
        sleds_missing_ntp_zone,
        sleds_missing_crucible_zone,
        out_of_eligible_sleds,
        // This only describes zone kinds that already have enough zones.
        sufficient_zones_exist: _,
        discretionary_zones_placed,
    } = add;

    let mut risks = Vec::new();
    match waiting_on {
        None => (),
        Some(ZoneAddWaitingOn::Blockers) => {
            risks.push(ControlPlaneRisk::ZoneAddsBlocked(
                add_update_blocked_reasons.clone(),
            ));
        }
    }

    for (kind, sleds) in [
        (NtpWait::NotInInventory, sleds_without_ntp_zones_in_inventory.clone()),
        (NtpWait::NoZpools, sleds_without_zpools_for_ntp_zones.clone()),
        (
            NtpWait::DiscretionaryZonesWaiting,
            sleds_waiting_for_ntp_zone.clone(),
        ),
        (
            NtpWait::GettingNtpAndDiscretionaryZones,
            sleds_getting_ntp_and_discretionary_zones.clone(),
        ),
        (NtpWait::Missing, sleds_missing_ntp_zone.clone()),
    ] {
        if !sleds.is_empty() {
            risks.push(ControlPlaneRisk::NtpZonesPending { kind, sleds });
        }
    }

    if !sleds_missing_crucible_zone.is_empty() {
        risks.push(ControlPlaneRisk::CrucibleZonesMissing {
            sleds: sleds_missing_crucible_zone.keys().copied().collect(),
        });
    }

    for (
        zone_kind,
        PlanningAddOutOfEligibleSleds { placed, wanted_to_place },
    ) in out_of_eligible_sleds
    {
        risks.push(ControlPlaneRisk::OutOfEligibleSleds {
            zone_kind: zone_kind.clone(),
            placed: *placed,
            wanted_to_place: *wanted_to_place,
        });
    }

    let nplaced: usize =
        discretionary_zones_placed.values().map(Vec::len).sum();
    if nplaced > 0 {
        risks.push(ControlPlaneRisk::DiscretionaryZonesPlaced {
            count: nplaced,
        });
    }

    risks
}

/// Returns risks for in-service zones in the blueprint that have not been
/// deployed successfully, according to inventory
fn zone_propagation_risks(
    blueprint: &Blueprint,
    inventory: &Collection,
) -> Vec<ControlPlaneRisk> {
    let zones = blueprint.in_service_zones().map(|(_sled_id, zone)| zone);
    zones_not_yet_propagated_to_inventory(zones, inventory)
        .into_iter()
        .filter_map(|zone| match zone.reason {
            // The zone is still running its previous image.  This is expected
            // during an upgrade and does not reduce redundancy.
            ZonePropagationStatus::ImageSourceMismatch { .. } => None,
            ZonePropagationStatus::MissingInInventory { .. } => {
                Some(ControlPlaneRisk::ZoneNotInInventory {
                    zone_id: zone.zone_id,
                    zone_kind: zone.zone_kind,
                })
            }
            ZonePropagationStatus::ReconciliationError { message, .. } => {
                Some(ControlPlaneRisk::ZoneReconciliationFailed {
                    zone_id: zone.zone_id,
                    zone_kind: zone.zone_kind,
                    message: message.to_owned(),
                })
            }
        })
        .collect()
}

/// Returns risks for services that lack the redundancy that they need,
/// according to the planner's zone safety checks
fn zone_safety_risks(
    blueprint: &Blueprint,
    inventory: &Collection,
    targets: &ZoneSafetyTargets,
) -> Vec<ControlPlaneRisk> {
    let checks = ZoneSafetyChecks::from_zones(
        blueprint.in_service_zones(),
        inventory,
        targets,
    );

    // The checks report the same reason for every zone of a given kind, so we
    // report each kind once.
    let by_kind: BTreeMap<ZoneKind, &ZoneUnsafeToShutdown> = checks
        .unsafe_zones()
        .map(|(_sled_id, _zone_id, reason)| (reason.zone_kind(), reason))
        .collect();
    by_kind
        .into_values()
        .map(|reason| ControlPlaneRisk::ServiceLacksRedundancy(reason.clone()))
        .collect()
}

/// A risk identified by the control plane check
///
/// The external API reports only the `Display` form of these.
#[derive(Debug, PartialEq, Eq)]
enum ControlPlaneRisk {
    /// this Nexus's planner has not run since Nexus started
    PlannerNotYetRun,
    /// this Nexus's most recent planner activation failed
    PlannerFailed(PlannerFailure),
    /// the blueprint from this Nexus's most recent planner activation no
    /// longer exists
    PlannedBlueprintMissing { blueprint_id: BlueprintUuid },
    /// an expunged sled still has in-service zones in the planned blueprint
    ExpungedSledNotAccountedFor { sled_id: SledUuid },
    /// an expunged disk is still in service in the planned blueprint
    ExpungedDiskNotAccountedFor { sled_id: SledUuid, disk_id: PhysicalDiskUuid },
    /// the planner did not add zones because of these blockers
    ZoneAddsBlocked(Vec<AddUpdateBlockedReason>),
    /// some sleds are still waiting for NTP zones
    NtpZonesPending { kind: NtpWait, sleds: BTreeSet<SledUuid> },
    /// some sleds are missing Crucible zones
    CrucibleZonesMissing { sleds: BTreeSet<SledUuid> },
    /// the planner could not place all of the zones of this kind that it wanted
    OutOfEligibleSleds {
        zone_kind: String,
        placed: usize,
        wanted_to_place: usize,
    },
    /// the planner placed new zones in its most recent blueprint
    DiscretionaryZonesPlaced { count: usize },
    /// no inventory collection has been loaded
    InventoryMissing,
    /// an in-service zone does not appear in inventory
    ZoneNotInInventory { zone_id: OmicronZoneUuid, zone_kind: ZoneKind },
    /// an in-service zone failed to start
    ZoneReconciliationFailed {
        zone_id: OmicronZoneUuid,
        zone_kind: ZoneKind,
        message: String,
    },
    /// a service lacks the redundancy that it needs
    ServiceLacksRedundancy(ZoneUnsafeToShutdown),
}

/// Why a set of sleds is still waiting for NTP zones
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NtpWait {
    /// the sleds' NTP zones have not yet appeared in inventory
    NotInInventory,
    /// the sleds have no in-service zpools for NTP zones
    NoZpools,
    /// discretionary zone placement on the sleds is waiting for NTP zones
    DiscretionaryZonesWaiting,
    /// the sleds are getting NTP zones and will then get discretionary zones
    GettingNtpAndDiscretionaryZones,
    /// the sleds are missing NTP zones
    Missing,
}

impl fmt::Display for ControlPlaneRisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ControlPlaneRisk::PlannerNotYetRun => write!(
                f,
                "the blueprint planner has not yet run, so it is not known \
                 whether the control plane has recovered"
            ),
            ControlPlaneRisk::PlannerFailed(reason) => {
                write!(
                    f,
                    "the most recent blueprint planning attempt failed ("
                )?;
                match reason {
                    PlannerFailure::Disabled => {
                        write!(f, "blueprint planning is disabled")?
                    }
                    PlannerFailure::ConfigNotYetLoaded => {
                        write!(f, "reconfigurator config not yet loaded")?
                    }
                    PlannerFailure::LimitReached { limit } => write!(
                        f,
                        "the system has reached its limit of {limit} \
                         blueprints"
                    )?,
                    PlannerFailure::Error(error) => write!(f, "{error}")?,
                }
                write!(
                    f,
                    "), so it is not known whether the control plane has \
                     recovered"
                )
            }
            ControlPlaneRisk::PlannedBlueprintMissing { blueprint_id } => {
                write!(
                    f,
                    "blueprint {blueprint_id} from the most recent planning \
                     run no longer exists, so it is not known whether the \
                     control plane has recovered"
                )
            }
            ControlPlaneRisk::ExpungedSledNotAccountedFor { sled_id } => {
                write!(
                    f,
                    "sled {sled_id} is expunged, but the blueprint planner has \
                     not yet accounted for that"
                )
            }
            ControlPlaneRisk::ExpungedDiskNotAccountedFor {
                sled_id,
                disk_id,
            } => write!(
                f,
                "physical disk {disk_id} (on sled {sled_id}) is expunged, but \
                 the blueprint planner has not yet accounted for that"
            ),
            ControlPlaneRisk::ZoneAddsBlocked(reasons) => {
                write!(f, "control plane zones cannot be added (")?;
                for (i, reason) in reasons.iter().enumerate() {
                    if i > 0 {
                        write!(f, "; ")?;
                    }
                    write!(f, "{reason}")?;
                }
                write!(f, ")")
            }
            ControlPlaneRisk::NtpZonesPending { kind, sleds } => {
                let description = match kind {
                    NtpWait::NotInInventory => {
                        "waiting for NTP zones to appear in inventory"
                    }
                    NtpWait::NoZpools => "no zpools in service for NTP zones",
                    NtpWait::DiscretionaryZonesWaiting => {
                        "control plane zone placement waiting for NTP zones"
                    }
                    NtpWait::GettingNtpAndDiscretionaryZones => {
                        "adding NTP zones and then other control plane zones"
                    }
                    NtpWait::Missing => "missing NTP zones",
                };
                write!(f, "{description} on sleds: ")?;
                write_sled_list(f, sleds)
            }
            ControlPlaneRisk::CrucibleZonesMissing { sleds } => {
                write!(f, "missing Crucible zones on sleds: ")?;
                write_sled_list(f, sleds)
            }
            ControlPlaneRisk::OutOfEligibleSleds {
                zone_kind,
                placed,
                wanted_to_place,
            } => write!(
                f,
                "only placed {placed}/{wanted_to_place} desired {zone_kind} \
                 zones"
            ),
            ControlPlaneRisk::DiscretionaryZonesPlaced { count } => write!(
                f,
                "the blueprint planner is still placing control plane zones \
                 ({count} placed in its most recent blueprint)"
            ),
            ControlPlaneRisk::InventoryMissing => write!(
                f,
                "no inventory collection is available, so it is not known \
                 whether control plane zones are running"
            ),
            ControlPlaneRisk::ZoneNotInInventory { zone_id, zone_kind } => {
                write!(
                    f,
                    "{} zone {zone_id} has not yet appeared in inventory",
                    zone_kind.report_str(),
                )
            }
            ControlPlaneRisk::ZoneReconciliationFailed {
                zone_id,
                zone_kind,
                message,
            } => write!(
                f,
                "{} zone {zone_id} failed to start: {message}",
                zone_kind.report_str(),
            ),
            ControlPlaneRisk::ServiceLacksRedundancy(reason) => {
                write!(f, "{reason}")
            }
        }
    }
}

fn write_sled_list(
    f: &mut fmt::Formatter<'_>,
    sleds: &BTreeSet<SledUuid>,
) -> fmt::Result {
    for (i, sled_id) in sleds.iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{sled_id}")?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use nexus_inventory::CollectionBuilder;
    use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
    use nexus_reconfigurator_simulation::BlueprintId;
    use nexus_reconfigurator_simulation::CollectionId;
    use nexus_types::deployment::BlueprintSource;
    use nexus_types::deployment::BlueprintZoneImageSource;
    use nexus_types::external_api::physical_disk::PhysicalDiskPolicy as ExternalPhysicalDiskPolicy;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ZpoolUuid;
    use reconfigurator_cli::test_utils::ReconfiguratorCliTestState;

    // The planner has accounted for an expungement once the blueprint has no
    // in-service zones on the expunged sled and no in-service expunged disks.
    #[test]
    fn test_unaccounted_expungements() {
        let logctx = dev::test_setup_log("test_unaccounted_expungements");
        let (_example, blueprint) = ExampleSystemBuilder::new(
            &logctx.log,
            "test_unaccounted_expungements",
        )
        .build();

        // Nothing expunged: nothing to account for.
        let nothing =
            ExpungedHardware { sleds: BTreeSet::new(), disks: BTreeSet::new() };
        assert_eq!(unaccounted_expungements(&blueprint, &nothing), []);

        // An expunged sled whose zones are still in service has not been
        // accounted for.
        let (sled_id, sled_config) =
            blueprint.sleds.iter().next().expect("example has sleds");
        let expunged_sled = ExpungedHardware {
            sleds: BTreeSet::from([*sled_id]),
            disks: BTreeSet::new(),
        };
        assert_eq!(
            unaccounted_expungements(&blueprint, &expunged_sled),
            [ControlPlaneRisk::ExpungedSledNotAccountedFor {
                sled_id: *sled_id
            }]
        );

        // Likewise an expunged disk that is still in service.
        let disk_id =
            sled_config.disks.iter().next().expect("example sled has disks").id;
        let expunged_disk = ExpungedHardware {
            sleds: BTreeSet::new(),
            disks: BTreeSet::from([disk_id]),
        };
        assert_eq!(
            unaccounted_expungements(&blueprint, &expunged_disk),
            [ControlPlaneRisk::ExpungedDiskNotAccountedFor {
                sled_id: *sled_id,
                disk_id,
            }]
        );

        // Sleds and disks that the blueprint does not know about at all have
        // been accounted for.
        let unknown = ExpungedHardware {
            sleds: BTreeSet::from([SledUuid::new_v4()]),
            disks: BTreeSet::from([PhysicalDiskUuid::new_v4()]),
        };
        assert_eq!(unaccounted_expungements(&blueprint, &unknown), []);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_zone_add_risks() {
        // An empty report: the planner added zones and had nothing to do.
        let report = PlanningAddStepReport::new();
        assert_eq!(zone_add_risks(&report), []);

        // Blockers stop the planner from adding zones.  That counts as
        // recovery still being in progress.
        let blocked_reasons =
            vec![AddUpdateBlockedReason::SledsWithMupdateOverride {
                sled_ids: BTreeSet::from([SledUuid::new_v4()]),
            }];
        let mut report =
            PlanningAddStepReport::waiting_on(ZoneAddWaitingOn::Blockers);
        report.add_update_blocked_reasons = blocked_reasons.clone();
        assert_eq!(
            zone_add_risks(&report),
            [ControlPlaneRisk::ZoneAddsBlocked(blocked_reasons.clone())]
        );

        // In target release generation 1, the planner adds zones despite
        // blockers.  That's not a risk by itself.
        let mut report = PlanningAddStepReport::new();
        report.add_update_blocked_reasons = blocked_reasons;
        report.target_release_generation_is_one = true;
        assert_eq!(zone_add_risks(&report), []);

        // The planner placed some zones.
        let sled_id = SledUuid::new_v4();
        let mut report = PlanningAddStepReport::new();
        report.discretionary_zone_placed(
            sled_id,
            "cockroach_db",
            &BlueprintZoneImageSource::InstallDataset,
        );
        report.discretionary_zone_placed(
            sled_id,
            "internal_dns",
            &BlueprintZoneImageSource::InstallDataset,
        );
        assert_eq!(
            zone_add_risks(&report),
            [ControlPlaneRisk::DiscretionaryZonesPlaced { count: 2 }]
        );

        // The planner wanted to place zones but could not.
        let mut report = PlanningAddStepReport::new();
        report.out_of_eligible_sleds("cockroach_db", 0, 1);
        assert_eq!(
            zone_add_risks(&report),
            [ControlPlaneRisk::OutOfEligibleSleds {
                zone_kind: String::from("cockroach_db"),
                placed: 0,
                wanted_to_place: 1,
            }]
        );

        // Sleds are missing NTP or Crucible zones.
        let mut report = PlanningAddStepReport::new();
        report.sleds_missing_ntp_zone.insert(sled_id);
        report.missing_crucible_zone(sled_id, ZpoolUuid::new_v4());
        assert_eq!(
            zone_add_risks(&report),
            [
                ControlPlaneRisk::NtpZonesPending {
                    kind: NtpWait::Missing,
                    sleds: BTreeSet::from([sled_id]),
                },
                ControlPlaneRisk::CrucibleZonesMissing {
                    sleds: BTreeSet::from([sled_id]),
                },
            ]
        );
    }

    // Zones must appear in inventory without reconciliation errors.
    #[test]
    fn test_zone_propagation_risks() {
        let logctx = dev::test_setup_log("test_zone_propagation_risks");
        let (example, blueprint) = ExampleSystemBuilder::new(
            &logctx.log,
            "test_zone_propagation_risks",
        )
        .build();

        // The example system's inventory reflects its blueprint.
        assert_eq!(zone_propagation_risks(&blueprint, &example.collection), []);

        // With an empty inventory, every in-service zone is missing.
        let empty = CollectionBuilder::new("test").build();
        let risks = zone_propagation_risks(&blueprint, &empty);
        let expected: Vec<_> = blueprint
            .in_service_zones()
            .map(|(_sled_id, zone)| ControlPlaneRisk::ZoneNotInInventory {
                zone_id: zone.id,
                zone_kind: zone.kind(),
            })
            .collect();
        assert!(!expected.is_empty());
        assert_eq!(risks, expected);

        logctx.cleanup_successful();
    }

    // Services that lack redundancy are reported once per kind of zone.
    #[test]
    fn test_zone_safety_risks() {
        let logctx = dev::test_setup_log("test_zone_safety_risks");
        let (example, blueprint) =
            ExampleSystemBuilder::new(&logctx.log, "test_zone_safety_risks")
                .build();
        let targets = ZoneSafetyTargets::from_planning_input(&example.input);

        // An empty inventory has no reports from CockroachDB, NTP, or DNS.  The
        // example system has several zones of each of these kinds, but each
        // kind should be reported only once.
        let empty = CollectionBuilder::new("test").build();
        let risks = zone_safety_risks(&blueprint, &empty, &targets);
        let kinds: Vec<_> = risks
            .iter()
            .map(|risk| match risk {
                ControlPlaneRisk::ServiceLacksRedundancy(reason) => {
                    reason.zone_kind()
                }
                other => panic!("unexpected risk: {other:?}"),
            })
            .collect();
        let expected_kinds: Vec<_> = [
            ZoneKind::BoundaryNtp,
            ZoneKind::CockroachDb,
            ZoneKind::InternalDns,
        ]
        .into_iter()
        .filter(|kind| {
            blueprint.in_service_zones().any(|(_, zone)| zone.kind() == *kind)
        })
        .collect();
        assert!(!expected_kinds.is_empty());
        assert_eq!(kinds, expected_kinds);

        logctx.cleanup_successful();
    }

    // Walks through recovery from a sled expungement in a simulated system,
    // using the real planner.
    #[test]
    fn test_recovery_from_sled_expungement() {
        check_recovery(
            "test_recovery_from_sled_expungement",
            |sim, blueprint| {
                // Expunge a sled that has an internal DNS zone.  This also
                // expunges its disks, as the real system does.
                let (sled_id, _) = blueprint
                    .in_service_zones()
                    .find(|(_, zone)| zone.zone_type.is_internal_dns())
                    .expect("example system has an internal DNS zone");
                sim.change_description("expunge sled", |description| {
                    description.sled_expunge(sled_id)?;
                    Ok(())
                })
                .expect("expunged sled");
                ControlPlaneRisk::ExpungedSledNotAccountedFor { sled_id }
            },
        );
    }

    // Walks through recovery from a physical disk expungement in a simulated
    // system, using the real planner.
    #[test]
    fn test_recovery_from_disk_expungement() {
        check_recovery(
            "test_recovery_from_disk_expungement",
            |sim, blueprint| {
                // Expunge the disk that holds an internal DNS zone.
                let (sled_id, zone) = blueprint
                    .in_service_zones()
                    .find(|(_, zone)| zone.zone_type.is_internal_dns())
                    .expect("example system has an internal DNS zone");
                let zpool_id = zone.filesystem_pool.id();
                let disk_id = blueprint.sleds[&sled_id]
                    .disks
                    .iter()
                    .find(|disk| disk.pool_id == zpool_id)
                    .expect("internal DNS zone's zpool has a disk")
                    .id;
                sim.change_description("expunge disk", |description| {
                    description
                        .get_sled_mut(sled_id)?
                        .resources_mut()
                        .zpools
                        .get_mut(&zpool_id)
                        .expect("zpool exists")
                        .policy = ExternalPhysicalDiskPolicy::Expunged;
                    Ok(())
                })
                .expect("expunged disk");
                ControlPlaneRisk::ExpungedDiskNotAccountedFor {
                    sled_id,
                    disk_id,
                }
            },
        );
    }

    /// Walks through recovery from an expungement in a simulated system, using
    /// the real planner
    ///
    /// `expunge` expunges something in the simulated system, given the
    /// blueprint that the system has settled on.  It returns the risk that the
    /// check should report until the planner accounts for the expungement.
    ///
    /// The check should report that the planner hasn't accounted for the
    /// expungement, then that recovery is in progress, and finally that
    /// recovery is complete.
    fn check_recovery(
        test_name: &str,
        expunge: impl FnOnce(
            &mut ReconfiguratorCliTestState,
            &Blueprint,
        ) -> ControlPlaneRisk,
    ) {
        let logctx = dev::test_setup_log(test_name);
        let mut sim = ReconfiguratorCliTestState::new(test_name, &logctx.log);

        // Use enough sleds that the planner can replace every zone on a sled
        // that we expunge.
        sim.load_example_customized(|builder| Ok(builder.nsleds(5)))
            .expect("loaded example system");

        // Before anything is expunged, the system should settle into a state
        // where the check passes.
        let blueprint = sim_recover(&mut sim);
        assert_eq!(sim_risks(&sim, &blueprint), []);
        let replaceable_zone_counts_before =
            replaceable_zone_counts(&blueprint);

        let expected_risk = expunge(&mut sim, &blueprint);

        // Until the planner runs, the check reports only that the planner has
        // not accounted for the expungement.
        let risks = sim_risks(&sim, &blueprint);
        assert!(risks.contains(&expected_risk), "{risks:#?}");
        assert!(risks.iter().all(is_unaccounted_expungement), "{risks:#?}");

        // Once the planner runs, it has accounted for the expungement, but the
        // check reports that recovery is still in progress.
        let blueprint = sim.run_planner().expect("planning succeeded");
        let risks = sim_risks(&sim, &blueprint);
        assert!(!risks.is_empty());
        assert!(!risks.iter().any(is_unaccounted_expungement), "{risks:#?}");

        // Eventually, the system recovers and the check passes again.  By then,
        // every zone that the planner replaces elsewhere should have been
        // replaced.
        let blueprint = sim_recover(&mut sim);
        assert_eq!(sim_risks(&sim, &blueprint), []);
        assert_eq!(
            replaceable_zone_counts(&blueprint),
            replaceable_zone_counts_before
        );

        logctx.cleanup_successful();
    }

    fn is_unaccounted_expungement(risk: &ControlPlaneRisk) -> bool {
        matches!(
            risk,
            ControlPlaneRisk::ExpungedSledNotAccountedFor { .. }
                | ControlPlaneRisk::ExpungedDiskNotAccountedFor { .. }
        )
    }

    /// Repeatedly deploys the latest blueprint, collects inventory, and runs
    /// the planner, until the control plane check passes
    ///
    /// Returns the planner's latest blueprint.  Panics if the check does not
    /// pass after a generous number of rounds.
    fn sim_recover(sim: &mut ReconfiguratorCliTestState) -> Arc<Blueprint> {
        const MAX_ROUNDS: usize = 20;
        let mut last_risks = Vec::new();
        for _ in 0..MAX_ROUNDS {
            let blueprint = sim.run_planner().expect("planning succeeded");
            sim.deploy_configs_to_active_sleds("deploy configs", &blueprint)
                .expect("deployed configs");
            sim.generate_inventory("collect inventory")
                .expect("generated inventory");
            last_risks = sim_risks(sim, &blueprint);
            if last_risks.is_empty() {
                return blueprint;
            }
        }
        panic!(
            "control plane check did not pass after {MAX_ROUNDS} rounds: \
             {last_risks:#?}"
        );
    }

    /// Returns the number of in-service zones of each kind in the blueprint,
    /// counting only the kinds of zones that the planner replaces on other
    /// sleds when a sled is expunged
    fn replaceable_zone_counts(
        blueprint: &Blueprint,
    ) -> BTreeMap<ZoneKind, usize> {
        let mut counts = BTreeMap::new();
        for (_sled_id, zone) in blueprint.in_service_zones() {
            let kind = zone.kind();
            let replaceable = match kind {
                // There is one of each of these for each sled or disk.  They
                // go away with the sled or disk and are not replaced.
                ZoneKind::Crucible | ZoneKind::InternalNtp => false,
                ZoneKind::BoundaryNtp
                | ZoneKind::Clickhouse
                | ZoneKind::ClickhouseKeeper
                | ZoneKind::ClickhouseServer
                | ZoneKind::CockroachDb
                | ZoneKind::CruciblePantry
                | ZoneKind::ExternalDns
                | ZoneKind::InternalDns
                | ZoneKind::Nexus
                | ZoneKind::Oximeter => true,
            };
            if replaceable {
                *counts.entry(kind).or_default() += 1;
            }
        }
        counts
    }

    /// Runs the control plane check against the simulated system, using the
    /// given planner-generated blueprint and the latest inventory collection
    fn sim_risks(
        sim: &ReconfiguratorCliTestState,
        blueprint: &Arc<Blueprint>,
    ) -> Vec<ControlPlaneRisk> {
        let report = match &blueprint.source {
            BlueprintSource::Planner(report) => Arc::clone(report),
            other => panic!("blueprint was not made by the planner: {other:?}"),
        };
        let input =
            sim.planning_input(BlueprintId::Latest).expect("planning input");
        let expunged = ExpungedHardware {
            sleds: input
                .all_sleds(SledFilter::All)
                .filter(|(_, details)| match details.policy {
                    SledPolicy::Expunged => true,
                    SledPolicy::InService { .. } => false,
                })
                .map(|(sled_id, _)| sled_id)
                .collect(),
            disks: input
                .all_sled_resources(SledFilter::All)
                .flat_map(|(_, resources)| resources.all_disks(DiskFilter::All))
                .filter(|(_, disk)| match disk.policy {
                    ExternalPhysicalDiskPolicy::Expunged => true,
                    ExternalPhysicalDiskPolicy::InService => false,
                })
                .map(|(_, disk)| disk.disk_id)
                .collect(),
        };
        let inventory = sim
            .inventory(CollectionId::Latest)
            .expect("latest inventory")
            .clone();
        ControlPlaneInput {
            plan: LatestPlan::Planned {
                blueprint: Arc::clone(blueprint),
                report,
            },
            expunged,
            inventory: Some(Arc::new(inventory)),
            targets: ZoneSafetyTargets::from_planning_input(&input),
        }
        .risks()
    }
}
