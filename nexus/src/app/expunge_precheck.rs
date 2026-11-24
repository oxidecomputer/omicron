// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Checking for conditions that make it risky to expunge a working sled or
//! physical disk
//!
//! All of this should presumably be subsumed by Fault Management / Active
//! Problems.

use chrono::DateTime;
use chrono::Utc;
use cockroach_admin_types::node::InternalNodeId;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::ExpungedPhysicalDiskResourcesMap;
use nexus_reconfigurator_planning::planner::cockroachdb_unhealthy_reasons;
use nexus_types::deployment::CockroachdbUnsafeToShutdown;
use nexus_types::deployment::SledFilter;
use nexus_types::external_api::expunge_precheck::CockroachdbProblemKind;
use nexus_types::external_api::expunge_precheck::ExpungePrecheck;
use nexus_types::external_api::expunge_precheck::ExpungePrecheckProblem;
use nexus_types::external_api::expunge_precheck::ExpungePrecheckResult;
use nexus_types::external_api::expunge_precheck::InventoryProblemKind;
use nexus_types::external_api::expunge_precheck::SledsProblemKind;
use nexus_types::external_api::expunge_precheck::UserDataProblemKind;
use nexus_types::identity::Asset;
use nexus_types::inventory::Collection;
use omicron_common::api::external::Error;
use omicron_common::policy::COCKROACHDB_REDUNDANCY;
use omicron_uuid_kinds::SledUuid;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

/// Maximum age of the latest inventory collection before the expunge precheck
/// reports it as a problem
///
/// The age is measured from the time that the collection started.  Nexus
/// collects inventory at least every 10 minutes, and a collection generally
/// takes much less time than that.  A threshold under 10 minutes would produce
/// false positives on a healthy system.
const EXPUNGE_PRECHECK_INVENTORY_MAX_AGE: Duration =
    Duration::from_secs(15 * 60);

impl super::Nexus {
    /// Reports on risks of expunging a working sled or physical disk
    ///
    /// Note that this is best-effort.  It doesn't check for all kinds of
    /// problems and it can miss risks introduced by recent failures (i.e.,
    /// those newer than the most recent inventory collection).
    pub(crate) async fn expunge_precheck(
        &self,
        opctx: &OpContext,
    ) -> Result<ExpungePrecheck, Error> {
        let datastore = self.datastore();

        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        // Load the latest inventory collection.  We'll use this to check for
        // two things:
        //
        // - We'll identify any sleds that appear unavailable (because their
        //   sled agent is missing from inventory) because those might reflect
        //   Crucible volumes currently at reduced availability.
        //
        // - We'll look for issues reported by CockroachDB like under-replicated
        //   ranges.
        //
        // As mentioned above, this is best-effort.  It's of course possible
        // that a sled is present in inventory but its Crucible downstairs is
        // offline.
        let Some(inventory) = self.inventory_load_rx().borrow().clone() else {
            return Err(Error::unavail(
                "no inventory collection has been loaded yet",
            ));
        };

        // Load the list of in-service sleds so that we know what to expect in
        // inventory.
        let in_service_sleds: BTreeMap<SledUuid, String> = datastore
            .sled_list_all_batched(opctx, SledFilter::InService)
            .await?
            .into_iter()
            .map(|sled| (sled.id(), sled.serial_number().to_owned()))
            .collect();

        // In principle, a system could have a different target CockroachDB
        // count, but today this constant is the only source of this value in
        // deployed systems.  It would be safer to get this from the `Policy` in
        // a planning input, but it's not allowed to construct that in an
        // external API context because it's too expensive.
        let cockroachdb_target_count = COCKROACHDB_REDUNDANCY;

        // Fetch information about Crucible resources on expunged disks.  If
        // these are present, then expunging another disk further risks
        // durability and availability of user data.
        let expunged_resources = datastore
            .crucible_resources_on_expunged_physical_disks(opctx)
            .await?;

        // Fetch information about ongoing Crucible repairs.  These too indicate
        // that expunging another disk is risky.  This is largely redundant with
        // looking at expunged resources, since in practice repairs correspond
        // with resources on expunged disks.  However, it is possible to request
        // a region replacement on an in-service disk using `omdb`.  This
        // catches those, too.
        let replacements_in_progress = InProgressCrucibleReplacements {
            region_replacements: datastore
                .count_incomplete_region_replacements(opctx)
                .await?,
            region_snapshot_replacements: datastore
                .count_incomplete_region_snapshot_replacements(opctx)
                .await?,
        };

        Ok(ExpungePrecheck::new(
            inventory.time_started,
            check_inventory(&inventory, Utc::now()),
            check_crucible_data(&expunged_resources, replacements_in_progress),
            check_cockroachdb(&inventory, cockroachdb_target_count),
            check_sleds(&inventory, &in_service_sleds),
        ))
    }
}

/// Checks whether the latest inventory collection is recent.
///
/// The other checks rely on this collection.  If it's old, they may miss
/// problems that have happened since it was collected.
fn check_inventory(
    inventory: &Collection,
    now: DateTime<Utc>,
) -> ExpungePrecheckResult<InventoryProblemKind> {
    let age = now.signed_duration_since(inventory.time_started);
    // `to_std()` fails if `age` is negative.  This is very unlikely but could
    // happen due to clock skew between systems.
    let too_old =
        age.to_std().is_ok_and(|age| age > EXPUNGE_PRECHECK_INVENTORY_MAX_AGE);
    if too_old {
        let summary = format!("Using inventory from {age} ago (too old)");
        let problems = vec![ExpungePrecheckProblem {
            kind: InventoryProblemKind::TooOld,
            message: format!(
                "Inventory is too old (age: {}, max: {:?})",
                age, EXPUNGE_PRECHECK_INVENTORY_MAX_AGE
            ),
        }];
        ExpungePrecheckResult { summary, problems }
    } else {
        let summary = format!("Using inventory from {age} ago");
        ExpungePrecheckResult { summary, problems: Vec::new() }
    }
}

/// Counts of Crucible replacement requests that have not yet completed
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct InProgressCrucibleReplacements {
    region_replacements: u64,
    region_snapshot_replacements: u64,
}

/// Checks whether Crucible data on expunged physical disks has been fully
/// re-replicated.
fn check_crucible_data(
    expunged_resources: &ExpungedPhysicalDiskResourcesMap,
    replacements_in_progress: InProgressCrucibleReplacements,
) -> ExpungePrecheckResult<UserDataProblemKind> {
    let InProgressCrucibleReplacements {
        region_replacements,
        region_snapshot_replacements,
    } = replacements_in_progress;

    let mut problems: Vec<_> = expunged_resources
        .iter()
        .map(|r| ExpungePrecheckProblem {
            kind: UserDataProblemKind::ExpungedDiskHasData,
            message: format!(
                "physical disk {} (serial {}) is expunged but still holds data \
                 (read-write regions: {}, read-only regions: {}, region \
                 snapshots: {})",
                r.disk_id,
                r.disk_serial,
                r.num_read_write_regions,
                r.num_read_only_regions,
                r.num_region_snapshots,
            ),
        })
        .collect();
    if region_replacements > 0 {
        problems.push(ExpungePrecheckProblem {
            kind: UserDataProblemKind::ReplacementInProgress,
            message: format!(
                "region replacements in progress: {region_replacements}"
            ),
        });
    }
    if region_snapshot_replacements > 0 {
        problems.push(ExpungePrecheckProblem {
            kind: UserDataProblemKind::ReplacementInProgress,
            message: format!(
                "region snapshot replacements in progress: \
                 {region_snapshot_replacements}"
            ),
        });
    }

    // XXX-dap it's silly to put this all in text
    let summary = format!(
        "Expunged physical disks still holding user data: {}. Region \
         replacements in progress: {region_replacements}. \
         Region snapshot replacements in progress: \
         {region_snapshot_replacements}.",
        expunged_resources.len(),
    );
    ExpungePrecheckResult { summary, problems }
}

/// Checks whether CockroachDB is fully healthy
///
/// This uses the same criteria that the planner uses to decide whether it's
/// safe to shut down a CockroachDB node: every expected node must report its
/// status, and each one must report no under-replicated ranges and enough live
/// nodes.
fn check_cockroachdb(
    inventory: &Collection,
    target: usize,
) -> ExpungePrecheckResult<CockroachdbProblemKind> {
    let reasons = cockroachdb_unhealthy_reasons(inventory, target);
    let nreporting = inventory.cockroach_status.len();
    let unhealthy_nodes: BTreeSet<&InternalNodeId> =
        reasons.iter().filter_map(|(node_id, _)| *node_id).collect();
    let nhealthy = nreporting.saturating_sub(unhealthy_nodes.len());
    let summary = format!(
        "{nreporting} of {target} expected CockroachDB nodes reported their \
         status. {nhealthy} of them reported 0 under-replicated ranges and \
         at least {target} live nodes."
    );

    let problems = reasons
        .into_iter()
        .map(|(node_id, reason)| {
            cockroachdb_problem(node_id, reason, nreporting, target)
        })
        .collect();
    ExpungePrecheckResult { summary, problems }
}

/// Converts one reason from `cockroachdb_unhealthy_reasons()` into a problem
/// for the external API.
fn cockroachdb_problem(
    node_id: Option<&InternalNodeId>,
    reason: CockroachdbUnsafeToShutdown,
    nreporting: usize,
    target: usize,
) -> ExpungePrecheckProblem<CockroachdbProblemKind> {
    let node = match node_id {
        Some(node_id) => format!("CockroachDB node {node_id}"),
        None => String::from("a CockroachDB node"),
    };
    let (kind, message) = match reason {
        CockroachdbUnsafeToShutdown::NotEnoughNodes => (
            CockroachdbProblemKind::StatusMissing,
            format!(
                "only {nreporting} of {target} expected CockroachDB nodes \
                 reported their status"
            ),
        ),
        CockroachdbUnsafeToShutdown::MissingUnderreplicatedStat => (
            CockroachdbProblemKind::StatusMissing,
            format!(
                "{node} did not report its number of under-replicated ranges"
            ),
        ),
        CockroachdbUnsafeToShutdown::MissingLiveNodesStat => (
            CockroachdbProblemKind::StatusMissing,
            format!("{node} did not report its number of live nodes"),
        ),
        CockroachdbUnsafeToShutdown::UnderreplicatedRanges { n } => (
            CockroachdbProblemKind::UnderReplicated,
            format!("{node} reports {n} under-replicated ranges"),
        ),
        CockroachdbUnsafeToShutdown::NotEnoughLiveNodes { live_nodes } => (
            CockroachdbProblemKind::TooFewLiveNodes,
            format!(
                "{node} reports {live_nodes} live nodes (expected at least \
                 {target})"
            ),
        ),
    };
    ExpungePrecheckProblem { kind, message }
}

/// Checks whether every in-service sled appears in the latest inventory
/// collection.
///
/// We do this because a sled that's missing from inventory may be down or
/// unreachable and that means its Crucible volumes might be offline.  This
/// isn't necessarily true, of course, and it's also possible that Crucible is
/// offline despite the sled being present in inventory.  Nonetheless, this
/// seems like a useful data point to elevate to the operator.
///
/// Note that if a sled is known to be broken, this will report that sled as
/// missing, and that will cause the expunge precheck to fail because of the
/// very sled the operator is trying to expunge.  However, if a sled is broken,
/// there's no point in using `expunge_precheck` at all.  It's only useful when
/// considering expunging a working sled.
fn check_sleds(
    inventory: &Collection,
    in_service_sleds: &BTreeMap<SledUuid, String>,
) -> ExpungePrecheckResult<SledsProblemKind> {
    let problems: Vec<_> = in_service_sleds
        .iter()
        .filter(|(sled_id, _)| !inventory.sled_agents.contains_key(*sled_id))
        .map(|(sled_id, serial)| ExpungePrecheckProblem {
            kind: SledsProblemKind::MissingFromInventory,
            message: format!(
                "sled {sled_id} (serial {serial}) is in service but is \
                 missing from the latest inventory collection (it may be \
                 offline)",
            ),
        })
        .collect();

    let nsleds = in_service_sleds.len();
    let npresent = nsleds - problems.len();
    let summary = format!(
        "{npresent} of {nsleds} in-service sleds are present in the latest \
         inventory collection."
    );
    ExpungePrecheckResult { summary, problems }
}

#[cfg(test)]
mod test {
    use super::*;
    use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
    use nexus_types::deployment::PlanningInput;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use std::sync::Arc;

    /// Returns the serial numbers of the in-service sleds in a planning
    /// input, by sled id.
    fn in_service_sleds(input: &PlanningInput) -> BTreeMap<SledUuid, String> {
        input
            .all_sleds(SledFilter::InService)
            .map(|(sled_id, details)| {
                (sled_id, details.baseboard_id.serial_number.clone())
            })
            .collect()
    }

    #[test]
    fn test_check_inventory() {
        static TEST_NAME: &str = "test_check_inventory";
        let logctx = test_setup_log(TEST_NAME);
        let (collection, _) = healthy_system(&logctx.log, TEST_NAME);
        let started = collection.time_started;
        let max_age =
            chrono::TimeDelta::from_std(EXPUNGE_PRECHECK_INVENTORY_MAX_AGE)
                .expect("maximum age fits in a TimeDelta");
        let one_second = chrono::TimeDelta::seconds(1);

        // A collection that's exactly at the limit is not a problem.
        let check = check_inventory(&collection, started + max_age);
        assert!(check.problems.is_empty(), "{check:?}");

        // A collection that's older than the limit is a problem.
        let check =
            check_inventory(&collection, started + max_age + one_second);
        assert_eq!(check.problems.len(), 1, "{check:?}");
        assert_eq!(check.problems[0].kind, InventoryProblemKind::TooOld);

        // A collection that appears to have started in the future (because
        // of clock skew) is not a problem.
        let check = check_inventory(&collection, started - one_second);
        assert!(check.problems.is_empty(), "{check:?}");

        logctx.cleanup_successful();
    }

    #[test]
    fn test_check_user_data() {
        let none_in_progress = InProgressCrucibleReplacements {
            region_replacements: 0,
            region_snapshot_replacements: 0,
        };

        // Nothing outstanding: no problems.
        let check = check_crucible_data(
            &ExpungedPhysicalDiskResourcesMap::default(),
            none_in_progress,
        );
        assert!(check.problems.is_empty(), "{check:?}");

        // A disk with resources on it is a problem even when no replacement
        // requests exist yet.
        let disk_id = PhysicalDiskUuid::new_v4();
        let disk_serial = String::from("disk-serial");
        let mut resources = ExpungedPhysicalDiskResourcesMap::default();
        resources.add_read_write_regions(disk_id, disk_serial.clone(), 1);
        resources.add_read_only_regions(disk_id, disk_serial.clone(), 2);
        resources.add_region_snapshots(disk_id, disk_serial, 3);
        let check = check_crucible_data(&resources, none_in_progress);
        assert_eq!(check.problems.len(), 1, "{check:?}");
        assert_eq!(
            check.problems[0].kind,
            UserDataProblemKind::ExpungedDiskHasData
        );
        assert!(
            check.problems[0].message.contains(&disk_id.to_string())
                && check.problems[0].message.contains("disk-serial"),
            "{check:?}"
        );

        // In-progress replacements are problems even when no resources remain
        // on expunged disks.
        let check = check_crucible_data(
            &ExpungedPhysicalDiskResourcesMap::default(),
            InProgressCrucibleReplacements {
                region_replacements: 1,
                region_snapshot_replacements: 2,
            },
        );
        assert_eq!(check.problems.len(), 2, "{check:?}");
        assert!(
            check
                .problems
                .iter()
                .all(|p| p.kind == UserDataProblemKind::ReplacementInProgress)
        );
    }

    /// Returns an inventory collection and a planning input (whose parent
    /// blueprint is the current target) describing a healthy system that runs
    /// every kind of zone.
    fn healthy_system(
        log: &slog::Logger,
        test_name: &str,
    ) -> (Collection, PlanningInput) {
        let (example, blueprint) = ExampleSystemBuilder::new(log, test_name)
            .all_zone_types()
            .expect("configured example system")
            .build();
        let input = example
            .system
            .to_planning_input_builder(Arc::new(blueprint))
            .expect("created planning input builder")
            .build();
        (example.collection, input)
    }

    #[test]
    fn test_healthy_system_has_no_problems() {
        static TEST_NAME: &str = "test_healthy_system_has_no_problems";
        let logctx = test_setup_log(TEST_NAME);
        let (collection, planning_input) =
            healthy_system(&logctx.log, TEST_NAME);
        // The target number of CockroachDB nodes comes from the planning
        // input, just as it does for the planner.
        let target = planning_input.target_cockroachdb_zone_count();

        let cockroachdb = check_cockroachdb(&collection, target);
        assert!(cockroachdb.problems.is_empty(), "{cockroachdb:?}");
        let sleds =
            check_sleds(&collection, &in_service_sleds(&planning_input));
        assert!(sleds.problems.is_empty(), "{sleds:?}");

        // The summaries should affirmatively report what was checked.
        assert!(
            cockroachdb.summary.starts_with(&format!(
                "{target} of {target} expected CockroachDB nodes"
            )),
            "{cockroachdb:?}"
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_cockroachdb_problems() {
        static TEST_NAME: &str = "test_cockroachdb_problems";
        let logctx = test_setup_log(TEST_NAME);
        let (mut collection, planning_input) =
            healthy_system(&logctx.log, TEST_NAME);

        let target = planning_input.target_cockroachdb_zone_count();

        // Break three nodes in three different ways.
        let node_ids: Vec<_> =
            collection.cockroach_status.keys().cloned().collect();
        assert!(node_ids.len() >= 3);
        collection
            .cockroach_status
            .get_mut(&node_ids[0])
            .expect("node exists")
            .ranges_underreplicated = Some(2);
        collection
            .cockroach_status
            .get_mut(&node_ids[1])
            .expect("node exists")
            .liveness_live_nodes = Some(1);
        collection.cockroach_status.remove(&node_ids[2]);

        let check = check_cockroachdb(&collection, target);
        let kinds: Vec<_> = check.problems.iter().map(|p| p.kind).collect();
        assert_eq!(
            kinds,
            [
                CockroachdbProblemKind::StatusMissing,
                CockroachdbProblemKind::UnderReplicated,
                CockroachdbProblemKind::TooFewLiveNodes,
            ],
            "{check:?}"
        );

        // A node that's missing a statistic is reported, too.
        collection
            .cockroach_status
            .get_mut(&node_ids[0])
            .expect("node exists")
            .ranges_underreplicated = None;
        let check = check_cockroachdb(&collection, target);
        assert!(
            check
                .problems
                .iter()
                .any(|p| p.kind == CockroachdbProblemKind::StatusMissing
                    && p.message.contains("under-replicated")),
            "{check:?}"
        );

        logctx.cleanup_successful();
    }

    #[test]
    fn test_sled_missing_from_inventory() {
        static TEST_NAME: &str = "test_sled_missing_from_inventory";
        let logctx = test_setup_log(TEST_NAME);
        let (mut collection, planning_input) =
            healthy_system(&logctx.log, TEST_NAME);

        let sled_id = planning_input
            .all_sled_ids(SledFilter::InService)
            .next()
            .expect("system has an in-service sled");
        collection.sled_agents.remove(sled_id);

        let check =
            check_sleds(&collection, &in_service_sleds(&planning_input));
        assert_eq!(check.problems.len(), 1, "{check:?}");
        assert_eq!(
            check.problems[0].kind,
            SledsProblemKind::MissingFromInventory
        );
        assert!(
            check.problems[0].message.contains(&sled_id.to_string()),
            "{check:?}"
        );

        logctx.cleanup_successful();
    }
}
