// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Checking for conditions that make it risky to expunge a working sled or
//! physical disk
//!
//! All of this should presumably be subsumed by Fault Management / Active
//! Problems.

use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::ExpungedPhysicalDiskResources;
use nexus_db_queries::db::datastore::ExpungedPhysicalDiskResourcesMap;
use nexus_types::external_api::expunge_precheck::ExpungePrecheck;
use nexus_types::external_api::expunge_precheck::UserDataPrecheck;
use omicron_common::api::external::Error;
use std::fmt;

impl super::Nexus {
    /// Checks whether expunging a working sled or physical disk would create
    /// problems because the system is still recovering from previous
    /// expungements or failures
    pub(crate) async fn expunge_precheck(
        &self,
        opctx: &OpContext,
    ) -> Result<ExpungePrecheck, Error> {
        let datastore = self.datastore();

        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        // Fetch information about Crucible resources on expunged disks.  If
        // these are present, then expunging another disk further risks
        // durability and availability of user data.
        let expunged_resources = datastore
            .crucible_resources_on_expunged_physical_disks(opctx)
            .await?;

        // Fetch information about ongoing Crucible replacements.  These too
        // indicate that expunging another disk is risky.  This is largely
        // redundant with looking at expunged resources, since in practice
        // replacements correspond with resources on expunged disks.  However,
        // it is possible to request a region replacement on an in-service disk
        // using `omdb`.  This catches those, too.
        let replacements_in_progress = InProgressCrucibleReplacements {
            region_replacements: datastore
                .count_incomplete_region_replacements(opctx)
                .await?,
            region_snapshot_replacements: datastore
                .count_incomplete_region_snapshot_replacements(opctx)
                .await?,
        };

        Ok(ExpungePrecheck::new(check_crucible_data(
            &expunged_resources,
            replacements_in_progress,
        )))
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
) -> UserDataPrecheck {
    let problems =
        crucible_data_problems(expunged_resources, replacements_in_progress)
            .iter()
            .map(|p| p.to_string())
            .collect();
    UserDataPrecheck {
        expunged_disks_with_data: expunged_resources.len(),
        region_replacements_in_progress: replacements_in_progress
            .region_replacements,
        region_snapshot_replacements_in_progress: replacements_in_progress
            .region_snapshot_replacements,
        problems,
    }
}

/// Returns the problems that the user data check found.
fn crucible_data_problems(
    expunged_resources: &ExpungedPhysicalDiskResourcesMap,
    replacements_in_progress: InProgressCrucibleReplacements,
) -> Vec<UserDataProblem<'_>> {
    let InProgressCrucibleReplacements {
        region_replacements,
        region_snapshot_replacements,
    } = replacements_in_progress;

    let mut problems: Vec<_> = expunged_resources
        .iter()
        .map(UserDataProblem::ExpungedDiskHasData)
        .collect();
    if region_replacements > 0 {
        problems.push(UserDataProblem::RegionReplacementsInProgress(
            region_replacements,
        ));
    }
    if region_snapshot_replacements > 0 {
        problems.push(UserDataProblem::RegionSnapshotReplacementsInProgress(
            region_snapshot_replacements,
        ));
    }
    problems
}

/// A problem found by the user data check
///
/// The external API reports only the `Display` form of these.
#[derive(Debug, PartialEq, Eq)]
enum UserDataProblem<'a> {
    /// an expunged physical disk still holds user data that has not been
    /// re-replicated elsewhere
    ExpungedDiskHasData(&'a ExpungedPhysicalDiskResources),
    /// some region replacements have not finished
    RegionReplacementsInProgress(u64),
    /// some region snapshot replacements have not finished
    RegionSnapshotReplacementsInProgress(u64),
}

impl fmt::Display for UserDataProblem<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UserDataProblem::ExpungedDiskHasData(r) => write!(
                f,
                "physical disk {} (serial {}) is expunged but still holds data \
                 (read-write regions: {}, read-only regions: {}, region \
                 snapshots: {})",
                r.disk_id,
                r.disk_serial,
                r.num_read_write_regions,
                r.num_read_only_regions,
                r.num_region_snapshots,
            ),
            UserDataProblem::RegionReplacementsInProgress(n) => {
                write!(f, "region replacements in progress: {n}")
            }
            UserDataProblem::RegionSnapshotReplacementsInProgress(n) => {
                write!(f, "region snapshot replacements in progress: {n}")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use omicron_uuid_kinds::PhysicalDiskUuid;

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
        let problems = crucible_data_problems(&resources, none_in_progress);
        let [UserDataProblem::ExpungedDiskHasData(r)] = problems.as_slice()
        else {
            panic!("unexpected problems: {problems:?}");
        };
        assert_eq!(r.disk_id, disk_id);
        let check = check_crucible_data(&resources, none_in_progress);
        assert_eq!(check.expunged_disks_with_data, 1, "{check:?}");
        assert_eq!(check.problems.len(), 1, "{check:?}");
        assert!(
            check.problems[0].contains(&disk_id.to_string())
                && check.problems[0].contains(&disk_serial),
            "{check:?}"
        );

        // In-progress replacements are problems even when no resources remain
        // on expunged disks.
        let no_resources = ExpungedPhysicalDiskResourcesMap::default();
        let in_progress = InProgressCrucibleReplacements {
            region_replacements: 1,
            region_snapshot_replacements: 2,
        };
        assert_eq!(
            crucible_data_problems(&no_resources, in_progress),
            [
                UserDataProblem::RegionReplacementsInProgress(1),
                UserDataProblem::RegionSnapshotReplacementsInProgress(2),
            ]
        );
        let check = check_crucible_data(&no_resources, in_progress);
        assert_eq!(check.expunged_disks_with_data, 0, "{check:?}");
        assert_eq!(check.region_replacements_in_progress, 1, "{check:?}");
        assert_eq!(
            check.region_snapshot_replacements_in_progress, 2,
            "{check:?}"
        );
        assert_eq!(check.problems.len(), 2, "{check:?}");
    }
}
