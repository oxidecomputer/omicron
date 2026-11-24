// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared test-only code for the `datastore` module.

use crate::authz;
use crate::context::OpContext;
use crate::db::DataStore;
use crate::db::datastore::ValidateTransition;
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use anyhow::ensure;
use futures::future::try_join_all;
use nexus_db_lookup::LookupPath;
use nexus_db_model::SledState;
use nexus_types::external_api::sled::{SledPolicy, SledProvisionPolicy};
use omicron_uuid_kinds::SledUuid;
use strum::EnumCount;

/// Denotes a specific way in which a sled is ineligible.
///
/// This should match the list of sleds in `IneligibleSleds`.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, EnumCount,
)]
pub(super) enum IneligibleSledKind {
    NonProvisionable,
    Expunged,
    Decommissioned,
    IllegalDecommissioned,
}

/// Specifies sleds to be marked as ineligible for provisioning.
///
/// This is less error-prone than several places duplicating this logic.
#[derive(Debug)]
pub(super) struct IneligibleSleds {
    pub(super) non_provisionable: SledUuid,
    pub(super) expunged: SledUuid,
    pub(super) decommissioned: SledUuid,
    pub(super) illegal_decommissioned: SledUuid,
}

impl IneligibleSleds {
    pub(super) fn iter(
        &self,
    ) -> impl Iterator<Item = (IneligibleSledKind, SledUuid)> + use<> {
        [
            (IneligibleSledKind::NonProvisionable, self.non_provisionable),
            (IneligibleSledKind::Expunged, self.expunged),
            (IneligibleSledKind::Decommissioned, self.decommissioned),
            (
                IneligibleSledKind::IllegalDecommissioned,
                self.illegal_decommissioned,
            ),
        ]
        .into_iter()
    }

    /// Marks the provided sleds as ineligible for provisioning.
    ///
    /// Assumes that:
    ///
    /// * the sleds have just been set up and are in the default state.
    /// * the UUIDs are all distinct.
    pub(super) async fn setup(
        &self,
        opctx: &OpContext,
        datastore: &DataStore,
    ) -> Result<()> {
        let non_provisionable_fut = async {
            sled_set_policy(
                &opctx,
                &datastore,
                self.non_provisionable,
                SledPolicy::InService {
                    provision_policy: SledProvisionPolicy::NonProvisionable,
                },
                ValidateTransition::Yes,
                Expected::Ok(SledPolicy::provisionable()),
            )
            .await
            .with_context(|| {
                format!(
                    "failed to set non-provisionable policy for sled {}",
                    self.non_provisionable
                )
            })
        };

        let expunged_fut = async {
            sled_set_policy(
                &opctx,
                &datastore,
                self.expunged,
                SledPolicy::Expunged,
                ValidateTransition::Yes,
                Expected::Ok(SledPolicy::provisionable()),
            )
            .await
            .with_context(|| {
                format!(
                    "failed to set expunged policy for sled {}",
                    self.non_provisionable
                )
            })
        };

        // Legally, we must set the policy to expunged before setting the state
        // to decommissioned. (In the future, we'll want to test graceful
        // removal as well.)
        let decommissioned_fut = async {
            sled_set_policy(
                &opctx,
                &datastore,
                self.decommissioned,
                SledPolicy::Expunged,
                ValidateTransition::Yes,
                Expected::Ok(SledPolicy::provisionable()),
            )
            .await
            .with_context(|| {
                format!(
                    "failed to set expunged policy for sled {}, \
                     as prerequisite for decommissioning",
                    self.decommissioned
                )
            })?;

            sled_set_state(
                &opctx,
                &datastore,
                self.decommissioned,
                SledState::Decommissioned,
                ValidateTransition::Yes,
                Expected::Ok(SledState::Active),
            )
            .await
            .with_context(|| {
                format!(
                    "failed to set decommissioned state for sled {}",
                    self.decommissioned
                )
            })
        };

        // This is _not_ a legal state, BUT we test it out to ensure that if
        // the system somehow enters this state anyway, we don't try and
        // provision resources on it.
        let illegal_decommissioned_fut = async {
            sled_set_state(
                &opctx,
                &datastore,
                self.illegal_decommissioned,
                SledState::Decommissioned,
                ValidateTransition::No,
                Expected::Ok(SledState::Active),
            )
            .await
            .with_context(|| {
                format!(
                    "failed to illegally set decommissioned state for sled {}",
                    self.illegal_decommissioned
                )
            })
        };

        // We're okay cancelling the rest of the futures if one of them fails,
        // since the overall test is going to fail anyway. Hence try_join
        // rather than join_then_try.
        futures::try_join!(
            non_provisionable_fut,
            expunged_fut,
            decommissioned_fut,
            illegal_decommissioned_fut
        )?;

        Ok(())
    }

    /// Brings all of the sleds back to being in-service and provisionable.
    ///
    /// This is never going to happen in production, but it's easier to do this
    /// in many tests than to set up a new set of sleds.
    ///
    /// Note: there's no memory of the previous state stored here -- this just
    /// resets the sleds to the default state.
    pub async fn undo(
        &self,
        opctx: &OpContext,
        datastore: &DataStore,
    ) -> Result<()> {
        async fn undo_single(
            opctx: &OpContext,
            datastore: &DataStore,
            sled_id: SledUuid,
            kind: IneligibleSledKind,
        ) -> Result<()> {
            sled_set_policy(
                &opctx,
                &datastore,
                sled_id,
                SledPolicy::provisionable(),
                ValidateTransition::No,
                Expected::Ignore,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to set provisionable policy for sled {} ({:?})",
                    sled_id, kind,
                )
            })?;

            sled_set_state(
                &opctx,
                &datastore,
                sled_id,
                SledState::Active,
                ValidateTransition::No,
                Expected::Ignore,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to set active state for sled {} ({:?})",
                    sled_id, kind,
                )
            })?;

            Ok(())
        }

        _ = try_join_all(self.iter().map(|(kind, sled_id)| {
            undo_single(opctx, datastore, sled_id, kind)
        }))
        .await?;

        Ok(())
    }
}

pub(super) async fn sled_set_policy(
    opctx: &OpContext,
    datastore: &DataStore,
    sled_id: SledUuid,
    new_policy: SledPolicy,
    check: ValidateTransition,
    expected_old_policy: Expected<SledPolicy>,
) -> Result<()> {
    let (authz_sled, _) = LookupPath::new(&opctx, datastore)
        .sled_id(sled_id)
        .fetch_for(authz::Action::Modify)
        .await
        .unwrap();

    let res = datastore
        .sled_set_policy_impl(opctx, &authz_sled, new_policy, check)
        .await;
    match expected_old_policy {
        Expected::Ok(expected) => {
            let actual = res.context(
                "failed transition that was expected to be successful",
            )?;
            ensure!(
                actual == expected,
                "actual old policy ({actual}) is not \
                 the same as expected ({expected})"
            );
        }
        Expected::Invalid => match res {
            Ok(old_policy) => {
                bail!(
                    "expected an invalid state transition error, \
                     but transition was accepted with old policy: \
                     {old_policy}"
                )
            }
            Err(error) => {
                error.ensure_invalid_transition()?;
            }
        },
        Expected::Ignore => {
            // The return value is ignored.
        }
    }

    Ok(())
}

pub(super) async fn sled_set_state(
    opctx: &OpContext,
    datastore: &DataStore,
    sled_id: SledUuid,
    new_state: SledState,
    check: ValidateTransition,
    expected_old_state: Expected<SledState>,
) -> Result<()> {
    let (authz_sled, _) = LookupPath::new(&opctx, datastore)
        .sled_id(sled_id)
        .fetch_for(authz::Action::Modify)
        .await
        .unwrap();

    let res = datastore
        .sled_set_state_impl(&opctx, &authz_sled, new_state, check)
        .await;
    match expected_old_state {
        Expected::Ok(expected) => {
            let actual = res.context(
                "failed transition that was expected to be successful",
            )?;
            ensure!(
                actual == expected,
                "actual old state ({actual:?}) \
                 is not the same as expected ({expected:?})"
            );
        }
        Expected::Invalid => match res {
            Ok(old_state) => {
                bail!(
                    "expected an invalid state transition error, \
                    but transition was accepted with old state: \
                    {old_state:?}"
                )
            }
            Err(error) => {
                error.ensure_invalid_transition()?;
            }
        },
        Expected::Ignore => {
            // The return value is ignored.
        }
    }

    Ok(())
}

/// For a transition, describes the expected value of the old state.
pub(super) enum Expected<T> {
    /// The transition is expected to successful, with the provided old
    /// value.
    Ok(T),

    /// The transition is expected to be invalid.
    Invalid,

    /// The return value is ignored.
    Ignore,
}

/// Shared test-only code for creating problems, used by tests of problems and
/// of the resources that may be attached to them.
pub(super) mod problem {
    use crate::authz;
    use crate::context::OpContext;
    use crate::db::DataStore;
    use chrono::DateTime;
    use chrono::Utc;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::ProblemIdentity;
    use nexus_db_model::ProblemRevision;
    use nexus_db_model::SqlU32;
    use nexus_types::fm::Sitrep;
    use nexus_types::fm::SitrepMetadata;
    use nexus_types::fm::problem::ProblemCode;
    use omicron_generation_kinds::ProblemRevNum;
    use omicron_generation_kinds::SitrepAlertGeneration;
    use omicron_generation_kinds::SitrepProblemGeneration;
    use omicron_generation_kinds::SitrepSupportBundleGeneration;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::ProblemUuid;
    use omicron_uuid_kinds::SitrepUuid;

    pub(in crate::db::datastore) fn rev(revision: u32) -> ProblemRevNum {
        ProblemRevNum::from_u32(revision)
    }

    /// Inserts a parentless sitrep with the given `problem_generation`, which
    /// becomes the current sitrep if no sitrep is current yet.
    ///
    /// Creating a problem requires a current sitrep whose `problem_generation`
    /// matches the one the creator expects.
    pub(in crate::db::datastore) async fn insert_current_sitrep(
        opctx: &OpContext,
        datastore: &DataStore,
        problem_generation: SitrepProblemGeneration,
    ) {
        let sitrep = Sitrep {
            metadata: SitrepMetadata {
                id: SitrepUuid::new_v4(),
                parent_sitrep_id: None,
                inv_collection_id: CollectionUuid::new_v4(),
                next_inv_min_time_started: Utc::now(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "problem test sitrep".to_string(),
                time_created: Utc::now(),
                alert_generation: SitrepAlertGeneration::new(),
                support_bundle_generation: SitrepSupportBundleGeneration::new(),
                problem_generation,
            },
            cases: Default::default(),
            ereports_by_id: Default::default(),
            problems: Default::default(),
        };
        datastore
            .fm_sitrep_insert(opctx, sitrep, None)
            .await
            .expect("inserting a sitrep should succeed");
    }

    /// Returns revision `revision` of the problem `id`.
    ///
    /// Each revision's `time_created` is `revision` seconds after
    /// `time_base`, so that revisions are distinguishable by timestamp.
    pub(in crate::db::datastore) fn make_revision(
        id: ProblemUuid,
        revision: u32,
        time_base: DateTime<Utc>,
        code: ProblemCode,
    ) -> ProblemRevision {
        ProblemRevision {
            problem_id: id.into(),
            revision: rev(revision).into(),
            time_created: time_base
                + chrono::TimeDelta::seconds(i64::from(revision)),
            time_resolved: None,
            code: SqlU32::new(code.into()),
            summary: format!("My Cool Problem (rev {revision})"),
            description: format!(
                "This is a bunch of interesting information about revision \
                 {revision} of my super cool and interesting problem. The \
                 problem is definitely real and exists."
            ),
            action_required: format!("fix the problem (rev {revision})"),
        }
    }

    /// Creates a problem and inserts `revisions` new revisions of it (starting
    /// at revision 1), without making any of them current.
    pub(in crate::db::datastore) async fn create_with_revisions(
        opctx: &OpContext,
        datastore: &DataStore,
        revisions: u32,
    ) -> (authz::Problem, Vec<ProblemRevision>) {
        let id = ProblemUuid::new_v4();
        datastore
            .problem_create(
                opctx,
                ProblemIdentity::new(id),
                SitrepProblemGeneration::new(),
            )
            .await
            .expect("creating a problem should succeed");

        let time_base = omicron_common::now_db_precision();
        let (authz_problem,) = LookupPath::new(opctx, datastore)
            .problem_id(id)
            .lookup_for(authz::Action::Modify)
            .await
            .unwrap();
        let mut inserted = Vec::new();
        for revision in 1..=revisions {
            let revision = make_revision(
                id,
                revision,
                time_base,
                ProblemCode::UnhealthyZpool,
            );
            let was_inserted = datastore
                .problem_revision_insert(
                    opctx,
                    &authz_problem,
                    revision.clone(),
                )
                .await
                .expect("inserting a revision should succeed");
            assert!(was_inserted, "a new revision should be inserted");
            inserted.push(revision);
        }
        (authz_problem, inserted)
    }
}
