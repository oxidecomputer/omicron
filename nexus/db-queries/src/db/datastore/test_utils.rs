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
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledProvisionPolicy;
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
    ) -> impl Iterator<Item = (IneligibleSledKind, SledUuid)> {
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
