// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared test-only code for the `datastore` module.

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::ValidateTransition;
use crate::db::lookup::LookupPath;
use crate::db::DataStore;
use anyhow::bail;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use dropshot::test_util::LogContext;
use nexus_db_model::SledState;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledProvisionPolicy;
use omicron_test_utils::dev::db::CockroachInstance;
use std::sync::Arc;
use strum::EnumCount;
use uuid::Uuid;

/// Constructs a DataStore for use in test suites that has preloaded the
/// built-in users, roles, and role assignments that are needed for basic
/// operation
#[cfg(test)]
pub async fn datastore_test(
    logctx: &LogContext,
    db: &CockroachInstance,
) -> (OpContext, Arc<DataStore>) {
    use crate::authn;

    let cfg = db::Config { url: db.pg_config().clone() };
    let pool = Arc::new(db::Pool::new(&logctx.log, &cfg));
    let datastore =
        Arc::new(DataStore::new(&logctx.log, pool, None).await.unwrap());

    // Create an OpContext with the credentials of "db-init" just for the
    // purpose of loading the built-in users, roles, and assignments.
    let opctx = OpContext::for_background(
        logctx.log.new(o!()),
        Arc::new(authz::Authz::new(&logctx.log)),
        authn::Context::internal_db_init(),
        Arc::clone(&datastore),
    );

    // TODO: Can we just call "Populate" instead of doing this?
    let rack_id = Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap();
    datastore.load_builtin_users(&opctx).await.unwrap();
    datastore.load_builtin_roles(&opctx).await.unwrap();
    datastore.load_builtin_role_asgns(&opctx).await.unwrap();
    datastore.load_builtin_silos(&opctx).await.unwrap();
    datastore.load_builtin_projects(&opctx).await.unwrap();
    datastore.load_builtin_vpcs(&opctx).await.unwrap();
    datastore.load_silo_users(&opctx).await.unwrap();
    datastore.load_silo_user_role_assignments(&opctx).await.unwrap();
    datastore
        .load_builtin_fleet_virtual_provisioning_collection(&opctx)
        .await
        .unwrap();
    datastore.load_builtin_rack_data(&opctx, rack_id).await.unwrap();

    // Create an OpContext with the credentials of "test-privileged" for general
    // testing.
    let opctx =
        OpContext::for_tests(logctx.log.new(o!()), Arc::clone(&datastore));

    (opctx, datastore)
}

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
    pub(super) non_provisionable: Uuid,
    pub(super) expunged: Uuid,
    pub(super) decommissioned: Uuid,
    pub(super) illegal_decommissioned: Uuid,
}

impl IneligibleSleds {
    pub(super) fn iter(
        &self,
    ) -> impl Iterator<Item = (IneligibleSledKind, Uuid)> {
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
}

pub(super) async fn sled_set_policy(
    opctx: &OpContext,
    datastore: &DataStore,
    sled_id: Uuid,
    new_policy: SledPolicy,
    check: ValidateTransition,
    expected_old_policy: Expected<SledPolicy>,
) -> Result<()> {
    let (authz_sled, _) = LookupPath::new(&opctx, &datastore)
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
    sled_id: Uuid,
    new_state: SledState,
    check: ValidateTransition,
    expected_old_state: Expected<SledState>,
) -> Result<()> {
    let (authz_sled, _) = LookupPath::new(&opctx, &datastore)
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
