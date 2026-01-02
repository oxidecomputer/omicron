// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Updates sled states required by a given blueprint

use anyhow::Context;
use nexus_db_lookup::LookupPath;
use nexus_db_model::SledState as DbSledState;
use nexus_db_queries::authz::Action;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::TransitionError;
use nexus_types::deployment::Blueprint;
use nexus_types::external_api::views::SledState;
use omicron_uuid_kinds::SledUuid;

pub(crate) async fn decommission_sleds(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
) -> Result<(), Vec<anyhow::Error>> {
    decommission_sleds_impl(
        opctx,
        datastore,
        blueprint
            .sleds
            .iter()
            .filter(|(_, sled)| sled.state == SledState::Decommissioned)
            .map(|(&sled_id, _)| sled_id),
    )
    .await
}

pub(crate) async fn decommission_sleds_impl(
    opctx: &OpContext,
    datastore: &DataStore,
    sled_ids_to_decommission: impl Iterator<Item = SledUuid>,
) -> Result<(), Vec<anyhow::Error>> {
    let mut errors = Vec::new();

    for sled_id in sled_ids_to_decommission {
        if let Err(err) = decommission_one_sled(opctx, datastore, sled_id).await
        {
            errors.push(err);
        }
    }

    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

async fn decommission_one_sled(
    opctx: &OpContext,
    datastore: &DataStore,
    sled_id: SledUuid,
) -> anyhow::Result<()> {
    let (authz_sled,) = LookupPath::new(opctx, datastore)
        .sled_id(sled_id)
        .lookup_for(Action::Modify)
        .await
        .with_context(|| {
            format!("failed to look up sled {sled_id} for modification")
        })?;
    match datastore.sled_set_state_to_decommissioned(opctx, &authz_sled).await {
        Ok(_) => Ok(()),
        // `sled_set_state_to_decommissioned` is not idempotent. If we're racing
        // another Nexus (or we're repeating realization of a blueprint we've
        // already realized), this sled may already be decommissioned; that's
        // fine.
        Err(TransitionError::InvalidTransition { current, .. })
            if current.state() == DbSledState::Decommissioned =>
        {
            Ok(())
        }
        Err(err) => Err(anyhow::Error::new(err)
            .context(format!("failed to decommission sled {sled_id}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::SledFilter;
    use nexus_types::identity::Asset;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

    async fn list_all_commissioned_sled_ids(
        opctx: &OpContext,
        datastore: &DataStore,
    ) -> Vec<SledUuid> {
        datastore
            .sled_list_all_batched(&opctx, SledFilter::Commissioned)
            .await
            .expect("listing sleds")
            .into_iter()
            .map(|sled| sled.id())
            .collect()
    }

    #[nexus_test]
    async fn test_decommission_is_idempotent(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let mut commissioned_sled_ids =
            list_all_commissioned_sled_ids(&opctx, datastore).await;

        // Pick a sled to decommission.
        let decommissioned_sled_id =
            commissioned_sled_ids.pop().expect("at least one sled");

        // Expunge the sled (required prior to decommissioning).
        let (authz_sled,) = LookupPath::new(&opctx, datastore)
            .sled_id(decommissioned_sled_id)
            .lookup_for(Action::Modify)
            .await
            .expect("lookup authz_sled");
        datastore
            .sled_set_policy_to_expunged(&opctx, &authz_sled)
            .await
            .expect("expunged sled");

        // Decommission the sled.
        decommission_sleds_impl(
            &opctx,
            datastore,
            std::iter::once(decommissioned_sled_id),
        )
        .await
        .expect("decommissioned sled");

        // Ensure the sled was marked decommissioned in the db.
        assert_eq!(
            commissioned_sled_ids,
            list_all_commissioned_sled_ids(&opctx, datastore).await
        );

        // Try to decommission the sled again; this should be fine.
        decommission_sleds_impl(
            &opctx,
            datastore,
            std::iter::once(decommissioned_sled_id),
        )
        .await
        .expect("decommissioned sled");
        assert_eq!(
            commissioned_sled_ids,
            list_all_commissioned_sled_ids(&opctx, datastore).await
        );
    }
}
