// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ensures CockroachDB settings are set

use anyhow::Context;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;

pub(crate) async fn ensure_settings(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
) -> anyhow::Result<()> {
    if let Some(value) =
        blueprint.cockroachdb_setting_preserve_downgrade.to_optional_string()
    {
        datastore
            .cockroachdb_setting_set_string(
                opctx,
                blueprint.cockroachdb_fingerprint.clone(),
                "cluster.preserve_downgrade_option",
                value,
            )
            .await
            .context("failed to set cluster.preserve_downgrade_option")?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::overridables_for_test;
    use crate::test_utils::realize_blueprint_and_expect;
    use nexus_db_queries::authn;
    use nexus_db_queries::authz;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::CockroachDbPreserveDowngrade;
    use std::sync::Arc;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

    #[nexus_test(extra_sled_agents = 1)]
    async fn test_ensure_preserve_downgrade_option(
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

        // Fetch the initial CockroachDB settings.
        let settings = datastore
            .cockroachdb_settings(&opctx)
            .await
            .expect("failed to get cockroachdb settings");
        // Fetch the initial blueprint installed during rack initialization.
        let (_blueprint_target, blueprint) = datastore
            .blueprint_target_get_current_full(&opctx)
            .await
            .expect("failed to get blueprint from datastore");
        eprintln!("blueprint: {}", blueprint.display());
        // The initial blueprint should already have the state fingerprint
        // filled in.
        assert_eq!(
            blueprint.cockroachdb_fingerprint,
            settings.state_fingerprint
        );
        // The initial blueprint should already have the preserve downgrade
        // setting filled in. (It might be the current or previous version, but
        // it should be `Set` regardless.)
        let CockroachDbPreserveDowngrade::Set(bp_preserve_downgrade) =
            blueprint.cockroachdb_setting_preserve_downgrade
        else {
            panic!("blueprint does not set preserve downgrade option");
        };
        // The cluster version, preserve downgrade setting, and the value in the
        // blueprint should all match.
        assert_eq!(settings.version, bp_preserve_downgrade.to_string());
        assert_eq!(
            settings.preserve_downgrade,
            bp_preserve_downgrade.to_string()
        );
        // Record the zpools so we don't fail to ensure datasets (unrelated to
        // crdb settings) during blueprint execution.
        let mut disk_test = DiskTest::new(&cptestctx).await;
        disk_test.add_blueprint_disks(&blueprint).await;

        // Execute the initial blueprint.
        let overrides = overridables_for_test(cptestctx);
        _ = realize_blueprint_and_expect(
            &opctx, datastore, resolver, &blueprint, &overrides,
        )
        .await;
        // The CockroachDB settings should not have changed.
        assert_eq!(
            settings,
            datastore
                .cockroachdb_settings(&opctx)
                .await
                .expect("failed to get cockroachdb settings")
        );

        // TODO(iliana): when we upgrade to v22.2, test an actual cluster
        // upgrade when crdb-seed is run with the old CockroachDB
    }
}
