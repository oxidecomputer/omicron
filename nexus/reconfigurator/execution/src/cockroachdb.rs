// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ensures CockroachDB settings are set

use anyhow::Context;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use slog::info;

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
                value.clone(),
            )
            .await
            .context("failed to set cluster.preserve_downgrade_option")?;
        info!(
            opctx.log,
            "set cockroachdb setting";
            "setting" => "cluster.preserve_downgrade_option",
            "value" => &value,
        );
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::overridables::Overridables;
    use nexus_db_queries::authn;
    use nexus_db_queries::authz;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::deployment::BlueprintTarget;
    use nexus_types::deployment::CockroachDbClusterVersion;
    use nexus_types::deployment::CockroachDbPreserveDowngrade;
    use std::sync::Arc;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

    #[nexus_test]
    async fn test_ensure_preserve_downgrade_option(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
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
        assert!(blueprint.cockroachdb_fingerprint.is_empty());
        assert_eq!(
            blueprint.cockroachdb_setting_preserve_downgrade,
            CockroachDbPreserveDowngrade::DoNotModify
        );
        // Execute the initial blueprint.
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
        // The CockroachDB settings should not have changed.
        assert_eq!(
            settings,
            datastore
                .cockroachdb_settings(&opctx)
                .await
                .expect("failed to get cockroachdb settings")
        );

        // Create and add a new blueprint (this will pull the current
        // CockroachDB settings and run the planner based on it).
        let blueprint2 = nexus
            .blueprint_create_regenerate(&opctx)
            .await
            .expect("failed to regenerate blueprint");
        eprintln!("blueprint: {}", blueprint2.display());
        // The new blueprint should have a matching fingerprint, and a correct
        // non-empty value for the preserve downgrade option.
        assert_eq!(
            blueprint2.cockroachdb_fingerprint,
            settings.state_fingerprint
        );
        assert_eq!(
            blueprint2.cockroachdb_setting_preserve_downgrade,
            CockroachDbClusterVersion::NEWLY_INITIALIZED.into(),
        );
        // Set the new blueprint as the target and execute it.
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
        .expect("failed to execute new blueprint");
        // Fetch the new CockroachDB settings.
        let settings2 = datastore
            .cockroachdb_settings(&opctx)
            .await
            .expect("failed to get cockroachdb settings");
        // The cluster version should not have changed.
        assert_eq!(settings.version, settings2.version);
        // The current "preserve downgrade option" setting should be set.
        assert_eq!(
            settings2.preserve_downgrade,
            CockroachDbClusterVersion::NEWLY_INITIALIZED.to_string()
        );
    }
}
