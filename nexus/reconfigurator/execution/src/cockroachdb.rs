// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ensures CockroachDB cluster settings are set

use anyhow::Context;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use slog::info;

pub(crate) async fn ensure_cluster_settings(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
) -> anyhow::Result<()> {
    datastore
        .cluster_setting_preserve_downgrade(
            opctx,
            blueprint.cockroachdb_preserve_downgrade.clone(),
        )
        .await
        .context("failed to set cluster.preserve_downgrade_option")?;
    info!(
        opctx.log,
        "set cluster setting";
        "setting" => "cluster.preserve_downgrade_option",
        "value" => &blueprint.cockroachdb_preserve_downgrade,
    );
    Ok(())
}
