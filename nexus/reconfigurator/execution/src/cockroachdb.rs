// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ensures CockroachDB cluster settings are set

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
    if let Some(value) = &blueprint.cockroachdb_setting_preserve_downgrade {
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
            "set cluster setting";
            "setting" => "cluster.preserve_downgrade_option",
            "value" => &value,
        );
    }
    Ok(())
}
