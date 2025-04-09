// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sets where oximeter reads from as per the oximter read policy

use anyhow::Context;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
// use nexus_types::deployment::Blueprint;
use slog::info;

pub(crate) async fn reroute_reads(
    opctx: &OpContext,
    datastore: &DataStore,
    // blueprint: &Blueprint,
) -> anyhow::Result<()> {

    // TODO-K: Instead of checking the database should I get it from the blueprint?
    // let policy = blueprint.oximeter_read_policy

    let oximeter_policy = datastore
        .oximeter_read_policy_get_latest(opctx)
        .await
        .context("failed to retrieve oximeter read policy")?;

    info!(opctx.log, "OXIMETER POLICY {:?}", oximeter_policy);
    println!("OXIMETER POLICY {:?}", oximeter_policy);

    Ok(())
}
