// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A li'l dingus for collecting a snapshot of fault management state.

use anyhow::Context;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;

pub use nexus_types::fm::analysis_reports::*;

pub struct SnapshotParams {
    pub requested_sitrep_id: Option<SitrepUuid>,
    pub max_historical_sitreps: usize,
}

pub fn snapshot(
    opctx: &OpContext,
    datastore: &DataStore,
    params: &SnapshotParams,
) -> anyhow::Result<FmStateReport> {
    // We are about to read A Whole Bunch of Stuff. Make sure that's oaky
    opctx.check_complex_operations_allowed()?;

    let (current_version, current_sitrep) =
        datastore.fm_sitrep_read_current(opctx).await?;
    let current_config = datastore
        .fm_config_get_latest(opctx)
        .await?
        .map_or_else(PlannerConfig::default, |c| c.config.planner_config);

    todo!("eliza: draw the rest of the owl")
}
