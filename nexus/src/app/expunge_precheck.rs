// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Checking for conditions that make it risky to expunge a working sled or
//! physical disk
//!
//! All of this should presumably be subsumed by Fault Management / Active
//! Problems.

use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::expunge_precheck::ExpungePrecheck;
use omicron_common::api::external::Error;

mod control_plane;
mod crucible;

impl super::Nexus {
    /// Checks whether expunging a working sled or physical disk would be risky
    /// because the system is still recovering from previous expungements or
    /// failures
    pub(crate) async fn expunge_precheck(
        &self,
        opctx: &OpContext,
    ) -> Result<ExpungePrecheck, Error> {
        let datastore = self.datastore();

        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        let distributed_storage = crucible::check(opctx, datastore).await?;

        // Clone these out of their watch channels so that we don't hold the
        // channels' locks while querying the database.
        let planner_latest = self.update_status.latest_planner();
        let target_blueprint = self
            .update_status
            .latest_blueprint()
            .map(|loaded| loaded.blueprint);
        let inventory = self.inventory_load_rx().borrow().clone();
        let control_plane = control_plane::check(
            opctx,
            datastore,
            planner_latest,
            target_blueprint,
            inventory,
        )
        .await?;

        Ok(ExpungePrecheck::new(distributed_storage, control_plane))
    }
}
