// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multirack regional cluster API implementation code.

use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::multirack::UnadoptedRack;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackUuid;
use uuid::Uuid;

impl super::Nexus {
    /// List racks that have been discovered via inventory but which have not
    /// yet been adopted into this regional cluster.
    ///
    /// Results are sorted by rack ID in ascending order, and `pagparams` is
    /// applied to that ordering.
    pub(crate) async fn rack_list_unadopted(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<UnadoptedRack> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        // Note that this API is sourced from the inventory system, so it may
        // take some time between when a new rack joins the underlay network and
        // when that rack appears in this API output, since we won't be able to
        // see it here until the inventory collection is refreshed. However, the
        // results returned by this API *are* filtered so that racks which have
        // joined the cluster do not appear in the response, hence the
        // "unadopted" naming scheme instead of the "discovered" naming scheme
        // used in the corresponding inventory data.

        // First grab the latest inventory collection:
        let coll = self.inventory_load_rx().borrow().clone();

        let Some(coll) = coll.filter(|c| !c.discovered_racks.is_empty()) else {
            // If we don't have any discovered racks in inventory (or haven't
            // collected inventory at all yet, in the None case), we don't need
            // to bother filtering out anything and can save a DB query and some
            // other work by just short circuiting out here.
            return Ok(Vec::new());
        };

        // Next, get all existing adopted rack IDs from the `rack` table so
        // that we know which racks we should filter out of the discovered
        // rack list due to them already having been adopted.
        //
        // We may want to properly paginate this query at some point instead
        // of just using [`DataPageParams::max_page()`], but that may be
        // overkill considering we're only grabbing IDs and there is an
        // upper bound of how many racks can be in a single regional
        // cluster.
        let adopted_rack_ids = self
            .datastore()
            .rack_id_list(opctx, &DataPageParams::max_page())
            .await?;

        // Finally, use the pagination parameters to determine the correct
        // subset of the unadopted racks to return:
        let last_seen =
            pagparams.marker.map(|uuid| RackUuid::from_untyped_uuid(*uuid));
        let limit = usize::try_from(u32::from(pagparams.limit))
            .expect("u32 should fit in usize on all supported platforms");

        Ok(coll
            .discovered_racks
            .iter()
            .filter(|rack| !adopted_rack_ids.contains(&rack.rack_id))
            .filter(|rack| Some(rack.rack_id) > last_seen)
            .take(limit)
            .cloned()
            .map(UnadoptedRack::from)
            .collect())
    }
}
