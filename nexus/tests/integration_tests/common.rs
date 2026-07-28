// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils::background::wait_for_all_volume_deletes;
use nexus_test_utils::resource_helpers::DiskTest;

pub(crate) async fn assert_all_crucible_resources_deleted<'a>(
    cptestctx: &ControlPlaneTestContext<omicron_nexus::Server>,
    disk_test: &DiskTest<'a, omicron_nexus::Server>,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let lockstep_client = &cptestctx.lockstep_client;
    wait_for_all_volume_deletes(&datastore, &lockstep_client).await;
    assert!(disk_test.crucible_resources_deleted().await);
}
