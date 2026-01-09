// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod common;

use std::fmt::Pointer;

use anyhow::Context;
use common::LiveTestContext;
use live_tests_macros::live_test;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;

#[live_test]
async fn test_trust_quorum_add_sled(lc: &LiveTestContext) {
    // Test setup
    let log = lc.log();
    let opctx = lc.opctx();
    let datastore = lc.datastore();
    let initial_nexus_clients = lc.all_internal_nexus_clients().await.unwrap();
}
