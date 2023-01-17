// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use super::current_simulator_state;
use super::setup;
use super::SpStateExt;
use dropshot::test_util;
use dropshot::Method;
use gateway_messages::SpPort;
use http::StatusCode;
use omicron_gateway::http_entrypoints::SpIdentifier;
use omicron_gateway::http_entrypoints::SpIgnition;
use omicron_gateway::http_entrypoints::SpInfo;
use omicron_gateway::http_entrypoints::SpState;
use omicron_gateway::http_entrypoints::SpType;
use sp_sim::Responsiveness;
use sp_sim::SimulatedSp;
use std::collections::BTreeSet;

// macro to compare two iterable "things" (typically slice or vec) that can be
// collected into `BTreeSet`s to do comparisons that ignore order in the
// original "thing"
macro_rules! assert_eq_unordered {
    ($items0:expr, $items1:expr) => {
        assert_eq!(
            $items0.iter().collect::<BTreeSet<_>>(),
            $items1.iter().collect::<BTreeSet<_>>(),
        );
    };
}

#[tokio::test]
async fn bulk_sp_get_all_online() {
    let testctx =
        setup::test_setup("bulk_sp_get_all_online", SpPort::One).await;
    let client = &testctx.client;

    // simulator just started; all SPs are online
    let expected = current_simulator_state(&testctx.simrack).await;

    // sanity check: we have at least 1 sidecar and at least 1 gimlet, and all
    // SPs are enabled
    assert!(expected.iter().any(|sp| sp.info.id.typ == SpType::Switch));
    assert!(expected.iter().any(|sp| sp.info.id.typ == SpType::Sled));
    assert!(expected.iter().all(|sp| sp.details.is_enabled()));

    let url = format!("{}", client.url("/sp"));

    let sps: Vec<SpInfo> = test_util::object_get(client, &url).await;

    assert_eq_unordered!(sps, expected);

    testctx.teardown().await;
}

#[tokio::test]
async fn bulk_sp_get_one_sp_powered_off() {
    let testctx =
        setup::test_setup("bulk_sp_get_all_online", SpPort::One).await;
    let client = &testctx.client;

    // simulator just started; all SPs are online
    let mut expected = current_simulator_state(&testctx.simrack).await;

    // sanity check: we have at least 1 sidecar and at least 1 gimlet, and all
    // SPs are enabled
    assert!(expected.iter().any(|sp| sp.info.id.typ == SpType::Switch));
    assert!(expected.iter().any(|sp| sp.info.id.typ == SpType::Sled));
    assert!(expected.iter().all(|sp| sp.details.is_enabled()));

    // power off sled 0 (guaranteed to exist via the assertion above)
    let url = format!("{}", client.url("/ignition/sled/0/power-off"));
    client
        .make_request_no_body(Method::POST, &url, StatusCode::NO_CONTENT)
        .await
        .unwrap();

    // update our expected state for sled 0 to `Disabled` since we just powered
    // it off
    for sp in &mut expected {
        if sp.info.id == (SpIdentifier { typ: SpType::Sled, slot: 0 }) {
            // TODO maybe extract into a `toggle_power()` helper?
            sp.info.details = match sp.info.details {
                SpIgnition::Absent | SpIgnition::CommunicationFailed { .. } => {
                    panic!("bad ignition state")
                }
                SpIgnition::Present {
                    id,
                    power: _power,
                    ctrl_detect_0,
                    ctrl_detect_1,
                    flt_a3,
                    flt_a2,
                    flt_rot,
                    flt_sp,
                } => SpIgnition::Present {
                    id,
                    power: false,
                    ctrl_detect_0,
                    ctrl_detect_1,
                    flt_a3,
                    flt_a2,
                    flt_rot,
                    flt_sp,
                },
            };
        }
    }

    let url = format!("{}", client.url("/sp"));
    let sps: Vec<SpInfo> = test_util::object_get(client, &url).await;

    assert_eq_unordered!(sps, expected);

    testctx.teardown().await;
}

#[tokio::test]
async fn bulk_sp_get_one_sp_unresponsive() {
    let testctx =
        setup::test_setup("bulk_sp_get_all_online", SpPort::One).await;
    let client = &testctx.client;

    // simulator just started; all SPs are online
    let mut expected = current_simulator_state(&testctx.simrack).await;

    // sanity check: we have at least 1 sidecar and at least 1 gimlet, and all
    // SPs are enabled
    assert!(expected.iter().any(|sp| sp.info.id.typ == SpType::Switch));
    assert!(expected.iter().any(|sp| sp.info.id.typ == SpType::Sled));
    assert!(expected.iter().all(|sp| sp.details.is_enabled()));

    // instruct simulator to disable sled 0
    testctx.simrack.gimlets[0]
        .set_responsiveness(Responsiveness::Unresponsive)
        .await;

    // What error message do we expect from MGS if an SP has previously been
    // found but is no longer responsive? Hard coding the exact message here
    // feels a little fragile, but (a) the number of attempts present in this
    // string is in our test control (our test config file) and (b) should not
    // change much.
    let unresponsive_errmsg = "error communicating with SP: RPC call failed (gave up after 3 attempts)".to_string();

    // Set sled 0 expected state to timeout
    expected
        .iter_mut()
        .find(|sp| sp.info.id == SpIdentifier { typ: SpType::Sled, slot: 0 })
        .unwrap()
        .details =
        SpState::CommunicationFailed { message: unresponsive_errmsg };

    let url = format!("{}", client.url("/sp"));
    let sps: Vec<SpInfo> = test_util::object_get(client, &url).await;
    assert_eq_unordered!(sps, expected);

    testctx.teardown().await;
}
