// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use super::setup;
use dropshot::test_util;
use dropshot::Method;
use dropshot::ResultsPage;
use gateway_messages::IgnitionFlags;
use http::StatusCode;
use omicron_gateway::http_entrypoints::SpIdentifier;
use omicron_gateway::http_entrypoints::SpIgnition;
use omicron_gateway::http_entrypoints::SpIgnitionInfo;
use omicron_gateway::http_entrypoints::SpInfo;
use omicron_gateway::http_entrypoints::SpState;
use omicron_gateway::http_entrypoints::SpType;
use sp_sim::ignition_id;
use sp_sim::Responsiveness;
use sp_sim::SimRack;
use sp_sim::SimulatedSp;
use std::collections::BTreeSet;

// Light TODO: Some of these helper functions are presumably going to be useful
// in other tests; move them to a common utility area when appropriate.

trait SpStateExt {
    fn is_enabled(&self) -> bool;
}

impl SpStateExt for SpState {
    fn is_enabled(&self) -> bool {
        match self {
            SpState::Enabled { .. } => true,
            SpState::Disabled | SpState::Unresponsive => false,
        }
    }
}

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

// query the running simulator and translate it into an ordered list of `SpInfo`
async fn current_simulator_state(simrack: &SimRack) -> Vec<SpInfo> {
    let sim_state =
        simrack.ignition_controller().current_ignition_state().await;

    let mut all_sps: Vec<SpInfo> = Vec::new();
    let mut slot = 0;
    for state in sim_state {
        let typ = match state.id {
            ignition_id::SIDECAR => SpType::Switch,
            ignition_id::GIMLET => SpType::Sled,
            // TODO ignition_id::POWER_SHELF_CONTROLLER
            other => panic!("unknown ignition ID {:#x}", other),
        };

        // we assume the simulator ignition state is grouped by type and ordered
        // by slot within each type; if we just switched to a new type, reset to
        // slot 0.
        //
        // this might warrant more thought / sim API, or maybe not since this is
        // just tests; we can probably keep this constraint in the simulator
        // setup.
        slot = all_sps.last().map_or(0, |prev_info| {
            // if the type changed, reset to slot 0; otherwise increment
            if prev_info.info.id.typ != typ {
                0
            } else {
                slot + 1
            }
        });

        let sp: &dyn SimulatedSp = match typ {
            SpType::Switch => &simrack.sidecars[slot as usize],
            SpType::Sled => &simrack.gimlets[slot as usize],
            SpType::Power => todo!(),
        };

        let details = if state.flags.intersects(IgnitionFlags::POWER) {
            SpState::Enabled { serial_number: sp.serial_number() }
        } else {
            SpState::Disabled
        };

        all_sps.push(SpInfo {
            info: SpIgnitionInfo {
                id: SpIdentifier { typ, slot },
                details: state.into(),
            },
            details,
        });
    }

    all_sps
}

#[tokio::test]
async fn bulk_sp_get_all_online() {
    let testctx = setup::test_setup("bulk_sp_get_all_online").await;
    let client = &testctx.client;

    // simulator just started; all SPs are online
    let expected = current_simulator_state(&testctx.simrack).await;

    // sanity check: we have at least 1 sidecar and at least 1 gimlet, and all
    // SPs are enabled
    assert!(expected.iter().any(|sp| sp.info.id.typ == SpType::Switch));
    assert!(expected.iter().any(|sp| sp.info.id.typ == SpType::Sled));
    assert!(expected.iter().all(|sp| sp.details.is_enabled()));

    let url = format!("{}", client.url("/sp"));

    let page: ResultsPage<SpInfo> =
        test_util::objects_list_page(client, &url).await;

    assert_eq_unordered!(page.items, expected);

    testctx.teardown().await;
}

#[tokio::test]
async fn bulk_sp_get_one_sp_powered_off() {
    let testctx = setup::test_setup("bulk_sp_get_all_online").await;
    let client = &testctx.client;

    // simulator just started; all SPs are online
    let mut expected = current_simulator_state(&testctx.simrack).await;

    // sanity check: we have at least 1 sidecar and at least 1 gimlet, and all
    // SPs are enabled
    assert!(expected.iter().any(|sp| sp.info.id.typ == SpType::Switch));
    assert!(expected.iter().any(|sp| sp.info.id.typ == SpType::Sled));
    assert!(expected.iter().all(|sp| sp.details.is_enabled()));

    // power off sled 0 (guaranteed to exist via the assertion above)
    let url = format!("{}", client.url("/sp/sled/0/power_off"));
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
                SpIgnition::Absent => panic!("bad ignition state"),
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
            sp.details = SpState::Disabled;
        }
    }

    let url = format!("{}", client.url("/sp"));
    let page: ResultsPage<SpInfo> =
        test_util::objects_list_page(client, &url).await;

    assert_eq_unordered!(page.items, expected);

    testctx.teardown().await;
}

#[tokio::test]
async fn bulk_sp_get_one_sp_unresponsive() {
    let testctx = setup::test_setup("bulk_sp_get_all_online").await;
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

    // With an unresponsive SP, we should get back an initial page containing
    // the responsive SPs, then the subsequent page will eventually give us the
    // unresponsive one. Remove it from `expected` and set it to unresponsive.
    let sled_0_index = expected
        .iter()
        .position(|sp| {
            sp.info.id == SpIdentifier { typ: SpType::Sled, slot: 0 }
        })
        .unwrap();
    let mut expected_sled_0 = expected.remove(sled_0_index);
    expected_sled_0.details = SpState::Unresponsive;

    let url = format!("{}", client.url("/sp"));
    let page: ResultsPage<SpInfo> =
        test_util::objects_list_page(client, &url).await;
    assert_eq_unordered!(page.items, expected);

    // get the subsequent page, which should tell us about the unresponsive SP
    let url = format!("{}?page_token={}", url, page.next_page.unwrap());
    let page: ResultsPage<SpInfo> =
        test_util::objects_list_page(client, &url).await;
    assert_eq_unordered!(page.items, [expected_sled_0]);

    testctx.teardown().await;
}
