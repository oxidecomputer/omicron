// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use gateway_messages::IgnitionFlags;
use omicron_gateway::http_entrypoints::SpIdentifier;
use omicron_gateway::http_entrypoints::SpIgnitionInfo;
use omicron_gateway::http_entrypoints::SpInfo;
use omicron_gateway::http_entrypoints::SpState;
use omicron_gateway::http_entrypoints::SpType;
use sp_sim::ignition_id;
use sp_sim::SimRack;
use sp_sim::SimulatedSp;

mod bulk_state_get;
mod commands;
mod component_list;
mod location_discovery;
mod serial_console;
mod setup;

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
