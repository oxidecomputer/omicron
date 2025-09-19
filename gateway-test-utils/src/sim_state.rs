// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use gateway_messages::ignition::SystemPowerState;
use gateway_messages::ignition::SystemType;
use gateway_types::component::SpIdentifier;
use gateway_types::component::SpState;
use gateway_types::component::SpType;
use gateway_types::ignition::SpIgnitionInfo;
use sp_sim::Gimlet;
use sp_sim::SimRack;
use sp_sim::SimulatedSp;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

pub struct SpInfo {
    pub ignition: SpIgnitionInfo,
    pub state: Result<SpState, String>,
}

/// Query the running simulator and translate it into an ordered list of `SpInfo`.
pub async fn current_simulator_state(simrack: &SimRack) -> Vec<SpInfo> {
    let sim_state =
        simrack.ignition_controller().current_ignition_state().await;

    let mut all_sps: Vec<SpInfo> = Vec::new();
    let mut slot = 0;
    for state in sim_state {
        let Some(target_state) = state.target else {
            continue;
        };
        let typ = match target_state.system_type {
            SystemType::Sidecar => SpType::Switch,
            SystemType::Gimlet | SystemType::Cosmo => SpType::Sled,
            SystemType::Psc => {
                todo!("testing simulated PSC not yet implemented")
            }
            SystemType::Unknown(id) => {
                panic!("unknown ignition id ({id}) not implemented in tests")
            }
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
            if prev_info.ignition.id.typ != typ { 0 } else { slot + 1 }
        });

        let sp: &dyn SimulatedSp = match typ {
            SpType::Switch => &simrack.sidecars[slot as usize],
            SpType::Sled => &simrack.gimlets[slot as usize],
            SpType::Power => todo!(),
        };

        let details =
            if matches!(target_state.power_state, SystemPowerState::On) {
                Ok(sp.state().await)
            } else {
                Err("powered off".to_string())
            };

        all_sps.push(SpInfo {
            ignition: SpIgnitionInfo {
                id: SpIdentifier { typ, slot },
                details: state.into(),
            },
            state: details,
        });
    }

    all_sps
}

/// simulated gimlets expose their serial console over TCP; this function spawns
/// a task to read/write on the serial console, returning channels for the caller
pub async fn sim_sp_serial_console(
    gimlet: &Gimlet,
) -> (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) {
    let mut conn =
        TcpStream::connect(gimlet.serial_console_addr("sp3-host-cpu").unwrap())
            .await
            .unwrap();

    let (console_write, mut console_write_rx) = mpsc::channel::<Vec<u8>>(16);
    let (console_read_tx, console_read) = mpsc::channel::<Vec<u8>>(16);
    tokio::spawn(async move {
        let mut buf = [0; 128];
        loop {
            tokio::select! {
                to_write = console_write_rx.recv() => {
                    match to_write {
                        Some(data) => {
                            conn.write_all(&data).await.unwrap();
                        }
                        None => return,
                    }
                }

                read_success = conn.read(&mut buf) => {
                    let n = read_success.unwrap();
                    console_read_tx.send(buf[..n].to_vec()).await.unwrap();
                }
            }
        }
    });

    (console_write, console_read)
}
