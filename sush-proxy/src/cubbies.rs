// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ask MGS which sled occupies each cubby.

use std::collections::BTreeMap;

use gateway_client::Client as MgsClient;
use gateway_client::Error as MgsError;
use gateway_client::types;
use gateway_types::component::SpType;
use gateway_types::ignition::SpIgnition;
use sled_hardware_types::BaseboardId;
use slog::{Logger, debug};
use sush_common::targets::{Cubbies, MAX_CUBBY};

/// What one MGS poll reports about the sled in a cubby.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SledState {
    /// MGS found no sled in the cubby.
    Absent,

    /// MGS found a sled with this baseboard ID.
    Present(BaseboardId),

    /// MGS found a sled but could not identify it.
    Unknown,
}

/// Ask `mgs` about the sled in each cubby and return a map of the results,
/// or the error if `mgs` cannot list the SPs.
pub async fn poll_cubbies(
    log: &Logger,
    mgs: &MgsClient,
) -> Result<BTreeMap<u8, SledState>, MgsError<types::Error>> {
    let ignition = mgs.ignition_list().await?.into_inner();

    let mut states = BTreeMap::new();
    for sp in
        ignition.into_iter().filter(|sp| matches!(sp.id.typ, SpType::Sled))
    {
        let Some(cubby) =
            u8::try_from(sp.id.slot).ok().filter(|cubby| *cubby <= MAX_CUBBY)
        else {
            debug!(log, "invalid cubby number"; "slot" => %sp.id.slot);
            continue;
        };
        match sp.details {
            SpIgnition::Absent => {
                states.insert(cubby, SledState::Absent);
            }
            SpIgnition::Present { power, .. } if power => {
                match mgs.sp_get(&sp.id.typ, sp.id.slot).await {
                    Ok(state) => {
                        let state = state.into_inner();
                        let baseboard = BaseboardId {
                            part_number: state.model,
                            serial_number: state.serial_number,
                        };
                        states.insert(cubby, SledState::Present(baseboard));
                    }
                    Err(err) => {
                        debug!(
                            log,
                            "no SP state for cubby";
                            "cubby" => cubby,
                            "error" => %err,
                        );
                        states.insert(cubby, SledState::Unknown);
                    }
                }
            }
            SpIgnition::Present { .. } => {
                states.insert(cubby, SledState::Unknown);
            }
        }
    }
    Ok(states)
}

/// Merge one round of sled states into an existing cubby map. If a cubby
/// is not reported in the new round, it keeps any existing entry. An
/// `Absent` or `Unknown` cubby loses its entry, since its old baseboard
/// may no longer be the one in the cubby. Duplicate baseboards are
/// handled by taking the newer entry, so a sled that moves or swaps places
/// appears only once.
pub fn merge_cubbies(cubbies: &mut Cubbies, states: BTreeMap<u8, SledState>) {
    for (cubby, state) in states {
        match state {
            SledState::Absent | SledState::Unknown => {
                cubbies.remove(&cubby);
            }
            SledState::Present(baseboard) => {
                cubbies.retain(|_, b| *b != baseboard);
                cubbies.insert(cubby, baseboard);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn baseboard(n: u32) -> BaseboardId {
        BaseboardId {
            part_number: "913-0000019".into(),
            serial_number: format!("BRM{n:08}"),
        }
    }

    fn present(n: u32) -> SledState {
        SledState::Present(baseboard(n))
    }

    #[test]
    fn merged_cubbies_survive_outages_and_moves() {
        // Start with sled 1 in cubby 14, sled 2 in cubby 15.
        let mut cubbies =
            Cubbies::from([(14, baseboard(1)), (15, baseboard(2))]);

        // A quiet round doesn't erase anything.
        merge_cubbies(&mut cubbies, BTreeMap::new());
        assert_eq!(
            cubbies,
            Cubbies::from([(14, baseboard(1)), (15, baseboard(2))])
        );

        // Sled 1 moves from cubby 14 to 20. Sled 2 misses the round
        // but keeps its entry.
        merge_cubbies(&mut cubbies, BTreeMap::from([(20, present(1))]));
        assert_eq!(
            cubbies,
            Cubbies::from([(15, baseboard(2)), (20, baseboard(1))])
        );

        // Sled 2 is swapped out for sled 3 in cubby 15.
        merge_cubbies(&mut cubbies, BTreeMap::from([(15, present(3))]));
        assert_eq!(
            cubbies,
            Cubbies::from([(15, baseboard(3)), (20, baseboard(1))])
        );

        // Sled 1's SP stops answering, so cubby 20 loses its entry.
        merge_cubbies(&mut cubbies, BTreeMap::from([(20, SledState::Unknown)]));
        assert_eq!(cubbies, Cubbies::from([(15, baseboard(3))]));

        // Sled 3 is pulled, leaving cubby 15 empty.
        merge_cubbies(&mut cubbies, BTreeMap::from([(15, SledState::Absent)]));
        assert_eq!(cubbies, Cubbies::new());
    }
}
