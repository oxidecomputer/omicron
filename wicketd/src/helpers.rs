// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers and utility functions for wicketd.

use std::fmt;

use itertools::Itertools;
use sled_hardware_types::Baseboard;
use wicket_common::inventory::{SpIdentifier, SpState, SpType};

/// Return true if the provided baseboard matches the provided `SpState`.
///
/// This currently checks for equality on all of part number (model), serial
/// number, and revision. In the future, following the definition of
/// `BaseboardId`, we may change this to only match on the part number and
/// serial number.
pub(crate) fn baseboard_matches_sp_state(
    baseboard: &Baseboard,
    state: &SpState,
) -> bool {
    baseboard.model() == state.model
        && baseboard.identifier() == state.serial_number
        && baseboard.revision() == state.revision
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub(crate) struct SpIdentifierDisplay(pub(crate) SpIdentifier);

impl From<SpIdentifier> for SpIdentifierDisplay {
    fn from(id: SpIdentifier) -> Self {
        SpIdentifierDisplay(id)
    }
}

impl<'a> From<&'a SpIdentifier> for SpIdentifierDisplay {
    fn from(id: &'a SpIdentifier) -> Self {
        SpIdentifierDisplay(*id)
    }
}

impl fmt::Display for SpIdentifierDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.typ {
            SpType::Sled => write!(f, "sled {}", self.0.slot),
            SpType::Switch => write!(f, "switch {}", self.0.slot),
            SpType::Power => write!(f, "PSC {}", self.0.slot),
        }
    }
}

pub(crate) fn sps_to_string<S: Into<SpIdentifierDisplay>>(
    sps: impl IntoIterator<Item = S>,
) -> String {
    sps.into_iter().map_into().join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    use gateway_types::component::PowerState;
    use wicket_common::inventory::RotState;

    fn sp_state(model: &str, serial_number: &str, revision: u32) -> SpState {
        SpState {
            model: model.to_string(),
            serial_number: serial_number.to_string(),
            revision,
            hubris_archive_id: "archive".to_string(),
            base_mac_address: [0; 6],
            power_state: PowerState::A0,
            rot: RotState::CommunicationFailed {
                message: "rot down".to_string(),
            },
        }
    }

    const MODEL: &str = "gimlet";
    const SERIAL: &str = "BRM123";
    const REVISION: u32 = 4;

    fn baseboard() -> Baseboard {
        // Note that Baseboard::new_gimlet takes the serial number first, which
        // is the opposite of BaseboardId's order (model/part number first).
        Baseboard::new_gimlet(SERIAL.to_string(), MODEL.to_string(), REVISION)
    }

    #[test]
    fn baseboard_matches_only_when_all_three_fields_agree() {
        assert!(
            baseboard_matches_sp_state(
                &baseboard(),
                &sp_state(MODEL, SERIAL, REVISION)
            ),
            "the same model, serial number, and revision is a match",
        );

        // Any of the three fields being different is a reason to say no.
        for (model, serial, revision, what) in [
            ("cosmo", SERIAL, REVISION, "a different model"),
            (MODEL, "BRM999", REVISION, "a different serial number"),
            (MODEL, SERIAL, 5, "a different revision"),
        ] {
            assert!(
                !baseboard_matches_sp_state(
                    &baseboard(),
                    &sp_state(model, serial, revision)
                ),
                "{what} is not a match",
            );
        }
    }

    #[test]
    fn unknown_baseboard_matches_its_own_placeholder_identity() {
        // This shows that `Baseboard::Unknown` reports "unknown"/"unknown"/0
        // rather than nothing. This is probably wrong and should be replaced
        // with BaseboardId in the future.
        assert!(baseboard_matches_sp_state(
            &Baseboard::unknown(),
            &sp_state("unknown", "unknown", 0)
        ));
        assert!(!baseboard_matches_sp_state(
            &Baseboard::unknown(),
            &sp_state(MODEL, SERIAL, REVISION)
        ));
    }
}
