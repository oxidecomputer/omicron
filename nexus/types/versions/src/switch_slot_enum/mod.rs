// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `SWITCH_SLOT_ENUM` of the Nexus external API.
//!
//! Changes several external API types that had `switch` or `switch_location`
//! fields of type `Name` or `String` (but which were, in practice, required to
//! be exactly "switch0" or "switch1") to `SwitchSlot`.

pub mod bfd;
pub mod networking;

// Private helper functions for converting between the old API types to and from
// `SwitchSlot`. These should only be used by `From` and `TryFrom` impls defined
// in our submodules.

use omicron_common::api::external::Error;
use omicron_common::api::external::Name;
use sled_agent_types::early_networking::SwitchSlot;

fn parse_str_as_switch_slot(switch_slot: &str) -> Result<SwitchSlot, Error> {
    match switch_slot {
        "switch0" => Ok(SwitchSlot::Switch0),
        "switch1" => Ok(SwitchSlot::Switch1),
        _ => Err(Error::invalid_request(format!(
            "invalid switch location `{switch_slot}` \
             (expected `switch0` or `switch1`)",
        ))),
    }
}

fn format_switch_slot_as_str(switch_slot: SwitchSlot) -> &'static str {
    match switch_slot {
        SwitchSlot::Switch0 => "switch0",
        SwitchSlot::Switch1 => "switch1",
    }
}

fn format_switch_slot_as_name(switch_slot: SwitchSlot) -> Name {
    format_switch_slot_as_str(switch_slot)
        .parse()
        .expect("switch slots form valid `Name`s")
}
