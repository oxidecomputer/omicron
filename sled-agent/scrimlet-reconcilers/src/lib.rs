// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This crate implements long-running reconciler tasks responsible for
//! configuration of services within a scrimlet's switch zone.
//!
//! These tasks _only_ talk to services on the same sled as the sled-agent
//! executing these tasks; we attempt to ensure this at runtime via types like
//! [`ThisSledSwitchZoneUnderlayIpAddr`]. A scrimlet running these tasks should
//! never attempt to talk to another scrimlet's switch zone, and a non-scrimlet
//! running these tasks should never attempt to talk to anything. (Non-scrimlet
//! sleds still create a [`ScrimletReconcilers`] handle, as sled-agent can't
//! easily tell the difference between "not a scrimlet because we're in a
//! different cubby" and "not a scrimlet because we should have a switch but it
//! isn't connected / isn't powered on / etc.", but the reconciliation tasks are
//! only spawned under conditions that only scrimlets can satisfy; see
//! [`ScrimletReconcilers`] for more detail.)
//!
//! These tasks are responsible for applying system-level networking, including:
//!
//! * Configuration of uplink ports within `dpd`
//! * Configuration of NAT entries for system-level services (boundary NTP,
//!   Nexus, external DNS - notably _not_ instances) within `dpd`
//! * Configuration of BGP within `mgd`
//! * Configuration of BFD within `mgd`
//! * Configuration of static routes within `mgd`
//! * Configuration of SMF properties for `uplinkd` and `lldpd`
//!
//! The specific configuration that should be applied comes from Nexus (or RSS,
//! at rack setup time) and is sent to `sled-agent` via the bootstore.
//!
//! In the past, responsibility for this configuration was split: sled-agent was
//! responsible for applying an initial config on sled boot (required for cold
//! boot of the rack), and Nexus was responsible for continuously keeping the
//! config in sync afterwards. This had a variety of problems; see
//! <https://github.com/oxidecomputer/omicron/issues/10167>. The split is now
//! that Nexus is responsible for maintaining what the configuration should be,
//! and each scrimlet is responsible for applying that configuration to its own
//! switch zone's services; the latter is implemented via this crate.
//!
//! [`ThisSledSwitchZoneUnderlayIpAddr`]:
//! sled_agent_types::sled::ThisSledSwitchZoneUnderlayIpAddr

mod dpd_reconciler;
mod handle;
mod lldpd_reconciler;
mod mgd_reconciler;
mod reconciler_task;
mod status;
mod switch_zone_slot;
mod uplinkd_reconciler;

pub use bootstrap_agent_lockstep_types::scrimlet_reconcilers::ScrimletStatus;

pub use handle::ScrimletReconcilers;
pub use handle::ScrimletReconcilersMode;
pub use handle::SledAgentNetworkingInfo;

// TODO-cleanup The SMF-based reconcilers need to know the name of the switch
// zone. This is a little spread out today: `illumos-utils` defines
// `zone_name()`, but calling it for the switch zone is a little awkward
// (`zone_name("switch", None)`) when all it does is prepend `oxz_`. We define a
// constant here, but maybe there's a better option? We have a unit test below
// to ensure this matches what `illumos-utils` would report.
const SWITCH_ZONE_NAME: &str = "oxz_switch";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_switch_zone_name_const_matches_illumos_utils_dynamic() {
        let expected = illumos_utils::zone::zone_name("switch", None);
        assert_eq!(SWITCH_ZONE_NAME, expected);
    }
}
