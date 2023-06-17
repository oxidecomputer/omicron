// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Actions for state stateful property based tests.

use super::network::FlowId;
use bootstore::schemes::v0::Ticks;
use sled_hardware::Baseboard;
use std::collections::BTreeSet;
use uuid::Uuid;

// Certain operations take different amounts of time to complete, and messages
// take some duration to deliver. We can parameterize how long operations
// and message delivery take, without being overly prescriptive by adopting a
// certain tick behavior at each point in the test run.
//
// While we could get complex and map different delivery times to different
// network flows, and have operations take different amounts of time at
// different sleds, we keep things relatively simple for now by having the tick
// behavior affect all flows and sleds equally.
#[derive(Debug)]
pub struct Delays {
    // The time to send a message from source to destination
    pub msg_delivery: Ticks,
    // The time for a receiver to process a message and return a share to the
    // requester or the requester to receive a share and store it in memory.
    pub share_processing: Ticks,
    // The time for a sled to compute the rack secret given enough shares
    pub rack_secret_computation: Ticks,
}

impl Default for Delays {
    fn default() -> Self {
        Delays {
            msg_delivery: 1,
            share_processing: 0,
            rack_secret_computation: 1,
        }
    }
}

/// A test action to drive the test forward
#[derive(Debug)]
pub enum Action {
    RackInit {
        rss_sled: Baseboard,
        rack_uuid: Uuid,
        initial_members: BTreeSet<Baseboard>,
    },
    //    ChangeDelays(Delays),
    Ticks(Ticks),
    //SledUnlock(Baseboard),

    // TODO: Generate these variants
    Connect(Vec<FlowId>),
    Disconnect(Vec<FlowId>),
}
