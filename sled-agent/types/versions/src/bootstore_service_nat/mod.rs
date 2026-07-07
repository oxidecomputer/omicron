// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `BOOTSTORE_SERVICE_NAT` of the Sled Agent API.

// Types in `system_networking` were formerly referred to as `early_networking`;
// we're moving away from that term as we remove the dueling implementations
// between Nexus and sled-agent.
pub mod system_networking;
