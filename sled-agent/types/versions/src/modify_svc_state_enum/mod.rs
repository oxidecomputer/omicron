// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `MODIFY_SVC_STATE_ENUM` of the Sled Agent API.
//!
//! This version adds `InTransition` and `Unrecognized` variants to `SvcState`,
//! and an `Unrecognized` variant to `SvcEnabledNotOnlineState`, so that SMF
//! service states that are in transition or that `svcs` reports as absent or
//! unrecognized can be represented.

pub mod inventory;
