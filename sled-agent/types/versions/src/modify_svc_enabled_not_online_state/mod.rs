// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `MODIFY_SVC_ENABLED_NOT_ONLINE_STATE` of the Sled Agent API.
//!
//! This version removes the `Uninitialized` variant from the
//! `SvcEnabledNotOnlineState` enum. `svcs -x` treats `uninitialized` as a
//! "running" state and does not include it in the list of enabled-but-not-
//! online services, so `Uninitialized` was unreachable in practice.

pub mod inventory;
