// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `MODIFY_SVCS_TYPES` of the Sled Agent API.
//!
//! This version modifies the representation of the command error to a string
//! and now includes a timestamp of when the error ocurred. It also modifies the
//! enum representation of possible SMF service states to exclude 'online',
//! 'disabled' and 'legacy_run'.

pub mod inventory;
