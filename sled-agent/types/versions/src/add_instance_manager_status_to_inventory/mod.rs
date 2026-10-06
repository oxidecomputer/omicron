// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_INSTANCE_MANAGER_STATUS_TO_INVENTORY` of the Sled Agent API.
//!
//! This version adds the `instance_manager_status` field to inventory. This
//! field reports this sled's instance manager's count of registered VMMs and
//! the update disposition on which it's currently acting, providing an atomic
//! view that allows Reconfigurator to determine whether the sled has been fully
//! evacuated for update.

pub mod inventory;
