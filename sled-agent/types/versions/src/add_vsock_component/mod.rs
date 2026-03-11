// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_VSOCK_COMPONENT` of Sled Agent API.
//!
//! This version updates the instance spec types to use the latest propolis
//! types, which add the `VirtioSocket` component variant and an optional
//! `smbios` field to `InstanceSpec`.

pub mod instance;
