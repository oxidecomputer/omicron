// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_SLED_REGISTRATION_INFO` of the Sled Agent API.
//!
//! This adds `GET /sled-registration-info` to the Sled Agent API, returning the
//! sled's immutable identity (sled ID and rack ID). This is additional
//! information that isn't present in the `sled_upsert` upcall to Nexus.

pub mod inventory;
