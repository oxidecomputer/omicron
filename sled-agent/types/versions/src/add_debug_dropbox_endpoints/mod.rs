// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_DEBUG_DROPBOX_ENDPOINTS` of the Sled Agent API.
//!
//! This version adds endpoints for enumerating and downloading debug dropbox
//! data on a per-zone basis, parallel to the existing `/support/logs`
//! endpoints.

pub mod diagnostics;
