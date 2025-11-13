// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! DNS protocol server implementation
//!
//! This module contains the DNS server implementation using hickory-server,
//! including the Authority trait implementation and the ServerFuture integration.

pub mod authority;
pub mod server;
