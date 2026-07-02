// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_DEBUG_ENDPOINT` of the NTP Admin API.
//!
//! This version adds a `/debug` endpoint that returns the output of
//! read-only diagnostic commands run inside the NTP zone, intended to aid
//! debugging of time synchronization issues.

pub mod debug;
