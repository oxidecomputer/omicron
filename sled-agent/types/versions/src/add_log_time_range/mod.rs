// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_LOG_TIME_RANGE` of the Sled Agent API.
//!
//! Adds optional inclusive `start_time`/`end_time` bounds to
//! sled-diagnostics log download requests, and makes `max_rotated`
//! optional.

pub mod diagnostics;
