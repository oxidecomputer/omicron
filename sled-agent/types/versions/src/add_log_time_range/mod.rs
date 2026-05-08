// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_LOG_TIME_RANGE` of the Sled Agent API.
//!
//! This version reshapes the `support_logs_download` endpoint's query
//! parameters to support time-bounded log collection: `max_rotated`
//! becomes optional, and `start_time`/`end_time` are added so callers
//! can request only logs whose `mtime` falls within the supplied
//! window.

pub mod diagnostics;
