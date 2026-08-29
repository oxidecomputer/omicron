// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `FILTER_LOG_ZONE_LIST` of the Sled Agent API.
//!
//! Adds optional inclusive `start_time`/`end_time` bounds to the
//! sled-diagnostics zone-listing request, so callers can skip zones
//! with no log content in a time window.

pub mod diagnostics;
