// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_MAX_ERROR_AND_OFFSET` of the NTP Admin API.
//!
//! This version extends [`timesync::TimeSync`] with additional fields for
//! clock error estimation: `last_offset`, `rms_offset`, `root_dispersion`,
//! and `max_error` (= root_delay/2 + root_dispersion).

pub mod timesync;
