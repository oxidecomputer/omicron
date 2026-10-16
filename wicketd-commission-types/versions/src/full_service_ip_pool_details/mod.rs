// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `FULL_SERVICE_IP_POOL_DETAILS` of the wicketd commissioning API.
//!
//! This version replaces `PutRssUserConfigInsensitive`'s flat
//! `internal_services_ip_pool_ranges` with structured, operator-specified
//! `service_ip_pools`. All other rack-setup types (and the inventory and
//! update trees) are unchanged from `v1`.

pub mod rack_setup;
