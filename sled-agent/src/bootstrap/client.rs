// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to a Sled Agent's Bootstrap API.
//! This is not its own crate because the only intended consumer is other
//! bootstrap peers within the cluster.

use omicron_common::generate_logging_api;

generate_logging_api!("../openapi/bootstrap-agent.json");
