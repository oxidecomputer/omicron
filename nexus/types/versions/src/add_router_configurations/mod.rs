// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_ROUTER_CONFIGURATIONS` of the Nexus external API.
//!
//! This version adds router configurations: named collections of rack routing
//! configuration (BGP configuration, BGP peers, static routes, and BFD peers)
//! managed under `/v1/system/networking/router-configurations`.

pub mod networking;
