// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `BGP_UNNUMBERED_PEERS` of the Nexus external API.
//!
//! This version (2026_02_13_01) adds support for BGP unnumbered peers:
//! - `BgpPeer.addr` becomes optional (unnumbered sessions).
//! - `BgpPeer.router_lifetime` is added for IPv6 router advertisement
//!   lifetime.
//! - `BgpConfigCreate` gains a `max_paths` field for BGP multipath.
//! - `BgpPeerStatus` gains a `peer_id` field.
//! - `BgpImported` replaces the IPv4-only `BgpImportedRouteIpv4`.
//! - `BgpExported` becomes per-route instead of a HashMap.

pub mod networking;
