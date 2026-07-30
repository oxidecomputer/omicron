// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the `STRICTER_BFD_TYPES` version of the Sled Agent API.
//!
//! This version tightens the types in [`early_networking::BfdPeerConfig`]:
//!
//! - `detection_threshold` becomes a `NonZeroU8`, matching the BFD
//!   specification (RFC 5880 section 6.8.1) and maghemite's BFD API.
//! - `required_rx` becomes a `u32`: it is sent on the wire as the 32-bit
//!   Required Min RX Interval field (RFC 5880 section 4.1), so larger values
//!   are not
//!   representable.

pub mod early_networking;
pub mod system_networking;
