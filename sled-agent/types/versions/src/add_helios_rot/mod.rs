// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_HELIOS_ROT` of the Sled Agent API.
//!
//! This version adds the Helios RoT to the RoT attestation endpoints, along
//! with the 48 bytes of qualifying data and P-384 attestation it uses. The
//! `Nonce` type is renamed to `QualifyingData`.

pub mod attestation;
