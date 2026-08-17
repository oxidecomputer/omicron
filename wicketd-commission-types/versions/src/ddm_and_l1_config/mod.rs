// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `DDM_AND_L1_CONFIG` of the wicketd commissioning API.
//!
//! This version restructures `UserSpecifiedPortConfig`. Its `Manual` variant
//! becomes `Uplink` and carries an [`rack_setup::UplinkPortConfig`] (the former
//! `ManualPortConfig`), and its `DdmAutoPortConfig` unit variant becomes `Ddm`
//! and carries an [`rack_setup::L1PortConfig`], giving DDM ports their own
//! physical-layer configuration. The types that transitively contain it
//! (`UserSpecifiedRackNetworkConfig` and `PutRssUserConfigInsensitive`) are
//! redefined here for that reason alone. Every other rack-setup type (and the
//! inventory and update trees) are unchanged from earlier versions.

pub mod rack_setup;
