// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `MULTIRACK_JOIN` of the wicketd commissioning API.
//!
//! This version adds the endpoint that joins a rack into an existing multirack
//! cluster, and with it [`rack_setup::MultirackJoinRequest`] and
//! [`rack_setup::RunMultirackJoinResponse`].
//!
//! It also restructures `UserSpecifiedPortConfig`, which is how a front port is
//! marked as carrying DDM traffic - what a cross-rack interconnect port is. Its
//! `Manual` variant becomes `Uplink` and carries an
//! [`rack_setup::UplinkPortConfig`] (the former `ManualPortConfig`), and its
//! `DdmAutoPortConfig` unit variant becomes `Ddm` and carries an
//! [`rack_setup::L1PortConfig`], giving DDM ports their own physical-layer
//! configuration. The types that transitively contain it
//! (`UserSpecifiedRackNetworkConfig` and `PutRssUserConfigInsensitive`) are
//! redefined here for that reason alone.
//!
//! Every other rack-setup type (and the inventory and update trees) are
//! unchanged from earlier versions.

pub mod rack_setup;
