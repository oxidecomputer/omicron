// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

pub mod error;
mod management_switch;

pub use management_switch::SpIdentifier;
pub use management_switch::SpType;

// TODO the following should probably not be pub once this crate is more
// complete; for now make them pub so `gateway` can use them directly
pub use management_switch::ManagementSwitch;
pub use management_switch::ManagementSwitchDiscovery;
pub use management_switch::SpSocket;
pub use management_switch::SwitchPort;

// TODO these will remain public for a while, but eventually will be removed
// altogther; currently these provide a way to hard-code the rack topology,
// which is not what we want.
pub use management_switch::KnownSps;
pub use management_switch::KnownSp;
