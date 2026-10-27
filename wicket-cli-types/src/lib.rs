// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Stable types for Wicket CLI JSON output and exit codes.
//!
//! The types here are a reduced form of the types in the wicketd commission
//! API, using fixed identifiers (rather than the floating `latest::`
//! identifiers). This is meant for scenarios where the commission API isn't
//! currently available, such as manufacturing.
//!
//! Eventually we'll likely want to expose the commission API to manufacturing
//! and retire this crate, but that has security implications that haven't been
//! fully worked out yet.
//!
//! # Updating the types
//!
//! If manufacturing requires new information not available with the current
//! versions of the types, follow these steps:
//!
//! 1. Add a new version of the commissioning API with the updated types, as
//!    desired.
//! 2. Update the types in this crate, along with the corresponding CLI version,
//!    e.g., [`rack_update::RACK_UPDATE_STATUS_CLI_VERSION`].
//! 3. Land that on main.
//! 4. Once this is in a released version of the Oxide system software, and
//!    once manufacturing is ready to switch to it, update the corresponding
//!    manufacturing repo (most likely facade) with the new version of
//!    wicket-cli-types.
//!
//! We don't have version negotiation or upgrades built in at the moment, so
//! updating the types exported by this crate will always result in a flag day
//! for manufacturing. We use oxide-versioned-envelope to ensure that a
//! divergence between Omicron and the manufacturing repo is detected (albeit at
//! runtime, not compile time).

pub mod rack_update;
