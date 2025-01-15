// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! openapi-manager library facilities for implementing the openapi-manager
//! command-line tool

// helpers
pub mod dispatch;
mod output;

// subcommands
mod check;
mod generate;
mod list;
mod new_check;
