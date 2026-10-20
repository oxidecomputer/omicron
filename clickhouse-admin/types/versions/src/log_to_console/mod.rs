// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `LOG_TO_CONSOLE` of the ClickHouse Admin APIs.
//!
//! In this version, ClickHouse is configured to log to the console rather than
//! to dedicated files. As a result:
//!
//! - `LogConfig` carries only a log level; the `log`, `errorlog`, `size`, and
//!   `count` fields are removed.
//! - `LogLevel` gains an `Information` variant, which is the new default for
//!   generated ClickHouse configuration.

pub mod config;
pub mod keeper;
pub mod server;
