// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Models for timeseries data in ClickHouse

// Copyright 2024 Oxide Computer Company

pub mod columns;
pub mod fields;
pub mod from_block;
pub mod measurements;
pub mod to_block;

/// Describes the version of the Oximeter database.
///
/// For usage and details see:
///
/// - [`crate::Client::initialize_db_with_version`]
/// - [`crate::Client::ensure_schema`]
/// - The `clickhouse-schema-updater` binary in this crate
pub const OXIMETER_VERSION: u64 = 14;
