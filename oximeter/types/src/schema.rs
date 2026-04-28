// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub use oximeter_types_versions::latest::schema::*;

/// Full path to the directory containing all schema.
///
/// This is defined in this crate as the single source of truth, but not
/// re-exported outside implementation crates (e.g., not via `oximeter` or
/// `oximeter-collector`.
pub const SCHEMA_DIRECTORY: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../oximeter/schema");
