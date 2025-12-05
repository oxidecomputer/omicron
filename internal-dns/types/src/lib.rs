// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common types for the management interface of DNS servers for internal and
//! external name resolution.
//!
//! ## Organization
//!
//! Published types for the DNS server API live in the `internal-dns-types-migrations`
//! crate, organized by version. This crate re-exports the latest versions as
//! floating identifiers for use by non-migration-related code.
//!
//! For fixed identifiers (used in API definitions), depend on
//! `internal-dns-types-migrations` directly.

pub mod config;
pub mod diff;
pub mod names;
