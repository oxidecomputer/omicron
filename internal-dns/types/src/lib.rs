// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common types for the management interface of DNS servers for internal and
//! external name resolution.
//!
//! ## Organization
//!
//! Some types in this crate are exposed by its dependents as part of versioned
//! HTTP interfaces, such as `dns-server`'s management interfaces. In those
//! cases we may want to support multiple HTTP interface versions concurrently,
//! and so this crate preserves old versions of select public items used in this
//! way.
//!
//! The alternative here would be to require dependents of `internal-dns` to
//! declare duplicate dependencies on `internal-dns` at different revisions.
//! That would force dependents to take all of `internal-dns`' dependencies at
//! versions of interest as transitive dependencies, and precludes maintenance
//! that would otherwise be able to preserve API compatibility of the public
//! types.
//!
//! `cargo xtask openapi` helps us check that we don't unintentionally break an
//! existing committed version, which also helps us be confident that future
//! maintenance on old versions' types does not introduce breaking changes.
//!
//! The top-level items here can be thought of as the "current" version, where
//! versioned items (and their previous versions) are in the `vN` modules with
//! their latest form re-exported as the "current" version.

pub mod v1;
pub mod v2;

pub mod config;
pub mod diff;
pub mod names;
