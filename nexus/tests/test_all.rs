// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration test driver
//!
//! All integration tests are driven from this top-level integration test so
//! that we only have to build one target. This was originally done because
//! `cargo test` does not parallelize across targets. We continue to do this
//! because any targets depending on omicron-nexus as a library are very
//! expensive to link, and it is better to only have to do that once.

#[macro_use]
extern crate slog;

// The individual tests themselves live in the "integration_tests" subdirectory.
// This extra level of indirection is annoying but we can't put them into the
// current directory because Cargo would try to build them individually.
mod integration_tests;
