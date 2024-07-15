// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for the installinator artifact server.
//!
//! Why use this weird layer of indirection, you might ask?  Cargo chooses to
//! compile *each file* within the "tests/" subdirectory as a separate crate.
//! This means that doing "file-granularity" conditional compilation is
//! difficult, since a file like "test_for_illumos_only.rs" would get compiled
//! and tested regardless of the contents of "mod.rs".
//!
//! However, by lumping all tests into a submodule, all integration tests are
//! joined into a single crate, which itself can filter individual files
//! by (for example) choice of target OS.

mod integration_tests;
