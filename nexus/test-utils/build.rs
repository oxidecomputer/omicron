// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::{test_util::LogContext, ConfigLogging, ConfigLoggingLevel};
use omicron_test_utils::dev::test_setup_database_seed;
use std::env;
use std::path::Path;

// Creates a "pre-populated" CockroachDB storage directory, which
// subsequent tests can copy instead of creating themselves.
//
// Is it critical this happens at build-time? No. However, it
// makes it more convenient for tests to assume this seeded
// directory exists, rather than all attempting to create it
// concurrently.
//
// Refer to the documentation of [`test_setup_database_seed`] for
// more context.
#[tokio::main]
async fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../schema/crdb/dbinit.sql");
    println!("cargo:rerun-if-changed=../../tools/cockroachdb_checksums");
    println!("cargo:rerun-if-changed=../../tools/cockroachdb_version");

    let logctx = LogContext::new(
        "crdb_seeding",
        &ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info },
    );

    let seed =
        Path::new(&env::var("OUT_DIR").expect("Missing output directory"))
            .join("crdb-base");
    test_setup_database_seed(&logctx.log, &seed).await;
    logctx.cleanup_successful();
}
