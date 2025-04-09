// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::process::Command;

/// Creates and prepares a `std::process::Command` for the `cargo` executable.
pub fn cargo_command(location: CargoLocation) -> Command {
    let mut command = location.resolve();

    for (key, _) in std::env::vars_os() {
        let Some(key) = key.to_str() else { continue };
        if SANITIZED_ENV_VARS.matches(key) {
            command.env_remove(key);
        }
    }

    command
}

/// How to determine the location of the `cargo` executable.
#[derive(Clone, Copy, Debug)]
pub enum CargoLocation {
    /// Use the `CARGO` environment variable, and fall back to `"cargo"` if it
    /// is not set.
    FromEnv,

    /// Do not use the `CARGO` environment variable, instead always using `"cargo"`.
    Fixed,
}

impl CargoLocation {
    fn resolve(self) -> Command {
        match self {
            CargoLocation::FromEnv => {
                let cargo = std::env::var("CARGO")
                    .unwrap_or_else(|_| String::from("cargo"));
                Command::new(&cargo)
            }
            CargoLocation::Fixed => Command::new("cargo"),
        }
    }
}

#[derive(Debug)]
struct SanitizedEnvVars {
    // At the moment we only ban some prefixes, but we may also want to ban env
    // vars by exact name in the future.
    prefixes: &'static [&'static str],
}

impl SanitizedEnvVars {
    const fn new() -> Self {
        // Remove many of the environment variables set in
        // https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-build-scripts.
        // This is done to avoid recompilation with crates like ring between
        // `cargo clippy` and `cargo xtask clippy`. (This is really a bug in
        // both ring's build script and in Cargo.)
        //
        // The current list is informed by looking at ring's build script, so
        // it's not guaranteed to be exhaustive and it may need to grow over
        // time.
        let prefixes = &["CARGO_PKG_", "CARGO_MANIFEST_", "CARGO_CFG_"];
        Self { prefixes }
    }

    fn matches(&self, key: &str) -> bool {
        self.prefixes.iter().any(|prefix| key.starts_with(prefix))
    }
}

static SANITIZED_ENV_VARS: SanitizedEnvVars = SanitizedEnvVars::new();
