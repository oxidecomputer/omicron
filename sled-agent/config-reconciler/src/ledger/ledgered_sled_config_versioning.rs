// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module for converting older formats of the sled configuration files.

use camino::Utf8PathBuf;
use omicron_common::ledger::Ledger;
use omicron_common::ledger::Ledgerable;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_types::inventory::OmicronSledConfig;
use sled_agent_types_versions::v4;
use sled_agent_types_versions::v10;
use sled_agent_types_versions::v11;
use sled_agent_types_versions::v14;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::error::Error as StdError;

/// Trait describing an ordered sequence of `OmicronSledConfig` versions, each
/// of which can be converted from its previous version.
///
/// When adding a new [`OmicronSledConfig`] version, add your new version to the
/// `version_conversion_chain!()` invocation below. Use the fully-versioned name
/// (e.g., `vN::inventory::OmicronSledConfig`), not the [`OmicronSledConfig`]
/// alias from `latest`.
///
/// Also update the unit tests at the bottom of this file to cover your new
/// version as well.
trait VersionConversionChain: Ledgerable {
    /// A description of the version. This shows up in logs.
    const DESCRIPTION: &str;

    /// Special terminal state; this must be `false` for all implementors except
    /// [`VersionConversionChainTerminal`].
    // `try_ledgered_config_versions_chain() uses this to know when to stop
    // recursing.
    const IS_TERMINAL: bool = false;

    /// The previous [`OmicronSledConfig`] version, which must be convertible
    /// into this version.
    type Previous: VersionConversionChain + TryInto<Self, Error: StdError>;
}

macro_rules! version_conversion_chain {
    // base case
    ($current:path, $previous:path) => {
        impl VersionConversionChain for $current {
            const DESCRIPTION: &str = stringify!($current);
            type Previous = $previous;
        }
    };

    // recursive case
    ($current:path, $previous:path, $($rest:path),+ $(,)?) => {
        version_conversion_chain!($current, $previous);
        version_conversion_chain!($previous, $($rest),+);
    };
}

// This list is ordered from newest to oldest; this is the order in which we'll
// attempt to parse the ledgered config. Add new versions to the top of the
// list.
version_conversion_chain!(
    v14::inventory::OmicronSledConfig,
    v11::inventory::OmicronSledConfig,
    v10::inventory::OmicronSledConfig,
    v4::inventory::OmicronSledConfig,
    VersionConversionChainTerminal,
);

/// Read the ledgered [`OmicronSledConfig`], converting from older versions if
/// needed.
///
/// # Panics
///
/// This panics if we're able to read a config (of any known older version) but
/// fail to convert it to the latest version. Most sled config conversions are
/// infallible, but occasionally we have fallible ones that only fail if we've
/// done something that ought to be impossible; e.g., in the dual-stack
/// networking work, there's a fallible conversion if we somehow
/// serialized a NIC with an IPv4 address, but on an IPv6 subnet. In such a
/// situation, we have no way of converting to the current format, which means
/// we have no way to proceed. Returning `None` is not correct, since that would
/// incorrectly indicate that we have no config at all. We _must_ panic and rely
/// on support correcting this (believed-to-be-impossible) situation.
pub(super) async fn read_ledgered_sled_config(
    log: &Logger,
    paths: Vec<Utf8PathBuf>,
) -> Option<OmicronSledConfig> {
    // Attempt to read the ledger as the current version; if this succeeds,
    // we're done.
    if let Some(config) = Ledger::new(log, paths.clone()).await {
        info!(log, "Ledger of sled config exists");
        return Some(config.into_inner());
    }

    // Try to read the config as the previous version; if we have an older
    // version on disk, this will recurse until we get to it, but then convert
    // it up through our previous version before returning.
    let prev_version = try_ledgered_config_versions_chain::<
        <OmicronSledConfig as VersionConversionChain>::Previous,
    >(log, paths.clone())
    .await?;

    let current_version = prev_version.try_into().unwrap_or_else(|e| {
        panic!(
            "failed to convert {} to the current version: {}",
            <OmicronSledConfig as VersionConversionChain>::DESCRIPTION,
            InlineErrorChain::new(&e)
        );
    });

    Some(write_converted_ledger(log, paths, current_version).await)
}

/// Reading old ledgers from disk in the face of multiple version changes is
/// tricky. Imagine we have this sequence in the versioning chain:
///
/// * v4 (the oldest supported version)
/// * v10
/// * v11
/// * v12 (current)
///
/// Our caller must have already attempted to read the ledgered disk as v12 and
/// failed, if it's trying to convert from an old version. To support the prior
/// three versions, we must:
///
/// * Attempt to read the ledger as v11. If that succeeds, convert it to v12 and
///   we're done.
/// * If that failed, attempt to read the ledger as v10. If that succeeds,
///   convert it to v11 then v12 and we're done.
/// * If that failed, attempt to read the ledger as v4. If that succeeds,
///   convert it to v10 then v11 then v12 and we're done.
///
/// This method handles that process by recursing. In this example, we start
/// with `T` being the v11 sled config type. We'll try to read the ledger; if
/// that succeeds, we return it (and our caller will convert to v12). If that
/// fails, we'll recurse and call ourselves with `T::Previous` (i.e., v10). If
/// v10 returns successfully (after possibly recursing itself!), we'll convert
/// v10 to v11 and return to our caller.
///
/// The recursion depth here is capped at the number of versions we support,
/// which we should be able to keep managable. Technically we can drop any
/// versions older than what was "current" as of the previously shipped release
/// (since we forbid updates from skipping releases). For extra paranoia, we can
/// keep versions covering the oldest deployed rack around.
#[async_recursion::async_recursion]
async fn try_ledgered_config_versions_chain<T>(
    log: &Logger,
    paths: Vec<Utf8PathBuf>,
) -> Option<T>
where
    T: VersionConversionChain,
{
    if T::IS_TERMINAL {
        return None;
    }

    if let Some(config) = Ledger::<T>::new(log, paths.clone()).await {
        info!(
            log,
            "successfully read ledgered config as version {}",
            T::DESCRIPTION
        );
        return Some(config.into_inner());
    }

    let old_config =
        try_ledgered_config_versions_chain::<T::Previous>(log, paths).await?;

    match old_config.try_into() {
        Ok(config) => {
            info!(
                log,
                "converted config read from ledger to version {}",
                T::DESCRIPTION
            );
            Some(config)
        }
        Err(err) => {
            panic!(
                "failed to convert legered config \
                 from version {} to version {}: {}",
                T::Previous::DESCRIPTION,
                T::DESCRIPTION,
                InlineErrorChain::new(&err),
            );
        }
    }
}

async fn write_converted_ledger(
    log: &Logger,
    paths: Vec<Utf8PathBuf>,
    sled_config: OmicronSledConfig,
) -> OmicronSledConfig {
    let mut config_ledger = Ledger::new_with(log, paths.clone(), sled_config);

    match config_ledger.commit().await {
        Ok(()) => (),
        Err(err) => {
            // We weren't able to write the new ledger, but we were still able
            // to _read_ it (via converting an old version). Log this failure
            // but return the config we read; we'll try converting again the
            // next time we run.
            warn!(
                log,
                "Failed to write new sled config converted from \
                 from older version";
                InlineErrorChain::new(&err),
            );
        }
    }

    config_ledger.into_inner()
}

// Terminal type for the [`VersionConversionChain`] above. This type is
// uninhabitable (equivalent to the `Never` / `!` type), and therefore its trait
// implementations can all safely panic (since no instance of it can exist at
// runtime for us to have a `self`).
#[derive(Debug, Serialize, Deserialize)]
enum VersionConversionChainTerminal {}

impl VersionConversionChain for VersionConversionChainTerminal {
    const DESCRIPTION: &str = "NEVER_USED_TERMINAL_STATE";
    const IS_TERMINAL: bool = true;

    type Previous = Self;
}

impl Ledgerable for VersionConversionChainTerminal {
    fn is_newer_than(&self, _: &Self) -> bool {
        unreachable!("terminal type is uninhabitable")
    }

    fn generation_bump(&mut self) {
        unreachable!("terminal type is uninhabitable")
    }
}

impl TryFrom<VersionConversionChainTerminal>
    for v4::inventory::OmicronSledConfig
{
    type Error = std::io::Error;

    fn try_from(
        _: VersionConversionChainTerminal,
    ) -> Result<Self, Self::Error> {
        unreachable!("terminal type is uninhabitable")
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use camino_tempfile::Utf8TempDir;
    use camino_tempfile_ext::prelude::*;
    use omicron_test_utils::dev;

    // v4 config collected from a test system.
    const V4_CONFIG_PATH: &str = "test-data/v4-sled-config.json";

    // paths for expectorate checks
    const EXPECTORATE_V10_CONFIG_PATH: &str =
        "expectorate/v10-sled-config.json";
    const EXPECTORATE_V11_CONFIG_PATH: &str =
        "expectorate/v11-sled-config.json";
    const EXPECTORATE_V14_CONFIG_PATH: &str =
        "expectorate/v14-sled-config.json";


    // This is solely an expectorate test to guarantee:
    //
    // * the conversions for various versions function (at least starting from
    //   the v4 config we've committed)
    // * we have input files at intermediate versions we can use in other tests
    #[tokio::test]
    async fn can_convert_v4_to_newer_versions() {
        let logctx = dev::test_setup_log("can_convert_v4_to_newer_versions");
        let log = &logctx.log;

        let v4 = Ledger::<v4::inventory::OmicronSledConfig>::new(
            log,
            vec![V4_CONFIG_PATH.into()],
        )
        .await
        .expect("read v4 from test-data")
        .into_inner();

        // For each version after the oldest, confirm we can convert and then
        // assert the expectorate contents.

        let v10 = v10::inventory::OmicronSledConfig::try_from(v4)
            .expect("converted from v4");
        let v11 = v11::inventory::OmicronSledConfig::try_from(v10.clone())
            .expect("converted from v10");
        let v14 = v14::inventory::OmicronSledConfig::try_from(v11.clone())
            .expect("converted from v11");

        expectorate::assert_contents(
            EXPECTORATE_V10_CONFIG_PATH,
            &serde_json::to_string_pretty(&v10).unwrap(),
        );
        expectorate::assert_contents(
            EXPECTORATE_V11_CONFIG_PATH,
            &serde_json::to_string_pretty(&v11).unwrap(),
        );
        expectorate::assert_contents(
            EXPECTORATE_V14_CONFIG_PATH,
            &serde_json::to_string_pretty(&v14).unwrap(),
        );
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn read_config_converts_from_older_versions() {
        let logctx = dev::test_setup_log("read_config_converts_from_older_versions");
        let log = &logctx.log;

        // All our configs should match the latest version. We use an explicit
        // type here instead of the generic `OmicronSledConfig` so we get a
        // compilation error if the latest version changes. Bump the
        // version here and add the new version's path to the array of ledger
        // paths below.
        let latest_version_path = EXPECTORATE_V14_CONFIG_PATH;
        let expected_config = v14::inventory::OmicronSledConfig::read_from(
            log,
            latest_version_path.into(),
        )
        .await
        .expect("read v14 config");

        // Reading old configs should rewrite the file to match the newest
        // version.
        let expected_rewritten =
            serde_json::to_string(&expected_config).expect("serialized config");

        // If no conversion was necessary, we should keep the same contents.
        // This is semantically equivalent to `expected_rewritten` but may be
        // formatted differently (e.g., our expectorate files are written
        // pretty-printed, but ledgered files generally aren't).
        let expected_unchanged = tokio::fs::read_to_string(latest_version_path)
            .await
            .expect("read latest version");

        let tempdir = Utf8TempDir::new().unwrap();

        // For each older version, confirm we can read a ledger of that version
        // and that it's converted to the current version.
        for src_ledger_path in [
            V4_CONFIG_PATH,
            EXPECTORATE_V10_CONFIG_PATH,
            EXPECTORATE_V11_CONFIG_PATH,
            EXPECTORATE_V14_CONFIG_PATH,
        ] {
            // Copy the ledger into `my-ledger.json`
            let dst_ledger_path = tempdir.child("my-ledger.json");
            dst_ledger_path.write_file(src_ledger_path.into()).unwrap();

            // Attempt to read `my-ledger.json`; this should give us back a
            // current-version `OmicronSledConfig` and also have rewritten the
            // config.
            let converted_config = read_ledgered_sled_config(
                log,
                vec![dst_ledger_path.to_path_buf()],
            )
            .await
            .expect("read and converted ledger");
            assert_eq!(expected_config, converted_config);

            // We should only rewrite the file if we converted it.
            let data = tokio::fs::read_to_string(&dst_ledger_path)
                .await
                .expect("read tempdir ledger");
            if data != expected_unchanged {
                // The data changed - we must have done a conversion. Assert it
                // matches what we expect.
                assert_eq!(data, expected_rewritten);
            }
        }

        logctx.cleanup_successful();
    }
}
