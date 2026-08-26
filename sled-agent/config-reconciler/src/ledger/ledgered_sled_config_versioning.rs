// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module for converting older formats of the sled configuration files.

use camino::Utf8PathBuf;
use omicron_ledger::Ledger;
use omicron_ledger::Ledgerable;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_types::inventory::OmicronSledConfig;
use sled_agent_types_versions::v4;
use sled_agent_types_versions::v10;
use sled_agent_types_versions::v11;
use sled_agent_types_versions::v14;
use sled_agent_types_versions::v49;
use sled_agent_types_versions::v50;
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
    v50::inventory::OmicronSledConfig,
    v49::inventory::OmicronSledConfig,
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
    const EXPECTORATE_V49_CONFIG_PATH: &str =
        "expectorate/v49-sled-config.json";
    const EXPECTORATE_V50_CONFIG_PATH: &str =
        "expectorate/v50-sled-config.json";

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
        let v49 = v49::inventory::OmicronSledConfig::from(v14.clone());
        let v50 = v50::inventory::OmicronSledConfig::from(v49.clone());

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
        expectorate::assert_contents(
            EXPECTORATE_V49_CONFIG_PATH,
            &serde_json::to_string_pretty(&v49).unwrap(),
        );
        expectorate::assert_contents(
            EXPECTORATE_V50_CONFIG_PATH,
            &serde_json::to_string_pretty(&v50).unwrap(),
        );
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn read_config_converts_from_older_versions() {
        let logctx =
            dev::test_setup_log("read_config_converts_from_older_versions");
        let log = &logctx.log;

        // All our configs should match the latest version. We use an explicit
        // type here instead of the generic `OmicronSledConfig` so we get a
        // compilation error if the latest version changes. Bump the
        // version here and add the new version's path to the array of ledger
        // paths below.
        type LatestConfig = v50::inventory::OmicronSledConfig;
        let latest_version_path = EXPECTORATE_V50_CONFIG_PATH;
        let expected_config =
            LatestConfig::read_from(log, latest_version_path.into())
                .await
                .expect("read expected config");

        // Reading old configs should rewrite the file to match the newest
        // version.
        let expected_rewritten =
            serde_json::to_string(&expected_config).expect("serialized config");

        let tempdir = Utf8TempDir::new().unwrap();

        // Guard against every fixture silently taking one branch (e.g. after a
        // wire-compatible bump or a forgotten array entry) by counting both
        // branches and asserting that both of them were > 0.
        let mut converted_count = 0usize;
        let mut unchanged_count = 0usize;

        // For each older version, confirm we can read a ledger of that version
        // and that it's converted to the current version.
        for src_ledger_path in [
            V4_CONFIG_PATH,
            EXPECTORATE_V10_CONFIG_PATH,
            EXPECTORATE_V11_CONFIG_PATH,
            EXPECTORATE_V14_CONFIG_PATH,
            EXPECTORATE_V49_CONFIG_PATH,
            EXPECTORATE_V50_CONFIG_PATH,
        ] {
            // Copy the ledger into `my-ledger.json`
            let dst_ledger_path = tempdir.child("my-ledger.json");
            dst_ledger_path.write_file(src_ledger_path.into()).unwrap();

            // Read the fixture contents. This is expected to be a
            // pretty-printed JSON file.
            let fixture_contents = tokio::fs::read_to_string(src_ledger_path)
                .await
                .expect("read source ledger");

            // Does this fixture need conversion (does it parse directly as the
            // latest version?)
            //
            // A source can also parse successfully as the latest version
            // without being the latest expectorate file, e.g. when a version
            // bump introduces a transparent newtype.
            //
            // Ledger::<LatestConfig>::new is (effectively) the first step of
            // read_ledgered_sled_config, so the test matches the SUT if a
            // version ever overrides Ledgerable::deserialize.
            let parses_as_latest = Ledger::<LatestConfig>::new(
                log,
                vec![dst_ledger_path.to_path_buf()],
            )
            .await
            .is_some();
            if src_ledger_path == V4_CONFIG_PATH {
                assert!(
                    !parses_as_latest,
                    "{src_ledger_path} is the oldest supported version \
                     and must not parse as the latest version"
                );
            }
            if src_ledger_path == latest_version_path {
                assert!(
                    parses_as_latest,
                    "{src_ledger_path} is the latest version \
                     and must parse as such"
                );
            }

            // Attempt to read `my-ledger.json`; this should give us back a
            // current-version `OmicronSledConfig` and, if a conversion was
            // needed, also have rewritten the config.
            let converted_config = read_ledgered_sled_config(
                log,
                vec![dst_ledger_path.to_path_buf()],
            )
            .await
            .expect("read and converted ledger");
            assert_eq!(expected_config, converted_config);

            let data = tokio::fs::read_to_string(&dst_ledger_path)
                .await
                .expect("read tempdir ledger");
            if parses_as_latest {
                unchanged_count += 1;
                // Ensure that the fixture contents are *not* byte-identical to
                // the serialized JSON that a rewrite would produce (fixtures
                // are pretty-printed while ledgered data is stored as compact
                // JSON).
                //
                // Why? In the assert_eq! below, we depend on the fact that
                // these two are not the same.
                //
                // * The expected behavior here is "if the data is successfully
                //   parsed as the latest version, the file isn't changed, even
                //   if what it would serialize to differs from the input".
                // * If the two were the same, and if we accidentally changed the
                //   SUT to always rewrite the file, then data == fixture_contents
                //   would be true even though the SUT had unexpected behavior.
                assert_ne!(
                    fixture_contents, expected_rewritten,
                    "{src_ledger_path} is byte-identical to its compact \
                     rewrite, so a spurious rewrite would be undetectable"
                );

                assert_eq!(
                    data, fixture_contents,
                    "{src_ledger_path} parses as the latest version \
                     and must not be rewritten"
                );
            } else {
                converted_count += 1;
                // We couldn't parse as the latest version, so the file must
                // have been rewritten in the latest format.
                assert_eq!(
                    data, expected_rewritten,
                    "{src_ledger_path} required conversion \
                     and must be rewritten"
                );
            }
        }

        assert!(
            converted_count > 0,
            "no fixture required conversion; the conversion chain is untested"
        );
        assert!(
            unchanged_count > 0,
            "no fixture parsed as the latest version; \
             the no-conversion path is untested"
        );

        logctx.cleanup_successful();
    }
}
