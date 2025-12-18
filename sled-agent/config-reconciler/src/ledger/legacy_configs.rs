// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module for converting older formats of the sled configuration files.

use camino::Utf8PathBuf;
use omicron_common::api::external;
use omicron_common::ledger::Ledger;
use omicron_common::ledger::Ledgerable;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_types::inventory::OmicronSledConfig;
use sled_agent_types_versions::v4;
use sled_agent_types_versions::v10;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::error::Error as StdError;

/// Trait describing an ordered sequence of `OmicronSledConfig` versions, each
/// of which can be converted from its previous version.
///
/// When adding a new [`OmicronSledConfig`] version, do the following:
///
/// 1. Implement [`VersionConversionChain`] for your new version. Its associated
///    `Previous` type should point to the prior version (what was the current
///    version before your change).
/// 2. Update the [`CurrentMinusOneSledConfigVersion`] type alias; this points
///    to the first version that [`try_convert_old_ledgered_config_versions`]
///    will attempt to read.
trait VersionConversionChain: Ledgerable {
    /// A description of the version. This shows up in logs.
    const DESCRIPTION: &str;

    /// Special terminal state; this must be `false` for all implementors except
    /// [`VersionConversionChainTerminal`].
    /// [`try_convert_old_ledgered_config_versions_chain()] uses this to know
    /// when to stop recursing.
    const IS_TERMINAL: bool = false;

    /// The previous [`OmicronSledConfig`] version, which must be convertible
    /// into this version.
    type Previous: VersionConversionChain + TryInto<Self, Error: StdError>;
}

type CurrentMinusOneSledConfigVersion = v10::inventory::OmicronSledConfig;

impl VersionConversionChain for v10::inventory::OmicronSledConfig {
    const DESCRIPTION: &str = "v10::inventory::OmicronSledConfig";

    type Previous = OmicronSledConfigLocal;
}

impl VersionConversionChain for OmicronSledConfigLocal {
    const DESCRIPTION: &str = "OmicronSledConfigLocal";

    type Previous = VersionConversionChainTerminal;
}

/// Convert from an earlier version of the sled configuration to the current, if
/// possible.
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
pub(super) async fn try_convert_old_ledgered_config_versions(
    log: &Logger,
    datasets: Vec<Utf8PathBuf>,
) -> Option<OmicronSledConfig> {
    // Try to read the config as the previous version; if we have an older
    // version on disk, this will recurse until we get to it, but then convert
    // it up throw `CurrentMinusOneSledConfigVersion` before returning.
    let prev_version = try_convert_old_ledgered_config_versions_chain::<
        CurrentMinusOneSledConfigVersion,
    >(log, datasets.clone())
    .await?;

    let current_version = prev_version.try_into().unwrap_or_else(|e| {
        panic!(
            "failed to convert {} to the current version: {}",
            CurrentMinusOneSledConfigVersion::DESCRIPTION,
            InlineErrorChain::new(&e)
        );
    });

    Some(write_converted_ledger(log, datasets, current_version).await)
}

/// Reading old ledgers from disk in the face of multiple version changes is
/// tricky. Imagine we have this sequence in the versioning change:
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
async fn try_convert_old_ledgered_config_versions_chain<T>(
    log: &Logger,
    datasets: Vec<Utf8PathBuf>,
) -> Option<T>
where
    T: VersionConversionChain,
{
    if T::IS_TERMINAL {
        return None;
    }

    if let Some(config) = Ledger::<T>::new(log, datasets.clone()).await {
        return Some(config.into_inner());
    }

    let old_config = try_convert_old_ledgered_config_versions_chain::<
        T::Previous,
    >(log, datasets)
    .await?;

    info!(
        log,
        "Successfully read ledgered config as old version {}; \
         will now convert to latest version",
        T::DESCRIPTION,
    );

    match old_config.try_into() {
        Ok(config) => Some(config),
        Err(err) => {
            panic!(
                "failed to convert legered config \
                 of version {} to version {}: {}",
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(transparent)]
struct OmicronSledConfigLocal(v4::inventory::OmicronSledConfig);

impl Ledgerable for OmicronSledConfigLocal {
    fn is_newer_than(&self, other: &Self) -> bool {
        self.0.generation > other.0.generation
    }

    fn generation_bump(&mut self) {
        self.0.generation = self.0.generation.next()
    }
}

impl TryFrom<OmicronSledConfigLocal> for v10::inventory::OmicronSledConfig {
    type Error = external::Error;

    fn try_from(value: OmicronSledConfigLocal) -> Result<Self, Self::Error> {
        Self::try_from(value.0)
    }
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

impl TryFrom<VersionConversionChainTerminal> for OmicronSledConfigLocal {
    type Error = std::io::Error;

    fn try_from(
        _: VersionConversionChainTerminal,
    ) -> Result<Self, Self::Error> {
        unreachable!("terminal type is uninhabitable")
    }
}

#[cfg(test)]
pub(super) mod tests {
    use crate::ledger::CONFIG_LEDGER_FILENAME;

    use super::*;
    use camino::Utf8Path;
    use camino_tempfile::Utf8TempDir;
    use omicron_test_utils::dev;

    // Legacy configs collected from a test system.
    pub(crate) const LEGACY_DISKS_PATH: &str =
        "test-data/omicron-physical-disks.json";
    pub(crate) const LEGACY_DATASETS_PATH: &str =
        "test-data/omicron-datasets.json";
    pub(crate) const LEGACY_ZONES_PATH: &str = "test-data/omicron-zones.json";

    // The merged legacy configs above. We assert that it matches in
    // test_merge_old_configs below.
    const MERGED_CONFIG_PATH: &str =
        "test-data/expectorate/merged-sled-config.json";

    #[test]
    fn test_old_config_schema() {
        let schema = schemars::schema_for!(OmicronZonesConfigLocal);
        expectorate::assert_contents(
            "../../schema/all-zones-requests.json",
            &serde_json::to_string_pretty(&schema).unwrap(),
        );
    }

    #[tokio::test]
    async fn can_convert_v9_config_version() {
        let logctx = dev::test_setup_log("can_convert_v9_config_version");
        let tempdir = Utf8TempDir::new().expect("created tempdir");

        // Copy version 6 into a tempdir.
        println!("logging to {}", tempdir.path());
        let dst_file_name = Utf8PathBuf::from(
            Utf8PathBuf::from(MERGED_CONFIG_PATH).file_name().unwrap(),
        );
        let dst_file = tempdir.path().join(&dst_file_name);
        tokio::fs::copy(MERGED_CONFIG_PATH, &dst_file)
            .await
            .expect("Copy old config into tempdir");
        println!("copied {} => {}", MERGED_CONFIG_PATH, dst_file);

        // Convert, which will rewrite the config as well.
        let converted =
            try_convert_v4_sled_config(&logctx.log, vec![dst_file.clone()])
                .await
                .expect("Should have found and converted v9 config");

        // And make sure it matches the new, directly loaded and converted from
        // disk.
        let new_as_v4: OmicronSledConfigV4 = serde_json::from_str(
            tokio::fs::read_to_string(dst_file).await.unwrap().as_str(),
        )
        .expect("successfully converted config");
        let new_as_v10 = OmicronSledConfigV10::try_from(new_as_v4)
            .expect("successfully converted v4 config to v10");
        let new = OmicronSledConfig::try_from(new_as_v10)
            .expect("successfully converted v10 config to current");
        assert_eq!(new, converted);
        logctx.cleanup_successful();
    }

    #[test]
    fn test_merge_old_configs() {
        let disks: OmicronPhysicalDisksConfig = {
            let mut f = std::fs::File::open(LEGACY_DISKS_PATH)
                .expect("opened disks test data");
            serde_json::from_reader(&mut f).expect("parsed disks test data")
        };
        let datasets: DatasetsConfig = {
            let mut f = std::fs::File::open(LEGACY_DATASETS_PATH)
                .expect("opened datasets test data");
            serde_json::from_reader(&mut f).expect("parsed datasets test data")
        };
        let zones: OmicronZonesConfigLocal = {
            let mut f = std::fs::File::open(LEGACY_ZONES_PATH)
                .expect("opened zones test data");
            serde_json::from_reader(&mut f).expect("parsed zones test data")
        };

        let merged_config =
            merge_old_configs(disks.clone(), datasets.clone(), zones.clone());

        assert_eq!(merged_config.generation, zones.omicron_generation);
        assert_eq!(merged_config.disks.len(), disks.disks.len());
        assert_eq!(merged_config.datasets.len(), datasets.datasets.len());
        assert_eq!(merged_config.zones.len(), zones.zones.len());

        for disk in disks.disks {
            assert_eq!(merged_config.disks.get(&disk.id), Some(&disk));
        }
        for dataset in datasets.datasets.into_values() {
            assert_eq!(merged_config.datasets.get(&dataset.id), Some(&dataset));
        }
        for zone in zones.zones.into_iter().map(|z| z.zone) {
            assert_eq!(merged_config.zones.get(&zone.id), Some(&zone));
        }

        let serialized_merged_config =
            serde_json::to_string_pretty(&merged_config)
                .expect("config always serializes");

        expectorate::assert_contents(
            MERGED_CONFIG_PATH,
            &serialized_merged_config,
        );
    }

    // Helper to read the expected sled config from our combined test data.
    pub(crate) fn test_data_merged_config() -> OmicronSledConfig {
        let mut f = std::fs::File::open(MERGED_CONFIG_PATH)
            .expect("opened merged sled config test data");
        serde_json::from_reader(&mut f).expect("parsed sled config")
    }

    #[tokio::test]
    async fn convert_legacy_ledgers_merges_old_configs() {
        let logctx =
            dev::test_setup_log("convert_legacy_ledgers_merges_old_configs");
        let tempdir = Utf8TempDir::new().expect("created tempdir");

        // Copy the legacy configs into this directory.
        for src in [LEGACY_DISKS_PATH, LEGACY_DATASETS_PATH, LEGACY_ZONES_PATH]
        {
            let src = Utf8Path::new(src);
            let dst = tempdir.path().join(src.file_name().unwrap());

            tokio::fs::copy(src, dst).await.expect("staged file in tempdir");
        }

        // We should get back the merged config.
        let config = match convert_legacy_ledgers(
            &[tempdir.path().to_owned()],
            &logctx.log,
        )
        .await
        {
            Some(config) => {
                assert_eq!(config, test_data_merged_config());
                config
            }
            None => panic!("convert_legacy_ledgers didn't merge configs"),
        };

        // The merged config should also have been written to the "dataset"...
        let merged =
            tokio::fs::read(tempdir.path().join(CONFIG_LEDGER_FILENAME))
                .await
                .expect("merged config written");
        assert_eq!(
            config,
            serde_json::from_slice::<OmicronSledConfig>(&merged)
                .expect("parsed merged config")
        );

        // ... and the legacy configs should have been removed.
        // Copy the legacy configs into this directory.
        for p in [LEGACY_DISKS_PATH, LEGACY_DATASETS_PATH, LEGACY_ZONES_PATH] {
            let p = Utf8Path::new(p);
            let old = tempdir.path().join(p.file_name().unwrap());
            assert!(!old.exists(), "legacy file wasn't removed: {old}");
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn convert_legacy_ledgers_returns_none_if_no_legacy_configs() {
        let logctx = dev::test_setup_log(
            "convert_legacy_ledgers_returns_none_if_no_legacy_configs",
        );
        let tempdir = Utf8TempDir::new().expect("created tempdir");

        match convert_legacy_ledgers(&[tempdir.path().to_owned()], &logctx.log)
            .await
        {
            Some(config) => panic!("unexpected config: {config:?}"),
            None => (),
        }

        logctx.cleanup_successful();
    }

    #[tokio::test]
    #[should_panic(
        expected = "Found partial legacy ledgers; unsafe to proceed"
    )]
    async fn convert_legacy_ledgers_panics_if_partial_legacy_configs() {
        // This test intends to panic, but we still want to cleanup the logctx
        // on that panic. Stuff it into a scopeguard to do so.
        let logctx =
            dev::test_setup_log("convert_legacy_ledgers_merges_old_configs");
        let logctx = scopeguard::guard(logctx, |logctx| {
            logctx.cleanup_successful();
        });

        // Copy just the disk and zones legacy configs; omit datasets. (This
        // test would work equally well if we copied any 1 or 2 of the legacy
        // files; we just pick one such combo.)
        let tempdir = Utf8TempDir::new().expect("created tempdir");
        for src in [LEGACY_DISKS_PATH, LEGACY_ZONES_PATH] {
            let src = Utf8Path::new(src);
            let dst = tempdir.path().join(src.file_name().unwrap());

            tokio::fs::copy(src, dst).await.expect("staged file in tempdir");
        }

        // This call should panic: it's not safe to proceed with startup if we
        // have some but not all three legacy configs.
        _ = convert_legacy_ledgers(&[tempdir.path().to_owned()], &logctx.log)
            .await;

        unreachable!("convert_legacy_ledgers should have panicked");
    }
}
