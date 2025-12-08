// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module for converting older formats of the sled configuration files.

use camino::Utf8PathBuf;
use omicron_common::api::external::Generation;
use omicron_common::disk::DatasetsConfig;
use omicron_common::disk::OmicronPhysicalDisksConfig;
use omicron_common::ledger::Ledger;
use omicron_common::ledger::Ledgerable;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_types::inventory::HostPhase2DesiredSlots;
use sled_agent_types::inventory::OmicronSledConfig;
use sled_agent_types_migrations::v4::inventory::OmicronSledConfig as OmicronSledConfigV4;
use sled_agent_types_migrations::v4::inventory::OmicronZoneConfig as OmicronZoneConfigV4;
use slog::Logger;
use slog::error;
use slog::warn;
use slog_error_chain::InlineErrorChain;

use super::CONFIG_LEDGER_FILENAME;

const LEGACY_DISKS_LEDGER_FILENAME: &str = "omicron-physical-disks.json";
const LEGACY_DATASETS_LEDGER_FILENAME: &str = "omicron-datasets.json";
const LEGACY_ZONES_LEDGER_FILENAME: &str = "omicron-zones.json";

/// Convert from version 8 of the sled-configuration to the current, if
/// possible. The later version includes dual-stack private IP configuration in
/// our internal network interface types.
///
/// # Panics
///
/// This panics if the conversion fails. That might happen if we somehow
/// serialized a NIC with an IPv4 address, but on an IPv6 subnet. That should
/// not be possible, but we have no way of recovering into the current format if
/// we do encounter that. Returning `None` is not correct, since that would
/// incorrectly indicate that we have no config at all. We _must_ panic and rely
/// on support correcting this (believed-to-be-impossible) situation.
pub(super) async fn try_convert_v9_sled_config(
    log: &Logger,
    datasets: Vec<Utf8PathBuf>,
) -> Option<OmicronSledConfig> {
    let old =
        Ledger::<OmicronSledConfigLocal>::new(log, datasets.clone()).await?;
    let new_config = old.into_inner().0.try_into().unwrap_or_else(|e| {
        panic!(
            "Failed to convert OmicronSledConfigV4 to the current version: {e}"
        )
    });
    write_converted_ledger(
        log,
        datasets,
        new_config,
        LegacyKind::SingleStackNic,
    )
    .await
}

/// Convert the legacy triple of sled config files (disks, datasets, and zones)
/// into the current unified [`OmicronSledConfig`].
pub(super) async fn convert_legacy_ledgers(
    config_datasets: &[Utf8PathBuf],
    log: &Logger,
) -> Option<OmicronSledConfig> {
    let disk_paths = config_datasets
        .iter()
        .map(|p| p.join(LEGACY_DISKS_LEDGER_FILENAME))
        .collect::<Vec<_>>();
    let dataset_paths = config_datasets
        .iter()
        .map(|p| p.join(LEGACY_DATASETS_LEDGER_FILENAME))
        .collect::<Vec<_>>();
    let zone_paths = config_datasets
        .iter()
        .map(|p| p.join(LEGACY_ZONES_LEDGER_FILENAME))
        .collect::<Vec<_>>();

    let loaded_ledgers = futures::join!(
        Ledger::<OmicronPhysicalDisksConfig>::new(log, disk_paths.clone()),
        Ledger::<DatasetsConfig>::new(log, dataset_paths.clone()),
        Ledger::<OmicronZonesConfigLocal>::new(log, zone_paths.clone()),
    );

    let (disks, datasets, zones) = match loaded_ledgers {
        // If we have all three, proceed below to our conversion.
        (Some(disks), Some(datasets), Some(zones)) => {
            (disks.into_inner(), datasets.into_inner(), zones.into_inner())
        }

        // If none of them exist, we have no conversion to do.
        (None, None, None) => return None,

        // Any other combo is terrible: we have one or two legacy ledgers, but
        // are missing the other two or one. This should only be possible if we
        // were interrupted mid-rack-setup, which already requires
        // clean-slate'ing (which should remove these config files!). Panic if
        // we hit this case: it is not safe to continue with sled-agent startup
        // if we are partially configured.
        (disks, datasets, zones) => {
            error!(
                log,
                "Found partial legacy ledgers; unsafe to proceed";
                "found-legacy-disks" => disks.is_some(),
                "legacy-disk-paths" => ?disk_paths,
                "found-legacy-datasets" => datasets.is_some(),
                "legacy-dataset-paths" => ?dataset_paths,
                "found-legacy-zones" => zones.is_some(),
                "legacy-zone-paths" => ?zone_paths,
            );
            panic!(
                "Found partial legacy ledgers; unsafe to proceed (\
                 found-legacy-disks: {}, \
                 legacy-disk-paths: {disk_paths:?}, \
                 found-legacy-datasets: {}, \
                 legacy-dataset-paths: {dataset_paths:?}, \
                 found-legacy-zones: {}, \
                 legacy-zone-paths: {zone_paths:?} \
                )",
                disks.is_some(),
                datasets.is_some(),
                zones.is_some(),
            );
        }
    };

    // Perform the actual merge; this is infallible.
    let sled_config = merge_old_configs(disks, datasets, zones);

    // At this point, we've converted from the old, 3-config world into the
    // 1-config world, but we've also converted into an older version of the
    // configuration type itself. The 3-file format predates support for
    // dual-stack network interfaces.
    //
    // Convert the merged configuration into this new format, and write that out
    // instead. This conversion _is_ fallible. Unfortunately, if it fails,
    // there's nothing we can do. That conversion is determinstic, so doing it
    // again won't change the result.
    let sled_config = OmicronSledConfig::try_from(sled_config)
        .unwrap_or_else(|e| panic!(
            "Failed to convert OmicronSledConfigV4 to the current version: {e}"
        ));

    // Write the newly-merged config to disk.
    let new_config_paths = config_datasets
        .iter()
        .map(|p| p.join(CONFIG_LEDGER_FILENAME))
        .collect::<Vec<_>>();
    let new_ledger = write_converted_ledger(
        log,
        new_config_paths,
        sled_config,
        LegacyKind::ThreeLedgerFormat,
    )
    .await?;

    // We've successfully written and reread our new combined config; remove the
    // old legacy ledgers.
    for old_ledger_path in
        disk_paths.iter().chain(dataset_paths.iter()).chain(zone_paths.iter())
    {
        if let Err(err) = tokio::fs::remove_file(old_ledger_path).await {
            // There isn't really anything we can do other than warn here;
            // future attempts to read the ledger will find the combined config
            // we wrote above, so we'll just leak the legacy configs here and
            // rely on support procedures to confirm we don't hit this during
            // the transition period.
            warn!(
                log,
                "Failed to remove legacy ledger";
                "path" => %old_ledger_path,
                InlineErrorChain::new(&err),
            );
        }
    }

    Some(new_ledger)
}

#[derive(Debug)]
enum LegacyKind {
    ThreeLedgerFormat,
    SingleStackNic,
}

async fn write_converted_ledger(
    log: &Logger,
    paths: Vec<Utf8PathBuf>,
    sled_config: OmicronSledConfig,
    kind: LegacyKind,
) -> Option<OmicronSledConfig> {
    let mut config_ledger = Ledger::new_with(log, paths.clone(), sled_config);

    match config_ledger.commit().await {
        Ok(()) => (),
        Err(err) => {
            // We weren't able to write the new ledger, but we were still able
            // to _read_ it (via the old ones). Log this failure but return the
            // config we read; we'll try converting again the next time we run.
            warn!(
                log,
                "Failed to write new sled config ledger built \
                from legacy ledgers";
                "legacy_kind" => ?kind,
                InlineErrorChain::new(&err),
            );
            return Some(config_ledger.into_inner());
        }
    }

    // Be paranoid before removing the legacy ledgers: confirm we can read back
    // the new combined config.
    match Ledger::new(log, paths.clone()).await {
        Some(reread_config) => {
            // Check that the contents we wrote match the contents we read. No
            // one should be modifying this file concurrently, so a failure here
            // means we've ledgered incorrect data, which could be disastrous.
            // Log an error and at least try remove the ledgers we just wrote,
            // then use the config we cobbled together from the legacy ledgers.
            if config_ledger.data() != reread_config.data() {
                error!(
                    log,
                    "Reading just-ledgered config returns unexpected contents!";
                    "legacy_kind" => ?kind,
                    "written" => ?config_ledger.data(),
                    "read" => ?reread_config.data(),
                );
                for p in &paths {
                    if let Err(err) = tokio::fs::remove_file(p).await {
                        // We're in really big trouble now: we've written a
                        // bogus ledger and can't remove it. We cannot safely
                        // proceed with startup.
                        error!(
                            log,
                            "Wrote bogus ledger and then failed to remove it!";
                            "legacy_kind" => ?kind,
                            "path" => %p,
                            InlineErrorChain::new(&err),
                        );
                        panic!(
                            "Wrote bogus ledger and then failed to remove it; \
                             contents of {p} are invalid based on the contents \
                             of the legacy configs! (removal error: {})",
                            InlineErrorChain::new(&err),
                        );
                    }
                }

                // This is a pretty weird case, but we're okay to proceed. We
                // successfully read and converted old ledgers, then somehow
                // wrote bad combined ledgers, but then successfully _removed_
                // those bad ledgers. We're back to the state we were in when we
                // started: the three legacy configs are still present, and we
                // know what our config should be. Return that config, and next
                // time we run we'll try converting again.
                return Some(config_ledger.into_inner());
            }
        }
        None => {
            // Not much we can do here - log the failure and return before we
            // try to remove the legacy ledgers.
            warn!(
                log, "Failed to read ledgered config we just wrote!";
                "legacy_kind" => ?kind,
                "paths" => ?paths,
            );
            return Some(config_ledger.into_inner());
        }
    }

    Some(config_ledger.into_inner())
}

fn merge_old_configs(
    disks: OmicronPhysicalDisksConfig,
    datasets: DatasetsConfig,
    zones: OmicronZonesConfigLocal,
) -> OmicronSledConfigV4 {
    OmicronSledConfigV4 {
        // Take the zone generation as the overall config generation; this is
        // consistent with Reconfigurator's transition from three configs to
        // one.
        generation: zones.omicron_generation,
        disks: disks.disks.into_iter().collect(),
        datasets: datasets.datasets.into_values().collect(),
        zones: zones.zones.into_iter().map(|z| z.zone).collect(),
        // Old configs are pre-mupdate overrides.
        remove_mupdate_override: None,
        // Old configs are pre-host-phase-2 knowledge.
        host_phase_2: HostPhase2DesiredSlots::current_contents(),
    }
}

/// Legacy type of the ledgered zone config.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
struct OmicronZonesConfigLocal {
    omicron_generation: Generation,
    ledger_generation: Generation,
    zones: Vec<OmicronZoneConfigLocal>,
}

impl Ledgerable for OmicronZonesConfigLocal {
    fn is_newer_than(&self, other: &OmicronZonesConfigLocal) -> bool {
        self.omicron_generation > other.omicron_generation
            || (self.omicron_generation == other.omicron_generation
                && self.ledger_generation >= other.ledger_generation)
    }

    fn generation_bump(&mut self) {
        self.ledger_generation = self.ledger_generation.next();
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
struct OmicronZoneConfigLocal {
    zone: OmicronZoneConfigV4,
    #[serde(rename = "root")]
    #[cfg_attr(test, schemars(with = "String"))]
    _root: Utf8PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(transparent)]
struct OmicronSledConfigLocal(OmicronSledConfigV4);

impl Ledgerable for OmicronSledConfigLocal {
    fn is_newer_than(&self, other: &Self) -> bool {
        self.0.generation > other.0.generation
    }

    fn generation_bump(&mut self) {
        self.0.generation = self.0.generation.next()
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
            try_convert_v9_sled_config(&logctx.log, vec![dst_file.clone()])
                .await
                .expect("Should have found and converted v9 config");

        // And make sure it matches the new, directly loaded and converted from
        // disk.
        let new_as_v4: OmicronSledConfigV4 = serde_json::from_str(
            tokio::fs::read_to_string(dst_file).await.unwrap().as_str(),
        )
        .expect("successfully converted config");
        let new = OmicronSledConfig::try_from(new_as_v4)
            .expect("successfully converted v9 config");
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
