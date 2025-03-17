// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::CurrentConfig;
use crate::services::OmicronZonesConfigLocal;
use camino::Utf8PathBuf;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use omicron_common::disk::DatasetsConfig;
use omicron_common::disk::OmicronPhysicalDisksConfig;
use omicron_common::ledger::Ledger;
use slog::Logger;
use slog_error_chain::InlineErrorChain;

const CONFIG_LEDGER_FILENAME: &str = "omicron-sled-config.json";

pub async fn load_sled_config(
    config_datasets: &[Utf8PathBuf],
    log: &Logger,
) -> CurrentConfig {
    if config_datasets.is_empty() {
        return CurrentConfig::WaitingForM2Disks;
    }

    // First try to load the ledger from our expect path(s).
    let paths = config_datasets
        .iter()
        .map(|p| p.join(CONFIG_LEDGER_FILENAME))
        .collect();
    if let Some(config) = Ledger::new(log, paths).await {
        info!(log, "Ledger of sled config exists");
        return CurrentConfig::Ledgered(config.into_inner());
    }

    // If we have no ledgered config, see if we can convert from the previous
    // triple of legacy ledgers.
    if let Some(config) = convert_legacy_ledgers(&config_datasets, log).await {
        info!(log, "Converted legacy triple of ledgers into new sled config");
        return CurrentConfig::Ledgered(config);
    }

    // We have no ledger and didn't find legacy ledgers to convert; we must be
    // waiting for RSS.
    info!(log, "No sled config ledger exists");
    CurrentConfig::WaitingForRackSetup
}

const LEGACY_DISKS_LEDGER_FILENAME: &str = "omicron-physical-disks.json";
const LEGACY_DATASETS_LEDGER_FILENAME: &str = "omicron-datasets.json";
const LEGACY_ZONES_LEDGER_FILENAME: &str = "omicron-zones.json";

async fn convert_legacy_ledgers(
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
        // If we have all three or none of the three, our decision is easy.
        (Some(disks), Some(datasets), Some(zones)) => {
            (disks.into_inner(), datasets.into_inner(), zones.into_inner())
        }
        (None, None, None) => return None,

        // Any other combo is terrible: we have one or two legacy ledgers, but
        // are missing the other two or one. Log an error and treat this as "no
        // ledgered config". This should only be possible if we were interrupted
        // mid-rack-setup, which already requires clean-slate'ing (which should
        // remove these config files!).
        (disks, datasets, zones) => {
            error!(
                log,
                "Found partial legacy ledgers; \
                 treating as no ledgered config";
                "found-legacy-disks" => disks.is_some(),
                "found-legacy-datasets" => datasets.is_some(),
                "found-legacy-zones" => zones.is_some(),
            );
            return None;
        }
    };

    // Peform the actual conversion of the legacy configs to the new combo
    // config.
    let config = {
        OmicronSledConfig {
            // Take the zone generation as the overall config generation; this
            // is consistent with Reconfigurator's transition from three configs
            // to one.
            generation: zones.omicron_generation,
            disks: disks.disks.into_iter().collect(),
            datasets: datasets.datasets.into_values().collect(),
            zones: zones.zones.into_iter().map(|z| z.zone).collect(),
        }
    };

    // Write the new config.
    let config_paths = config_datasets
        .iter()
        .map(|p| p.join(CONFIG_LEDGER_FILENAME))
        .collect::<Vec<_>>();
    let mut config_ledger = Ledger::new_with(log, config_paths.clone(), config);
    if let Err(err) = config_ledger.commit().await {
        // We weren't able to write the new ledger, but we were still able to
        // _read_ it (via the old ones). Log this failure but return the config
        // we read; we'll try converting again the next time we run.
        warn!(
            log, "Failed to write sled config ledger built from legacy ledgers";
            InlineErrorChain::new(&err),
        );
        return Some(config_ledger.into_inner());
    }

    // Be paranoid before removing the legacy ledgers: confirm we can read back
    // the new combined config.
    match Ledger::new(log, config_paths.clone()).await {
        Some(reread_config) => {
            // Check that the contents we wrote match the contents we read. No
            // one should be modifying this file concurrently, so a failure here
            // means we've ledgered incorrect data, which could be disasterous.
            // Log an error and at least try remove the ledgers we just wrote,
            // then use the config we cobbled together from the legacy ledgers.
            if config_ledger.data() != reread_config.data() {
                error!(
                    log,
                    "Reading just-ledgered config returns unexpected contents!";
                    "written" => ?config_ledger.data(),
                    "read" => ?reread_config.data(),
                );
                for p in &config_paths {
                    if let Err(err) = tokio::fs::remove_file(p).await {
                        // We're in really big trouble now: we've written a
                        // bogus ledger and can't remove it. Maybe this should
                        // panic instead? (Except then sled-agent will
                        // immediately restart and try to use the bad ledger...)
                        error!(
                            log,
                            "Failed to remove potentially-bogus ledger!";
                            "path" => %p,
                            InlineErrorChain::new(&err),
                        );
                    }
                }
                return Some(config_ledger.into_inner());
            }
        }
        None => {
            // Not much we can do here - log the failure and return before we
            // try to remove the legacy ledgers.
            warn!(log, "Failed to read ledgered config we just wrote!");
            return Some(config_ledger.into_inner());
        }
    }

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

    Some(config_ledger.into_inner())
}
