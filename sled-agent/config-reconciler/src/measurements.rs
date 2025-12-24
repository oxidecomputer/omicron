// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Measurements
use crate::InternalDisks;
use crate::mupdate_override::ResolverStatusExt;
use camino::Utf8PathBuf;
use iddqd::IdOrdMap;
use sled_agent_types::inventory::ConfigReconcilerInventoryResult;
use sled_agent_types::inventory::OmicronSingleMeasurement;
use sled_agent_types::inventory::ReconciledSingleMeasurement;
use sled_agent_types::zone_images::OmicronZoneFileSource;
use sled_agent_types::zone_images::ResolverStatus;
use slog::Logger;

pub struct PreparedOmicronMeasurements {
    pub sources: Vec<OmicronZoneFileSource>,
}

impl PreparedOmicronMeasurements {
    pub fn new(sources: Vec<OmicronZoneFileSource>) -> Self {
        Self { sources }
    }

    pub fn file_sources(&self) -> &Vec<OmicronZoneFileSource> {
        &self.sources
    }
}

pub(crate) async fn reconcile_measurements(
    resolver_status: &ResolverStatus,
    internal_disks: &InternalDisks,
    desired: &Vec<OmicronSingleMeasurement>,
    log: &Logger,
) -> IdOrdMap<ReconciledSingleMeasurement> {
    let set =
        resolver_status.prepare_all_measurements(log, desired, internal_disks);

    let mut unique = IdOrdMap::new();

    for entry in set.sources {
        let mut found = false;
        for path in entry.file_source.search_paths {
            let full_path = path.join(entry.file_source.file_name.clone());
            if let Ok(exists) = tokio::fs::try_exists(&full_path).await {
                if exists {
                    unique
                        .insert_unique(ReconciledSingleMeasurement {
                            file_name: entry.file_source.file_name.clone(),
                            path: full_path,
                            result: ConfigReconcilerInventoryResult::Ok,
                        })
                        .expect("file names should be unique");
                    found = true;
                    break;
                }
            }
        }
        if !found {
            unique
                .insert_unique(ReconciledSingleMeasurement {
                    file_name: entry.file_source.file_name.clone(),
                    path: Utf8PathBuf::new(),
                    result: ConfigReconcilerInventoryResult::Err {
                        message:
                            "The measurement file does not exist in any path"
                                .to_string(),
                    },
                })
                .expect("file names should be unique");
        }
    }

    unique
}
