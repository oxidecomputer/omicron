// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated Crucible upstairs implementation

use super::storage::Storage;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::SledUuid;
use propolis_client::VolumeConstructionRequest;
use slog::Logger;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

#[derive(Default)]
pub(crate) struct SimulatedUpstairsInner {
    id_to_vcr: HashMap<Uuid, VolumeConstructionRequest>,
    sled_to_storage: HashMap<SledUuid, Storage>,
}

#[derive(Clone)]
pub struct SimulatedUpstairs {
    log: Logger,
    inner: Arc<Mutex<SimulatedUpstairsInner>>,
}

impl SimulatedUpstairs {
    pub fn new(log: Logger) -> SimulatedUpstairs {
        SimulatedUpstairs {
            log,
            inner: Arc::new(Mutex::new(SimulatedUpstairsInner::default())),
        }
    }

    pub(crate) fn register_storage(
        &self,
        sled_id: SledUuid,
        storage: &Storage,
    ) {
        let mut inner = self.inner.lock().unwrap();
        inner.sled_to_storage.insert(sled_id, storage.clone());

        info!(self.log, "registered sled {sled_id} storage");
    }

    /// Map id to VCR for later lookup
    pub fn map_id_to_vcr(
        &self,
        id: Uuid,
        volume_construction_request: &VolumeConstructionRequest,
    ) {
        let mut inner = self.inner.lock().unwrap();

        // VCR changes occur all the time due to read-only parent removal and
        // the various replacements. Unconditionally overwrite the previously
        // stored VCR here.
        inner.id_to_vcr.insert(id, volume_construction_request.clone());

        info!(self.log, "mapped vcr with id {id}");
    }

    pub fn snapshot(&self, id: Uuid, snapshot_id: Uuid) -> Result<(), Error> {
        // In order to fulfill the snapshot request, emulate creating snapshots
        // for each region that makes up the disk. Search each simulated sled
        // agent's storage for the target and port, then manually create
        // snapshots here.

        let inner = self.inner.lock().unwrap();

        let volume_construction_request = inner.id_to_vcr.get(&id).unwrap();

        let targets = extract_targets_from_volume_construction_request(
            volume_construction_request,
        )
        .map_err(|e| {
            Error::invalid_request(&format!("bad socketaddr: {e:?}"))
        })?;

        for target in targets {
            let mut snapshot_made_for_target = false;

            for (sled_id, storage) in inner.sled_to_storage.iter() {
                let storage = storage.lock();
                let Some(region) = storage.get_region_for_port(target.port())
                else {
                    continue;
                };

                let region_id = Uuid::from_str(&region.id.0).unwrap();

                // This should be Some if the above found a region!
                let crucible_data =
                    storage.get_dataset_for_region(region_id).unwrap();

                snapshot_made_for_target = true;
                info!(
                    self.log,
                    "found region";
                    "region_id" => %region_id,
                    "snapshot_id" => %snapshot_id,
                    "sled_id" => %sled_id,
                );

                crucible_data
                    .create_snapshot(region_id, snapshot_id)
                    .map_err(|e| Error::internal_error(&e.to_string()))?;

                break;
            }

            if !snapshot_made_for_target {
                return Err(Error::internal_error(&format!(
                    "no region for port {}",
                    target.port()
                )));
            }
        }

        info!(self.log, "successfully created snapshot {snapshot_id}");

        Ok(())
    }
}

fn extract_targets_from_volume_construction_request(
    vcr: &VolumeConstructionRequest,
) -> Result<Vec<SocketAddr>, std::net::AddrParseError> {
    // A snapshot is simply a flush with an extra parameter, and flushes are
    // only sent to sub volumes, not the read only parent. Flushes are only
    // processed by regions, so extract each region that would be affected by a
    // flush.

    let mut res = vec![];
    let mut parts: VecDeque<&VolumeConstructionRequest> = VecDeque::new();
    parts.push_back(vcr);

    while let Some(vcr_part) = parts.pop_front() {
        match vcr_part {
            VolumeConstructionRequest::Volume { sub_volumes, .. } => {
                for sub_volume in sub_volumes {
                    parts.push_back(sub_volume);
                }
            }

            VolumeConstructionRequest::Url { .. } => {
                // noop
            }

            VolumeConstructionRequest::Region { opts, .. } => {
                for target in &opts.target {
                    res.push(*target);
                }
            }

            VolumeConstructionRequest::File { .. } => {
                // noop
            }
        }
    }

    Ok(res)
}
