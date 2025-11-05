// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent storage implementation
//!
//! Note, this refers to the "storage which exists on the Sled", rather
//! than the representation of "virtual disks" which would be presented
//! through Nexus' external API.

use crate::sim::SimulatedUpstairs;
use crate::sim::http_entrypoints_pantry::ExpectedDigest;
use crate::sim::http_entrypoints_pantry::PantryStatus;
use crate::sim::http_entrypoints_pantry::VolumeStatus;
use crate::support_bundle::storage::SupportBundleManager;
use anyhow::{self, Result, bail};
use camino::Utf8Path;
use camino_tempfile::Utf8TempDir;
use chrono::prelude::*;
use crucible_agent_client::types::{
    CreateRegion, Region, RegionId, RunningSnapshot, Snapshot, State,
};
use dropshot::HandlerTaskMode;
use dropshot::HttpError;
use illumos_utils::zfs::DatasetProperties;
use omicron_common::api::external::ByteCount;
use omicron_common::disk::DatasetManagementStatus;
use omicron_common::disk::DatasetName;
use omicron_common::disk::DatasetsConfig;
use omicron_common::disk::DatasetsManagementResult;
use omicron_common::disk::DiskIdentity;
use omicron_common::disk::DiskManagementStatus;
use omicron_common::disk::DiskVariant;
use omicron_common::disk::DisksManagementResult;
use omicron_common::disk::OmicronPhysicalDisksConfig;
use omicron_common::disk::SharedDatasetConfig;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::ZpoolUuid;
use propolis_client::VolumeConstructionRequest;
use serde::Serialize;
use sled_storage::nested_dataset::NestedDatasetConfig;
use sled_storage::nested_dataset::NestedDatasetListOptions;
use sled_storage::nested_dataset::NestedDatasetLocation;
use slog::Logger;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

type CreateCallback = Box<dyn Fn(&CreateRegion) -> State + Send + 'static>;

#[derive(Serialize)]
struct CrucibleDataInner {
    #[serde(skip)]
    log: Logger,
    regions: HashMap<Uuid, Region>,
    snapshots: HashMap<Uuid, HashMap<String, Snapshot>>,
    running_snapshots: HashMap<Uuid, HashMap<String, RunningSnapshot>>,
    #[serde(skip)]
    on_create: Option<CreateCallback>,
    region_creation_error: bool,
    region_deletion_error: bool,
    creating_a_running_snapshot_should_fail: bool,
    start_port: u16,
    end_port: u16,
    used_ports: HashSet<u16>,
}

/// Returns Some with a string if there's a mismatch
pub fn mismatch(params: &CreateRegion, r: &Region) -> Option<String> {
    if params.block_size != r.block_size {
        Some(format!(
            "requested block size {} instead of {}",
            params.block_size, r.block_size
        ))
    } else if params.extent_size != r.extent_size {
        Some(format!(
            "requested extent size {} instead of {}",
            params.extent_size, r.extent_size
        ))
    } else if params.extent_count != r.extent_count {
        Some(format!(
            "requested extent count {} instead of {}",
            params.extent_count, r.extent_count
        ))
    } else if params.encrypted != r.encrypted {
        Some(format!(
            "requested encrypted {} instead of {}",
            params.encrypted, r.encrypted
        ))
    } else if params.cert_pem != r.cert_pem {
        Some(format!(
            "requested cert_pem {:?} instead of {:?}",
            params.cert_pem, r.cert_pem
        ))
    } else if params.key_pem != r.key_pem {
        Some(format!(
            "requested key_pem {:?} instead of {:?}",
            params.key_pem, r.key_pem
        ))
    } else if params.root_pem != r.root_pem {
        Some(format!(
            "requested root_pem {:?} instead of {:?}",
            params.root_pem, r.root_pem
        ))
    } else if params.source != r.source {
        Some(format!(
            "requested source {:?} instead of {:?}",
            params.source, r.source
        ))
    } else {
        None
    }
}

impl CrucibleDataInner {
    fn new(log: Logger, start_port: u16, end_port: u16) -> Self {
        Self {
            log,
            regions: HashMap::new(),
            snapshots: HashMap::new(),
            running_snapshots: HashMap::new(),
            on_create: None,
            region_creation_error: false,
            region_deletion_error: false,
            creating_a_running_snapshot_should_fail: false,
            start_port,
            end_port,
            used_ports: HashSet::new(),
        }
    }

    fn set_create_callback(&mut self, callback: CreateCallback) {
        self.on_create = Some(callback);
    }

    fn list(&self) -> Vec<Region> {
        self.regions.values().cloned().collect()
    }

    fn get_free_port(&mut self) -> u16 {
        for port in self.start_port..self.end_port {
            if self.used_ports.contains(&port) {
                continue;
            }
            self.used_ports.insert(port);
            return port;
        }

        panic!("no free ports for simulated crucible agent!");
    }

    fn create(&mut self, params: CreateRegion) -> Result<Region, HttpError> {
        let id = Uuid::from_str(&params.id.0).unwrap();

        let state = if let Some(on_create) = &self.on_create {
            on_create(&params)
        } else {
            State::Requested
        };

        if self.region_creation_error {
            return Err(HttpError::for_internal_error(
                "region creation error!".to_string(),
            ));
        }

        if let Some(region) = self.regions.get(&id) {
            if let Some(mismatch) = mismatch(&params, &region) {
                let s = format!(
                    "region {region:?} already exists as {params:?}: {mismatch}"
                );
                warn!(self.log, "{s}");
                return Err(HttpError::for_client_error(
                    None,
                    dropshot::ClientErrorStatusCode::CONFLICT,
                    s,
                ));
            }
        }

        let read_only = params.source.is_some();

        let region = Region {
            id: params.id,
            block_size: params.block_size,
            extent_size: params.extent_size,
            extent_count: params.extent_count,
            // NOTE: This is a lie - no server is running.
            port_number: self.get_free_port(),
            state,
            encrypted: params.encrypted,
            cert_pem: params.cert_pem,
            key_pem: params.key_pem,
            root_pem: params.root_pem,
            source: params.source,
            read_only,
        };

        let old = self.regions.insert(id, region.clone());

        if let Some(old) = old {
            assert_eq!(
                old.id.0, region.id.0,
                "Region already exists, but with a different ID"
            );
        }

        info!(self.log, "created region {}", region.id);

        Ok(region)
    }

    fn get(&self, id: RegionId) -> Option<Region> {
        let id = Uuid::from_str(&id.0).unwrap();
        self.regions.get(&id).cloned()
    }

    fn delete(&mut self, id: RegionId) -> Result<Option<Region>> {
        // Can't delete a ZFS dataset if there are snapshots
        if !self.snapshots_for_region(&id).is_empty() {
            bail!(
                "must delete snapshots {:?} first!",
                self.snapshots_for_region(&id)
                    .into_iter()
                    .map(|s| s.name)
                    .collect::<Vec<String>>(),
            );
        }

        if self.region_deletion_error {
            bail!("region deletion error!");
        }

        let id = Uuid::from_str(&id.0).unwrap();
        if let Some(region) = self.regions.get_mut(&id) {
            region.state = State::Destroyed;
            self.used_ports.remove(&region.port_number);
            info!(self.log, "deleted region {:?}", region.id);
            Ok(Some(region.clone()))
        } else {
            Ok(None)
        }
    }

    fn create_snapshot(
        &mut self,
        id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<Snapshot> {
        info!(self.log, "creating region {} snapshot {}", id, snapshot_id);

        if let Some(region) = self.get(RegionId(id.to_string())) {
            match region.state {
                State::Failed | State::Destroyed | State::Tombstoned => {
                    bail!(
                        "cannot create snapshot of region {id:?} in state {:?}",
                        region.state
                    );
                }

                State::Requested | State::Created => {
                    // ok
                }
            }
        } else {
            bail!("cannot create snapshot of non-existent region {id:?}!");
        }

        Ok(self
            .snapshots
            .entry(id)
            .or_insert_with(|| HashMap::new())
            .entry(snapshot_id.to_string())
            .or_insert_with(|| Snapshot {
                name: snapshot_id.to_string(),
                created: Utc::now(),
            })
            .clone())
    }

    fn snapshots_for_region(&self, id: &RegionId) -> Vec<Snapshot> {
        let id = Uuid::from_str(&id.0).unwrap();
        match self.snapshots.get(&id) {
            Some(map) => map.values().cloned().collect(),
            None => vec![],
        }
    }

    fn get_snapshot_for_region(
        &self,
        id: &RegionId,
        snapshot_id: &str,
    ) -> Option<Snapshot> {
        let id = Uuid::from_str(&id.0).unwrap();
        self.snapshots
            .get(&id)
            .and_then(|hm| hm.get(&snapshot_id.to_string()).cloned())
    }

    fn running_snapshots_for_id(
        &self,
        id: &RegionId,
    ) -> HashMap<String, RunningSnapshot> {
        let id = Uuid::from_str(&id.0).unwrap();
        match self.running_snapshots.get(&id) {
            Some(map) => map.clone(),
            None => HashMap::new(),
        }
    }

    fn delete_snapshot(&mut self, id: &RegionId, name: &str) -> Result<()> {
        let running_snapshots_for_id = self.running_snapshots_for_id(id);
        if let Some(running_snapshot) = running_snapshots_for_id.get(name) {
            match &running_snapshot.state {
                State::Created | State::Requested | State::Tombstoned => {
                    bail!(
                        "downstairs running for region {} snapshot {}",
                        id.0,
                        name
                    );
                }

                State::Destroyed => {
                    // ok
                }

                State::Failed => {
                    bail!(
                        "failed downstairs running for region {} snapshot {}",
                        id.0,
                        name
                    );
                }
            }
        }

        info!(self.log, "Deleting region {} snapshot {}", id.0, name);

        let region_id = Uuid::from_str(&id.0).unwrap();
        if let Some(map) = self.snapshots.get_mut(&region_id) {
            map.remove(name);
        } else {
            bail!("trying to delete snapshot for non-existent region!");
        }

        Ok(())
    }

    fn set_creating_a_running_snapshot_should_fail(&mut self) {
        self.creating_a_running_snapshot_should_fail = true;
    }

    fn set_region_creation_error(&mut self, value: bool) {
        self.region_creation_error = value;
    }

    fn set_region_deletion_error(&mut self, value: bool) {
        self.region_deletion_error = value;
    }

    fn create_running_snapshot(
        &mut self,
        id: &RegionId,
        name: &str,
    ) -> Result<RunningSnapshot> {
        if self.creating_a_running_snapshot_should_fail {
            bail!("failure creating running snapshot");
        }

        if self.get_snapshot_for_region(id, name).is_none() {
            bail!("cannot create running snapshot, snapshot does not exist!");
        }

        let id = Uuid::from_str(&id.0).unwrap();

        let map =
            self.running_snapshots.entry(id).or_insert_with(|| HashMap::new());

        // If a running snapshot exists already, return it - this endpoint must
        // be idempotent.
        if let Some(running_snapshot) = map.get(&name.to_string()) {
            return Ok(running_snapshot.clone());
        }

        let port_number = self.get_free_port();

        let running_snapshot = RunningSnapshot {
            id: RegionId(Uuid::new_v4().to_string()),
            name: name.to_string(),
            port_number,
            state: State::Created,
        };

        let map =
            self.running_snapshots.entry(id).or_insert_with(|| HashMap::new());
        map.insert(name.to_string(), running_snapshot.clone());

        Ok(running_snapshot)
    }

    fn delete_running_snapshot(
        &mut self,
        id: &RegionId,
        name: &str,
    ) -> Result<()> {
        let id = Uuid::from_str(&id.0).unwrap();

        let map =
            self.running_snapshots.entry(id).or_insert_with(|| HashMap::new());

        if let Some(running_snapshot) = map.get_mut(&name.to_string()) {
            running_snapshot.state = State::Destroyed;
            self.used_ports.remove(&running_snapshot.port_number);
        }

        Ok(())
    }

    /// Return true if there are no undeleted Crucible resources
    pub fn is_empty(&self) -> bool {
        let non_destroyed_regions = self
            .regions
            .values()
            .filter(|r| r.state != State::Destroyed)
            .count();

        let snapshots = self.snapshots.values().flatten().count();

        let running_snapshots = self
            .running_snapshots
            .values()
            .flat_map(|hm| hm.values())
            .filter(|rs| rs.state != State::Destroyed)
            .count();

        let empty = non_destroyed_regions == 0
            && snapshots == 0
            && running_snapshots == 0;

        if !empty {
            info!(
                self.log,
                "is_empty state: {:?}",
                serde_json::to_string(&self).unwrap(),
            );

            info!(
                self.log,
                "is_empty non_destroyed_regions {} snapshots {} running_snapshots {}",
                non_destroyed_regions,
                snapshots,
                running_snapshots,
            );
        }

        empty
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use omicron_common::api::external::Generation;
    use omicron_common::disk::DatasetConfig;
    use omicron_common::disk::DatasetKind;
    use omicron_common::disk::DatasetName;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev::test_setup_log;

    /// Validate that the simulated Crucible agent reuses ports when regions are
    /// deleted.
    #[test]
    fn crucible_ports_get_reused() {
        let logctx = test_setup_log("crucible_ports_get_reused");
        let mut agent = CrucibleDataInner::new(logctx.log.clone(), 1000, 2000);

        // Create a region, then delete it.

        let region_id = Uuid::new_v4();
        let region = agent
            .create(CreateRegion {
                block_size: 512,
                extent_count: 10,
                extent_size: 10,
                id: RegionId(region_id.to_string()),
                encrypted: true,
                cert_pem: None,
                key_pem: None,
                root_pem: None,
                source: None,
            })
            .unwrap();

        let first_region_port = region.port_number;

        assert!(
            agent.delete(RegionId(region_id.to_string())).unwrap().is_some()
        );

        // Create another region, make sure it gets the same port number, but
        // don't delete it.

        let second_region_id = Uuid::new_v4();
        let second_region = agent
            .create(CreateRegion {
                block_size: 512,
                extent_count: 10,
                extent_size: 10,
                id: RegionId(second_region_id.to_string()),
                encrypted: true,
                cert_pem: None,
                key_pem: None,
                root_pem: None,
                source: None,
            })
            .unwrap();

        assert_eq!(second_region.port_number, first_region_port,);

        // Create another region, delete it. After this, we still have the
        // second region.

        let third_region = agent
            .create(CreateRegion {
                block_size: 512,
                extent_count: 10,
                extent_size: 10,
                id: RegionId(Uuid::new_v4().to_string()),
                encrypted: true,
                cert_pem: None,
                key_pem: None,
                root_pem: None,
                source: None,
            })
            .unwrap();

        let third_region_port = third_region.port_number;

        assert!(
            agent
                .delete(RegionId(third_region.id.to_string()))
                .unwrap()
                .is_some()
        );

        // Create a running snapshot, make sure it gets the same port number
        // as the third region did. This ensures that the Crucible agent shares
        // ports between regions and running snapshots.

        let snapshot_id = Uuid::new_v4();
        agent.create_snapshot(second_region_id, snapshot_id).unwrap();

        let running_snapshot = agent
            .create_running_snapshot(
                &RegionId(second_region_id.to_string()),
                &snapshot_id.to_string(),
            )
            .unwrap();

        assert_eq!(running_snapshot.port_number, third_region_port,);

        logctx.cleanup_successful();
    }

    /// Validate that users must delete snapshots before deleting the region
    #[test]
    fn must_delete_snapshots_first() {
        let logctx = test_setup_log("must_delete_snapshots_first");
        let mut agent = CrucibleDataInner::new(logctx.log.clone(), 1000, 2000);

        let region_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        let _region = agent.create(CreateRegion {
            block_size: 512,
            extent_count: 10,
            extent_size: 10,
            id: RegionId(region_id.to_string()),
            encrypted: true,
            cert_pem: None,
            key_pem: None,
            root_pem: None,
            source: None,
        });

        agent.create_snapshot(region_id, snapshot_id).unwrap();

        agent.delete(RegionId(region_id.to_string())).unwrap_err();

        logctx.cleanup_successful();
    }

    /// Validate that users cannot delete snapshots before deleting the "running
    /// snapshots" (the read-only downstairs for that snapshot)
    #[test]
    fn must_delete_read_only_downstairs_first() {
        let logctx = test_setup_log("must_delete_read_only_downstairs_first");
        let mut agent = CrucibleDataInner::new(logctx.log.clone(), 1000, 2000);

        let region_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        let _region = agent.create(CreateRegion {
            block_size: 512,
            extent_count: 10,
            extent_size: 10,
            id: RegionId(region_id.to_string()),
            encrypted: true,
            cert_pem: None,
            key_pem: None,
            root_pem: None,
            source: None,
        });

        agent.create_snapshot(region_id, snapshot_id).unwrap();

        agent
            .create_running_snapshot(
                &RegionId(region_id.to_string()),
                &snapshot_id.to_string(),
            )
            .unwrap();

        agent
            .delete_snapshot(
                &RegionId(region_id.to_string()),
                &snapshot_id.to_string(),
            )
            .unwrap_err();

        logctx.cleanup_successful();
    }

    /// Validate that users cannot boot a read-only downstairs for a snapshot
    /// that does not exist.
    #[test]
    fn cannot_boot_read_only_downstairs_with_no_snapshot() {
        let logctx =
            test_setup_log("cannot_boot_read_only_downstairs_with_no_snapshot");
        let mut agent = CrucibleDataInner::new(logctx.log.clone(), 1000, 2000);

        let region_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        let _region = agent.create(CreateRegion {
            block_size: 512,
            extent_count: 10,
            extent_size: 10,
            id: RegionId(region_id.to_string()),
            encrypted: true,
            cert_pem: None,
            key_pem: None,
            root_pem: None,
            source: None,
        });

        agent
            .create_running_snapshot(
                &RegionId(region_id.to_string()),
                &snapshot_id.to_string(),
            )
            .unwrap_err();

        logctx.cleanup_successful();
    }

    /// Validate that users cannot create a snapshot from a non-existent region
    #[test]
    fn snapshot_needs_region() {
        let logctx = test_setup_log("snapshot_needs_region");
        let mut agent = CrucibleDataInner::new(logctx.log.clone(), 1000, 2000);

        let region_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        agent.create_snapshot(region_id, snapshot_id).unwrap_err();

        logctx.cleanup_successful();
    }

    /// Validate that users cannot create a "running" snapshot from a
    /// non-existent region
    #[test]
    fn running_snapshot_needs_region() {
        let logctx = test_setup_log("snapshot_needs_region");
        let mut agent = CrucibleDataInner::new(logctx.log.clone(), 1000, 2000);

        let region_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        agent
            .create_running_snapshot(
                &RegionId(region_id.to_string()),
                &snapshot_id.to_string(),
            )
            .unwrap_err();

        logctx.cleanup_successful();
    }

    /// Validate that users cannot create snapshots for destroyed regions
    #[test]
    fn cannot_create_snapshot_for_destroyed_region() {
        let logctx =
            test_setup_log("cannot_create_snapshot_for_destroyed_region");
        let mut agent = CrucibleDataInner::new(logctx.log.clone(), 1000, 2000);

        let region_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();

        let _region = agent.create(CreateRegion {
            block_size: 512,
            extent_count: 10,
            extent_size: 10,
            id: RegionId(region_id.to_string()),
            encrypted: true,
            cert_pem: None,
            key_pem: None,
            root_pem: None,
            source: None,
        });

        agent.delete(RegionId(region_id.to_string())).unwrap();

        agent.create_snapshot(region_id, snapshot_id).unwrap_err();

        logctx.cleanup_successful();
    }

    #[test]
    fn nested_dataset_not_found_missing_dataset() {
        let logctx = test_setup_log("nested_dataset_not_found_missing_dataset");

        let storage = StorageInner::new(
            Uuid::new_v4(),
            0,
            std::net::Ipv4Addr::LOCALHOST.into(),
            logctx.log.clone(),
        );

        let zpool_id = ZpoolUuid::new_v4();

        let err = storage
            .nested_dataset_list(
                NestedDatasetLocation {
                    path: String::new(),
                    root: DatasetName::new(
                        ZpoolName::new_external(zpool_id),
                        DatasetKind::Debug,
                    ),
                },
                NestedDatasetListOptions::SelfAndChildren,
            )
            .expect_err("Nested dataset listing should fail on fake dataset");

        assert_eq!(err.status_code, 404);

        logctx.cleanup_successful();
    }

    #[test]
    fn nested_dataset() {
        let logctx = test_setup_log("nested_dataset");

        let mut storage = StorageInner::new(
            Uuid::new_v4(),
            0,
            std::net::Ipv4Addr::LOCALHOST.into(),
            logctx.log.clone(),
        );

        let zpool_id = ZpoolUuid::new_v4();
        let zpool_name = ZpoolName::new_external(zpool_id);
        let dataset_id = DatasetUuid::new_v4();
        let dataset_name = DatasetName::new(zpool_name, DatasetKind::Debug);

        let config = DatasetsConfig {
            generation: Generation::new(),
            datasets: BTreeMap::from([(
                dataset_id,
                DatasetConfig {
                    id: dataset_id,
                    name: dataset_name.clone(),
                    inner: SharedDatasetConfig::default(),
                },
            )]),
        };

        // Create the debug dataset on which we'll store everything else.
        let result = storage.datasets_ensure(config).unwrap();
        assert!(!result.has_error());

        // The list of nested datasets should only contain the root dataset.
        let nested_datasets = storage
            .nested_dataset_list(
                NestedDatasetLocation {
                    path: String::new(),
                    root: dataset_name.clone(),
                },
                NestedDatasetListOptions::SelfAndChildren,
            )
            .unwrap();
        assert_eq!(
            nested_datasets,
            vec![NestedDatasetConfig {
                name: NestedDatasetLocation {
                    path: String::new(),
                    root: dataset_name.clone(),
                },
                inner: SharedDatasetConfig::default(),
            }]
        );

        // Or, if we're requesting children explicitly, it should be empty.
        let nested_dataset_root = NestedDatasetLocation {
            path: String::new(),
            root: dataset_name.clone(),
        };

        let nested_datasets = storage
            .nested_dataset_list(
                nested_dataset_root.clone(),
                NestedDatasetListOptions::ChildrenOnly,
            )
            .unwrap();
        assert_eq!(nested_datasets, vec![]);

        // We can request a nested dataset explicitly.
        let foo_config = NestedDatasetConfig {
            name: NestedDatasetLocation {
                path: "foo".into(),
                root: dataset_name.clone(),
            },
            inner: SharedDatasetConfig::default(),
        };
        storage.nested_dataset_ensure(foo_config.clone()).unwrap();
        let foobar_config = NestedDatasetConfig {
            name: NestedDatasetLocation {
                path: "foo/bar".into(),
                root: dataset_name.clone(),
            },
            inner: SharedDatasetConfig::default(),
        };
        storage.nested_dataset_ensure(foobar_config.clone()).unwrap();

        // We can observe the nested datasets we just created
        let nested_datasets = storage
            .nested_dataset_list(
                nested_dataset_root.clone(),
                NestedDatasetListOptions::ChildrenOnly,
            )
            .unwrap();
        assert_eq!(nested_datasets, vec![foo_config.clone(),]);

        let nested_datasets = storage
            .nested_dataset_list(
                foo_config.name.clone(),
                NestedDatasetListOptions::ChildrenOnly,
            )
            .unwrap();
        assert_eq!(nested_datasets, vec![foobar_config.clone(),]);

        // We can destroy nested datasets too
        storage.nested_dataset_destroy(foobar_config.name.clone()).unwrap();
        storage.nested_dataset_destroy(foo_config.name.clone()).unwrap();

        logctx.cleanup_successful();
    }

    #[test]
    fn nested_dataset_child_parent_relationship() {
        let logctx = test_setup_log("nested_dataset_child_parent_relationship");

        let mut storage = StorageInner::new(
            Uuid::new_v4(),
            0,
            std::net::Ipv4Addr::LOCALHOST.into(),
            logctx.log.clone(),
        );

        let zpool_id = ZpoolUuid::new_v4();
        let zpool_name = ZpoolName::new_external(zpool_id);
        let dataset_id = DatasetUuid::new_v4();
        let dataset_name = DatasetName::new(zpool_name, DatasetKind::Debug);

        let config = DatasetsConfig {
            generation: Generation::new(),
            datasets: BTreeMap::from([(
                dataset_id,
                DatasetConfig {
                    id: dataset_id,
                    name: dataset_name.clone(),
                    inner: SharedDatasetConfig::default(),
                },
            )]),
        };

        // Create the debug dataset on which we'll store everything else.
        let result = storage.datasets_ensure(config).unwrap();
        assert!(!result.has_error());
        let nested_dataset_root = NestedDatasetLocation {
            path: String::new(),
            root: dataset_name.clone(),
        };
        let nested_datasets = storage
            .nested_dataset_list(
                nested_dataset_root.clone(),
                NestedDatasetListOptions::ChildrenOnly,
            )
            .unwrap();
        assert_eq!(nested_datasets, vec![]);

        // If we try to create a nested dataset "foo/bar" before the parent
        // "foo", we expect an error.

        let foo_config = NestedDatasetConfig {
            name: NestedDatasetLocation {
                path: "foo".into(),
                root: dataset_name.clone(),
            },
            inner: SharedDatasetConfig::default(),
        };
        let foobar_config = NestedDatasetConfig {
            name: NestedDatasetLocation {
                path: "foo/bar".into(),
                root: dataset_name.clone(),
            },
            inner: SharedDatasetConfig::default(),
        };

        let err = storage
            .nested_dataset_ensure(foobar_config.clone())
            .expect_err("Should have failed to provision foo/bar before foo");
        assert_eq!(err.status_code, 404);

        // Try again, but creating them successfully this time.
        storage.nested_dataset_ensure(foo_config.clone()).unwrap();
        storage.nested_dataset_ensure(foobar_config.clone()).unwrap();

        // We can observe the nested datasets we just created
        let nested_datasets = storage
            .nested_dataset_list(
                nested_dataset_root.clone(),
                NestedDatasetListOptions::ChildrenOnly,
            )
            .unwrap();
        assert_eq!(nested_datasets, vec![foo_config.clone(),]);
        let nested_datasets = storage
            .nested_dataset_list(
                foo_config.name.clone(),
                NestedDatasetListOptions::ChildrenOnly,
            )
            .unwrap();
        assert_eq!(nested_datasets, vec![foobar_config.clone(),]);

        // Destroying the nested dataset parent should destroy children.
        storage.nested_dataset_destroy(foo_config.name.clone()).unwrap();

        let nested_datasets = storage
            .nested_dataset_list(
                nested_dataset_root.clone(),
                NestedDatasetListOptions::ChildrenOnly,
            )
            .unwrap();
        assert_eq!(nested_datasets, vec![]);

        logctx.cleanup_successful();
    }
}

/// Represents a running Crucible Agent. Contains regions.
pub struct CrucibleData {
    inner: Mutex<CrucibleDataInner>,
}

impl CrucibleData {
    fn new(log: Logger, start_port: u16, end_port: u16) -> Self {
        Self {
            inner: Mutex::new(CrucibleDataInner::new(
                log, start_port, end_port,
            )),
        }
    }

    pub fn set_create_callback(&self, callback: CreateCallback) {
        self.inner.lock().unwrap().set_create_callback(callback);
    }

    pub fn list(&self) -> Vec<Region> {
        self.inner.lock().unwrap().list()
    }

    pub fn create(&self, params: CreateRegion) -> Result<Region, HttpError> {
        self.inner.lock().unwrap().create(params)
    }

    pub fn get(&self, id: RegionId) -> Option<Region> {
        self.inner.lock().unwrap().get(id)
    }

    pub fn delete(&self, id: RegionId) -> Result<Option<Region>> {
        self.inner.lock().unwrap().delete(id)
    }

    pub fn create_snapshot(
        &self,
        id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<Snapshot> {
        self.inner.lock().unwrap().create_snapshot(id, snapshot_id)
    }

    pub fn snapshots_for_region(&self, id: &RegionId) -> Vec<Snapshot> {
        self.inner.lock().unwrap().snapshots_for_region(id)
    }

    pub fn get_snapshot_for_region(
        &self,
        id: &RegionId,
        snapshot_id: &str,
    ) -> Option<Snapshot> {
        self.inner.lock().unwrap().get_snapshot_for_region(id, snapshot_id)
    }

    pub fn running_snapshots_for_id(
        &self,
        id: &RegionId,
    ) -> HashMap<String, RunningSnapshot> {
        self.inner.lock().unwrap().running_snapshots_for_id(id)
    }

    pub fn delete_snapshot(&self, id: &RegionId, name: &str) -> Result<()> {
        self.inner.lock().unwrap().delete_snapshot(id, name)
    }

    pub fn set_creating_a_running_snapshot_should_fail(&self) {
        self.inner
            .lock()
            .unwrap()
            .set_creating_a_running_snapshot_should_fail();
    }

    pub fn set_region_creation_error(&self, value: bool) {
        self.inner.lock().unwrap().set_region_creation_error(value);
    }

    pub fn set_region_deletion_error(&self, value: bool) {
        self.inner.lock().unwrap().set_region_deletion_error(value);
    }

    pub fn create_running_snapshot(
        &self,
        id: &RegionId,
        name: &str,
    ) -> Result<RunningSnapshot> {
        self.inner.lock().unwrap().create_running_snapshot(id, name)
    }

    pub fn delete_running_snapshot(
        &self,
        id: &RegionId,
        name: &str,
    ) -> Result<()> {
        self.inner.lock().unwrap().delete_running_snapshot(id, name)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

/// A simulated Crucible Dataset.
///
/// Contains both the data and the HTTP server.
pub struct CrucibleServer {
    server: dropshot::HttpServer<Arc<CrucibleData>>,
    data: Arc<CrucibleData>,
}

impl CrucibleServer {
    fn new(
        log: &Logger,
        crucible_ip: IpAddr,
        start_port: u16,
        end_port: u16,
    ) -> Self {
        // SocketAddr::new with port set to 0 will grab any open port to host
        // the emulated crucible agent, but set the fake downstairs listen ports
        // to start at `crucible_port`.
        let data = Arc::new(CrucibleData::new(
            log.new(slog::o!("start_port" => format!("{start_port}"), "end_port" => format!("{end_port}"))),
            start_port, end_port,
        ));
        let config = dropshot::ConfigDropshot {
            bind_address: SocketAddr::new(crucible_ip, 0),
            ..Default::default()
        };
        let dropshot_log = log
            .new(o!("component" => "Simulated CrucibleAgent Dropshot Server"));
        let server = dropshot::ServerBuilder::new(
            super::http_entrypoints_storage::api(),
            data.clone(),
            dropshot_log,
        )
        .config(config)
        .start()
        .expect("Could not initialize server");
        info!(&log, "Created Simulated Crucible Server"; "address" => server.local_addr());

        CrucibleServer { server, data }
    }

    fn address(&self) -> SocketAddr {
        self.server.local_addr()
    }

    pub fn data(&self) -> Arc<CrucibleData> {
        self.data.clone()
    }
}

pub(crate) struct PhysicalDisk {
    pub(crate) identity: DiskIdentity,
    pub(crate) variant: DiskVariant,
    pub(crate) slot: i64,
}

/// Describes data being simulated within a dataset.
pub(crate) enum DatasetContents {
    Crucible(CrucibleServer),
}

pub(crate) struct Zpool {
    id: ZpoolUuid,
    physical_disk_id: PhysicalDiskUuid,
    total_size: u64,
    datasets: HashMap<DatasetUuid, DatasetContents>,
}

impl Zpool {
    fn new(
        id: ZpoolUuid,
        physical_disk_id: PhysicalDiskUuid,
        total_size: u64,
    ) -> Self {
        Zpool { id, physical_disk_id, total_size, datasets: HashMap::new() }
    }

    fn insert_crucible_dataset(
        &mut self,
        log: &Logger,
        id: DatasetUuid,
        crucible_ip: IpAddr,
        start_port: u16,
        end_port: u16,
    ) -> &CrucibleServer {
        self.datasets.insert(
            id,
            DatasetContents::Crucible(CrucibleServer::new(
                log,
                crucible_ip,
                start_port,
                end_port,
            )),
        );
        let DatasetContents::Crucible(crucible) = self
            .datasets
            .get(&id)
            .expect("Failed to get the dataset we just inserted");
        crucible
    }

    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    pub fn get_dataset_for_region(
        &self,
        region_id: Uuid,
    ) -> Option<Arc<CrucibleData>> {
        for dataset in self.datasets.values() {
            let DatasetContents::Crucible(dataset) = dataset;
            for region in &dataset.data().list() {
                let id = Uuid::from_str(&region.id.0).unwrap();
                if id == region_id {
                    return Some(dataset.data());
                }
            }
        }

        None
    }

    pub fn get_region_for_port(&self, port: u16) -> Option<Region> {
        let mut regions = vec![];

        for dataset in self.datasets.values() {
            let DatasetContents::Crucible(dataset) = dataset;
            for region in &dataset.data().list() {
                if region.state == State::Destroyed {
                    continue;
                }

                if port == region.port_number {
                    regions.push(region.clone());
                }
            }
        }

        // At most, 1 active region with a port should be returned.
        assert!(regions.len() < 2);

        regions.pop()
    }

    pub fn drop_dataset(&mut self, id: DatasetUuid) {
        let _ = self.datasets.remove(&id).expect("Failed to get the dataset");
    }
}

/// Represents a nested dataset
pub struct NestedDatasetStorage {
    config: NestedDatasetConfig,
    // In-memory flag for whether this dataset pretends to be mounted; defaults
    // to true.
    //
    // Nothing in the simulated storage implementation acts on this value; it is
    // merely a sticky bool that remembers the most recent value passed to
    // `nested_dataset_set_mounted()`.
    mounted: bool,
    // We intentionally store the children before the mountpoint,
    // so they are deleted first.
    children: BTreeMap<String, NestedDatasetStorage>,
    // We store this directory as a temporary directory so it gets
    // removed when this struct is dropped.
    #[allow(dead_code)]
    mountpoint: Utf8TempDir,
}

impl NestedDatasetStorage {
    fn new(
        zpool_root: &Utf8Path,
        dataset_root: DatasetName,
        path: String,
        shared_config: SharedDatasetConfig,
    ) -> Self {
        let name = NestedDatasetLocation { path, root: dataset_root };

        // Create a mountpoint for the nested dataset storage that lasts
        // as long as the nested dataset does.
        let mountpoint = name.mountpoint(zpool_root);
        let parent = mountpoint.as_path().parent().unwrap();
        std::fs::create_dir_all(&parent).unwrap();

        let new_dir_name = mountpoint.as_path().file_name().unwrap();
        let mountpoint = camino_tempfile::Builder::new()
            .rand_bytes(0)
            .prefix(new_dir_name)
            .tempdir_in(parent)
            .unwrap();

        Self {
            config: NestedDatasetConfig { name, inner: shared_config },
            mounted: true,
            children: BTreeMap::new(),
            mountpoint,
        }
    }
}

/// Simulated representation of all storage on a sled.
#[derive(Clone)]
pub(crate) struct Storage {
    inner: Arc<Mutex<StorageInner>>,
}

impl Storage {
    pub fn new(
        sled_id: Uuid,
        sled_index: u16,
        crucible_ip: IpAddr,
        log: Logger,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StorageInner::new(
                sled_id,
                sled_index,
                crucible_ip,
                log,
            ))),
        }
    }

    pub fn lock(&self) -> std::sync::MutexGuard<'_, StorageInner> {
        self.inner.lock().unwrap()
    }

    pub fn as_support_bundle_storage<'a>(
        &'a self,
        log: &'a Logger,
    ) -> SupportBundleManager<'a> {
        SupportBundleManager::new(log, self)
    }
}

/// Simulated representation of all storage on a sled.
///
/// Guarded by a mutex from [Storage].
pub(crate) struct StorageInner {
    log: Logger,
    sled_id: Uuid,
    root: Utf8TempDir,
    config: Option<OmicronPhysicalDisksConfig>,
    dataset_config: Option<DatasetsConfig>,
    nested_datasets: HashMap<DatasetName, NestedDatasetStorage>,
    physical_disks: HashMap<PhysicalDiskUuid, PhysicalDisk>,
    next_disk_slot: i64,
    zpools: HashMap<ZpoolUuid, Zpool>,
    crucible_ip: IpAddr,
    start_crucible_port: u16,
    next_crucible_port: u16,
}

impl StorageInner {
    pub fn new(
        sled_id: Uuid,
        sled_index: u16,
        crucible_ip: IpAddr,
        log: Logger,
    ) -> Self {
        Self {
            sled_id,
            log,
            root: camino_tempfile::tempdir().unwrap(),
            config: None,
            dataset_config: None,
            nested_datasets: HashMap::new(),
            physical_disks: HashMap::new(),
            next_disk_slot: 0,
            zpools: HashMap::new(),
            crucible_ip,
            start_crucible_port: (sled_index + 1) * 1000,
            next_crucible_port: (sled_index + 1) * 1000,
        }
    }

    /// Returns a path to the "zpool root" for storage.
    pub fn root(&self) -> &Utf8Path {
        self.root.path()
    }

    /// Returns an immutable reference to all (currently known) physical disks
    pub fn physical_disks(&self) -> &HashMap<PhysicalDiskUuid, PhysicalDisk> {
        &self.physical_disks
    }

    pub fn datasets_config_list(&self) -> Result<DatasetsConfig, HttpError> {
        let Some(config) = self.dataset_config.as_ref() else {
            return Err(HttpError::for_not_found(
                None,
                "No control plane datasets".into(),
            ));
        };
        Ok(config.clone())
    }

    pub fn dataset_get(
        &self,
        dataset_name: &String,
    ) -> Result<DatasetProperties, HttpError> {
        let Some(config) = self.dataset_config.as_ref() else {
            return Err(HttpError::for_not_found(
                None,
                "No control plane datasets".into(),
            ));
        };

        for (id, dataset) in config.datasets.iter() {
            if dataset.name.full_name().as_str() == dataset_name {
                return Ok(DatasetProperties {
                    id: Some(*id),
                    name: dataset_name.to_string(),
                    // We should have an entry in `self.nested_datasets` for
                    // every entry in `config.datasets` (`datasets_ensure()`
                    // keeps these in sync), but we only keeping track of a
                    // `mounted` property on nested datasets. Look that up here.
                    mounted: self
                        .nested_datasets
                        .get(&dataset.name)
                        .map_or(true, |d| d.mounted),
                    avail: ByteCount::from_kibibytes_u32(1024),
                    used: ByteCount::from_kibibytes_u32(1024),
                    quota: dataset.inner.quota,
                    reservation: dataset.inner.reservation,
                    compression: dataset.inner.compression.to_string(),
                });
            }
        }

        for (nested_dataset_name, nested_dataset_storage) in
            self.nested_datasets.iter()
        {
            if nested_dataset_name.full_name().as_str() == dataset_name {
                let config = &nested_dataset_storage.config.inner;

                return Ok(DatasetProperties {
                    id: None,
                    name: dataset_name.to_string(),
                    mounted: nested_dataset_storage.mounted,
                    avail: ByteCount::from_kibibytes_u32(1024),
                    used: ByteCount::from_kibibytes_u32(1024),
                    quota: config.quota,
                    reservation: config.reservation,
                    compression: config.compression.to_string(),
                });
            }
        }

        return Err(HttpError::for_not_found(None, "Dataset not found".into()));
    }

    pub fn datasets_ensure(
        &mut self,
        config: DatasetsConfig,
    ) -> Result<DatasetsManagementResult, HttpError> {
        if let Some(stored_config) = self.dataset_config.as_ref() {
            if stored_config.generation > config.generation {
                return Err(HttpError::for_client_error(
                    None,
                    dropshot::ClientErrorStatusCode::BAD_REQUEST,
                    "Generation number too old".to_string(),
                ));
            } else if stored_config.generation == config.generation
                && *stored_config != config
            {
                return Err(HttpError::for_client_error(
                    None,
                    dropshot::ClientErrorStatusCode::BAD_REQUEST,
                    "Generation number unchanged but data is different"
                        .to_string(),
                ));
            }
        }
        self.dataset_config.replace(config.clone());

        // Add a "nested dataset" entry for all datasets that should exist,
        // and remove it for all datasets that have been removed.
        let dataset_names: HashSet<_> = config
            .datasets
            .values()
            .map(|config| config.name.clone())
            .collect();
        for dataset in &dataset_names {
            // Datasets delegated to zones manage their own storage.
            if dataset.kind().zoned() {
                continue;
            }

            let root = self.root().to_path_buf();
            self.nested_datasets.entry(dataset.clone()).or_insert_with(|| {
                NestedDatasetStorage::new(
                    &root,
                    dataset.clone(),
                    String::new(),
                    SharedDatasetConfig::default(),
                )
            });
        }
        self.nested_datasets
            .retain(|dataset, _| dataset_names.contains(&dataset));

        Ok(DatasetsManagementResult {
            status: config
                .datasets
                .values()
                .map(|config| DatasetManagementStatus {
                    dataset_name: config.name.clone(),
                    err: None,
                })
                .collect(),
        })
    }

    pub fn nested_dataset_list(
        &self,
        name: NestedDatasetLocation,
        options: NestedDatasetListOptions,
    ) -> Result<Vec<NestedDatasetConfig>, HttpError> {
        let Some(mut nested_dataset) = self.nested_datasets.get(&name.root)
        else {
            return Err(HttpError::for_not_found(
                None,
                "Dataset not found".to_string(),
            ));
        };

        for path_component in name.path.split('/') {
            if path_component.is_empty() {
                continue;
            }
            match nested_dataset.children.get(path_component) {
                Some(dataset) => nested_dataset = dataset,
                None => {
                    return Err(HttpError::for_not_found(
                        None,
                        "Dataset not found".to_string(),
                    ));
                }
            };
        }

        let mut children: Vec<_> = nested_dataset
            .children
            .values()
            .map(|storage| storage.config.clone())
            .collect();

        match options {
            NestedDatasetListOptions::ChildrenOnly => return Ok(children),
            NestedDatasetListOptions::SelfAndChildren => {
                children.insert(0, nested_dataset.config.clone());
                return Ok(children);
            }
        }
    }

    #[cfg(test)]
    pub fn nested_dataset_is_mounted(
        &self,
        dataset: &NestedDatasetLocation,
    ) -> Result<bool, HttpError> {
        let Some(mut nested_dataset) = self.nested_datasets.get(&dataset.root)
        else {
            return Err(HttpError::for_not_found(
                None,
                "Dataset not found".to_string(),
            ));
        };
        for component in dataset.path.split('/') {
            if component.is_empty() {
                continue;
            }
            nested_dataset =
                nested_dataset.children.get(component).ok_or_else(|| {
                    HttpError::for_not_found(
                        None,
                        "Dataset not found".to_string(),
                    )
                })?;
        }
        Ok(nested_dataset.mounted)
    }

    pub fn nested_dataset_set_mounted(
        &mut self,
        dataset: &NestedDatasetLocation,
        mounted: bool,
    ) -> Result<(), HttpError> {
        let Some(mut nested_dataset) =
            self.nested_datasets.get_mut(&dataset.root)
        else {
            return Err(HttpError::for_not_found(
                None,
                "Dataset not found".to_string(),
            ));
        };
        for component in dataset.path.split('/') {
            if component.is_empty() {
                continue;
            }
            nested_dataset = nested_dataset
                .children
                .get_mut(component)
                .ok_or_else(|| {
                    HttpError::for_not_found(
                        None,
                        "Dataset not found".to_string(),
                    )
                })?;
        }
        nested_dataset.mounted = mounted;
        Ok(())
    }

    pub fn nested_dataset_ensure(
        &mut self,
        config: NestedDatasetConfig,
    ) -> Result<(), HttpError> {
        let name = &config.name;
        let nested_path = name.path.to_string();
        let zpool_root = self.root().to_path_buf();
        let Some(mut nested_dataset) = self.nested_datasets.get_mut(&name.root)
        else {
            return Err(HttpError::for_not_found(
                None,
                "Dataset not found".to_string(),
            ));
        };

        let mut path_components = nested_path.split('/').peekable();
        while let Some(path_component) = path_components.next() {
            if path_component.is_empty() {
                continue;
            }

            // Final component of path -- insert it here if it doesn't exist
            // already.
            if path_components.peek().is_none() {
                let entry =
                    nested_dataset.children.entry(path_component.to_string());
                entry
                    .and_modify(|storage| {
                        storage.config = config.clone();
                    })
                    .or_insert_with(|| {
                        NestedDatasetStorage::new(
                            &zpool_root,
                            config.name.root,
                            nested_path,
                            config.inner,
                        )
                    });
                return Ok(());
            }

            match nested_dataset.children.get_mut(path_component) {
                Some(dataset) => nested_dataset = dataset,
                None => {
                    return Err(HttpError::for_not_found(
                        None,
                        "Dataset not found".to_string(),
                    ));
                }
            };
        }
        return Err(HttpError::for_not_found(
            None,
            "Nested Dataset not found".to_string(),
        ));
    }

    pub fn nested_dataset_destroy(
        &mut self,
        name: NestedDatasetLocation,
    ) -> Result<(), HttpError> {
        let Some(mut nested_dataset) = self.nested_datasets.get_mut(&name.root)
        else {
            return Err(HttpError::for_not_found(
                None,
                "Dataset not found".to_string(),
            ));
        };

        let mut path_components = name.path.split('/').peekable();
        while let Some(path_component) = path_components.next() {
            if path_component.is_empty() {
                continue;
            }

            // Final component of path -- remove it if it exists.
            if path_components.peek().is_none() {
                if nested_dataset.children.remove(path_component).is_none() {
                    return Err(HttpError::for_not_found(
                        None,
                        "Nested Dataset not found".to_string(),
                    ));
                };
                return Ok(());
            }
            match nested_dataset.children.get_mut(path_component) {
                Some(dataset) => nested_dataset = dataset,
                None => {
                    return Err(HttpError::for_not_found(
                        None,
                        "Dataset not found".to_string(),
                    ));
                }
            };
        }
        return Err(HttpError::for_not_found(
            None,
            "Nested Dataset not found".to_string(),
        ));
    }

    pub fn omicron_physical_disks_list(
        &self,
    ) -> Result<OmicronPhysicalDisksConfig, HttpError> {
        let Some(config) = self.config.as_ref() else {
            return Err(HttpError::for_not_found(
                None,
                "No control plane disks".into(),
            ));
        };
        Ok(config.clone())
    }

    pub fn omicron_physical_disks_ensure(
        &mut self,
        config: OmicronPhysicalDisksConfig,
    ) -> Result<DisksManagementResult, HttpError> {
        if let Some(stored_config) = self.config.as_ref() {
            if stored_config.generation > config.generation {
                return Err(HttpError::for_client_error(
                    None,
                    dropshot::ClientErrorStatusCode::BAD_REQUEST,
                    "Generation number too old".to_string(),
                ));
            } else if stored_config.generation == config.generation
                && *stored_config != config
            {
                return Err(HttpError::for_client_error(
                    None,
                    dropshot::ClientErrorStatusCode::BAD_REQUEST,
                    "Generation number unchanged but data is different"
                        .to_string(),
                ));
            }
        }
        self.config.replace(config.clone());

        Ok(DisksManagementResult {
            status: config
                .disks
                .into_iter()
                .map(|config| DiskManagementStatus {
                    identity: config.identity,
                    err: None,
                })
                .collect(),
        })
    }

    pub fn insert_physical_disk(
        &mut self,
        id: PhysicalDiskUuid,
        identity: DiskIdentity,
        variant: DiskVariant,
    ) {
        let slot = self.next_disk_slot;
        self.next_disk_slot += 1;
        self.physical_disks
            .insert(id, PhysicalDisk { identity, variant, slot });
    }

    /// Adds a Zpool to the sled's simulated storage.
    pub fn insert_zpool(
        &mut self,
        zpool_id: ZpoolUuid,
        disk_id: PhysicalDiskUuid,
        size: u64,
    ) {
        // Update our local data
        self.zpools.insert(zpool_id, Zpool::new(zpool_id, disk_id, size));
    }

    /// Returns an immutable reference to all zpools
    pub fn zpools(&self) -> &HashMap<ZpoolUuid, Zpool> {
        &self.zpools
    }

    /// Adds a Crucible dataset to the sled's simulated storage.
    pub fn insert_crucible_dataset(
        &mut self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
    ) -> SocketAddr {
        // There's a limit to the number of simulated Crucible agents per
        // dataset:
        //
        // - [`StorageInner::new`] allocates 1000 ports per sled for all
        //   simulated Crucible agents
        //
        // - this function allocates 50 ports per simulated Crucible agent,
        //   meaning a maximum of 20 agents can be created in the tests.
        //
        // Assert here if that limit is reached.
        assert!((self.next_crucible_port - self.start_crucible_port) < 1000);

        // Update our local data
        let dataset = self
            .zpools
            .get_mut(&zpool_id)
            .expect("Zpool does not exist")
            .insert_crucible_dataset(
                &self.log,
                dataset_id,
                self.crucible_ip,
                self.next_crucible_port,
                self.next_crucible_port + 50,
            );

        self.next_crucible_port += 50;

        dataset.address()
    }

    pub fn get_all_physical_disks(
        &self,
    ) -> Vec<nexus_lockstep_client::types::PhysicalDiskPutRequest> {
        self.physical_disks
            .iter()
            .map(|(id, disk)| {
                let variant = match disk.variant {
                    DiskVariant::U2 => {
                        nexus_lockstep_client::types::PhysicalDiskKind::U2
                    }
                    DiskVariant::M2 => {
                        nexus_lockstep_client::types::PhysicalDiskKind::M2
                    }
                };

                nexus_lockstep_client::types::PhysicalDiskPutRequest {
                    id: *id,
                    vendor: disk.identity.vendor.clone(),
                    serial: disk.identity.serial.clone(),
                    model: disk.identity.model.clone(),
                    variant,
                    sled_id: self.sled_id,
                }
            })
            .collect()
    }

    pub fn get_all_zpools(
        &self,
    ) -> Vec<nexus_lockstep_client::types::ZpoolPutRequest> {
        self.zpools
            .values()
            .map(|pool| nexus_lockstep_client::types::ZpoolPutRequest {
                id: pool.id.into_untyped_uuid(),
                sled_id: self.sled_id,
                physical_disk_id: pool.physical_disk_id,
            })
            .collect()
    }

    pub fn get_all_crucible_datasets(
        &self,
        zpool_id: ZpoolUuid,
    ) -> Vec<(DatasetUuid, SocketAddr)> {
        let zpool = self.zpools.get(&zpool_id).expect("Zpool does not exist");

        zpool
            .datasets
            .iter()
            .map(|(id, dataset)| match dataset {
                DatasetContents::Crucible(server) => (*id, server.address()),
            })
            .collect()
    }

    pub fn has_zpool(&self, zpool_id: ZpoolUuid) -> bool {
        self.zpools.contains_key(&zpool_id)
    }

    pub fn get_dataset(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
    ) -> &DatasetContents {
        self.zpools
            .get(&zpool_id)
            .expect("Zpool does not exist")
            .datasets
            .get(&dataset_id)
            .expect("Dataset does not exist")
    }

    pub fn get_crucible_dataset(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
    ) -> Arc<CrucibleData> {
        match self.get_dataset(zpool_id, dataset_id) {
            DatasetContents::Crucible(crucible) => crucible.data.clone(),
        }
    }

    pub fn get_dataset_for_region(
        &self,
        region_id: Uuid,
    ) -> Option<Arc<CrucibleData>> {
        for zpool in self.zpools.values() {
            if let Some(dataset) = zpool.get_dataset_for_region(region_id) {
                return Some(dataset);
            }
        }

        None
    }

    pub fn get_region_for_port(&self, port: u16) -> Option<Region> {
        let mut regions = vec![];
        for zpool in self.zpools.values() {
            if let Some(region) = zpool.get_region_for_port(port) {
                regions.push(region);
            }
        }

        // At most, 1 active region with a port should be returned.
        assert!(regions.len() < 2);

        regions.pop()
    }

    pub fn drop_dataset(
        &mut self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
    ) {
        self.zpools
            .get_mut(&zpool_id)
            .expect("Zpool does not exist")
            .drop_dataset(dataset_id)
    }
}

pub struct PantryVolume {
    vcr: VolumeConstructionRequest, // Please rewind!
    status: VolumeStatus,
    activate_job: Option<String>,
}

pub struct PantryInner {
    /// Map Volume UUID to PantryVolume struct
    volumes: HashMap<String, PantryVolume>,

    jobs: HashSet<String>,

    /// Auto activate volumes attached in the background
    auto_activate_volumes: bool,
}

/// Simulated crucible pantry
pub struct Pantry {
    pub id: OmicronZoneUuid,
    simulated_upstairs: Arc<SimulatedUpstairs>,
    inner: Mutex<PantryInner>,
}

impl Pantry {
    pub fn new(simulated_upstairs: Arc<SimulatedUpstairs>) -> Self {
        Self {
            id: OmicronZoneUuid::new_v4(),
            simulated_upstairs,
            inner: Mutex::new(PantryInner {
                volumes: HashMap::default(),
                jobs: HashSet::default(),
                auto_activate_volumes: false,
            }),
        }
    }

    pub fn status(&self) -> Result<PantryStatus, HttpError> {
        let inner = self.inner.lock().unwrap();
        Ok(PantryStatus {
            volumes: inner.volumes.keys().cloned().collect(),
            num_job_handles: inner.jobs.len(),
        })
    }

    pub fn entry(
        &self,
        volume_id: String,
    ) -> Result<VolumeConstructionRequest, HttpError> {
        let inner = self.inner.lock().unwrap();

        match inner.volumes.get(&volume_id) {
            Some(entry) => Ok(entry.vcr.clone()),

            None => Err(HttpError::for_not_found(None, volume_id)),
        }
    }

    pub fn attach(
        &self,
        volume_id: String,
        volume_construction_request: VolumeConstructionRequest,
    ) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

        inner.volumes.insert(
            volume_id,
            PantryVolume {
                vcr: volume_construction_request,
                status: VolumeStatus {
                    active: true,
                    seen_active: true,
                    num_job_handles: 0,
                },
                activate_job: None,
            },
        );

        Ok(())
    }

    pub fn set_auto_activate_volumes(&self) {
        self.inner.lock().unwrap().auto_activate_volumes = true;
    }

    pub fn attach_activate_background(
        &self,
        volume_id: String,
        activate_job_id: String,
        volume_construction_request: VolumeConstructionRequest,
    ) -> Result<(), HttpError> {
        let mut inner = self.inner.lock().unwrap();

        let auto_activate_volumes = inner.auto_activate_volumes;

        inner.volumes.insert(
            volume_id,
            PantryVolume {
                vcr: volume_construction_request,
                status: VolumeStatus {
                    active: auto_activate_volumes,
                    seen_active: auto_activate_volumes,
                    num_job_handles: 1,
                },
                activate_job: Some(activate_job_id.clone()),
            },
        );

        inner.jobs.insert(activate_job_id);

        Ok(())
    }

    pub fn activate_background_attachment(
        &self,
        volume_id: String,
    ) -> Result<String, HttpError> {
        let activate_job = {
            let inner = self.inner.lock().unwrap();
            inner.volumes.get(&volume_id).unwrap().activate_job.clone().unwrap()
        };

        let mut status = self.volume_status(volume_id.clone())?;

        status.active = true;
        status.seen_active = true;

        self.update_volume_status(volume_id, status)?;

        Ok(activate_job)
    }

    pub fn volume_status(
        &self,
        volume_id: String,
    ) -> Result<VolumeStatus, HttpError> {
        let inner = self.inner.lock().unwrap();

        match inner.volumes.get(&volume_id) {
            Some(pantry_volume) => Ok(pantry_volume.status.clone()),

            None => Err(HttpError::for_not_found(None, volume_id)),
        }
    }

    pub fn update_volume_status(
        &self,
        volume_id: String,
        status: VolumeStatus,
    ) -> Result<(), HttpError> {
        let mut inner = self.inner.lock().unwrap();

        match inner.volumes.get_mut(&volume_id) {
            Some(pantry_volume) => {
                pantry_volume.status = status;
                Ok(())
            }

            None => Err(HttpError::for_not_found(None, volume_id)),
        }
    }

    pub fn is_job_finished(&self, job_id: String) -> Result<bool, HttpError> {
        let inner = self.inner.lock().unwrap();
        if !inner.jobs.contains(&job_id) {
            return Err(HttpError::for_not_found(None, job_id));
        }
        Ok(true)
    }

    pub fn get_job_result(
        &self,
        job_id: String,
    ) -> Result<Result<bool>, HttpError> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.jobs.contains(&job_id) {
            return Err(HttpError::for_not_found(None, job_id));
        }
        inner.jobs.remove(&job_id);
        Ok(Ok(true))
    }

    pub fn import_from_url(
        &self,
        volume_id: String,
        _url: String,
        _expected_digest: Option<ExpectedDigest>,
    ) -> Result<String, HttpError> {
        self.entry(volume_id)?;

        // Make up job
        let mut inner = self.inner.lock().unwrap();
        let job_id = Uuid::new_v4().to_string();
        inner.jobs.insert(job_id.clone());

        Ok(job_id)
    }

    pub fn snapshot(
        &self,
        volume_id: String,
        snapshot_id: String,
    ) -> Result<(), HttpError> {
        // Perform the disk id -> region id mapping just as is done by during
        // the simulated instance ensure.
        let inner = self.inner.lock().unwrap();

        let volume_construction_request =
            &inner.volumes.get(&volume_id).unwrap().vcr;

        self.simulated_upstairs.map_id_to_vcr(
            volume_id.parse().unwrap(),
            volume_construction_request,
        );

        self.simulated_upstairs
            .snapshot(volume_id.parse().unwrap(), snapshot_id.parse().unwrap())
            .map_err(|e| HttpError::for_internal_error(e.to_string()))
    }

    pub fn bulk_write(
        &self,
        volume_id: String,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<(), HttpError> {
        let vcr = self.entry(volume_id)?;

        // Currently, Nexus will only make volumes where the first subvolume is
        // a Region. This will change in the future!
        let (region_block_size, region_size) = match vcr {
            VolumeConstructionRequest::Volume { sub_volumes, .. } => {
                match sub_volumes[0] {
                    VolumeConstructionRequest::Region {
                        block_size,
                        blocks_per_extent,
                        extent_count,
                        ..
                    } => (
                        block_size,
                        block_size
                            * blocks_per_extent
                            * u64::from(extent_count),
                    ),

                    _ => {
                        panic!("unexpected Volume layout");
                    }
                }
            }

            _ => {
                panic!("unexpected Volume layout");
            }
        };

        if (offset % region_block_size) != 0 {
            return Err(HttpError::for_bad_request(
                None,
                "offset not multiple of block size!".to_string(),
            ));
        }

        if (data.len() as u64 % region_block_size) != 0 {
            return Err(HttpError::for_bad_request(
                None,
                "data length not multiple of block size!".to_string(),
            ));
        }

        if (offset + data.len() as u64) > region_size {
            return Err(HttpError::for_bad_request(
                None,
                "offset + data length off end of region!".to_string(),
            ));
        }

        Ok(())
    }

    pub fn scrub(&self, volume_id: String) -> Result<String, HttpError> {
        self.entry(volume_id)?;

        // Make up job
        let mut inner = self.inner.lock().unwrap();
        let job_id = Uuid::new_v4().to_string();
        inner.jobs.insert(job_id.clone());

        Ok(job_id)
    }

    pub fn detach(&self, volume_id: String) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.volumes.remove(&volume_id);
        Ok(())
    }
}

pub struct PantryServer {
    pub server: dropshot::HttpServer<Arc<Pantry>>,
    pub pantry: Arc<Pantry>,
}

impl PantryServer {
    pub fn new(
        log: Logger,
        ip: IpAddr,
        simulated_upstairs: Arc<SimulatedUpstairs>,
    ) -> Self {
        let pantry = Arc::new(Pantry::new(simulated_upstairs));

        let server = dropshot::ServerBuilder::new(
            super::http_entrypoints_pantry::api(),
            pantry.clone(),
            log.new(o!("component" => "dropshot")),
        )
        .config(dropshot::ConfigDropshot {
            bind_address: SocketAddr::new(ip, 0),
            // This has to be large enough to support:
            // - bulk writes into disks
            default_request_body_max_bytes: 8192 * 1024,
            default_handler_task_mode: HandlerTaskMode::Detached,
            log_headers: vec![],
        })
        .start()
        .expect("Could not initialize pantry server");

        info!(&log, "Started Simulated Crucible Pantry"; "address" => server.local_addr());

        PantryServer { server, pantry }
    }

    pub fn addr(&self) -> SocketAddr {
        self.server.local_addr()
    }
}
