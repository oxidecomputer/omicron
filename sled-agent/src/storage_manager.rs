// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of sled-local storage.

use crate::illumos::running_zone::{
    Error as RunningZoneError, InstalledZone, RunningZone,
};
use crate::illumos::vnic::VnicAllocator;
use crate::illumos::zone::AddressRequest;
use crate::illumos::zpool::ZpoolName;
use crate::illumos::{zfs::Mountpoint, zone::ZONE_PREFIX, zpool::ZpoolInfo};
use crate::nexus::NexusClient;
use futures::stream::FuturesOrdered;
use futures::FutureExt;
use futures::StreamExt;
use nexus_client::types::{DatasetPutRequest, ZpoolPutRequest};
use omicron_common::api::external::{ByteCount, ByteCountRangeError};
use omicron_common::api::internal::nexus::DatasetKind as NexusDatasetKind;
use omicron_common::api::internal::sled_agent::DatasetKind;
use omicron_common::backoff;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::fs::{create_dir_all, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[cfg(test)]
use crate::illumos::{zfs::MockZfs as Zfs, zpool::MockZpool as Zpool};
#[cfg(not(test))]
use crate::illumos::{zfs::Zfs, zpool::Zpool};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Datalink(#[from] crate::illumos::dladm::Error),

    #[error(transparent)]
    Zfs(#[from] crate::illumos::zfs::Error),

    #[error(transparent)]
    Zpool(#[from] crate::illumos::zpool::Error),

    #[error("Failed to configure a zone: {0}")]
    ZoneConfiguration(crate::illumos::zone::Error),

    #[error("Failed to manage a running zone: {0}")]
    ZoneManagement(#[from] crate::illumos::running_zone::Error),

    #[error("Error parsing pool size: {0}")]
    BadPoolSize(#[from] ByteCountRangeError),

    #[error("Failed to parse as UUID: {0}")]
    Parse(#[from] uuid::Error),

    #[error("Timed out waiting for service: {0}")]
    Timeout(String),

    #[error("Object Not Found: {0}")]
    NotFound(String),

    #[error("Failed to serialize toml: {0}")]
    Serialize(#[from] toml::ser::Error),

    #[error("Failed to deserialize toml: {0}")]
    Deserialize(#[from] toml::de::Error),

    #[error("Failed to perform I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// A ZFS storage pool.
struct Pool {
    id: Uuid,
    info: ZpoolInfo,
    // ZFS filesytem UUID -> Zone.
    zones: HashMap<Uuid, RunningZone>,
}

impl Pool {
    /// Queries for an existing Zpool by name.
    ///
    /// Returns Ok if the pool exists.
    fn new(name: &ZpoolName) -> Result<Pool, Error> {
        let info = Zpool::get_info(&name.to_string())?;

        // NOTE: This relies on the name being a UUID exactly.
        // We could be more flexible...
        Ok(Pool { id: name.id(), info, zones: HashMap::new() })
    }

    /// Associate an already running zone with this pool object.
    ///
    /// Typically this is used when a dataset within the zone (identified
    /// by ID) has a running zone (e.g. Crucible, Cockroach) operating on
    /// behalf of that data.
    fn add_zone(&mut self, id: Uuid, zone: RunningZone) {
        self.zones.insert(id, zone);
    }

    /// Access a zone managing data within this pool.
    fn get_zone(&self, id: Uuid) -> Option<&RunningZone> {
        self.zones.get(&id)
    }

    /// Returns the ID of the pool itself.
    fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the path for the configuration of a particular
    /// dataset within the pool. This configuration file provides
    /// the necessary information for zones to "launch themselves"
    /// after a reboot.
    async fn dataset_config_path(
        &self,
        dataset_id: Uuid,
    ) -> Result<PathBuf, Error> {
        let path = std::path::Path::new(crate::OMICRON_CONFIG_PATH)
            .join(self.id.to_string());
        create_dir_all(&path).await?;
        let mut path = path.join(dataset_id.to_string());
        path.set_extension("toml");
        Ok(path)
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
struct DatasetName {
    // A unique identifier for the Zpool on which the dataset is stored.
    pool_name: String,
    // A name for the dataset within the Zpool.
    dataset_name: String,
}

impl DatasetName {
    fn new(pool_name: &str, dataset_name: &str) -> Self {
        Self {
            pool_name: pool_name.to_string(),
            dataset_name: dataset_name.to_string(),
        }
    }

    fn full(&self) -> String {
        format!("{}/{}", self.pool_name, self.dataset_name)
    }
}

// Description of a dataset within a ZFS pool, which should be created
// by the Sled Agent.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct DatasetInfo {
    // TODO: Is this always "/data"?
    data_directory: String,
    address: SocketAddr,
    kind: DatasetKind,
    name: DatasetName,
}

impl DatasetInfo {
    fn new(pool: &str, kind: DatasetKind, address: SocketAddr) -> DatasetInfo {
        match kind {
            DatasetKind::CockroachDb { .. } => DatasetInfo {
                name: DatasetName::new(pool, "cockroachdb"),
                data_directory: "/data".to_string(),
                address,
                kind,
            },
            DatasetKind::Clickhouse { .. } => DatasetInfo {
                name: DatasetName::new(pool, "clickhouse"),
                data_directory: "/data".to_string(),
                address,
                kind,
            },
            DatasetKind::Crucible { .. } => DatasetInfo {
                name: DatasetName::new(pool, "crucible"),
                data_directory: "/data".to_string(),
                address,
                kind,
            },
        }
    }

    fn zone_prefix(&self) -> String {
        format!("{}{}_", ZONE_PREFIX, self.name.full())
    }

    async fn start_zone(
        &self,
        log: &Logger,
        zone: &RunningZone,
        address: SocketAddr,
        do_format: bool,
    ) -> Result<(), Error> {
        match self.kind {
            DatasetKind::CockroachDb { .. } => {
                // Load the CRDB manifest.
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "import",
                    "/var/svc/manifest/site/cockroachdb/manifest.xml",
                ])?;

                // Set parameters which are passed to the CRDB binary.
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    "svc:system/illumos/cockroachdb",
                    "setprop",
                    &format!("config/listen_addr={}", address),
                ])?;
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    "svc:system/illumos/cockroachdb",
                    "setprop",
                    &format!("config/store={}", self.data_directory),
                ])?;
                // TODO: Set these addresses, use "start" instead of
                // "start-single-node".
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    "svc:system/illumos/cockroachdb",
                    "setprop",
                    &format!("config/join_addrs={}", "unknown"),
                ])?;

                // Refresh the manifest with the new properties we set,
                // so they become "effective" properties when the service is enabled.
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    "svc:system/illumos/cockroachdb:default",
                    "refresh",
                ])?;

                zone.run_cmd(&[
                    crate::illumos::zone::SVCADM,
                    "enable",
                    "-t",
                    "svc:/system/illumos/cockroachdb:default",
                ])?;

                // Await liveness of the cluster.
                let check_health = || async {
                    let http_addr = SocketAddr::new(address.ip(), 8080);
                    reqwest::get(format!("http://{}/health?ready=1", http_addr))
                        .await
                        .map_err(backoff::BackoffError::Transient)
                };
                let log_failure = |_, _| {
                    warn!(log, "cockroachdb not yet alive");
                };
                backoff::retry_notify(
                    backoff::internal_service_policy(),
                    check_health,
                    log_failure,
                )
                .await
                .expect("expected an infinite retry loop waiting for crdb");

                info!(log, "CRDB is online");
                // If requested, format the cluster with the initial tables.
                if do_format {
                    info!(log, "Formatting CRDB");
                    zone.run_cmd(&[
                        "/opt/oxide/cockroachdb/bin/cockroach",
                        "sql",
                        "--insecure",
                        "--host",
                        &address.to_string(),
                        "--file",
                        "/opt/oxide/cockroachdb/sql/dbwipe.sql",
                    ])?;
                    zone.run_cmd(&[
                        "/opt/oxide/cockroachdb/bin/cockroach",
                        "sql",
                        "--insecure",
                        "--host",
                        &address.to_string(),
                        "--file",
                        "/opt/oxide/cockroachdb/sql/dbinit.sql",
                    ])?;
                    info!(log, "Formatting CRDB - Completed");
                }

                Ok(())
            }
            DatasetKind::Clickhouse { .. } => {
                info!(log, "Initialiting Clickhouse");
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "import",
                    "/var/svc/manifest/site/clickhouse/manifest.xml",
                ])?;

                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    "svc:system/illumos/clickhouse",
                    "setprop",
                    &format!("config/listen_host={}", address.ip()),
                ])?;
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    "svc:system/illumos/clickhouse",
                    "setprop",
                    &format!("config/store={}", self.data_directory),
                ])?;

                // Refresh the manifest with the new properties we set,
                // so they become "effective" properties when the service is enabled.
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    "svc:system/illumos/clickhouse:default",
                    "refresh",
                ])?;

                zone.run_cmd(&[
                    crate::illumos::zone::SVCADM,
                    "enable",
                    "-t",
                    "svc:/system/illumos/clickhouse:default",
                ])?;

                Ok(())
            }
            DatasetKind::Crucible { .. } => {
                info!(log, "Initializing Crucible");

                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "import",
                    "/var/svc/manifest/site/crucible/agent.xml",
                ])?;

                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "import",
                    "/var/svc/manifest/site/crucible/downstairs.xml",
                ])?;

                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    "svc:oxide/crucible/agent",
                    "setprop",
                    &format!("config/listen={}", address),
                ])?;

                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    "svc:oxide/crucible/agent",
                    "setprop",
                    &format!("config/dataset={}", self.name.full()),
                ])?;

                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    "svc:oxide/crucible/agent",
                    "setprop",
                    &format!("config/uuid={}", Uuid::new_v4()),
                ])?;

                // Refresh the manifest with the new properties we set,
                // so they become "effective" properties when the service is enabled.
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    "svc:oxide/crucible/agent:default",
                    "refresh",
                ])?;

                zone.run_cmd(&[
                    crate::illumos::zone::SVCADM,
                    "enable",
                    "-t",
                    "svc:/oxide/crucible/agent:default",
                ])?;

                Ok(())
            }
        }
    }
}

// Ensures that a zone backing a particular dataset is running.
async fn ensure_running_zone(
    log: &Logger,
    vnic_allocator: &VnicAllocator,
    dataset_info: &DatasetInfo,
    dataset_name: &DatasetName,
    do_format: bool,
) -> Result<RunningZone, Error> {
    let address_request =
        AddressRequest::new_static(dataset_info.address.ip(), None);

    match RunningZone::get(log, &dataset_info.zone_prefix(), address_request)
        .await
    {
        Ok(zone) => {
            info!(log, "Zone for {} is already running", dataset_name.full());
            return Ok(zone);
        }
        Err(RunningZoneError::NotFound) => {
            info!(log, "Zone for {} was not found", dataset_name.full());

            let installed_zone = InstalledZone::install(
                log,
                vnic_allocator,
                &dataset_info.name.dataset_name,
                Some(&dataset_name.pool_name),
                &[zone::Dataset { name: dataset_name.full() }],
                &[],
                vec![],
            )
            .await?;

            let zone = RunningZone::boot(installed_zone).await?;

            zone.ensure_address(address_request).await?;
            dataset_info
                .start_zone(log, &zone, dataset_info.address, do_format)
                .await?;

            Ok(zone)
        }
        Err(RunningZoneError::NotRunning(_state)) => {
            // TODO:
            unimplemented!("Handle a zone which exists, but is not running");
        }
        Err(_) => {
            // TODO:
            unimplemented!(
                "Handle a zone which exists, has some other problem"
            );
        }
    }
}

type NotifyFut =
    dyn futures::Future<Output = Result<(), nexus_client::Error<()>>> + Send;

#[derive(Debug)]
struct NewFilesystemRequest {
    zpool_id: Uuid,
    dataset_kind: DatasetKind,
    address: SocketAddr,
    responder: oneshot::Sender<Result<(), Error>>,
}

// A worker that starts zones for pools as they are received.
struct StorageWorker {
    log: Logger,
    sled_id: Uuid,
    nexus_client: Arc<NexusClient>,
    pools: Arc<Mutex<HashMap<ZpoolName, Pool>>>,
    new_pools_rx: mpsc::Receiver<ZpoolName>,
    new_filesystems_rx: mpsc::Receiver<NewFilesystemRequest>,
    vnic_allocator: VnicAllocator,
}

impl StorageWorker {
    // Ensures the named dataset exists as a filesystem with a UUID, optionally
    // creating it if `do_format` is true.
    //
    // Returns the UUID attached to the ZFS filesystem.
    fn ensure_dataset_with_id(
        dataset_name: &DatasetName,
        do_format: bool,
    ) -> Result<Uuid, Error> {
        let fs_name = &dataset_name.full();
        Zfs::ensure_filesystem(
            &fs_name,
            Mountpoint::Path(PathBuf::from("/data")),
            do_format,
        )?;
        // Ensure the dataset has a usable UUID.
        if let Ok(id_str) = Zfs::get_oxide_value(&fs_name, "uuid") {
            if let Ok(id) = id_str.parse::<Uuid>() {
                return Ok(id);
            }
        }
        let id = Uuid::new_v4();
        Zfs::set_oxide_value(&fs_name, "uuid", &id.to_string())?;
        Ok(id)
    }

    // Starts the zone for a dataset within a particular zpool.
    //
    // If requested via the `do_format` parameter, may also initialize
    // these resources.
    //
    // Returns the UUID attached to the underlying ZFS partition.
    // Returns (was_inserted, Uuid).
    async fn initialize_dataset_and_zone(
        &self,
        pool: &mut Pool,
        dataset_info: &DatasetInfo,
        do_format: bool,
    ) -> Result<(bool, Uuid), Error> {
        // Ensure the underlying dataset exists before trying to poke at zones.
        let dataset_name = &dataset_info.name;
        info!(&self.log, "Ensuring dataset {} exists", dataset_name.full());
        let id =
            StorageWorker::ensure_dataset_with_id(&dataset_name, do_format)?;

        // If this zone has already been processed by us, return immediately.
        if let Some(_) = pool.get_zone(id) {
            return Ok((false, id));
        }
        // Otherwise, the zone may or may not exit.
        // We need to either look up or create the zone.
        info!(
            &self.log,
            "Ensuring zone for {} is running",
            dataset_name.full()
        );
        let zone = ensure_running_zone(
            &self.log,
            &self.vnic_allocator,
            dataset_info,
            &dataset_name,
            do_format,
        )
        .await?;

        info!(
            &self.log,
            "Zone {} with address {} is running",
            zone.name(),
            dataset_info.address,
        );
        pool.add_zone(id, zone);
        Ok((true, id))
    }

    // Adds a "notification to nexus" to `nexus_notifications`,
    // informing it about the addition of `pool_id` to this sled.
    fn add_zpool_notify(
        &self,
        nexus_notifications: &mut FuturesOrdered<Pin<Box<NotifyFut>>>,
        pool_id: Uuid,
        size: ByteCount,
    ) {
        let sled_id = self.sled_id;
        let nexus = self.nexus_client.clone();
        let notify_nexus = move || {
            let zpool_request = ZpoolPutRequest { size: size.into() };
            let nexus = nexus.clone();
            async move {
                nexus
                    .zpool_put(&sled_id, &pool_id, &zpool_request)
                    .await
                    .map_err(backoff::BackoffError::Transient)?;
                Ok::<(), backoff::BackoffError<nexus_client::Error<()>>>(())
            }
        };
        let log = self.log.clone();
        let log_post_failure = move |_, delay| {
            warn!(
                log,
                "failed to notify nexus, will retry in {:?}", delay;
            );
        };
        nexus_notifications.push(
            backoff::retry_notify(
                backoff::internal_service_policy(),
                notify_nexus,
                log_post_failure,
            )
            .boxed(),
        );
    }

    // Adds a "notification to nexus" to `nexus_notifications`,
    // informing it about the addition of `datasets` to `pool_id`.
    fn add_datasets_notify(
        &self,
        nexus_notifications: &mut FuturesOrdered<Pin<Box<NotifyFut>>>,
        datasets: Vec<(Uuid, SocketAddr, NexusDatasetKind)>,
        pool_id: Uuid,
    ) {
        let nexus = self.nexus_client.clone();
        let notify_nexus = move || {
            let nexus = nexus.clone();
            let datasets = datasets.clone();
            async move {
                for (id, address, kind) in datasets {
                    let request = DatasetPutRequest {
                        address: address.to_string(),
                        kind: kind.into(),
                    };
                    nexus
                        .dataset_put(&pool_id, &id, &request)
                        .await
                        .map_err(backoff::BackoffError::Transient)?;
                }

                Ok::<(), backoff::BackoffError<nexus_client::Error<()>>>(())
            }
        };
        let log = self.log.clone();
        let log_post_failure = move |_, delay| {
            warn!(
                log,
                "failed to notify nexus about datasets, will retry in {:?}", delay;
            );
        };
        nexus_notifications.push(
            backoff::retry_notify(
                backoff::internal_service_policy(),
                notify_nexus,
                log_post_failure,
            )
            .boxed(),
        );
    }

    // TODO: a lot of these functions act on the `FuturesOrdered` - should
    // that just be a part of the "worker" struct?

    // Attempts to add a dataset within a zpool, according to `request`.
    async fn add_dataset(
        &self,
        nexus_notifications: &mut FuturesOrdered<Pin<Box<NotifyFut>>>,
        request: &NewFilesystemRequest,
    ) -> Result<(), Error> {
        let mut pools = self.pools.lock().await;
        let name = ZpoolName::new(request.zpool_id);
        let pool = pools.get_mut(&name).ok_or_else(|| {
            Error::NotFound(format!("zpool: {}", request.zpool_id))
        })?;

        let dataset_info = DatasetInfo::new(
            pool.info.name(),
            request.dataset_kind.clone(),
            request.address,
        );
        let (is_new_dataset, id) = self
            .initialize_dataset_and_zone(
                pool,
                &dataset_info,
                /* do_format= */ true,
            )
            .await?;

        if !is_new_dataset {
            return Ok(());
        }

        // Now that the dataset has been initialized, record the configuration
        // so it can re-initialize itself after a reboot.
        let info_str = toml::to_string(&dataset_info)?;
        let path = pool.dataset_config_path(id).await?;
        let mut file = File::create(path).await?;
        file.write_all(info_str.as_bytes()).await?;

        self.add_datasets_notify(
            nexus_notifications,
            vec![(id, dataset_info.address, dataset_info.kind.into())],
            pool.id(),
        );

        Ok(())
    }

    async fn load_dataset(
        &self,
        pool: &mut Pool,
        dataset_name: &DatasetName,
    ) -> Result<(Uuid, SocketAddr, NexusDatasetKind), Error> {
        let id = Zfs::get_oxide_value(&dataset_name.full(), "uuid")?
            .parse::<Uuid>()?;
        let config_path = pool.dataset_config_path(id).await?;
        info!(
            self.log,
            "Loading Dataset from {}",
            config_path.to_string_lossy()
        );
        let dataset_info: DatasetInfo =
            toml::from_slice(&tokio::fs::read(config_path).await?)?;
        self.initialize_dataset_and_zone(
            pool,
            &dataset_info,
            /* do_format= */ false,
        )
        .await?;

        Ok((id, dataset_info.address, dataset_info.kind.into()))
    }

    // Small wrapper around `Self::do_work_internal` that ensures we always
    // emit info to the log when we exit.
    async fn do_work(&mut self) -> Result<(), Error> {
        self.do_work_internal()
            .await
            .map(|()| {
                info!(self.log, "StorageWorker exited successfully");
            })
            .map_err(|e| {
                warn!(self.log, "StorageWorker exited unexpectedly: {}", e);
                e
            })
    }

    async fn do_work_internal(&mut self) -> Result<(), Error> {
        let mut nexus_notifications = FuturesOrdered::new();

        loop {
            tokio::select! {
                _ = nexus_notifications.next(), if !nexus_notifications.is_empty() => {},
                Some(pool_name) = self.new_pools_rx.recv() => {
                    let mut pools = self.pools.lock().await;
                    let pool = pools.get_mut(&pool_name).unwrap();

                    info!(
                        &self.log,
                        "Storage manager processing zpool: {:#?}", pool.info
                    );

                    let size = ByteCount::try_from(pool.info.size())?;

                    // If we find filesystems within our datasets, ensure their
                    // zones are up-and-running.
                    let mut datasets = vec![];
                    let existing_filesystems = Zfs::list_filesystems(&pool_name.to_string())?;
                    for fs_name in existing_filesystems {
                        info!(&self.log, "StorageWorker loading fs {} on zpool {}", fs_name, pool_name.to_string());
                        // We intentionally do not exit on error here -
                        // otherwise, the failure of a single dataset would
                        // stop the storage manager from processing all storage.
                        //
                        // Instead, we opt to log the failure.
                        let dataset_name = DatasetName::new(&pool_name.to_string(), &fs_name);
                        let result = self.load_dataset(pool, &dataset_name).await;
                        match result {
                            Ok(dataset) => datasets.push(dataset),
                            Err(e) => warn!(&self.log, "StorageWorker Failed to load dataset: {}", e),
                        }
                    }

                    // Notify Nexus of the zpool and all datasets within.
                    self.add_zpool_notify(
                        &mut nexus_notifications,
                        pool.id(),
                        size,
                    );

                    self.add_datasets_notify(
                        &mut nexus_notifications,
                        datasets,
                        pool.id(),
                    );
                },
                Some(request) = self.new_filesystems_rx.recv() => {
                    let result = self.add_dataset(&mut nexus_notifications, &request).await;
                    let _ = request.responder.send(result);
                }
            }
        }
    }
}

/// A sled-local view of all attached storage.
pub struct StorageManager {
    // A map of "zpool name" to "pool".
    pools: Arc<Mutex<HashMap<ZpoolName, Pool>>>,
    new_pools_tx: mpsc::Sender<ZpoolName>,
    new_filesystems_tx: mpsc::Sender<NewFilesystemRequest>,

    // A handle to a worker which updates "pools".
    task: JoinHandle<Result<(), Error>>,
}

impl StorageManager {
    /// Creates a new [`StorageManager`] which should manage local storage.
    pub async fn new(
        log: &Logger,
        sled_id: Uuid,
        nexus_client: Arc<NexusClient>,
    ) -> Result<Self, Error> {
        let log = log.new(o!("component" => "sled agent storage manager"));
        let pools = Arc::new(Mutex::new(HashMap::new()));
        let (new_pools_tx, new_pools_rx) = mpsc::channel(10);
        let (new_filesystems_tx, new_filesystems_rx) = mpsc::channel(10);
        let mut worker = StorageWorker {
            log,
            sled_id,
            nexus_client,
            pools: pools.clone(),
            new_pools_rx,
            new_filesystems_rx,
            vnic_allocator: VnicAllocator::new("Storage"),
        };
        Ok(StorageManager {
            pools,
            new_pools_tx,
            new_filesystems_tx,
            task: tokio::task::spawn(async move { worker.do_work().await }),
        })
    }

    /// Adds a zpool to the storage manager.
    pub async fn upsert_zpool(&self, name: &ZpoolName) -> Result<(), Error> {
        let zpool = Pool::new(name)?;

        let is_new = {
            let mut pools = self.pools.lock().await;
            let entry = pools.entry(name.clone());
            let is_new =
                matches!(entry, std::collections::hash_map::Entry::Vacant(_));

            // Ensure that the pool info is up-to-date.
            entry
                .and_modify(|e| {
                    e.info = zpool.info.clone();
                })
                .or_insert_with(|| zpool);
            is_new
        };

        // If we hadn't previously been handling this zpool, hand it off to the
        // worker for management (zone creation).
        if is_new {
            self.new_pools_tx.send(name.clone()).await.unwrap();
        }
        Ok(())
    }

    pub async fn upsert_filesystem(
        &self,
        zpool_id: Uuid,
        dataset_kind: DatasetKind,
        address: SocketAddr,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let request = NewFilesystemRequest {
            zpool_id,
            dataset_kind,
            address,
            responder: tx,
        };

        self.new_filesystems_tx
            .send(request)
            .await
            .expect("Storage worker bug (not alive)");
        rx.await.expect(
            "Storage worker bug (dropped responder without responding)",
        )?;

        Ok(())
    }
}

impl Drop for StorageManager {
    fn drop(&mut self) {
        // NOTE: Ideally, with async drop, we'd await completion of the worker
        // somehow.
        //
        // Without that option, we instead opt to simply cancel the worker
        // task to ensure it does not remain alive beyond the StorageManager
        // itself.
        self.task.abort();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn serialize_dataset_info() {
        let dataset_info = DatasetInfo {
            data_directory: "/here/is/my/path".to_string(),
            address: "127.0.0.1:8080".parse().unwrap(),
            kind: DatasetKind::Crucible,
            name: DatasetName::new("pool", "dataset"),
        };

        toml::to_string(&dataset_info).unwrap();
    }
}
