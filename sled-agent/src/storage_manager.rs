// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of sled-local storage.

use crate::illumos::dladm::Etherstub;
use crate::illumos::link::VnicAllocator;
use crate::illumos::running_zone::{InstalledZone, RunningZone};
use crate::illumos::zone::AddressRequest;
use crate::illumos::zpool::ZpoolName;
use crate::illumos::{zfs::Mountpoint, zone::ZONE_PREFIX, zpool::ZpoolInfo};
use crate::nexus::LazyNexusClient;
use crate::params::DatasetKind;
use futures::stream::FuturesOrdered;
use futures::FutureExt;
use futures::StreamExt;
use nexus_client::types::{DatasetPutRequest, ZpoolPutRequest};
use omicron_common::api::external::{ByteCount, ByteCountRangeError};
use omicron_common::backoff;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::{IpAddr, Ipv6Addr, SocketAddrV6};
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

const COCKROACH_SVC: &str = "svc:/system/illumos/cockroachdb";
const COCKROACH_DEFAULT_SVC: &str = "svc:/system/illumos/cockroachdb:default";

const CLICKHOUSE_SVC: &str = "svc:/system/illumos/clickhouse";
const CLICKHOUSE_DEFAULT_SVC: &str = "svc:/system/illumos/clickhouse:default";

const CRUCIBLE_AGENT_SVC: &str = "svc:/oxide/crucible/agent";
const CRUCIBLE_AGENT_DEFAULT_SVC: &str = "svc:/oxide/crucible/agent:default";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    // TODO: We could add the context of "why are we doint this op", maybe?
    #[error(transparent)]
    ZfsListDataset(#[from] crate::illumos::zfs::ListDatasetsError),

    #[error(transparent)]
    ZfsEnsureFilesystem(#[from] crate::illumos::zfs::EnsureFilesystemError),

    #[error(transparent)]
    ZfsSetValue(#[from] crate::illumos::zfs::SetValueError),

    #[error(transparent)]
    ZfsGetValue(#[from] crate::illumos::zfs::GetValueError),

    #[error(transparent)]
    GetZpoolInfo(#[from] crate::illumos::zpool::GetInfoError),

    #[error(transparent)]
    ZoneCommand(#[from] crate::illumos::running_zone::RunCommandError),

    #[error(transparent)]
    ZoneBoot(#[from] crate::illumos::running_zone::BootError),

    #[error(transparent)]
    ZoneEnsureAddress(#[from] crate::illumos::running_zone::EnsureAddressError),

    #[error(transparent)]
    ZoneInstall(#[from] crate::illumos::running_zone::InstallZoneError),

    #[error("Error parsing pool {name}'s size: {err}")]
    BadPoolSize {
        name: String,
        #[source]
        err: ByteCountRangeError,
    },

    #[error("Failed to parse the dataset {name}'s UUID: {err}")]
    ParseDatasetUuid {
        name: String,
        #[source]
        err: uuid::Error,
    },

    #[error("Zpool Not Found: {0}")]
    ZpoolNotFound(String),

    #[error("Failed to serialize toml (intended for {path:?}): {err}")]
    Serialize {
        path: PathBuf,
        #[source]
        err: toml::ser::Error,
    },

    #[error("Failed to deserialize toml from {path:?}: {err}")]
    Deserialize {
        path: PathBuf,
        #[source]
        err: toml::de::Error,
    },

    #[error("Failed to perform I/O: {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },
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
        let path = std::path::Path::new(omicron_common::OMICRON_CONFIG_PATH)
            .join(self.id.to_string());
        create_dir_all(&path).await.map_err(|err| Error::Io {
            message: format!("creating config dir {path:?}, which would contain config for {dataset_id}"),
            err,
        })?;
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
    address: SocketAddrV6,
    kind: DatasetKind,
    name: DatasetName,
}

impl DatasetInfo {
    fn new(
        pool: &str,
        kind: DatasetKind,
        address: SocketAddrV6,
    ) -> DatasetInfo {
        match kind {
            DatasetKind::CockroachDb { .. } => DatasetInfo {
                name: DatasetName::new(pool, "cockroachdb"),
                address,
                kind,
            },
            DatasetKind::Clickhouse { .. } => DatasetInfo {
                name: DatasetName::new(pool, "clickhouse"),
                address,
                kind,
            },
            DatasetKind::Crucible { .. } => DatasetInfo {
                name: DatasetName::new(pool, "crucible"),
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
        address: SocketAddrV6,
        do_format: bool,
    ) -> Result<(), Error> {
        match self.kind {
            DatasetKind::CockroachDb { .. } => {
                info!(log, "start_zone: Loading CRDB manifest");
                // Load the CRDB manifest.
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "import",
                    "/var/svc/manifest/site/cockroachdb/manifest.xml",
                ])?;

                // Set parameters which are passed to the CRDB binary.
                info!(
                    log,
                    "start_zone: setting CRDB's config/listen_addr: {}",
                    address
                );
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    COCKROACH_SVC,
                    "setprop",
                    &format!("config/listen_addr={}", address),
                ])?;

                info!(log, "start_zone: setting CRDB's config/store");
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    COCKROACH_SVC,
                    "setprop",
                    "config/store=/data",
                ])?;
                // TODO(https://github.com/oxidecomputer/omicron/issues/727)
                //
                // Set these addresses, use "start" instead of
                // "start-single-node".
                info!(log, "start_zone: setting CRDB's config/join_addrs");
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    COCKROACH_SVC,
                    "setprop",
                    &format!("config/join_addrs={}", "unknown"),
                ])?;

                // Refresh the manifest with the new properties we set,
                // so they become "effective" properties when the service is enabled.
                info!(log, "start_zone: refreshing manifest");
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    COCKROACH_DEFAULT_SVC,
                    "refresh",
                ])?;

                info!(log, "start_zone: enabling CRDB service");
                zone.run_cmd(&[
                    crate::illumos::zone::SVCADM,
                    "enable",
                    "-t",
                    COCKROACH_DEFAULT_SVC,
                ])?;

                // Await liveness of the cluster.
                info!(log, "start_zone: awaiting liveness of CRDB");
                let check_health = || async {
                    let http_addr =
                        SocketAddrV6::new(*address.ip(), 8080, 0, 0);
                    reqwest::get(format!("http://{}/health?ready=1", http_addr))
                        .await
                        .map_err(backoff::BackoffError::transient)
                };
                let log_failure = |_, _| {
                    warn!(log, "cockroachdb not yet alive");
                };
                backoff::retry_notify(
                    backoff::retry_policy_local(),
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
                    CLICKHOUSE_SVC,
                    "setprop",
                    &format!("config/listen_host={}", address.ip()),
                ])?;
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    CLICKHOUSE_SVC,
                    "setprop",
                    "config/store=/data",
                ])?;

                // Refresh the manifest with the new properties we set,
                // so they become "effective" properties when the service is enabled.
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    CLICKHOUSE_DEFAULT_SVC,
                    "refresh",
                ])?;

                zone.run_cmd(&[
                    crate::illumos::zone::SVCADM,
                    "enable",
                    "-t",
                    CLICKHOUSE_DEFAULT_SVC,
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
                    CRUCIBLE_AGENT_SVC,
                    "setprop",
                    &format!("config/listen={}", address),
                ])?;

                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    CRUCIBLE_AGENT_SVC,
                    "setprop",
                    &format!("config/dataset={}", self.name.full()),
                ])?;

                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    CRUCIBLE_AGENT_SVC,
                    "setprop",
                    &format!("config/uuid={}", Uuid::new_v4()),
                ])?;

                // Refresh the manifest with the new properties we set,
                // so they become "effective" properties when the service is enabled.
                zone.run_cmd(&[
                    crate::illumos::zone::SVCCFG,
                    "-s",
                    CRUCIBLE_AGENT_DEFAULT_SVC,
                    "refresh",
                ])?;

                zone.run_cmd(&[
                    crate::illumos::zone::SVCADM,
                    "enable",
                    "-t",
                    CRUCIBLE_AGENT_DEFAULT_SVC,
                ])?;

                Ok(())
            }
        }
    }
}

// Ensures that a zone backing a particular dataset is running.
async fn ensure_running_zone(
    log: &Logger,
    vnic_allocator: &VnicAllocator<Etherstub>,
    dataset_info: &DatasetInfo,
    dataset_name: &DatasetName,
    do_format: bool,
    underlay_address: Ipv6Addr,
) -> Result<RunningZone, Error> {
    let address_request = AddressRequest::new_static(
        IpAddr::V6(*dataset_info.address.ip()),
        None,
    );

    let err =
        RunningZone::get(log, &dataset_info.zone_prefix(), address_request)
            .await;
    match err {
        Ok(zone) => {
            info!(log, "Zone for {} is already running", dataset_name.full());
            return Ok(zone);
        }
        Err(crate::illumos::running_zone::GetZoneError::NotFound {
            ..
        }) => {
            info!(log, "Zone for {} was not found", dataset_name.full());

            let installed_zone = InstalledZone::install(
                log,
                vnic_allocator,
                &dataset_info.name.dataset_name,
                Some(&dataset_name.pool_name),
                &[zone::Dataset { name: dataset_name.full() }],
                &[],
                vec![],
                None,
                vec![],
            )
            .await?;

            let zone = RunningZone::boot(installed_zone).await?;

            zone.ensure_address(address_request).await?;

            let gateway = underlay_address;
            zone.add_default_route(gateway)
                .await
                .map_err(Error::ZoneCommand)?;

            dataset_info
                .start_zone(log, &zone, dataset_info.address, do_format)
                .await?;

            Ok(zone)
        }
        Err(crate::illumos::running_zone::GetZoneError::NotRunning {
            name,
            state,
        }) => {
            // TODO(https://github.com/oxidecomputer/omicron/issues/725):
            unimplemented!("Handle a zone which exists, but is not running: {name}, in {state:?}");
        }
        Err(err) => {
            // TODO(https://github.com/oxidecomputer/omicron/issues/725):
            unimplemented!(
                "Handle a zone which exists, has some other problem: {err}"
            );
        }
    }
}

type NotifyFut = dyn futures::Future<Output = Result<(), String>> + Send;

#[derive(Debug)]
struct NewFilesystemRequest {
    zpool_id: Uuid,
    dataset_kind: DatasetKind,
    address: SocketAddrV6,
    responder: oneshot::Sender<Result<(), Error>>,
}

// A worker that starts zones for pools as they are received.
struct StorageWorker {
    log: Logger,
    sled_id: Uuid,
    lazy_nexus_client: LazyNexusClient,
    pools: Arc<Mutex<HashMap<ZpoolName, Pool>>>,
    new_pools_rx: mpsc::Receiver<ZpoolName>,
    new_filesystems_rx: mpsc::Receiver<NewFilesystemRequest>,
    vnic_allocator: VnicAllocator<Etherstub>,
    underlay_address: Ipv6Addr,
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
        Zfs::ensure_zoned_filesystem(
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
    // Returns the UUID attached to the underlying ZFS dataset.
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
        // Otherwise, the zone may or may not exist.
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
            self.underlay_address,
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
        let lazy_nexus_client = self.lazy_nexus_client.clone();
        let notify_nexus = move || {
            let zpool_request = ZpoolPutRequest { size: size.into() };
            let lazy_nexus_client = lazy_nexus_client.clone();
            async move {
                lazy_nexus_client
                    .get()
                    .await
                    .map_err(|e| {
                        backoff::BackoffError::transient(e.to_string())
                    })?
                    .zpool_put(&sled_id, &pool_id, &zpool_request)
                    .await
                    .map_err(|e| {
                        backoff::BackoffError::transient(e.to_string())
                    })?;
                Ok(())
            }
        };
        let log = self.log.clone();
        let log_post_failure = move |_, delay| {
            warn!(
                log,
                "failed to notify nexus, will retry in {:?}", delay;
            );
        };
        nexus_notifications.push_back(
            backoff::retry_notify(
                backoff::retry_policy_internal_service_aggressive(),
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
        datasets: Vec<(Uuid, SocketAddrV6, DatasetKind)>,
        pool_id: Uuid,
    ) {
        let lazy_nexus_client = self.lazy_nexus_client.clone();
        let notify_nexus = move || {
            let lazy_nexus_client = lazy_nexus_client.clone();
            let datasets = datasets.clone();
            async move {
                let nexus = lazy_nexus_client.get().await.map_err(|e| {
                    backoff::BackoffError::transient(e.to_string())
                })?;

                for (id, address, kind) in datasets {
                    let request = DatasetPutRequest {
                        address: address.to_string(),
                        kind: kind.into(),
                    };
                    nexus.dataset_put(&pool_id, &id, &request).await.map_err(
                        |e| backoff::BackoffError::transient(e.to_string()),
                    )?;
                }
                Ok(())
            }
        };
        let log = self.log.clone();
        let log_post_failure = move |_, delay| {
            warn!(
                log,
                "failed to notify nexus about datasets, will retry in {:?}", delay;
            );
        };
        nexus_notifications.push_back(
            backoff::retry_notify(
                backoff::retry_policy_internal_service_aggressive(),
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
        info!(self.log, "add_dataset: {:?}", request);
        let mut pools = self.pools.lock().await;
        let name = ZpoolName::new(request.zpool_id);
        let pool = pools.get_mut(&name).ok_or_else(|| {
            Error::ZpoolNotFound(format!(
                "{}, looked up while trying to add dataset",
                request.zpool_id
            ))
        })?;

        let pool_name = pool.info.name();
        let dataset_info = DatasetInfo::new(
            pool_name,
            request.dataset_kind.clone(),
            request.address,
        );
        let (is_new_dataset, id) = self
            .initialize_dataset_and_zone(
                pool,
                &dataset_info,
                // do_format=
                true,
            )
            .await?;

        if !is_new_dataset {
            return Ok(());
        }

        // Now that the dataset has been initialized, record the configuration
        // so it can re-initialize itself after a reboot.
        let path = pool.dataset_config_path(id).await?;
        let info_str = toml::to_string(&dataset_info)
            .map_err(|err| Error::Serialize { path: path.clone(), err })?;
        let pool_name = pool.info.name();
        let mut file = File::create(&path).await.map_err(|err| Error::Io {
            message: format!("Failed creating config file at {path:?} for pool {pool_name}, dataset: {id}"),
            err,
        })?;
        file.write_all(info_str.as_bytes()).await.map_err(|err| Error::Io {
            message: format!("Failed writing config to {path:?} for pool {pool_name}, dataset: {id}"),
            err,
        })?;

        self.add_datasets_notify(
            nexus_notifications,
            vec![(id, dataset_info.address, dataset_info.kind)],
            pool.id(),
        );

        Ok(())
    }

    async fn load_dataset(
        &self,
        pool: &mut Pool,
        dataset_name: &DatasetName,
    ) -> Result<(Uuid, SocketAddrV6, DatasetKind), Error> {
        let name = dataset_name.full();
        let id = Zfs::get_oxide_value(&name, "uuid")?
            .parse::<Uuid>()
            .map_err(|err| Error::ParseDatasetUuid { name, err })?;
        let config_path = pool.dataset_config_path(id).await?;
        info!(
            self.log,
            "Loading Dataset from {}",
            config_path.to_string_lossy()
        );
        let pool_name = pool.info.name();
        let dataset_info: DatasetInfo =
            toml::from_slice(
                &tokio::fs::read(&config_path).await.map_err(|err| Error::Io {
                    message: format!("read config for pool {pool_name}, dataset {dataset_name:?} from {config_path:?}"),
                    err,
                })?
            ).map_err(|err| {
                Error::Deserialize {
                    path: config_path,
                    err,
                }
            })?;
        self.initialize_dataset_and_zone(
            pool,
            &dataset_info,
            // do_format=
            false,
        )
        .await?;

        Ok((id, dataset_info.address, dataset_info.kind))
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

                    let size = ByteCount::try_from(pool.info.size())
                        .map_err(|err| Error::BadPoolSize { name: pool_name.to_string(), err })?;

                    // If we find filesystems within our datasets, ensure their
                    // zones are up-and-running.
                    let mut datasets = vec![];
                    let existing_filesystems = Zfs::list_datasets(&pool_name.to_string())?;
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
        lazy_nexus_client: LazyNexusClient,
        etherstub: Etherstub,
        underlay_address: Ipv6Addr,
    ) -> Self {
        let log = log.new(o!("component" => "StorageManager"));
        let pools = Arc::new(Mutex::new(HashMap::new()));
        let (new_pools_tx, new_pools_rx) = mpsc::channel(10);
        let (new_filesystems_tx, new_filesystems_rx) = mpsc::channel(10);
        let mut worker = StorageWorker {
            log,
            sled_id,
            lazy_nexus_client,
            pools: pools.clone(),
            new_pools_rx,
            new_filesystems_rx,
            vnic_allocator: VnicAllocator::new("Storage", etherstub),
            underlay_address,
        };
        StorageManager {
            pools,
            new_pools_tx,
            new_filesystems_tx,
            task: tokio::task::spawn(async move { worker.do_work().await }),
        }
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
        address: SocketAddrV6,
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
            address: "[::1]:8080".parse().unwrap(),
            kind: DatasetKind::Crucible,
            name: DatasetName::new("pool", "dataset"),
        };

        toml::to_string(&dataset_info).unwrap();
    }
}
