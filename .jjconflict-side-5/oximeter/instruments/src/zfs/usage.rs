// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Report metrics about zfs pool and dataset usage on the host system

use illumos_utils::zfs::DatasetProperties;
use illumos_utils::zfs::Zfs;
use illumos_utils::zpool::Zpool;
use illumos_utils::zpool::ZpoolInfo;
use omicron_common::api::internal::shared::SledIdentifiers;
use omicron_common::disk::{DatasetKind, DatasetName};
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::GenericUuid;
use oximeter::MetricsError;
use oximeter::Sample;
use oximeter::queue::BoundedQueue;
use slog::Logger;
use slog::warn;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::interval;
use uuid::Uuid;

oximeter::use_timeseries!("zfs_pool.toml");
oximeter::use_timeseries!("zfs_dataset.toml");

const POLL_INTERVAL: Duration = Duration::from_secs(60);
const POLL_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_QUEUE_LENGTH: usize = 1000;

#[derive(Clone, Debug)]
pub struct ZfsUsageProducer {
    samples: Arc<Mutex<BoundedQueue<Sample>>>,
    _worker: Arc<JoinHandle<()>>,
}

impl ZfsUsageProducer {
    pub fn new(log: Logger, sled: &SledIdentifiers) -> Self {
        let samples = Arc::new(Mutex::new(BoundedQueue::new(MAX_QUEUE_LENGTH)));

        let pool = zfs_pool::ZfsPool {
            rack_id: sled.rack_id,
            sled_id: sled.sled_id,
            sled_model: sled.model.clone().into(),
            sled_revision: sled.revision,
            sled_serial: sled.serial.clone().into(),
        };
        let dataset = zfs_dataset::ZfsDataset {
            rack_id: sled.rack_id,
            sled_id: sled.sled_id,
            sled_model: sled.model.clone().into(),
            sled_revision: sled.revision,
            sled_serial: sled.serial.clone().into(),
        };

        let worker = tokio::spawn(worker(samples.clone(), pool, dataset, log));

        Self { samples, _worker: Arc::new(worker) }
    }
}

impl oximeter::Producer for ZfsUsageProducer {
    fn produce(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Sample>>, MetricsError> {
        let samples = self.samples.lock().unwrap().drain();
        Ok(Box::new(samples.into_iter()))
    }
}

async fn worker(
    samples: Arc<Mutex<BoundedQueue<Sample>>>,
    pool: zfs_pool::ZfsPool,
    dataset: zfs_dataset::ZfsDataset,
    log: Logger,
) {
    let mut tick = interval(POLL_INTERVAL);
    loop {
        tick.tick().await;

        let mut new_samples = Vec::new();

        match tokio::time::timeout(POLL_TIMEOUT, collect_zpools(&pool)).await {
            Ok(Ok(samples)) => new_samples.extend(samples),
            Ok(Err(err)) => {
                warn!(log, "failed to collect zpool samples"; "err" => %err)
            }
            Err(_) => warn!(log, "zpool collection timed out"),
        }

        match tokio::time::timeout(POLL_TIMEOUT, collect_datasets(&dataset))
            .await
        {
            Ok(Ok(samples)) => new_samples.extend(samples),
            Ok(Err(err)) => {
                warn!(log, "failed to collect dataset samples"; "err" => %err);
            }
            Err(_) => warn!(log, "dataset collection timed out"),
        }

        let mut q = samples.lock().unwrap();
        let n_dropped = q.extend(new_samples);
        if n_dropped > 0 {
            warn!(log, "sample queue overflow; dropping samples"; "dropped" => n_dropped);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to list zpools")]
    ListZpools(#[from] illumos_utils::zpool::ListError),

    #[error("Failed to fetch dataset properties")]
    GetDatasetProperties(#[from] anyhow::Error),

    #[error("Failed to build sample")]
    MetricsError(#[from] oximeter::MetricsError),
}

async fn collect_zpools(
    target: &zfs_pool::ZfsPool,
) -> Result<Vec<Sample>, Error> {
    let pools = Zpool::list_with_info().await?;
    let samples = pools
        .iter()
        .map(|pool| zpool_samples(target, pool))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(samples)
}

async fn collect_datasets(
    target: &zfs_dataset::ZfsDataset,
) -> Result<Vec<Sample>, Error> {
    let pools = Zpool::list_with_info().await?;
    let anchors: Vec<String> =
        pools.iter().flat_map(|pool| dataset_anchors(pool.name())).collect();
    let datasets = Zfs::get_dataset_properties(
        &anchors,
        illumos_utils::zfs::WhichDatasets::SelfAndChildren,
    )
    .await
    .map_err(Error::GetDatasetProperties)?;
    let samples = datasets
        .iter()
        .map(|dataset| dataset_samples(target, dataset))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(samples)
}

fn dataset_anchors(pool_name: &str) -> Vec<String> {
    vec![
        pool_name.to_string(),
        format!("{pool_name}/crypt"),
        format!("{pool_name}/crypt/zone"),
    ]
}

// Parse a zpool into bytes_allocated and bytes_total metrics.
//
// Note: we extract zpool kind and uuid for pools named ox(i|p)_<uuid>.
// Otherwise, we leave those fields empty.
fn zpool_samples(
    target: &zfs_pool::ZfsPool,
    info: &ZpoolInfo,
) -> Result<Vec<Sample>, Error> {
    let (pool_kind, pool_id) = match ZpoolName::from_str(info.name()) {
        Ok(parsed) => {
            (parsed.kind().to_string(), parsed.id().into_untyped_uuid())
        }
        Err(_) => (String::new(), Uuid::nil()),
    };
    let allocated = zfs_pool::BytesAllocated {
        pool_name: info.name().to_string().into(),
        pool_kind: pool_kind.clone().into(),
        pool_id,
        datum: info.allocated(),
    };
    let total = zfs_pool::BytesTotal {
        pool_name: info.name().to_string().into(),
        pool_kind: pool_kind.into(),
        pool_id,
        datum: info.size(),
    };
    Ok(vec![Sample::new(target, &allocated)?, Sample::new(target, &total)?])
}

// Parse a dataset into a bytes_used metric.
//
// Note: we omit bytes_quota because no datasets currently set quotas;
// we'll add this metric if it becomes relevant in the future.
//
// Note: we extract dataset, zpool, and zone metadata (kind, id) if
// encoded in the dataset name as
// ox(i|p)_<pool_uuid>/crypt/zone/<zone_uuid>. For datasets that don't
// follow this naming pattern, we omit this metadata.
fn dataset_samples(
    target: &zfs_dataset::ZfsDataset,
    props: &DatasetProperties,
) -> Result<Vec<Sample>, Error> {
    let mut dataset_kind = String::new();

    let mut pool_kind = String::new();
    let mut pool_id = Uuid::nil();

    let mut zone_name = String::new();

    let dataset_id =
        props.id.map(|id| id.into_untyped_uuid()).unwrap_or(Uuid::nil());

    let maybe_dataset = DatasetName::from_str(&props.name).ok();
    if let Some(dataset) = &maybe_dataset {
        dataset_kind = match dataset.kind() {
            DatasetKind::TransientZoneRoot => "transient_zone_root".to_string(),
            DatasetKind::TransientZone { .. } => "transient_zone".to_string(),
            other => other.to_string(),
        };
        zone_name = match dataset.kind() {
            DatasetKind::TransientZone { name } => name.to_string(),
            _ => zone_name,
        };
    };

    // Best-effort attempt at parsing the zpool. If we parsed the DatasetName,
    // look up its associated zpool metadata. Otherwise, split the dataset name
    // by /, and attempt to parse the 0th part as a ZpoolName. This allows us
    // to extract pool metadata from datasets like
    // ox(i|p)_<uuid>/(crypt|install|...), which don't parse to a DatasetName.
    let maybe_pool =
        maybe_dataset.as_ref().map(|dataset| *dataset.pool()).or_else(|| {
            let prefix = props
                .name
                .split_once('/')
                .map(|(part, _)| part)
                .unwrap_or(&props.name);
            ZpoolName::from_str(prefix).ok()
        });
    if let Some(pool) = maybe_pool {
        pool_kind = pool.kind().to_string();
        pool_id = pool.id().into_untyped_uuid();
    }

    let used = zfs_dataset::BytesUsed {
        dataset_name: props.name.clone().into(),
        dataset_id,
        dataset_kind: dataset_kind.into(),
        pool_kind: pool_kind.clone().into(),
        pool_id,
        zone_name: zone_name.into(),
        datum: props.used.to_bytes(),
    };
    Ok(vec![Sample::new(target, &used)?])
}

#[cfg(test)]
mod tests {
    use omicron_uuid_kinds::DatasetUuid;

    use super::*;

    #[test]
    fn test_dataset_samples() {
        let target = zfs_dataset::ZfsDataset {
            rack_id: uuid::Uuid::nil(),
            sled_id: uuid::Uuid::nil(),
            sled_model: "test".into(),
            sled_revision: 0,
            sled_serial: "test".into(),
        };
        let dataset_id = DatasetUuid::new_v4();
        let props = DatasetProperties {
            id: Some(dataset_id),
            name: "oxp_0e485ad3-04e6-404b-b619-87d4fea9f5ae/crypt/zone/oxz_clickhouse_aa646c82-c6d7-4d0c-8401-150130927759".into(),
            mounted: true,
            avail: 0u32.into(),
            used: 12345u32.into(),
            quota: None,
            reservation: None,
            compression: String::new(),
            epoch: None,
        };
        let samples = dataset_samples(&target, &props).unwrap();
        assert_eq!(samples.len(), 1);

        let sample = &samples[0];
        assert_eq!(sample.timeseries_name, "zfs_dataset:bytes_used");

        let fields = sample.sorted_metric_fields();
        assert_eq!(fields["dataset_name"].value, props.name.clone().into());
        assert_eq!(fields["dataset_kind"].value, "transient_zone".into());
        assert_eq!(
            fields["dataset_id"].value,
            dataset_id.into_untyped_uuid().into(),
        );
        assert_eq!(fields["pool_kind"].value, "external".into());
        assert_eq!(
            fields["pool_id"].value,
            uuid::uuid!("0e485ad3-04e6-404b-b619-87d4fea9f5ae").into(),
        );
        assert_eq!(
            fields["zone_name"].value,
            "oxz_clickhouse_aa646c82-c6d7-4d0c-8401-150130927759".into(),
        );

        assert_eq!(sample.measurement.datum(), &oximeter::Datum::U64(12345));
    }

    #[cfg(target_os = "illumos")]
    #[tokio::test]
    async fn test_collect_zpools() {
        let target = zfs_pool::ZfsPool {
            rack_id: uuid::Uuid::nil(),
            sled_id: uuid::Uuid::nil(),
            sled_model: "test".into(),
            sled_revision: 0,
            sled_serial: "test".into(),
        };
        let samples =
            collect_zpools(&target).await.expect("collect should succeed");

        assert!(
            samples
                .iter()
                .filter(|s| s.timeseries_name == "zfs_pool:bytes_allocated")
                .count()
                >= 1,
            "expected at least one sample for bytes_allocated"
        );
        assert!(
            samples
                .iter()
                .filter(|s| s.timeseries_name == "zfs_pool:bytes_total")
                .count()
                >= 1,
            "expected at least one sample for bytes_total"
        );
    }

    #[cfg(target_os = "illumos")]
    #[tokio::test]
    async fn test_collect_datasets() {
        let target = zfs_dataset::ZfsDataset {
            rack_id: uuid::Uuid::nil(),
            sled_id: uuid::Uuid::nil(),
            sled_model: "test".into(),
            sled_revision: 0,
            sled_serial: "test".into(),
        };
        let samples =
            collect_datasets(&target).await.expect("collect should succeed");

        assert!(
            samples
                .iter()
                .filter(|s| s.timeseries_name == "zfs_dataset:bytes_used")
                .count()
                >= 1,
            "expected at least one sample for bytes_used"
        );
    }
}
