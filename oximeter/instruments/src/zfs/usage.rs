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

oximeter::use_timeseries!("zfs-pool.toml");
oximeter::use_timeseries!("zfs-dataset.toml");

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
    let pool_names: Vec<String> =
        pools.iter().map(|pool| pool.name().to_string()).collect();
    let datasets = Zfs::get_dataset_properties(
        &pool_names,
        illumos_utils::zfs::WhichDatasets::Recursive,
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

#[derive(Debug, Default, PartialEq)]
struct DatasetAttributes {
    dataset_id: Uuid,
    dataset_kind: String,
    pool_id: Uuid,
    pool_kind: String,
    zone_name: String,
}

// Parse a dataset into metric attributes.
//
// This is necessarily a bit of a mess. Some datasets are managed by sled-agent, others by crucible, etc., and naming and metadata vary by source.
fn parse_dataset_attributes(props: &DatasetProperties) -> DatasetAttributes {
    let mut attributes = DatasetAttributes::default();

    // Zones managed by reconfigurator set a uuid via zfs metadata. Prefer this id if present; else we'll attempt to parse it from the dataset's name below.
    if let Some(id) = props.id {
        attributes.dataset_id = id.into_untyped_uuid();
    }

    // If the dataset can be parsed by DatasetName, use its parsed metadata.
    let maybe_dataset = DatasetName::from_str(&props.name).ok();
    if let Some(dataset) = &maybe_dataset {
        attributes.dataset_kind = match dataset.kind() {
            DatasetKind::TransientZoneRoot => "transient_zone_root".to_string(),
            DatasetKind::TransientZone { .. } => "transient_zone".to_string(),
            other => other.to_string(),
        };
        if let DatasetKind::TransientZone { name } = dataset.kind() {
            attributes.zone_name = name.clone();
        }
    // Otherwise, check against known dataset naming patterns.
    } else if let Some((_, rest)) = props.name.split_once('/') {
        let parts: Vec<&str> = rest.split('/').collect();
        let (dataset_kind, dataset_id) = match parts[..] {
            // crucible/regions/<REGION_UUID> represents a crucible region.
            ["crucible", "regions", id] => ("crucible_region", id),
            // local_storage_unencrypted/<ALLOCATION_ID> represents the parent dataset for an unencrypted local storage disk.
            ["local_storage_unencrypted", id] => {
                ("local_storage_unencrypted_allocation", id)
            }
            // local_storage_unencrypted/<ALLOCATION_ID>/vol represents the zvol for an unencrypted local storage disk.
            ["local_storage_unencrypted", id, "vol"] => {
                ("local_storage_unencrypted_volume", id)
            }
            // The encrypted variants of the above.
            ["crypt", "local_storage", id] => ("local_storage_allocation", id),
            ["crypt", "local_storage", id, "vol"] => {
                ("local_storage_volume", id)
            }
            _ => ("", ""),
        };
        attributes.dataset_kind = dataset_kind.to_string();
        if attributes.dataset_id.is_nil() {
            attributes.dataset_id = dataset_id.parse().unwrap_or_default();
        }
    }

    // Parse the zfs pool metadata from the dataset name.
    let dataset_prefix =
        props.name.split_once('/').map(|(part, _)| part).unwrap_or(&props.name);
    if let Ok(pool) = ZpoolName::from_str(dataset_prefix) {
        attributes.pool_id = pool.id().into_untyped_uuid();
        attributes.pool_kind = pool.kind().to_string();
    }

    attributes
}

// Parse a dataset into metrics.
fn dataset_samples(
    target: &zfs_dataset::ZfsDataset,
    props: &DatasetProperties,
) -> Result<Vec<Sample>, Error> {
    let attributes = parse_dataset_attributes(props);

    let mut samples = Vec::<Sample>::new();

    samples.push(Sample::new(
        target,
        &zfs_dataset::BytesUsed {
            dataset_name: props.name.clone().into(),
            dataset_id: attributes.dataset_id,
            dataset_kind: attributes.dataset_kind.clone().into(),
            pool_kind: attributes.pool_kind.clone().into(),
            pool_id: attributes.pool_id,
            zone_name: attributes.zone_name.clone().into(),
            datum: props.used.to_bytes(),
        },
    )?);
    samples.push(Sample::new(
        target,
        &zfs_dataset::BytesAvailable {
            dataset_name: props.name.clone().into(),
            dataset_id: attributes.dataset_id,
            dataset_kind: attributes.dataset_kind.clone().into(),
            pool_kind: attributes.pool_kind.clone().into(),
            pool_id: attributes.pool_id,
            zone_name: attributes.zone_name.clone().into(),
            datum: props.avail.to_bytes(),
        },
    )?);
    if let Some(reservation) = props.reservation {
        samples.push(Sample::new(
            target,
            &zfs_dataset::BytesReserved {
                dataset_name: props.name.clone().into(),
                dataset_id: attributes.dataset_id,
                dataset_kind: attributes.dataset_kind.clone().into(),
                pool_kind: attributes.pool_kind.clone().into(),
                pool_id: attributes.pool_id,
                zone_name: attributes.zone_name.clone().into(),
                datum: reservation.to_bytes(),
            },
        )?);
    }
    if let Some(quota) = props.quota {
        samples.push(Sample::new(
            target,
            &zfs_dataset::BytesQuota {
                dataset_name: props.name.clone().into(),
                dataset_id: attributes.dataset_id,
                dataset_kind: attributes.dataset_kind.into(),
                pool_kind: attributes.pool_kind.into(),
                pool_id: attributes.pool_id,
                zone_name: attributes.zone_name.into(),
                datum: quota.to_bytes(),
            },
        )?);
    }

    Ok(samples)
}

#[cfg(test)]
mod tests {
    use omicron_uuid_kinds::DatasetUuid;
    use rstest::rstest;

    use super::*;

    const POOL: &str = "oxp_4c7ad252-55c2-4a1a-9d93-9dfcdfdfacca";
    const POOL_ID: Uuid = uuid::uuid!("4c7ad252-55c2-4a1a-9d93-9dfcdfdfacca");
    const M2_POOL_ID: Uuid =
        uuid::uuid!("01376d9a-434a-42d3-a4c8-dc78edcdfd06");

    fn props(name: &str, id: Option<Uuid>) -> DatasetProperties {
        DatasetProperties {
            id: id.map(DatasetUuid::from_untyped_uuid),
            name: name.to_string(),
            mounted: true,
            avail: 0u32.into(),
            used: 0u32.into(),
            quota: None,
            reservation: None,
            compression: String::new(),
            epoch: None,
        }
    }

    fn external(
        dataset_kind: &str,
        dataset_id: Uuid,
        zone_name: &str,
    ) -> DatasetAttributes {
        DatasetAttributes {
            dataset_id,
            dataset_kind: dataset_kind.to_string(),
            pool_id: POOL_ID,
            pool_kind: "external".to_string(),
            zone_name: zone_name.to_string(),
        }
    }

    #[rstest]
    #[case::named_with_oxide_uuid(
        props(
            &format!("{POOL}/crucible"),
            Some(uuid::uuid!("56d4dbcc-3b4a-4ed0-8795-7734aadcc4c0")),
        ),
        external(
            "crucible",
            uuid::uuid!("56d4dbcc-3b4a-4ed0-8795-7734aadcc4c0"),
            "",
        ),
    )]
    #[case::transient_zone_root(
        props(
            &format!("{POOL}/crypt/zone"),
            Some(uuid::uuid!("3019d76f-20c1-4ac6-a39f-59b901195fc5")),
        ),
        external(
            "transient_zone_root",
            uuid::uuid!("3019d76f-20c1-4ac6-a39f-59b901195fc5"),
            "",
        ),
    )]
    #[case::propolis_zone(
        props(
            &format!(
                "{POOL}/crypt/zone/oxz_propolis-server_0507dfc2-a99c-4e11-9065-f53a26f5b4b7"
            ),
            None,
        ),
        external(
            "transient_zone",
            Uuid::nil(),
            "oxz_propolis-server_0507dfc2-a99c-4e11-9065-f53a26f5b4b7",
        ),
    )]
    #[case::crucible_region(
        props(
            &format!("{POOL}/crucible/regions/0a3c50c6-35bd-4e74-a733-0341dc193538"),
            None,
        ),
        external(
            "crucible_region",
            uuid::uuid!("0a3c50c6-35bd-4e74-a733-0341dc193538"),
            "",
        ),
    )]
    #[case::local_storage_unencrypted_allocation(
        props(
            &format!(
                "{POOL}/local_storage_unencrypted/5ccb2f5c-b96e-4f16-a6ae-2531a97cb3ce"
            ),
            None,
        ),
        external(
            "local_storage_unencrypted_allocation",
            uuid::uuid!("5ccb2f5c-b96e-4f16-a6ae-2531a97cb3ce"),
            "",
        ),
    )]
    #[case::local_storage_unencrypted_volume(
        props(
            &format!(
                "{POOL}/local_storage_unencrypted/5ccb2f5c-b96e-4f16-a6ae-2531a97cb3ce/vol"
            ),
            None,
        ),
        external(
            "local_storage_unencrypted_volume",
            uuid::uuid!("5ccb2f5c-b96e-4f16-a6ae-2531a97cb3ce"),
            "",
        ),
    )]
    #[case::unrecognized_u2(
        props(&format!("{POOL}/crucible/regions"), None),
        external("", Uuid::nil(), ""),
    )]
    #[case::m2_debug(
        props("oxi_01376d9a-434a-42d3-a4c8-dc78edcdfd06/debug", None),
        DatasetAttributes {
            dataset_id: Uuid::nil(),
            dataset_kind: String::new(),
            pool_id: M2_POOL_ID,
            pool_kind: "internal".to_string(),
            zone_name: String::new(),
        },
    )]
    #[case::rpool(props("rpool/zone", None), DatasetAttributes::default())]
    fn test_parse_dataset_attributes(
        #[case] props: DatasetProperties,
        #[case] expected: DatasetAttributes,
    ) {
        assert_eq!(parse_dataset_attributes(&props), expected);
    }

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
        assert_eq!(samples.len(), 2);

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
