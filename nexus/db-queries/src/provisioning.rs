// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types to export metrics about provisioning information.

use crate::db::model::VirtualProvisioningCollection;
use oximeter::{types::Sample, MetricsError};
use std::sync::{Arc, Mutex};

oximeter::use_timeseries!("collection-target.toml");
use collection_target::CollectionTarget;
use collection_target::CpusProvisioned;
use collection_target::RamProvisioned;
use collection_target::VirtualDiskSpaceProvisioned;

/// An oximeter producer for reporting [`VirtualProvisioningCollection`] information to Clickhouse.
///
/// This producer collects samples whenever the database record for a collection
/// is created or updated. This implies that the CockroachDB record is always
/// kept up-to-date, and the Clickhouse historical records are batched and
/// transmitted once they are collected (as is the norm for Clickhouse metrics).
#[derive(Debug, Default, Clone)]
pub(crate) struct Producer {
    samples: Arc<Mutex<Vec<Sample>>>,
}

impl Producer {
    pub fn new() -> Self {
        Self { samples: Arc::new(Mutex::new(vec![])) }
    }

    pub fn append_all_metrics(
        &self,
        provisions: &Vec<VirtualProvisioningCollection>,
    ) -> Result<(), MetricsError> {
        self.append_cpu_metrics(&provisions)?;
        self.append_disk_metrics(&provisions)
    }

    pub fn append_disk_metrics(
        &self,
        provisions: &Vec<VirtualProvisioningCollection>,
    ) -> Result<(), MetricsError> {
        let new_samples = provisions
            .iter()
            .map(|provision| {
                Sample::new_with_timestamp(
                    provision
                        .time_modified
                        .expect("Should always have default value"),
                    &CollectionTarget { id: provision.id },
                    &VirtualDiskSpaceProvisioned {
                        datum: provision.virtual_disk_bytes_provisioned.into(),
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        self.append(new_samples);
        Ok(())
    }

    pub fn append_cpu_metrics(
        &self,
        provisions: &Vec<VirtualProvisioningCollection>,
    ) -> Result<(), MetricsError> {
        let new_samples = provisions
            .iter()
            .map(|provision| {
                Sample::new_with_timestamp(
                    provision
                        .time_modified
                        .expect("Should always have default value"),
                    &CollectionTarget { id: provision.id },
                    &CpusProvisioned { datum: provision.cpus_provisioned },
                )
            })
            .chain(provisions.iter().map(|provision| {
                Sample::new_with_timestamp(
                    provision
                        .time_modified
                        .expect("Should always have default value"),
                    &CollectionTarget { id: provision.id },
                    &RamProvisioned { datum: provision.ram_provisioned.into() },
                )
            }))
            .collect::<Result<Vec<_>, _>>()?;

        self.append(new_samples);
        Ok(())
    }

    fn append(&self, mut new_samples: Vec<Sample>) {
        let mut pending_samples = self.samples.lock().unwrap();
        pending_samples.append(&mut new_samples);
    }
}

impl oximeter::Producer for Producer {
    fn produce(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Sample> + 'static>, MetricsError> {
        let samples =
            std::mem::replace(&mut *self.samples.lock().unwrap(), vec![]);
        Ok(Box::new(samples.into_iter()))
    }
}
