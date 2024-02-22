// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Ensures dataset records required by a given blueprint

use anyhow::Context;
use illumos_utils::zpool::ZpoolName;
use nexus_db_model::Dataset;
use nexus_db_model::DatasetKind;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::OmicronZoneConfig;
use nexus_types::deployment::OmicronZoneType;
use nexus_types::identity::Asset;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::net::SocketAddrV6;

/// For each crucible zone in `blueprint`, ensure that a corresponding dataset
/// record exists in `datastore`
///
/// Does not modify any existing dataset records. Returns the number of datasets
/// inserted.
pub(crate) async fn ensure_crucible_dataset_records_exist(
    opctx: &OpContext,
    datastore: &DataStore,
    all_omicron_zones: impl Iterator<Item = &OmicronZoneConfig>,
) -> anyhow::Result<usize> {
    // Before attempting to insert any datasets, first query for any existing
    // dataset records so we can filter them out. This looks like a typical
    // TOCTOU issue, but it is purely a performance optimization. We expect
    // almost all executions of this function to do nothing: new crucible
    // datasets are created very rarely relative to how frequently blueprint
    // realization happens. We could remove this check and filter and instead
    // run the below "insert if not exists" query on every crucible zone, and
    // the behavior would still be correct. However, that would issue far more
    // queries than necessary in the very common case of "we don't need to do
    // anything at all".
    let mut crucible_datasets = datastore
        .dataset_list_all_batched(opctx, Some(DatasetKind::Crucible))
        .await
        .context("failed to list all datasets")?
        .into_iter()
        .map(|dataset| dataset.id())
        .collect::<BTreeSet<_>>();

    let datasets_to_insert = all_omicron_zones.filter_map(|zone| {
        let OmicronZoneType::Crucible { address, dataset } = &zone.zone_type
        else {
            return None;
        };

        let id = zone.id;

        // If already present in the datastore, move on.
        if crucible_datasets.remove(&id) {
            return None;
        }

        // Map progenitor client strings into the types we need. We never
        // expect these to fail.
        let addr: SocketAddrV6 = match address.parse() {
            Ok(addr) => addr,
            Err(err) => {
                warn!(
                    opctx.log, "failed to parse crucible zone address";
                    "address" => address,
                    "err" => InlineErrorChain::new(&err),
                );
                return None;
            }
        };
        let zpool_name: ZpoolName = match dataset.pool_name.parse() {
            Ok(name) => name,
            Err(err) => {
                warn!(
                    opctx.log, "failed to parse crucible zone pool name";
                    "pool_name" => &*dataset.pool_name,
                    "err" => err,
                );
                return None;
            }
        };

        let pool_id = zpool_name.id();

        Some(Dataset::new(id, pool_id, addr, DatasetKind::Crucible))
    });

    let mut num_inserted = 0;

    for dataset in datasets_to_insert {
        let id = dataset.id();
        let maybe_dataset = datastore
            .dataset_insert_if_not_exists(dataset)
            .await
            .with_context(|| {
                format!("failed to insert dataset record for dataset {id}")
            })?;

        // If we succeeded in inserting, log it; if `maybe_dataset` is `None`,
        // we must have lost the TOCTOU race described above, and another Nexus
        // must have inserted this dataset before we could.
        if let Some(dataset) = maybe_dataset {
            info!(
                opctx.log,
                "inserted new dataset for crucible zone";
                "id" => %dataset.id(),
            );
            num_inserted += 1;
        }
    }

    // We don't currently support removing datasets, so this would be
    // surprising: the database contains dataset records that are no longer in
    // our blueprint. We can't do anything about this, so just warn.
    if !crucible_datasets.is_empty() {
        warn!(
            opctx.log,
            "database contains {} unexpected crucible datasets",
            crucible_datasets.len();
            "dataset_ids" => ?crucible_datasets,
        );
    }

    Ok(num_inserted)
}
