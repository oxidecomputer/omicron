// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Individual support bundle collection steps

use crate::cache::Cache;
use crate::step::CollectionStep;
use futures::FutureExt;
use nexus_types::internal_api::background::SupportBundleCollectionStep;

mod ereports;
mod host_info;
mod metadata;
mod reconfigurator;
mod sled_cubby;
mod sp_dumps;

/// Returns all steps necessary to collect a bundle.
///
/// Note that these steps themselves may spawn additional steps while executing
/// (e.g., there is a step to read the set of sleds, from which additional
/// sled-specific steps may be created).
pub fn all(cache: &Cache) -> Vec<CollectionStep> {
    vec![
        CollectionStep::new(
            SupportBundleCollectionStep::STEP_BUNDLE_ID,
            Box::new(|collection, output| {
                metadata::collect_bundle_id(collection, output).boxed()
            }),
        ),
        CollectionStep::new(
            SupportBundleCollectionStep::STEP_REASON_FOR_CREATION,
            Box::new(|collection, output| {
                metadata::collect_reason_for_creation(collection, output)
                    .boxed()
            }),
        ),
        CollectionStep::new(
            SupportBundleCollectionStep::STEP_RECONFIGURATOR_STATE,
            Box::new(|collection, output| {
                reconfigurator::collect(collection, output).boxed()
            }),
        ),
        CollectionStep::new(
            SupportBundleCollectionStep::STEP_EREPORTS,
            Box::new(|collection, output| {
                ereports::collect(collection, output).boxed()
            }),
        ),
        CollectionStep::new(
            SupportBundleCollectionStep::STEP_SLED_CUBBY_INFO,
            Box::new({
                let cache = cache.clone();
                move |collection, output| {
                    async move {
                        sled_cubby::collect(collection, &cache, output).await
                    }
                    .boxed()
                }
            }),
        ),
        CollectionStep::new(
            SupportBundleCollectionStep::STEP_SPAWN_SP_DUMPS,
            Box::new({
                let cache = cache.clone();
                move |collection, output| {
                    async move {
                        sp_dumps::spawn_collection_steps(
                            collection, &cache, output,
                        )
                        .await
                    }
                    .boxed()
                }
            }),
        ),
        CollectionStep::new(
            SupportBundleCollectionStep::STEP_SPAWN_SLEDS,
            Box::new({
                let cache = cache.clone();
                move |collection, _| {
                    async move {
                        host_info::spawn_query_all_sleds(collection, &cache)
                            .await
                    }
                    .boxed()
                }
            }),
        ),
    ]
}
