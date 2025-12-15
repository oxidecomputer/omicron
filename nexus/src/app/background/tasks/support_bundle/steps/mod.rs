// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Individual support bundle collection steps

use crate::app::background::tasks::support_bundle::cache::Cache;
use crate::app::background::tasks::support_bundle::step::CollectionStep;
use futures::FutureExt;
use nexus_types::internal_api::background::SupportBundleCollectionStep;

mod bundle_id;
mod ereports;
mod host_info;
mod omdb;
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
            Box::new(|collection, dir| {
                bundle_id::collect(collection, dir).boxed()
            }),
        ),
        CollectionStep::new(
            SupportBundleCollectionStep::STEP_RECONFIGURATOR_STATE,
            Box::new(|collection, dir| {
                reconfigurator::collect(collection, dir).boxed()
            }),
        ),
        CollectionStep::new(
            SupportBundleCollectionStep::STEP_EREPORTS,
            Box::new(|collection, dir| {
                ereports::collect(collection, dir).boxed()
            }),
        ),
        CollectionStep::new(
            SupportBundleCollectionStep::STEP_SLED_CUBBY_INFO,
            Box::new({
                let cache = cache.clone();
                move |collection, dir| {
                    async move {
                        sled_cubby::collect(
                            collection,
                            &cache,
                            dir
                        ).await
                    }
                    .boxed()
                }
            }),
        ),
        CollectionStep::new(
            SupportBundleCollectionStep::STEP_SPAWN_SP_DUMPS,
            Box::new({
                let cache = cache.clone();
                move |collection, dir| {
                    async move {
                        sp_dumps::spawn_collection_steps(
                            collection, &cache, dir,
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
        CollectionStep::new(
            SupportBundleCollectionStep::STEP_OMDB,
            Box::new(|collection, dir| omdb::collect(collection, dir).boxed()),
        ),
    ]
}
