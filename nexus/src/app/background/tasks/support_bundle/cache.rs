// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Cached data or clients which are collected by the bundle
//!
//! This is used to share data which may be used by multiple
//! otherwise independent steps.

use crate::app::background::tasks::support_bundle::collection::BundleCollection;

use gateway_client::Client as MgsClient;
use internal_dns_types::names::ServiceName;
use nexus_db_model::Sled;
use nexus_types::deployment::SledFilter;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Caches information which can be derived from the BundleCollection.
///
/// This is exists as a small optimization for independent steps which may try
/// to read / access similar data, especially when it's fallible: we only need
/// to attempt to look it up once, and all steps can share it.
#[derive(Clone)]
pub struct Cache {
    inner: Arc<Inner>,
}

struct Inner {
    all_sleds: OnceCell<Option<Vec<Sled>>>,
    mgs_client: OnceCell<Option<MgsClient>>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                all_sleds: OnceCell::new(),
                mgs_client: OnceCell::new(),
            }),
        }
    }

    pub async fn get_or_initialize_all_sleds<'a>(
        &'a self,
        collection: &BundleCollection,
    ) -> Option<&'a Vec<Sled>> {
        self.inner
            .all_sleds
            .get_or_init(|| async {
                collection
                    .datastore()
                    .sled_list_all_batched(
                        &collection.opctx(),
                        SledFilter::InService,
                    )
                    .await
                    .ok()
            })
            .await
            .as_ref()
    }

    pub async fn get_or_initialize_mgs_client<'a>(
        &'a self,
        collection: &BundleCollection,
    ) -> Option<&'a MgsClient> {
        self.inner
            .mgs_client
            .get_or_init(|| async { create_mgs_client(collection).await.ok() })
            .await
            .as_ref()
    }
}

async fn create_mgs_client(
    collection: &BundleCollection,
) -> anyhow::Result<MgsClient> {
    let log = collection.log();
    collection
        .resolver()
        .lookup_socket_v6(ServiceName::ManagementGatewayService)
        .await
        .map(|sockaddr| {
            let url = format!("http://{}", sockaddr);
            gateway_client::Client::new(&url, log.clone())
        }).map_err(|e| {
            error!(log, "failed to resolve MGS address"; "error" => InlineErrorChain::new(&e));
            e.into()
        })
}
