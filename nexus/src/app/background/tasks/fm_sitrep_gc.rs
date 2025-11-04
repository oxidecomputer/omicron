// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for fault management sitrep garbage collection.

use crate::app::background::BackgroundTask;
use crate::db::model::SqlU32;
use crate::db::pagination::Paginator;
use dropshot::PaginationOrder;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_types::internal_api::background::OrphanedSitreps;
use nexus_types::internal_api::background::SitrepGcStatus as Status;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;

pub struct SitrepGc {
    datastore: Arc<DataStore>,
}

impl BackgroundTask for SitrepGc {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async {
            let status = self.actually_activate(opctx).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(err) => {
                    let err = format!(
                        "could not serialize task status: {}",
                        InlineErrorChain::new(&err)
                    );
                    json!({ "error": err })
                }
            }
        })
    }
}

impl SitrepGc {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }

    async fn actually_activate(&mut self, opctx: &OpContext) -> Status {
        let mut status = Status::default();
        let mut total_found = 0;
        let mut total_deleted = 0;
        let mut paginator =
            Paginator::new(SQL_BATCH_SIZE, PaginationOrder::Descending);

        while let Some(p) = paginator.next() {
            let versions = match self
                .datastore
                .fm_sitrep_version_list(opctx, &p.current_pagparams())
                .await
            {
                Ok(versions) => versions,
                Err(err) => {
                    let err = InlineErrorChain::new(&err);
                    slog::error!(
                        &opctx.log,
                        "failed to list committed sitrep versions";
                        &err,
                    );
                    status.errors.push(format!(
                        "failed to list sitrep versions: {err}",
                    ));
                    break;
                }
            };

            paginator = p.found_batch(&versions, &|v| SqlU32::from(v.version));

            for v in versions {
                let version = v.version;
                let orphans = match self
                    .datastore
                    .fm_sitrep_list_orphaned(opctx, &v)
                    .await
                {
                    Ok(orphans) if orphans.is_empty() => {
                        slog::trace!(
                            &opctx.log,
                            "no orphaned sitreps found at v{version}";
                            "version" => version,
                        );
                        continue;
                    }
                    Ok(orphans) => orphans,
                    Err(err) => {
                        const MSG: &str =
                            "failed to list orphaned sitreps at v";
                        let err = InlineErrorChain::new(&err);
                        slog::error!(
                            &opctx.log,
                            "{MSG}{version}";
                            "version" => version,
                            &err,
                        );
                        status.errors.push(format!("{MSG}{version}: {err}"));
                        continue;
                    }
                };

                status.versions_scanned += 1;
                let found = orphans.len();
                total_found += found;
                slog::trace!(
                    &opctx.log,
                    "found {found} orphaned sitreps at v{version}";
                    "version" => version,
                );

                let deleted = match self
                    .datastore
                    .fm_sitrep_delete_all(
                        &opctx,
                        orphans.into_iter().map(|m| m.id),
                    )
                    .await
                {
                    Ok(deleted) => deleted,
                    Err(err) => {
                        let err = InlineErrorChain::new(&err);
                        const MSG: &str =
                            "failed to delete orphaned sitreps at v";
                        slog::error!(
                            &opctx.log,
                            "{MSG}{version}";
                            "version" => v.version,
                            &err,
                        );
                        status.errors.push(format!("{MSG}{version}: {err}"));
                        continue;
                    }
                };

                total_deleted += deleted;
                status
                    .orphaned_sitreps
                    .insert(version, OrphanedSitreps { found, deleted });

                if deleted > 0 {
                    slog::debug!(
                        &opctx.log,
                        "deleted {deleted} of {found} orphaned sitreps at \
                         v{version}";
                        "version" => v.version,
                    );
                } else {
                    slog::trace!(
                        &opctx.log,
                        "all {found} orphaned sitreps at v{version} have \
                         already been deleted";
                        "version" => v.version,
                    );
                }
            }
        }

        slog::info!(
            &opctx.log,
            "sitrep garbage collection scanned {} versions, \
             found {total_found} orphaned sitreps, deleted {total_deleted}",
            status.versions_scanned,
        );

        status
    }
}
