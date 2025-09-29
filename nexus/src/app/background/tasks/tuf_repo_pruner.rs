// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for determining when to prune artifacts from TUF repos

use crate::app::background::BackgroundTask;
use anyhow::Context;
use futures::future::BoxFuture;
use iddqd::IdOrdMap;
use nexus_config::TufRepoPrunerConfig;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::internal_api::background::TufRepoInfo;
use nexus_types::internal_api::background::TufRepoPrunerStatus;
use omicron_uuid_kinds::GenericUuid as _;
use omicron_uuid_kinds::TufRepoUuid;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::Arc;

/// number of recent distinct target releases that we always keep, regardless of
/// configuration
///
/// This is intended to include (1) the current target release, and (2) the
/// previous target release.  We shouldn't need more because you can't generally
/// start another update until one has finished.
const NKEEP_RECENT_TARGET_RELEASES_ALWAYS: u8 = 2;

/// number of distinct newly-uploaded releases that we always keep, regardless
/// of configuration
///
/// This is intended to cover a release that the operator has just uploaded in
/// order to update to it.
const NKEEP_RECENT_UPLOADS_ALWAYS: u8 = 1;

/// Background task that marks TUF repos for pruning
pub struct TufRepoPruner {
    datastore: Arc<DataStore>,
    config: TufRepoPrunerConfig,
}

impl TufRepoPruner {
    pub fn new(datastore: Arc<DataStore>, config: TufRepoPrunerConfig) -> Self {
        Self { datastore, config }
    }
}

impl BackgroundTask for TufRepoPruner {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move {
            match tuf_repos_prune(opctx, &self.datastore, &self.config).await {
                Ok(status) => match serde_json::to_value(status) {
                    Ok(val) => val,
                    Err(err) => json!({
                        "error": format!(
                            "could not serialize task status: {}",
                             InlineErrorChain::new(&err)
                        ),
                    }),
                },
                Err(error) => json!({
                    "error": error.to_string(),
                }),
            }
        })
    }
}

async fn tuf_repos_prune(
    opctx: &OpContext,
    datastore: &DataStore,
    config: &TufRepoPrunerConfig,
) -> Result<TufRepoPrunerStatus, anyhow::Error> {
    let nkeep_recent_releases = NKEEP_RECENT_TARGET_RELEASES_ALWAYS
        + config.nkeep_extra_target_releases;
    let nkeep_recent_uploads =
        NKEEP_RECENT_UPLOADS_ALWAYS + config.nkeep_extra_newly_uploaded;

    let all_tuf_repos = fetch_all_tuf_repos(opctx, datastore, SQL_BATCH_SIZE)
        .await
        .context("fetching all TUF repos")?;
    let recent_releases = datastore
        .target_release_fetch_recent_distinct(opctx, nkeep_recent_releases)
        .await
        .context("listing recent target releases")?;

    // After this point, errors are not fatal.  They just generate warnings.
    let mut status = TufRepoPrunerStatus {
        repos_keep: IdOrdMap::new(),
        repos_prune: IdOrdMap::new(),
        warnings: Vec::new(),
        nkeep_recent_releases,
        nkeep_recent_uploads,
    };
    decide_prune(&all_tuf_repos, &recent_releases, &mut status);
    for tuf_repo in &status.repos_prune {
        if let Err(error) =
            datastore.tuf_repo_mark_pruned(opctx, tuf_repo.id).await
        {
            status.warnings.push(format!(
                "failed to prune {}: {}",
                tuf_repo.id,
                InlineErrorChain::new(&error),
            ));
        };
    }

    Ok(status)
}

async fn fetch_all_tuf_repos(
    opctx: &OpContext,
    datastore: &DataStore,
    batch_size: NonZeroU32,
) -> Result<IdOrdMap<TufRepoInfo>, anyhow::Error> {
    let mut paginator =
        Paginator::new(batch_size, dropshot::PaginationOrder::Ascending);
    let mut rv = IdOrdMap::new();
    while let Some(p) = paginator.next() {
        let batch = datastore
            .tuf_list_repos_unpruned(opctx, &p.current_pagparams())
            .await
            .context("fetching page of TUF repos")?;
        paginator = p.found_batch(&batch, &|a| a.id.into_untyped_uuid());
        for repo in batch {
            // It should be impossible to have duplicates here and there's
            // nothing to do about it if we find them.
            rv.insert_overwrite(TufRepoInfo {
                id: repo.id(),
                system_version: repo.system_version.into(),
                time_created: repo.time_created,
            });
        }
    }

    Ok(rv)
}

/// Given the complete list of TUF repos and a set of recent releases that we
/// definitely want to keep, decide what to prune and what to keep.
fn decide_prune(
    all_repos: &IdOrdMap<TufRepoInfo>,
    releases_to_keep: &BTreeSet<TufRepoUuid>,
    status: &mut TufRepoPrunerStatus,
) {
    // Record that we're keeping all of the `releases_to_keep`.
    for repo_id in releases_to_keep {
        match all_repos.get(repo_id) {
            Some(repo_info) => {
                status.repos_keep.insert_overwrite(repo_info.clone());
            }
            None => {
                // This is unusual: there's a recent target release with no
                // associated entry in the `tuf_repo` table.  This is
                // conceivable if the repo has just been uploaded and made the
                // target release, though still quite surprising since that
                // all would have had to happen between our two database queries
                // above.  Still, it's not a problem.  We'll note it here, but
                // otherwise move on.  (We will not wind up pruning this.)
                status.warnings.push(format!(
                    "wanting to keep recent target release repo {repo_id}, \
                     but did not find it in the tuf_repo table",
                ));
            }
        }
    }

    // Keep as many other uploaded repos as makes sense, preferring the most
    // recently uploaded.
    let mut nleft = status.nkeep_recent_uploads;
    let mut candidate_repos_by_upload_time =
        all_repos.into_iter().collect::<Vec<_>>();
    candidate_repos_by_upload_time.sort_by_key(|k| k.time_created);
    candidate_repos_by_upload_time.reverse();
    let mut candidates = candidate_repos_by_upload_time.into_iter();
    while nleft > 0 {
        let Some(next) = candidates.next() else {
            break;
        };

        // Only decrement `nleft` if we weren't already keeping this one.
        if let Ok(_) = status.repos_keep.insert_unique(next.clone()) {
            nleft -= 1;
        }
    }

    // Everything else will be pruned.
    for repo in all_repos {
        if !status.repos_keep.contains_key(&repo.id) {
            status.repos_prune.insert_overwrite(repo.clone());
        }
    }
}

#[cfg(test)]
mod test {
    use super::decide_prune;
    use iddqd::IdOrdMap;
    use nexus_types::internal_api::background::TufRepoInfo;
    use nexus_types::internal_api::background::TufRepoPrunerStatus;
    use omicron_uuid_kinds::TufRepoUuid;
    use std::collections::BTreeSet;

    fn make_test_repos() -> [TufRepoInfo; 6] {
        [
            TufRepoInfo {
                id: TufRepoUuid::new_v4(),
                time_created: "2025-09-26T01:00:00Z".parse().unwrap(),
                system_version: "1.0.0".parse().unwrap(),
            },
            TufRepoInfo {
                id: TufRepoUuid::new_v4(),
                time_created: "2025-09-26T02:00:00Z".parse().unwrap(),
                system_version: "2.0.0".parse().unwrap(),
            },
            TufRepoInfo {
                id: TufRepoUuid::new_v4(),
                time_created: "2025-09-26T03:00:00Z".parse().unwrap(),
                system_version: "3.0.0".parse().unwrap(),
            },
            TufRepoInfo {
                id: TufRepoUuid::new_v4(),
                time_created: "2025-09-26T04:00:00Z".parse().unwrap(),
                system_version: "4.0.0".parse().unwrap(),
            },
            TufRepoInfo {
                id: TufRepoUuid::new_v4(),
                time_created: "2025-09-26T05:00:00Z".parse().unwrap(),
                system_version: "4.1.0".parse().unwrap(),
            },
            TufRepoInfo {
                id: TufRepoUuid::new_v4(),
                time_created: "2025-09-26T06:00:00Z".parse().unwrap(),
                system_version: "4.2.0".parse().unwrap(),
            },
        ]
    }

    #[test]
    fn test_decide_prune() {
        let all_repos = make_test_repos();
        let [r1, r2, r3, r4, r5, r6] = &all_repos;
        let all_repos: IdOrdMap<_> = all_repos.clone().into_iter().collect();

        // Trivial case
        let mut status = TufRepoPrunerStatus {
            repos_keep: IdOrdMap::new(),
            repos_prune: IdOrdMap::new(),
            warnings: Vec::new(),
            nkeep_recent_releases: 2,
            nkeep_recent_uploads: 1,
        };
        decide_prune(&IdOrdMap::new(), &BTreeSet::new(), &mut status);
        assert!(status.warnings.is_empty());
        assert!(status.repos_keep.is_empty());
        assert!(status.repos_prune.is_empty());

        // Simple case: have some target releases to keep, no uploads.
        let releases_to_keep: BTreeSet<_> =
            [r3.id, r4.id].into_iter().collect();
        let mut status = TufRepoPrunerStatus {
            repos_keep: IdOrdMap::new(),
            repos_prune: IdOrdMap::new(),
            warnings: Vec::new(),
            nkeep_recent_releases: u8::try_from(releases_to_keep.len())
                .unwrap(),
            nkeep_recent_uploads: 0,
        };
        decide_prune(&all_repos, &releases_to_keep, &mut status);
        assert!(status.warnings.is_empty());
        assert_eq!(status.repos_keep.len(), 2);
        assert!(status.repos_keep.contains_key(&r3.id));
        assert!(status.repos_keep.contains_key(&r4.id));
        assert_eq!(
            status.repos_prune.len(),
            all_repos.len() - releases_to_keep.len()
        );
        assert!(status.repos_keep.contains_key(&r1.id));
        assert!(status.repos_keep.contains_key(&r2.id));
        assert!(status.repos_keep.contains_key(&r5.id));
        assert!(status.repos_keep.contains_key(&r6.id));

        // Simple case: have no target releases to keep, but some uploads.
        let releases_to_keep = BTreeSet::new();
        let mut status = TufRepoPrunerStatus {
            repos_keep: IdOrdMap::new(),
            repos_prune: IdOrdMap::new(),
            warnings: Vec::new(),
            nkeep_recent_releases: u8::try_from(releases_to_keep.len())
                .unwrap(),
            nkeep_recent_uploads: 0,
        };
        decide_prune(&all_repos, &releases_to_keep, &mut status);
        assert!(status.warnings.is_empty());
        assert_eq!(status.repos_keep.len(), 2);
        assert!(status.repos_keep.contains_key(&r3.id));
        assert!(status.repos_keep.contains_key(&r4.id));
        assert_eq!(
            status.repos_prune.len(),
            all_repos.len() - releases_to_keep.len()
        );
        assert!(status.repos_keep.contains_key(&r1.id));
        assert!(status.repos_keep.contains_key(&r2.id));
        assert!(status.repos_keep.contains_key(&r5.id));
        assert!(status.repos_keep.contains_key(&r6.id));

        // Case 1: a common case
        // XXX-dap
        // let releases_to_keep: BTreeSet<_> =
        //     [r3.id, r4.id].into_iter().collect();
        // let mut status = TufRepoPrunerStatus {
        //     repos_keep: IdOrdMap::new(),
        //     repos_prune: IdOrdMap::new(),
        //     warnings: Vec::new(),
        //     nkeep_recent_releases: u8::try_from(releases_to_keep.len())
        //         .unwrap(),
        //     nkeep_recent_uploads: 1,
        // };
    }
}
