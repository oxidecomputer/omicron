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
use nexus_types::internal_api::background::TufRepoInfo;
use nexus_types::internal_api::background::TufRepoPrunerStatus;
use omicron_uuid_kinds::TufRepoUuid;
use serde_json::json;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
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
    // Compute configuration.
    let nkeep_recent_releases = NKEEP_RECENT_TARGET_RELEASES_ALWAYS
        + config.nkeep_extra_target_releases;
    let nkeep_recent_uploads =
        NKEEP_RECENT_UPLOADS_ALWAYS + config.nkeep_extra_newly_uploaded;

    // Fetch the state we need to make a decision.
    let tuf_generation = datastore
        .tuf_get_generation(opctx)
        .await
        .context("fetching current TUF generation")?;
    let all_tuf_repos: IdOrdMap<_> = datastore
        .tuf_list_repos_unpruned_batched(opctx)
        .await
        .context("listing unpruned TUF repos")?
        .into_iter()
        .map(|repo| TufRepoInfo {
            id: repo.id(),
            system_version: repo.system_version.into(),
            time_created: repo.time_created,
        })
        .collect();
    let recent_releases = datastore
        .target_release_fetch_recent_distinct(opctx, nkeep_recent_releases)
        .await
        .context("listing recent target releases")?;

    // Finally, make the decision about what to prune.
    let mut status = decide_prune(TufRepoPrune {
        nkeep_recent_releases,
        nkeep_recent_uploads,
        all_tuf_repos: &all_tuf_repos,
        recent_releases: &recent_releases.releases,
    });

    // If we decided to prune something, do it.
    if let Some(to_prune) = &status.repo_prune {
        let prune_id = to_prune.id;
        if let Err(error) = datastore
            .tuf_repo_mark_pruned(
                opctx,
                tuf_generation,
                &recent_releases,
                prune_id,
            )
            .await
        {
            status.warnings.push(format!(
                "failed to prune {} (release {}): {}",
                prune_id,
                to_prune.system_version,
                InlineErrorChain::new(&error),
            ));
        }
    }

    Ok(status)
}

/// Arguments to `decide_prune()`
struct TufRepoPrune<'a> {
    /// how many of the most recent target releases to keep
    nkeep_recent_releases: u8,
    /// how many recent uploads (that aren't also recent relases) to keep
    nkeep_recent_uploads: u8,
    /// description of all unpruned TUF repos in the system
    all_tuf_repos: &'a IdOrdMap<TufRepoInfo>,
    /// set of recent target releases
    recent_releases: &'a BTreeSet<TufRepoUuid>,
}

/// Given the complete list of TUF repos and a set of recent releases that we
/// definitely want to keep, decide what to prune and what to keep.
fn decide_prune(args: TufRepoPrune) -> TufRepoPrunerStatus {
    let TufRepoPrune {
        nkeep_recent_releases,
        nkeep_recent_uploads,
        all_tuf_repos,
        recent_releases,
    } = args;

    let mut warnings = Vec::new();

    // Record that we're keeping all of the `releases_to_keep`.
    let mut repos_keep_target_release = IdOrdMap::new();
    for repo_id in recent_releases {
        match all_tuf_repos.get(repo_id) {
            Some(repo_info) => {
                repos_keep_target_release.insert_overwrite(repo_info.clone());
            }
            None => {
                // This is unusual: there's a recent target release with no
                // associated entry in the `tuf_repo` table.  This is
                // conceivable if the repo has just been uploaded and made the
                // target release, though still quite surprising since that
                // all would have had to happen between our two database queries
                // above.  Still, it's not a problem.  We'll note it here, but
                // otherwise move on.  (We will not wind up pruning this.)
                warnings.push(format!(
                    "wanting to keep recent target release repo {repo_id}, \
                     but did not find it in the tuf_repo table",
                ));
            }
        }
    }

    // Partition all TUF repos *other* than those into:
    // - ones that we're keeping because they were most recently uploaded
    // - the one we want to prune
    // - other ones we would have pruned, but won't because we only prune one
    let mut non_target_release_repos = all_tuf_repos
        .iter()
        .filter(|r| !repos_keep_target_release.contains_key(&r.id))
        .collect::<Vec<_>>();
    non_target_release_repos.sort_by_key(|k| k.time_created);
    non_target_release_repos.reverse();
    let mut repos_keep_recent_uploads = IdOrdMap::new();
    let mut repo_prune = None;
    let mut other_repos_eligible_to_prune = IdOrdMap::new();
    for repo in non_target_release_repos {
        if repos_keep_recent_uploads.len() < usize::from(nkeep_recent_uploads) {
            repos_keep_recent_uploads.insert_overwrite(repo.clone());
            continue;
        }

        if repo_prune.is_none() {
            repo_prune = Some(repo.clone());
            continue;
        }

        other_repos_eligible_to_prune.insert_overwrite(repo.clone());
    }

    TufRepoPrunerStatus {
        nkeep_recent_releases,
        nkeep_recent_uploads,
        repos_keep_target_release,
        repos_keep_recent_uploads,
        repo_prune,
        other_repos_eligible_to_prune,
        warnings,
    }
}

#[cfg(test)]
mod test {
    use super::decide_prune;
    use crate::app::background::tasks::tuf_repo_pruner::TufRepoPrune;
    use iddqd::IdOrdMap;
    use nexus_types::internal_api::background::TufRepoInfo;
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

        // Trivial case: nothing available to keep or prune.
        let empty_map = IdOrdMap::new();
        let empty_set = BTreeSet::new();
        let status = decide_prune(TufRepoPrune {
            nkeep_recent_releases: 2,
            nkeep_recent_uploads: 1,
            all_tuf_repos: &empty_map,
            recent_releases: &empty_set,
        });
        assert_eq!(status.nkeep_recent_releases, 2);
        assert_eq!(status.nkeep_recent_uploads, 1);
        assert!(status.warnings.is_empty());
        assert!(status.repos_keep_target_release.is_empty());
        assert!(status.repos_keep_recent_uploads.is_empty());
        assert!(status.repo_prune.is_none());
        assert!(status.other_repos_eligible_to_prune.is_empty());

        // Simple case: we're only allowing it to keep recent releases.
        let releases_to_keep: BTreeSet<_> =
            [r3.id, r4.id].into_iter().collect();
        let status = decide_prune(TufRepoPrune {
            nkeep_recent_releases: u8::try_from(releases_to_keep.len())
                .unwrap(),
            nkeep_recent_uploads: 0,
            all_tuf_repos: &all_repos,
            recent_releases: &releases_to_keep,
        });
        assert!(status.warnings.is_empty());
        assert_eq!(status.repos_keep_target_release.len(), 2);
        assert!(status.repos_keep_target_release.contains_key(&r3.id));
        assert!(status.repos_keep_target_release.contains_key(&r4.id));
        assert!(status.repos_keep_recent_uploads.is_empty());
        assert_eq!(status.repo_prune.expect("repo to prune").id, r1.id);
        assert_eq!(
            status.other_repos_eligible_to_prune.len(),
            all_repos.len() - releases_to_keep.len()
        );
        assert!(status.other_repos_eligible_to_prune.contains_key(&r2.id));
        assert!(status.other_repos_eligible_to_prune.contains_key(&r5.id));
        assert!(status.other_repos_eligible_to_prune.contains_key(&r6.id));

        // Simple case: we're only allowing it to keep recent uploads,
        // by virtue of having no recent target releases.
        let status = decide_prune(TufRepoPrune {
            nkeep_recent_releases: 3,
            nkeep_recent_uploads: 2,
            all_tuf_repos: &all_repos,
            recent_releases: &empty_set,
        });
        assert!(status.warnings.is_empty());
        assert!(status.repos_keep_target_release.is_empty());
        assert!(status.repos_keep_target_release.contains_key(&r5.id));
        assert!(status.repos_keep_target_release.contains_key(&r6.id));
        assert_eq!(status.repos_keep_recent_uploads.len(), 2);
        assert_eq!(status.repo_prune.expect("repo to prune").id, r1.id);
        assert_eq!(
            status.other_repos_eligible_to_prune.len(),
            all_repos.len() - 1 - status.repos_keep_recent_uploads.len(),
        );
        assert!(status.other_repos_eligible_to_prune.contains_key(&r2.id));
        assert!(status.other_repos_eligible_to_prune.contains_key(&r3.id));
        assert!(status.other_repos_eligible_to_prune.contains_key(&r4.id));

        // Keep a combination of recent target releases and recent uploads.
        let releases_to_keep: BTreeSet<_> =
            [r3.id, r4.id].into_iter().collect();
        let status = decide_prune(TufRepoPrune {
            nkeep_recent_releases: u8::try_from(releases_to_keep.len())
                .unwrap(),
            nkeep_recent_uploads: 1,
            all_tuf_repos: &all_repos,
            recent_releases: &releases_to_keep,
        });
        assert!(status.warnings.is_empty());
        assert_eq!(status.repos_keep_target_release.len(), 2);
        assert!(status.repos_keep_target_release.contains_key(&r3.id));
        assert!(status.repos_keep_target_release.contains_key(&r4.id));
        assert_eq!(status.repos_keep_recent_uploads.len(), 1);
        assert!(status.repos_keep_recent_uploads.contains_key(&r6.id));
        assert_eq!(status.repo_prune.expect("repo to prune").id, r1.id);
        assert_eq!(
            status.other_repos_eligible_to_prune.len(),
            all_repos.len()
                - releases_to_keep.len()
                - status.repos_keep_recent_uploads.len(),
        );
        assert!(status.other_repos_eligible_to_prune.contains_key(&r5.id));
    }
}
