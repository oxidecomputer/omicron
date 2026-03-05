// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for pruning old blueprints

use crate::app::background::BackgroundTask;
use anyhow::Context;
use futures::future::BoxFuture;
use nexus_auth::authz;
use nexus_db_model::BpTarget;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::internal_api::background::BlueprintPrunerDetails;
use nexus_types::internal_api::background::BlueprintPrunerStatus;
use nexus_types::internal_api::background::DeletedBlueprint;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::BlueprintUuid;
use serde_json::json;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::sync::Arc;
use omicron_uuid_kinds::GenericUuid;

/// Background task that prunes old blueprints from the database
pub struct BlueprintPruner {
    datastore: Arc<DataStore>,
}

impl BlueprintPruner {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }
}

impl BackgroundTask for BlueprintPruner {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move {
            match blueprint_prune(opctx, &self.datastore, &opctx.log).await {
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
                    "error": InlineErrorChain::new(&*error).to_string(),
                }),
            }
        })
    }
}

async fn blueprint_prune(
    opctx: &OpContext,
    datastore: &DataStore,
    log: &Logger,
) -> Result<BlueprintPrunerStatus, anyhow::Error> {
    // This task prunes rows from two closely related sets of tables:
    //
    // - `bp_target`, the list of historical target blueprints.  At any given
    //   time, only the last (highest-version) row in this table is meaningful.
    //   The rest remains so that we can see the history, but we need to prune
    //   it so it doesn't grow unbounded.
    //
    // - `blueprint` and the other `bp_*` tables that represent the contents of
    //   a single blueprint.  (We can mostly just think about the `blueprint`
    //   table.  When we delete a blueprint, we'll use
    //   `datastore.blueprint_delete()`, which will also delete its rows from
    //   these other tables.)
    //
    // The question is: which rows can we delete?  And how do we find them?  All
    // of the blueprints in the system fall into one of a few categories:
    //
    // - The current target blueprint.  At any given time, there's only one of
    //   these.
    //
    // - Old blueprints that were previous target blueprints.
    //
    //   This is by far the most common case: the system marches along creating
    //   new blueprints, making each new one the target, leaving the older ones
    //   around.  We can identify these because they will have corresponding
    //   entries in `bp_target` that are *not* the highest-versioned row in that
    //   table.  (You could also identify these because they are ancestors of
    //   the current target blueprint, but determining this requires having
    //   loaded the metadata for all the blueprints in the chain between the one
    //   you're checking and the current target.)
    //
    // - Old blueprints that were never made the target.
    //
    //   - One way this can happen is if the autoplanner (the `blueprint_plan`
    //     task) creates a blueprint, tries to make it the target, but that
    //     fails (usually because another Nexus's autoplanner beat it to the
    //     punch).  In this case, though, the autoplanner usually deletes the
    //     blueprint it created.  These would only stick around if the
    //     autoplanner crashed at the wrong time.
    //
    //   - These may have been created by a human who imported the blueprint or
    //     ran the planner explicitly, but then never made it the target.
    //
    //   Both of these cases should be uncommon enough that we can ignore them.
    //
    // - New blueprints that have never been made the target.
    //
    //   The only real difference between this category and the previous one is
    //   that these blueprints could still become the current target.  There are
    //   two cases here:
    //
    //   - The blueprint's parent blueprint is the current target.
    //
    //   - Some other ancestor of the parent blueprint is the current target.
    //     (This could happen if a person created a chain of blueprints based on
    //     the current target and imported them all, but hasn't set any of them
    //     to be the target.  They could subsequently go and set them each to
    //     the target, one after another.)
    //
    //   In an ideal world, we wouldn't delete any of these -- especially ones
    //   whose parent blueprint is the current target.  If we did, we might
    //   wind up fighting with the blueprint planner or a person that's in the
    //   process of setting the system's target blueprint.
    //
    // Having said that, a safe approach here is to only delete blueprints that
    // *have* been made the target (but are not currently the target).  These
    // cannot possibly be used again since they cannot be made the target again.
    // This will leave around blueprints that were never made the target, but as
    // we said above, there shouldn't ever be many of these.
    //
    // In fact, there's no need to prune everything that's not the current
    // target.  It's helpful for system debugging to keep around the last N,
    // where N corresponds to 1-2 upgrades' worth of blueprints.  That way,
    // developers and support can use `omdb` on the live system to debug
    // everything going back to the last upgrade.
    //
    // It's important to note that, aside from a transition period described
    // below, we're not losing this information forever when we delete it from
    // the database.  Nexus saves a Reconfigurator state file into the debug
    // dropbox whenever either the autoplanner generates a new blueprint that
    // becomes the target or when a human operator adds a blueprint and makes it
    // the target.  Thus, we should have a complete historical record of all
    // blueprints ever part of this system.  But we don't need this stuff in the
    // database itself (which, by comparison with unreplicated flat files, is an
    // expensive resource).
    //
    // Finally, note too that none of this needs to be transactional because the
    // approach is conservative.

    // Keep up to MAX_NKEEP blueprints in the database.
    // (That is: stop pruning once we reach this number.)
    const MAX_NKEEP: usize = 1000;

    // We won't delete more than this many per activation.  This gives us a
    // chance to report status periodically.  It does mean it'll take a while to
    // clean stuff up, but in general, we should be keeping up with what the
    // system is producing.
    const MAX_DELETE_ATTEMPTS_PER_ACTIVATION: usize = 50;

    // Figure out the maximum version that we'd consider pruning.
    let target_table_status =
        fetch_target_table_status(opctx, datastore, MAX_NKEEP).await?;
    let Some(first_version_to_keep) = target_table_status.first_version_to_keep
    else {
        return Ok(BlueprintPrunerStatus::Enabled(BlueprintPrunerDetails {
            deleted: vec![],
            nkept: target_table_status.nfound,
            warnings: vec![],
        }));
    };

    // Now, we can walk through the `bp_target` table, cleaning up rows and
    // their associated blueprints as we go.
    //
    // We don't use a typical paginator here because we're going to be deleting
    // rows as we go.  We'll always fetch the first page.
    let mut nattempts = 0;
    let mut deleted = vec![];
    let mut warnings = vec![];
    let mut highest_deleted = None;
    let candidates = datastore
        .bp_target_list_page(
            opctx,
            &DataPageParams {
                marker: None,
                direction: dropshot::PaginationOrder::Ascending,
                limit: SQL_BATCH_SIZE,
            },
        )
        .await
        .context("fetching page of bp_target rows for cleanup")?;
    for candidate in candidates {
        if *candidate.version >= first_version_to_keep {
            info!(
                log,
                "stopping after reaching first version to keep";
                "version" => first_version_to_keep,
            );
            break;
        }

        if nattempts > MAX_DELETE_ATTEMPTS_PER_ACTIVATION {
            info!(
                log,
                "stopping after hitting attempt limit";
                "nattempts" => nattempts
            );
            break;
        }

        let blueprint_id = BlueprintUuid::from(candidate.blueprint_id);
        nattempts += 1;
        debug!(
            log,
            "deleting old target blueprint";
            "version" => *candidate.version,
            "blueprint_id" => blueprint_id.to_string(),
        );
        let authz_blueprint = authz::Blueprint::new(
            authz::FLEET,
            blueprint_id.into_untyped_uuid(),
            LookupType::ById(blueprint_id.into_untyped_uuid()),
        );
        match datastore.blueprint_delete(opctx, &authz_blueprint).await {
            Ok(_) => {
                info!(
                    log,
                    "former target blueprint deleted";
                    "version" => *candidate.version,
                    "blueprint_id" => blueprint_id.to_string(),
                );
                if let Some(previous_highest) = highest_deleted
                    && previous_highest >= *candidate.version
                {
                    // This should be impossible, since we're doing this in
                    // increasing order of version.
                    warnings
                        .push(format!("highest version seen went backwards"));
                    break;
                }
                deleted.push(DeletedBlueprint {
                    id: blueprint_id,
                    time_made_target: candidate.time_made_target,
                });
                highest_deleted = Some(*candidate.version);
            }

            Err(Error::ObjectNotFound {
                type_name,
                lookup_type: LookupType::ById(id),
            }) if type_name == ResourceType::Blueprint
                && blueprint_id.into_untyped_uuid() == id =>
            {
                info!(
                    log,
                    "former target blueprint had already been deleted";
                    "version" => *candidate.version,
                    "blueprint_id" => blueprint_id.to_string(),
                );
                if let Some(previous_highest) = highest_deleted
                    && previous_highest >= *candidate.version
                {
                    // This should be impossible, since we're doing this in
                    // increasing order of version.
                    warnings
                        .push(format!("highest version seen went backwards"));
                    break;
                }
                highest_deleted = Some(*candidate.version);
            }

            Err(error) => {
                // For any other kind of error, stop.  We'll try again later
                // when we're activated again.
                let error = InlineErrorChain::new(&error);
                warn!(
                    log,
                    "failed to delete former target blueprint";
                    "version" => *candidate.version,
                    "blueprint_id" => blueprint_id.to_string(),
                    &error,
                );
                warnings.push(format!(
                    "failed to delete blueprint {blueprint_id}: {}",
                    error,
                ));
                break;
            }
        }
    }

    let targets_deleted = if let Some(highest_deleted) = highest_deleted {
        // At this point, we must have deleted all blueprints associated with
        // bp_target rows with version up to and including `highest_deleted`.
        //
        // That's because:
        // - we iterate the bp_target rows in increasing order of `version`,
        //   starting with the smallest
        // - we delete each one
        // - we stop (without having updated `highest_deleted`) if we reach one
        //   that we can't delete for whatever reason
        //
        // It's not a problem if this fails after having deleted the blueprints.
        // We'll try again the next time around.
        // XXX-dap debug log that we're doing this
        // XXX-dap can this function somehow predicate on this *NOT* being the
        // max version?
        match datastore
            .bp_target_delete_older(opctx, highest_deleted)
            .await
        {
            Ok(count) => count, // XXX-dap info log that we did it
            Err(error) => 0,    // XXX-dap technically we may have deleted some
                                 // XXX-dap report warning
                                 // XXX-dap log warning
        }
    } else {
        0
    };

    // XXX-dap update status to show how many bp_target rows we deleted
    Ok(BlueprintPrunerStatus::Enabled(BlueprintPrunerDetails {
        deleted,
        nkept: target_table_status.nfound,
        warnings,
    }))
}

struct TargetTableStatus {
    first_version_to_keep: Option<u32>,
    nfound: usize,
    nscanned: usize,
}

/// Figure out the maximum `version` in the `bp_target` table that we'd consider
/// pruning.
// There are lots of ways to do this.  We'll do it by paging backwards through
// the table until we identify MAX_NKEEP distinct blueprints.
//
// Alternative considered: take MAX(version) and subtract MAX_NKEEP.  This is
// easy to do, but might result in keeping fewer blueprints if some of those
// rows just involve having enabled or disabled the target.  That's not a big
// deal while MAX_NKEEP = 1000, given how uncommon it is to enable/disable the
// target.  But it could also be *very* wrong if for whatever reason somebody
// has already removed rows near the end of the table.  This too should be
// impossible, but there's no need to rely on it.
//
// Alternative considered: have the database tell us the version that's
// `MAX_NKEEP` rows from the end.  e.g., something like
//
//     SELECT version FROM bp_target ORDER BY version DESC OFFSET `MAX_NKEEP`
//
// That query is somewhat expensive (well, proportional to `MAX_NKEEP`) and
// still has the problem of not keeping enough blueprints if some of these rows
// correspond to just enabling/disabling the current target.
//
// By comparison, the approach we pick here is just a paginated scan (no exotic
// SQL), each query's cost is proportional to the page size (rather than
// `MAX_NKEEP`), and it allows us to keep the right number of distinct
// blueprints.
async fn fetch_target_table_status(
    opctx: &OpContext,
    datastore: &DataStore,
    nkeep: usize,
) -> Result<TargetTableStatus, anyhow::Error> {
    let mut nscanned = 0;
    let mut blueprint_ids_to_keep: BTreeSet<BlueprintUuid> = BTreeSet::new();
    let mut paginator =
        Paginator::new(SQL_BATCH_SIZE, dropshot::PaginationOrder::Descending);
    while let Some(p) = paginator.next() {
        let records_batch = datastore
            .bp_target_list_page(opctx, &p.current_pagparams())
            .await
            .context("fetching page of bp_target rows")?;
        paginator = p.found_batch(&records_batch, &|m: &BpTarget| m.version);
        for row in records_batch {
            nscanned += 1;
            blueprint_ids_to_keep.insert(row.blueprint_id.into());

            if blueprint_ids_to_keep.len() >= nkeep {
                return Ok(TargetTableStatus {
                    first_version_to_keep: Some(*row.version),
                    nfound: blueprint_ids_to_keep.len(),
                    nscanned,
                });
            }
        }
    }

    return Ok(TargetTableStatus {
        first_version_to_keep: None,
        nfound: blueprint_ids_to_keep.len(),
        nscanned,
    });
}
