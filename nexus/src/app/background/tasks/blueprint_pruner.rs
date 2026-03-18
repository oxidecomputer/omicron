// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for pruning old blueprints

//! This task prunes rows from two closely related sets of tables:
//!
//! - `bp_target`, the list of historical target blueprints.  At any given
//!   time, only the last (highest-version) row in this table is meaningful.
//!   The rest remains so that we can see the history, but we need to prune
//!   it so it doesn't grow unbounded.
//!
//! - `blueprint` and the other `bp_*` tables that represent the contents of
//!   a single blueprint.  (We can mostly just think about the `blueprint`
//!   table.  When we delete a blueprint, we'll use
//!   `datastore.blueprint_delete()`, which will also delete its rows from
//!   these other tables.)
//!
//! The question is: which rows can we delete?  And how do we find them?  All
//! of the blueprints in the system fall into one of a few categories:
//!
//! - The current target blueprint.  At any given time, there's only one of
//!   these.
//!
//! - Old blueprints that were previous target blueprints.
//!
//!   This is by far the most common case: the system marches along creating
//!   new blueprints, making each new one the target, leaving the older ones
//!   around.  We can identify these because they will have corresponding
//!   entries in `bp_target` that are *not* the highest-versioned row in that
//!   table.  (You could also identify these because they are ancestors of
//!   the current target blueprint, but determining this requires having
//!   loaded the metadata for all the blueprints in the chain between the one
//!   you're checking and the current target.)
//!
//! - Old blueprints that were never made the target.
//!
//!   - One way this can happen is if the autoplanner (the `blueprint_plan`
//!     task) creates a blueprint, tries to make it the target, but that
//!     fails (usually because another Nexus's autoplanner beat it to the
//!     punch).  In this case, though, the autoplanner usually deletes the
//!     blueprint it created.  These would only stick around if the
//!     autoplanner crashed at the wrong time.
//!
//!   - These may have been created by a human who imported the blueprint or
//!     ran the planner explicitly, but then never made it the target.
//!
//!   Both of these cases should be uncommon enough that we can ignore them.
//!
//! - New blueprints that have never been made the target.
//!
//!   The only real difference between this category and the previous one is
//!   that these blueprints could still become the current target.  There are
//!   two cases here:
//!
//!   - The blueprint's parent blueprint is the current target.
//!
//!   - Some other ancestor of the parent blueprint is the current target.
//!     (This could happen if a person created a chain of blueprints based on
//!     the current target and imported them all, but hasn't set any of them
//!     to be the target.  They could subsequently go and set them each to
//!     the target, one after another.)
//!
//!   In an ideal world, we wouldn't delete any of these -- especially ones
//!   whose parent blueprint is the current target.  If we did, we might
//!   wind up fighting with the blueprint planner or a person that's in the
//!   process of setting the system's target blueprint.
//!
//! Having said that, a safe approach here is to only delete blueprints that
//! *have* been made the target (but are not currently the target).  These
//! cannot possibly be used again since they cannot be made the target again.
//! This will leave around blueprints that were never made the target, but as
//! we said above, there shouldn't ever be many of these.
//!
//! In fact, there's no need to prune everything that's not the current
//! target.  It's helpful for system debugging to keep around the last N,
//! where N corresponds to 1-2 upgrades' worth of blueprints.  That way,
//! developers and support can use `omdb` on the live system to debug
//! everything going back to the last upgrade.
//!
//! It's important to note that, aside from a transition period described
//! below, we're not losing this information forever when we delete it from
//! the database.  Nexus saves a Reconfigurator state file into the debug
//! dropbox whenever either the autoplanner generates a new blueprint that
//! becomes the target or when a human operator adds a blueprint and makes it
//! the target.  Thus, we should have a complete historical record of all
//! blueprints ever part of this system.  But we don't need this stuff in the
//! database itself (which, by comparison with unreplicated flat files, is an
//! expensive resource).
//!
//! Finally, note too that none of this needs to be transactional because the
//! approach is conservative.

use crate::app::background::BackgroundTask;
use anyhow::Context;
use anyhow::anyhow;
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
use omicron_uuid_kinds::GenericUuid;
use serde_json::json;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::Arc;

/// Maximum number of recent target blueprints to keep in the database
///
/// (That is: stop pruning once there are this many blueprints left.)
const MAX_NKEEP: usize = 1000;

/// Max number of bp_target rows to consider deleting per activation of the task
///
/// Limiting this number gives us a chance to report status periodically.  It
/// does mean it'll take a while to clean stuff up, but in general, we should be
/// keeping up with what the system is producing.
const MAX_DELETE_ATTEMPTS_PER_ACTIVATION: usize = 50;

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
            match prune_blueprints(
                opctx,
                &self.datastore,
                &opctx.log,
                MAX_NKEEP,
                MAX_DELETE_ATTEMPTS_PER_ACTIVATION,
            )
            .await
            {
                Ok(details) => {
                    let status = BlueprintPrunerStatus::Enabled(details);
                    match serde_json::to_value(status) {
                        Ok(val) => val,
                        Err(err) => json!({
                            "error": format!(
                                "could not serialize task status: {}",
                                InlineErrorChain::new(&err)
                            ),
                        }),
                    }
                }
                Err(error) => json!({
                    "error": InlineErrorChain::new(&*error).to_string(),
                }),
            }
        })
    }
}

/// Figure out which blueprints can be pruned and prune them.
async fn prune_blueprints(
    opctx: &OpContext,
    datastore: &DataStore,
    log: &Logger,
    nkeep: usize,
    max_delete_attempts: usize,
) -> Result<BlueprintPrunerDetails, anyhow::Error> {
    // Clamp `nkeep` at 3 on the low end.
    let nkeep = nkeep.clamp(3, usize::MAX);
    // Figure out the maximum version that we'd consider pruning.
    let target_table_status =
        determine_bp_target_rows_to_keep(opctx, datastore, nkeep).await?;
    let keep_version = match target_table_status.keep {
        KeepWhat::All => {
            info!(
                log,
                "keeping all blueprints";
                "nfound" => target_table_status.nfound,
                "nscanned" => target_table_status.nscanned,
            );
            return Ok(BlueprintPrunerDetails {
                nkept_by_policy: target_table_status.nfound,
                deleted: vec![],
                ntargets_deleted: 0,
                ntargets_removable: 0,
                warnings: vec![],
            });
        }
        KeepWhat::StartingFromVersion(version) => {
            info!(
                log,
                "will prune blueprints up through version";
                "nfound" => target_table_status.nfound,
                "nscanned" => target_table_status.nscanned,
                "version" => version,
            );

            version
        }
    };

    // Prune both blueprints and `bp_target` rows up to `bp_target` version
    // `pargs.keep_version` in increasing order of `bp_target`.`version`.  Stops
    // on any error, when running into the specified version, or when hitting
    // MAX_DELETE_ATTEMPTS_PER_ACTIVATION.
    //
    // We won't return an error after this point because even if we run into
    // problems, we may have done some useful work, too.  So after this point,
    // we'll return a status struct that reflects both the work done and any
    // errors.
    let pargs = PruneArgs {
        opctx,
        datastore,
        log,
        keep_version,
        nblueprints_found: target_table_status.nfound,
        max_delete_attempts,
    };
    let mut pop = PruneTracker::new();
    let details = loop {
        match prune_batch(&pargs, pop).await {
            ControlFlow::Continue(newpop) => pop = newpop,
            ControlFlow::Break(newpop) => break newpop.into_details(&pargs),
        }
    };

    Ok(details)
}

/// Summarizes the most recent rows of the `bp_target` table
struct TargetTableStatus {
    /// how many rows we scanned (starting at the end of the table)
    nscanned: usize,
    /// how many distinct blueprint ids we found
    nfound: usize,
    /// which rows to keep
    keep: KeepWhat,
}

/// Describes which versions in `bp_target` to keep, based on how many were
/// requested to be kept and what we actually found in the table
enum KeepWhat {
    /// Keep everything because there aren't more than the requested number
    All,
    /// Keep only rows after (and including) the provided version
    StartingFromVersion(u32),
}

/// Look at the most recent rows from the `bp_target` table and determine which
/// version(s) can be deleted, assuming we want to keep `nkeep` distinct
/// blueprints.
async fn determine_bp_target_rows_to_keep(
    opctx: &OpContext,
    datastore: &DataStore,
    nkeep: usize,
) -> Result<TargetTableStatus, anyhow::Error> {
    // There are lots of ways to do this.  We'll do it by paging backwards
    // through the table until we identify MAX_NKEEP distinct blueprints.
    //
    // Alternative considered: take MAX(version) and subtract MAX_NKEEP.  This
    // is easy to do, but might result in keeping fewer blueprints if some of
    // those rows just involve having enabled or disabled the target.  That's
    // not a big deal while MAX_NKEEP = 1000, given how uncommon it is to
    // enable/disable the target.  But it could also be *very* wrong if for
    // whatever reason somebody has already removed rows near the end of the
    // table.  This too should be impossible, but there's no need to rely on it.
    //
    // Alternative considered: have the database tell us the version that's
    // `MAX_NKEEP` rows from the end.  e.g., something like
    //
    //     SELECT version FROM bp_target ORDER BY version DESC OFFSET `MAX_NKEEP`
    //
    // That query is somewhat expensive (well, proportional to `MAX_NKEEP`) and
    // still has the problem of not keeping enough blueprints if some of these
    // rows correspond to just enabling/disabling the current target.
    //
    // By comparison, the approach we pick here is just a paginated scan (no
    // exotic SQL), each query's cost is proportional to the page size (rather
    // than `MAX_NKEEP`), and it allows us to keep the right number of distinct
    // blueprints.

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
                    nscanned,
                    nfound: blueprint_ids_to_keep.len(),
                    keep: KeepWhat::StartingFromVersion(*row.version),
                });
            }
        }
    }

    return Ok(TargetTableStatus {
        nscanned,
        nfound: blueprint_ids_to_keep.len(),
        keep: KeepWhat::All,
    });
}

/// Combines a bunch of state used by a bunch of the helpers below
struct PruneArgs<'a> {
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    log: &'a Logger,
    keep_version: u32,
    max_delete_attempts: usize,
    nblueprints_found: usize,
}

/// Tracks progress and errors for the overall prune activation
struct PruneTracker {
    ntargets_removable: usize,
    ntargets_deleted: usize,
    deleted: Vec<DeletedBlueprint>,
    errors: Vec<anyhow::Error>,
}

impl PruneTracker {
    pub fn new() -> PruneTracker {
        PruneTracker {
            ntargets_removable: 0,
            ntargets_deleted: 0,
            deleted: vec![],
            errors: vec![],
        }
    }

    pub fn record_error(mut self, error: anyhow::Error) -> Self {
        self.errors.push(error);
        self
    }

    pub fn record_batch(
        self,
        stop_reason: BatchStopReason,
    ) -> ControlFlow<Self, Self> {
        if !self.errors.is_empty() {
            return ControlFlow::Break(self);
        }

        match stop_reason {
            BatchStopReason::EndOfBatch => ControlFlow::Continue(self),
            BatchStopReason::OutOfPruneable
            | BatchStopReason::DeleteLimit
            | BatchStopReason::Error => ControlFlow::Break(self),
        }
    }

    pub fn record_targets_deleted(mut self, count: usize) -> Self {
        self.ntargets_deleted += count;
        self
    }

    fn into_details(self, pargs: &PruneArgs) -> BlueprintPrunerDetails {
        BlueprintPrunerDetails {
            nkept_by_policy: pargs.nblueprints_found,
            deleted: self.deleted,
            ntargets_removable: self.ntargets_removable,
            ntargets_deleted: self.ntargets_deleted,
            warnings: self
                .errors
                .into_iter()
                .map(|e| InlineErrorChain::new(&*e).to_string())
                .collect(),
        }
    }
}

/// Query for the oldest `bp_target` rows and prune both blueprints and
/// `bp_target` rows up through (and not including) version `keep_version`.
/// Stop on any error or when `pargs.max_delete_attempts` deletes have been
/// attempted.
///
/// This keeps `pop` updated with work done and errors encountered.
///
/// Returns a `ControlFlow` indicating whether the caller should proceed with
/// another batch.
async fn prune_batch(
    pargs: &PruneArgs<'_>,
    pop: PruneTracker,
) -> ControlFlow<PruneTracker, PruneTracker> {
    let PruneArgs { opctx, datastore, log, .. } = pargs;

    // Fetch a page worth of the oldest `bp_target` rows
    let firstpageparams = DataPageParams {
        marker: None,
        direction: dropshot::PaginationOrder::Ascending,
        limit: SQL_BATCH_SIZE,
    };
    let candidates = match datastore
        .bp_target_list_page(opctx, &firstpageparams)
        .await
        .context("fetching page of bp_target rows for cleanup")
    {
        Ok(candidates) => candidates,
        Err(error) => {
            return ControlFlow::Break(pop.record_error(error));
        }
    };

    // Prune as many blueprints as we can from that page.  If we deleted no
    // blueprints, then there's nothing else to do here.
    let batch_result = prune_batch_blueprints(pargs, pop, candidates).await;
    let mut pop = batch_result.pop;
    if let Some(deleted_up_to) = batch_result.highest_version_deleted {
        // At this point, we know that the blueprints associated with
        // `bp_target` rows up to version `deleted_up_to` have been deleted.  We
        // can now delete these rows.
        //
        // It's okay to do this even if `prune_batch_blueprints` ran into an
        // error because its contract is that there were no errors up through
        // `highest_deleted`.
        let result = datastore
            .bp_target_delete_up_to(opctx, deleted_up_to)
            .await
            .with_context(|| {
                format!("deleting bp_target rows up to {deleted_up_to}")
            });
        match result {
            Ok(count) => {
                info!(
                    log,
                    "deleted oldest bp_target rows";
                    "count" => count,
                    "max_version_deleted" => deleted_up_to,
                );

                pop = pop.record_targets_deleted(count);
            }
            Err(error) => {
                warn!(
                    log,
                    "failed to delete oldest bp_target rows";
                    InlineErrorChain::new(&*error),
                );

                return ControlFlow::Break(pop.record_error(error));
            }
        }
    }

    pop.record_batch(batch_result.stop_reason)
}

/// what happened after we finished trying to prune a batch of `bp_target` rows
#[derive(Debug, Clone, Copy)]
enum BatchStopReason {
    /// we ran out of rows in the batch (i.e., we can keep going with another
    /// batch)
    EndOfBatch,
    /// we ran into an unpruneable row (i.e., there's nothing left to prune)
    OutOfPruneable,
    /// we deleted as many as we're willing to in this activation
    DeleteLimit,
    /// we ran into an error
    Error,
}

/// Tracks the state of pruning one batch of blueprints from `bp_target`
///
/// This object is generally immutable because the methods here consume `self`
/// and return:
///
/// - another `BatchTracker`, when the method is infallible
///
/// - a `BatchResult` summarizing the final state, when the method you've called
///   is the last one involved in processing a batch
///
/// - a `ControlFlow`, when the method decides whether the caller should proceed
///   or stop (usually based on whether an error happened).  Generally the
///   `Continue` variant will provide another `BatchTracker` and the `Break`
///   variant will provide a `BatchResult`.
///
/// We use `ControlFlow` rather than `Result` because it's confusing to think of
/// these as reflecting success or failure.  In general, the batch processing
/// operation can successfully do work (that we want to report) *and* produce an
/// error.  So in the end, we'll wind up reporting both.  Hence: it's really a
/// question of whether we proceed (`ControlFlow::Continue`) or stop
/// (`ControlFlow::Break`).
struct BatchTracker {
    /// used to track activity like deletes, errors, etc.
    pop: PruneTracker,
    /// tracks the highest bp_target version deleted from this batch
    highest_version_deleted: Option<u32>,
}

impl BatchTracker {
    pub fn new(pop: PruneTracker) -> Self {
        BatchTracker { pop, highest_version_deleted: None }
    }

    /// Record that the given bp_target row's blueprint was deleted.  Returns:
    ///
    /// - `ControlFlow::Break(result)` if this was not the next `bp_target` row
    ///   to delete.  This is generally a bug.  But we handle it gracefully to
    ///   avoid crashing all of Nexus just for this problem.
    /// - `ControlFlow::Continue(self)` otherwise
    pub fn record_blueprint_deleted(
        mut self,
        target: BpTarget,
    ) -> ControlFlow<BatchResult, Self> {
        // Record the deleted blueprint before checking the version because it
        // was, in fact, deleted, whether that was correct or not.
        let deleted = DeletedBlueprint {
            id: BlueprintUuid::from(target.blueprint_id),
            time_made_target: target.time_made_target,
        };
        self.pop.deleted.push(deleted);
        let mut rv = self.new_highest_version(*target.version)?;
        rv.pop.ntargets_removable += 1;
        ControlFlow::Continue(rv)
    }

    /// Record that the given `bp_target` row's blueprint had already been
    /// deleted by the time we went to delete it.  Returns the same values as
    /// `record_blueprint_deleted`, for the same reasons.
    pub fn record_blueprint_already_deleted(
        self,
        target: BpTarget,
    ) -> ControlFlow<BatchResult, Self> {
        let mut rv = self.new_highest_version(*target.version)?;
        rv.pop.ntargets_removable += 1;
        ControlFlow::Continue(rv)
    }

    /// Internally, record that we've got a new highest-bp_target-version seen.
    /// This produces an error if the value appears to go backwards.  This would
    /// be a bug.
    fn new_highest_version(mut self, v: u32) -> ControlFlow<BatchResult, Self> {
        if let Some(old) = self.highest_version_deleted {
            if old >= v {
                return ControlFlow::Break(self.record_error(anyhow!(
                    "internal error: recorded blueprint version {v} \
                     after having previously seen version {old}",
                )));
            }
        }

        self.highest_version_deleted = Some(v);
        ControlFlow::Continue(self)
    }

    /// Record an error.  The error winds up in the top-level `PruneTracker`.
    ///
    /// This finishes processing of the batch, returning a `BatchPruneResult`
    /// summarizing the work done.
    pub fn record_error(mut self, error: anyhow::Error) -> BatchResult {
        self.pop = self.pop.record_error(error);
        self.finish_value(BatchStopReason::Error)
    }

    fn finish_value(self, stop_reason: BatchStopReason) -> BatchResult {
        BatchResult {
            stop_reason,
            pop: self.pop,
            highest_version_deleted: self.highest_version_deleted,
        }
    }

    /// Finish processing the batch of `bp_target` rows due to the given
    /// `stop_reason`.
    ///
    /// Returns a `ControlFlow` telling the caller that's processing this batch
    /// whether they should proceed with processing another batch.
    pub fn finish(
        self,
        stop_reason: BatchStopReason,
    ) -> ControlFlow<BatchResult, BatchResult> {
        let value = self.finish_value(stop_reason);
        match stop_reason {
            BatchStopReason::EndOfBatch => ControlFlow::Continue(value),
            BatchStopReason::OutOfPruneable
            | BatchStopReason::DeleteLimit
            | BatchStopReason::Error => ControlFlow::Break(value),
        }
    }
}

/// Summarizes the result of pruning one batch of `bp_target` rows
pub struct BatchResult {
    /// tracks overall operation state (actions taken, errors, etc.)
    pop: PruneTracker,

    /// why processing of this batch terminated
    stop_reason: BatchStopReason,

    /// All `bp_target` rows with this version or less have their corresponding
    /// blueprint pruned
    highest_version_deleted: Option<u32>,
}

/// Prunes as many blueprints as possible referenced by the given `bp_target`
/// rows until either:
///
/// - we find a row with version at least `pargs.keep_version`
/// - we reach `pargs.max_delete_attempts` total rows deleted
/// - we run into an error
///
/// Activity (blueprints deleted, rows processed, and errors) are recorded into
/// `pop`.  See `PruneTracker`.
///
/// The result is summarized in a `BatchResult`.  This operation can both do
/// useful work (that needs to be reported and acted upon) and produce an error
/// so it always returns the same `BatchResult`.
async fn prune_batch_blueprints(
    pargs: &PruneArgs<'_>,
    pop: PruneTracker,
    bp_target_rows: Vec<BpTarget>,
) -> BatchResult {
    // The caller doesn't care about the different `ControlFlow` variants here.
    // This type is only used here to make the helper function cleaner and less
    // brittle.
    prune_batch_blueprints_impl(pargs, pop, bp_target_rows).await.into_inner()
}

// This helper implements the body of `prune_batch_blueprints`.  Returning
// `ControlFlow` makes it cleaner (and less error-prone) to return early in all
// the cases that it should.
async fn prune_batch_blueprints_impl(
    pargs: &PruneArgs<'_>,
    pop: PruneTracker,
    bp_target_rows: Vec<BpTarget>,
) -> ControlFlow<BatchResult, BatchResult> {
    let PruneArgs { opctx, datastore, log, .. } = pargs;
    let mut batch = BatchTracker::new(pop);
    for row in bp_target_rows {
        if *row.version >= pargs.keep_version {
            return batch.finish(BatchStopReason::OutOfPruneable);
        }

        if batch.pop.ntargets_removable >= pargs.max_delete_attempts {
            return batch.finish(BatchStopReason::DeleteLimit);
        }

        let blueprint_id = BlueprintUuid::from(row.blueprint_id);
        let authz_blueprint = authz::Blueprint::new_for_id(blueprint_id);
        debug!(
            log,
            "deleting old target blueprint";
            "version" => *row.version,
            "blueprint_id" => blueprint_id.to_string(),
        );

        match datastore.blueprint_delete(opctx, &authz_blueprint).await {
            Ok(()) => {
                info!(
                    log,
                    "former target blueprint deleted";
                    "version" => *row.version,
                    "blueprint_id" => blueprint_id.to_string(),
                );
                batch = batch.record_blueprint_deleted(row)?;
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
                    "version" => *row.version,
                    "blueprint_id" => blueprint_id.to_string(),
                );
                batch = batch.record_blueprint_already_deleted(row)?;
            }

            Err(error) => {
                // For any other kind of error, stop.  We'll try again later
                // when we're activated again.
                warn!(
                    log,
                    "failed to delete former target blueprint";
                    "version" => *row.version,
                    "blueprint_id" => blueprint_id.to_string(),
                    InlineErrorChain::new(&error),
                );
                let error = anyhow!(error).context(format!(
                    "failed to delete former target blueprint {blueprint_id} \
                     (version {})",
                    *row.version
                ));
                return ControlFlow::Break(batch.record_error(error));
            }
        }
    }

    batch.finish(BatchStopReason::EndOfBatch)
}

// This is a non-nightly implementation of Rust's `ControlFlow::into_value()`.
trait ControlFlowExt {
    type Inner;
    fn into_inner(self) -> Self::Inner;
}
impl<T> ControlFlowExt for ControlFlow<T, T> {
    type Inner = T;
    fn into_inner(self) -> Self::Inner {
        match self {
            ControlFlow::Continue(val) => val,
            ControlFlow::Break(val) => val,
        }
    }
}

#[cfg(test)]
mod test {
    use super::prune_blueprints;
    use chrono::Utc;
    use nexus_auth::authz;
    use nexus_auth::context::OpContext;
    use nexus_db_model::BpTarget;
    use nexus_db_queries::db::DataStore;
    use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
    use nexus_db_queries::db::pagination::Paginator;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_reconfigurator_planning::planner::PlannerRng;
    use nexus_test_utils::db::TestDatabase;
    use nexus_types::deployment::BlueprintMetadata;
    use nexus_types::deployment::BlueprintSource;
    use nexus_types::deployment::BlueprintTarget;
    use nexus_types::internal_api::background::BlueprintPrunerDetails;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::BlueprintUuid;
    use omicron_uuid_kinds::GenericUuid;
    use std::collections::BTreeSet;
    use std::collections::VecDeque;

    /// Describes the `bp_target` rows and blueprints stored in the database
    #[derive(Eq, PartialEq, Debug)]
    struct BlueprintDatabaseState {
        all_blueprint_ids: BTreeSet<BlueprintUuid>,
        target_rows: VecDeque<TargetRow>,
    }

    #[derive(Eq, PartialEq, Debug)]
    struct TargetRow {
        id: BlueprintUuid,
        version: u32,
        enabled: bool,
    }

    impl BlueprintDatabaseState {
        pub async fn load(
            opctx: &OpContext,
            datastore: &DataStore,
        ) -> BlueprintDatabaseState {
            // We load:
            //
            // - all bp_target rows
            // - all blueprints that exist, whether or not they're referenced in
            //   `bp_target`
            //
            // This gives us ground truth about which blueprints are in the
            // database and which bp_target rows are there.  Normally we can
            // make certain assumptions (like if a `bp_target` row is deleted,
            // then its blueprint is also gone, or its blueprint is at least
            // irrelevant), but this is the very behavior we're testing here so
            // we avoid assuming stuff like that.
            //
            // Note that we're not testing blueprint deletion itself here.  We
            // assume that a blueprint is either fully present (if its row
            // exists in `blueprint`) or it's fully deleted (if not).  Other
            // code in the datastore tests that deletion is atomic across the
            // various blueprint-related tables.
            let mut all_blueprint_ids = BTreeSet::new();
            let mut paginator = Paginator::new(
                SQL_BATCH_SIZE,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let records_batch = datastore
                    .blueprints_list(opctx, &p.current_pagparams())
                    .await
                    .expect("fetching page of blueprint table");
                paginator = p
                    .found_batch(&records_batch, &|m: &BlueprintMetadata| {
                        m.id.into_untyped_uuid()
                    });
                for row in records_batch {
                    all_blueprint_ids.insert(row.id);
                }
            }

            let mut target_rows = VecDeque::new();
            let mut paginator = Paginator::new(
                SQL_BATCH_SIZE,
                dropshot::PaginationOrder::Ascending,
            );

            while let Some(p) = paginator.next() {
                let records_batch = datastore
                    .bp_target_list_page(opctx, &p.current_pagparams())
                    .await
                    .expect("fetching page of bp_target rows");
                paginator =
                    p.found_batch(&records_batch, &|m: &BpTarget| m.version);
                for row in records_batch {
                    target_rows.push_back(TargetRow {
                        id: BlueprintUuid::from(row.blueprint_id),
                        version: *row.version,
                        enabled: row.enabled,
                    });
                }
            }

            BlueprintDatabaseState { all_blueprint_ids, target_rows }
        }

        /// Verifies that all blueprints referenced by bp_target rows do exist.
        ///
        /// Note that this is not always expected to be true.  Systems that have
        /// previously cleaned up old blueprints may have bp_target rows that
        /// refer to blueprints that have been deleted.
        pub fn verify_blueprints_referenced_by_targets(&self) {
            for t in &self.target_rows {
                let id = t.id;
                if !self.all_blueprint_ids.contains(&id) {
                    panic!(
                        "expected blueprint id {id} from `bp_target` row to be \
                         present in `blueprint`, but it wasn't"
                    );
                }
            }
        }

        /// Verifies that all blueprints that exist are referenced by a
        /// `bp_target` row.
        ///
        /// Note that this is not always expected to be true.  Blueprints can be
        /// created and never made the target.
        pub fn verify_targets_referenced_by_blueprints(&self) {
            let mut unreferenced = self.all_blueprint_ids.clone();
            for row in &self.target_rows {
                // It's possible for a blueprint to be referenced multiple times
                // in `bp_target` (if it's been enabled/disabled), so it's okay
                // if `remove()` doesn't find the id.
                unreferenced.remove(&row.id);
            }

            if !unreferenced.is_empty() {
                panic!(
                    "blueprints found in `blueprint` that are not referenced \
                     by `bp_target`: {:?}",
                    unreferenced,
                );
            }
        }

        /// Re-loads the database state and verifies that it exactly matches
        /// the state reflected by this object.
        pub async fn verify_database_matches(
            &self,
            opctx: &OpContext,
            datastore: &DataStore,
        ) {
            let other = Self::load(opctx, datastore).await;
            assert_eq!(*self, other);
        }

        /// Creates a new blueprint based on the current target, inserts it, and
        /// sets it as the new target.
        pub async fn add_target_blueprint(
            &mut self,
            opctx: &OpContext,
            datastore: &DataStore,
        ) {
            // Find the highest-versioned target blueprint that we loaded and
            // read the whole blueprint.
            let parent_blueprint_target = self
                .target_rows
                .iter()
                .last()
                .expect("at least one bp_target row");
            let enabled = parent_blueprint_target.enabled;
            let parent_blueprint = datastore
                .blueprint_read(
                    opctx,
                    &authz::Blueprint::new_for_id(parent_blueprint_target.id),
                )
                .await
                .expect("loaded blueprint");

            // Build a new blueprint with no meaningful changes from that one.
            // Insert it and make it the new target.
            let builder = BlueprintBuilder::new_based_on(
                &opctx.log,
                &parent_blueprint,
                "test-suite",
                // It would be nice to use a seeded RNG for the tests, but the
                // builder takes ownership of the RNG (and for good reason --
                // its state changes as it's used).  It's technically cloneable,
                // but that doesn't have the semantics we want (we don't want
                // clones to generate the same random sequences).  Really what
                // we need is an interface through which the builder has a
                // mutable borrow of the RNG or else returns it back to us when
                // it's done.
                PlannerRng::from_entropy(),
            )
            .expect("creating BlueprintBuilder");

            let new_blueprint = builder.build(BlueprintSource::Test);
            datastore
                .blueprint_insert(opctx, &new_blueprint)
                .await
                .expect("inserting new blueprint");
            datastore
                .blueprint_target_set_current(
                    opctx,
                    BlueprintTarget {
                        target_id: new_blueprint.id,
                        enabled,
                        time_made_target: Utc::now(),
                    },
                )
                .await
                .expect("setting new blueprint as target");

            // Update our state to reflect the new blueprint.
            self.all_blueprint_ids.insert(new_blueprint.id);
            self.target_rows.push_back(TargetRow {
                id: new_blueprint.id,
                version: parent_blueprint_target.version + 1,
                enabled,
            });
        }

        /// Removes the oldest N target rows and their associated blueprints
        /// from just the in-memory state.
        // XXX-dap is it a problem that we assume we can delete a blueprint if
        // we find a bp_target row referencing it because what if a different,
        // *later* row references it?  I guess we can say that such rows must be
        // contiguous, at least.
        pub fn drop_oldest(&mut self, count: usize) -> Vec<TargetRow> {
            assert!(
                count < self.target_rows.len(),
                "test attempted to drop more target rows than there are"
            );
            let rv: Vec<_> = self.target_rows.drain(0..count).collect();
            for t in &rv {
                // It's possible that this blueprint id isn't present in
                // `all_blueprint_ids`, as in the case where manual cleanup had
                // been run on this system (or, more accurately here, when we're
                // testing that scenario).
                self.all_blueprint_ids.remove(&t.id);
            }
            rv
        }
    }

    /// Inserts an initial blueprint into the database
    async fn add_initial_blueprint(opctx: &OpContext, datastore: &DataStore) {
        // Verify the initial state set up by the test suite.
        let blueprints = BlueprintDatabaseState::load(opctx, datastore).await;
        assert!(blueprints.all_blueprint_ids.is_empty());
        assert!(blueprints.target_rows.is_empty());

        // Insert an initial blueprint.
        let blueprint = BlueprintBuilder::build_empty_seeded(
            "test-suite",
            PlannerRng::from_entropy(),
        );
        datastore
            .blueprint_insert(opctx, &blueprint)
            .await
            .expect("inserting initial blueprint");
        datastore
            .blueprint_target_set_current(
                opctx,
                BlueprintTarget {
                    target_id: blueprint.id,
                    enabled: false,
                    time_made_target: Utc::now(),
                },
            )
            .await
            .expect("inserting initial blueprint target");
    }

    /// Runs and verifies a `prune` operation where:
    ///
    /// - input: keep `nkeep` blueprints
    /// - input: at most `nmax` delete attempts
    ///
    /// This verifies:
    ///
    /// - that no errors were encountered
    /// - that we deleted the same number of blueprints as `bp_target` rows
    /// - that these were the _right_ bp_target rows / blueprints (the oldest)
    /// - that no other `bp_target` rows were deleted
    /// - that the reported stats (ntargets_removable, ntargets_deleted) are
    ///   correct (note that `nkept_by_policy` is up to the caller to check, as
    ///   is the actual number of deleted blueprints)
    async fn verify_prune(
        opctx: &OpContext,
        datastore: &DataStore,
        blueprints: &mut BlueprintDatabaseState,
        nkeep: usize,
        nmax: usize,
    ) -> BlueprintPrunerDetails {
        let details =
            prune_blueprints(opctx, datastore, &opctx.log, nkeep, nmax)
                .await
                .expect("successful prune");
        println!("{details:?}");
        // Verify no problems encountered.
        assert!(details.warnings.is_empty());
        // Verify that whatever blueprints we deleted were the oldest ones.
        let deleted = if !details.deleted.is_empty() {
            let oldest = blueprints.drop_oldest(details.deleted.len());
            for (old, pruned) in oldest.iter().zip(details.deleted.iter()) {
                assert_eq!(old.id, pruned.id);
            }
            oldest
        } else {
            vec![]
        };

        // Verify that we reported the right number of blueprints gone.
        assert_eq!(details.ntargets_removable, deleted.len());
        // Verify that we actually deleted the right number of bp_target rows.
        assert_eq!(details.ntargets_deleted, deleted.len());
        // Verify that the runtime state we've been using and verifying matches
        // the underlying database state.
        blueprints.verify_database_matches(opctx, datastore).await;

        details
    }

    /// Tests basic behavior of pruner:
    ///
    /// - pruning deletes the right blueprints and bp_target rows
    /// - per-activation delete cap is honored
    /// - minimum "nkept" is honored
    /// - does nothing when there's nothing to prune
    #[tokio::test]
    async fn test_basic() {
        let logctx = dev::test_setup_log("blueprint_pruner_basic");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Load the initial state.
        add_initial_blueprint(opctx, datastore).await;

        // Verify that newly loaded state reflects one blueprint.
        let mut blueprints =
            BlueprintDatabaseState::load(opctx, datastore).await;
        assert_eq!(blueprints.all_blueprint_ids.len(), 1);
        assert_eq!(blueprints.target_rows.len(), 1);
        blueprints.verify_blueprints_referenced_by_targets();
        blueprints.verify_targets_referenced_by_blueprints();

        // Trivial case: pruning at this point should do nothing, even if we
        // choose to keep 0.  That's because we'll never remove the last few
        // blueprints.
        let details =
            verify_prune(opctx, datastore, &mut blueprints, 1, 100).await;
        assert_eq!(details.nkept_by_policy, 1);
        assert_eq!(details.deleted.len(), 0);

        // Add several more blueprints and make sure our representation of the
        // database state matches up with the real thing.
        for _ in 0..22 {
            blueprints.add_target_blueprint(opctx, datastore).await;
        }
        blueprints.verify_blueprints_referenced_by_targets();
        blueprints.verify_targets_referenced_by_blueprints();
        blueprints.verify_database_matches(opctx, datastore).await;

        // At this point, we have 23 blueprints.
        //
        // Basic case: prune down to some minimum number.  Specifically:
        // `prune(nkeep = 13, max_deletes = 100)`.  This should remove 10
        // blueprints, leaving 13.
        let details =
            verify_prune(opctx, datastore, &mut blueprints, 13, 100).await;
        assert_eq!(details.nkept_by_policy, 13);
        assert_eq!(details.deleted.len(), 10);

        // Now, exercise pruning with a large backlog -- i.e., where the number
        // of blueprints to be pruned exceeds the maximum number we're willing
        // to prune in each activation and we have to run the operation multiple
        // times.
        //
        // Start with `prune(nkeep = 3, max_deletes = 4).  This should remove 4
        // blueprints and leave 9.
        let details =
            verify_prune(opctx, datastore, &mut blueprints, 3, 4).await;
        assert_eq!(details.nkept_by_policy, 3);
        assert_eq!(details.deleted.len(), 4);

        // Continue pruning the backlog.  Do the same thing again, removing
        // another 4 blueprints and leaving 5.  This shows that when we stop due
        // to running into the max, the next attempt will prune more.
        //
        // In this example, we'll also set `nkeep = 0` to test the clamping
        // behavior.  This will be implicitly clamped (below) at 3.
        let details =
            verify_prune(opctx, datastore, &mut blueprints, 0, 4).await;
        assert_eq!(details.nkept_by_policy, 3);
        assert_eq!(details.deleted.len(), 4);

        // Finish pruning the backlog.  Only two blueprints will be pruned,
        // leaving 3.  This tests what happens when we finally do run into the
        // limit.
        let details =
            verify_prune(opctx, datastore, &mut blueprints, 3, 4).await;
        assert_eq!(details.nkept_by_policy, 3);
        assert_eq!(details.deleted.len(), 2);

        // Prune one more time.  This shouldn't do anything since we're still at
        // the limit.
        let details =
            verify_prune(opctx, datastore, &mut blueprints, 3, 4).await;
        assert_eq!(details.nkept_by_policy, 3);
        assert_eq!(details.deleted.len(), 0);

        // Now exercise a typical steady-state: if we create another blueprint,
        // then prune, then we'll wind up pruning the oldest one.
        blueprints.add_target_blueprint(opctx, datastore).await;
        let details =
            verify_prune(opctx, datastore, &mut blueprints, 3, 4).await;
        assert_eq!(details.nkept_by_policy, 3);
        assert_eq!(details.deleted.len(), 1);

        // Sometimes, the pruner may get ahead of generation and we'll have
        // nothing to prune.
        let details =
            verify_prune(opctx, datastore, &mut blueprints, 3, 4).await;
        assert_eq!(details.nkept_by_policy, 3);
        assert_eq!(details.deleted.len(), 0);

        // Other times, the planner may get ahead and we'll have multiple to
        // prune.
        blueprints.add_target_blueprint(opctx, datastore).await;
        blueprints.add_target_blueprint(opctx, datastore).await;
        let details =
            verify_prune(opctx, datastore, &mut blueprints, 3, 4).await;
        assert_eq!(details.nkept_by_policy, 3);
        assert_eq!(details.deleted.len(), 2);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Tests pruning when some of the entries in `bp_target` have already had
    /// their blueprints removed
    ///
    /// This will commonly be the case on deployed systems because of manual
    /// cleanup that has been run in the past with `omdb reconfigurator
    /// archive`.
    #[tokio::test]
    async fn test_prune_missing_blueprint() {
        let logctx = dev::test_setup_log("blueprint_pruner_missing_blueprint");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Load the initial state.
        add_initial_blueprint(opctx, datastore).await;

        // Add several more blueprints and make sure our representation of the
        // database state matches up with the real thing.
        let mut blueprints =
            BlueprintDatabaseState::load(opctx, datastore).await;
        for _ in 0..20 {
            blueprints.add_target_blueprint(opctx, datastore).await;
        }
        blueprints.verify_blueprints_referenced_by_targets();
        blueprints.verify_targets_referenced_by_blueprints();
        blueprints.verify_database_matches(opctx, datastore).await;
        let nblueprints = blueprints.all_blueprint_ids.len();
        assert_eq!(
            blueprints.all_blueprint_ids.len(),
            blueprints.target_rows.len()
        );

        // Delete the first few blueprints from the database.
        let ndeleted = 4;
        for i in 0..ndeleted {
            datastore
                .blueprint_delete(
                    opctx,
                    &authz::Blueprint::new_for_id(blueprints.target_rows[i].id),
                )
                .await
                .expect("deleting blueprint");
        }

        // Reload the database state.
        let blueprints =
            BlueprintDatabaseState::load(opctx, datastore).await;
        // Verify that there are now fewer blueprints in the database than
        // target rows.
        assert_eq!(blueprints.all_blueprint_ids.len(), nblueprints - ndeleted);
        assert_eq!(blueprints.target_rows.len(), nblueprints);
        blueprints.verify_database_matches(opctx, datastore).await;

        // Now, prune.
        let nkeep = 5;
        let details =
            prune_blueprints(opctx, datastore, &opctx.log, 5, nblueprints)
                .await
                .expect("successful prune");
        println!("{details:?}");
        assert!(details.warnings.is_empty());

        // This is the crux of the test: we should have marked more bp_target
        // rows as removable than blueprints deleted.  And we should have
        // deleted all of the removable rows.
        assert_eq!(details.ntargets_removable, nblueprints - nkeep);
        assert_eq!(details.deleted.len(), nblueprints - nkeep - ndeleted);
        assert_eq!(details.ntargets_deleted, nblueprints - nkeep);

        // Load the database state again and make sure what's in the database
        // matches what we expect: just `nkeep` blueprints and target rows that
        // exactly match up.
        let blueprints = BlueprintDatabaseState::load(opctx, datastore).await;
        assert_eq!(blueprints.target_rows.len(), nkeep);
        blueprints.verify_blueprints_referenced_by_targets();
        blueprints.verify_targets_referenced_by_blueprints();

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // XXX-dap TODO-coverage test:
    // - pruning with multiple SQL batches
    // - pruning where it takes multiple batches to determine how many to keep
    // - extra blueprints (not referenced) are not touched
    // - case: gap in bp_target table
    // - same blueprint enabled/disabled a lot, at various points
}
