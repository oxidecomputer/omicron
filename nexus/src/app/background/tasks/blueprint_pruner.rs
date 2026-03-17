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
            match prune_blueprints(opctx, &self.datastore, &opctx.log).await {
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

/// Figure out which blueprints can be pruned and prune them.
async fn prune_blueprints(
    opctx: &OpContext,
    datastore: &DataStore,
    log: &Logger,
) -> Result<BlueprintPrunerStatus, anyhow::Error> {
    // Figure out the maximum version that we'd consider pruning.
    let target_table_status =
        determine_bp_target_rows_to_keep(opctx, datastore, MAX_NKEEP).await?;
    let keep_version = match target_table_status.keep {
        KeepWhat::All => {
            info!(
                log,
                "keeping all blueprints";
                "nfound" => target_table_status.nfound,
                "nscanned" => target_table_status.nscanned,
            );
            return Ok(BlueprintPrunerStatus::Enabled(
                BlueprintPrunerDetails {
                    nkept: target_table_status.nfound,
                    deleted: vec![],
                    ntargets_deleted: 0,
                    ntargets_removable: 0,
                    warnings: vec![],
                },
            ));
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
        max_delete_attempts: MAX_DELETE_ATTEMPTS_PER_ACTIVATION,
    };
    let mut pop = PruneTracker::new();
    let details = loop {
        match prune_batch(&pargs, pop).await {
            ControlFlow::Continue(newpop) => pop = newpop,
            ControlFlow::Break(newpop) => break newpop.into_details(&pargs),
        }
    };

    Ok(BlueprintPrunerStatus::Enabled(details))
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
    highest_version_deleted: Option<u32>,
}

impl PruneTracker {
    pub fn new() -> PruneTracker {
        PruneTracker {
            ntargets_removable: 0,
            ntargets_deleted: 0,
            deleted: vec![],
            errors: vec![],
            highest_version_deleted: None,
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

    pub fn record_targets_deleted(
        mut self,
        version_deleted_up_to: u32,
        count: usize,
    ) -> ControlFlow<Self, Self> {
        let Some(previous) = self.highest_version_deleted else {
            // This should be impossible.
            return ControlFlow::Break(self.record_error(anyhow!(
                "recording having deleted up to version \
                 {version_deleted_up_to} without having recorded deleting \
                 blueprints",
            )));
        };

        if previous < version_deleted_up_to {
            // This should be impossible.
            return ControlFlow::Break(self.record_error(anyhow!(
                "recorded having deleted up to version \
                 {version_deleted_up_to} but had only seen up to version \
                 {previous}",
            )));
        }

        self.ntargets_deleted += count;
        self.highest_version_deleted = None;
        ControlFlow::Continue(self)
    }

    fn into_details(self, pargs: &PruneArgs) -> BlueprintPrunerDetails {
        BlueprintPrunerDetails {
            nkept: pargs.nblueprints_found,
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
            .bp_target_delete_older(opctx, deleted_up_to)
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

                pop = pop.record_targets_deleted(deleted_up_to, count)?;
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
        debug!(
            log,
            "deleting old target blueprint";
            "version" => *row.version,
            "blueprint_id" => blueprint_id.to_string(),
        );
        let authz_blueprint = authz::Blueprint::new(
            authz::FLEET,
            blueprint_id.into_untyped_uuid(),
            LookupType::ById(blueprint_id.into_untyped_uuid()),
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
