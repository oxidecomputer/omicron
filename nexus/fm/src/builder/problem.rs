// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::rng::ProblemBuilderRng;
use crate::problem::ProblemCode;
use iddqd::{IdOrdMap, id_ord_map};
use nexus_types::fm::AllProblems;
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::fm::SitrepProblem;
use nexus_types::fm::analysis_reports::DebugLog;
use nexus_types::fm::problem::{AlertLink, SupportBundleLink};
use omicron_generation_kinds::ProblemRevNum;
use omicron_uuid_kinds::ProblemUuid;
use std::sync::Arc;

/// A request to create the initial revision of an active problem.
pub struct ActiveProblemRequest {
    pub code: ProblemCode,
    pub summary: String,
    pub description: String,
    pub action_required: String,
    pub comment: String,
}

#[derive(Debug)]
pub struct ProblemSetBuilder<'parent> {
    log: slog::Logger,
    pending_revisions: IdOrdMap<PendingRevision>,
    rng: ProblemBuilderRng,
    new_active_problems_requested: usize,
    parent_problems: Option<&'parent AllProblems>,
}

pub struct RevisionBuilder<'rev> {
    revision: id_ord_map::RefMut<'rev, PendingRevision>,
    de: DiagnosisEngineKind,
}

#[derive(Debug)]
struct PendingRevision {
    id: ProblemUuid,
    log: slog::Logger,
    num: ProblemRevNum,
    code: Option<ProblemCode>,
    summary: Option<String>,
    description: Option<String>,
    comment: Option<String>,
    action_required: Option<String>,
    debug_log: DebugLog,
    resolved: bool,
    parent: Option<Arc<SitrepProblem>>,
    // Alert links added in this revision.
    new_alerts: IdOrdMap<AlertLink>,
    // Support bundle links added in this revision.
    new_support_bundles: IdOrdMap<SupportBundleLink>,
}

impl iddqd::IdOrdItem for PendingRevision {
    type Key<'a> = &'a ProblemUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.id
    }
    iddqd::id_upcast!();
}

impl<'parent> ProblemSetBuilder<'parent> {
    pub fn new(
        rng: ProblemBuilderRng,
        parent_sitrep: Option<&'parent nexus_types::fm::Sitrep>,
        log: slog::Logger,
    ) -> Self {
        Self {
            log,
            pending_revisions: IdOrdMap::default(),
            rng,
            new_active_problems_requested: 0,
            parent_problems: parent_sitrep.map(|s| &s.problems),
        }
    }

    pub fn request_active_problem(
        &mut self,
        de: DiagnosisEngineKind,
        request: ActiveProblemRequest,
    ) -> RevisionBuilder<'_> {
        let id = loop {
            let id = self.rng.next_active_problem();

            // Check if this UUID already exists in the current set of pending
            // revisions *or* in the parent sitrep at all (since there may be a
            // carried forward problem that has not (yet) been revised in this
            // sitrep).
            let already_exists = self.pending_revisions.contains_key(&id)
                || self
                    .parent_problems
                    .map_or(false, |p| p.by_id().contains_key(&id));
            if !already_exists {
                break id;
            }
        };

        let ActiveProblemRequest {
            code,
            summary,
            description,
            comment,
            action_required,
        } = request;

        let num = ProblemRevNum::new();
        let log = self.log.new(slog::o!(
            "problem_id" => id.to_string(),
            "revision" => num.as_u64(),
        ));

        let mut debug_log = DebugLog::default();
        debug_log
            .entry(&log, "requested a new active problem")
            .kv("code", code)
            .kv("requested_by", de)
            .comment(&comment)
            .finish();

        let builder = PendingRevision {
            id,
            log,
            num,
            debug_log,
            code: Some(code),
            resolved: false,
            summary: Some(summary),
            description: Some(description),
            action_required: Some(action_required),
            comment: Some(comment),
            parent: None,
            new_alerts: IdOrdMap::default(),
            new_support_bundles: IdOrdMap::default(),
        };

        match self.pending_revisions.entry(&id) {
            id_ord_map::Entry::Occupied(_) => {
                unreachable!(
                    "active problem ID {id} should not already exist in pending
                    revisions, since we just checked that it didn't exist!"
                );
            }
            id_ord_map::Entry::Vacant(entry) => {
                self.new_active_problems_requested += 1;
                let revision = entry.insert(builder);
                RevisionBuilder { revision, de }
            }
        }
    }

    pub fn revise<'problem>(
        &'problem mut self,
        de: DiagnosisEngineKind,
        id: &ProblemUuid,
    ) -> Option<RevisionBuilder<'problem>> {
        match self.pending_revisions.entry(id) {
            // There's already a pending revision request for this problem, so
            // return that.
            id_ord_map::Entry::Occupied(entry) => {
                Some(RevisionBuilder { revision: entry.into_mut(), de })
            }
            id_ord_map::Entry::Vacant(entry) => {
                // Okay, does the problem exist in the parent sitrep? If so,
                // create a pending revision for it, provided that the previous
                // revision did not resolve it.
                let parent_problem = self.parent_problems?.by_id().get(id)?;
                if parent_problem.is_resolved() {
                    return None;
                }
                let id = parent_problem.id;
                let num = parent_problem.current_revision().number.next();
                let log = self.log.new(slog::o!(
                    "problem_id" => id.to_string(),
                    "revision" => num.as_u64(),
                ));
                let revision = entry.insert(PendingRevision {
                    id,
                    log,
                    num,
                    debug_log: DebugLog::default(),
                    resolved: false,
                    code: None,
                    summary: None,
                    description: None,
                    action_required: None,
                    comment: None,
                    new_alerts: IdOrdMap::default(),
                    new_support_bundles: IdOrdMap::default(),
                    parent: Some(parent_problem.clone()),
                });
                Some(RevisionBuilder { revision, de })
            }
        }
    }
}
