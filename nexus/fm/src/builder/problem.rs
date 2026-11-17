// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::rng::ProblemBuilderRng;
use crate::problem::ProblemCode;
use iddqd::{IdOrdMap, id_ord_map};
use nexus_types::fm::AllProblems;
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::fm::SitrepProblem;
use nexus_types::fm::analysis_reports::{
    ActiveProblemReport, DebugLog, LogEntryBuilder,
};
use nexus_types::fm::problem::{AlertLink, SitrepRevision, SupportBundleLink};
use omicron_generation_kinds::ProblemRevNum;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::ProblemUuid;
use omicron_uuid_kinds::SitrepUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use std::sync::Arc;

/// A request to create the initial revision of an active problem.
pub struct ActiveProblemRequest {
    pub code: ProblemCode,
    pub summary: String,
    pub description: String,
    pub action_required: String,
}

#[derive(Debug)]
pub struct ProblemSetBuilder<'parent> {
    log: slog::Logger,
    pending_revisions: IdOrdMap<PendingRevision>,
    rng: ProblemBuilderRng,
    sitrep_id: SitrepUuid,
    new_active_problems_requested: usize,
    parent_problems: Option<&'parent AllProblems>,
}

/// A builder for a revision of a problem requested in the current sitrep.
///
/// The accessors on this type return the problem's state *as of this revision*.
/// If that field has not changed in this revision, they return the value in the
/// previous revision; if it has changed, they return the value being set in the
/// in-progress revision.
///
/// Setting a field to the value it already has in the parent sitrep's current
/// does not record a change for that field (and will undo any previously set
/// change). If there is no change when the sitrep is built, no new revision
/// will be created.
pub struct RevisionBuilder<'rev> {
    revision: id_ord_map::RefMut<'rev, PendingRevision>,
    sitrep_id: SitrepUuid,
    de: DiagnosisEngineKind,
}

#[derive(Debug)]
pub struct PendingRevision {
    /// The ID of the problem this revision is for.
    id: ProblemUuid,

    //
    // Internal bookkeeping for building the revision's debug log.
    //
    log: slog::Logger,
    debug_log: DebugLog,

    /// The problem as of the parent sitrep, or `None` if this problem is newly
    /// requested.
    ///
    /// If this is `None`, then `code`, `summary`, `description`, and
    /// `action_required` are always `Some`, because there is no previous
    /// revision for them to overlay on top of.
    parent: Option<Arc<SitrepProblem>>,

    //
    // Fields that may be modified in this revision
    //
    num: ProblemRevNum,
    code: Option<ProblemCode>,
    summary: Option<String>,
    description: Option<String>,
    action_required: Option<String>,
    resolved: bool,
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

impl PendingRevision {
    /// Returns the ID of the problem being revised.
    pub fn id(&self) -> ProblemUuid {
        self.id
    }

    /// Returns the number this revision will have, if it is created.
    pub fn number(&self) -> ProblemRevNum {
        self.num
    }

    /// Returns the problem's code as of this revision.
    pub fn code(&self) -> ProblemCode {
        self.code
            .or_else(|| self.previous_revision().map(|p| p.code))
            .expect(PARENT_OR_ALL_FIELDS_SET)
    }

    /// Returns the problem's summary as of this revision.
    pub fn summary(&self) -> &str {
        self.summary
            .as_deref()
            .or_else(|| self.previous_revision().map(|p| p.summary.as_str()))
            .expect(PARENT_OR_ALL_FIELDS_SET)
    }

    /// Returns the problem's description as of this revision.
    pub fn description(&self) -> &str {
        self.description
            .as_deref()
            .or_else(|| {
                self.previous_revision().map(|p| p.description.as_str())
            })
            .expect(PARENT_OR_ALL_FIELDS_SET)
    }

    /// Returns the problem's required operator action as of this revision.
    pub fn action_required(&self) -> &str {
        self.action_required
            .as_deref()
            .or_else(|| {
                self.previous_revision().map(|p| p.action_required.as_str())
            })
            .expect(PARENT_OR_ALL_FIELDS_SET)
    }

    /// Returns `true` if this revision resolves the problem.
    ///
    /// [`ProblemSetBuilder::revise`] will never revise a problem that was
    /// already resolved in the parent sitrep, so this is only `true` if the
    /// problem was resolved in the sitrep being built.
    pub fn is_resolved(&self) -> bool {
        self.resolved
    }

    /// Returns an iterator over all alerts attached to this problem, including
    /// both those attached in earlier revisions and alerts newly attached in
    /// this revision.
    pub fn alerts(&self) -> impl Iterator<Item = &AlertLink> + '_ {
        self.parent
            .iter()
            .flat_map(|p| p.alerts.iter())
            .chain(self.new_alerts.iter())
    }

    /// Returns `true` if the alert with the provided alert UUID has been
    /// attached to this problem, either in this revision or in an earlier one.
    pub fn contains_alert(&self, alert: impl AsRef<AlertUuid>) -> bool {
        let alert_id = alert.as_ref();
        self.new_alerts.contains_key(alert_id)
            || self.parent_matches(|p| p.alerts.contains_key(alert_id))
    }

    /// Returns an iterator over all support bundles attached to this problem,
    /// including both those attached in earlier revisions and support bundles
    /// newly attached in this revision.
    pub fn support_bundles(
        &self,
    ) -> impl Iterator<Item = &SupportBundleLink> + '_ {
        self.parent
            .iter()
            .flat_map(|p| p.support_bundles.iter())
            .chain(self.new_support_bundles.iter())
    }

    /// Returns `true` if the alert with the provided alert UUID has been
    /// attached to this problem, either in this revision or in an earlier one.
    pub fn contains_support_bundle(
        &self,
        bundle: impl AsRef<SupportBundleUuid>,
    ) -> bool {
        let id = bundle.as_ref();
        self.new_support_bundles.contains_key(id)
            || self.parent_matches(|p| p.support_bundles.contains_key(id))
    }

    /// Returns `true` if this revision has changed the problem's state from its
    /// previous revision.
    ///
    /// If this is `false` when the sitrep is built, no revision will be
    /// created.
    pub fn has_changed(&self) -> bool {
        // IMPORTANT NOTE: this function uses an exhaustive destructuring
        // specifically to ensure any newly-added fields are considered. If this
        // destructuring produces a compiler error, it means you have added a
        // new field to the `PendingRevision` struct. If the newly-added field
        // represents data that makes it into the actual revision of the problem
        // (which it probably does), you must include it in the check for
        // changes here. If it does not, ignore the new field explicitly, rather
        // than adding `..`!
        let Self {
            // Ignore fields that are not part of the revision being
            // constructed.
            id: _,
            num: _,
            log: _,
            debug_log: _,
            parent: _,
            // Every other field is part of the new revision being constructed,
            // and we must test whether any of them have changed.
            code,
            summary,
            description,
            action_required,
            new_alerts,
            new_support_bundles,
            resolved,
        } = self;
        code.is_some()
            || summary.is_some()
            || description.is_some()
            || action_required.is_some()
            || *resolved
            || !new_alerts.is_empty()
            || !new_support_bundles.is_empty()
    }

    /// Returns the problem's current revision in the parent sitrep, if the
    /// problem existed there.
    fn previous_revision(&self) -> Option<&SitrepRevision> {
        self.parent.as_deref().map(SitrepProblem::current_revision)
    }

    fn parent_matches(
        &self,
        predicate: impl FnOnce(&SitrepProblem) -> bool,
    ) -> bool {
        self.parent.as_deref().map_or(false, predicate)
    }

    fn build(
        self,
        now: DateTime<Utc>,
    ) -> (Arc<SitrepProblem>, ActiveProblemReport) {
        if !self.has_changed() {
            let problem = self.parent.expect(
                "if an active problem has not changed in this sitrep, it must \
                 have existed in a previous sitrep",
            );
            let rev = problem.current_revision();
            let resolved_sitrep_id = if rev.resolved_sitrep_id.is_some() {
                Some(rev.requested_sitrep_id)
            } else {
                None
            };
            let report = ActiveProblemReport {
                id: problem.id,
                requested_sitrep_id: problem.requested_sitrep_id,
                revised_sitrep_id: rev.requested_sitrep_id,
                resolved_sitrep_id,
            };
            (problem, report)
        }
    }
}

impl<'parent> ProblemSetBuilder<'parent> {
    pub fn new(
        rng: ProblemBuilderRng,
        sitrep_id: SitrepUuid,
        parent_sitrep: Option<&'parent nexus_types::fm::Sitrep>,
        log: slog::Logger,
    ) -> Self {
        Self {
            log,
            pending_revisions: IdOrdMap::default(),
            rng,
            sitrep_id,
            new_active_problems_requested: 0,
            parent_problems: parent_sitrep.map(|s| &s.problems),
        }
    }

    pub fn request_active_problem(
        &mut self,
        de: DiagnosisEngineKind,
        request: ActiveProblemRequest,
        comment: impl ToString,
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
            .comment(comment)
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
                RevisionBuilder { revision, sitrep_id: self.sitrep_id, de }
            }
        }
    }

    pub fn revise<'problem>(
        &'problem mut self,
        de: DiagnosisEngineKind,
        id: &ProblemUuid,
    ) -> Option<RevisionBuilder<'problem>> {
        let sitrep_id = self.sitrep_id;
        match self.pending_revisions.entry(id) {
            // There's already a pending revision request for this problem, so
            // return that.
            id_ord_map::Entry::Occupied(entry) => Some(RevisionBuilder {
                revision: entry.into_mut(),
                sitrep_id,
                de,
            }),
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
                    new_alerts: IdOrdMap::default(),
                    new_support_bundles: IdOrdMap::default(),
                    parent: Some(parent_problem.clone()),
                });
                Some(RevisionBuilder { revision, sitrep_id, de })
            }
        }
    }

    /// Immutably borrows the pending revision for the given problem ID, if one
    /// exists. If this method returns `None`, the problem may exist in the
    /// parent sitrep and may still be copied forward into this sitrep, but no
    /// revision for it is currently being prepared in this sitrep.
    pub fn pending_revision_for(
        &self,
        id: &ProblemUuid,
    ) -> Option<&PendingRevision> {
        self.pending_revisions.get(id)
    }

    pub fn build(self) -> (AllProblems, IdOrdMap<ActiveProblemReport>) {
        todo!()
    }
}

macro_rules! revise_field {
    ($self:ident, $field:ident, $value:expr, $comment:expr) => {{
        let rev = &mut *$self.revision;
        let prior = rev.parent.as_deref().map(|p| &p.current_revision().$field);
        let value = $value;
        if rev.$field.as_ref().or(prior) != Some(&value) {
            rev.$field = if prior == Some(&value) { None } else { Some(value) };
            let revised_val = rev.$field.as_ref().or(prior);
            rev.debug_log
                .entry(&rev.log, concat!("revised ", stringify!($field)))
                .kv(stringify!($field), revised_val)
                .kv("revised_by", &$self.de)
                .comment($comment)
                .finish();
        }
    }};
}

impl RevisionBuilder<'_> {
    /// Sets the problem's code as of this revision.
    pub fn set_code(
        &mut self,
        code: ProblemCode,
        comment: impl ToString,
    ) -> &mut Self {
        revise_field!(self, code, code, comment);
        self
    }

    /// Sets the problem's one-sentence summary as of this revision.
    pub fn set_summary(
        &mut self,
        summary: impl ToString,
        comment: impl ToString,
    ) -> &mut Self {
        revise_field!(self, summary, summary.to_string(), comment);
        self
    }

    /// Sets the problem's description as of this revision.
    pub fn set_description(
        &mut self,
        description: impl ToString,
        comment: impl ToString,
    ) -> &mut Self {
        revise_field!(self, description, description.to_string(), comment);
        self
    }

    /// Sets the operator action required to address the problem as of this
    /// revision.
    pub fn set_action_required(
        &mut self,
        action_required: impl ToString,
        comment: impl ToString,
    ) -> &mut Self {
        revise_field!(
            self,
            action_required,
            action_required.to_string(),
            comment
        );
        self
    }

    /// Marks the problem as resolved as of this revision.
    ///
    /// Resolution cannot be undone. Once the problem has been resolved,
    /// subsequent sitreps will no longer be able to resolve it.
    pub fn resolve(&mut self, comment: impl ToString) {
        if self.revision.resolved {
            self.log_warning("attempted to resolve a problem twice")
                .comment(comment)
                .finish();
            return;
        }
        self.revision.resolved = true;
        self.log_event("resolved problem").comment(comment).finish();
    }

    /// Attaches an alert to this problem, associating it with this revision.
    ///
    /// If the alert is already attached to this problem, either in the parent
    /// sitrep or earlier in the current one, the existing attachment is left
    /// unchanged and a warning is recorded in the debug log.
    pub fn attach_alert(
        &mut self,
        alert_id: AlertUuid,
        comment: impl ToString,
    ) {
        let comment = comment.to_string();
        let rev = &mut *self.revision;
        let existing = rev
            .parent
            .as_deref()
            .and_then(|p| p.alerts.get(&alert_id))
            .or_else(|| rev.new_alerts.get(&alert_id))
            .map(|link| (link.revision, link.linked_sitrep_id));
        if let Some((revision, linked_sitrep_id)) = existing {
            self.log_warning("attempted to link an alert to a problem twice")
                .kv("alert_id", alert_id)
                .kv("existing_link_revision", revision.as_u64())
                .kv("existing_link_sitrep_id", linked_sitrep_id)
                .comment(comment)
                .finish();
            return;
        }

        let link = AlertLink {
            alert_id,
            revision: rev.num,
            linked_sitrep_id: self.sitrep_id,
            comment: comment.clone(),
        };
        rev.new_alerts
            .insert_unique(link)
            .expect("we just checked that the alert is not already linked");
        self.log_event("added alert to problem")
            .kv("alert_id", alert_id)
            .comment(comment)
            .finish();
    }

    /// Attaches a support bundle to this problem, associating it with this
    /// revision.
    ///
    /// If the support bundle is already linked to this problem, either in the
    /// parent sitrep or earlier in the current one, the existing link is left
    /// unchanged and a warning is recorded in the debug log.
    pub fn attach_support_bundle(
        &mut self,
        support_bundle_id: SupportBundleUuid,
        comment: impl ToString,
    ) {
        let comment = comment.to_string();
        let rev = &mut *self.revision;
        let existing = rev
            .parent
            .as_deref()
            .and_then(|p| p.support_bundles.get(&support_bundle_id))
            .or_else(|| rev.new_support_bundles.get(&support_bundle_id))
            .map(|link| (link.revision, link.linked_sitrep_id));
        if let Some((revision, linked_sitrep_id)) = existing {
            self.log_warning(
                "attempted to attach a support bundle to a problem twice",
            )
            .kv("support_bundle_id", support_bundle_id)
            .kv("existing_link_revision", revision.as_u64())
            .kv("existing_link_sitrep_id", linked_sitrep_id)
            .comment(comment)
            .finish();
            return;
        }

        let link = SupportBundleLink {
            support_bundle_id,
            revision: rev.num,
            linked_sitrep_id: self.sitrep_id,
            comment: comment.clone(),
        };
        rev.new_support_bundles.insert_unique(link).expect(
            "we just checked that the support bundle is not already linked",
        );
        self.log_event("linked support bundle")
            .kv("support_bundle_id", support_bundle_id)
            .comment(comment)
            .finish();
    }

    /// Adds an event to this problem's analysis debug log.
    pub fn log_event(&mut self, event: impl ToString) -> LogEntryBuilder<'_> {
        let de = self.de;
        let rev = &mut *self.revision;
        rev.debug_log.entry(&rev.log, event).kv("de", de)
    }

    /// Adds a warning-level event to this problem's analysis debug log.
    pub fn log_warning(&mut self, event: impl ToString) -> LogEntryBuilder<'_> {
        let de = self.de;
        let rev = &mut *self.revision;
        rev.debug_log.warning(&rev.log, event).kv("de", de)
    }
}

impl std::ops::Deref for RevisionBuilder<'_> {
    type Target = PendingRevision;
    fn deref(&self) -> &Self::Target {
        &self.revision
    }
}

const PARENT_OR_ALL_FIELDS_SET: &str = "a pending revision without a parent \
    problem was created by `request_active_problem`, which sets every field, \
    and fields are only cleared when they equal the previous revision's value, \
    so they cannot have been unset when there is no previous revision";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::SitrepBuilderRng;
    use omicron_test_utils::dev;

    const DE: DiagnosisEngineKind = DiagnosisEngineKind::PhysicalDisk;

    fn debug_log_len(log: &DebugLog) -> usize {
        serde_json::to_value(log)
            .expect("debug logs are serializable")
            .as_array()
            .expect("debug logs serialize as arrays")
            .len()
    }

    /// Returns a parent problem with a single, unresolved revision, which
    /// has one linked alert and one linked support bundle.
    fn parent_problem() -> SitrepProblem {
        let sitrep_id = SitrepUuid::new_v4();
        let num = ProblemRevNum::new();
        let mut revisions = IdOrdMap::new();
        revisions
            .insert_unique(SitrepRevision {
                number: num,
                requested_sitrep_id: sitrep_id,
                resolved_sitrep_id: None,
                code: ProblemCode::UnhealthyZpool,
                summary: "parent summary".to_string(),
                description: "parent description".to_string(),
                action_required: "parent action".to_string(),
                comment: "parent comment".to_string(),
            })
            .expect("map is empty");
        let mut alerts = IdOrdMap::new();
        alerts
            .insert_unique(AlertLink {
                alert_id: AlertUuid::new_v4(),
                revision: num,
                linked_sitrep_id: sitrep_id,
                comment: "parent alert".to_string(),
            })
            .expect("map is empty");
        let mut support_bundles = IdOrdMap::new();
        support_bundles
            .insert_unique(SupportBundleLink {
                support_bundle_id: SupportBundleUuid::new_v4(),
                revision: num,
                linked_sitrep_id: sitrep_id,
                comment: "parent support bundle".to_string(),
            })
            .expect("map is empty");
        SitrepProblem {
            id: ProblemUuid::new_v4(),
            requested_sitrep_id: sitrep_id,
            comment: "parent problem".to_string(),
            revisions,
            alerts,
            support_bundles,
        }
    }

    fn builder<'a>(
        log: &slog::Logger,
        parent_problems: Option<&'a AllProblems>,
    ) -> ProblemSetBuilder<'a> {
        let mut rng = SitrepBuilderRng::from_seed("problem_set");
        ProblemSetBuilder {
            log: log.clone(),
            pending_revisions: IdOrdMap::new(),
            rng: rng.problem_builder_rng(),
            sitrep_id: SitrepUuid::new_v4(),
            new_active_problems_requested: 0,
            parent_problems,
        }
    }

    /// Setting a field to the parent revision's value must not record a
    /// change, and setting a changed field back to the parent's value must
    /// retract the change rather than recording a redundant one.
    #[test]
    fn fields_matching_parent_are_not_recorded() {
        let logctx =
            dev::test_setup_log("fields_matching_parent_are_not_recorded");
        let parent = parent_problem();
        let id = parent.id;
        let parent_problems = AllProblems::try_from_iter([parent])
            .expect("parent problem is valid");
        let mut problems = builder(&logctx.log, Some(&parent_problems));
        let mut rev = problems.revise(DE, &id).expect("problem is unresolved");

        rev.set_summary("parent summary", "no-op");
        rev.set_code(ProblemCode::UnhealthyZpool, "no-op");
        assert_eq!(rev.revision.summary, None);
        assert_eq!(rev.revision.code, None);
        assert_eq!(rev.summary(), "parent summary");
        assert_eq!(debug_log_len(&rev.revision.debug_log), 0);

        rev.set_summary("new summary", "changed");
        assert_eq!(rev.revision.summary.as_deref(), Some("new summary"));
        assert_eq!(rev.summary(), "new summary");
        assert_eq!(debug_log_len(&rev.revision.debug_log), 1);

        // Setting the same new value again is not a change.
        rev.set_summary("new summary", "no-op");
        assert_eq!(debug_log_len(&rev.revision.debug_log), 1);

        rev.set_summary("parent summary", "reverted");
        assert_eq!(rev.revision.summary, None);
        assert_eq!(rev.summary(), "parent summary");
        assert_eq!(debug_log_len(&rev.revision.debug_log), 2);

        // Fields we never touched still read through to the parent.
        assert_eq!(rev.description(), "parent description");
        assert_eq!(rev.action_required(), "parent action");
        drop(rev);
        logctx.cleanup_successful();
    }

    /// Linking an alert or support bundle that is already linked, in either
    /// the parent sitrep or the current one, must not add a second link.
    #[test]
    fn duplicate_links_are_not_added() {
        let logctx = dev::test_setup_log("duplicate_links_are_not_added");
        let parent = parent_problem();
        let id = parent.id;
        let parent_alert = parent.alerts.iter().next().unwrap().alert_id;
        let parent_bundle =
            parent.support_bundles.iter().next().unwrap().support_bundle_id;
        let parent_problems = AllProblems::try_from_iter([parent])
            .expect("parent problem is valid");
        let mut problems = builder(&logctx.log, Some(&parent_problems));
        let mut rev = problems.revise(DE, &id).expect("problem is unresolved");

        rev.attach_alert(parent_alert, "already linked in parent");
        rev.attach_support_bundle(parent_bundle, "already linked in parent");
        assert!(rev.revision.new_alerts.is_empty());
        assert!(rev.revision.new_support_bundles.is_empty());

        let new_alert = AlertUuid::new_v4();
        let new_bundle = SupportBundleUuid::new_v4();
        rev.attach_alert(new_alert, "new");
        rev.attach_alert(new_alert, "already linked in this sitrep");
        rev.attach_support_bundle(new_bundle, "new");
        rev.attach_support_bundle(new_bundle, "already linked in this sitrep");
        assert_eq!(rev.revision.new_alerts.len(), 1);
        assert_eq!(rev.revision.new_support_bundles.len(), 1);
        let link = rev.revision.new_alerts.get(&new_alert).unwrap();
        assert_eq!(link.revision, rev.number());
        assert_eq!(link.comment, "new");

        assert_eq!(rev.alerts().count(), 2);
        assert_eq!(rev.support_bundles().count(), 2);
        // Every attempted link, successful or not, leaves a log entry.
        assert_eq!(debug_log_len(&rev.revision.debug_log), 6);
        drop(rev);
        logctx.cleanup_successful();
    }
}
