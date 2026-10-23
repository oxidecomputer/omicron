// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::display;
use super::display::const_max_len;
use crate::alert::AlertClass;
use crate::fm::DiagnosisEngineKind;
use crate::fm::Ereport;
use crate::fm::EreportId;
use crate::fm::FactMetadata;
use crate::fm::FactPayload;
use crate::fm::{CertificateFact, DiskFact, SagaFact, SmfServiceFact};
use crate::support_bundle::BundleDataSelection;
use iddqd::{IdOrdItem, IdOrdMap};
use omicron_uuid_kinds::{
    AlertUuid, CaseEreportUuid, CaseUuid, FactUuid, ProblemUuid, SitrepUuid,
    SupportBundleUuid,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Case {
    pub id: CaseUuid,
    #[serde(flatten)]
    pub metadata: Metadata,

    pub ereports: IdOrdMap<CaseEreport>,
    pub alerts_requested: IdOrdMap<AlertRequest>,
    pub support_bundles_requested: IdOrdMap<SupportBundleRequest>,
    /// Diagnosis-engine-derived facts attached to this case. See
    /// [`Fact`] for semantics.
    pub facts: IdOrdMap<Fact>,
    /// Problems associated with this case, indexed by their ID.
    pub problems: IdOrdMap<CaseProblem>,
}

impl Case {
    pub fn id(&self) -> &CaseUuid {
        &self.id
    }

    pub fn is_open(&self) -> bool {
        self.metadata.is_open()
    }

    pub fn display_indented(
        &self,
        indent: usize,
        sitrep_id: Option<SitrepUuid>,
    ) -> DisplayCase<'_> {
        DisplayCase { case: self, indent, sitrep_id, colored: false }
    }

    pub fn display_summary(
        &self,
        indent: usize,
        sitrep_id: Option<SitrepUuid>,
    ) -> DisplayCaseSummary<'_> {
        DisplayCaseSummary { case: self, indent, sitrep_id, colored: false }
    }

    /// Returns `true` if any records were added to this case in the given
    /// `sitrep_id`.
    fn additions_in_sitrep(&self, sitrep_id: SitrepUuid) -> bool {
        self.ereports.iter().any(|e| e.assigned_sitrep_id == sitrep_id)
            || self
                .alerts_requested
                .iter()
                .any(|a| a.requested_sitrep_id == sitrep_id)
            || self
                .support_bundles_requested
                .iter()
                .any(|b| b.requested_sitrep_id == sitrep_id)
            || self
                .facts
                .iter()
                .any(|f| f.metadata.created_sitrep_id == sitrep_id)
            || self.problems.iter().any(|p| {
                // TODO(eliza): we *might* also consider checking if the problem
                // itself has a revision that was created in this sitrep, but
                // that would require passing in the whole sitrep...and, I'm on
                // the fence as to whether that should be considered a change to
                // the *case* or not...
                p.created_sitrep_id == sitrep_id
            })
    }
}

impl fmt::Display for Case {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.display_indented(0, None).fmt(f)
    }
}

impl IdOrdItem for Case {
    type Key<'a> = &'a CaseUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    iddqd::id_upcast!();
}

/// Metadata about a case.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Metadata {
    pub created_sitrep_id: SitrepUuid,
    pub closed_sitrep_id: Option<SitrepUuid>,

    pub de: DiagnosisEngineKind,

    pub comment: String,
}

impl Metadata {
    pub fn is_open(&self) -> bool {
        self.closed_sitrep_id.is_none()
    }

    pub fn display_multiline(
        &self,
        indent: usize,
        sitrep: Option<SitrepUuid>,
    ) -> DisplayMetadata<'_> {
        DisplayMetadata {
            meta: self,
            indent,
            sitrep_id: sitrep,
            colored: false,
        }
    }
}

/// Displays a case's [`Metadata`], as returned by
/// [`Metadata::display_multiline`].
#[must_use = "this struct does nothing unless displayed"]
pub struct DisplayMetadata<'a> {
    meta: &'a Metadata,
    indent: usize,
    sitrep_id: Option<SitrepUuid>,
    colored: bool,
}

impl DisplayMetadata<'_> {
    /// If `colored` is true, style the output with ANSI terminal colors.
    pub fn colored(mut self, colored: bool) -> Self {
        self.colored = colored;
        self
    }
}

impl fmt::Display for DisplayMetadata<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let DisplayMetadata {
            meta: Metadata { de, created_sitrep_id, closed_sitrep_id, comment },
            indent,
            sitrep_id,
            colored,
        } = self;
        let styles = display::Styles::new(*colored);
        let sitrep_id = sitrep_id.as_ref();
        let this_sitrep = move |s| {
            styles.annotation_if(Some(s) == sitrep_id, " <-- this sitrep")
        };

        const DE: &str = "diagnosis engine:";
        const OPENED_IN: &str = "opened in sitrep:";
        const CLOSED_IN: &str = "closed in sitrep:";
        const WIDTH: usize = const_max_len(&[DE, OPENED_IN, CLOSED_IN]);

        display::Comment::from(comment)
            .indent(*indent)
            .colored(*colored)
            .fmt(f)?;
        writeln!(f, "{:>indent$}{} {de}", "", styles.label(DE, WIDTH))?;
        writeln!(
            f,
            "{:>indent$}{} {created_sitrep_id}{}",
            "",
            styles.label(OPENED_IN, WIDTH),
            this_sitrep(created_sitrep_id)
        )?;
        if let Some(closed_id) = closed_sitrep_id {
            writeln!(
                f,
                "{:>indent$}{} {closed_id}{}",
                "",
                styles.label(CLOSED_IN, WIDTH),
                this_sitrep(closed_id)
            )?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct CaseEreport {
    pub id: CaseEreportUuid,
    pub ereport: Arc<Ereport>,
    pub assigned_sitrep_id: SitrepUuid,
    pub comment: String,
}

impl IdOrdItem for CaseEreport {
    type Key<'a> = <Arc<Ereport> as IdOrdItem>::Key<'a>;
    fn key(&self) -> Self::Key<'_> {
        self.ereport.key()
    }

    iddqd::id_upcast!();
}

impl CaseEreport {
    pub fn ereport_id(&self) -> &EreportId {
        &self.ereport.id
    }
}

/// A diagnosis-engine-derived fact attached to a [`Case`].
///
/// Facts are **immutable**: to "update" a fact, the diagnosis engine
/// removes the old one and adds a fresh one. As long as a fact's content
/// matches the engine's current view, the same fact is carried forward
/// across sitreps unchanged.
///
/// The `payload` is a fully-typed [`FactPayload`] whose variant is owned by
/// the case's diagnosis engine (see [`Metadata::de`]).
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Fact {
    #[serde(flatten)]
    pub metadata: FactMetadata,
    pub payload: FactPayload,
}

impl Fact {
    /// The saga payload, or a [`ForeignFact`] error if this fact belongs to
    /// a different diagnosis engine.
    pub fn as_saga(&self) -> Result<&SagaFact, ForeignFact> {
        self.payload.as_saga().ok_or_else(|| self.foreign_fact())
    }

    /// The physical-disk payload, or a [`ForeignFact`] error if this fact
    /// belongs to a different diagnosis engine.
    pub fn as_physical_disk(&self) -> Result<&DiskFact, ForeignFact> {
        self.payload.as_physical_disk().ok_or_else(|| self.foreign_fact())
    }

    /// The certificate payload, or a [`ForeignFact`] error if this fact
    /// belongs to a different diagnosis engine.
    pub fn as_certificate(&self) -> Result<&CertificateFact, ForeignFact> {
        self.payload.as_certificate().ok_or_else(|| self.foreign_fact())
    }

    /// The SMF service payload, or a [`ForeignFact`] error if this fact
    /// belongs to a different diagnosis engine.
    pub fn as_smf_service(&self) -> Result<&SmfServiceFact, ForeignFact> {
        self.payload.as_smf_service().ok_or_else(|| self.foreign_fact())
    }

    fn foreign_fact(&self) -> ForeignFact {
        ForeignFact { fact_id: self.metadata.id, actual: self.payload.engine() }
    }
}

/// Error returned by [`Fact::as_saga`] and friends: the fact's payload is
/// owned by a different diagnosis engine than the caller's.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("fact {fact_id} belongs to the {actual} diagnosis engine")]
pub struct ForeignFact {
    pub fact_id: FactUuid,
    /// The engine that owns the fact's payload.
    pub actual: DiagnosisEngineKind,
}

impl IdOrdItem for Fact {
    type Key<'a> = &'a FactUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.metadata.id
    }
    iddqd::id_upcast!();
}

impl Fact {
    pub fn display_multiline(
        &self,
        indent: usize,
        sitrep_id: Option<SitrepUuid>,
    ) -> DisplayFact<'_> {
        DisplayFact { fact: self, indent, sitrep_id, colored: false }
    }
}

/// Displays a [`Fact`], as returned by [`Fact::display_multiline`].
#[must_use = "this struct does nothing unless displayed"]
pub struct DisplayFact<'a> {
    fact: &'a Fact,
    indent: usize,
    sitrep_id: Option<SitrepUuid>,
    colored: bool,
}

impl DisplayFact<'_> {
    /// If `colored` is true, style the output with ANSI terminal colors.
    pub fn colored(mut self, colored: bool) -> Self {
        self.colored = colored;
        self
    }
}

impl fmt::Display for DisplayFact<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const BULLET: &str = "* ";

        let &Self {
            fact:
                Fact {
                    metadata: FactMetadata { id, created_sitrep_id, comment },
                    payload,
                },
            indent,
            sitrep_id,
            colored,
        } = self;
        let styles = display::Styles::new(colored);
        let this_sitrep =
            |s| styles.annotation_if(Some(s) == sitrep_id, " <-- this sitrep");

        writeln!(
            f,
            "{BULLET:>indent$}{}",
            styles.heading().style(format_args!("fact {id}"))
        )?;
        display::Comment::from(comment)
            .indent(indent)
            .colored(colored)
            .fmt(f)?;
        writeln!(
            f,
            "{:>indent$}{}: {created_sitrep_id}{}",
            "",
            styles.heading().style("added in"),
            this_sitrep(*created_sitrep_id),
        )?;
        let payload =
            serde_json::to_value(payload).unwrap_or(serde_json::Value::Null);
        display::fmt_json_value(f, "payload", &payload, indent, styles)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AlertRequest {
    pub id: AlertUuid,
    pub class: AlertClass,
    pub version: u32,
    pub payload: serde_json::Value,
    pub requested_sitrep_id: SitrepUuid,
    pub comment: String,
}

impl iddqd::IdOrdItem for AlertRequest {
    type Key<'a> = &'a AlertUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    iddqd::id_upcast!();
}

/// A request to create a support bundle, associated with a [`Case`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SupportBundleRequest {
    /// Unique identifier for this support bundle request.
    pub id: SupportBundleUuid,
    /// The sitrep in which this support bundle was requested.
    pub requested_sitrep_id: SitrepUuid,
    /// Which data to include in the support bundle. Use
    /// [`BundleDataSelection::all()`] to request all data.
    pub data_selection: BundleDataSelection,
    pub comment: String,
}

impl iddqd::IdOrdItem for SupportBundleRequest {
    type Key<'a> = &'a SupportBundleUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    iddqd::id_upcast!();
}

/// An association between a [`Case`] and a problem.
///
/// These associations are used by diagnosis engine to track whether a problem
/// exists for a given case across multiple sitreps. The relationship between
/// cases and problems is many-to-many: a problem may be created due to the
/// observations contained in one case, but diagnosis engines may determine
/// that other cases contain observations also related to the same problem. In
/// the other direction, a case may result in the diagnosis of any number of
/// problems.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CaseProblem {
    /// The ID of the associated problem.
    pub problem_id: ProblemUuid,
    /// The sitrep in which this association between the case and the problem
    /// was first created.
    ///
    /// This is *not* the sitrep in which either the case or the problem was
    /// created.
    pub linked_sitrep_id: SitrepUuid,
    /// A human-readable comment why the problem is associated with the
    /// case, for debugging purposes.
    pub comment: String,
}

impl iddqd::IdOrdItem for CaseProblem {
    type Key<'a> = &'a ProblemUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.problem_id
    }

    iddqd::id_upcast!();
}

/// Displays a [`Case`], as returned by [`Case::display_indented`].
#[must_use = "this struct does nothing unless displayed"]
pub struct DisplayCase<'a> {
    case: &'a Case,
    indent: usize,
    sitrep_id: Option<SitrepUuid>,
    colored: bool,
}

impl DisplayCase<'_> {
    /// If `colored` is true, style the output with ANSI terminal colors.
    pub fn colored(mut self, colored: bool) -> Self {
        self.colored = colored;
        self
    }
}

const BULLET: &str = "* ";

impl fmt::Display for DisplayCase<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let &Self {
            case:
                Case {
                    id,
                    metadata,
                    ereports,
                    alerts_requested,
                    support_bundles_requested,
                    facts,
                    problems,
                },
            indent,
            sitrep_id,
            colored,
        } = self;

        let styles = display::Styles::new(colored);
        let heading = styles.heading();
        let this_sitrep = move |s| {
            styles.annotation_if(Some(s) == sitrep_id, " <-- this sitrep")
        };

        writeln!(
            f,
            "{:>indent$}{}",
            if indent > 0 { BULLET } else { "" },
            heading.style(format_args!("case {id}")),
        )?;
        metadata
            .display_multiline(indent, sitrep_id)
            .colored(colored)
            .fmt(f)?;

        if !ereports.is_empty() {
            writeln!(f, "{:>indent$}{}:", "", heading.style("ereports"))?;

            let indent = indent + 2;
            for CaseEreport { id, ereport, assigned_sitrep_id, comment } in
                ereports
            {
                const CLASS: &str = "class:";
                const REPORTED_BY: &str = "reported by:";
                const ADDED_IN: &str = "added in:";
                const ASSIGNMENT_ID: &str = "assignment ID:";

                const WIDTH: usize = const_max_len(&[
                    CLASS,
                    REPORTED_BY,
                    ADDED_IN,
                    ASSIGNMENT_ID,
                ]);

                let pn = styles
                    .or_missing(ereport.part_number.as_deref(), "<UNKNOWN>");
                let sn = styles
                    .or_missing(ereport.serial_number.as_deref(), "<UNKNOWN>");
                writeln!(
                    f,
                    "{BULLET:>indent$}{}",
                    heading.style(format_args!("ereport {}", ereport.id))
                )?;
                display::Comment::from(comment)
                    .indent(indent)
                    .colored(colored)
                    .fmt(f)?;
                writeln!(
                    f,
                    "{:>indent$}{} {}",
                    "",
                    styles.label(CLASS, WIDTH),
                    styles.or_missing(ereport.class.as_deref(), "<NONE>")
                )?;
                writeln!(
                    f,
                    "{:>indent$}{} {pn:>11}:{sn:<11} ({})",
                    "",
                    styles.label(REPORTED_BY, WIDTH),
                    ereport.reporter
                )?;
                writeln!(
                    f,
                    "{:>indent$}{} {assigned_sitrep_id}{}",
                    "",
                    styles.label(ADDED_IN, WIDTH),
                    this_sitrep(*assigned_sitrep_id)
                )?;
                writeln!(
                    f,
                    "{:>indent$}{} {id}",
                    "",
                    styles.label(ASSIGNMENT_ID, WIDTH)
                )?;
            }
        }

        if !facts.is_empty() {
            writeln!(f, "{:>indent$}{}:", "", heading.style("facts"))?;

            let indent = indent + 2;
            for fact in facts.iter() {
                fact.display_multiline(indent, sitrep_id)
                    .colored(colored)
                    .fmt(f)?;
            }
        }

        if !alerts_requested.is_empty() {
            writeln!(
                f,
                "{:>indent$}{}:",
                "",
                heading.style("alerts requested")
            )?;

            let indent = indent + 2;
            for AlertRequest {
                id,
                class,
                version,
                payload: _,
                requested_sitrep_id,
                comment,
            } in alerts_requested.iter()
            {
                const CLASS: &str = "class:";
                const REQUESTED_IN: &str = "requested in:";

                const WIDTH: usize = const_max_len(&[CLASS, REQUESTED_IN]);

                writeln!(
                    f,
                    "{BULLET:>indent$}{}",
                    heading.style(format_args!("alert {id}"))
                )?;
                display::Comment::from(comment)
                    .indent(indent)
                    .colored(colored)
                    .fmt(f)?;
                writeln!(
                    f,
                    "{:>indent$}{} {class}, v{version}",
                    "",
                    styles.label(CLASS, WIDTH),
                )?;
                writeln!(
                    f,
                    "{:>indent$}{} {requested_sitrep_id}{}",
                    "",
                    styles.label(REQUESTED_IN, WIDTH),
                    this_sitrep(*requested_sitrep_id)
                )?;
            }
        }

        if !support_bundles_requested.is_empty() {
            writeln!(
                f,
                "{:>indent$}{}:",
                "",
                heading.style("support bundles requested")
            )?;

            let indent = indent + 2;
            for SupportBundleRequest {
                id,
                requested_sitrep_id,
                data_selection,
                comment,
            } in support_bundles_requested.iter()
            {
                const REQUESTED_IN: &str = "requested in:";
                const DATA: &str = "data:";
                const WIDTH: usize = const_max_len(&[REQUESTED_IN, DATA]);

                writeln!(
                    f,
                    "{BULLET:>indent$}{}",
                    heading.style(format_args!("bundle {id}"))
                )?;
                display::Comment::from(comment)
                    .indent(indent)
                    .colored(colored)
                    .fmt(f)?;
                writeln!(
                    f,
                    "{:>indent$}{} {requested_sitrep_id}{}",
                    "",
                    styles.label(REQUESTED_IN, WIDTH),
                    this_sitrep(*requested_sitrep_id)
                )?;
                writeln!(f, "{:>indent$}{}", "", styles.label(DATA, 0))?;
                writeln!(f, "{}", data_selection.display(indent + 2))?;
            }
        }

        if !problems.is_empty() {
            writeln!(
                f,
                "{:>indent$}{}:",
                "",
                heading.style("active problems")
            )?;

            let indent = indent + 2;
            for CaseProblem { problem_id, linked_sitrep_id, comment } in
                problems.iter()
            {
                const ADDED_IN: &str = "added in:";
                const WIDTH: usize = const_max_len(&[ADDED_IN]);

                writeln!(
                    f,
                    "{BULLET:>indent$}{}",
                    heading.style(format_args!("problem {problem_id}"))
                )?;
                display::Comment::from(comment)
                    .indent(indent)
                    .colored(colored)
                    .fmt(f)?;
                writeln!(
                    f,
                    "{:>indent$}{} {linked_sitrep_id}{}",
                    "",
                    styles.label(ADDED_IN, WIDTH),
                    this_sitrep(*linked_sitrep_id)
                )?;
            }
        }

        Ok(())
    }
}

/// Displays a summary of a [`Case`] as a single line, as returned by
/// [`Case::display_summary`].
///
/// The line begins with a two-column gutter describing whether the case had
/// interesting activity in the sitrep being displayed, if this displayer was
/// provided with a sitrep UUID. This format is summarized by [`Self::legend`].
///
/// - column 1 is `+` if the case was opened in the current sitrep, `*` if it
///   was opened in an earlier sitrep and data was added in the current sitrep,
///   and blank otherwise,
/// - column 2 is `-` if the case was closed in this sitrep, and blank
///   otherwise. If a case opened in this sitrep was closed in some earlier
///   sitrep, the record is inconsistent (how could the case close before it
///   opened?), and column 2 is `!`.
///
/// Alongside the `open`/`closed` state that follows, the gutter allows the
/// reader to determine whether these states are the result of changes in the
/// current sitrep, or if the case was already in that state in an earlier
/// sitrep. If no sitrep ID was provided, the gutter is blank, so that rows
/// remain aligned.
#[must_use = "this struct does nothing unless displayed"]
pub struct DisplayCaseSummary<'a> {
    case: &'a Case,
    indent: usize,
    sitrep_id: Option<SitrepUuid>,
    colored: bool,
}

impl DisplayCaseSummary<'_> {
    /// Explains the gutter markers in [`DisplayCaseSummary`]'s output. Callers
    /// displaying a list of case summaries should print this legend.
    pub fn legend() -> DisplayCaseSummaryLegend {
        DisplayCaseSummaryLegend { colored: false }
    }

    /// If `colored` is true, style the output with ANSI terminal colors.
    pub fn colored(mut self, colored: bool) -> Self {
        self.colored = colored;
        self
    }

    const GUTTER_ADDED: char = '+';
    const GUTTER_MODIFIED: char = '*';
    const GUTTER_REMOVED: char = '-';
}

pub struct DisplayCaseSummaryLegend {
    colored: bool,
}

impl DisplayCaseSummaryLegend {
    pub fn colored(self, colored: bool) -> Self {
        Self { colored }
    }
}

impl fmt::Display for DisplayCaseSummaryLegend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let &Self { colored } = self;
        let styles = display::Styles::new(colored);
        write!(
            f,
            "activity in this sitrep: {} opened, {} changed, {} closed",
            styles.added().style(DisplayCaseSummary::GUTTER_ADDED),
            styles.modified().style(DisplayCaseSummary::GUTTER_MODIFIED),
            styles.removed().style(DisplayCaseSummary::GUTTER_REMOVED),
        )
    }
}

impl fmt::Display for DisplayCaseSummary<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let &Self { case, indent, sitrep_id, colored } = self;
        let Case {
            id,
            metadata,
            ereports: _,
            alerts_requested: _,
            support_bundles_requested: _,
            facts: _,
            problems: _,
        } = case;
        let Metadata { created_sitrep_id, closed_sitrep_id, de, comment } =
            metadata;

        let styles = display::Styles::new(colored);

        const OPEN: &str = "open";
        const CLOSED: &str = "closed";
        const STATE_WIDTH: usize = display::const_max_len(&[OPEN, CLOSED]);

        let state = if closed_sitrep_id.is_some() { CLOSED } else { OPEN };
        let (opened, closed) = match sitrep_id {
            Some(this_sitrep) => {
                let opened_here = *created_sitrep_id == this_sitrep;
                let opened = if opened_here {
                    styles.added().style(Self::GUTTER_ADDED)
                } else if case.additions_in_sitrep(this_sitrep) {
                    styles.modified().style(Self::GUTTER_MODIFIED)
                } else {
                    styles.normal().style(' ')
                };
                let closed = match closed_sitrep_id {
                    Some(s) if *s == this_sitrep => {
                        styles.removed().style(Self::GUTTER_REMOVED)
                    }
                    Some(_) if opened_here => styles.warning().style('!'),
                    _ => styles.normal().style(' '),
                };
                (opened, closed)
            }
            None => (styles.normal().style(' '), styles.normal().style(' ')),
        };
        let de_width = DiagnosisEngineKind::MAX_WIDTH;

        let comment = display::Comment::new(comment, 0)
            .wrap_mode(display::WrapMode::SingleLine)
            .colored(colored);
        write!(
            f,
            "{:>indent$}{opened}{closed} {} \
             {state:<STATE_WIDTH$} {de:<de_width$} {comment}",
            "",
            styles.heading().style(format_args!("case {id}")),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fm::DiagnosisEngineKind;
    use crate::fm::ereport::EreportFilters;
    use crate::fm::{DiskFact, ZpoolUnhealthyFactPayload};
    use crate::inventory::{SpType, ZpoolHealth};
    use crate::support_bundle::BundleDataSelection;
    use ereport_types::{Ena, EreportId};
    use omicron_uuid_kinds::{
        AlertUuid, CaseUuid, CollectionUuid, EreporterRestartUuid, FactUuid,
        OmicronZoneUuid, PhysicalDiskUuid, ProblemUuid, SitrepUuid,
        SupportBundleUuid, ZpoolUuid,
    };
    use std::str::FromStr;
    use std::sync::Arc;

    fn example_case() -> (Case, SitrepUuid) {
        // Create UUIDs for the case
        let case_id =
            CaseUuid::from_str("b0d36461-e3e7-4a53-9162-82414fb30088").unwrap();
        let created_sitrep_id =
            SitrepUuid::from_str("ea7affb0-36eb-4a9a-b9bd-22e00f1bcc04")
                .unwrap();
        let closed_sitrep_id =
            SitrepUuid::from_str("bba63ba7-bf6b-45f4-b241-d13ccd07fe1c")
                .unwrap();
        let restart_id = EreporterRestartUuid::from_str(
            "b633f40b-38c7-41f6-813b-61fc76381696",
        )
        .unwrap();
        let collector_id =
            OmicronZoneUuid::from_str("34f3b79e-168a-46f8-be49-4fe8eaff539a")
                .unwrap();
        let alert1_id =
            AlertUuid::from_str("7fe8c664-5b91-490f-b71b-b19e6e07ff3a")
                .unwrap();
        let alert2_id =
            AlertUuid::from_str("8a6f88ef-c436-44a9-b4cb-cae91d7306c9")
                .unwrap();
        let bundle1_id =
            SupportBundleUuid::from_str("d1a2b3c4-e5f6-7890-abcd-ef1234567890")
                .unwrap();
        let bundle2_id =
            SupportBundleUuid::from_str("a9b8c7d6-e5f4-3210-fedc-ba0987654321")
                .unwrap();

        // Create some ereports
        let mut ereports = IdOrdMap::new();
        let time_collected = chrono::DateTime::<chrono::Utc>::MIN_UTC;

        let ereport1 = CaseEreport {
            id: CaseEreportUuid::from_str(
                "89f650fd-c67c-4dcc-9acc-0ce02d43a62b",
            )
            .unwrap(),
            ereport: Arc::new(Ereport {
                id: EreportId { restart_id, ena: Ena::from(2u64) },
                time_collected,
                collector_id,
                data: crate::fm::ereport::EreportData {
                    serial_number: Some("BRM6900420".to_string()),
                    part_number: Some("913-0000037".to_string()),
                    class: Some("hw.pwr.remove.psu".to_string()),
                    report: serde_json::json!({}),
                },
                reporter: crate::fm::ereport::Reporter::Sp {
                    sp_type: SpType::Power,
                    slot: 0,
                },
                marked_seen_in: Some(created_sitrep_id),
            }),
            assigned_sitrep_id: created_sitrep_id,
            comment: "PSU removed".to_string(),
        };
        ereports.insert_unique(ereport1).unwrap();

        let ereport2 = CaseEreport {
            id: CaseEreportUuid::from_str(
                "7b923ffc-f5fc-4001-acf4-1224dad7d3ef",
            )
            .unwrap(),
            ereport: Arc::new(Ereport {
                id: EreportId { restart_id, ena: Ena::from(3u64) },
                time_collected,
                collector_id,
                data: crate::fm::ereport::EreportData {
                    serial_number: Some("BRM6900420".to_string()),
                    part_number: Some("913-0000037".to_string()),
                    class: Some("hw.pwr.insert.psu".to_string()),
                    report: serde_json::json!({}),
                },
                reporter: crate::fm::ereport::Reporter::Sp {
                    sp_type: SpType::Power,
                    slot: 0,
                },
                marked_seen_in: None,
            }),
            assigned_sitrep_id: closed_sitrep_id,
            comment: "PSU inserted, closing this case".to_string(),
        };
        ereports.insert_unique(ereport2).unwrap();

        let mut alerts_requested = IdOrdMap::new();
        alerts_requested
            .insert_unique(AlertRequest {
                id: alert1_id,
                class: AlertClass::TestFoo,
                version: 0,
                payload: serde_json::json!({}),
                requested_sitrep_id: created_sitrep_id,
                comment: "power shelf rectifier removed".to_string(),
            })
            .unwrap();
        alerts_requested
            .insert_unique(AlertRequest {
                id: alert2_id,
                class: AlertClass::TestFooBar,
                version: 0,
                payload: serde_json::json!({}),
                requested_sitrep_id: closed_sitrep_id,
                comment: String::new(),
            })
            .unwrap();

        let bundle1_data = BundleDataSelection::new()
            .with_reconfigurator()
            .with_sp_dumps()
            .with_all_sleds()
            .with_ereports(EreportFilters::new().with_classes(["hw.pwr.*"]));

        let mut support_bundles_requested = IdOrdMap::new();
        support_bundles_requested
            .insert_unique(SupportBundleRequest {
                id: bundle1_id,
                requested_sitrep_id: created_sitrep_id,
                data_selection: bundle1_data,
                comment: "test support bundle".to_string(),
            })
            .unwrap();
        support_bundles_requested
            .insert_unique(SupportBundleRequest {
                id: bundle2_id,
                requested_sitrep_id: closed_sitrep_id,
                data_selection: BundleDataSelection::all(),
                comment: String::new(),
            })
            .unwrap();

        let mut facts = IdOrdMap::new();
        facts
            .insert_unique(Fact {
                metadata: FactMetadata {
                    id: FactUuid::from_str(
                        "f00f00f0-0f00-4f00-8f00-f00f00f00f00",
                    )
                    .unwrap(),
                    created_sitrep_id,
                    comment: "made-up fact for display test".to_string(),
                },
                payload: FactPayload::PhysicalDisk(DiskFact::ZpoolUnhealthy(
                    ZpoolUnhealthyFactPayload {
                        physical_disk_id: PhysicalDiskUuid::from_str(
                            "d15d15d1-5d15-4d15-8d15-d15d15d15d15",
                        )
                        .unwrap(),
                        zpool_id: ZpoolUuid::from_str(
                            "200100f0-0100-4f00-8f00-f00f00f00f00",
                        )
                        .unwrap(),
                        last_seen_health: ZpoolHealth::Degraded,
                        observed_in_inv: CollectionUuid::from_str(
                            "c0110011-c011-4011-8011-c0110011c011",
                        )
                        .unwrap(),
                        time_observed: chrono::DateTime::<chrono::Utc>::MIN_UTC,
                    },
                )),
            })
            .unwrap();

        let mut problems = IdOrdMap::new();
        problems
            .insert_unique(CaseProblem {
                problem_id: ProblemUuid::from_str(
                    "a0b1e3a0-4c0d-4e5f-8a9b-0c1d2e3f4a5b",
                )
                .unwrap(),
                linked_sitrep_id: closed_sitrep_id,
                comment: "the rectifier is missing".to_string(),
            })
            .unwrap();

        // Create the case
        let case = Case {
            id: case_id,
            metadata: Metadata {
                created_sitrep_id,
                closed_sitrep_id: Some(closed_sitrep_id),
                de: DiagnosisEngineKind::PowerShelf,
                comment: "Power shelf rectifier added and removed here :-)"
                    .to_string(),
            },
            ereports,
            alerts_requested,
            support_bundles_requested,
            facts,
            problems,
        };

        (case, closed_sitrep_id)
    }

    #[test]
    fn test_case_display() {
        let (case, closed_sitrep_id) = example_case();

        eprintln!("example case display:");
        eprintln!("=====================\n");
        eprintln!("{case}");

        eprintln!("example case display (indented by 4):");
        eprintln!("=====================================\n");
        eprintln!("{}", case.display_indented(4, Some(closed_sitrep_id)));
    }

    /// Checks that the colored and non-colored display outputs are the same
    /// string, see: [`display::test_utils::check_colored_display`].
    #[test]
    fn test_case_display_colored() {
        let (case, closed_sitrep_id) = example_case();
        eprintln!("example case display (colored):");
        eprintln!("===============================\n");
        display::test_utils::check_colored_display(|colored| {
            case.display_indented(4, Some(closed_sitrep_id)).colored(colored)
        });
    }
}
