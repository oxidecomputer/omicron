// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::checks;
use crate::report::SlippyReport;
use crate::report::SlippyReportSortKey;
use core::fmt;
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::fm::EreportId;
use nexus_types::fm::Sitrep;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::CaseEreportUuid;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::FactUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use omicron_uuid_kinds::ZpoolUuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
    pub severity: Severity,
    pub kind: Kind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// A real violation that a diagnosis engine has already contained by
    /// closing the offending case.
    ///
    /// The invariant break happened while producing some ancestor sitrep;
    /// this sitrep merely records the evidence, and may itself be the
    /// faithful output of a correctly-functioning engine. The case leaves
    /// the sitrep once its remaining rendezvous work drains.
    Quarantined,
    /// A serious problem that means the sitrep is invalid: whatever
    /// produced or persisted this sitrep violated an invariant, and
    /// consumers may misbehave on it.
    Fatal,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Quarantined => write!(f, "QUARANTINED"),
            Severity::Fatal => write!(f, "FATAL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    /// Problems with the structure of the sitrep as a whole.
    Sitrep(SitrepKind),
    /// Problems attached to a specific case.
    Case { case_id: CaseUuid, kind: Box<CaseKind> },
}

impl Kind {
    pub fn display_component(&self) -> impl fmt::Display + '_ {
        enum Component<'a> {
            Sitrep,
            Case(&'a CaseUuid),
        }

        impl fmt::Display for Component<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    Component::Sitrep => write!(f, "sitrep"),
                    Component::Case(id) => write!(f, "case {id}"),
                }
            }
        }

        match self {
            Kind::Sitrep(_) => Component::Sitrep,
            Kind::Case { case_id, .. } => Component::Case(case_id),
        }
    }

    pub fn display_subkind(&self) -> impl fmt::Display + '_ {
        enum Subkind<'a> {
            Sitrep(&'a SitrepKind),
            Case(&'a CaseKind),
        }

        impl fmt::Display for Subkind<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    Subkind::Sitrep(kind) => write!(f, "{kind}"),
                    Subkind::Case(kind) => write!(f, "{kind}"),
                }
            }
        }

        match self {
            Kind::Sitrep(kind) => Subkind::Sitrep(kind),
            Kind::Case { kind, .. } => Subkind::Case(kind),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SitrepKind {
    /// A case references an ereport that is missing from the sitrep's
    /// `ereports_by_id` index.
    EreportMissingFromIndex { case_id: CaseUuid, ereport_id: EreportId },
    /// An `ereports_by_id` entry is referenced by no case.
    OrphanedIndexedEreport { ereport_id: EreportId },
    /// A case's copy of an ereport differs from the `ereports_by_id` entry
    /// with the same ereport ID.
    IndexedEreportContentMismatch { case_id: CaseUuid, ereport_id: EreportId },
    /// Two cases have ereport assignments with the same `CaseEreportUuid`.
    DuplicateCaseEreportId {
        id: CaseEreportUuid,
        case1: CaseUuid,
        case2: CaseUuid,
    },
    /// Two cases have facts with the same `FactUuid`.
    DuplicateFactId { fact_id: FactUuid, case1: CaseUuid, case2: CaseUuid },
    /// Two cases have alert requests with the same `AlertUuid`.
    DuplicateAlertId { alert_id: AlertUuid, case1: CaseUuid, case2: CaseUuid },
    /// Two cases have support bundle requests with the same
    /// `SupportBundleUuid`.
    DuplicateSupportBundleId {
        bundle_id: SupportBundleUuid,
        case1: CaseUuid,
        case2: CaseUuid,
    },
}

impl fmt::Display for SitrepKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SitrepKind::EreportMissingFromIndex { case_id, ereport_id } => {
                write!(
                    f,
                    "ereport {ereport_id} (assigned to case {case_id}) is \
                     missing from the sitrep's ereports_by_id index",
                )
            }
            SitrepKind::OrphanedIndexedEreport { ereport_id } => {
                write!(
                    f,
                    "ereports_by_id contains ereport {ereport_id}, which is \
                     not referenced by any case",
                )
            }
            SitrepKind::IndexedEreportContentMismatch {
                case_id,
                ereport_id,
            } => {
                write!(
                    f,
                    "case {case_id}'s copy of ereport {ereport_id} differs \
                     from the ereports_by_id entry with the same ID",
                )
            }
            SitrepKind::DuplicateCaseEreportId { id, case1, case2 } => {
                write!(
                    f,
                    "duplicate ereport assignment ID {id} \
                     (cases {case1} and {case2})",
                )
            }
            SitrepKind::DuplicateFactId { fact_id, case1, case2 } => {
                write!(
                    f,
                    "duplicate fact ID {fact_id} (cases {case1} and {case2})",
                )
            }
            SitrepKind::DuplicateAlertId { alert_id, case1, case2 } => {
                write!(
                    f,
                    "duplicate alert request ID {alert_id} \
                     (cases {case1} and {case2})",
                )
            }
            SitrepKind::DuplicateSupportBundleId {
                bundle_id,
                case1,
                case2,
            } => {
                write!(
                    f,
                    "duplicate support bundle request ID {bundle_id} \
                     (cases {case1} and {case2})",
                )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CaseKind {
    /// A case has a fact whose payload belongs to a different diagnosis
    /// engine than the case.
    ForeignFactPayload {
        fact_id: FactUuid,
        case_de: DiagnosisEngineKind,
        payload_de: DiagnosisEngineKind,
    },
    /// Problems specific to the physical-disk diagnosis engine's cases.
    PhysicalDisk(PhysicalDiskCaseKind),
}

impl fmt::Display for CaseKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CaseKind::ForeignFactPayload { fact_id, case_de, payload_de } => {
                write!(
                    f,
                    "fact {fact_id} belongs to the {payload_de} diagnosis \
                     engine, but its case belongs to {case_de}",
                )
            }
            CaseKind::PhysicalDisk(kind) => write!(f, "{kind}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PhysicalDiskCaseKind {
    /// An open physical-disk case has no facts, so the disk it concerns
    /// cannot be determined.
    OpenCaseWithNoFacts,
    /// Facts on one physical-disk case reference different physical disks.
    /// Each case is about exactly one disk.
    DisagreeingDisks {
        expected: PhysicalDiskUuid,
        found: PhysicalDiskUuid,
        fact_id: FactUuid,
    },
    /// Two open cases concern the same physical disk. Each disk has at most
    /// one open case.
    DuplicateOpenCaseForDisk {
        other_case: CaseUuid,
        physical_disk_id: PhysicalDiskUuid,
    },
    /// A `ZpoolUnhealthy` fact whose recorded health is `Online`, which is
    /// self-contradictory.
    ZpoolUnhealthyFactClaimsOnline { fact_id: FactUuid, zpool_id: ZpoolUuid },
}

impl fmt::Display for PhysicalDiskCaseKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PhysicalDiskCaseKind::OpenCaseWithNoFacts => {
                write!(
                    f,
                    "open physical-disk case has no facts, so the disk it \
                     concerns cannot be determined",
                )
            }
            PhysicalDiskCaseKind::DisagreeingDisks {
                expected,
                found,
                fact_id,
            } => {
                write!(
                    f,
                    "fact {fact_id} references physical disk {found}, but \
                     other facts on the case reference disk {expected} \
                     (each case is about exactly one disk)",
                )
            }
            PhysicalDiskCaseKind::DuplicateOpenCaseForDisk {
                other_case,
                physical_disk_id,
            } => {
                write!(
                    f,
                    "open case for physical disk {physical_disk_id} \
                     duplicates open case {other_case} \
                     (each disk has at most one open case)",
                )
            }
            PhysicalDiskCaseKind::ZpoolUnhealthyFactClaimsOnline {
                fact_id,
                zpool_id,
            } => {
                write!(
                    f,
                    "ZpoolUnhealthy fact {fact_id} records zpool {zpool_id} \
                     health as Online, which is self-contradictory",
                )
            }
        }
    }
}

impl Note {
    pub fn display(&self, sort_key: SlippyReportSortKey) -> NoteDisplay<'_> {
        NoteDisplay { note: self, sort_key }
    }
}

#[derive(Debug)]
pub struct NoteDisplay<'a> {
    note: &'a Note,
    sort_key: SlippyReportSortKey,
}

impl fmt::Display for NoteDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.sort_key {
            SlippyReportSortKey::Kind => {
                write!(
                    f,
                    "{}: {} note: {}",
                    self.note.kind.display_component(),
                    self.note.severity,
                    self.note.kind.display_subkind(),
                )
            }
            SlippyReportSortKey::Severity => {
                write!(
                    f,
                    "{} note: {}: {}",
                    self.note.severity,
                    self.note.kind.display_component(),
                    self.note.kind.display_subkind(),
                )
            }
        }
    }
}

#[derive(Debug)]
pub struct Slippy<'a> {
    sitrep: &'a Sitrep,
    notes: Vec<Note>,
}

impl<'a> Slippy<'a> {
    /// Check `sitrep` for internal inconsistencies.
    ///
    /// Callers that have `sitrep`'s parent available should pass it.
    pub fn new(sitrep: &'a Sitrep, parent: Option<&Sitrep>) -> Self {
        let mut slf = Self { sitrep, notes: Vec::new() };
        checks::perform_single_sitrep_checks(&mut slf);
        if let Some(_parent) = parent {
            // TODO: cross-sitrep continuity checks (fact immutability,
            // created_sitrep_id provenance, generation bumps) belong here.
        }
        slf
    }

    /// Check `sitrep` for internal inconsistencies, without any parent
    /// sitrep cross-checks.
    pub fn new_sitrep_only(sitrep: &'a Sitrep) -> Self {
        Self::new(sitrep, None)
    }

    pub fn sitrep(&self) -> &'a Sitrep {
        self.sitrep
    }

    pub(crate) fn push_sitrep_note(
        &mut self,
        severity: Severity,
        kind: SitrepKind,
    ) {
        self.notes.push(Note { severity, kind: Kind::Sitrep(kind) });
    }

    pub(crate) fn push_case_note(
        &mut self,
        case_id: CaseUuid,
        severity: Severity,
        kind: CaseKind,
    ) {
        self.notes.push(Note {
            severity,
            kind: Kind::Case { case_id, kind: Box::new(kind) },
        });
    }

    pub fn into_report(
        self,
        sort_key: SlippyReportSortKey,
    ) -> SlippyReport<'a> {
        SlippyReport::new(self.sitrep, self.notes, sort_key)
    }
}
