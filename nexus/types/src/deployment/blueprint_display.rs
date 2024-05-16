// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types helpful for rendering [`Blueprints`].

use omicron_common::api::external::Generation;
use std::fmt;

pub mod constants {
    pub(super) const ADDED_PREFIX: char = '+';
    pub(super) const REMOVED_PREFIX: char = '-';
    pub(super) const MODIFIED_PREFIX: char = '*';
    pub(super) const UNCHANGED_PREFIX: char = ' ';

    pub const ARROW: &str = "->";
    pub const METADATA_HEADING: &str = "METADATA";
    pub const CREATED_BY: &str = "created by";
    pub const CREATED_AT: &str = "created at";
    pub const INTERNAL_DNS_VERSION: &str = "internal DNS version";
    pub const EXTERNAL_DNS_VERSION: &str = "external DNS version";
    pub const COMMENT: &str = "comment";

    pub const UNCHANGED_PARENS: &str = "(unchanged)";
    pub const NONE_PARENS: &str = "(none)";
    pub const NOT_PRESENT_IN_COLLECTION_PARENS: &str =
        "(not present in collection)";
}
use constants::*;

/// The state of a sled or resource (e.g. zone or physical disk) in this
/// blueprint, with regards to the parent blueprint
#[derive(Debug, Clone, Copy)]
pub enum BpDiffState {
    Unchanged,
    Removed,
    Modified,
    Added,
}

impl BpDiffState {
    pub fn prefix(&self) -> char {
        match self {
            BpDiffState::Unchanged => UNCHANGED_PREFIX,
            BpDiffState::Removed => REMOVED_PREFIX,
            BpDiffState::Modified => MODIFIED_PREFIX,
            BpDiffState::Added => ADDED_PREFIX,
        }
    }
}

impl fmt::Display for BpDiffState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            BpDiffState::Unchanged => "UNCHANGED",
            BpDiffState::Removed => "REMOVED",
            BpDiffState::Modified => "MODIFIED",
            BpDiffState::Added => "ADDED",
        };
        write!(f, "{s}")
    }
}

/// A wrapper aound generation numbers for blueprints or blueprint diffs
#[derive(Debug, Clone, Copy)]
pub enum BpGeneration {
    // A value in a single blueprint
    Value(Generation),

    // A diff between two blueprints
    Diff { before: Option<Generation>, after: Option<Generation> },
}

impl fmt::Display for BpGeneration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BpGeneration::Value(generation) => {
                write!(f, "at generation {generation}")
            }
            BpGeneration::Diff { before: None, after: Some(after) } => {
                write!(f, "at generation {after}")
            }
            BpGeneration::Diff { before: Some(before), after: None } => {
                write!(f, "from generation {before}")
            }
            BpGeneration::Diff { before: Some(before), after: Some(after) } => {
                if before == after {
                    write!(f, "at generation {after}")
                } else {
                    write!(f, "generation {before} -> {after}")
                }
            }
            BpGeneration::Diff { before: None, after: None } => {
                write!(f, "Error: unknown generation")
            }
        }
    }
}

/// A row in a [`BpSledSubtable`]
pub struct BpSledSubtableRow {
    state: BpDiffState,
    columns: Vec<String>,
}

impl BpSledSubtableRow {
    pub fn new(state: BpDiffState, columns: Vec<String>) -> Self {
        BpSledSubtableRow { state, columns }
    }
}

/// Metadata about all instances of specific type of [`BpSledSubtable`],
/// such as omicron zones or physical disks.
pub trait BpSledSubtableSchema {
    fn table_name(&self) -> &'static str;
    fn column_names(&self) -> &'static [&'static str];
}

// Provide data specific to an instance of a [`BpSledSubtable`]
pub trait BpSledSubtableData {
    fn bp_generation(&self) -> BpGeneration;
    fn rows(
        &self,
        state: BpDiffState,
    ) -> impl Iterator<Item = BpSledSubtableRow>;
}

/// A table specific to a sled resource, such as a zone or disk.
/// `BpSledSubtable`s are always nested under [`BpSledTable`]s.
pub struct BpSledSubtable {
    table_name: &'static str,
    column_names: &'static [&'static str],
    generation: BpGeneration,
    rows: Vec<BpSledSubtableRow>,
}

impl BpSledSubtable {
    pub fn new(
        schema: impl BpSledSubtableSchema,
        generation: BpGeneration,
        rows: Vec<BpSledSubtableRow>,
    ) -> BpSledSubtable {
        BpSledSubtable {
            table_name: schema.table_name(),
            column_names: schema.column_names(),
            generation,
            rows,
        }
    }

    /// Compute the max column widths based on the contents of `column_names`
    // and `rows`.
    fn column_widths(&self) -> Vec<usize> {
        let mut widths: Vec<usize> =
            self.column_names.iter().map(|s| s.len()).collect();

        for row in &self.rows {
            assert_eq!(row.columns.len(), widths.len());
            for (i, s) in row.columns.iter().enumerate() {
                widths[i] = usize::max(s.len(), widths[i]);
            }
        }

        widths
    }
}

const SUBTABLE_INDENT: usize = 4;
const COLUMN_GAP: usize = 3;

impl fmt::Display for BpSledSubtable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let widths = self.column_widths();
        let mut total_width =
            widths.iter().fold(0, |acc, i| acc + i + COLUMN_GAP);
        total_width -= COLUMN_GAP;

        // Write the name of the subtable
        writeln!(
            f,
            "{:<SUBTABLE_INDENT$}{} {}:",
            "", self.table_name, self.generation
        )?;

        // Write the top header border
        writeln!(f, "{:<SUBTABLE_INDENT$}{:-<total_width$}", "", "")?;

        // Write the column names
        write!(f, "{:<SUBTABLE_INDENT$}", "")?;
        for (i, (column, width)) in
            self.column_names.iter().zip(&widths).enumerate()
        {
            if i != 0 {
                write!(f, "{:<COLUMN_GAP$}{column:<width$}", "")?;
            } else {
                write!(f, "{column:<width$}")?;
            }
        }

        // Write the bottom header border
        writeln!(f, "\n{:<SUBTABLE_INDENT$}{:-<total_width$}", "", "")?;

        // Write the rows
        for row in &self.rows {
            let prefix = row.state.prefix();
            write!(f, "{prefix:<SUBTABLE_INDENT$}")?;
            for (i, (column, width)) in
                row.columns.iter().zip(&widths).enumerate()
            {
                if i != 0 {
                    write!(f, "{:<COLUMN_GAP$}{column:<width$}", "")?;
                } else {
                    write!(f, "{column:<width$}")?;
                }
            }
            write!(f, "\n")?;
        }

        Ok(())
    }
}

/// The [`BpSledSubtable`] schema for physical disks
pub struct BpPhysicalDisksSubtableSchema {}
impl BpSledSubtableSchema for BpPhysicalDisksSubtableSchema {
    fn table_name(&self) -> &'static str {
        "physical disks"
    }

    fn column_names(&self) -> &'static [&'static str] {
        &["vendor", "model", "serial"]
    }
}

/// The [`BpSledSubtable`] schema for omicron zones
pub struct BpOmicronZonesSubtableSchema {}
impl BpSledSubtableSchema for BpOmicronZonesSubtableSchema {
    fn table_name(&self) -> &'static str {
        "omicron zones"
    }
    fn column_names(&self) -> &'static [&'static str] {
        &["zone type", "zone id", "disposition", "underlay IP"]
    }
}

// An entry in a [`KvListWithHeading`]
#[derive(Debug)]
pub struct KvPair {
    state: BpDiffState,
    key: String,
    val: String,
}

impl KvPair {
    pub fn new_unchanged<S1: Into<String>, S2: Into<String>>(
        key: S1,
        val: S2,
    ) -> KvPair {
        KvPair {
            state: BpDiffState::Unchanged,
            key: key.into(),
            val: val.into(),
        }
    }

    pub fn new<S1: Into<String>, S2: Into<String>>(
        state: BpDiffState,
        key: S1,
        val: S2,
    ) -> KvPair {
        KvPair { state, key: key.into(), val: val.into() }
    }
}

// A top-to-bottom list of KV pairs with a heading
#[derive(Debug)]
pub struct KvListWithHeading {
    heading: &'static str,
    kv: Vec<KvPair>,
}

impl KvListWithHeading {
    pub fn new_unchanged<S1: Into<String>, S2: Into<String>>(
        heading: &'static str,
        kv: Vec<(S1, S2)>,
    ) -> KvListWithHeading {
        let kv =
            kv.into_iter().map(|(k, v)| KvPair::new_unchanged(k, v)).collect();
        KvListWithHeading { heading, kv }
    }

    pub fn new(heading: &'static str, kv: Vec<KvPair>) -> KvListWithHeading {
        KvListWithHeading { heading, kv }
    }

    /// Compute the max width of the keys for alignment purposes
    fn max_key_width(&self) -> usize {
        self.kv.iter().fold(0, |acc, kv| usize::max(acc, kv.key.len()))
    }
}

impl fmt::Display for KvListWithHeading {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Write the heading
        writeln!(f, " {}:", self.heading)?;

        // Write the rows
        let key_width = self.max_key_width() + 1;
        for kv in &self.kv {
            let prefix = kv.state.prefix();
            writeln!(
                f,
                "{prefix:<SUBTABLE_INDENT$}{::<key_width$}{:<COLUMN_GAP$}{}",
                kv.key, "", kv.val
            )?;
        }

        Ok(())
    }
}

pub fn linear_table_modified(
    before: &dyn fmt::Display,
    after: &dyn fmt::Display,
) -> String {
    format!("{before} {ARROW} {after}")
}

pub fn linear_table_unchanged(value: &dyn fmt::Display) -> String {
    format!("{value} {UNCHANGED_PARENS}")
}
