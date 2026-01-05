// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types helpful for rendering blueprints.

use daft::Leaf;
use omicron_common::api::external::Generation;
use std::fmt;

pub mod constants {
    pub(super) const ADDED_PREFIX: char = '+';
    pub(super) const REMOVED_PREFIX: char = '-';
    pub(super) const MODIFIED_PREFIX: char = '*';
    pub(super) const UNCHANGED_PREFIX: char = ' ';

    #[allow(unused)]
    pub(super) const SUB_NOT_LAST: &str = "├─";
    pub(super) const SUB_LAST: &str = "└─";

    pub const ARROW: &str = "->";
    pub const WILL_REMOVE_MUPDATE_OVERRIDE: &str =
        "will remove mupdate override";
    pub const WOULD_HAVE_REMOVED_MUPDATE_OVERRIDE: &str =
        "would have removed mupdate override";
    pub const COCKROACHDB_HEADING: &str = "COCKROACHDB SETTINGS";
    pub const COCKROACHDB_FINGERPRINT: &str = "state fingerprint";
    pub const COCKROACHDB_PRESERVE_DOWNGRADE: &str =
        "cluster.preserve_downgrade_option";
    pub const METADATA_HEADING: &str = "METADATA";
    pub const CLICKHOUSE_CLUSTER_CONFIG_HEADING: &str =
        "CLICKHOUSE CLUSTER CONFIG";
    pub const CLICKHOUSE_MAX_USED_SERVER_ID: &str = "max used server id";
    pub const CLICKHOUSE_MAX_USED_KEEPER_ID: &str = "max used keeper id";
    pub const CLICKHOUSE_CLUSTER_NAME: &str = "cluster name";
    pub const CLICKHOUSE_CLUSTER_SECRET: &str = "cluster secret";
    pub const CLICKHOUSE_HIGHEST_SEEN_KEEPER_LEADER_COMMITTED_LOG_INDEX: &str =
        "highest seen keeper leader committed log index";
    pub const OXIMETER_HEADING: &str = "OXIMETER SETTINGS";
    pub const OXIMETER_READ_FROM: &str = "read from";
    pub const CREATED_BY: &str = "created by";
    pub const CREATED_AT: &str = "created at";
    pub const INTERNAL_DNS_VERSION: &str = "internal DNS version";
    pub const EXTERNAL_DNS_VERSION: &str = "external DNS version";
    // Keep this a bit short to not make the key column too wide.
    pub const TARGET_RELEASE_MIN_GEN: &str = "target release min gen";
    pub const NEXUS_GENERATION: &str = "nexus gen";
    pub const COMMENT: &str = "comment";

    pub const UNCHANGED_PARENS: &str = "(unchanged)";
    pub const NONE_PARENS: &str = "(none)";
    pub const NOT_PRESENT_IN_COLLECTION_PARENS: &str =
        "(not present in collection)";
    pub const INVALID_VALUE_PARENS: &str = "(invalid value)";
    pub const GENERATION: &str = "generation";
    pub const STATE: &str = "state";
    pub const CONFIG_GENERATION: &str = "config generation";
    pub const SUBNET: &str = "subnet";
}
use constants::*;
use std::fmt::Display;

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

impl BpGeneration {
    // Used when there isn't a corresponding generation
    pub fn unknown() -> Self {
        BpGeneration::Diff { before: None, after: None }
    }
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
                write!(f, "unknown generation")
            }
        }
    }
}

#[derive(Debug)]
pub enum BpTableColumn {
    Value(String),
    Diff { before: String, after: String },
}

impl BpTableColumn {
    pub fn new<T: Display + Eq>(before: T, after: T) -> BpTableColumn {
        if before != after {
            BpTableColumn::Diff {
                before: before.to_string(),
                after: after.to_string(),
            }
        } else {
            BpTableColumn::Value(before.to_string())
        }
    }

    pub fn value(s: String) -> BpTableColumn {
        BpTableColumn::Value(s)
    }

    pub fn diff(before: String, after: String) -> BpTableColumn {
        BpTableColumn::Diff { before, after }
    }

    pub fn len(&self) -> usize {
        match self {
            BpTableColumn::Value(s) => s.len(),
            BpTableColumn::Diff { before, after } => {
                // Add 1 for the added/removed prefix and 1 for a space
                //
                // This will need to change if we change how we render diffs in
                // the `Display` impl for `BpTable`. However, putting it
                // here allows to minimize any extra horizontal spacing in case
                // other values for the same column are already longer than the
                // the before or after values + 2.
                usize::max(before.len(), after.len()) + 2
            }
        }
    }
}

/// A row in a [`BpTable`]
#[derive(Debug)]
pub struct BpTableRow {
    state: BpDiffState,
    columns: Vec<BpTableColumn>,
}

impl BpTableRow {
    pub fn new(state: BpDiffState, columns: Vec<BpTableColumn>) -> Self {
        BpTableRow { state, columns }
    }

    pub fn from_strings(state: BpDiffState, columns: Vec<String>) -> Self {
        BpTableRow {
            state,
            columns: columns.into_iter().map(BpTableColumn::Value).collect(),
        }
    }
}

/// Metadata about all instances of specific type of [`BpTable`],
/// such as omicron zones or physical disks.
pub trait BpTableSchema {
    fn table_name(&self) -> &'static str;
    fn column_names(&self) -> &'static [&'static str];
}

// Provide data specific to an instance of a [`BpTable`]
pub trait BpTableData {
    fn rows(&self, state: BpDiffState) -> impl Iterator<Item = BpTableRow>;
}

/// A table specific to a sled resource, such as a zone or disk.
pub struct BpTable {
    table_name: &'static str,
    column_names: &'static [&'static str],
    generation: Option<BpGeneration>,
    rows: Vec<BpTableRow>,
}

impl BpTable {
    pub fn new(
        schema: impl BpTableSchema,
        generation: Option<BpGeneration>,
        rows: Vec<BpTableRow>,
    ) -> Self {
        Self {
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
            assert_eq!(
                row.columns.len(),
                widths.len(),
                "for {}, number of header columns matches number of columns in row: {:?}",
                self.table_name,
                row
            );
            for (i, s) in row.columns.iter().enumerate() {
                widths[i] = usize::max(s.len(), widths[i]);
            }
        }

        widths
    }
}

const SUBTABLE_INDENT: usize = 4;
const COLUMN_GAP: usize = 3;

impl fmt::Display for BpTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let widths = self.column_widths();
        let mut total_width =
            widths.iter().fold(0, |acc, i| acc + i + COLUMN_GAP);
        total_width -= COLUMN_GAP;

        // Write the name of the subtable
        if let Some(generation) = self.generation {
            writeln!(
                f,
                "{:<SUBTABLE_INDENT$}{} {}:",
                "", self.table_name, generation
            )?;
        } else {
            writeln!(f, "{:<SUBTABLE_INDENT$}{}:", "", self.table_name,)?;
        }

        // Write the top header border
        writeln!(f, "{:<SUBTABLE_INDENT$}{:-<total_width$}", "", "")?;

        // Write the column names
        write!(f, "{:<SUBTABLE_INDENT$}", "")?;
        for (i, (column, width)) in
            self.column_names.iter().zip(&widths).enumerate()
        {
            if i == 0 {
                write!(f, "{column:<width$}")?;
            } else {
                write!(f, "{:<COLUMN_GAP$}{column:<width$}", "")?;
            }
        }

        // Write the bottom header border
        writeln!(f, "\n{:<SUBTABLE_INDENT$}{:-<total_width$}", "", "")?;

        // Write the rows
        for row in &self.rows {
            let prefix = row.state.prefix();
            write!(f, "{prefix:<SUBTABLE_INDENT$}")?;
            let mut multiline_row = false;
            for (i, (column, width)) in
                row.columns.iter().zip(&widths).enumerate()
            {
                let (column, needs_multiline) = match column {
                    BpTableColumn::Value(s) => (s.clone(), false),
                    BpTableColumn::Diff { before, .. } => {
                        // If we remove the prefix and space, we'll need to also
                        // modify `BpTableColumn::len` to reflect this.
                        (format!("{REMOVED_PREFIX} {before}"), true)
                    }
                };
                multiline_row |= needs_multiline;

                if i == 0 {
                    write!(f, "{column:<width$}")?;
                } else {
                    write!(f, "{:<COLUMN_GAP$}{column:<width$}", "")?;
                }
            }
            write!(f, "\n")?;

            // Do we need any multiline output?
            if multiline_row {
                write!(f, "{UNCHANGED_PREFIX:<SUBTABLE_INDENT$}")?;
                for (i, (column, width)) in
                    row.columns.iter().zip(&widths).enumerate()
                {
                    // Write the after columns or nothing
                    let column = match column {
                        BpTableColumn::Value(_) => "".to_string(),
                        BpTableColumn::Diff { after, .. } => {
                            // If we remove the prefix and space, we'll need to also
                            // modify `BpTableColumn::len` to reflect this.
                            format!("{ADDED_PREFIX} {after}")
                        }
                    };
                    if i == 0 {
                        let s = format!(" {SUB_LAST} {column}");
                        write!(f, "{s:<width$}")?;
                    } else {
                        write!(f, "{:<COLUMN_GAP$}{column:<width$}", "")?;
                    }
                }
                write!(f, "\n")?;
            }
        }

        Ok(())
    }
}

/// The [`BpTable`] schema for desired host phase 2 contents
pub struct BpHostPhase2TableSchema {}
impl BpTableSchema for BpHostPhase2TableSchema {
    fn table_name(&self) -> &'static str {
        "host phase 2 contents"
    }

    fn column_names(&self) -> &'static [&'static str] {
        &["slot", "boot image source"]
    }
}

/// The [`BpTable`] schema for physical disks
pub struct BpPhysicalDisksTableSchema {}
impl BpTableSchema for BpPhysicalDisksTableSchema {
    fn table_name(&self) -> &'static str {
        "physical disks"
    }

    fn column_names(&self) -> &'static [&'static str] {
        &["vendor", "model", "serial", "disposition"]
    }
}

/// The [`BpTable`] schema for datasets
pub struct BpDatasetsTableSchema {}
impl BpTableSchema for BpDatasetsTableSchema {
    fn table_name(&self) -> &'static str {
        "datasets"
    }

    fn column_names(&self) -> &'static [&'static str] {
        &[
            "dataset name",
            "dataset id",
            "disposition",
            "quota",
            "reservation",
            "compression",
        ]
    }
}

/// The [`BpTable`] schema for omicron zones
pub struct BpOmicronZonesTableSchema {}
impl BpTableSchema for BpOmicronZonesTableSchema {
    fn table_name(&self) -> &'static str {
        "omicron zones"
    }
    fn column_names(&self) -> &'static [&'static str] {
        &["zone type", "zone id", "image source", "disposition", "underlay IP"]
    }
}

/// The [`BpTable`] schema for clickhouse keepers
pub struct BpClickhouseKeepersTableSchema {}
impl BpTableSchema for BpClickhouseKeepersTableSchema {
    fn table_name(&self) -> &'static str {
        "clickhouse keepers"
    }

    fn column_names(&self) -> &'static [&'static str] {
        &["zone id", "keeper id"]
    }
}

/// The [`BpTable`] schema for clickhouse servers
pub struct BpClickhouseServersTableSchema {}
impl BpTableSchema for BpClickhouseServersTableSchema {
    fn table_name(&self) -> &'static str {
        "clickhouse servers"
    }

    fn column_names(&self) -> &'static [&'static str] {
        &["zone id", "server id"]
    }
}

/// The [`BpTable`] schema for pending MGS updates
pub struct BpPendingMgsUpdates {}
impl BpTableSchema for BpPendingMgsUpdates {
    fn table_name(&self) -> &'static str {
        "Pending MGS-managed updates (all baseboards)"
    }

    fn column_names(&self) -> &'static [&'static str] {
        &[
            "sp_type",
            "slot",
            "part_number",
            "serial_number",
            "artifact_hash",
            "artifact_version",
            "details",
        ]
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

    /// Create a new `KvPair` with option semantics, tracking unchanged and
    /// modified entries.
    pub fn new_option_leaf<K: Into<String>, V: fmt::Display + Eq>(
        key: K,
        leaf: Leaf<Option<V>>,
    ) -> KvPair {
        match (leaf.before, leaf.after) {
            (None, None) => {
                KvPair::new_unchanged(key, linear_table_unchanged(&NONE_PARENS))
            }
            (None, Some(after)) => KvPair::new(
                BpDiffState::Added,
                key,
                linear_table_modified(&NONE_PARENS, &after),
            ),
            (Some(before), None) => KvPair::new(
                BpDiffState::Removed,
                key,
                linear_table_modified(&before, &NONE_PARENS),
            ),
            (Some(before), Some(after)) if before == after => {
                KvPair::new_unchanged(key, linear_table_unchanged(&after))
            }
            (Some(before), Some(after)) => KvPair::new(
                BpDiffState::Modified,
                key,
                linear_table_modified(&before, &after),
            ),
        }
    }
}

// A top-to-bottom list of KV pairs, with or without a heading
#[derive(Debug)]
pub struct KvList {
    heading: Option<&'static str>,
    kv: Vec<KvPair>,
}

impl KvList {
    pub fn new_unchanged<S1: Into<String>, S2: Into<String>>(
        heading: Option<&'static str>,
        kv: Vec<(S1, S2)>,
    ) -> KvList {
        let kv =
            kv.into_iter().map(|(k, v)| KvPair::new_unchanged(k, v)).collect();
        KvList { heading, kv }
    }

    pub fn new(heading: Option<&'static str>, kv: Vec<KvPair>) -> KvList {
        KvList { heading, kv }
    }

    /// Compute the max width of the keys for alignment purposes
    fn max_key_width(&self) -> usize {
        self.kv.iter().fold(0, |acc, kv| usize::max(acc, kv.key.len()))
    }
}

impl fmt::Display for KvList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Write the heading
        if let Some(heading) = self.heading {
            writeln!(f, " {}:", heading)?;
        }

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
