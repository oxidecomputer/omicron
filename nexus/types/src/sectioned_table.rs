// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for tables with builtin sections.
//!
//! This could live in its own crate (within omicron, or even on crates.io),
//! but is here for now.

use std::collections::HashSet;
use std::iter;

use tabled::builder::Builder;
use tabled::grid::config::Border;
use tabled::settings::object::Columns;
use tabled::settings::object::Object;
use tabled::settings::object::Rows;
use tabled::settings::span::ColumnSpan;
use tabled::settings::Modify;
use tabled::settings::Padding;
use tabled::settings::Style;
use tabled::Table;

/// A sectioned table.
///
/// A sectioned table allows sections and subsections to be defined, with each
/// section having a title and a list of rows in that section. The section
/// headers and other rows can break standard table conventions.
///
/// There are two kinds of special rows:
///
/// 1. Headings: rows that span all columns.
/// 2. Spanned rows: also rows that span all columns, but not as headings.
///
/// This builder does not currently automatically indent sections or records --
/// that can be done in the future, though it has to be done with some care.
#[derive(Debug)]
pub(crate) struct StBuilder {
    builder: Builder,
    // Rows that are marked off with ---- on both sides.
    header_rows: Vec<usize>,
    // Heading rows that span all columns.
    headings: Vec<(HeadingSpacing, usize)>,
    // Other rows that span all columns.
    spanned_rows: Vec<usize>,
}

impl StBuilder {
    pub(crate) fn new() -> Self {
        let builder = Builder::new();

        Self {
            builder,
            header_rows: Vec::new(),
            headings: Vec::new(),
            spanned_rows: Vec::new(),
        }
    }

    /// Adds a header row to the table.
    ///
    /// This row contains column titles, along with *two* initial columns of
    /// padding. The border will extend to the first column but not the second
    /// one.
    pub(crate) fn push_header_row(&mut self, row: Vec<String>) {
        self.header_rows.push(self.builder.count_records());
        self.push_record(row);
    }

    /// Adds a record to the table.
    pub(crate) fn push_record(&mut self, row: Vec<String>) {
        self.builder.push_record(row);
    }

    /// Makes a new section of the table.
    ///
    /// This section will not be added to the table unless at least one row is
    /// added to it, either directly or via nested sections.
    pub(crate) fn make_section(
        &mut self,
        spacing: SectionSpacing,
        heading: String,
        cb: impl FnOnce(&mut StSectionBuilder),
    ) {
        let mut section = StSectionBuilder::from_builder(
            self,
            spacing.resolve(self.headings.is_empty()),
            heading,
        );
        cb(&mut section);
        section.finish_with_root(self);
    }

    /// Does the final build to produce a [`Table`].
    pub(crate) fn build(mut self) -> Table {
        // Insert a column between 0 and 1 to enable header borders to be
        // properly aligned with the rest of the text.
        self.builder.insert_column(
            1,
            iter::repeat("").take(self.builder.count_records()),
        );

        let mut table = self.builder.build();
        table
            .with(Style::blank())
            .with(
                // Columns 0 and 1 (indent/gutter) should not have any border
                // and padding.
                Modify::new(Columns::new(0..=1))
                    .with(Border::empty())
                    .with(Padding::zero()),
            )
            .with(
                Modify::new(Columns::single(2))
                    // Column 2 (first column of actual data) should not have
                    // left padding.
                    .with(Padding::new(0, 1, 0, 0)),
            )
            .with(
                Modify::new(Columns::last())
                    // Rightmost column should have no border and padding.
                    .with(Border::empty())
                    .with(Padding::zero()),
            );
        apply_normal_row_settings(
            &mut table,
            self.header_rows
                .iter()
                .copied()
                .chain(self.headings.iter().map(|(_, i)| *i))
                .chain(self.spanned_rows.iter().copied())
                .collect(),
        );
        apply_header_row_settings(&mut table, &self.header_rows);
        apply_heading_settings(&mut table, &self.headings);
        apply_spanned_row_settings(&mut table, &self.spanned_rows);

        table
    }
}

/// A part of a sectioned table.
///
/// Created by [`StBuilder::make_section`] or
/// [`StNestedBuilder::make_subsection`].
#[derive(Debug)]
pub(crate) struct StSectionBuilder {
    start_index: usize,
    spacing: HeadingSpacing,
    heading: String,
    rows: Vec<Vec<String>>,
    // Indexes for special rows, stored as absolute indexes wrt the overall
    // zone table (i.e. start_index + 1 + index in rows).
    nested_headings: Vec<(HeadingSpacing, usize)>,
    spanned_rows: Vec<usize>,
}

impl StSectionBuilder {
    fn from_builder(
        builder: &StBuilder,
        spacing: HeadingSpacing,
        heading: String,
    ) -> Self {
        let start_index = builder.builder.count_records();
        Self {
            start_index,
            spacing,
            heading,
            rows: Vec::new(),
            nested_headings: Vec::new(),
            spanned_rows: Vec::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(crate) fn push_record(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }

    pub(crate) fn push_spanned_row(&mut self, row: String) {
        self.spanned_rows.push(self.next_row());
        self.rows.push(vec![row]);
    }

    pub(crate) fn push_nested_heading(
        &mut self,
        spacing: SectionSpacing,
        heading: String,
    ) {
        self.nested_headings.push((
            spacing.resolve(self.nested_headings.is_empty()),
            self.next_row(),
        ));
        self.rows.push(vec![heading]);
    }

    /// Makes a new subsection of this section.
    ///
    /// This subsection will not be added to the table unless at least one row
    /// is added to it, either directly or via nested sections.
    pub(crate) fn make_subsection(
        &mut self,
        spacing: SectionSpacing,
        heading: String,
        cb: impl FnOnce(&mut Self),
    ) {
        let mut subsection = Self {
            start_index: self.next_row(),
            spacing: spacing.resolve(self.nested_headings.is_empty()),
            heading,
            rows: Vec::new(),
            nested_headings: Vec::new(),
            spanned_rows: Vec::new(),
        };
        cb(&mut subsection);
        subsection.finish_with_parent(self);
    }

    fn next_row(&self) -> usize {
        // +1 to account for the heading row.
        self.start_index + 1 + self.rows.len()
    }

    fn finish_with_root(self, root: &mut StBuilder) {
        if !self.rows.is_empty() {
            // Push all the indexes.
            root.headings.push((self.spacing, self.start_index));
            root.headings.extend(self.nested_headings);
            root.spanned_rows.extend(self.spanned_rows);

            // Push all the rows.
            root.push_record(vec![self.heading]);
            for row in self.rows {
                root.push_record(row);
            }
        }
    }

    fn finish_with_parent(self, parent: &mut StSectionBuilder) {
        if !self.rows.is_empty() {
            // Push all the indexes.
            parent.nested_headings.push((self.spacing, self.start_index));
            parent.nested_headings.extend(self.nested_headings);
            parent.spanned_rows.extend(self.spanned_rows);

            // Push all the rows.
            parent.rows.push(vec![self.heading]);
            parent.rows.extend(self.rows);
        }
    }
}

/// Spacing for sections.
#[derive(Copy, Clone, Debug)]
pub(crate) enum SectionSpacing {
    /// Always add a line of spacing above the section heading.
    ///
    /// There will always be one row of padding above the heading.
    Always,

    /// Only add a line of spacing if this isn't the first heading in the
    /// series.
    IfNotFirst,

    /// Do not add a line of spacing above the heading.
    Never,
}

impl SectionSpacing {
    fn resolve(self, is_empty: bool) -> HeadingSpacing {
        match (self, is_empty) {
            (SectionSpacing::Always, _) => HeadingSpacing::Yes,
            (SectionSpacing::IfNotFirst, true) => HeadingSpacing::No,
            (SectionSpacing::IfNotFirst, false) => HeadingSpacing::Yes,
            (SectionSpacing::Never, _) => HeadingSpacing::No,
        }
    }
}

/// Spacing for headings -- a resolved form of [`SectionSpacing`].
#[derive(Copy, Clone, Debug)]
enum HeadingSpacing {
    /// Add a line of padding above the heading.
    Yes,

    /// Do not add a line of padding above the heading.
    No,
}

fn apply_normal_row_settings(table: &mut Table, special_rows: HashSet<usize>) {
    for row in 0..table.count_rows() {
        if special_rows.contains(&row) {
            continue;
        }

        table.with(
            Modify::new((row, 0))
                // Adjust the first column to span 2 (the extra indent).
                .with(ColumnSpan::new(2)),
        );
    }
}

fn apply_header_row_settings(table: &mut Table, header_rows: &[usize]) {
    for &hr in header_rows {
        table.with(
            Modify::new(Rows::single(hr).intersect(Columns::new(1..)))
                // Column 1 onwards (everything after the initial indent) have
                // borders.
                .with(Border::new(
                    // top/bottom
                    Some('-'),
                    Some('-'),
                    // no left/right
                    None,
                    None,
                    // corners
                    Some('-'),
                    Some('-'),
                    Some('-'),
                    Some('-'),
                )),
        );
    }
}

fn apply_heading_settings(
    table: &mut Table,
    headings: &[(HeadingSpacing, usize)],
) {
    for &(kind, h) in headings {
        let padding = match kind {
            HeadingSpacing::Yes => Padding::new(0, 0, 1, 0),
            HeadingSpacing::No => Padding::new(0, 0, 0, 0),
        };

        table.with(
            Modify::new((h, 0))
                // Adjust each heading row to span the whole row.
                .with(ColumnSpan::max())
                .with(padding),
        );
    }
}

fn apply_spanned_row_settings(table: &mut Table, spanned_rows: &[usize]) {
    for &sr in spanned_rows {
        table.with(
            Modify::new((sr, 0))
                // Adjust each spanned row to span the whole row.
                .with(ColumnSpan::max()),
        );
    }
}
