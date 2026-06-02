// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8Path;
use iddqd::IdOrdMap;
use miette::LabeledSpan;
use miette::NamedSource;
use miette::SourceSpan;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::ColumnOption;
use sqlparser::ast::CreateTable;
use sqlparser::ast::DataType;
use sqlparser::ast::Expr;
use sqlparser::ast::Spanned;
use sqlparser::ast::TableConstraint;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Location;
use sqlparser::tokenizer::Span;
use sqlparser::tokenizer::Token;
use sqlparser::tokenizer::TokenWithSpan;
use sqlparser::tokenizer::Tokenizer;
use sqlparser::tokenizer::Whitespace;
use std::collections::BTreeMap;
use std::sync::Arc;

const ANNOTATION_PREFIX: &str = "#!";
const FM_FACT_ANNOTATION: &str = "fm_fact";
const DE_ANNOTATION: &str = "de";
const VARIANT_ANNOTATION: &str = "name";

#[derive(Debug)]
pub struct Schema {
    #[allow(dead_code)]
    pub(crate) all_fact_tables: IdOrdMap<Arc<FactTable>>,
    pub(crate) fact_tables_by_de: BTreeMap<Arc<str>, IdOrdMap<Arc<FactTable>>>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct FactTable {
    create_stmt: CreateTable,
    // Rust ident for the diagnosis engine enum variant
    pub(crate) de_name: Arc<str>,
    // Rust ident for the fact variant enum variant
    pub(crate) fact_variant_name: String,
}

impl iddqd::IdOrdItem for FactTable {
    type Key<'a> = &'a sqlparser::ast::ObjectName;

    fn key(&self) -> Self::Key<'_> {
        &self.create_stmt.name
    }

    iddqd::id_upcast!();
}

#[derive(Debug, thiserror::Error, miette::Diagnostic)]
#[error("invalid schema")]
#[diagnostic()]
pub struct SchemaError {
    #[source_code]
    src: NamedSource<String>,
    #[related]
    errors: Vec<InnerSchemaError>,
}

/// An error encountered while parsing the database schema, along with the
/// location in the source SQL where it occurred.
#[derive(Debug, thiserror::Error, miette::Diagnostic)]
#[error("{message}")]
struct InnerSchemaError {
    message: String,
    #[label(collection)]
    labels: Vec<LabeledSpan>,
    #[help]
    help: Option<String>,
}

impl Schema {
    pub fn from_sql(
        path: impl AsRef<Utf8Path>,
        sql: &str,
    ) -> Result<Self, SchemaError> {
        let ctx = SourceContext::new(path.as_ref(), sql);
        let tokens = match Tokenizer::new(&PostgreSqlDialect {}, sql)
            .tokenize_with_location()
        {
            Ok(tokens) => tokens,
            // A tokenizer error is fatal: we can't recover enough to find any
            // tables, so it's the only error we have to report.
            Err(e) => {
                return Err(ctx.mk_schema_error(vec![
                    *ctx.error(format!("SQL syntax error: {}", e.message))
                        .label(Span::new(e.location, e.location), "here")
                        .build(),
                ]));
            }
        };

        // Find every annotated `CREATE TABLE` statement, along with any errors
        // that were encountered while parsing the schema.
        let (possible_fact_tables, mut errors) = fm_fact_tables(&ctx, &tokens);

        // Validate each annotated table and add them to the map, continuing to
        // accumulate errors if any table is semantically invalid.
        let mut fact_tables_by_de: BTreeMap<_, IdOrdMap<Arc<FactTable>>> =
            BTreeMap::new();
        let mut all_fact_tables = IdOrdMap::new();
        for (create_stmt, annotations) in possible_fact_tables {
            let table = match FactTable::try_from_create_table(
                &ctx,
                create_stmt,
                annotations,
            ) {
                Ok(table) => Arc::new(table),
                Err(e) => {
                    errors.push(*e);
                    continue;
                }
            };
            let de_name = table.de_name.clone();
            if let Err(err) = all_fact_tables.insert_unique(table.clone()) {
                let new = err.new_item();
                let mut builder = ctx
                    .error(format!(
                        "duplicate table name {}",
                        new.create_stmt.name
                    ))
                    .label(new.create_stmt.name.span(), "redefined here");
                if let Some(existing) = err.duplicates().first() {
                    builder = builder.label(
                        existing.create_stmt.name.span(),
                        "first defined here",
                    );
                }
                errors.push(*builder.build());
            }
            if let Err(err) = fact_tables_by_de
                .entry(de_name)
                .or_default()
                .insert_unique(table)
            {
                unreachable!(
                    "duplicate table name {:?} in DE index for de {:?} \
                     this is probably a bug, and shouldn't happen, since we \
                     successfully inserted the table into the map of all \
                     fact tables, which ensures the table name is unique!",
                    err.new_item().create_stmt.name,
                    err.new_item().de_name,
                )
            }
        }

        if !errors.is_empty() {
            return Err(ctx.mk_schema_error(errors));
        }

        Ok(Self { fact_tables_by_de, all_fact_tables })
    }
}

impl FactTable {
    fn try_from_create_table(
        ctx: &SourceContext,
        create_stmt: CreateTable,
        annotations: Vec<Annotation<'_>>,
    ) -> Result<Self, Box<InnerSchemaError>> {
        let table_name = &create_stmt.name;
        if annotations[0].text != FM_FACT_ANNOTATION {
            return Err(ctx
                .error(format!(
                    "expected fm_fact annotation, got {}",
                    annotations[0].text
                ))
                .label(annotations[0].span, "this annotation")
                .help(
                    "the first `--#!` annotation on a fact table must be \
                     `fm_fact`",
                )
                .build());
        }

        validate_shape(ctx, &create_stmt)?;

        let mut de_name = None;
        let mut fact_variant_name = None;
        for annotation in &annotations[1..] {
            parse_annotation(ctx, DE_ANNOTATION, &mut de_name, annotation)?;
            parse_annotation(
                ctx,
                VARIANT_ANNOTATION,
                &mut fact_variant_name,
                annotation,
            )?;
        }

        let de_name = de_name.map(|(ident, _)| ident).ok_or_else(|| {
            ctx.error(format!(
                "'{table_name}' is missing a '{DE_ANNOTATION}' annotation",
            ))
            .label(table_name.span(), "this fact table")
            .help(format!(
                "add a `--#! {DE_ANNOTATION} = <ident>` annotation above the table"
            ))
            .build()
        })?.into();
        let fact_variant_name =
            fact_variant_name.map(|(ident, _)| ident).ok_or_else(|| {
                ctx.error(format!(
                    "'{table_name}' is missing a '{VARIANT_ANNOTATION}' annotation",
                ))
                .label(table_name.span(), "this fact table")
                .help(format!(
                    "add a `--#! {VARIANT_ANNOTATION} = <ident>` annotation above \
                     the table"
                ))
                .build()
            })?;

        Ok(FactTable { create_stmt, de_name, fact_variant_name })
    }
}

/// Returns every `CREATE TABLE` statement in `tokens` that is annotated with at
/// least one `--#!` annotation comment placed directly above it, paired with
/// those annotations, along with any errors encountered while parsing those
/// statements.
fn fm_fact_tables<'a>(
    ctx: &SourceContext,
    tokens: &'a [TokenWithSpan],
) -> (Vec<(CreateTable, Vec<Annotation<'a>>)>, Vec<InnerSchemaError>) {
    let mut tables = Vec::new();
    let mut errors = Vec::new();
    for segment in statement_segments(tokens) {
        if !is_create_table(segment.tokens) {
            continue;
        }
        let mut parser = Parser::new(&PostgreSqlDialect {})
            .with_tokens_with_locations(segment.tokens.to_vec());

        // `parse_create_table` expects the `CREATE TABLE` keywords to already
        // be consumed, so step over them first.
        let Ok(_) = parser.expect_keyword_is(Keyword::CREATE) else {
            continue;
        };
        let Ok(_) = parser.expect_keyword_is(Keyword::TABLE) else {
            continue;
        };
        match parser.parse_create_table(false, false, None, false) {
            Ok(stmt) => tables.push((stmt, segment.annotations)),
            Err(e) => {
                let span = segment
                    .tokens
                    .iter()
                    .find(|t| !matches!(t.token, Token::Whitespace(_)))
                    .map(|t| t.span)
                    .unwrap_or(Span::empty());
                errors.push(
                    *ctx.error(format!("syntax error: {e}"))
                        .label(span, "while parsing this statement")
                        .build(),
                );
            }
        }
    }
    (tables, errors)
}

fn parse_annotation(
    ctx: &SourceContext,
    name: &str,
    into: &mut Option<(String, Span)>,
    annotation: &Annotation<'_>,
) -> Result<(), Box<InnerSchemaError>> {
    let Some(value) = parse_annotation_kv(name, annotation.text) else {
        return Ok(());
    };
    if let Some((prior, prior_span)) = into {
        return Err(ctx
            .error(format!(
                "duplicate value for '{name}' annotation: {value} (previous \
                 value was {prior})"
            ))
            .label(annotation.span, "redefined here")
            .label(*prior_span, "previously defined here")
            .build());
    }
    // We throw out the actual identifier, since it's easier to work with names
    // as regular Rust `String`s.
    let _ = syn::parse_str::<syn::Ident>(value).map_err(|e| {
        ctx.error(format!(
            "'{name}' annotation value {value:?} is not a valid Rust \
            identifier"
        ))
        .label(annotation.span, "this annotation")
        .help(e.to_string())
        .build()
    })?;
    *into = Some((value.to_string(), annotation.span));
    Ok(())
}

fn parse_annotation_kv<'v>(key: &str, value: &'v str) -> Option<&'v str> {
    Some(value.trim().strip_prefix(key)?.trim().strip_prefix("=")?.trim())
}

fn validate_shape(
    ctx: &SourceContext,
    table: &CreateTable,
) -> Result<(), Box<InnerSchemaError>> {
    const ID_COL: &str = "id";
    const SITREP_ID_COL: &str = "sitrep_id";

    let not_a_fact =
        || format!("'{}' is not a valid fm_fact table", table.name);

    // check all the expected UUID columns we want are there
    let mut want_columns = [
        (ID_COL, 0),
        (SITREP_ID_COL, 0),
        ("case_id", 0),
        ("created_sitrep_id", 0),
    ];
    'columns: for column in &table.columns {
        for (want_name, found) in want_columns.iter_mut() {
            if column.name.value.eq_ignore_ascii_case(want_name) {
                validate_id_col(ctx, table, column)?;
                *found += 1;
                continue 'columns;
            }
        }
    }
    for (name, found) in want_columns {
        if found < 1 {
            return Err(ctx
                .error(format!("missing required column `{name}`"))
                .label(table.name.span(), "this fact table")
                .help(not_a_fact())
                .build());
        } else if found > 1 {
            // the parser should definitely not have allowed this, but it did!
            // what to heck
            return Err(ctx
                .error(format!("duplicate column `{name}`"))
                .label(table.name.span(), "this fact table")
                .help(
                    "this is probably a bug in `fm-build`, the SQL parser \
                     should have rejected this",
                )
                .build());
        }
    }

    // okay, check that the primary key is the ID and sitrep ID columns.
    let pk = table.constraints.iter().find_map(|c| match c {
        TableConstraint::PrimaryKey(pk) => Some(pk),
        _ => None,
    });
    let Some(pk) = pk else {
        return Err(ctx
            .error(format!(
                "table's primary key must be ({ID_COL}, {SITREP_ID_COL}), but \
                 I couldn't find a compound primary key constraint (perhaps \
                 one of the columns is the PK?)"
            ))
            .label(table.name.span(), "this fact table")
            .help(not_a_fact())
            .build());
    };

    let wrong_pk = || {
        ctx.error(format!(
            "primary key must be ({ID_COL}, {SITREP_ID_COL}), but is {pk}",
        ))
        .label(pk.span(), "this primary key")
        .help(not_a_fact())
        .build()
    };
    if pk.columns.len() != 2 {
        return Err(wrong_pk());
    }
    for col in &pk.columns {
        let Expr::Identifier(ref colname) = col.column.expr else {
            return Err(wrong_pk());
        };
        if !(colname.value.eq_ignore_ascii_case(ID_COL)
            || colname.value.eq_ignore_ascii_case(SITREP_ID_COL))
        {
            return Err(wrong_pk());
        }
    }

    Ok(())
}

fn validate_id_col(
    ctx: &SourceContext,
    table: &CreateTable,
    column: &ColumnDef,
) -> Result<(), Box<InnerSchemaError>> {
    let not_a_fact =
        || format!("'{}' is not a valid fm_fact table", table.name);

    let name = &column.name.value;
    if !matches!(column.data_type, DataType::Uuid) {
        return Err(ctx
            .error(format!(
                "expected `{name}` column to be of type `UUID`, found `{:?}`",
                column.data_type
            ))
            .label(column.span(), "this column")
            .help(not_a_fact())
            .build());
    }

    if !column
        .options
        .iter()
        .any(|opt| matches!(opt.option, ColumnOption::NotNull))
    {
        return Err(ctx
            .error(format!(
                "expected `{name}` column to be `NOT NULL`, but it wasn't"
            ))
            .label(column.span(), "this column")
            .help(not_a_fact())
            .build());
    }

    Ok(())
}

/// Returns whether `tokens` begins with the `CREATE TABLE` keywords (ignoring
/// leading comments and whitespace).
fn is_create_table(tokens: &[TokenWithSpan]) -> bool {
    let mut keywords = tokens
        .iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .map(|t| match &t.token {
            Token::Word(word) => word.keyword,
            _ => Keyword::NoKeyword,
        });
    keywords.next() == Some(Keyword::CREATE)
        && keywords.next() == Some(Keyword::TABLE)
}

/// A single annotation comment, along with the span of the comment in the
/// source SQL.
struct Annotation<'a> {
    text: &'a str,
    span: Span,
}

/// A single `;`-separated statement, as a slice of the original token stream,
/// along with any annotation comments found.
struct StatementSegment<'a> {
    tokens: &'a [TokenWithSpan],
    annotations: Vec<Annotation<'a>>,
}

/// Splits a token stream into individual statements at each top-level
/// semicolon.
///
/// Comments and whitespace that precede a statement (i.e. that appear after the
/// previous statement's terminating semicolon) are considered part of that
/// statement's segment, which is how we associate a leading annotation comment
/// with the statement it sits on top of. Segments that contain no significant
/// tokens (e.g. a trailing comment at the end of the file) are dropped.
fn statement_segments(tokens: &[TokenWithSpan]) -> Vec<StatementSegment<'_>> {
    let mut segments = Vec::new();
    let mut start = 0;
    for (idx, token) in tokens.iter().enumerate() {
        if token.token == Token::SemiColon {
            try_parse_segment(&mut segments, &tokens[start..=idx]);
            start = idx + 1;
        }
    }
    // Handle a final statement that isn't terminated by a semicolon.
    if start < tokens.len() {
        try_parse_segment(&mut segments, &tokens[start..]);
    }
    segments
}

fn try_parse_segment<'a>(
    segments: &mut Vec<StatementSegment<'a>>,
    tokens: &'a [TokenWithSpan],
) {
    let has_statement =
        tokens.iter().any(|t| !matches!(t.token, Token::Whitespace(_)));
    if !has_statement {
        return;
    }
    let annotations = tokens
        .iter()
        .take_while(|t| matches!(t.token, Token::Whitespace(_)))
        .filter_map(|t| match &t.token {
            Token::Whitespace(Whitespace::SingleLineComment {
                comment,
                ..
            }) => {
                let text = comment.strip_prefix(ANNOTATION_PREFIX)?.trim();
                (!text.is_empty()).then_some(Annotation { text, span: t.span })
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if !annotations.is_empty() {
        segments.push(StatementSegment { tokens, annotations });
    }
}

/// Holds the source SQL and its filename so that errors can be reported with a
/// labeled location.
struct SourceContext<'src> {
    path: &'src Utf8Path,
    sql: &'src str,
    /// Byte offset of the start of each line, used to convert a
    /// [`Location`]'s (line, column) into a byte offset.
    line_starts: Vec<usize>,
}

impl<'src> SourceContext<'src> {
    fn new(path: &'src Utf8Path, sql: &'src str) -> Self {
        let line_starts = std::iter::once(0)
            .chain(sql.match_indices('\n').map(|(i, _)| i + 1))
            .collect();
        Self { path, sql, line_starts }
    }

    /// Converts a 1-based (line, column) [`Location`] into a byte offset into
    /// the source. (Line 0 / column 0 are used by `sqlparser` for "empty"
    /// locations, and map to the start of the source.)
    fn offset(&self, loc: Location) -> usize {
        if loc.line == 0 {
            return 0;
        }
        let line = (loc.line - 1) as usize;
        let col = loc.column.saturating_sub(1) as usize;
        let Some(&line_start) = self.line_starts.get(line) else {
            return self.sql.len();
        };
        let rest = &self.sql[line_start..];
        let byte_in_line =
            rest.char_indices().nth(col).map_or(rest.len(), |(i, _)| i);
        (line_start + byte_in_line).min(self.sql.len())
    }

    /// Converts a `sqlparser` [`Span`] into a miette [`SourceSpan`].
    fn source_span(&self, span: Span) -> SourceSpan {
        let start = self.offset(span.start);
        let end = self.offset(span.end);
        SourceSpan::new(start.into(), end.saturating_sub(start))
    }

    fn error(&self, message: impl Into<String>) -> ErrorBuilder<'_> {
        ErrorBuilder {
            ctx: self,
            message: message.into(),
            labels: Vec::new(),
            help: None,
        }
    }

    fn mk_schema_error(&self, errors: Vec<InnerSchemaError>) -> SchemaError {
        SchemaError {
            src: NamedSource::new(self.path.as_str(), self.sql.to_string()),
            errors,
        }
    }
}

/// Builder for a [`SchemaError`], used to attach labeled source spans and help
/// text to an error message.
struct ErrorBuilder<'a> {
    ctx: &'a SourceContext<'a>,
    message: String,
    labels: Vec<LabeledSpan>,
    help: Option<String>,
}

impl ErrorBuilder<'_> {
    fn label(mut self, span: Span, text: impl Into<String>) -> Self {
        self.labels.push(LabeledSpan::new_with_span(
            Some(text.into()),
            self.ctx.source_span(span),
        ));
        self
    }

    fn help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }

    fn build(self) -> Box<InnerSchemaError> {
        Box::new(InnerSchemaError {
            message: self.message,
            labels: self.labels,
            help: self.help,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use miette::IntoDiagnostic;

    /// A fixed, fake filename used for error-rendering golden tests.
    const FAKE_PATH: &str = "fm_schema.sql";

    fn fact_tables(sql: &str) -> Vec<(String, Vec<String>)> {
        let ctx = SourceContext::new(Utf8Path::new(FAKE_PATH), sql);
        let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
            .tokenize_with_location()
            .unwrap();
        let (tables, errors) = fm_fact_tables(&ctx, &tokens);
        assert!(errors.is_empty(), "unexpected parse errors: {errors:?}");
        tables
            .into_iter()
            .map(|(table, annotations)| {
                (
                    table.name.to_string(),
                    annotations
                        .into_iter()
                        .map(|a| a.text.to_string())
                        .collect(),
                )
            })
            .collect()
    }

    /// Renders the errors returned by `Schema::from_sql` for `sql` as graphical
    /// (no-color) diagnostics, for golden-file comparison.
    fn render_errors(sql: &str) -> String {
        let errors = Schema::from_sql(Utf8Path::new(FAKE_PATH), sql)
            .expect_err("schema should be invalid");
        let handler = miette::GraphicalReportHandler::new_themed(
            miette::GraphicalTheme::unicode_nocolor(),
        )
        .with_width(80);
        let mut rendered = String::new();
        handler
            .render_report(&mut rendered, &errors)
            .expect("rendering a report should succeed");
        rendered
    }

    #[test]
    fn returns_only_annotated_create_tables() {
        let sql = "
            CREATE TABLE not_a_fact (id UUID PRIMARY KEY);

            --#! fm_fact
            CREATE TABLE a_fact (id UUID PRIMARY KEY, name TEXT);

            --#! fm_fact
            --#! de = FooBar
            CREATE TABLE another_fact (id UUID PRIMARY KEY);
        ";
        assert_eq!(
            fact_tables(sql),
            vec![
                ("a_fact".to_owned(), vec!["fm_fact".to_owned(),]),
                (
                    "another_fact".to_owned(),
                    vec!["fm_fact".to_owned(), "de = FooBar".to_owned()]
                )
            ]
        );
    }

    #[test]
    fn plain_comments_are_not_annotations() {
        let sql = "
            -- fm_fact
            CREATE TABLE looks_like_a_fact (id UUID);

            -- just a regular comment about the next table
            CREATE TABLE regular (id UUID);
        ";
        assert!(fact_tables(sql).is_empty());
    }

    #[test]
    fn annotation_tolerates_blank_lines_and_spacing() {
        let sql = "
            --#!  fm_fact

            --#! de = FooBar

            CREATE TABLE spaced_out (id UUID);
        ";
        assert_eq!(
            fact_tables(sql),
            vec![(
                "spaced_out".to_owned(),
                vec!["fm_fact".to_owned(), "de = FooBar".to_owned()]
            )]
        );
    }

    #[test]
    fn annotation_on_non_table_statement_is_ignored() {
        let sql = "
            --#! fm_fact
            CREATE INDEX my_index ON some_table (id);

            --#! fm_fact
            CREATE TABLE the_fact (id UUID);
        ";
        assert_eq!(
            fact_tables(sql),
            vec![("the_fact".to_owned(), vec!["fm_fact".to_owned(),])]
        );
    }

    #[test]
    fn handles_final_statement_without_trailing_semicolon() {
        let sql = "--#! fm_fact\nCREATE TABLE trailing (id UUID)";
        assert_eq!(
            fact_tables(sql),
            vec![("trailing".to_owned(), vec!["fm_fact".to_owned(),])]
        );
    }

    #[test]
    fn empty_input_yields_no_tables() {
        assert!(fact_tables("").is_empty());
        assert!(fact_tables("-- nothing here\n").is_empty());
    }

    const VALID_FACT: &str = "
        --#! fm_fact
        --#! de = ExampleEngine
        --#! name = ExampleFact
        CREATE TABLE example (
            id UUID NOT NULL,
            sitrep_id UUID NOT NULL,
            case_id UUID NOT NULL,
            created_sitrep_id UUID NOT NULL,
            PRIMARY KEY (id, sitrep_id)
        );
    ";

    #[test]
    fn from_sql_accepts_valid_fact_table() {
        let schema = Schema::from_sql(Utf8Path::new(FAKE_PATH), VALID_FACT)
            .expect("should be valid");
        assert_eq!(schema.all_fact_tables.len(), 1);
        let table = schema.all_fact_tables.iter().next().expect("one table");
        assert_eq!(table.create_stmt.name.to_string(), "example");
        assert_eq!(table.de_name.as_ref(), "ExampleEngine");
        assert_eq!(table.fact_variant_name, "ExampleFact");
    }

    #[test]
    fn from_sql_requires_de_and_fact_variant() {
        let sql = "
            --#! fm_fact
            CREATE TABLE example (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id UUID NOT NULL,
                created_sitrep_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );
        ";
        expectorate::assert_contents(
            "tests/output/requires_de_and_fact_variant.txt",
            &render_errors(sql),
        );
    }

    #[test]
    fn from_sql_requires_not_null() {
        let sql = "
            --#! fm_fact
            CREATE TABLE example (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id UUID,
                created_sitrep_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );
        ";
        expectorate::assert_contents(
            "tests/output/requires_not_null.txt",
            &render_errors(sql),
        );
    }

    #[test]
    fn from_sql_rejects_invalid_rust_ident() {
        let sql = "
            --#! fm_fact
            --#! de = 9nope
            --#! name = F
            CREATE TABLE example (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id UUID NOT NULL,
                created_sitrep_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );
        ";
        expectorate::assert_contents(
            "tests/output/rejects_invalid_rust_ident.txt",
            &render_errors(sql),
        );
    }

    #[test]
    fn from_sql_rejects_missing_required_column() {
        let sql = "
            --#! fm_fact
            --#! de = E
            --#! name = F
            CREATE TABLE example (
                id UUID NOT NULL, sitrep_id UUID NOT NULL,
                created_sitrep_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );
        ";
        expectorate::assert_contents(
            "tests/output/rejects_missing_required_column.txt",
            &render_errors(sql),
        );
    }

    #[test]
    fn from_sql_rejects_non_uuid_column() {
        let sql = "
            --#! fm_fact
            --#! de = E
            --#! name = F
            CREATE TABLE example (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id TEXT NOT NULL,
                created_sitrep_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );
        ";
        expectorate::assert_contents(
            "tests/output/rejects_non_uuid_column.txt",
            &render_errors(sql),
        );
    }

    #[test]
    fn from_sql_rejects_wrong_primary_key() {
        let sql = "
            --#! fm_fact
            --#! de = E
            --#! name = F
            CREATE TABLE example (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id UUID NOT NULL,
                created_sitrep_id UUID NOT NULL,
                PRIMARY KEY (id)
            );
        ";
        expectorate::assert_contents(
            "tests/output/rejects_wrong_primary_key.txt",
            &render_errors(sql),
        );
    }

    #[test]
    fn from_sql_rejects_duplicate_table_names() {
        let sql = "
            --#! fm_fact
            --#! de = E
            --#! name = F
            CREATE TABLE dupe (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id UUID NOT NULL,
                created_sitrep_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );

            --#! fm_fact
            --#! de = E2
            --#! name = F2
            CREATE TABLE dupe (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id UUID NOT NULL,
                created_sitrep_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );
        ";
        expectorate::assert_contents(
            "tests/output/rejects_duplicate_table_names.txt",
            &render_errors(sql),
        );
    }

    #[test]
    fn from_sql_reports_every_invalid_table() {
        // Each of these tables is invalid in a different way; all of the
        // errors should be reported, not just the first one.
        let sql = "
            --#! fm_fact
            --#! de = First
            --#! name = First
            CREATE TABLE first (
                id UUID NOT NULL, sitrep_id UUID NOT NULL,
                created_sitrep_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );

            --#! fm_fact
            --#! de = Second
            --#! name = Second
            CREATE TABLE second (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id TEXT NOT NULL,
                created_sitrep_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );

            --#! fm_fact
            CREATE TABLE third (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id UUID NOT NULL,
                created_sitrep_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );
        ";
        expectorate::assert_contents(
            "tests/output/reports_every_invalid_table.txt",
            &render_errors(sql),
        );
    }
}
