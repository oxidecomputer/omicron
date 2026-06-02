// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use iddqd::IdOrdMap;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::ColumnOption;
use sqlparser::ast::CreateTable;
use sqlparser::ast::DataType;
use sqlparser::ast::Expr;
use sqlparser::ast::TableConstraint;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;
use sqlparser::tokenizer::TokenWithSpan;
use sqlparser::tokenizer::Tokenizer;
use sqlparser::tokenizer::Whitespace;

const ANNOTATION_PREFIX: &str = "#!";
const FM_FACT_ANNOTATION: &str = "fm_fact";
const DE_ANNOTATION: &str = "de";
const VARIANT_ANNOTATION: &str = "name";

#[derive(Debug)]
pub struct Schema {
    #[allow(dead_code)]
    pub(crate) fact_tables: IdOrdMap<FactTable>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct FactTable {
    create_stmt: CreateTable,
    // Rust ident for the diagnosis engine enum variant
    pub(crate) de_name: syn::Ident,
    // Rust ident for the fact variant enum variant
    pub(crate) fact_variant_name: syn::Ident,
}

impl iddqd::IdOrdItem for FactTable {
    type Key<'a> = &'a sqlparser::ast::ObjectName;

    fn key(&self) -> Self::Key<'_> {
        &self.create_stmt.name
    }

    iddqd::id_upcast!();
}

impl Schema {
    pub fn from_sql(sql: &str) -> anyhow::Result<Self> {
        let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
            .tokenize_with_location()?;

        let mut fact_tables = IdOrdMap::new();
        for (create_stmt, annotations) in fm_fact_tables(&tokens)? {
            if annotations.is_empty() {
                // The parser should not let this happen, but whatever...
                continue;
            }

            let table_name = &create_stmt.name;
            if annotations[0] != FM_FACT_ANNOTATION {
                // TODO(eliza): good error message with sql source location lol
                anyhow::bail!(
                    "expected fm_fact annotation, got {}",
                    annotations[0]
                );
            }

            validate_shape(&create_stmt).with_context(|| {
                format!("'{table_name}' is not a valid fm_fact table")
            })?;

            let mut de_name = None;
            let mut fact_variant_name = None;
            for annotation in &annotations[1..] {
                parse_annotation(DE_ANNOTATION, &mut de_name, annotation)?;
                parse_annotation(
                    VARIANT_ANNOTATION,
                    &mut fact_variant_name,
                    annotation,
                )?;
            }

            let de_name = de_name.ok_or_else(|| {
                anyhow::anyhow!(
                    "'{table_name}' is missing a '{DE_ANNOTATION}' annotation",
                )
            })?;
            let fact_variant_name = fact_variant_name.ok_or_else(|| anyhow::anyhow!(
                "'{table_name}' is missing a '{VARIANT_ANNOTATION}' annotation",
            ))?;
            let table = FactTable { create_stmt, de_name, fact_variant_name };
            fact_tables.insert_unique(table).map_err(|e| {
                anyhow::anyhow!(
                    "duplicate table name {}",
                    e.new_item().create_stmt.name
                )
            })?;
        }
        Ok(Self { fact_tables })
    }
}

/// Returns an iterator over every `CREATE TABLE` statement in `sql` that is
/// annotated with a `--! fm_fact` comment placed directly above it.
fn fm_fact_tables(
    tokens: &[TokenWithSpan],
) -> Result<Vec<(CreateTable, Vec<&str>)>, ParserError> {
    statement_segments(tokens)
        .into_iter()
        .filter_map(|segment| {
            if !is_create_table(segment.tokens) {
                return None;
            }
            let mut parser = Parser::new(&PostgreSqlDialect {})
                .with_tokens_with_locations(segment.tokens.to_vec());

            // `parse_create_table` expects the `CREATE TABLE` keywords to already
            // be consumed, so step over them first.
            let Ok(_) = parser.expect_keyword_is(Keyword::CREATE) else {
                return None;
            };
            let Ok(_) = parser.expect_keyword_is(Keyword::TABLE) else {
                return None;
            };
            let create_stmt = parser
                .parse_create_table(false, false, None, false)
                .map(|stmt| (stmt, segment.annotations));
            Some(create_stmt)
        })
        .collect()
}

fn parse_annotation(
    name: &str,
    into: &mut Option<syn::Ident>,
    value: &str,
) -> anyhow::Result<()> {
    let Some(value) = parse_annotation_kv(name, value) else {
        return Ok(());
    };
    if let Some(prior) = into {
        anyhow::bail!(
            "duplicate value for '{name}' annotation: {value} (previous value \
             was {prior})"
        )
    }
    let ident = syn::parse_str::<syn::Ident>(value).with_context(|| {
        format!(
            "'{name}' annotation value {value:?} is not a valid Rust \
            identifier"
        )
    })?;
    *into = Some(ident);
    Ok(())
}

fn parse_annotation_kv<'v>(key: &str, value: &'v str) -> Option<&'v str> {
    Some(value.trim().strip_prefix(key)?.trim().strip_prefix("=")?.trim())
}

fn validate_shape(table: &CreateTable) -> anyhow::Result<()> {
    const ID_COL: &str = "id";
    const SITREP_ID_COL: &str = "sitrep_id";

    // check all the expected UUID columns we want are there
    let mut want_columns = [(ID_COL, 0), (SITREP_ID_COL, 0), ("case_id", 0)];
    'columns: for column in &table.columns {
        for (want_name, found) in want_columns.iter_mut() {
            if column.name.value.eq_ignore_ascii_case(want_name) {
                validate_id_col(column)?;
                *found += 1;
                continue 'columns;
            }
        }
    }
    for (name, found) in want_columns {
        if found < 1 {
            anyhow::bail!("missing required column `{name}`");
        } else if found > 1 {
            // the parser should definitely not have allowed this, but it did!
            // what to heck
            anyhow::bail!("duplicate column `{name}` (that's weird!)");
        }
    }

    // okay, check that the primary key is the ID and sitrep ID columns.
    let pk = table.constraints.iter().find_map(|c| match c {
        TableConstraint::PrimaryKey(pk) => Some(pk),
        _ => None,
    });
    let Some(pk) = pk else {
        anyhow::bail!(
            "table's primary key must be ({ID_COL}, {SITREP_ID_COL}), but I \
             couldn't find a compound primary key constraint (perhaps one of \
             the columns is the PK?)"
        )
    };

    anyhow::ensure!(
        pk.columns.len() == 2,
        "primary key must be ({ID_COL}, {SITREP_ID_COL}), but is {pk}",
    );
    for col in &pk.columns {
        let Expr::Identifier(ref colname) = col.column.expr else {
            anyhow::bail!(
                "primary key must be ({ID_COL}, {SITREP_ID_COL}), but is {pk}",
            )
        };
        anyhow::ensure!(
            colname.value.eq_ignore_ascii_case(ID_COL)
                || colname.value.eq_ignore_ascii_case(SITREP_ID_COL),
            "primary key must be ({ID_COL}, {SITREP_ID_COL}), but is {pk}",
        );
    }

    Ok(())
}

fn validate_id_col(column: &ColumnDef) -> anyhow::Result<()> {
    let name = &column.name.value;
    if !matches!(column.data_type, DataType::Uuid) {
        anyhow::bail!(
            "expected `{name}` column to be of type `UUID`, found `{:?}`",
            column.data_type
        );
    }

    if !column
        .options
        .iter()
        .any(|opt| matches!(opt.option, ColumnOption::NotNull))
    {
        anyhow::bail!(
            "expected `{name}` column to be `NOT NULL`, but it wasn't"
        );
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

/// A single `;`-separated statement, as a slice of the original token stream,
/// along with any annotation comments found.
struct StatementSegment<'a> {
    tokens: &'a [TokenWithSpan],
    annotations: Vec<&'a str>,
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
                let annotation =
                    comment.strip_prefix(ANNOTATION_PREFIX)?.trim();
                if !annotation.is_empty() { Some(annotation) } else { None }
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if !annotations.is_empty() {
        segments.push(StatementSegment { tokens, annotations });
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn fact_tables(sql: &str) -> Vec<(String, Vec<String>)> {
        let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
            .tokenize_with_location()
            .unwrap();
        fm_fact_tables(&tokens)
            .expect("SQL should parse")
            .into_iter()
            .map(|(table, annotations)| {
                (
                    table.name.to_string(),
                    annotations.into_iter().map(|s| s.to_string()).collect(),
                )
            })
            .collect()
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
            PRIMARY KEY (id, sitrep_id)
        );
    ";

    #[test]
    fn from_sql_accepts_valid_fact_table() {
        let schema =
            dbg!(Schema::from_sql(VALID_FACT)).expect("should be valid");
        assert_eq!(schema.fact_tables.len(), 1);
        let table = schema.fact_tables.iter().next().expect("one table");
        assert_eq!(table.create_stmt.name.to_string(), "example");
        assert_eq!(table.de_name, "ExampleEngine");
        assert_eq!(table.fact_variant_name, "ExampleFact");
    }

    #[test]
    fn from_sql_requires_de_and_fact_variant() {
        let sql = "
            --#! fm_fact
            CREATE TABLE example (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );
        ";
        assert!(dbg!(Schema::from_sql(dbg!(sql))).is_err());
    }

    #[test]
    fn from_sql_requires_not_null() {
        let sql = "
            --#! fm_fact
            CREATE TABLE example (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id UUID,
                PRIMARY KEY (id, sitrep_id)
            );
        ";
        assert!(dbg!(Schema::from_sql(dbg!(sql))).is_err());
    }

    #[test]
    fn from_sql_rejects_invalid_rust_ident() {
        let sql = "
            --#! fm_fact
            --#! de = 9nope
            --#! name = F
            CREATE TABLE example (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );
        ";
        assert!(dbg!(Schema::from_sql(dbg!(sql))).is_err());
    }

    #[test]
    fn from_sql_rejects_missing_required_column() {
        let sql = "
            --#! fm_fact
            --#! de = E
            --#! name = F
            CREATE TABLE example (
                id UUID NOT NULL, sitrep_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );
        ";
        assert!(dbg!(Schema::from_sql(dbg!(sql))).is_err());
    }

    #[test]
    fn from_sql_rejects_non_uuid_column() {
        let sql = "
            --#! fm_fact
            --#! de = E
            --#! name = F
            CREATE TABLE example (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id TEXT NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );
        ";
        assert!(dbg!(Schema::from_sql(dbg!(sql))).is_err());
    }

    #[test]
    fn from_sql_rejects_wrong_primary_key() {
        let sql = "
            --#! fm_fact
            --#! de = E
            --#! name = F
            CREATE TABLE example (
                id UUID, sitrep_id UUID, case_id UUID,
                PRIMARY KEY (id)
            );
        ";
        assert!(dbg!(Schema::from_sql(dbg!(sql))).is_err());
    }

    #[test]
    fn from_sql_rejects_duplicate_table_names() {
        let sql = "
            --#! fm_fact
            --#! de = E
            --#! name = F
            CREATE TABLE dupe (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );

            --#! fm_fact
            --#! de = E2
            --#! name = F2
            CREATE TABLE dupe (
                id UUID NOT NULL, sitrep_id UUID NOT NULL, case_id UUID NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            );
        ";
        assert!(dbg!(Schema::from_sql(dbg!(sql))).is_err());
    }
}
