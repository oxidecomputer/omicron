// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use iddqd::IdOrdMap;
use sqlparser::ast::CreateTable;
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

pub struct Schema {
    pub(crate) fact_tables: IdOrdMap<FactTable>,
}

pub(crate) struct FactTable {
    create_stmt: CreateTable,
    // Rust ident for the diagnosis engine enum variant
    de_name: String,
    // Rust ident for the fact variant enum variant
    fact_variant_name: String,
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

            if annotations[0] != FM_FACT_ANNOTATION {
                // TODO(eliza): good error message with sql source location lol
                anyhow::bail!(
                    "expected fm_fact annotation, got {}",
                    annotations[0]
                );
            }

            // TODO(eliza): also parse the following required annotations:
            // - `de = <rust ident for diagnosis engine enum variant>`
            // - `fact_variant = <rust ident for fact variant enum variant>/name of generated struct`

            // TODO(eliza): check that the table has the following:
            // - id UUID
            // - sitrep_id UUID
            // - case_id UUID
            // - PRIMARY KEY (id, sitrep_id)
            // if not, it is not valid to be a FM fact
            let table = FactTable {
                create_stmt,
                de_name: todo!("parse from annotations"),
                fact_variant_name: todo!("parse from annotations"),
            };
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
                ("a_fact".to_owned(), vec![]),
                ("another_fact".to_owned(), vec!["de = FooBar".to_owned()])
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
            vec![("spaced_out".to_owned(), vec!["de = FooBar".to_owned()])]
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
        assert_eq!(fact_tables(sql), vec![("the_fact".to_owned(), vec![])]);
    }

    #[test]
    fn handles_final_statement_without_trailing_semicolon() {
        let sql = "--! fm_fact\nCREATE TABLE trailing (id UUID)";
        assert_eq!(fact_tables(sql), vec![("trailing".to_owned(), vec![])]);
    }

    #[test]
    fn empty_input_yields_no_tables() {
        assert!(fact_tables("").is_empty());
        assert!(fact_tables("-- nothing here\n").is_empty());
    }
}
