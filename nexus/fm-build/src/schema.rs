// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Extracting fault-management fact tables from a SQL DDL file.

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

/// The annotation that marks a `CREATE TABLE` statement as a fault-management
/// fact table.
///
/// It is written as a single-line "directive" comment placed directly above
/// the statement it applies to, for example:
///
/// ```sql
/// --! fm_fact
/// CREATE TABLE my_fact ( ... );
/// ```
const FM_FACT_ANNOTATION: &str = "fm_fact";
// TODO(eliza): also parse the following required annotations:
// - `de = <rust ident for diagnosis engine enum variant>`
// - `fact_variant = <rust ident for fact variant enum variant>/name of generated struct`

pub struct Schema {
    pub(crate) fact_tables: IdOrdMap<FactTable>,
}

pub(crate) struct FactTable {
    create_stmt: CreateTable,
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
        let mut fact_tables = IdOrdMap::new();
        for create_stmt in fm_fact_tables(sql)? {
            // TODO(eliza): check that the table has the following:
            // - id UUID
            // - sitrep_id UUID
            // - case_id UUID
            // - PRIMARY KEY (id, sitrep_id)
            // if not, it is not valid to be a FM fact
            fact_tables.insert_unique(FactTable { create_stmt }).map_err(
                |e| {
                    anyhow::anyhow!(
                        "duplicate table name {}",
                        e.new_item().create_stmt.name
                    )
                },
            )?;
        }
        Ok(Self { fact_tables })
    }
}

/// Returns an iterator over every `CREATE TABLE` statement in `sql` that is
/// annotated with a `--! fm_fact` comment placed directly above it.
fn fm_fact_tables(sql: &str) -> Result<Vec<CreateTable>, ParserError> {
    let dialect = PostgreSqlDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize_with_location()?;

    statement_segments(&tokens)
        .into_iter()
        .filter_map(|segment| {
            // We only care about `CREATE TABLE` statements wearing the
            // `--! fm_fact` hat, so we can avoid parsing everything else in the
            // file.
            if !segment.annotated || !is_create_table(segment.tokens) {
                return None;
            }

            // `parse_create_table` expects the `CREATE TABLE` keywords to already
            // be consumed, so step over them first.
            let mut parser = Parser::new(&dialect)
                .with_tokens_with_locations(segment.tokens.to_vec());
            let Ok(_) = parser.expect_keyword_is(Keyword::CREATE) else {
                return None;
            };
            let Ok(_) = parser.expect_keyword_is(Keyword::TABLE) else {
                return None;
            };
            Some(parser.parse_create_table(false, false, None, false))
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
/// along with whether it carried the [`FM_FACT_ANNOTATION`].
struct StatementSegment<'a> {
    tokens: &'a [TokenWithSpan],
    annotated: bool,
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
            push_segment(&mut segments, &tokens[start..=idx]);
            start = idx + 1;
        }
    }
    // Handle a final statement that isn't terminated by a semicolon.
    if start < tokens.len() {
        push_segment(&mut segments, &tokens[start..]);
    }
    segments
}

/// Records `tokens` as a statement segment, unless it contains only whitespace
/// and comments (in which case there's no statement to associate with).
fn push_segment<'a>(
    segments: &mut Vec<StatementSegment<'a>>,
    tokens: &'a [TokenWithSpan],
) {
    let has_statement =
        tokens.iter().any(|t| !matches!(t.token, Token::Whitespace(_)));
    if !has_statement {
        return;
    }

    // The annotation, if present, lives in the run of leading comments and
    // whitespace before the statement's first significant token.
    let annotated = tokens
        .iter()
        .take_while(|t| matches!(t.token, Token::Whitespace(_)))
        .any(|t| match &t.token {
            Token::Whitespace(Whitespace::SingleLineComment {
                comment,
                ..
            }) => is_fm_fact_annotation(comment),
            _ => false,
        });

    segments.push(StatementSegment { tokens, annotated });
}

/// Returns whether a single-line comment's body is the `fm_fact` annotation.
///
/// `comment` is the text following the `--` (or `#`) prefix, e.g.
/// `"! fm_fact\n"`. A directive comment is distinguished from an ordinary
/// comment by a leading `!`.
fn is_fm_fact_annotation(comment: &str) -> bool {
    comment
        .trim()
        .strip_prefix('!')
        .is_some_and(|directive| directive.trim() == FM_FACT_ANNOTATION)
}

#[cfg(test)]
mod test {
    use super::*;

    fn table_names(sql: &str) -> Vec<String> {
        fm_fact_tables(sql)
            .expect("SQL should parse")
            .into_iter()
            .map(|table| table.name.to_string())
            .collect()
    }

    #[test]
    fn returns_only_annotated_create_tables() {
        let sql = "
            CREATE TABLE not_a_fact (id UUID PRIMARY KEY);

            --! fm_fact
            CREATE TABLE a_fact (id UUID PRIMARY KEY, name TEXT);

            --! fm_fact
            CREATE TABLE another_fact (id UUID PRIMARY KEY);
        ";
        assert_eq!(table_names(sql), vec!["a_fact", "another_fact"]);
    }

    #[test]
    fn plain_comments_are_not_annotations() {
        let sql = "
            -- fm_fact
            CREATE TABLE looks_like_a_fact (id UUID);

            -- just a regular comment about the next table
            CREATE TABLE regular (id UUID);
        ";
        assert!(table_names(sql).is_empty());
    }

    #[test]
    fn annotation_tolerates_blank_lines_and_spacing() {
        let sql = "
            --!  fm_fact

            CREATE TABLE spaced_out (id UUID);
        ";
        assert_eq!(table_names(sql), vec!["spaced_out"]);
    }

    #[test]
    fn annotation_on_non_table_statement_is_ignored() {
        let sql = "
            --! fm_fact
            CREATE INDEX my_index ON some_table (id);

            --! fm_fact
            CREATE TABLE the_fact (id UUID);
        ";
        assert_eq!(table_names(sql), vec!["the_fact"]);
    }

    #[test]
    fn handles_final_statement_without_trailing_semicolon() {
        let sql = "--! fm_fact\nCREATE TABLE trailing (id UUID)";
        assert_eq!(table_names(sql), vec!["trailing"]);
    }

    #[test]
    fn empty_input_yields_no_tables() {
        assert!(table_names("").is_empty());
        assert!(table_names("-- nothing here\n").is_empty());
    }
}
