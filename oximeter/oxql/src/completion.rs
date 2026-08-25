use serde::{Deserialize, Serialize};

use crate::Diagnostic;
use crate::ast::Query;
use crate::grammar;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CompletionSite {
    None,
    TableOperation,
    Timeseries,
    FilterIdentifier,
    FilterLiteral,
    AlignmentMethod,
    GroupField,
    Reducer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplacementSpan {
    pub start: usize,
    pub end: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompletionContext {
    pub site: CompletionSite,
    pub replacement: ReplacementSpan,
    pub referenced_timeseries: Vec<String>,
}

/// Determine the language construct being completed at `cursor`.
///
/// This returns language context only. Consumers remain responsible for
/// choosing and presenting completion options from that context.
/// `cursor` and the returned replacement span are UTF-8 byte offsets.
pub fn completion_context(
    source: &str,
    cursor: usize,
) -> Result<CompletionContext, Diagnostic> {
    if cursor > source.len() || !source.is_char_boundary(cursor) {
        return Err(Diagnostic {
            code: "invalid-cursor",
            phase: "completion",
            severity: "error",
            message: format!(
                "Cursor {cursor} is not a UTF-8 boundary within a {}-byte query",
                source.len()
            ),
            line: None,
            column: None,
        });
    }

    let prefix = &source[..cursor];
    let replacement =
        ReplacementSpan { start: completion_token_start(prefix), end: cursor };

    let recovered = recover_prior_clauses(prefix);
    if let Ok(query) = grammar::query_parser::document(&recovered) {
        return Ok(context_from_query(
            CompletionSite::None,
            replacement,
            &recovered,
            cursor,
            &query,
        ));
    }

    for repair in repairs(prefix, replacement.start) {
        let mut repaired = prefix.to_owned();
        repaired.push_str(&repair.suffix);
        repaired.push_str(&closing_braces(prefix));
        if repaired.len() > crate::MAX_QUERY_LEN {
            continue;
        }
        let Ok(query) = grammar::query_parser::document(&repaired) else {
            continue;
        };
        return Ok(context_from_query(
            repair.site,
            replacement,
            &repaired,
            cursor,
            &query,
        ));
    }

    Ok(CompletionContext {
        site: CompletionSite::None,
        replacement,
        referenced_timeseries: Vec::new(),
    })
}

fn context_from_query(
    site: CompletionSite,
    replacement: ReplacementSpan,
    repaired: &str,
    cursor: usize,
    query: &Query,
) -> CompletionContext {
    let query = query.innermost_query_at(cursor);
    let referenced_timeseries = query
        .all_timeseries_names()
        .into_iter()
        .filter(|name| name.start < cursor)
        .filter_map(|name| repaired.get(name.start..name.end.min(cursor)))
        .map(str::to_owned)
        .collect();
    CompletionContext { site, replacement, referenced_timeseries }
}

struct Repair {
    site: CompletionSite,
    suffix: String,
}

fn repairs(prefix: &str, token_start: usize) -> Vec<Repair> {
    let token = &prefix[token_start..];
    let mut repairs = vec![
        Repair { site: CompletionSite::FilterLiteral, suffix: "0".to_owned() },
        Repair {
            site: CompletionSite::FilterIdentifier,
            suffix: "datum == 0".to_owned(),
        },
        Repair {
            site: CompletionSite::FilterIdentifier,
            suffix: " == 0".to_owned(),
        },
        Repair {
            site: CompletionSite::GroupField,
            suffix: "], mean".to_owned(),
        },
    ];

    if let Some(suffix) = completion_suffix("mean_within", token) {
        repairs.push(Repair {
            site: CompletionSite::AlignmentMethod,
            suffix: format!("{suffix}(1s)"),
        });
    }
    for reducer in ["mean", "sum"] {
        if let Some(suffix) = completion_suffix(reducer, token) {
            repairs.push(Repair {
                site: CompletionSite::Reducer,
                suffix: suffix.to_owned(),
            });
        }
    }

    let timeseries_suffix = if token.is_empty() {
        Some("completion:placeholder")
    } else if token.ends_with(':') {
        Some("placeholder")
    } else if token.contains(':') {
        None
    } else {
        Some(":placeholder")
    };
    if let Some(suffix) = timeseries_suffix {
        repairs.push(Repair {
            site: CompletionSite::Timeseries,
            suffix: suffix.to_owned(),
        });
    }

    let operation_suffixes = [
        ("get", " completion:placeholder"),
        ("filter", " datum == 0"),
        ("align", " mean_within(1s)"),
        ("group_by", " [], mean"),
        ("join", ""),
        ("first", " 1"),
        ("last", " 1"),
    ];
    for (operation, arguments) in operation_suffixes {
        if let Some(keyword_suffix) = completion_suffix(operation, token) {
            repairs.push(Repair {
                site: CompletionSite::TableOperation,
                suffix: format!("{keyword_suffix}{arguments}"),
            });
        }
    }
    if token.is_empty() {
        repairs.push(Repair {
            site: CompletionSite::TableOperation,
            suffix: "last 1".to_owned(),
        });
    }

    repairs
}

fn completion_suffix<'a>(candidate: &'a str, prefix: &str) -> Option<&'a str> {
    candidate.strip_prefix(prefix)
}

fn completion_token_start(prefix: &str) -> usize {
    prefix
        .char_indices()
        .rev()
        .find_map(|(index, character)| {
            (!is_completion_character(character))
                .then_some(index + character.len_utf8())
        })
        .unwrap_or(0)
}

fn is_completion_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | ':' | '@')
}

fn closing_braces(prefix: &str) -> String {
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    for character in prefix.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quote.is_some() {
            escaped = true;
            continue;
        }
        if let Some(delimiter) = quote {
            if character == delimiter {
                quote = None;
            }
            continue;
        }
        match character {
            '\'' | '"' => quote = Some(character),
            '{' => depth += 1,
            '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    " }".repeat(depth)
}

fn recover_prior_clauses(prefix: &str) -> String {
    let mut recovered = prefix.as_bytes().to_vec();
    let mut clause_start = 0;
    let mut quote = None;
    let mut escaped = false;
    let bytes = prefix.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if escaped {
            escaped = false;
            index += 1;
            continue;
        }
        if byte == b'\\' && quote.is_some() {
            escaped = true;
            index += 1;
            continue;
        }
        if let Some(delimiter) = quote {
            if byte == delimiter {
                quote = None;
            }
            index += 1;
            continue;
        }
        match byte {
            b'\'' | b'"' => quote = Some(byte),
            b'|' if bytes.get(index + 1) == Some(&b'|') => {
                index += 2;
                continue;
            }
            b'|' | b'{' | b';' | b'}' => {
                recover_clause(&mut recovered, prefix, clause_start, index);
                clause_start = index + 1;
            }
            _ => {}
        }
        index += 1;
    }
    String::from_utf8(recovered).expect("recovery only inserts ASCII")
}

fn recover_clause(
    recovered: &mut [u8],
    source: &str,
    start: usize,
    end: usize,
) {
    let clause = &source[start..end];
    let content = clause.trim();
    if content.is_empty() {
        return;
    }
    let content_start = start + clause.find(content).unwrap();
    let content_end = content_start + content.len();
    let content = &source[content_start..content_end];
    let Some(identifier) = content.strip_prefix("filter ") else {
        return;
    };
    if identifier.is_empty()
        || !identifier.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '_'
        })
        || content.len() < "last 1".len()
    {
        return;
    }

    recovered[content_start..content_end].fill(b' ');
    recovered[content_start..content_start + "last 1".len()]
        .copy_from_slice(b"last 1");
}
