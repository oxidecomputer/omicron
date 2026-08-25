use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    query: String,
    accepted: bool,
}

fn corpus() -> Vec<Case> {
    serde_json::from_str(include_str!("../test-data/query-corpus.json"))
        .unwrap()
}

#[test]
fn query_corpus() {
    for case in corpus() {
        let result = oxql::parse(&case.query);
        assert_eq!(
            result.diagnostics.is_empty(),
            case.accepted,
            "{}: {:#?}",
            case.name,
            result.diagnostics,
        );
    }
}

#[test]
fn query_length_limit_is_structured() {
    let query = format!("get a:b{}", " | last 1".repeat(oxql::MAX_QUERY_LEN));
    let result = oxql::parse(&query);
    assert_eq!(result.diagnostics[0].code, "query-too-long");
    assert_eq!(result.diagnostics[0].phase, "parse");
}

#[test]
fn syntax_error_has_location() {
    let result = oxql::parse("get vm");
    let diagnostic = &result.diagnostics[0];
    assert_eq!(diagnostic.code, "invalid-syntax");
    assert_eq!(diagnostic.line, Some(1));
    assert!(diagnostic.column.is_some());
    assert!(diagnostic.message.starts_with("Error at 1:"));
}
