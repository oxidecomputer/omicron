use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    source: String,
    expected_site: oxql::CompletionSite,
    expected_names: Vec<String>,
}

fn corpus() -> Vec<Case> {
    serde_json::from_str(include_str!("../test-data/completion-corpus.json"))
        .unwrap()
}

#[test]
fn matches_lezer_probe_cases() {
    for case in corpus() {
        let context =
            oxql::completion_context(&case.source, case.source.len()).unwrap();
        assert_eq!(context.site, case.expected_site, "{}", case.source);
        assert_eq!(
            context.referenced_timeseries, case.expected_names,
            "{}",
            case.source
        );
    }
}

#[test]
fn replacement_span_covers_the_partial_token() {
    let source = "get hardware_component:";
    let context = oxql::completion_context(source, source.len()).unwrap();
    assert_eq!(
        &source[context.replacement.start..context.replacement.end],
        "hardware_component:"
    );

    let source = "get a:b | filter x == 0 || sl";
    let context = oxql::completion_context(source, source.len()).unwrap();
    assert_eq!(
        &source[context.replacement.start..context.replacement.end],
        "sl"
    );
}

#[test]
fn rejects_a_cursor_inside_a_utf8_codepoint() {
    let error = oxql::completion_context("é", 1).unwrap_err();
    assert_eq!(error.code, "invalid-cursor");
}
