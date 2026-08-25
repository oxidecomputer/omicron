use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    query: String,
    accepted: bool,
}

#[test]
fn legacy_and_extracted_parsers_match_corpus() {
    let cases: Vec<Case> = serde_json::from_str(include_str!(
        "../../oxql/test-data/query-corpus.json"
    ))
    .unwrap();

    for case in cases {
        let legacy_accepted =
            oximeter_db::oxql::Query::new(&case.query).is_ok();
        let extracted_accepted =
            oxql::parse(&case.query).diagnostics.is_empty();
        assert_eq!(legacy_accepted, case.accepted, "legacy: {}", case.name);
        assert_eq!(
            extracted_accepted, case.accepted,
            "extracted: {}",
            case.name
        );
    }
}
