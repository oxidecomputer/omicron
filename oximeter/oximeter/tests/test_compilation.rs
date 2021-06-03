#[test]
#[ignore]
fn test_compilation() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/fail/*.rs");
}
