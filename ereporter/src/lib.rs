use std::collections::BTreeMap;

pub struct Ereport {
    class: String,
    facts: BTreeMap<String, String>,
}
