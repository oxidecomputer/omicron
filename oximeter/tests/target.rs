mod common;
use oximeter::{FieldType, FieldValue, Target};

#[test]
fn test_simple_target() {
    let sled = common::Sled::new();
    assert_eq!(sled.name(), "sled");
    assert_eq!(sled.field_names(), &["name", "id"]);
    assert_eq!(sled.field_types(), &[FieldType::String, FieldType::Uuid]);
    assert_eq!(
        sled.field_values()[0],
        FieldValue::String(String::from("a-sled-name"))
    );
}
