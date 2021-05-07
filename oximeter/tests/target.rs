mod common;
use oximeter::{FieldType, FieldValue, Target};

#[test]
fn test_target() {
    let sled = common::Sled::new();
    assert_eq!("sled", sled.name());
    assert_eq!(&["name", "id"], sled.field_names());
    assert_eq!(vec![FieldType::String, FieldType::Uuid], sled.field_types());
    assert_eq!(
        vec![
            FieldValue::String(sled.name.clone()),
            FieldValue::Uuid(sled.id)
        ],
        sled.field_values()
    );
    assert_eq!(format!("sled:{}:{}", sled.name, sled.id), sled.key());
}
