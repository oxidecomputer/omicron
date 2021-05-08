mod common;
use oximeter::{FieldType, FieldValue, MeasurementType, Metric};

#[test]
fn test_simple_metric() {
    let gauge = common::SimpleGauge::new();
    assert_eq!(gauge.name(), "simple_gauge");
    assert_eq!(gauge.field_names(), &["field"]);
    assert_eq!(gauge.field_types(), &[FieldType::Bool]);
    assert_eq!(gauge.field_values()[0], FieldValue::Bool(false));
    assert_eq!(gauge.measurement_type(), MeasurementType::I64);
    assert_eq!(gauge.key(), "false:simple_gauge");
}
