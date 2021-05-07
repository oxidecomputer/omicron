mod common;
use oximeter::{FieldType, FieldValue, Metric, MetricKind, MetricType};

#[test]
fn test_simple_metric() {
    let cpu = common::CpuBusy::new();
    assert_eq!("cpu_busy", cpu.name());
    assert_eq!(&["cpu_id"], cpu.field_names());
    assert_eq!(&[FieldType::I64], cpu.field_types());
    assert_eq!(vec![FieldValue::I64(cpu.cpu_id)], cpu.field_values());
    assert_eq!(MetricKind::Gauge, cpu.metric_kind());
    assert_eq!(MetricType::I64, cpu.metric_type());
    assert_eq!(format!("{}:cpu_busy", cpu.cpu_id), cpu.key());
}
