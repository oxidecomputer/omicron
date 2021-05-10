mod common;
use oximeter::{
    types::{Measurement, Sample},
    Metric, Target,
};

#[test]
fn test_sample() {
    let sled = common::Sled::new();
    let gauge = common::SimpleGauge::new();
    let data_point: i64 = 10;
    let sample = Sample::new(sled.clone(), gauge.clone(), data_point, None);
    assert_eq!(sample.target.name, sled.name());
    assert_eq!(sample.target.field_values, sled.field_values());
    assert_eq!(sample.metric.key, gauge.key());
    assert_eq!(sample.measurement, Measurement::I64(data_point));
}
