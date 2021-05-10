use std::boxed::Box;

mod common;
use oximeter::{
    traits::{Metric, Producer, Target},
    types::{Measurement, Sample},
};

#[derive(Debug, Clone)]
pub struct GaugeProducer {
    pub target: common::Sled,
    pub metric: common::SimpleGauge,
    pub value: i64,
}

impl Producer for GaugeProducer {
    fn produce(&mut self) -> Box<dyn Iterator<Item = Sample>> {
        Box::new(
            vec![Sample::new(
                self.target.clone(),
                self.metric.clone(),
                self.value,
                None,
            )]
            .into_iter(),
        )
    }
}

#[test]
fn test_producer() {
    let sled = common::Sled::new();
    let gauge = common::SimpleGauge::new();
    let mut producer = GaugeProducer {
        target: sled,
        metric: gauge,
        value: 0,
    };

    let sample = producer.produce().next().unwrap();
    assert_eq!(sample.measurement, Measurement::I64(0));

    producer.value = 10;
    let sample = producer.produce().next().unwrap();
    assert_eq!(sample.measurement, Measurement::I64(10));
}

pub struct MultiProducer {
    pub sled_target: common::Sled,
    pub disk_target: common::Disk,
    pub metric: common::SimpleGauge,
    pub value: i64,
}

impl Producer for MultiProducer {
    fn produce(&mut self) -> Box<dyn Iterator<Item = Sample>> {
        let data = vec![
            Sample::new(
                self.sled_target.clone(),
                self.metric.clone(),
                self.value,
                None,
            ),
            Sample::new(
                self.disk_target.clone(),
                self.metric.clone(),
                self.value,
                None,
            ),
        ];
        Box::new(data.into_iter())
    }
}

#[test]
fn test_producer_multiple_types() {
    let sled = common::Sled::new();
    let disk = common::Disk::new();
    let gauge = common::SimpleGauge::new();
    let mut producer = MultiProducer {
        sled_target: sled.clone(),
        disk_target: disk.clone(),
        metric: gauge.clone(),
        value: 0,
    };

    let samples: Vec<_> = producer.produce().collect();
    assert_eq!(samples[0].measurement, Measurement::I64(0));
    assert_eq!(samples[0].target.key, sled.key());
    assert_eq!(samples[0].metric.key, gauge.key());
    assert_eq!(samples[1].target.key, disk.key());
    assert_eq!(samples[1].metric.key, gauge.key());
}
