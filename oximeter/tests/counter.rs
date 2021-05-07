mod common;
use oximeter::{Collector, Measurement};

#[test]
fn test_counter() {
    let disk = common::Disk::new();
    let intr = common::Interrupts::new();
    let mut collector = Collector::new();
    let (_, counter) = collector
        .register_counter(&disk, &intr)
        .expect("Failed to register counter");
    counter.add(10);
    let data = collector.collect().expect("Failed to collect data");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].measurement, Measurement::I64(10));
}
