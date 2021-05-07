mod common;
use oximeter::{Collector, FieldValue};

#[test]
fn test_single_producer() {
    let sled = common::Sled::new();
    let cpu = common::CpuBusy::new();
    let mut collector = Collector::new();
    let sampler = common::CpuSampler::new(1);
    let token = collector
        .register(&sled, &cpu, &sampler)
        .expect("Failed to register single metric");
    assert_eq!(
        collector
            .collection_info(token)
            .expect("Failed to get collection info")
            .0,
        1,
        "Expected a collection with length 1"
    );
    let data = collector.collect().expect("Failed to collect metrics");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].target_name, "sled");
    assert_eq!(
        data[0].target_fields["name"],
        FieldValue::String(sled.name.clone())
    );
    assert!(collector.deregister(token).is_ok());
    assert!(collector.deregister(token).is_err());
}

#[test]
fn test_collection() {
    let sled = common::Sled::new();
    let cpu = common::CpuBusy::new();
    let (sleds, cpus) = {
        let mut sleds = Vec::with_capacity(10);
        let mut cpus = Vec::with_capacity(10);
        for i in 0..cpus.capacity() {
            let mut cpu_ = cpu.clone();
            cpu_.cpu_id = i as _;
            cpus.push(cpu_);
            sleds.push(sled.clone());
        }
        (sleds, cpus)
    };
    let sampler = common::CpuSampler::new(cpus.len());
    let mut collector = Collector::new();
    let token = collector
        .register_collection(&sleds, &cpus, &sampler)
        .expect("Failed to register collection of metrics");
    assert_eq!(collector.n_collections(), 1);
    assert_eq!(collector.n_items(), sleds.len());
    let data = collector.collect().expect("Failed to collect data");
    assert_eq!(data.len(), collector.n_items());
    assert_eq!(data[0].target_name, "sled");
    assert_eq!(data[0].target_fields["name"], FieldValue::String(sled.name));
    assert!(collector.deregister(token).is_ok());
    assert!(collector.deregister(token).is_err());
}

#[test]
fn test_collection_type_mismatch() {
    let sled = common::Sled::new();
    let cpu = common::CpuBusy::new();
    let mut collector = Collector::new();
    let bad_producer = oximeter::Distribution::<f64>::new(&[0f64, 1.0, 2.0]).unwrap();
    let _ = collector
        .register(&sled, &cpu, &bad_producer)
        .expect("Failed to register single metric");
    assert!(matches!(
        collector.collect(),
        Err(oximeter::Error::ProducerTypeMismatch(_, _))
    ));
}
