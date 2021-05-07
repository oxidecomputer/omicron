mod common;
use common::{Disk, IoLatency};
use oximeter::{Collector, Measurement};

#[test]
fn test_simple_distribution() {
    let disk = Disk::new();
    let io = IoLatency::new();
    let mut collector = Collector::new();
    let bins = vec![1e-6, 1e-3, 1e-2, 1e-1, 1.0];
    let (tok, mut dist) = collector
        .register_distribution_f64(&disk, &io, &bins)
        .unwrap();

    let samples = &[0.5, 0.4, 2e-6, 3e-6, 0.0];
    let expected_counts = &[1, 2, 0, 0, 2];
    for &sample in samples {
        dist.sample(sample)
            .expect("Failed to insert sample into distribution");
    }
    let data = collector.collect().expect("Failed to collect data");
    let dist = &data[0];
    assert_eq!(dist.target_name, "disk");
    assert_eq!(dist.metric_name, "io_latency");
    assert!(
        matches!(dist.measurement, Measurement::DistributionF64(_)),
        "Expected a distribution over floats"
    );
    match dist.measurement {
        Measurement::DistributionF64(ref d) => {
            for (i, (bin, count)) in d.iter().take(bins.len() - 1).enumerate() {
                assert!(
                    (*bin.end() - bins[i]).abs() <= f64::EPSILON,
                    "Right bin edges of returned distribution do not match"
                );
                assert_eq!(
                    *count, expected_counts[i],
                    "Distribution was not sampled correctly"
                );
            }
        }
        _ => {}
    }
    assert!(collector.deregister(tok).is_ok());
    assert!(collector.deregister(tok).is_err());
    assert_eq!(collector.n_collections(), 0);
}
