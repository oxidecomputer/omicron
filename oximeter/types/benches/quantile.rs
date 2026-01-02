// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmarks for the implementation of the P² algorithm with
//! quantile estimation.

// Copyright 2024 Oxide Computer Company

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use oximeter_types::Quantile;
use rand_distr::{Distribution, Normal};

/// Emulates baseline code in a Python implementation of the P²
/// algorithm:
/// <https://github.com/rfrenoy/psquare/blob/master/tests/test_psquare.py#L47>.
fn normal_distribution_quantile(size: i32, p: f64) -> f64 {
    let mu = 500.;
    let sigma = 100.;
    let mut q = Quantile::new(p).unwrap();
    let normal = Normal::new(mu, sigma).unwrap();
    for _ in 0..size {
        q.append(normal.sample(&mut rand::rng())).unwrap();
    }
    q.estimate().unwrap()
}

fn baseline_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Quantile");
    let size = 1000;
    for p in [0.5, 0.9, 0.95, 0.99].iter() {
        group.bench_with_input(
            BenchmarkId::new("Estimate on Normal Distribution", p),
            p,
            |b, p| b.iter(|| normal_distribution_quantile(size, *p)),
        );
    }
    group.finish();
}

criterion_group!(benches, baseline_benchmark);
criterion_main!(benches);
