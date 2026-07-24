// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmark of the pure-CPU per-sample insert work: `timeseries_key`
//! derivation and unrolling samples into table blocks, i.e. the second pass
//! of `Client::unroll_samples` without the schema round-trip or any
//! ClickHouse connection.
//!
//! Reports wall time (criterion) and allocations per sample (counting
//! global allocator; platform-independent, transfers to Helios). Payload
//! matches oximeter/oximeter/benches/deserialize.rs so parse-side and
//! insert-side numbers compose. See plans/oximeter-alloc-bench.md.

use criterion::Criterion;
use criterion::Throughput;
use criterion::criterion_group;
use criterion::criterion_main;
use oximeter::Metric;
use oximeter::Target;
use oximeter::histogram::Histogram;
use oximeter::histogram::Record as _;
use oximeter::types::Cumulative;
use oximeter::types::Sample;
use oximeter_db::bench;
use oximeter_db::native::block::Block;
use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::btree_map::Entry;
use std::hint::black_box;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use uuid::Uuid;

// A global allocator that counts allocations and requested bytes.
struct CountingAllocator;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(
        &self,
        ptr: *mut u8,
        layout: Layout,
        new_size: usize,
    ) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn snapshot() -> (u64, u64) {
    (ALLOCATIONS.load(Ordering::Relaxed), BYTES.load(Ordering::Relaxed))
}

// Same payload shape as oximeter/oximeter/benches/deserialize.rs;
// duplicated so each bench applies to main standalone.
#[derive(Clone, Target)]
struct VirtualMachine {
    pub project_id: Uuid,
    pub instance_id: Uuid,
    pub silo_name: String,
    pub sled_serial: String,
}

#[derive(Clone, Metric)]
struct VcpuBusy {
    pub cpu_id: i64,
    pub state: String,
    pub datum: Cumulative<f64>,
}

#[derive(Clone, Metric)]
struct IoLatency {
    pub disk_id: Uuid,
    pub datum: Histogram<f64>,
}

const N_INSTANCES: usize = 20;
const N_CPUS: i64 = 4;
const N_DISKS: usize = 2;
const N_SAMPLES_PER_SERIES: usize = 2;

fn build_samples() -> Vec<Sample> {
    let mut samples = Vec::new();
    let bins: Vec<f64> = (0..32).map(|i| (1u64 << i) as f64).collect();
    for i in 0..N_INSTANCES {
        let vm = VirtualMachine {
            project_id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
            silo_name: "engineering".to_string(),
            sled_serial: format!("BRM442202{i:02}"),
        };
        for cpu in 0..N_CPUS {
            for s in 0..N_SAMPLES_PER_SERIES {
                for state in ["running", "waiting"] {
                    let metric = VcpuBusy {
                        cpu_id: cpu,
                        state: state.to_string(),
                        datum: Cumulative::new(s as f64),
                    };
                    samples.push(Sample::new(&vm, &metric).unwrap());
                }
            }
        }
        for _ in 0..N_DISKS {
            let disk_id = Uuid::new_v4();
            let mut hist = Histogram::new(&bins).unwrap();
            for x in [3.0, 250.0, 8192.0] {
                hist.sample(x).unwrap();
            }
            for _ in 0..N_SAMPLES_PER_SERIES {
                let metric = IoLatency { disk_id, datum: hist.clone() };
                samples.push(Sample::new(&vm, &metric).unwrap());
            }
        }
    }
    samples
}

// The second pass of Client::unroll_samples: per sample, derive the
// timeseries key; extract field blocks once per (name, key); extract the
// measurement block always; merge blocks per table.
fn unroll(samples: &[Sample]) -> BTreeMap<String, Block> {
    let mut seen_timeseries = HashSet::new();
    let mut table_blocks = BTreeMap::new();
    for sample in samples {
        let key =
            (sample.timeseries_name.as_str(), bench::timeseries_key(sample));
        if !seen_timeseries.contains(&key) {
            for (table_name, block) in bench::extract_fields_as_block(sample)
            {
                match table_blocks.entry(table_name) {
                    Entry::Vacant(entry) => {
                        entry.insert(block);
                    }
                    Entry::Occupied(mut entry) => {
                        bench::concat_blocks(entry.get_mut(), block)
                    }
                }
            }
        }
        let (table_name, measurement_block) =
            bench::extract_measurement_as_block(sample);
        match table_blocks.entry(table_name) {
            Entry::Vacant(entry) => {
                entry.insert(measurement_block);
            }
            Entry::Occupied(mut entry) => {
                bench::concat_blocks(entry.get_mut(), measurement_block)
            }
        }
        seen_timeseries.insert(key);
    }
    table_blocks
}

fn benches(c: &mut Criterion) {
    let samples = build_samples();
    let n_samples = samples.len();

    // Counted passes (second run, so any warm-up is excluded).
    let warmup = unroll(&samples);
    drop(warmup);
    let (a0, b0) = snapshot();
    let blocks = unroll(&samples);
    let (a1, b1) = snapshot();
    drop(blocks);
    println!(
        "unroll: {} samples, {:.1} allocations/sample, {:.0} bytes/sample",
        n_samples,
        (a1 - a0) as f64 / n_samples as f64,
        (b1 - b0) as f64 / n_samples as f64,
    );

    let (a0, b0) = snapshot();
    for sample in &samples {
        black_box(bench::timeseries_key(sample));
    }
    let (a1, b1) = snapshot();
    println!(
        "timeseries_key: {:.1} allocations/sample, {:.0} bytes/sample",
        (a1 - a0) as f64 / n_samples as f64,
        (b1 - b0) as f64 / n_samples as f64,
    );

    let mut group = c.benchmark_group("unroll");
    group.measurement_time(std::time::Duration::from_secs(10));
    group.throughput(Throughput::Elements(n_samples as u64));
    group.bench_function("unroll_samples", |b| {
        b.iter(|| unroll(black_box(&samples)))
    });
    group.bench_function("timeseries_key", |b| {
        b.iter(|| {
            for sample in &samples {
                black_box(bench::timeseries_key(black_box(sample)));
            }
        })
    });
    group.finish();
}

criterion_group!(benches_group, benches);
criterion_main!(benches_group);
