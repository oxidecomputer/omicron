// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmark of the collector's hot path: deserializing `ProducerResults`
//! from JSON, plus producer-side `Sample` construction.
//!
//! Reports two kinds of numbers:
//!
//! - Wall time (criterion): parse throughput in samples/second.
//! - Allocation counts, via a counting global allocator: heap allocations
//!   and bytes requested per parsed sample. Allocation counts are
//!   platform-independent, so measurements taken on a workstation transfer
//!   directly to Helios (wall time does not — the allocators differ).
//!
//! The payload models a rack-like producer response: per-vCPU cumulative
//! metrics (UUID + string target fields, string metric field) and per-disk
//! I/O latency histograms.
//!
//! See plans/oximeter-allocations.md. To compare against main, this file and
//! the Cargo.toml bench stanza apply cleanly by themselves:
//!
//!   git checkout <branch> -- oximeter/oximeter/benches oximeter/oximeter/Cargo.toml
//!   cargo bench -p oximeter --bench deserialize

use criterion::Criterion;
use criterion::Throughput;
use criterion::criterion_group;
use criterion::criterion_main;
use oximeter::Metric;
use oximeter::Target;
use oximeter::histogram::Histogram;
use oximeter::histogram::Record as _;
use std::hint::black_box;
use oximeter::types::Cumulative;
use oximeter::types::ProducerResults;
use oximeter::types::ProducerResultsItem;
use oximeter::types::Sample;
use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
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

fn parse(payload: &str) -> ProducerResults {
    serde_json::from_str(payload).unwrap()
}

fn benches(c: &mut Criterion) {
    let samples = build_samples();
    let n_samples = samples.len();
    let payload =
        serde_json::to_string(&vec![ProducerResultsItem::Ok(samples)])
            .unwrap();

    // One counted pass outside the timing loop: allocations and bytes per
    // parsed sample. Parse twice and count the second pass, so one-time
    // costs (interner warm-up, if present) don't skew the steady state.
    let warmup = parse(&payload);
    drop(warmup);
    let (a0, b0) = snapshot();
    let parsed = parse(&payload);
    let (a1, b1) = snapshot();
    drop(parsed);
    println!("payload: {} samples, {} bytes of JSON", n_samples, payload.len());
    println!(
        "deserialize: {:.1} allocations/sample, {:.0} bytes/sample",
        (a1 - a0) as f64 / n_samples as f64,
        (b1 - b0) as f64 / n_samples as f64,
    );

    // Same for producer-side construction.
    let vm = VirtualMachine {
        project_id: Uuid::new_v4(),
        instance_id: Uuid::new_v4(),
        silo_name: "engineering".to_string(),
        sled_serial: "BRM44220201".to_string(),
    };
    let metric = VcpuBusy {
        cpu_id: 0,
        state: "running".to_string(),
        datum: Cumulative::new(1.0),
    };
    let warmup = Sample::new(&vm, &metric).unwrap();
    drop(warmup);
    let (a0, b0) = snapshot();
    let constructed = Sample::new(&vm, &metric).unwrap();
    let (a1, b1) = snapshot();
    drop(constructed);
    println!(
        "Sample::new: {} allocations, {} bytes",
        a1 - a0,
        b1 - b0,
    );

    let mut group = c.benchmark_group("deserialize");
    group.measurement_time(std::time::Duration::from_secs(10));
    group.throughput(Throughput::Elements(n_samples as u64));
    group.bench_function("producer_results", |b| {
        b.iter(|| parse(black_box(&payload)))
    });
    group.finish();

    let mut group = c.benchmark_group("construct");
    group.throughput(Throughput::Elements(1));
    group.bench_function("sample_new", |b| {
        b.iter(|| Sample::new(black_box(&vm), black_box(&metric)).unwrap())
    });
    group.finish();
}

criterion_group!(benches_group, benches);
criterion_main!(benches_group);
