// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmarks turning zone log zips downloaded from sled agents into a
//! support bundle.
//!
//! Setup builds one zip per zone the way sled-diagnostics does, with one
//! zstd-compressed entry per log file. Each iteration then places those zips
//! in a fresh collection directory as though they had just been downloaded,
//! and writes the bundle with [`bundle_to_writer`].
//!
//! The uncompressed logs total 100 MiB by default; set `ZONE_LOGS_BENCH_MIB`
//! to change that.

use camino::Utf8Path;
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::io::Write;
use std::time::{Duration, Instant};
use support_bundle_collection::zip::{MERGE_ZIP_SUFFIX, bundle_to_writer};
use zip::write::FullFileOptions;

const ZONES: usize = 20;
const SERVICES_PER_ZONE: usize = 2;
/// The current log, plus rotated logs.
const LOGS_PER_SERVICE: usize = 3;
const DEFAULT_TOTAL_MIB: usize = 100;

const MESSAGES: [&str; 6] = [
    "fake request completed",
    "fake background task activated",
    "fake connection established",
    "fake cache miss, fetching fake object",
    "fake retry scheduled after fake error",
    "fake state transition",
];

/// Generates about `len` bytes of bunyan-style log lines.
fn fake_log(rng: &mut StdRng, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 512);
    let mut time_ns: u64 = 0;
    while out.len() < len {
        time_ns += rng.random_range(0..50_000_000);
        let secs = time_ns / 1_000_000_000;
        let nanos = time_ns % 1_000_000_000;
        let msg = MESSAGES[rng.random_range(0..MESSAGES.len())];
        let level = [20, 30, 30, 40, 50][rng.random_range(0..5)];
        let req_id: u64 = rng.random();
        let latency_us = rng.random_range(0..5_000_000);
        writeln!(
            out,
            "{{\"msg\":\"{msg}\",\"v\":0,\"name\":\"fake-service\",\
             \"level\":{level},\"time\":\"2000-01-01T{:02}:{:02}:{:02}.\
             {nanos:09}Z\",\"hostname\":\"fake-host\",\"pid\":1,\
             \"req_id\":\"{req_id:016x}\",\"latency_us\":{latency_us}}}",
            (secs / 3600) % 24,
            (secs / 60) % 60,
            secs % 60,
        )
        .unwrap();
    }
    out
}

/// Builds a zone's log zip, laid out and compressed the way sled-diagnostics
/// builds them.
fn fake_zone_zip(rng: &mut StdRng, bytes_per_log: usize) -> Vec<u8> {
    let options = FullFileOptions::default()
        .compression_method(zip::CompressionMethod::Zstd)
        .compression_level(Some(3))
        .large_file(true);
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for service in 0..SERVICES_PER_ZONE {
        for log in 0..LOGS_PER_SERVICE {
            let logtype = if log == 0 { "current" } else { "archive" };
            zip.start_file(
                format!(
                    "fake-service-{service}/{logtype}/fake-service-{service}.log.{log}"
                ),
                options.clone(),
            )
            .unwrap();
            zip.write_all(&fake_log(rng, bytes_per_log)).unwrap();
        }
    }
    zip.finish().unwrap().into_inner()
}

fn dir_size(dir: &Utf8Path) -> u64 {
    let mut total = 0;
    for entry in dir.read_dir_utf8().unwrap() {
        let entry = entry.unwrap();
        let metadata = entry.metadata().unwrap();
        if metadata.is_dir() {
            total += dir_size(entry.path());
        } else {
            total += metadata.len();
        }
    }
    total
}

struct Sizes {
    /// Bytes in the collection directory just before it is zipped.
    staged: u64,
    /// Bytes in the final bundle.
    bundle: u64,
}

/// Turns the zone zips into a bundle, returning how long that took.
fn build_bundle(zone_zips: &[Vec<u8>]) -> (Duration, Sizes) {
    let dir = camino_tempfile::tempdir().unwrap();
    for (i, bytes) in zone_zips.iter().enumerate() {
        let zone_dir = dir.path().join(format!("logs/oxz_fake_zone_{i}"));
        std::fs::create_dir_all(&zone_dir).unwrap();
        std::fs::write(zone_dir.join(format!("logs{MERGE_ZIP_SUFFIX}")), bytes)
            .unwrap();
    }
    let mut bundle = camino_tempfile::tempfile().unwrap();

    let start = Instant::now();
    bundle_to_writer(&dir, &mut bundle).unwrap();
    let elapsed = start.elapsed();

    let sizes = Sizes {
        staged: dir_size(dir.path()),
        bundle: bundle.metadata().unwrap().len(),
    };
    (elapsed, sizes)
}

fn zone_logs(c: &mut Criterion) {
    let total_mib = std::env::var("ZONE_LOGS_BENCH_MIB")
        .map(|mib| mib.parse().expect("ZONE_LOGS_BENCH_MIB is a number"))
        .unwrap_or(DEFAULT_TOTAL_MIB);
    let bytes_per_log =
        total_mib * (1 << 20) / (ZONES * SERVICES_PER_ZONE * LOGS_PER_SERVICE);
    // A fixed seed, so that every run uses the same logs.
    let mut rng = StdRng::seed_from_u64(0x5eed);
    let zone_zips: Vec<_> =
        (0..ZONES).map(|_| fake_zone_zip(&mut rng, bytes_per_log)).collect();

    // Criterion only reports time, so report the sizes that matter here once.
    let mib = |bytes: u64| bytes as f64 / f64::from(1 << 20);
    let zipped: u64 = zone_zips.iter().map(|z| z.len() as u64).sum();
    let (_, sizes) = build_bundle(&zone_zips);
    eprintln!(
        "zone_logs: {total_mib} MiB of logs in {ZONES} zone zips \
         ({:.1} MiB); collection directory before zipping: {:.1} MiB; \
         bundle: {:.1} MiB",
        mib(zipped),
        mib(sizes.staged),
        mib(sizes.bundle),
    );

    let mut group = c.benchmark_group("zone_logs");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function(format!("bundle_{total_mib}MiB"), |b| {
        b.iter_custom(|iters| {
            (0..iters).map(|_| build_bundle(&zone_zips).0).sum()
        })
    });
    group.finish();
}

criterion_group!(benches, zone_logs);
criterion_main!(benches);
