// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmarks turning zone log zips downloaded from sled agents into a
//! support bundle.
//!
//! Setup builds one zip per zone the way sled-diagnostics does, with one
//! zstd-compressed entry per log file. Each iteration then places those zips
//! in a fresh collection directory as though they had just been downloaded,
//! runs [`prepare_zone_log_zip`] on each of them, and writes the bundle with
//! [`bundle_to_writer`].
//!
//! The uncompressed logs total 100 MiB by default; set `ZONE_LOGS_BENCH_MIB`
//! to change that.

use camino::Utf8Path;
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use std::io::Write;
use std::time::{Duration, Instant};
use support_bundle_collection::zip::{bundle_to_writer, prepare_zone_log_zip};
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

/// A small deterministic generator (xorshift64), so every run uses the same
/// logs.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Generates about `len` bytes of bunyan-style log lines.
fn fake_log(rng: &mut Rng, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 512);
    let mut time_ns: u64 = 0;
    while out.len() < len {
        time_ns += rng.next() % 50_000_000;
        let secs = time_ns / 1_000_000_000;
        let nanos = time_ns % 1_000_000_000;
        let msg = MESSAGES[rng.below(MESSAGES.len())];
        let level = [20, 30, 30, 40, 50][rng.below(5)];
        let req_id = rng.next();
        let latency_us = rng.next() % 5_000_000;
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
fn fake_zone_zip(rng: &mut Rng, bytes_per_log: usize) -> Vec<u8> {
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
    let zip_paths: Vec<_> = zone_zips
        .iter()
        .enumerate()
        .map(|(i, bytes)| {
            let zone_dir = dir.path().join(format!("logs/oxz_fake_zone_{i}"));
            std::fs::create_dir_all(&zone_dir).unwrap();
            let zip_path = zone_dir.join("logs.zip");
            std::fs::write(&zip_path, bytes).unwrap();
            zip_path
        })
        .collect();
    let mut bundle = camino_tempfile::tempfile().unwrap();

    let start = Instant::now();
    for zip_path in &zip_paths {
        prepare_zone_log_zip(zip_path).unwrap();
    }
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
    let mut rng = Rng(0x5eed);
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
    group.bench_function(format!("prepare_and_bundle_{total_mib}MiB"), |b| {
        b.iter_custom(|iters| {
            (0..iters).map(|_| build_bundle(&zone_zips).0).sum()
        })
    });
    group.finish();
}

criterion_group!(benches, zone_logs);
criterion_main!(benches);
