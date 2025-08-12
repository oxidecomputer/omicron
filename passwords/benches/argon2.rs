// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmarks password hashing

use argon2::Algorithm;
use argon2::Argon2;
use argon2::Params;
use argon2::Version;
use criterion::{Criterion, criterion_group, criterion_main};
use omicron_passwords::Hasher;
use omicron_passwords::Password;

fn password_benchmark(c: &mut Criterion) {
    let password = Password::new("hunter2").unwrap();
    struct Config {
        name: &'static str,
        params: Argon2<'static>,
    }

    let configs = vec![
        Config {
            name: "our parameters",
            params: omicron_passwords::external_password_argon(),
        },
        Config { name: "argon2 crate default", params: Argon2::default() },
        // These OWASP parameters come from the Password Storage Cheat Sheet.
        Config {
            name: "OWASP #1",
            params: Argon2::new(
                Algorithm::Argon2id,
                Version::default(),
                Params::new(37 * 1024, 1, 1, None).unwrap(),
            ),
        },
        Config {
            name: "OWASP #2",
            params: Argon2::new(
                Algorithm::Argon2id,
                Version::default(),
                Params::new(15 * 1024, 2, 1, None).unwrap(),
            ),
        },
        Config {
            name: "100 MiB, 12 iterations",
            params: Argon2::new(
                Algorithm::Argon2id,
                Version::default(),
                Params::new(100 * 1024, 12, 1, None).unwrap(),
            ),
        },
    ];

    let mut group = c.benchmark_group("Password verify");
    for c in &configs {
        let mut hasher = Hasher::new(c.params.clone(), rand::rng());
        let hash_str = hasher.create_password(&password).unwrap();
        group.bench_function(c.name, |b| {
            b.iter(|| {
                hasher.verify_password(&password, &hash_str).unwrap();
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("Password create");
    for c in configs {
        let mut hasher = Hasher::new(c.params, rand::rng());
        group.bench_function(c.name, |b| {
            b.iter(|| {
                hasher.create_password(&password).unwrap();
            });
        });
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = password_benchmark
);
criterion_main!(benches);
