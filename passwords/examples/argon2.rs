// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Command-line tool for playing with Argon2 parameters

use anyhow::Context;
use argon2::Algorithm;
use argon2::Argon2;
use argon2::Params;
use argon2::Version;
use clap::Parser;
use omicron_passwords::ARGON2_COST_M_KIB;
use omicron_passwords::ARGON2_COST_P;
use omicron_passwords::ARGON2_COST_T;
use omicron_passwords::Hasher;
use omicron_passwords::Password;

/// Quickly check performance of Argon2 hashing with given parameter values
///
/// For a bit more stats, modify the associated benchmark to use your values and
/// run that.  This tool is aimed at more quickly iterating on which values are
/// worth benchmarking.
#[derive(Parser)]
struct Cli {
    /// iterations
    #[arg(long, default_value_t = 5)]
    count: u128,
    /// input (password) to hash
    #[arg(long, default_value_t = String::from("hunter2"))]
    input: String,
    /// argon2 parameter 'm': memory size in 1 KiB blocks
    #[arg(long, default_value_t = ARGON2_COST_M_KIB)]
    m_cost: u32,
    /// argon2 parameter 'p': degree of parallelism
    #[arg(long, default_value_t = ARGON2_COST_P)]
    p_cost: u32,
    /// argon2 parameter 't': number of iterations
    #[arg(long, default_value_t = ARGON2_COST_T)]
    t_cost: u32,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    const ALGORITHM: Algorithm = Algorithm::Argon2id;
    let version = Version::default();
    const OUTPUT_SIZE_OVERRIDE: Option<usize> = None;
    let params =
        Params::new(cli.m_cost, cli.t_cost, cli.p_cost, OUTPUT_SIZE_OVERRIDE)
            .context("unsupported Argon2 parameters")?;
    let argon = Argon2::new(ALGORITHM, version, params);
    let mut hasher = Hasher::new(argon.clone(), rand::rng());
    let password = Password::new(&cli.input).unwrap();
    let password_hash = hasher.create_password(&password).unwrap();

    println!("algorithm:  {} version {:?}", ALGORITHM, version);
    println!("  'm' cost: {} KiB", cli.m_cost);
    println!("  'p' cost: {} (degree of parallelism)", cli.p_cost);
    println!("  't' cost: {} (number of iterations)", cli.t_cost);
    println!("password hash: {}", password_hash);
    println!(
        "output size override: {}",
        OUTPUT_SIZE_OVERRIDE
            .map(|s| s.to_string())
            .as_deref()
            .unwrap_or("none")
    );
    println!("trials: {}", cli.count);

    if cfg!(debug_assertions) {
        eprintln!(
            "WARN: running a debug binary \
            (performance numbers are not meaningful)"
        );
    }

    let start = std::time::Instant::now();
    for i in 0..cli.count {
        eprint!("iter {} ... ", i + 1);
        let iter_start = std::time::Instant::now();
        hasher.verify_password(&password, &password_hash).unwrap();
        let iter_elapsed = iter_start.elapsed();
        eprintln!("{} ms", iter_elapsed.as_millis());
    }

    let total_elapsed = start.elapsed();
    println!(
        "completed {} iteration{} in {} ms (average: {} ms per iteration)",
        cli.count,
        if cli.count == 1 { "" } else { "s" },
        total_elapsed.as_millis(),
        total_elapsed.as_millis() / cli.count,
    );

    Ok(())
}
