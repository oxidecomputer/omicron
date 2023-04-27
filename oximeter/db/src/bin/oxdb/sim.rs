// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Methods for simulating timeseries data, useful for developing against the
//! telemetry database.

// Copyright 2023 Oxide Computer Company

use crate::PopulateArgs;
use anyhow::Context;
use oximeter::types::Cumulative;
use oximeter::types::Sample;
use oximeter::Metric;
use oximeter::Target;
use oximeter_db::Client;
use oximeter_db::DbWrite;
use slog::debug;
use slog::info;
use slog::Logger;
use uuid::Uuid;

// Samples are inserted in chunks of this size, to avoid large allocations when inserting huge
// numbers of timeseries.
const INSERT_CHUNK_SIZE: usize = 100_000;

/// A target identifying a single virtual machine instance
#[derive(Debug, Clone, Copy, Target)]
struct VirtualMachine {
    pub project_id: Uuid,
    pub instance_id: Uuid,
}

impl VirtualMachine {
    pub fn new() -> Self {
        Self { project_id: Uuid::new_v4(), instance_id: Uuid::new_v4() }
    }
}

/// A metric recording the total time a vCPU is busy, by its ID
#[derive(Debug, Clone, Copy, Metric)]
struct CpuBusy {
    pub cpu_id: i64,
    #[datum]
    pub busy: Cumulative<f64>,
}

/// Describe the simulated data.
pub fn describe_data() {
    let vm = VirtualMachine::new();
    print!("Target:\n\n Name: {target_name:?}\n", target_name = vm.name());
    for (i, (field_name, field_type)) in
        vm.field_names().iter().zip(vm.field_types()).enumerate()
    {
        print!(
            " Field {i}:\n   Name: {field_name:?}\n   Type: {field_type}\n",
            i = i,
            field_name = field_name,
            field_type = field_type
        );
    }

    let cpu = CpuBusy { cpu_id: 0, busy: 0.0f64.into() };
    print!(
        "\nMetric:\n\n Name: {metric_name:?}\n Type: {ty:?}\n",
        metric_name = cpu.name(),
        ty = cpu.datum_type()
    );
    for (i, (field_name, field_type)) in
        cpu.field_names().iter().zip(cpu.field_types()).enumerate()
    {
        print!(
            " Field {i}:\n   Name: {field_name:?}\n   Type: {field_type}\n",
            i = i,
            field_name = field_name,
            field_type = field_type
        );
    }
}

// Insert simulated samples into the timeseries database.
async fn insert_samples(
    client: &Client,
    log: &Logger,
    samples: &Vec<Sample>,
    dry_run: bool,
) -> Result<(), anyhow::Error> {
    debug!(
        log,
        "inserting {} simulated samples data into database",
        samples.len();
        "dry_run" => dry_run
    );
    if !dry_run {
        client
            .insert_samples(samples)
            .await
            .context("Failed to insert samples")?;
    }
    Ok(())
}

/// Populate the timeseries database with simulated data.
pub async fn populate(
    client: &Client,
    log: &Logger,
    args: PopulateArgs,
) -> Result<(), anyhow::Error> {
    info!(log, "populating Oximeter database");
    let n_timeseries = args.n_projects * args.n_instances * args.n_cpus;
    debug!(
        log,
        "generating simulated data";
        "n_projects" => args.n_projects,
        "n_instances" => args.n_instances,
        "n_cpus" => args.n_cpus,
        "n_samples" => args.n_samples,
        "n_timeseries" => n_timeseries,
    );

    let chunk_size = (args.n_samples * n_timeseries).min(INSERT_CHUNK_SIZE);
    let mut samples = Vec::with_capacity(chunk_size);
    for _ in 0..args.n_projects {
        let project_id = Uuid::new_v4();
        for _ in 0..args.n_instances {
            let vm = VirtualMachine { project_id, instance_id: Uuid::new_v4() };
            info!(
                log,
                "simulating vm";
                "project_id" => %vm.project_id,
                "instance_id" => %vm.instance_id
            );
            for cpu in 0..args.n_cpus {
                for sample in 0..args.n_samples {
                    let cpu_busy = CpuBusy {
                        cpu_id: cpu as _,
                        busy: Cumulative::from(sample as f64),
                    };
                    let sample = Sample::new(&vm, &cpu_busy);
                    samples.push(sample);
                    if samples.len() == chunk_size {
                        insert_samples(client, log, &samples, args.dry_run)
                            .await?;
                        samples.clear();
                    }
                }
            }
        }
    }
    if !samples.is_empty() {
        insert_samples(client, log, &samples, args.dry_run).await?;
    }
    Ok(())
}
