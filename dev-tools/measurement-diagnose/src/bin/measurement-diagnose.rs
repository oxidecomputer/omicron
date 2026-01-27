// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Diagnose problems with reference measurements on a specific sled
//! This is designed to be run from the global zone of the specific sled.
//!
use anyhow::Context;
use anyhow::bail;
use attest_data::MeasurementLog;
use clap::Parser;
use clap::Subcommand;
use dice_verifier::ipcc::AttestIpcc;
use dice_verifier::{
    Attest, MeasurementSet, ReferenceMeasurements, VerifyMeasurementsError,
    verify_measurements,
};
use sled_agent_types::inventory::ConfigReconcilerInventoryResult;

fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    oxide_tokio_rt::run(args.exec())
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List the measurements from the inventory
    List,
    /// Attempt to diagnose what measurement is incorrect
    Diagnose,
}

#[derive(Debug, Parser)]
struct Args {
    /// log level filter
    #[arg(
        env,
        long,
        value_parser = parse_dropshot_log_level,
        default_value = "warn",
        global = true,
    )]
    log_level: dropshot::ConfigLoggingLevel,

    /// URL of the Sled internal API
    #[clap(long, env = "OMDB_SLED_AGENT_URL", global = true)]
    sled_agent_url: Option<String>,

    #[command(subcommand)]
    command: Command,
}

impl Args {
    async fn exec(&self) -> Result<(), anyhow::Error> {
        // This is a little goofy. The sled URL is required, but can come
        // from the environment, in which case it won't be on the command line.
        let Some(ref sled_agent_url) = self.sled_agent_url else {
            bail!(
                "sled URL must be specified with --sled-agent-url or \
                OMDB_SLED_AGENT_URL"
            );
        };

        let log = dropshot::ConfigLogging::StderrTerminal {
            level: self.log_level.clone(),
        }
        .to_logger("measurement-diagnose")
        .context("failed to create logger")?;

        let client =
            sled_agent_client::Client::new(&sled_agent_url, log.clone());

        match self.command {
            Command::List => {
                list_measurements(&client).await?;
            }
            Command::Diagnose => {
                diagnose_measurements(&client).await?;
            }
        }
    
        Ok(())
    }

}

fn parse_dropshot_log_level(
    s: &str,
) -> Result<dropshot::ConfigLoggingLevel, anyhow::Error> {
    serde_json::from_str(&format!("{:?}", s)).context("parsing log level")
}

async fn diagnose_measurements(
    client: &sled_agent_client::Client,
) -> Result<(), anyhow::Error> {
    // We access our certs over IPCC
    let ipcc = AttestIpcc::new()?;

    let certs = ipcc.get_certificates()?;

    let log = ipcc.get_measurement_log()?;

    // We do this intentionally to get two sets
    let cert_set =
        MeasurementSet::from_artifacts(&certs, &MeasurementLog::default())?;

    let measurement_set = MeasurementSet::from_artifacts(&Vec::new(), &log)?;

    let response = client.inventory().await.context("inventory")?;
    let inventory = response.into_inner();
    let reference_measurements = inventory.reference_measurements;

    let mut corims = Vec::new();

    for m in reference_measurements {
        match m.result {
            ConfigReconcilerInventoryResult::Ok => {
                corims.push(dice_verifier::Corim::from_file(m.path)?);
            }
            ConfigReconcilerInventoryResult::Err { message: _ } => {}
        }
    }

    let reference = ReferenceMeasurements::try_from(corims.as_slice())?;

    if let Err(VerifyMeasurementsError::NotSubset(s)) =
        verify_measurements(&cert_set, &reference)
    {
        println!("The measurement from the certificate is missing: {s}");
    }

    if let Err(VerifyMeasurementsError::NotSubset(s)) =
        verify_measurements(&measurement_set, &reference)
    {
        println!("The measurements from the log are missing: {s}");
    }

    Ok(())
}

async fn list_measurements(
    client: &sled_agent_client::Client,
) -> Result<(), anyhow::Error> {
    let response = client.inventory().await.context("inventory")?;
    let inventory = response.into_inner();
    let reference_measurements = inventory.reference_measurements;
    println!("reference measurements");
    for m in reference_measurements {
        match m.result {
            ConfigReconcilerInventoryResult::Ok => {
                let corim = dice_verifier::Corim::from_file(m.path)?;
                println!("{corim}");
            }
            ConfigReconcilerInventoryResult::Err { message } => {
                println!("Error: {message}");
            }
        }
    }

    Ok(())
}
