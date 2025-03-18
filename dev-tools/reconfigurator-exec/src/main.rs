// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Execute blueprints from the command line

use anyhow::Context;
use anyhow::anyhow;
use camino::Utf8PathBuf;
use clap::Parser;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_reconfigurator_execution::realize_blueprint;
use nexus_types::deployment::UnstableReconfiguratorState;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use slog::info;
use std::net::SocketAddr;
use std::sync::Arc;
use update_engine::EventBuffer;
use update_engine::NestedError;
use update_engine::display::LineDisplay;
use update_engine::display::LineDisplayStyles;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = ReconfiguratorExec::parse();

    if let Err(error) = args.exec().await {
        eprintln!("error: {:#}", error);
        std::process::exit(1);
    }

    Ok(())
}

/// Execute blueprints from the command line
#[derive(Debug, Parser)]
struct ReconfiguratorExec {
    /// log level filter
    #[arg(
        env,
        long,
        value_parser = parse_dropshot_log_level,
        default_value = "info",
        global = true,
    )]
    log_level: dropshot::ConfigLoggingLevel,

    /// an internal DNS server in this deployment
    dns_server: SocketAddr,

    /// path to a reconfigurator save file
    reconfigurator_save_file: Utf8PathBuf,

    /// blueprint (contained in the save file) to execute
    blueprint_id: BlueprintUuid,
}

fn parse_dropshot_log_level(
    s: &str,
) -> Result<dropshot::ConfigLoggingLevel, anyhow::Error> {
    serde_json::from_str(&format!("{:?}", s)).context("parsing log level")
}

impl ReconfiguratorExec {
    async fn exec(self) -> Result<(), anyhow::Error> {
        let log = dropshot::ConfigLogging::StderrTerminal {
            level: self.log_level.clone(),
        }
        .to_logger("reconfigurator-exec")
        .context("failed to create logger")?;

        info!(&log, "setting up resolver");
        let qorb_resolver =
            internal_dns_resolver::QorbResolver::new(vec![self.dns_server]);

        info!(&log, "setting up database pool");
        let pool = Arc::new(db::Pool::new(&log, &qorb_resolver));
        let datastore = Arc::new(
            DataStore::new_failfast(&log, pool)
                .await
                .context("creating datastore")?,
        );

        let result = self.do_exec(log, &datastore).await;
        datastore.terminate().await;
        result
    }

    async fn do_exec(
        &self,
        log: slog::Logger,
        datastore: &Arc<DataStore>,
    ) -> Result<(), anyhow::Error> {
        info!(&log, "setting up arguments for execution");
        let opctx = OpContext::for_tests(log.clone(), datastore.clone());
        let resolver = internal_dns_resolver::Resolver::new_from_addrs(
            log.clone(),
            &[self.dns_server],
        )
        .with_context(|| {
            format!(
                "creating DNS resolver for DNS server {:?}",
                self.dns_server
            )
        })?;

        let input_path = &self.reconfigurator_save_file;
        info!(
            &log,
            "loading reconfigurator state file";
            "input_path" => %input_path
        );
        let file = std::fs::File::open(&self.reconfigurator_save_file)
            .with_context(|| format!("open {:?}", input_path))?;
        let bufread = std::io::BufReader::new(file);
        let bundle: UnstableReconfiguratorState =
            serde_json::from_reader(bufread)
                .with_context(|| format!("read and parse {:?}", &input_path))?;

        info!(
            &log,
            "loading blueprint from state file";
            "blueprint_id" => %self.blueprint_id
        );
        let blueprint = bundle
            .blueprints
            .iter()
            .find(|b| b.id == self.blueprint_id)
            .ok_or_else(|| {
                anyhow!(
                    "blueprint {:?} not found in {:?}",
                    self.blueprint_id,
                    &input_path
                )
            })?;

        let (sender, mut receiver) = update_engine::channel();

        let receiver_task = tokio::spawn(async move {
            let mut event_buffer = EventBuffer::default();
            while let Some(event) = receiver.recv().await {
                event_buffer.add_event(event);
            }
            event_buffer
        });

        info!(&log, "beginning execution");
        let rv = realize_blueprint(
            &opctx,
            &datastore,
            &resolver,
            &blueprint,
            None,
            sender,
        )
        .await
        .context("blueprint execution failed");

        // Get and dump the report from the receiver task.
        let event_buffer =
            receiver_task.await.map_err(|error| NestedError::new(&error))?;
        let mut line_display = LineDisplay::new(std::io::stdout());
        // XXX-dap make conditional
        // if should_colorize(color, supports_color::Stream::Stdout) {
        line_display.set_styles(LineDisplayStyles::colorized());
        // }
        line_display.write_event_buffer(&event_buffer)?;

        rv.map(|_| ())
    }
}
