// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Execute blueprints from the command line

use anyhow::Context;
use anyhow::bail;
use camino::Utf8PathBuf;
use clap::ColorChoice;
use clap::Parser;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_reconfigurator_execution::{RequiredRealizeArgs, realize_blueprint};
use nexus_types::deployment::Blueprint;
use omicron_uuid_kinds::GenericUuid;
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
    )]
    log_level: dropshot::ConfigLoggingLevel,

    /// an internal DNS server in this deployment
    // This default value is currently appropriate for all deployed systems.
    // That relies on two assumptions:
    //
    // 1. The internal DNS servers' underlay addresses are at a fixed location
    //    from the base of the AZ subnet.  This is unlikely to change, since the
    //    DNS servers must be discoverable with virtually no other information.
    // 2. The AZ subnet used for all deployments today is fixed.
    //
    // For simulated systems (e.g., `cargo xtask omicron-dev run-all`), or if
    // these assumptions change in the future, we may need to adjust this.
    #[arg(long, default_value = "[fd00:1122:3344:3::1]:53")]
    dns_server: SocketAddr,

    /// Color output
    #[arg(long, value_enum, default_value_t)]
    color: ColorChoice,

    /// path to a serialized (JSON) blueprint file
    blueprint_file: Utf8PathBuf,
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

        let input_path = &self.blueprint_file;
        info!(
            &log,
            "loading blueprint file";
            "input_path" => %input_path
        );
        let file = std::fs::File::open(&self.blueprint_file)
            .with_context(|| format!("open {:?}", input_path))?;
        let bufread = std::io::BufReader::new(file);
        let blueprint: Blueprint = serde_json::from_reader(bufread)
            .with_context(|| format!("read and parse {:?}", &input_path))?;

        // Check that the blueprint is the current system target.
        // (This is currently redundant with a check done early during blueprint
        // realization, but we want to avoid assuming that here in this tool.)
        let target = datastore
            .blueprint_target_get_current(&opctx)
            .await
            .context("loading current target blueprint")?;
        if target.target_id != blueprint.id {
            bail!(
                "requested blueprint {} does not match current target ({})",
                blueprint.id,
                target.target_id
            );
        }

        let (sender, mut receiver) = update_engine::channel();

        let receiver_task = tokio::spawn(async move {
            let mut event_buffer = EventBuffer::default();
            while let Some(event) = receiver.recv().await {
                event_buffer.add_event(event);
            }
            event_buffer
        });

        // This uuid uses similar conventions as the DB fixed data.  It's
        // intended to be recognizable by a human (maybe just as "a little
        // strange looking") and ideally evoke this tool.
        let creator =
            uuid::Uuid::from_u128(0x001de000_4cf4_4000_8000_4ec04f140000);
        //                         "oxide"  "rcfg"         "reconfig[urator]"
        info!(&log, "beginning execution");
        let rv = realize_blueprint(
            RequiredRealizeArgs {
                opctx: &opctx,
                datastore: &datastore,
                resolver: &resolver,
                blueprint: &blueprint,
                creator: OmicronZoneUuid::from_untyped_uuid(creator),
                sender,
            }
            .into(),
        )
        .await
        .context("blueprint execution failed");

        // Get and dump the report from the receiver task.
        let event_buffer =
            receiver_task.await.map_err(|error| NestedError::new(&error))?;
        let mut line_display = LineDisplay::new(std::io::stdout());
        let should_colorize = match self.color {
            ColorChoice::Always => true,
            ColorChoice::Auto => {
                supports_color::on(supports_color::Stream::Stdout).is_some()
            }
            ColorChoice::Never => false,
        };
        if should_colorize {
            line_display.set_styles(LineDisplayStyles::colorized());
        }
        line_display.write_event_buffer(&event_buffer)?;

        rv.map(|_| ())
    }
}
