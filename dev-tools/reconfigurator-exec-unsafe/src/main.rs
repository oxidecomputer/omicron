// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Execute blueprints from the command line

use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use camino::Utf8PathBuf;
use clap::ColorChoice;
use clap::Parser;
use dropshot::PaginationOrder;
use internal_dns_types::names::ServiceName;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_mgs_updates::ArtifactCache;
use nexus_mgs_updates::MgsUpdateDriver;
use nexus_reconfigurator_execution::{RequiredRealizeArgs, realize_blueprint};
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::PendingMgsUpdates;
use nexus_types::deployment::SledFilter;
use omicron_common::api::external::DataPageParams;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use qorb::resolver::Resolver;
use qorb::resolvers::fixed::FixedResolver;
use slog::info;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use update_engine::EventBuffer;
use update_engine::NestedError;
use update_engine::display::LineDisplay;
use update_engine::display::LineDisplayStyles;

fn main() -> Result<(), anyhow::Error> {
    let args = ReconfiguratorExec::parse();

    if let Err(error) = oxide_tokio_rt::run(args.exec()) {
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

    /// enable MGS-managed updates (will keep the process running after
    /// blueprint execution completes)
    #[arg(long, default_value_t = false)]
    mgs_updates: bool,

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

        let result = self.do_exec(log, &datastore, &qorb_resolver).await;
        datastore.terminate().await;
        result
    }

    async fn do_exec(
        &self,
        log: slog::Logger,
        datastore: &Arc<DataStore>,
        qorb_resolver: &internal_dns_resolver::QorbResolver,
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

        // If requested, set up a driver for MGS-managed updates.
        let (mgs_updates, mgs) = if self.mgs_updates {
            info!(&log, "setting up MGS update driver");
            let mut mgs_resolver = qorb_resolver
                .for_service(ServiceName::ManagementGatewayService);
            let mgs_rx = mgs_resolver.monitor();
            // Pick an arbitrary in-service sled to act as our repo depot
            // server.
            let repo_depot_sled = datastore
                .sled_list(
                    &opctx,
                    &DataPageParams {
                        marker: None,
                        direction: PaginationOrder::Ascending,
                        limit: NonZeroU32::new(1).expect("1 is not 0"),
                    },
                    SledFilter::TufArtifactReplication,
                )
                .await
                .context("listing sleds")?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("found no sleds with TUF artifacts"))?;
            let repo_depot_addr = SocketAddrV6::new(
                *repo_depot_sled.ip,
                *repo_depot_sled.repo_depot_port,
                0,
                0,
            );
            let mut repo_depot_resolver =
                FixedResolver::new([SocketAddr::from(repo_depot_addr)]);
            let artifact_cache = Arc::new(ArtifactCache::new(
                log.clone(),
                repo_depot_resolver.monitor(),
            ));
            let (requests_tx, requests_rx) =
                watch::channel(PendingMgsUpdates::new());
            let driver = MgsUpdateDriver::new(
                log.clone(),
                artifact_cache,
                requests_rx,
                mgs_rx,
                Duration::from_secs(20),
            );
            let status_rx = driver.status_rx();
            let driver_task = tokio::spawn(async move {
                driver.run().await;
            });
            (requests_tx, Some((driver_task, status_rx, repo_depot_resolver)))
        } else {
            let (mgs_updates, _rx) = watch::channel(PendingMgsUpdates::new());
            (mgs_updates, None)
        };

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
                // The driver shuts down when the tx side of this channel gets
                // closed.  Clone this sender so that it doesn't get shut down
                // right away.
                mgs_updates: mgs_updates.clone(),
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

        if let Some((driver_task, status_rx, mut repo_depot_resolver)) = mgs {
            let nupdates = blueprint.pending_mgs_updates.len();
            info!(
                &log,
                "waiting for requested SP updates to complete";
                "nupdates" => nupdates
            );

            loop {
                let status = status_rx.borrow();
                info!(&log, "MGS update status"; "status" => ?status);
                if !status.recent.is_empty() && status.in_progress.is_empty() {
                    break;
                }
                info!(
                    &log,
                    "waiting for more updates";
                    "nwaiting" => nupdates,
                    "nrecent" => status.recent.len(),
                    "nin_progress" => status.in_progress.len(),
                );
                drop(status);
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }

            info!(&log, "waiting for repo depot resolver to stop");
            repo_depot_resolver.terminate().await;
            info!(&log, "waiting for driver to stop");
            // We're ready to drop this sender so that the driver shuts down.
            drop(mgs_updates);
            driver_task.await.context("waiting for driver to stop")?;
        }

        rv.map(|_| ())
    }
}
