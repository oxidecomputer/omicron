// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb support-bundle collect` — collect a support bundle locally,
//! without going through Nexus.
//!
//! Unlike the Nexus background task, this path:
//!
//! - Does not register a row in the `support_bundle` table.
//! - Does not transfer the resulting bundle to a sled-agent for durable
//!   storage. The zip is written to a local file path.
//! - Does not require Nexus to be up. It only needs CRDB, internal
//!   DNS, MGS, and the rack's sled-agents reachable on the underlay.
//!
//! This is intended for incident response, where the operator may need
//! to collect a bundle precisely because Nexus is unhealthy.

use crate::Omdb;
use crate::db::DbUrlOptions;
use anyhow::Context;
use camino::Utf8PathBuf;
use camino_tempfile::tempdir_in;
use clap::Args;
use clap::Subcommand;
use clap::ValueEnum;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::fm::ereport::EreportFilters;
use nexus_types::support_bundle::BundleDataCategory;
use nexus_types::support_bundle::BundleDataSelection;
use omicron_uuid_kinds::SupportBundleUuid;
use std::io::Write;
use std::sync::Arc;
use support_bundle_collection::BundleCollection;
use support_bundle_collection::BundleInfo;
use support_bundle_collection::zip::bundle_to_stream;
use support_bundle_collection::zip::bundle_to_writer;

/// Arguments to the "omdb support-bundle" subcommand
#[derive(Debug, Args)]
pub struct SupportBundleArgs {
    #[command(subcommand)]
    command: SupportBundleCommands,
}

#[derive(Debug, Subcommand)]
enum SupportBundleCommands {
    /// Collect a support bundle without involving Nexus.
    ///
    /// Connects directly to CockroachDB, internal DNS, MGS, and the
    /// rack's sled-agents — none of which depend on Nexus being up.
    /// The bundle is written to a local zip file. No row is created
    /// in the `support_bundle` table.
    Collect(CollectArgs),
}

#[derive(Debug, Args)]
struct CollectArgs {
    #[command(flatten)]
    db_url_opts: DbUrlOptions,

    /// Optional path where the bundle zip will be written. If omitted,
    /// the zip is streamed to stdout (suitable for piping over ssh).
    #[clap(long, short = 'o')]
    output: Option<Utf8PathBuf>,

    /// Reason recorded inside the bundle's metadata.
    #[clap(long, default_value = "collected via omdb")]
    reason: String,

    /// Directory to use for staging the bundle contents before zipping.
    #[clap(long, default_value = "/var/tmp")]
    tempdir: Utf8PathBuf,

    /// Categories of data to collect. May be supplied multiple times.
    /// Defaults to all categories.
    #[clap(long, value_enum)]
    include: Vec<BundleDataCategory>,
}

impl CollectArgs {
    fn data_selection(&self) -> BundleDataSelection {
        let categories: &[BundleDataCategory] = if self.include.is_empty() {
            BundleDataCategory::value_variants()
        } else {
            self.include.as_slice()
        };

        let mut sel = BundleDataSelection::new();
        for category in categories {
            sel = match category {
                BundleDataCategory::Reconfigurator => sel.with_reconfigurator(),
                BundleDataCategory::HostInfo => sel.with_all_sleds(),
                BundleDataCategory::SledCubbyInfo => sel.with_sled_cubby_info(),
                BundleDataCategory::SpDumps => sel.with_sp_dumps(),
                BundleDataCategory::Ereports => sel.with_ereports(
                    EreportFilters::new()
                        .with_start_time(
                            omicron_common::now_db_precision()
                                - chrono::Days::new(7),
                        )
                        .expect("no end time set, cannot fail"),
                ),
            };
        }
        sel
    }
}

impl SupportBundleArgs {
    pub async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &slog::Logger,
    ) -> anyhow::Result<()> {
        match &self.command {
            SupportBundleCommands::Collect(args) => args.run(omdb, log).await,
        }
    }
}

impl CollectArgs {
    async fn run(&self, omdb: &Omdb, log: &slog::Logger) -> anyhow::Result<()> {
        // Collecting a full bundle stages every file in --tempdir before
        // (or while) writing the zip. On the switch zone, where this
        // command typically runs during incident response, disk space is
        // limited and a large bundle can fill it. Gate the command behind
        // -w/--destructive so an operator opts in knowingly.
        let _token = omdb.check_allow_destructive()?;
        self.db_url_opts
            .with_datastore(omdb, log, async |opctx, datastore| {
                self.collect(omdb, log, opctx, datastore).await
            })
            .await
    }

    async fn collect(
        &self,
        omdb: &Omdb,
        log: &slog::Logger,
        opctx: OpContext,
        datastore: Arc<DataStore>,
    ) -> anyhow::Result<()> {
        let resolver = omdb.dns_resolver(log.clone()).await?;

        let bundle = BundleInfo {
            id: SupportBundleUuid::new_v4(),
            reason_for_creation: self.reason.clone(),
        };
        let bundle_log = log.new(slog::o!("bundle" => bundle.id.to_string()));
        eprintln!("Collecting support bundle {}", bundle.id);

        let collection = Arc::new(BundleCollection::new(
            datastore,
            resolver,
            bundle_log,
            opctx,
            self.data_selection(),
            bundle,
        ));

        // Wire Ctrl-C to cancel the in-flight collection.
        let cancel_handle = tokio::spawn({
            let token = collection.cancellation_token().clone();
            async move {
                let _ = tokio::signal::ctrl_c().await;
                eprintln!("\nCtrl-C received — cancelling bundle collection.");
                token.cancel();
            }
        });

        let dir = tempdir_in(&self.tempdir).with_context(|| {
            format!("creating temp dir under {}", self.tempdir)
        })?;
        let collect_result = collection.collect_bundle_locally(&dir).await;
        cancel_handle.abort();
        let _ = cancel_handle.await;
        let report = collect_result?;

        let output = self.output.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            match output {
                Some(path) => {
                    let file = std::fs::File::create(&path)
                        .with_context(|| format!("creating {path}"))?;
                    bundle_to_writer(&dir, &file)?;
                }
                None => {
                    let mut stdout = std::io::stdout().lock();
                    bundle_to_stream(&dir, &mut stdout)?;
                    stdout.flush()?;
                }
            }
            Ok(())
        })
        .await
        .context("zip task panicked")??;

        if let Some(path) = &self.output {
            eprintln!("Wrote bundle to {path}");
        } else {
            eprintln!("Bundle streamed to stdout");
        }
        eprintln!("{} steps executed:", report.steps.len());
        for step in &report.steps {
            let dur = step.end - step.start;
            eprintln!(
                "  {:>9}ms  {:?}  {}",
                dur.num_milliseconds(),
                step.status,
                step.name,
            );
            if let Some(details) = &step.details {
                let pretty = serde_json::to_string_pretty(details)
                    .unwrap_or_else(|_| details.to_string());
                eprintln!("{}", textwrap::indent(&pretty, "      "));
            }
        }
        Ok(())
    }
}
