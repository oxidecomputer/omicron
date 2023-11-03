// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Command-line driven rack update.
//!
//! This is an alternative to using the Wicket UI to perform a rack update.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddrV6,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand};
use slog::Logger;
use tokio::{sync::watch, task::JoinHandle};
use update_engine::display::{GroupDisplay, LineDisplayStyles};
use wicket_common::update_events::EventReport;
use wicketd_client::types::StartUpdateParams;

use crate::{
    cli::GlobalOpts,
    state::{parse_event_report_map, ComponentId, CreateStartUpdateOptions},
    wicketd::{create_wicketd_client, WICKETD_TIMEOUT},
};

#[derive(Debug, Subcommand)]
pub(crate) enum RackUpdateArgs {
    /// Start one or more updates.
    Start(StartRackUpdateArgs),
    /// Attach to one or more running updates.
    Attach(AttachArgs),
}

impl RackUpdateArgs {
    pub(crate) fn exec(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
        global_opts: GlobalOpts,
    ) -> Result<()> {
        let runtime =
            tokio::runtime::Runtime::new().context("creating tokio runtime")?;
        runtime.block_on(self.exec_impl(log, wicketd_addr, global_opts))
    }

    async fn exec_impl(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
        global_opts: GlobalOpts,
    ) -> Result<()> {
        match self {
            RackUpdateArgs::Start(args) => {
                args.exec(log, wicketd_addr, global_opts).await
            }
            RackUpdateArgs::Attach(args) => {
                args.exec(log, wicketd_addr, global_opts).await
            }
        }
    }
}

#[derive(Debug, Args)]
pub(crate) struct StartRackUpdateArgs {
    #[clap(flatten)]
    component_ids: ComponentIdSelector,

    /// Force update the RoT even if the version is the same.
    #[clap(long, help_heading = "Update options")]
    force_update_rot: bool,

    /// Force update the SP even if the version is the same.
    #[clap(long, help_heading = "Update options")]
    force_update_sp: bool,

    /// Detach after starting the update.
    ///
    /// The `attach` command can be used to reattach to the running update.
    #[clap(short, long, help_heading = "Update options")]
    detach: bool,
}

impl StartRackUpdateArgs {
    async fn exec(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
        global_opts: GlobalOpts,
    ) -> Result<()> {
        // NOTE: This process is not idempotent. Doing so would require using a
        // UUID, and storing that UUID in wicket.
        let client = create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);

        let update_ids = self.component_ids.to_component_ids()?;
        let options = CreateStartUpdateOptions {
            force_update_rot: self.force_update_rot,
            force_update_sp: self.force_update_sp,
        }
        .to_start_update_options()?;

        let num_update_ids = update_ids.len();

        let params = StartUpdateParams {
            targets: update_ids.iter().copied().map(Into::into).collect(),
            options,
        };

        slog::debug!(log, "Sending post_start_update"; "num_update_ids" => num_update_ids);
        match client.post_start_update(&params).await {
            Ok(_) => {
                slog::info!(log, "Update started for {num_update_ids} targets");
            }
            Err(error) => {
                // Error responses can be printed out more clearly.
                if let wicketd_client::Error::ErrorResponse(rv) = &error {
                    slog::error!(
                        log,
                        "Error response from wicketd: {}",
                        rv.message
                    );
                    bail!("Received error from wicketd while starting update");
                } else {
                    bail!(error);
                }
            }
        }

        if self.detach {
            return Ok(());
        }

        // Now, attach to the update by printing out update logs.
        do_attach_to_updates(log, client, update_ids, global_opts).await?;

        Ok(())
    }
}

#[derive(Debug, Args)]
pub(crate) struct AttachArgs {
    #[clap(flatten)]
    component_ids: ComponentIdSelector,
}

impl AttachArgs {
    async fn exec(
        self,
        log: Logger,
        wicketd_addr: SocketAddrV6,
        global_opts: GlobalOpts,
    ) -> Result<()> {
        let client = create_wicketd_client(&log, wicketd_addr, WICKETD_TIMEOUT);

        let update_ids = self.component_ids.to_component_ids()?;
        do_attach_to_updates(log, client, update_ids, global_opts).await
    }
}

async fn do_attach_to_updates(
    log: Logger,
    client: wicketd_client::Client,
    update_ids: BTreeSet<ComponentId>,
    global_opts: GlobalOpts,
) -> Result<()> {
    let mut display = GroupDisplay::new_with_display(
        update_ids.iter().copied(),
        std::io::stderr(),
    );
    if global_opts.use_color() {
        display.set_styles(LineDisplayStyles::colorized());
    }

    let (mut rx, handle) = start_fetch_reports_task(&log, client.clone()).await;
    let mut status_timer = tokio::time::interval(Duration::from_secs(5));
    status_timer.tick().await;

    while !display.stats().is_terminal() {
        tokio::select! {
            res = rx.changed() => {
                if res.is_err() {
                    // The sending end is closed, which means that the task
                    // created by start_fetch_reports_task died... this can
                    // happen either due to a panic or due to an error.
                    match handle.await {
                        Ok(Ok(())) => {
                            // The task exited normally, which means that the
                            // sending end was closed normally. This cannot
                            // happen.
                            bail!("fetch_reports task exited with Ok(()) \
                                   -- this should never happen here");
                        }
                        Ok(Err(error)) => {
                            // The task exited with an error.
                            return Err(error).context("fetch_reports task errored out");
                        }
                        Err(error) => {
                            // The task panicked.
                            return Err(anyhow!(error)).context("fetch_reports task panicked");
                        }
                    }
                }

                let event_reports = rx.borrow_and_update();
                // TODO: parallelize this computation?
                for (id, event_report) in &*event_reports {
                    // If display.add_event_report errors out, it's for a report for a
                    // component we weren't interested in. Ignore it.
                    _ = display.add_event_report(&id, event_report.clone());
                }

                // Print out status for each component ID at the end -- do it here so
                // that we also consider components for which we haven't seen status
                // yet.
                display.write_events()?;
            }
            _ = status_timer.tick() => {
                display.write_stats("Status")?;
            }
        }
    }

    // Show any remaining events.
    display.write_events()?;
    // And also show a summary.
    display.write_stats("Summary")?;

    std::mem::drop(rx);
    handle
        .await
        .context("fetch_reports task panicked after rx dropped")?
        .context("fetch_reports task errored out after rx dropped")?;

    if display.stats().has_failures() {
        bail!("one or more failures occurred");
    }

    Ok(())
}

async fn start_fetch_reports_task(
    log: &Logger,
    client: wicketd_client::Client,
) -> (watch::Receiver<BTreeMap<ComponentId, EventReport>>, JoinHandle<Result<()>>)
{
    // Since reports are always cumulative, we can use a watch receiver here
    // rather than an mpsc receiver. If we start using incremental reports at
    // some point this would need to be changed to be an mpsc receiver.
    let (tx, rx) = watch::channel(BTreeMap::new());
    let log = log.new(slog::o!("task" => "fetch_reports"));

    let handle = tokio::spawn(async move {
        loop {
            let response = client.get_artifacts_and_event_reports().await?;
            let reports = response.into_inner().event_reports;
            let reports = parse_event_report_map(&log, reports);
            if tx.send(reports).is_err() {
                // The receiving end is closed, exit.
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                _ = tx.closed() => {
                    // The receiving end is closed, exit.
                    break;
                }
            }
        }

        Ok(())
    });
    (rx, handle)
}

/// Command-line arguments for selecting component IDs.
#[derive(Debug, Args)]
#[clap(next_help_heading = "Component selectors")]
struct ComponentIdSelector {
    /// The sleds to operate on.
    #[clap(long, value_delimiter = ',')]
    sled: Vec<u8>,

    /// The switches to operate on.
    #[clap(long, value_delimiter = ',')]
    switch: Vec<u8>,

    /// The PSCs to operate on.
    #[clap(long, value_delimiter = ',')]
    psc: Vec<u8>,
}

impl ComponentIdSelector {
    /// Validates that all the sleds, switches, and PSCs are reasonable (though
    /// they might not exist on the actual hardware), then return the set of
    /// selected component IDs.
    fn to_component_ids(&self) -> Result<BTreeSet<ComponentId>> {
        let mut component_ids = BTreeSet::new();
        for sled in &self.sled {
            component_ids.insert(ComponentId::new_sled(*sled)?);
        }
        for switch in &self.switch {
            component_ids.insert(ComponentId::new_switch(*switch)?);
        }
        for psc in &self.psc {
            component_ids.insert(ComponentId::new_psc(*psc)?);
        }
        if component_ids.is_empty() {
            bail!("at least one component ID must be selected via --sled, --switch or --psc");
        }

        Ok(component_ids)
    }
}
