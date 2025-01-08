// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query or update specific Nexus instances

use crate::check_allow_destructive::DestructiveOperationToken;
use crate::db::DbUrlOptions;
use crate::helpers::const_max_len;
use crate::helpers::should_colorize;
use crate::helpers::CONNECTION_OPTIONS_HEADING;
use crate::Omdb;
use anyhow::bail;
use anyhow::Context;
use camino::Utf8PathBuf;
use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use clap::Args;
use clap::ColorChoice;
use clap::Subcommand;
use clap::ValueEnum;
use futures::future::try_join;
use futures::TryStreamExt;
use http::StatusCode;
use internal_dns_types::names::ServiceName;
use itertools::Itertools;
use nexus_client::types::ActivationReason;
use nexus_client::types::BackgroundTask;
use nexus_client::types::BackgroundTasksActivateRequest;
use nexus_client::types::CurrentStatus;
use nexus_client::types::LastResult;
use nexus_client::types::PhysicalDiskPath;
use nexus_client::types::SagaState;
use nexus_client::types::SledSelector;
use nexus_client::types::UninitializedSledId;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::DataStore;
use nexus_inventory::now_db_precision;
use nexus_saga_recovery::LastPass;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::ClickhouseMode;
use nexus_types::deployment::ClickhousePolicy;
use nexus_types::internal_api::background::AbandonedVmmReaperStatus;
use nexus_types::internal_api::background::InstanceReincarnationStatus;
use nexus_types::internal_api::background::InstanceUpdaterStatus;
use nexus_types::internal_api::background::LookupRegionPortStatus;
use nexus_types::internal_api::background::RegionReplacementDriverStatus;
use nexus_types::internal_api::background::RegionReplacementStatus;
use nexus_types::internal_api::background::RegionSnapshotReplacementFinishStatus;
use nexus_types::internal_api::background::RegionSnapshotReplacementGarbageCollectStatus;
use nexus_types::internal_api::background::RegionSnapshotReplacementStartStatus;
use nexus_types::internal_api::background::RegionSnapshotReplacementStepStatus;
use nexus_types::inventory::BaseboardId;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::DemoSagaUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use reedline::DefaultPrompt;
use reedline::DefaultPromptSegment;
use reedline::Reedline;
use serde::Deserialize;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;
use tabled::settings::object::Columns;
use tabled::settings::Padding;
use tabled::Tabled;
use tokio::sync::OnceCell;
use update_engine::display::LineDisplay;
use update_engine::display::LineDisplayStyles;
use update_engine::display::ProgressRatioDisplay;
use update_engine::events::EventReport;
use update_engine::events::StepOutcome;
use update_engine::EventBuffer;
use update_engine::ExecutionStatus;
use update_engine::ExecutionTerminalInfo;
use update_engine::NestedError;
use update_engine::NestedSpec;
use update_engine::TerminalKind;
use uuid::Uuid;

/// Arguments to the "omdb nexus" subcommand
#[derive(Debug, Args)]
pub struct NexusArgs {
    /// URL of the Nexus internal API
    #[clap(
        long,
        env = "OMDB_NEXUS_URL",
        global = true,
        help_heading = CONNECTION_OPTIONS_HEADING,
    )]
    nexus_internal_url: Option<String>,

    #[command(subcommand)]
    command: NexusCommands,
}

/// Subcommands for the "omdb nexus" subcommand
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
enum NexusCommands {
    /// print information about background tasks
    BackgroundTasks(BackgroundTasksArgs),
    /// interact with blueprints
    Blueprints(BlueprintsArgs),
    /// Interact with clickhouse policy
    ClickhousePolicy(ClickhousePolicyArgs),
    /// view sagas, create and complete demo sagas
    Sagas(SagasArgs),
    /// interact with sleds
    Sleds(SledsArgs),
}

#[derive(Debug, Args)]
struct BackgroundTasksArgs {
    #[command(subcommand)]
    command: BackgroundTasksCommands,
}

#[derive(Debug, Subcommand)]
enum BackgroundTasksCommands {
    /// Show documentation about background tasks
    Doc,
    /// Print a summary of the status of all background tasks
    List,
    /// Print human-readable summary of the status of each background task
    Show(BackgroundTasksShowArgs),
    /// Print an event report for a background task if available.
    PrintReport(BackgroundTasksPrintReportArgs),
    /// Activate one or more background tasks
    Activate(BackgroundTasksActivateArgs),
}

#[derive(Debug, Args)]
struct BackgroundTasksShowArgs {
    /// Names of background tasks to show (default: all)
    ///
    /// You can use any background task name here or one of the special strings
    /// "all", "dns_external", or "dns_internal".
    #[clap(value_name = "TASK_NAME")]
    tasks: Vec<String>,
}

#[derive(Debug, Args)]
struct BackgroundTasksPrintReportArgs {
    /// The name of the background task to print a report for.
    #[clap(value_name = "TASK_NAME")]
    task: String,
}

#[derive(Debug, Args)]
struct BackgroundTasksActivateArgs {
    /// Name of the background tasks to activate
    #[clap(value_name = "TASK_NAME", required = true)]
    tasks: Vec<String>,
}

#[derive(Debug, Args)]
struct BlueprintsArgs {
    #[command(subcommand)]
    command: BlueprintsCommands,
}

#[derive(Debug, Subcommand)]
enum BlueprintsCommands {
    /// List all blueprints
    List,
    /// Show a blueprint
    Show(BlueprintIdArgs),
    /// Diff two blueprints
    Diff(BlueprintIdsArgs),
    /// Delete a blueprint
    Delete(BlueprintIdArgs),
    /// Interact with the current target blueprint
    Target(BlueprintsTargetArgs),
    /// Generate a new blueprint
    Regenerate,
    /// Import a blueprint
    Import(BlueprintImportArgs),
}

#[derive(Debug, Clone, Copy)]
enum BlueprintIdOrCurrentTarget {
    CurrentTarget,
    BlueprintId(Uuid),
}

impl FromStr for BlueprintIdOrCurrentTarget {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if matches!(s, "current-target" | "current" | "target") {
            Ok(Self::CurrentTarget)
        } else {
            let id = s.parse()?;
            Ok(Self::BlueprintId(id))
        }
    }
}

impl BlueprintIdOrCurrentTarget {
    async fn resolve_to_id(
        &self,
        client: &nexus_client::Client,
    ) -> anyhow::Result<Uuid> {
        match self {
            Self::CurrentTarget => {
                let target = client
                    .blueprint_target_view()
                    .await
                    .context("getting current blueprint target")?;
                Ok(target.target_id)
            }
            Self::BlueprintId(id) => Ok(*id),
        }
    }

    async fn resolve_to_blueprint(
        &self,
        client: &nexus_client::Client,
    ) -> anyhow::Result<Blueprint> {
        let id = self.resolve_to_id(client).await?;
        let response = client.blueprint_view(&id).await.with_context(|| {
            let suffix = match self {
                BlueprintIdOrCurrentTarget::CurrentTarget => {
                    " (current target)"
                }
                BlueprintIdOrCurrentTarget::BlueprintId(_) => "",
            };
            format!("fetching blueprint {id}{suffix}")
        })?;
        Ok(response.into_inner())
    }
}

#[derive(Debug, Clone, Copy, Args)]
struct BlueprintIdArgs {
    /// id of blueprint (or `target` for the current target)
    blueprint_id: BlueprintIdOrCurrentTarget,
}

#[derive(Debug, Args)]
struct BlueprintIdsArgs {
    /// id of first blueprint (or `target` for the current target)
    blueprint1_id: BlueprintIdOrCurrentTarget,
    /// id of second blueprint (or `target` for the current target)
    blueprint2_id: BlueprintIdOrCurrentTarget,
}

#[derive(Debug, Args)]
struct CollectionIdArgs {
    /// id of an inventory collection
    collection_id: CollectionUuid,
}

#[derive(Debug, Args)]
struct BlueprintsTargetArgs {
    #[command(subcommand)]
    command: BlueprintTargetCommands,
}

#[derive(Debug, Subcommand)]
enum BlueprintTargetCommands {
    /// Show the current target blueprint
    Show,
    /// Change the current target blueprint
    Set(BlueprintTargetSetArgs),
    /// Enable the current target blueprint
    ///
    /// Fails if the specified blueprint id is not the current target
    Enable(BlueprintIdArgs),
    /// Disable the current target blueprint
    ///
    /// Fails if the specified blueprint id is not the current target
    Disable(BlueprintIdArgs),
}

#[derive(Debug, Args)]
struct BlueprintTargetSetArgs {
    /// id of blueprint to make target
    blueprint_id: Uuid,
    /// whether this blueprint should be enabled
    enabled: BlueprintTargetSetEnabled,
    /// if specified, diff against the current target and wait for confirmation
    /// before proceeding
    #[clap(long)]
    diff: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum BlueprintTargetSetEnabled {
    /// set the new current target as enabled
    Enabled,
    /// set the new current target as disabled
    Disabled,
    /// use the enabled setting from the parent blueprint
    Inherit,
}

#[derive(Debug, Args)]
struct BlueprintImportArgs {
    /// path to a file containing a JSON-serialized blueprint
    input: Utf8PathBuf,
}

#[derive(Debug, Args)]
struct ClickhousePolicyArgs {
    #[command(subcommand)]
    command: ClickhousePolicyCommands,
}

#[derive(Debug, Subcommand)]
enum ClickhousePolicyCommands {
    /// Get the current policy
    Get,
    /// Set the new policy
    Set(ClickhousePolicySetArgs),
}

#[derive(Debug, Args)]
struct ClickhousePolicySetArgs {
    mode: ClickhousePolicyMode,

    /// The number of servers in a clickhouse cluster
    #[arg(long, default_value_t = 3)]
    target_servers: u8,
    /// The number of keepers in a clickhouse cluster
    #[arg(long, default_value_t = 5)]
    target_keepers: u8,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ClickhousePolicyMode {
    /// Run only a single node clickhouse instance
    SingleNodeOnly,
    // Run only a clickhouse cluster
    ClusterOnly,
    // Run both single-node and clustered clickhouse deployments
    Both,
}

#[derive(Debug, Args)]
struct SagasArgs {
    #[command(subcommand)]
    command: SagasCommands,
}

#[derive(Debug, Subcommand)]
enum SagasCommands {
    /// List sagas run by this Nexus
    ///
    /// Note that this is reporting in-memory state about sagas run by *this*
    /// Nexus instance.  You'll get different answers if you ask different Nexus
    /// instances.
    List,

    /// Create a "demo" saga
    ///
    /// This saga will wait until it's explicitly completed using the
    /// "demo-complete" subcommand.
    DemoCreate,

    /// Complete a demo saga started with "demo-create".
    DemoComplete(DemoSagaIdArgs),
}

#[derive(Debug, Args)]
struct DemoSagaIdArgs {
    demo_saga_id: DemoSagaUuid,
}

#[derive(Debug, Args)]
struct SledsArgs {
    #[command(subcommand)]
    command: SledsCommands,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
enum SledsCommands {
    /// List all uninitialized sleds
    ListUninitialized,
    /// Add an uninitialized sled
    Add(SledAddArgs),
    /// Expunge a sled (DANGEROUS)
    Expunge(SledExpungeArgs),
    /// Expunge a disk (DANGEROUS)
    ExpungeDisk(DiskExpungeArgs),
}

#[derive(Debug, Args)]
struct SledAddArgs {
    /// sled's serial number
    serial: String,
    /// sled's part number
    part: String,
}

#[derive(Debug, Args)]
struct SledExpungeArgs {
    // expunge is _extremely_ dangerous, so we also require a database
    // connection to perform some safety checks
    #[clap(flatten)]
    db_url_opts: DbUrlOptions,

    /// sled ID
    sled_id: SledUuid,
}

#[derive(Debug, Args)]
struct DiskExpungeArgs {
    // expunge is _extremely_ dangerous, so we also require a database
    // connection to perform some safety checks
    #[clap(flatten)]
    db_url_opts: DbUrlOptions,

    /// Physical disk ID
    physical_disk_id: PhysicalDiskUuid,
}

impl NexusArgs {
    /// Run a `omdb nexus` subcommand.
    pub(crate) async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &slog::Logger,
    ) -> Result<(), anyhow::Error> {
        let nexus_url = match &self.nexus_internal_url {
            Some(cli_or_env_url) => cli_or_env_url.clone(),
            None => {
                eprintln!(
                    "note: Nexus URL not specified.  Will pick one from DNS."
                );
                let addr = omdb
                    .dns_lookup_one(log.clone(), ServiceName::Nexus)
                    .await?;
                format!("http://{}", addr)
            }
        };
        eprintln!("note: using Nexus URL {}", &nexus_url);
        let client = nexus_client::Client::new(&nexus_url, log.clone());

        match &self.command {
            NexusCommands::BackgroundTasks(BackgroundTasksArgs {
                command: BackgroundTasksCommands::Doc,
            }) => cmd_nexus_background_tasks_doc(&client).await,
            NexusCommands::BackgroundTasks(BackgroundTasksArgs {
                command: BackgroundTasksCommands::List,
            }) => cmd_nexus_background_tasks_list(&client).await,
            NexusCommands::BackgroundTasks(BackgroundTasksArgs {
                command: BackgroundTasksCommands::Show(args),
            }) => cmd_nexus_background_tasks_show(&client, args).await,
            NexusCommands::BackgroundTasks(BackgroundTasksArgs {
                command: BackgroundTasksCommands::PrintReport(args),
            }) => {
                cmd_nexus_background_tasks_print_report(
                    &client,
                    args,
                    omdb.output.color,
                )
                .await
            }
            NexusCommands::BackgroundTasks(BackgroundTasksArgs {
                command: BackgroundTasksCommands::Activate(args),
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_nexus_background_tasks_activate(&client, args, token).await
            }

            NexusCommands::Blueprints(BlueprintsArgs {
                command: BlueprintsCommands::List,
            }) => cmd_nexus_blueprints_list(&client).await,
            NexusCommands::Blueprints(BlueprintsArgs {
                command: BlueprintsCommands::Show(args),
            }) => cmd_nexus_blueprints_show(&client, args).await,
            NexusCommands::Blueprints(BlueprintsArgs {
                command: BlueprintsCommands::Diff(args),
            }) => cmd_nexus_blueprints_diff(&client, args).await,
            NexusCommands::Blueprints(BlueprintsArgs {
                command: BlueprintsCommands::Delete(args),
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_nexus_blueprints_delete(&client, args, token).await
            }
            NexusCommands::Blueprints(BlueprintsArgs {
                command:
                    BlueprintsCommands::Target(BlueprintsTargetArgs {
                        command: BlueprintTargetCommands::Show,
                    }),
            }) => cmd_nexus_blueprints_target_show(&client).await,
            NexusCommands::Blueprints(BlueprintsArgs {
                command:
                    BlueprintsCommands::Target(BlueprintsTargetArgs {
                        command: BlueprintTargetCommands::Set(args),
                    }),
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_nexus_blueprints_target_set(&client, args, token).await
            }
            NexusCommands::Blueprints(BlueprintsArgs {
                command:
                    BlueprintsCommands::Target(BlueprintsTargetArgs {
                        command: BlueprintTargetCommands::Enable(args),
                    }),
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_nexus_blueprints_target_set_enabled(
                    &client, args, true, token,
                )
                .await
            }
            NexusCommands::Blueprints(BlueprintsArgs {
                command:
                    BlueprintsCommands::Target(BlueprintsTargetArgs {
                        command: BlueprintTargetCommands::Disable(args),
                    }),
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_nexus_blueprints_target_set_enabled(
                    &client, args, false, token,
                )
                .await
            }
            NexusCommands::Blueprints(BlueprintsArgs {
                command: BlueprintsCommands::Regenerate,
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_nexus_blueprints_regenerate(&client, token).await
            }
            NexusCommands::Blueprints(BlueprintsArgs {
                command: BlueprintsCommands::Import(args),
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_nexus_blueprints_import(&client, token, args).await
            }

            NexusCommands::ClickhousePolicy(ClickhousePolicyArgs {
                command,
            }) => match command {
                ClickhousePolicyCommands::Get => {
                    cmd_nexus_clickhouse_policy_get(&client).await
                }
                ClickhousePolicyCommands::Set(args) => {
                    let token = omdb.check_allow_destructive()?;
                    cmd_nexus_clickhouse_policy_set(&client, args, token).await
                }
            },

            NexusCommands::Sagas(SagasArgs { command }) => {
                if self.nexus_internal_url.is_none() {
                    eprintln!(
                        "{}",
                        textwrap::wrap(
                            "WARNING: A Nexus instance was selected from DNS \
                            because a specific one was not specified.  But \
                            the `omdb nexus sagas` commands usually only make \
                            sense when targeting a specific Nexus instance.",
                            80
                        )
                        .join("\n")
                    );
                }
                match command {
                    SagasCommands::List => cmd_nexus_sagas_list(&client).await,
                    SagasCommands::DemoCreate => {
                        let token = omdb.check_allow_destructive()?;
                        cmd_nexus_sagas_demo_create(&client, token).await
                    }
                    SagasCommands::DemoComplete(args) => {
                        let token = omdb.check_allow_destructive()?;
                        cmd_nexus_sagas_demo_complete(&client, args, token)
                            .await
                    }
                }
            }

            NexusCommands::Sleds(SledsArgs {
                command: SledsCommands::ListUninitialized,
            }) => cmd_nexus_sleds_list_uninitialized(&client).await,
            NexusCommands::Sleds(SledsArgs {
                command: SledsCommands::Add(args),
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_nexus_sled_add(&client, args, token).await
            }
            NexusCommands::Sleds(SledsArgs {
                command: SledsCommands::Expunge(args),
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_nexus_sled_expunge(&client, args, omdb, log, token).await
            }
            NexusCommands::Sleds(SledsArgs {
                command: SledsCommands::ExpungeDisk(args),
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_nexus_sled_expunge_disk(&client, args, omdb, log, token)
                    .await
            }
        }
    }
}

/// Runs `omdb nexus background-tasks doc`
async fn cmd_nexus_background_tasks_doc(
    client: &nexus_client::Client,
) -> Result<(), anyhow::Error> {
    let response =
        client.bgtask_list().await.context("listing background tasks")?;
    let tasks = response.into_inner();
    let tasks: BTreeMap<_, _> = tasks.into_iter().collect();
    for (_, bgtask) in &tasks {
        println!("task: {:?}", bgtask.name);
        println!(
            "{}",
            textwrap::fill(
                &bgtask.description,
                &textwrap::Options::new(80)
                    .initial_indent("    ")
                    .subsequent_indent("    ")
            )
        );

        println!("\n");
    }

    Ok(())
}

/// Runs `omdb nexus background-tasks list`
async fn cmd_nexus_background_tasks_list(
    client: &nexus_client::Client,
) -> Result<(), anyhow::Error> {
    let response =
        client.bgtask_list().await.context("listing background tasks")?;
    // Convert the HashMap to a BTreeMap because we want the keys in sorted
    // order.
    let tasks = response.into_inner().into_iter().collect::<BTreeMap<_, _>>();
    let table_rows = tasks.values().map(BackgroundTaskStatusRow::from);
    let table = tabled::Table::new(table_rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{}", table);
    Ok(())
}

/// Runs `omdb nexus background-tasks show`
async fn cmd_nexus_background_tasks_show(
    client: &nexus_client::Client,
    args: &BackgroundTasksShowArgs,
) -> Result<(), anyhow::Error> {
    let response =
        client.bgtask_list().await.context("listing background tasks")?;
    // Convert the HashMap to a BTreeMap because we want the keys in sorted
    // order.
    let mut tasks =
        response.into_inner().into_iter().collect::<BTreeMap<_, _>>();

    // Now, pick out the tasks that the user selected.
    //
    // The set of user tasks may include:
    //
    // - nothing at all, in which case we include all tasks
    // - individual task names
    // - certain groups that we recognize, like "dns_external" for all the tasks
    //   related to external DNS propagation.  "all" means "all tasks".
    let selected_set: BTreeSet<_> =
        args.tasks.iter().map(AsRef::as_ref).collect();
    let selected_all = selected_set.is_empty() || selected_set.contains("all");
    if !selected_all {
        for s in &selected_set {
            if !tasks.contains_key(*s)
                && *s != "all"
                && *s != "dns_external"
                && *s != "dns_internal"
            {
                bail!(
                    "unknown task name: {:?} (known task names: all, \
                    dns_external, dns_internal, {})",
                    s,
                    tasks.keys().join(", ")
                );
            }
        }

        tasks.retain(|k, _| {
            selected_set.contains(k.as_str())
                || selected_set.contains("all")
                || (selected_set.contains("dns_external")
                    && k.starts_with("dns_")
                    && k.ends_with("_external"))
                || (selected_set.contains("dns_internal")
                    && k.starts_with("dns_")
                    && k.ends_with("_internal"))
        });
    }

    // Some tasks should be grouped and printed together in a certain order,
    // even though their names aren't alphabetical.  Notably, the DNS tasks
    // logically go from config -> servers -> propagation, so we want to print
    // them in that order.  So we pick these out first and then print anything
    // else that we find in alphabetical order.
    for name in [
        "dns_config_internal",
        "dns_servers_internal",
        "dns_propagation_internal",
        "dns_config_external",
        "dns_servers_external",
        "dns_propagation_external",
        "nat_v4_garbage_collector",
        "blueprint_loader",
        "blueprint_executor",
    ] {
        if let Some(bgtask) = tasks.remove(name) {
            print_task(&bgtask);
        } else if selected_all {
            eprintln!("warning: expected to find background task {:?}", name);
        }
    }

    for (_, bgtask) in &tasks {
        print_task(bgtask);
    }

    Ok(())
}

/// Runs `omdb nexus background-tasks print-report`
async fn cmd_nexus_background_tasks_print_report(
    client: &nexus_client::Client,
    args: &BackgroundTasksPrintReportArgs,
    color: ColorChoice,
) -> Result<(), anyhow::Error> {
    let response = client
        .bgtask_view(&args.task)
        .await
        .context("fetching background task")?;
    let task = response.into_inner();
    match task.last {
        LastResult::NeverCompleted => {
            bail!("task {:?} has never completed", args.task);
        }
        LastResult::Completed(last) => {
            // Clone the details so the original details are preserved for the
            // error below.
            let event_buffer = extract_event_buffer(&mut last.details.clone())
                .with_context(|| {
                    format!(
                        "error extracting `event_report` from task details \
                         -- found {:?}",
                        last.details
                    )
                })?;
            let Some(event_buffer) = event_buffer else {
                bail!("task {:?} has no event report", args.task);
            };

            let mut line_display = LineDisplay::new(std::io::stdout());
            line_display.set_start_time(last.start_time);
            if should_colorize(color, supports_color::Stream::Stdout) {
                line_display.set_styles(LineDisplayStyles::colorized());
            }

            line_display.write_event_buffer(&event_buffer)?;
        }
    }

    Ok(())
}

/// Runs `omdb nexus background-tasks activate`
async fn cmd_nexus_background_tasks_activate(
    client: &nexus_client::Client,
    args: &BackgroundTasksActivateArgs,
    // This isn't quite "destructive" in the sense that of it being potentially
    // dangerous, but it does modify the system rather than being a read-only
    // view on it.
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let body =
        BackgroundTasksActivateRequest { bgtask_names: args.tasks.clone() };
    client
        .bgtask_activate(&body)
        .await
        .context("error activating background tasks")?;

    eprintln!("activated background tasks: {}", args.tasks.join(", "));
    Ok(())
}

fn print_task(bgtask: &BackgroundTask) {
    println!("task: {:?}", bgtask.name);
    println!(
        "  configured period: every {}",
        humantime::format_duration(bgtask.period.clone().into())
    );
    print!("  currently executing: ");
    match &bgtask.current {
        CurrentStatus::Idle => println!("no"),
        CurrentStatus::Running(current) => {
            let elapsed = std::time::SystemTime::from(current.start_time)
                .elapsed()
                .map(|s| format!("{:.3}ms", s.as_millis()))
                .unwrap_or_else(|error| format!("(unknown: {:#})", error));
            print!(
                "iter {}, triggered by {}\n",
                current.iteration,
                reason_str(&current.reason)
            );
            print!(
                "    started at {}, running for {}\n",
                humantime::format_rfc3339_millis(current.start_time.into()),
                elapsed,
            );
        }
    };

    print!("  last completed activation: ");
    match &bgtask.last {
        LastResult::NeverCompleted => print!("never\n"),
        LastResult::Completed(last) => {
            print!(
                "iter {}, triggered by {}\n",
                last.iteration,
                reason_str(&last.reason)
            );
            print!(
                "    started at {} ({}s ago) and ran for {:.3}ms\n",
                humantime::format_rfc3339_millis(last.start_time.into()),
                (Utc::now() - last.start_time).num_seconds(),
                std::time::Duration::from(last.elapsed.clone()).as_millis(),
            );
        }
    };

    // Print extra task-specific information.  This data is particularly
    // unstable -- it gets exposed by background tasks as unstructured
    // (schemaless) data.  We make a best effort to interpret it.
    if let LastResult::Completed(completed) = &bgtask.last {
        print_task_details(&bgtask, &completed.details);
    }
}

/// Interprets the unstable, schemaless output from each particular background
/// task and print a human-readable summary
///
/// Each activation of a background task produces an arbitrary JSON value that
/// gets passed to us here.  The task can put whatever status it wants here --
/// it's solely for debugging.  This function decodes these values and prints
/// them out.
///
/// As implied by not having a schema, the specific values are currently
/// undocumented and unstable (subject to change).  That does make this code
/// both ugly and brittle.  It's not a fatal error to fail to parse these, but
/// we do warn the user if that happens.
fn print_task_details(bgtask: &BackgroundTask, details: &serde_json::Value) {
    // All tasks might produce an "error" property.  If we find one, print that
    // out and stop.
    #[derive(Deserialize)]
    struct TaskError {
        error: String,
    }
    if let Ok(found_error) =
        serde_json::from_value::<TaskError>(details.clone())
    {
        println!("    last completion reported error: {}", found_error.error);
        println!("");
        return;
    }

    // The rest is task-specific, keyed by the name.
    let name = &bgtask.name;
    if name == "dns_config_external" || name == "dns_config_internal" {
        // The "dns_config" tasks emit the generation number of the config that
        // they read.
        #[derive(Deserialize)]
        struct DnsConfigSuccess {
            generation: usize,
        }

        match serde_json::from_value::<DnsConfigSuccess>(details.clone()) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),
            Ok(found_dns_config) => println!(
                "    last generation found: {}",
                found_dns_config.generation
            ),
        };
    } else if name == "dns_servers_external" || name == "dns_servers_internal" {
        // The "dns_servers" tasks emit the list of servers that were found.
        #[derive(Deserialize)]
        struct DnsServersSuccess {
            addresses: Vec<String>,
        }

        match serde_json::from_value::<DnsServersSuccess>(details.clone()) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),
            Ok(found_dns_servers) => {
                println!(
                    "    servers found: {}\n",
                    found_dns_servers.addresses.len(),
                );

                if !found_dns_servers.addresses.is_empty() {
                    #[derive(Tabled)]
                    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
                    struct ServerRow<'a> {
                        dns_server_addr: &'a str,
                    }

                    let mut addrs = found_dns_servers.addresses;
                    addrs.sort();
                    let rows = addrs
                        .iter()
                        .map(|dns_server_addr| ServerRow { dns_server_addr });
                    let table = tabled::Table::new(rows)
                        .with(tabled::settings::Style::empty())
                        .with(tabled::settings::Padding::new(0, 1, 0, 0))
                        .to_string();
                    println!(
                        "{}",
                        textwrap::indent(&table.to_string(), "      ")
                    );
                }
            }
        }
    } else if name == "dns_propagation_internal"
        || name == "dns_propagation_external"
    {
        // The "dns_propagation" tasks emit a mapping of (dns server address) to
        // (result of propagation attempt).  There's no data in the success
        // variant.  On error, there's an error message.
        #[derive(Deserialize)]
        struct DnsPropSuccess {
            generation: usize,
            server_results: BTreeMap<String, Result<(), String>>,
        }

        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct DnsPropRow<'a> {
            dns_server_addr: &'a str,
            last_result: &'static str,
        }

        match serde_json::from_value::<DnsPropSuccess>(details.clone()) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),
            Ok(details) => {
                println!(
                    "    attempt to propagate generation: {}\n",
                    details.generation
                );
                let server_results = &details.server_results;

                if !server_results.is_empty() {
                    let rows = server_results.iter().map(|(addr, result)| {
                        DnsPropRow {
                            dns_server_addr: addr,
                            last_result: match result {
                                Ok(_) => "success",
                                Err(_) => "error (see below)",
                            },
                        }
                    });

                    let table = tabled::Table::new(rows)
                        .with(tabled::settings::Style::empty())
                        .with(tabled::settings::Padding::new(0, 1, 0, 0))
                        .to_string();
                    println!(
                        "{}",
                        textwrap::indent(&table.to_string(), "      ")
                    );
                }

                println!("");
                for (addr, error) in
                    server_results.iter().filter_map(|(addr, result)| {
                        match result {
                            Ok(_) => None,
                            Err(error) => Some((addr, error)),
                        }
                    })
                {
                    println!("    error: server {}: {}", addr, error);
                }
            }
        };
    } else if name == "external_endpoints" {
        // The "external_endpoints" task emits somewhat complex data.
        // This corresponds to the `ExternalEndpoints` type in Nexus.
        #[derive(Deserialize)]
        struct EndpointsFound {
            /// mapping of DNS names on which we serve the API to "endpoint"
            by_dns_name: BTreeMap<String, Endpoint>,
            /// an endpoint used when we cannot figure out the DNS name of an
            /// incoming request
            default_endpoint: Option<Endpoint>,
            /// pending problems related to DNS/TLS configuration
            warnings: Vec<String>,
        }

        #[derive(Deserialize)]
        struct Endpoint {
            /// the silo id whose endpoint this is
            silo_id: Uuid,
            /// TLS certificates that could be used for this endpoint
            /// (digests only)
            tls_certs: Vec<String>,
        }

        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct EndpointRow<'a> {
            #[tabled(rename = " ")]
            is_default: char,
            silo_id: Uuid,
            dns_name: &'a str,
        }

        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct TlsCertRow<'a> {
            dns_name: &'a str,
            digest: &'a str,
        }

        match serde_json::from_value::<EndpointsFound>(details.clone()) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),
            Ok(details) => {
                println!(
                    "    external API endpoints: {} \
                    ('*' below marks default)\n",
                    details.by_dns_name.len()
                );
                let endpoint_rows =
                    details.by_dns_name.iter().map(|(dns_name, endpoint)| {
                        let is_default = match &details.default_endpoint {
                            Some(e) if e.silo_id == endpoint.silo_id => '*',
                            _ => ' ',
                        };

                        EndpointRow {
                            silo_id: endpoint.silo_id,
                            is_default,
                            dns_name,
                        }
                    });
                let table = tabled::Table::new(endpoint_rows)
                    .with(tabled::settings::Style::empty())
                    .with(tabled::settings::Padding::new(0, 1, 0, 0))
                    .to_string();
                println!(
                    "{}\n",
                    textwrap::indent(&table.to_string(), "        ")
                );

                let tls_cert_rows: Vec<TlsCertRow> = details
                    .by_dns_name
                    .iter()
                    .flat_map(|(dns_name, endpoint)| {
                        endpoint
                            .tls_certs
                            .iter()
                            .map(|digest| TlsCertRow { dns_name, digest })
                    })
                    .collect();

                println!("    warnings: {}", details.warnings.len());
                for w in &details.warnings {
                    println!("        warning: {}", w);
                }

                println!("");
                println!("    TLS certificates: {}", tls_cert_rows.len());
                if !tls_cert_rows.is_empty() {
                    let table = tabled::Table::new(tls_cert_rows)
                        .with(tabled::settings::Style::empty())
                        .with(tabled::settings::Padding::new(0, 1, 0, 0))
                        .to_string();
                    println!(
                        "{}",
                        textwrap::indent(&table.to_string(), "        ")
                    );
                }
            }
        }
    } else if name == "inventory_collection" {
        #[derive(Deserialize)]
        struct InventorySuccess {
            collection_id: Uuid,
            time_started: DateTime<Utc>,
            time_done: DateTime<Utc>,
        }

        match serde_json::from_value::<InventorySuccess>(details.clone()) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),
            Ok(found_inventory) => {
                println!(
                    "    last collection id:      {}",
                    found_inventory.collection_id
                );
                println!(
                    "    last collection started: {}",
                    found_inventory
                        .time_started
                        .to_rfc3339_opts(SecondsFormat::Secs, true),
                );
                println!(
                    "    last collection done:    {}",
                    found_inventory
                        .time_done
                        .to_rfc3339_opts(SecondsFormat::Secs, true),
                );
            }
        };
    } else if name == "phantom_disks" {
        #[derive(Deserialize)]
        struct TaskSuccess {
            /// how many phantom disks were deleted ok
            phantom_disk_deleted_ok: usize,

            /// how many phantom disks could not be deleted
            phantom_disk_deleted_err: usize,
        }

        match serde_json::from_value::<TaskSuccess>(details.clone()) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),
            Ok(success) => {
                println!(
                    "    number of phantom disks deleted: {}",
                    success.phantom_disk_deleted_ok
                );
                println!(
                    "    number of phantom disk delete errors: {}",
                    success.phantom_disk_deleted_err
                );
            }
        };
    } else if name == "region_replacement" {
        match serde_json::from_value::<RegionReplacementStatus>(details.clone())
        {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),

            Ok(status) => {
                println!(
                    "    region replacement requests created ok: {}",
                    status.requests_created_ok.len()
                );
                for line in &status.requests_created_ok {
                    println!("    > {line}");
                }

                println!(
                    "    region replacement start sagas started ok: {}",
                    status.start_invoked_ok.len()
                );
                for line in &status.start_invoked_ok {
                    println!("    > {line}");
                }

                println!(
                    "    region replacement requests set to completed ok: {}",
                    status.requests_completed_ok.len()
                );
                for line in &status.requests_completed_ok {
                    println!("    > {line}");
                }

                println!("    errors: {}", status.errors.len());
                for line in &status.errors {
                    println!("    > {line}");
                }
            }
        };
    } else if name == "instance_watcher" {
        #[derive(Deserialize)]
        struct TaskSuccess {
            /// total number of instances checked
            total_instances: usize,

            /// number of stale instance metrics that were deleted
            pruned_instances: usize,

            /// update sagas queued due to instance updates.
            update_sagas_queued: usize,

            /// instance states from completed checks.
            ///
            /// this is a mapping of stringified instance states to the number
            /// of instances in that state. these stringified states correspond
            /// to the `state` field recorded by the instance watcher's
            /// `virtual_machine:check` timeseries with the `healthy` field set
            /// to `true`. any changes to the instance state type which cause it
            /// to print differently will be counted as a distinct state.
            instance_states: BTreeMap<String, usize>,

            /// instance check failures.
            ///
            /// this is a mapping of stringified instance check failure reasons
            /// to the number of instances with checks that failed for that
            /// reason. these stringified  failure reasons correspond to the
            /// `state` field recorded by the instance watcher's
            /// `virtual_machine:check` timeseries with the `healthy` field set
            /// to `false`. any changes to the instance state type which cause
            /// it to print differently will be counted as a distinct failure
            /// reason.
            failed_checks: BTreeMap<String, usize>,

            /// instance checks that could not be completed successfully.
            ///
            /// this is a mapping of stringified instance check errors
            /// to the number of instance checks that were not completed due to
            /// that error. these stringified errors correspond to the `reason `
            /// field recorded by the instance watcher's
            /// `virtual_machine:incomplete_check` timeseries. any changes to
            /// the check error type which cause it to print
            /// differently will be counted as a distinct check error.
            incomplete_checks: BTreeMap<String, usize>,
        }

        match serde_json::from_value::<TaskSuccess>(details.clone()) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),
            Ok(TaskSuccess {
                total_instances,
                update_sagas_queued,
                pruned_instances,
                instance_states,
                failed_checks,
                incomplete_checks,
            }) => {
                let total_successes: usize = instance_states.values().sum();
                let total_failures: usize = failed_checks.values().sum();
                let total_incomplete: usize = incomplete_checks.values().sum();
                println!("    total instances checked: {total_instances}",);
                println!(
                    "    checks completed: {}",
                    total_successes + total_failures
                );
                println!("       successful checks: {total_successes}",);
                for (state, count) in &instance_states {
                    println!("       -> {count} instances {state}")
                }
                println!("       update sagas queued: {update_sagas_queued}");
                println!("       failed checks: {total_failures}");
                for (failure, count) in &failed_checks {
                    println!("       -> {count} {failure}")
                }
                println!(
                    "    checks that could not be completed: {total_incomplete}",
                );
                for (error, count) in &incomplete_checks {
                    println!("       -> {count} {error} errors")
                }
                println!(
                    "    stale instance metrics pruned: {pruned_instances}"
                );
            }
        };
    } else if name == "service_firewall_rule_propagation" {
        match serde_json::from_value::<serde_json::Value>(details.clone()) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),
            Ok(serde_json::Value::Object(map)) => {
                if !map.is_empty() {
                    eprintln!(
                        "    unexpected return value from task: {:?}",
                        map
                    )
                }
            }
            Ok(val) => {
                eprintln!("    unexpected return value from task: {:?}", val)
            }
        };
    } else if name == "abandoned_vmm_reaper" {
        match serde_json::from_value::<AbandonedVmmReaperStatus>(
            details.clone(),
        ) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),
            Ok(AbandonedVmmReaperStatus {
                vmms_found,
                vmms_deleted,
                vmms_already_deleted,
                sled_reservations_deleted,
                errors,
            }) => {
                if !errors.is_empty() {
                    println!(
                        "    task did not complete successfully! ({} errors)",
                        errors.len()
                    );
                    for error in errors {
                        println!("    > {error}");
                    }
                }

                const VMMS_FOUND: &'static str = "total abandoned VMMs found:";
                const VMMS_DELETED: &'static str = "  VMM records deleted:";
                const VMMS_ALREADY_DELETED: &'static str =
                    "  VMMs already deleted by another Nexus:";
                const SLED_RESERVATIONS_DELETED: &'static str =
                    "sled resource reservations deleted:";
                // To align the number column, figure out the length of the
                // longest line of text and add one (so that there's a space).
                //
                // Yes, I *could* just count the number of characters in each
                // line myself, but why do something by hand when you could make
                // the computer do it for you? And, this way, if we change the
                // text, we won't need to figure it out again.
                const WIDTH: usize = const_max_len(&[
                    VMMS_FOUND,
                    VMMS_DELETED,
                    VMMS_ALREADY_DELETED,
                    SLED_RESERVATIONS_DELETED,
                ]) + 1;
                const NUM_WIDTH: usize = 3;

                println!("    {VMMS_FOUND:<WIDTH$}{vmms_found:>NUM_WIDTH$}");
                println!(
                    "    {VMMS_DELETED:<WIDTH$}{vmms_deleted:>NUM_WIDTH$}"
                );
                println!(
                    "    {VMMS_ALREADY_DELETED:<WIDTH$}{:>NUM_WIDTH$}",
                    vmms_already_deleted
                );
                println!(
                    "    {SLED_RESERVATIONS_DELETED:<WIDTH$}{:>NUM_WIDTH$}",
                    sled_reservations_deleted,
                );
            }
        };
    } else if name == "region_replacement_driver" {
        match serde_json::from_value::<RegionReplacementDriverStatus>(
            details.clone(),
        ) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),

            Ok(status) => {
                println!(
                    "    region replacement drive sagas started ok: {}",
                    status.drive_invoked_ok.len()
                );
                for line in &status.drive_invoked_ok {
                    println!("    > {line}");
                }

                println!(
                    "    region replacement finish sagas started ok: {}",
                    status.finish_invoked_ok.len()
                );
                for line in &status.finish_invoked_ok {
                    println!("    > {line}");
                }

                println!("    errors: {}", status.errors.len());
                for line in &status.errors {
                    println!("    > {line}");
                }
            }
        };
    } else if name == "saga_recovery" {
        match serde_json::from_value::<nexus_saga_recovery::Report>(
            details.clone(),
        ) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),

            Ok(report) => {
                println!("    since Nexus started:");
                println!(
                    "        sagas recovered:         {:3}",
                    report.ntotal_recovered
                );
                println!(
                    "        sagas recovery errors:   {:3}",
                    report.ntotal_failures,
                );
                println!(
                    "        sagas observed started:  {:3}",
                    report.ntotal_started
                );
                println!(
                    "        sagas inferred finished: {:3}",
                    report.ntotal_finished
                );
                println!(
                    "        missing from SEC:        {:3}",
                    report.ntotal_sec_errors_missing,
                );
                println!(
                    "        bad state in SEC:        {:3}",
                    report.ntotal_sec_errors_bad_state,
                );
                match report.last_pass {
                    LastPass::NeverStarted => {
                        println!("    never run");
                    }
                    LastPass::Failed { message } => {
                        println!("    last pass FAILED: {}", message);
                    }
                    LastPass::Success(success) => {
                        println!("    last pass:");
                        println!(
                            "        found sagas: {:3} \
                            (in-progress, assigned to this Nexus)",
                            success.nfound
                        );
                        println!(
                            "        recovered:   {:3} (successfully)",
                            success.nrecovered
                        );
                        println!("        failed:      {:3}", success.nfailed);
                        println!(
                            "        skipped:     {:3} (already running)",
                            success.nskipped
                        );
                        println!(
                            "        removed:     {:3} (newly finished)",
                            success.nskipped
                        );
                    }
                };

                if report.recent_recoveries.is_empty() {
                    println!("    no recovered sagas");
                } else {
                    println!(
                        "    recently recovered sagas ({}):",
                        report.recent_recoveries.len()
                    );

                    #[derive(Tabled)]
                    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
                    struct SagaRow {
                        time: String,
                        saga_id: String,
                    }
                    let table_rows =
                        report.recent_recoveries.iter().map(|r| SagaRow {
                            time: r
                                .time
                                .to_rfc3339_opts(SecondsFormat::Secs, true),
                            saga_id: r.saga_id.to_string(),
                        });
                    let table = tabled::Table::new(table_rows)
                        .with(tabled::settings::Style::empty())
                        .with(tabled::settings::Padding::new(0, 1, 0, 0))
                        .to_string();
                    println!(
                        "{}",
                        textwrap::indent(&table.to_string(), "        ")
                    );
                }

                if report.recent_failures.is_empty() {
                    println!("    no saga recovery failures");
                } else {
                    println!(
                        "    recent sagas recovery failures ({}):",
                        report.recent_failures.len()
                    );

                    #[derive(Tabled)]
                    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
                    struct SagaRow<'a> {
                        time: String,
                        saga_id: String,
                        message: &'a str,
                    }
                    let table_rows =
                        report.recent_failures.iter().map(|r| SagaRow {
                            time: r
                                .time
                                .to_rfc3339_opts(SecondsFormat::Secs, true),
                            saga_id: r.saga_id.to_string(),
                            message: &r.message,
                        });
                    let table = tabled::Table::new(table_rows)
                        .with(tabled::settings::Style::empty())
                        .with(tabled::settings::Padding::new(0, 1, 0, 0))
                        .to_string();
                    println!(
                        "{}",
                        textwrap::indent(&table.to_string(), "        ")
                    );
                }
            }
        }
    } else if name == "lookup_region_port" {
        match serde_json::from_value::<LookupRegionPortStatus>(details.clone())
        {
            Ok(LookupRegionPortStatus { found_port_ok, errors }) => {
                println!("    total filled in ports: {}", found_port_ok.len());
                for line in &found_port_ok {
                    println!("    > {line}");
                }

                println!("    errors: {}", errors.len());
                for line in &errors {
                    println!("    > {line}");
                }
            }

            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details,
            ),
        }
    } else if name == "instance_updater" {
        let status = match serde_json::from_value::<InstanceUpdaterStatus>(
            details.clone(),
        ) {
            Err(error) => {
                eprintln!(
                    "warning: failed to interpret task details: {:?}: {:?}",
                    error, details,
                );
                return;
            }
            Ok(status) => status,
        };
        let errors = status.errors();
        let instances_found = status.total_instances_found();
        let InstanceUpdaterStatus {
            disabled,
            destroyed_active_vmms,
            failed_active_vmms,
            terminated_active_migrations,
            sagas_started,
            sagas_completed,
            saga_errors,
            query_errors,
        } = status;

        if disabled {
            println!("    task explicitly disabled by config!")
        }

        const FOUND: &'static str = "instances in need of updates:";
        const DESTROYED: &'static str =
            "  instances with Destroyed active VMMs:";
        const FAILED: &'static str = "  instances with Failed active VMMs:";
        const MIGRATIONS: &'static str =
            "  instances with terminated migrations:";
        const SAGAS_STARTED: &'static str = "update sagas started:";
        const SAGAS_COMPLETED: &'static str =
            "  update sagas completed successfully:";
        const SAGA_ERRORS: &'static str = "  update sagas failed:";
        const QUERY_ERRORS: &'static str =
            "  errors finding instances to update:";
        const WIDTH: usize = const_max_len(&[
            FOUND,
            DESTROYED,
            FAILED,
            MIGRATIONS,
            SAGAS_STARTED,
            SAGA_ERRORS,
            QUERY_ERRORS,
        ]) + 1;
        const NUM_WIDTH: usize = 3;
        if errors > 0 {
            println!(
                "    task did not complete successfully! ({errors} errors)"
            );
            println!(
                "      {QUERY_ERRORS:<WIDTH$}{:>NUM_WIDTH$}",
                query_errors.len()
            );
            for error in query_errors {
                println!("     > {error}");
            }
            println!(
                "      {SAGA_ERRORS:<WIDTH$}{:>NUM_WIDTH$}",
                saga_errors.len()
            );
            for (instance_id, error) in &saga_errors {
                match instance_id {
                    Some(id) => println!("      > {id}: {error}"),
                    None => println!("      > {error}"),
                }
            }
        }

        println!("    {FOUND:<WIDTH$}{instances_found:>NUM_WIDTH$}");
        println!("    {DESTROYED:<WIDTH$}{destroyed_active_vmms:>NUM_WIDTH$}",);
        println!("    {FAILED:<WIDTH$}{failed_active_vmms:>NUM_WIDTH$}");
        println!(
            "    {MIGRATIONS:<WIDTH$}{:>NUM_WIDTH$}",
            terminated_active_migrations,
        );
        println!("    {SAGAS_STARTED:<WIDTH$}{sagas_started:>NUM_WIDTH$}");
        println!("    {SAGAS_COMPLETED:<WIDTH$}{sagas_completed:>NUM_WIDTH$}",);
        println!("    {SAGA_ERRORS:<WIDTH$}{:>NUM_WIDTH$}", saga_errors.len());
    } else if name == "region_snapshot_replacement_start" {
        match serde_json::from_value::<RegionSnapshotReplacementStartStatus>(
            details.clone(),
        ) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),

            Ok(status) => {
                println!(
                    "    total requests created ok: {}",
                    status.requests_created_ok.len(),
                );
                for line in &status.requests_created_ok {
                    println!("    > {line}");
                }

                println!(
                    "    total start saga invoked ok: {}",
                    status.start_invoked_ok.len(),
                );
                for line in &status.start_invoked_ok {
                    println!("    > {line}");
                }

                println!(
                    "    total requests completed ok: {}",
                    status.requests_completed_ok.len(),
                );
                for line in &status.requests_completed_ok {
                    println!("    > {line}");
                }

                println!("    errors: {}", status.errors.len());
                for line in &status.errors {
                    println!("    > {line}");
                }
            }
        }
    } else if name == "region_snapshot_replacement_garbage_collection" {
        match serde_json::from_value::<
            RegionSnapshotReplacementGarbageCollectStatus,
        >(details.clone())
        {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),

            Ok(status) => {
                println!(
                    "    total garbage collections requested: {}",
                    status.garbage_collect_requested.len(),
                );
                for line in &status.garbage_collect_requested {
                    println!("    > {line}");
                }

                println!("    errors: {}", status.errors.len());
                for line in &status.errors {
                    println!("    > {line}");
                }
            }
        }
    } else if name == "region_snapshot_replacement_step" {
        match serde_json::from_value::<RegionSnapshotReplacementStepStatus>(
            details.clone(),
        ) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),

            Ok(status) => {
                println!(
                    "    total step records created ok: {}",
                    status.step_records_created_ok.len(),
                );
                for line in &status.step_records_created_ok {
                    println!("    > {line}");
                }

                println!(
                    "    total step garbage collect saga invoked ok: {}",
                    status.step_garbage_collect_invoked_ok.len(),
                );
                for line in &status.step_garbage_collect_invoked_ok {
                    println!("    > {line}");
                }

                println!(
                    "    total step saga invoked ok: {}",
                    status.step_invoked_ok.len(),
                );
                for line in &status.step_invoked_ok {
                    println!("    > {line}");
                }

                println!(
                    "    total steps set to volume_deleted ok: {}",
                    status.step_set_volume_deleted_ok.len(),
                );
                for line in &status.step_set_volume_deleted_ok {
                    println!("    > {line}");
                }

                println!("    errors: {}", status.errors.len());
                for line in &status.errors {
                    println!("    > {line}");
                }
            }
        }
    } else if name == "blueprint_loader" {
        #[derive(Deserialize)]
        struct BlueprintLoaderStatus {
            target_id: Uuid,
            time_created: DateTime<Utc>,
            status: String,
            enabled: bool,
        }

        match serde_json::from_value::<BlueprintLoaderStatus>(details.clone()) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),
            Ok(status) => {
                println!("    target blueprint: {}", status.target_id);
                println!(
                    "    execution:        {}",
                    if status.enabled { "enabled" } else { "disabled" }
                );
                println!(
                    "    created at:       {}",
                    humantime::format_rfc3339_millis(
                        status.time_created.into()
                    )
                );
                println!("    status:           {}", status.status);
            }
        }
    } else if name == "blueprint_executor" {
        let mut value = details.clone();
        // Extract and remove the event report. (If we don't do this, the
        // `Debug` output can be quite large.)
        //
        // TODO: show more of the event buffer.
        let event_buffer = extract_event_buffer(&mut value);

        #[derive(Deserialize)]
        struct BlueprintExecutorStatus {
            target_id: Uuid,
            enabled: bool,
            execution_error: Option<NestedError>,
        }

        match serde_json::from_value::<BlueprintExecutorStatus>(value) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),
            Ok(status) => {
                // TODO: switch the other outputs to tabled as well.
                let mut builder = tabled::builder::Builder::default();
                builder.push_record([
                    "target blueprint:".to_string(),
                    status.target_id.to_string(),
                ]);
                builder.push_record([
                    "execution:".to_string(),
                    if status.enabled {
                        "enabled".to_string()
                    } else {
                        "disabled".to_string()
                    },
                ]);

                push_event_buffer_summary(event_buffer, &mut builder);

                match status.execution_error {
                    Some(error) => {
                        builder.push_record([
                            "error:".to_string(),
                            error.to_string(),
                        ]);

                        for source in error.sources() {
                            builder.push_record([
                                "  caused by:".to_string(),
                                source.to_string(),
                            ]);
                        }
                    }
                    None => {
                        builder.push_record([
                            "error:".to_string(),
                            "(none)".to_string(),
                        ]);
                    }
                }

                let mut table = builder.build();
                bgtask_apply_kv_style(&mut table);
                println!("{}", table);
            }
        }
    } else if name == "region_snapshot_replacement_finish" {
        match serde_json::from_value::<RegionSnapshotReplacementFinishStatus>(
            details.clone(),
        ) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),

            Ok(status) => {
                println!(
                    "    region snapshot replacement finish sagas started \
                    ok: {}",
                    status.finish_invoked_ok.len()
                );
                for line in &status.finish_invoked_ok {
                    println!("    > {line}");
                }

                println!("    errors: {}", status.errors.len());
                for line in &status.errors {
                    println!("    > {line}");
                }
            }
        }
    } else if name == "instance_reincarnation" {
        match serde_json::from_value::<InstanceReincarnationStatus>(
            details.clone(),
        ) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),
            Ok(status) => {
                const FOUND: &'static str =
                    "instances eligible for reincarnation:";
                const REINCARNATED: &'static str =
                    "instances reincarnated successfully:";
                const CHANGED_STATE: &'static str =
                    "instances which changed state before they could reincarnate:";
                const ERRORS: &'static str =
                    "instances which failed to reincarnate:";
                const WIDTH: usize = const_max_len(&[
                    FOUND,
                    REINCARNATED,
                    CHANGED_STATE,
                    ERRORS,
                ]);
                if status.disabled {
                    println!(
                        "    instance reincarnation explicitly disabled \
                         by config!"
                    );
                    return;
                }

                if !status.errors.is_empty() {
                    println!(
                        "    errors occurred while finding instances to \
                          reincarnate:"
                    );
                    for error in &status.errors {
                        println!("    > {error}")
                    }
                }

                let n_restart_errors = status.restart_errors.len();
                let n_restarted = status.instances_reincarnated.len();
                let n_changed_state = status.changed_state.len();
                println!(
                    "    {FOUND:<WIDTH$} {:>3}",
                    status.total_instances_found()
                );
                for (reason, count) in &status.instances_found {
                    let reason = format!("  {reason} instances:");
                    println!("    {reason:<WIDTH$} {count:>3}",);
                }
                println!("    {REINCARNATED:<WIDTH$} {n_restarted:>3}");
                println!("    {CHANGED_STATE:<WIDTH$} {n_changed_state:>3}",);
                println!("    {ERRORS:<WIDTH$} {n_restart_errors:>3}");

                if n_restart_errors > 0 {
                    println!(
                        "    errors occurred while restarting the following \
                         instances:"
                    );
                    for (id, error) in status.restart_errors {
                        println!("    > {id}: {error}");
                    }
                }

                if n_restarted > 0 {
                    println!("    the following instances have reincarnated:");
                    for id in status.instances_reincarnated {
                        println!("    > {id}")
                    }
                }

                if n_changed_state > 0 {
                    println!(
                        "    the following instances states changed before \
                         they could be reincarnated:"
                    );
                    for id in status.changed_state {
                        println!("    > {id}")
                    }
                }
            }
        };
    } else {
        println!(
            "warning: unknown background task: {:?} \
            (don't know how to interpret details: {:?})",
            name, details
        );
    }

    println!("");
}

/// Summarizes an `ActivationReason`
fn reason_str(reason: &ActivationReason) -> &'static str {
    match reason {
        ActivationReason::Signaled => "an explicit signal",
        ActivationReason::Dependency => "a dependent task completing",
        ActivationReason::Timeout => "a periodic timer firing",
    }
}

fn bgtask_apply_kv_style(table: &mut tabled::Table) {
    let style = tabled::settings::Style::empty();
    table.with(style).with(
        tabled::settings::Modify::new(Columns::first())
            // Background task tables are offset by 4 characters.
            .with(Padding::new(4, 0, 0, 0)),
    );
}

/// Extract and remove the event report, returning None if it wasn't found and
/// an error if something else went wrong. (If we don't do this, the `Debug`
/// output can be quite large.)
fn extract_event_buffer(
    value: &mut serde_json::Value,
) -> anyhow::Result<Option<EventBuffer<NestedSpec>>> {
    let Some(obj) = value.as_object_mut() else {
        bail!("expected value to be an object")
    };
    let Some(event_report) = obj.remove("event_report") else {
        return Ok(None);
    };

    // Try deserializing the event report generically. We could deserialize to
    // a more explicit spec, e.g. `ReconfiguratorExecutionSpec`, but that's
    // unnecessary for omdb's purposes.
    let value: Result<EventReport<NestedSpec>, NestedError> =
        serde_json::from_value(event_report)
            .context("failed to deserialize event report")?;
    let event_report = value.context(
        "event report stored as Err rather than Ok (did receiver task panic?)",
    )?;

    let mut event_buffer = EventBuffer::default();
    event_buffer.add_event_report(event_report);
    Ok(Some(event_buffer))
}

// Make a short summary of the current state of an execution based on an event
// buffer, and add it to the table.
fn push_event_buffer_summary(
    event_buffer: anyhow::Result<Option<EventBuffer<NestedSpec>>>,
    builder: &mut tabled::builder::Builder,
) {
    match event_buffer {
        Ok(Some(buffer)) => {
            event_buffer_summary_impl(buffer, builder);
        }
        Ok(None) => {
            builder.push_record(["status:", "(no event report found)"]);
        }
        Err(error) => {
            builder.push_record([
                "event report error:".to_string(),
                error.to_string(),
            ]);
            for source in error.chain() {
                builder.push_record([
                    "  caused by:".to_string(),
                    source.to_string(),
                ]);
            }
        }
    }
}

fn event_buffer_summary_impl(
    buffer: EventBuffer<NestedSpec>,
    builder: &mut tabled::builder::Builder,
) {
    let Some(summary) = buffer.root_execution_summary() else {
        builder.push_record(["status:", "(no information found)"]);
        return;
    };

    match summary.execution_status {
        ExecutionStatus::NotStarted => {
            builder.push_record(["status:", "not started"]);
        }
        ExecutionStatus::Running { step_key, .. } => {
            let step_data = buffer.get(&step_key).expect("step exists");
            builder.push_record([
                "status:".to_string(),
                format!(
                    "running: {} (step {})",
                    step_data.step_info().description,
                    ProgressRatioDisplay::index_and_total(
                        step_key.index,
                        summary.total_steps,
                    ),
                ),
            ]);
        }
        ExecutionStatus::Terminal(info) => {
            push_event_buffer_terminal_info(
                &info,
                summary.total_steps,
                &buffer,
                builder,
            );
        }
    }

    // Also look for warnings.
    for (_, step_data) in buffer.iter_steps_recursive() {
        if let Some(reason) = step_data.step_status().completion_reason() {
            if let Some(info) = reason.step_completed_info() {
                if let StepOutcome::Warning { message, .. } = &info.outcome {
                    builder.push_record([
                        "warning:".to_string(),
                        // This can be a nested step, so don't print out the
                        // index.
                        format!(
                            "at: {}: {}",
                            step_data.step_info().description,
                            message
                        ),
                    ]);
                }
            }
        }
    }
}

fn push_event_buffer_terminal_info(
    info: &ExecutionTerminalInfo,
    total_steps: usize,
    buffer: &EventBuffer<NestedSpec>,
    builder: &mut tabled::builder::Builder,
) {
    let step_data = buffer.get(&info.step_key).expect("step exists");

    match info.kind {
        TerminalKind::Completed => {
            let v = format!("completed ({} steps)", total_steps);
            builder.push_record(["status:".to_string(), v]);
        }
        TerminalKind::Failed => {
            let v = format!(
                "failed at: {} (step {})",
                step_data.step_info().description,
                ProgressRatioDisplay::index_and_total(
                    info.step_key.index,
                    total_steps,
                )
            );
            builder.push_record(["status:".to_string(), v]);

            // Don't show the error here, because it's duplicated in another
            // field that's already shown.
        }
        TerminalKind::Aborted => {
            let v = format!(
                "aborted at: {} (step {})",
                step_data.step_info().description,
                ProgressRatioDisplay::index_and_total(
                    info.step_key.index,
                    total_steps,
                )
            );
            builder.push_record(["status:".to_string(), v]);

            let Some(reason) = step_data.step_status().abort_reason() else {
                builder.push_record(["  reason:", "(no reason found)"]);
                return;
            };

            builder.push_record([
                "  reason:".to_string(),
                reason.message_display(&buffer).to_string(),
            ]);
            // TODO: show last progress event
        }
    }
}

/// Used for printing background task status as a table
#[derive(Tabled)]
struct BackgroundTaskStatusRow {
    task_name: String,
    #[tabled(rename = "PGEN#")]
    completed_generation: String,
    #[tabled(rename = "PSTART")]
    completed_start_time: String,
    #[tabled(rename = "Psecs")]
    completed_elapsed: String,
    #[tabled(rename = "P")]
    completed_reason: char,
    #[tabled(rename = "CSTART")]
    running_since: String,
    #[tabled(rename = "C")]
    running_reason: char,
}

impl<'a> From<&'a BackgroundTask> for BackgroundTaskStatusRow {
    fn from(t: &'a BackgroundTask) -> Self {
        let (
            completed_generation,
            completed_start_time,
            completed_elapsed,
            completed_reason,
        ) = match &t.last {
            LastResult::NeverCompleted => {
                (String::from("-"), String::from("-"), String::from("-"), '-')
            }
            LastResult::Completed(last) => (
                last.iteration.to_string(),
                last.start_time.to_rfc3339_opts(SecondsFormat::Secs, true),
                format!(
                    "{:5.1}",
                    std::time::Duration::from(last.elapsed.clone())
                        .as_secs_f64()
                ),
                reason_code(last.reason),
            ),
        };

        let (running_since, running_reason) = match &t.current {
            CurrentStatus::Idle => (String::from("-"), '-'),
            CurrentStatus::Running(current) => (
                current.start_time.to_rfc3339_opts(SecondsFormat::Secs, true),
                reason_code(current.reason),
            ),
        };

        BackgroundTaskStatusRow {
            task_name: t.name.clone(),
            completed_generation,
            completed_start_time,
            completed_elapsed,
            completed_reason,
            running_since,
            running_reason,
        }
    }
}

fn reason_code(reason: ActivationReason) -> char {
    match reason {
        ActivationReason::Signaled => 'S',
        ActivationReason::Dependency => 'D',
        ActivationReason::Timeout => 'T',
    }
}

async fn cmd_nexus_blueprints_list(
    client: &nexus_client::Client,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct BlueprintRow {
        #[tabled(rename = "T")]
        is_target: &'static str,
        #[tabled(rename = "ENA")]
        enabled: &'static str,
        id: String,
        parent: String,
        time_created: String,
    }

    let target = match client.blueprint_target_view().await {
        Ok(result) => Some(result.into_inner()),
        Err(error) => {
            // This request will fail if there's no target configured, so it's
            // not necessarily a big deal.
            eprintln!(
                "warn: failed to fetch current target: {}",
                InlineErrorChain::new(&error),
            );
            None
        }
    };

    let mut rows = client
        .blueprint_list_stream(None, None)
        .try_collect::<Vec<_>>()
        .await
        .context("listing blueprints")?;

    rows.sort_unstable_by_key(|blueprint| blueprint.time_created);

    let rows: Vec<_> = rows
        .into_iter()
        .map(|blueprint| {
            let (is_target, enabled) = match &target {
                Some(target) if target.target_id == blueprint.id => {
                    let enabled = if target.enabled { "yes" } else { "no" };
                    ("*", enabled)
                }
                _ => ("", ""),
            };

            BlueprintRow {
                is_target,
                enabled,
                id: blueprint.id.to_string(),
                parent: blueprint
                    .parent_blueprint_id
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| String::from("<none>")),
                time_created: humantime::format_rfc3339_millis(
                    blueprint.time_created.into(),
                )
                .to_string(),
            }
        })
        .collect();

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);
    Ok(())
}

async fn cmd_nexus_blueprints_show(
    client: &nexus_client::Client,
    args: &BlueprintIdArgs,
) -> Result<(), anyhow::Error> {
    let blueprint = args.blueprint_id.resolve_to_blueprint(client).await?;
    println!("{}", blueprint.display());
    Ok(())
}

async fn cmd_nexus_blueprints_diff(
    client: &nexus_client::Client,
    args: &BlueprintIdsArgs,
) -> Result<(), anyhow::Error> {
    let (b1, b2) = try_join(
        args.blueprint1_id.resolve_to_blueprint(client),
        args.blueprint2_id.resolve_to_blueprint(client),
    )
    .await?;
    let diff = b2.diff_since_blueprint(&b1);
    println!("{}", diff.display());
    Ok(())
}

async fn cmd_nexus_blueprints_delete(
    client: &nexus_client::Client,
    args: &BlueprintIdArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let blueprint_id = args.blueprint_id.resolve_to_id(client).await?;
    let _ = client
        .blueprint_delete(&blueprint_id)
        .await
        .with_context(|| format!("deleting blueprint {blueprint_id}"))?;
    println!("blueprint {blueprint_id} deleted");
    Ok(())
}

async fn cmd_nexus_blueprints_target_show(
    client: &nexus_client::Client,
) -> Result<(), anyhow::Error> {
    let target = client
        .blueprint_target_view()
        .await
        .context("fetching target blueprint")?;
    println!("target blueprint: {}", target.target_id);
    println!("made target at:   {}", target.time_made_target);
    println!("enabled:          {}", target.enabled);
    Ok(())
}

async fn cmd_nexus_blueprints_target_set(
    client: &nexus_client::Client,
    args: &BlueprintTargetSetArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    // Helper to only fetch the current target once. We may need it immediately
    // if `args.diff` is true, or later if `args.enabled` is "inherit" (or
    // both).
    let current_target = OnceCell::new();
    let get_current_target = || async {
        current_target
            .get_or_try_init(|| client.blueprint_target_view())
            .await
            .context("failed to fetch current target blueprint")
    };

    if args.diff {
        let current_target = get_current_target().await?;
        let blueprint1 = client
            .blueprint_view(&current_target.target_id)
            .await
            .context("failed to fetch target blueprint")?
            .into_inner();
        let blueprint2 =
            client.blueprint_view(&args.blueprint_id).await.with_context(
                || format!("fetching blueprint {}", args.blueprint_id),
            )?;
        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!("{}", diff.display());
        println!(
            "\nDo you want to make {} the target blueprint?",
            args.blueprint_id
        );
        let mut prompt = ConfirmationPrompt::new();
        prompt.read_and_validate("y/N", "y")?;
    }

    let enabled = match args.enabled {
        BlueprintTargetSetEnabled::Enabled => true,
        BlueprintTargetSetEnabled::Disabled => false,
        // There's a small TOCTOU race with "inherit": What if the user wants to
        // inherit the parent blueprint enabled bit but the current target
        // blueprint enabled bit is flipped or the current target blueprint is
        // changed? We expect neither of these to be problematic in practice:
        // the only way for the `enable` bit to be set to anything at all is via
        // `omdb`, so the user would have to be racing with another `omdb`
        // operator. (In the case of the current target blueprint being changed
        // entirely, that will result in a failure to set the current target
        // below, because its parent will no longer be the current target.)
        BlueprintTargetSetEnabled::Inherit => {
            get_current_target().await?.enabled
        }
    };

    client
        .blueprint_target_set(&nexus_client::types::BlueprintTargetSet {
            target_id: args.blueprint_id,
            enabled,
        })
        .await
        .with_context(|| {
            format!("setting target to blueprint {}", args.blueprint_id)
        })?;
    eprintln!("set target blueprint to {}", args.blueprint_id);
    Ok(())
}

async fn cmd_nexus_blueprints_target_set_enabled(
    client: &nexus_client::Client,
    args: &BlueprintIdArgs,
    enabled: bool,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let blueprint_id = args.blueprint_id.resolve_to_id(client).await?;
    let description = if enabled { "enabled" } else { "disabled" };
    client
        .blueprint_target_set_enabled(
            &nexus_client::types::BlueprintTargetSet {
                target_id: blueprint_id,
                enabled,
            },
        )
        .await
        .with_context(|| {
            format!("setting blueprint {blueprint_id} to {description}")
        })?;
    eprintln!("set target blueprint {blueprint_id} to {description}");
    Ok(())
}

async fn cmd_nexus_blueprints_regenerate(
    client: &nexus_client::Client,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let blueprint =
        client.blueprint_regenerate().await.context("generating blueprint")?;
    eprintln!("generated new blueprint {}", blueprint.id);
    Ok(())
}

async fn cmd_nexus_blueprints_import(
    client: &nexus_client::Client,
    _destruction_token: DestructiveOperationToken,
    args: &BlueprintImportArgs,
) -> Result<(), anyhow::Error> {
    let input_path = &args.input;
    let contents = std::fs::read_to_string(input_path)
        .with_context(|| format!("open {:?}", input_path))?;
    let blueprint: Blueprint = serde_json::from_str(&contents)
        .with_context(|| format!("read {:?}", input_path))?;
    client
        .blueprint_import(&blueprint)
        .await
        .with_context(|| format!("upload {:?}", input_path))?;
    eprintln!("uploaded new blueprint {}", blueprint.id);
    Ok(())
}

async fn cmd_nexus_clickhouse_policy_get(
    client: &nexus_client::Client,
) -> Result<(), anyhow::Error> {
    let res = client.clickhouse_policy_get().await;

    match res {
        Err(err) => {
            if err.status() == Some(StatusCode::NOT_FOUND) {
                println!(
                    "No clickhouse policy: \
                    Defaulting to single-node deployment"
                );
            } else {
                eprintln!("error: {:#}", err);
            }
        }
        Ok(policy) => {
            println!("Clickhouse Policy: ");
            println!("    version: {}", policy.version);
            println!("    creation time: {}", policy.time_created);
            match policy.mode {
                ClickhouseMode::SingleNodeOnly => {
                    println!("    mode: single-node-only");
                }
                ClickhouseMode::ClusterOnly {
                    target_servers,
                    target_keepers,
                } => {
                    println!("    mode: cluster-only");
                    println!("    target-servers: {}", target_servers);
                    println!("    target-keepers: {}", target_keepers);
                }
                ClickhouseMode::Both { target_servers, target_keepers } => {
                    println!("    mode: both single-node and cluster");
                    println!("    target-servers: {}", target_servers);
                    println!("    target-keepers: {}", target_keepers);
                }
            }
        }
    }

    Ok(())
}

async fn cmd_nexus_clickhouse_policy_set(
    client: &nexus_client::Client,
    args: &ClickhousePolicySetArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let mode = match args.mode {
        ClickhousePolicyMode::SingleNodeOnly => ClickhouseMode::SingleNodeOnly,
        ClickhousePolicyMode::ClusterOnly => ClickhouseMode::ClusterOnly {
            target_servers: args.target_servers,
            target_keepers: args.target_keepers,
        },
        ClickhousePolicyMode::Both => ClickhouseMode::Both {
            target_servers: args.target_servers,
            target_keepers: args.target_keepers,
        },
    };

    let res = client.clickhouse_policy_get().await;
    let new_policy = match res {
        Err(err) => {
            if err.status() == Some(StatusCode::NOT_FOUND) {
                ClickhousePolicy {
                    version: 1,
                    mode,
                    time_created: now_db_precision(),
                }
            } else {
                eprintln!("error: {:#}", err);
                return Err(err).context("retrieving clickhouse policy");
            }
        }
        Ok(policy) => ClickhousePolicy {
            version: policy.version + 1,
            mode,
            time_created: now_db_precision(),
        },
    };

    client.clickhouse_policy_set(&new_policy).await.with_context(|| {
        format!("inserting new context at version {}", new_policy.version)
    })?;

    println!(
        "Successfully inserted new policy at version {}",
        new_policy.version
    );

    Ok(())
}

/// Runs `omdb nexus sagas list`
async fn cmd_nexus_sagas_list(
    client: &nexus_client::Client,
) -> Result<(), anyhow::Error> {
    // We don't want users to confuse this with a general way to list all sagas.
    // Such a command would read database state and it would go under "omdb db".
    eprintln!(
        "{}",
        textwrap::wrap(
            "NOTE: This command only reads in-memory state from the targeted \
            Nexus instance.  Sagas may be missing if they were run by a \
            different Nexus instance or if they finished before this Nexus \
            instance last started up.",
            80
        )
        .join("\n")
    );

    let saga_stream = client.saga_list_stream(None, None);
    let sagas =
        saga_stream.try_collect::<Vec<_>>().await.context("listing sagas")?;

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct SagaRow {
        saga_id: Uuid,
        state: &'static str,
    }
    let rows = sagas.into_iter().map(|saga| SagaRow {
        saga_id: saga.id,
        state: match saga.state {
            SagaState::Running => "running",
            SagaState::Succeeded => "succeeded",
            SagaState::Failed { .. } => "failed",
            SagaState::Stuck { .. } => "stuck",
        },
    });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{}", table);
    Ok(())
}

/// Runs `omdb nexus sagas demo-create`
async fn cmd_nexus_sagas_demo_create(
    client: &nexus_client::Client,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let demo_saga =
        client.saga_demo_create().await.context("creating demo saga")?;
    println!("saga id:      {}", demo_saga.saga_id);
    println!(
        "demo saga id: {} (use this with `demo-complete`)",
        demo_saga.demo_saga_id,
    );
    Ok(())
}

/// Runs `omdb nexus sagas demo-complete`
async fn cmd_nexus_sagas_demo_complete(
    client: &nexus_client::Client,
    args: &DemoSagaIdArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    if let Err(error) = client
        .saga_demo_complete(&args.demo_saga_id)
        .await
        .context("completing demo saga")
    {
        eprintln!("error: {:#}", error);
        eprintln!(
            "note: `demo-complete` must be run against the same Nexus \
            instance that is currently running that saga."
        );
        eprintln!(
            "note: Be sure that you're using the demo_saga_id, not the saga_id."
        );
        Err(error)
    } else {
        Ok(())
    }
}

/// Runs `omdb nexus sleds list-uninitialized`
async fn cmd_nexus_sleds_list_uninitialized(
    client: &nexus_client::Client,
) -> Result<(), anyhow::Error> {
    let response = client
        .sled_list_uninitialized()
        .await
        .context("listing uninitialized sleds")?;
    let sleds = response.into_inner();
    if sleds.next_page.is_some() {
        eprintln!(
            "warning: response includes next_page token; \
             pagination not implemented"
        );
    }
    let mut sleds = sleds.items;
    sleds.sort_by_key(|sled| sled.cubby);

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct UninitializedSledRow {
        rack_id: Uuid,
        cubby: u16,
        serial: String,
        part: String,
        revision: u32,
    }
    let rows = sleds.into_iter().map(|sled| UninitializedSledRow {
        rack_id: sled.rack_id,
        cubby: sled.cubby,
        serial: sled.baseboard.serial,
        part: sled.baseboard.part,
        revision: sled.baseboard.revision,
    });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{}", table);
    Ok(())
}

/// Runs `omdb nexus sleds add`
async fn cmd_nexus_sled_add(
    client: &nexus_client::Client,
    args: &SledAddArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let sled_id = client
        .sled_add(&UninitializedSledId {
            part: args.part.clone(),
            serial: args.serial.clone(),
        })
        .await
        .context("adding sled")?
        .into_inner()
        .id;
    eprintln!("added sled {} ({}): {sled_id}", args.serial, args.part);
    Ok(())
}

struct ConfirmationPrompt(Reedline);

impl ConfirmationPrompt {
    fn new() -> Self {
        Self(Reedline::create())
    }

    fn read(&mut self, message: &str) -> Result<String, anyhow::Error> {
        let prompt = DefaultPrompt::new(
            DefaultPromptSegment::Basic(message.to_string()),
            DefaultPromptSegment::Empty,
        );
        if let Ok(reedline::Signal::Success(input)) = self.0.read_line(&prompt)
        {
            Ok(input)
        } else {
            bail!("operation aborted")
        }
    }

    fn read_and_validate(
        &mut self,
        message: &str,
        expected: &str,
    ) -> Result<(), anyhow::Error> {
        let input = self.read(message)?;
        if input != expected {
            bail!("Aborting, input did not match expected value");
        }
        Ok(())
    }
}

/// Runs `omdb nexus sleds expunge`
async fn cmd_nexus_sled_expunge(
    client: &nexus_client::Client,
    args: &SledExpungeArgs,
    omdb: &Omdb,
    log: &slog::Logger,
    destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let datastore = args.db_url_opts.connect(omdb, log).await?;
    let result = cmd_nexus_sled_expunge_with_datastore(
        &datastore,
        client,
        args,
        log,
        destruction_token,
    )
    .await;
    datastore.terminate().await;
    result
}

// `omdb nexus sleds expunge`, but borrowing a datastore
async fn cmd_nexus_sled_expunge_with_datastore(
    datastore: &Arc<DataStore>,
    client: &nexus_client::Client,
    args: &SledExpungeArgs,
    log: &slog::Logger,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    // This is an extremely dangerous and irreversible operation. We put a
    // couple of safeguards in place to ensure this cannot be called without
    // due consideration:
    //
    // 1. We'll require manual input on stdin to confirm the sled to be removed
    // 2. We'll warn sternly if the sled-to-be-expunged is still present in the
    //    most recent inventory collection
    use nexus_db_queries::context::OpContext;

    let opctx = OpContext::for_tests(log.clone(), datastore.clone());
    let opctx = &opctx;

    // First, we need to look up the sled so we know its serial number.
    let (_authz_sled, sled) = LookupPath::new(opctx, &datastore)
        .sled_id(args.sled_id.into_untyped_uuid())
        .fetch()
        .await
        .with_context(|| format!("failed to find sled {}", args.sled_id))?;

    // Helper to get confirmation messages from the user.
    let mut prompt = ConfirmationPrompt::new();

    // Now check whether its sled-agent or SP were found in the most recent
    // inventory collection.
    match datastore
        .inventory_get_latest_collection(opctx)
        .await
        .context("loading latest collection")?
    {
        Some(collection) => {
            let sled_baseboard = BaseboardId {
                part_number: sled.part_number().to_string(),
                serial_number: sled.serial_number().to_string(),
            };
            // Check the collection for either the sled-id or the baseboard. In
            // general a sled-agent's self report should result in both the
            // sled-id and the baseboard being present in the collection, but
            // there are some test environments (e.g., `omicron-dev run-all`)
            // where that isn't guaranteed.
            let sled_present_in_collection =
                collection.sled_agents.contains_key(&args.sled_id)
                    || collection.baseboards.contains(&sled_baseboard);
            if sled_present_in_collection {
                eprintln!(
                    "WARNING: sled {} is PRESENT in the most recent inventory \
                     collection (spotted at {}). It is dangerous to expunge a \
                     sled that is still running. Are you sure you want to \
                     proceed anyway?",
                    args.sled_id, collection.time_done,
                );
                prompt.read_and_validate("y/N", "y")?;
            }
        }
        None => {
            eprintln!(
                "WARNING: cannot verify that the sled is physically gone \
                 because there are no inventory collections present. Please \
                 make sure that the sled has been physically removed."
            );
        }
    }

    eprintln!(
        "WARNING: This operation will PERMANENTLY and IRRECOVABLY mark sled \
        {} ({}) expunged. To proceed, type the sled's serial number.",
        args.sled_id,
        sled.serial_number(),
    );
    prompt.read_and_validate("sled serial number", sled.serial_number())?;

    let old_policy = client
        .sled_expunge(&SledSelector { sled: args.sled_id.into_untyped_uuid() })
        .await
        .context("expunging sled")?
        .into_inner();
    eprintln!(
        "expunged sled {} (previous policy: {old_policy:?})",
        args.sled_id
    );
    Ok(())
}

/// Runs `omdb nexus sleds expunge-disk`
async fn cmd_nexus_sled_expunge_disk(
    client: &nexus_client::Client,
    args: &DiskExpungeArgs,
    omdb: &Omdb,
    log: &slog::Logger,
    destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let datastore = args.db_url_opts.connect(omdb, log).await?;
    let result = cmd_nexus_sled_expunge_disk_with_datastore(
        &datastore,
        client,
        args,
        log,
        destruction_token,
    )
    .await;
    datastore.terminate().await;
    result
}

async fn cmd_nexus_sled_expunge_disk_with_datastore(
    datastore: &Arc<DataStore>,
    client: &nexus_client::Client,
    args: &DiskExpungeArgs,
    log: &slog::Logger,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    use nexus_db_queries::context::OpContext;

    let opctx = OpContext::for_tests(log.clone(), datastore.clone());
    let opctx = &opctx;

    // First, we need to look up the disk so we can lookup identity information.
    let (_authz_physical_disk, physical_disk) =
        LookupPath::new(opctx, &datastore)
            .physical_disk(args.physical_disk_id)
            .fetch()
            .await
            .with_context(|| {
                format!(
                    "failed to find physical disk {}",
                    args.physical_disk_id
                )
            })?;

    // Helper to get confirmation messages from the user.
    let mut prompt = ConfirmationPrompt::new();

    // Now check whether its sled-agent was found in the most recent
    // inventory collection.
    match datastore
        .inventory_get_latest_collection(opctx)
        .await
        .context("loading latest collection")?
    {
        Some(collection) => {
            let disk_identity = omicron_common::disk::DiskIdentity {
                vendor: physical_disk.vendor.clone(),
                serial: physical_disk.serial.clone(),
                model: physical_disk.model.clone(),
            };

            let mut sleds_containing_disk = vec![];

            for (sled_id, sled_agent) in collection.sled_agents {
                for sled_disk in sled_agent.disks {
                    if sled_disk.identity == disk_identity {
                        sleds_containing_disk.push(sled_id);
                    }
                }
            }

            match sleds_containing_disk.len() {
                0 => {}
                1 => {
                    eprintln!(
                        "WARNING: physical disk {} is PRESENT in the most \
                         recent inventory collection (spotted at {}). Although \
                         expunging a running disk is supported, it is safer \
                         to expunge a disk from a system where it has been \
                         removed. Are you sure you want to proceed anyway?",
                        args.physical_disk_id, collection.time_done,
                    );
                    prompt.read_and_validate("y/N", "y")?;
                }
                _ => {
                    // This should be impossible due to a unique database index,
                    // "vendor_serial_model_unique".
                    //
                    // Even if someone tried moving a disk, it would need to be
                    // decommissioned before being re-commissioned elsewhere.
                    //
                    // However, we still print out an error message here in the
                    // (unlikely) even that it happens anyway.
                    eprintln!(
                        "ERROR: physical disk {} is PRESENT MULTIPLE TIMES in \
                        the most recent inventory collection (spotted at {}).
                        This should not be possible, and is an indication of a \
                        database issue.",
                        args.physical_disk_id, collection.time_done,
                    );
                    bail!("Physical Disk appeared on multiple sleds");
                }
            }
        }
        None => {
            eprintln!(
                "ERROR: cannot verify the physical disk inventory status \
                 because there are no inventory collections present. Please \
                 ensure that inventory may be collected."
            );
            bail!("No inventory");
        }
    }

    eprintln!(
        "WARNING: This operation will PERMANENTLY and IRRECOVABLY mark \
        physical disk {} ({}) expunged. To proceed, type the physical disk's \
        serial number.",
        args.physical_disk_id, physical_disk.serial,
    );
    prompt.read_and_validate("disk serial number", &physical_disk.serial)?;

    client
        .physical_disk_expunge(&PhysicalDiskPath {
            disk_id: args.physical_disk_id.into_untyped_uuid(),
        })
        .await
        .context("expunging disk")?;
    eprintln!("expunged disk {}", args.physical_disk_id);
    Ok(())
}
