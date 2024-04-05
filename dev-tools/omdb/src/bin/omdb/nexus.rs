// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query or update specific Nexus instances

use crate::check_allow_destructive::DestructiveOperationToken;
use crate::db::DbUrlOptions;
use crate::Omdb;
use anyhow::bail;
use anyhow::Context;
use camino::Utf8PathBuf;
use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use clap::Args;
use clap::Subcommand;
use clap::ValueEnum;
use futures::TryStreamExt;
use nexus_client::types::ActivationReason;
use nexus_client::types::BackgroundTask;
use nexus_client::types::CurrentStatus;
use nexus_client::types::LastResult;
use nexus_client::types::SledSelector;
use nexus_client::types::UninitializedSledId;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_types::deployment::Blueprint;
use nexus_types::inventory::BaseboardId;
use reedline::DefaultPrompt;
use reedline::DefaultPromptSegment;
use reedline::Reedline;
use serde::Deserialize;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use tabled::Tabled;
use uuid::Uuid;

/// Arguments to the "omdb nexus" subcommand
#[derive(Debug, Args)]
pub struct NexusArgs {
    /// URL of the Nexus internal API
    #[clap(long, env("OMDB_NEXUS_URL"))]
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
    Show,
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
    /// Diff two blueprint
    Diff(BlueprintIdsArgs),
    /// Delete a blueprint
    Delete(BlueprintIdArgs),
    /// Interact with the current target blueprint
    Target(BlueprintsTargetArgs),
    /// Generate an initial blueprint from a specific inventory collection
    GenerateFromCollection(CollectionIdArgs),
    /// Generate a new blueprint
    Regenerate,
    /// Import a blueprint
    Import(BlueprintImportArgs),
}

#[derive(Debug, Args)]
struct BlueprintIdArgs {
    /// id of a blueprint
    blueprint_id: Uuid,
}

#[derive(Debug, Args)]
struct BlueprintIdsArgs {
    /// id of first blueprint
    blueprint1_id: Uuid,
    /// id of second blueprint
    blueprint2_id: Uuid,
}

#[derive(Debug, Args)]
struct CollectionIdArgs {
    /// id of an inventory collection
    collection_id: Uuid,
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
    sled_id: Uuid,
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
                let addrs = omdb
                    .dns_lookup_all(
                        log.clone(),
                        internal_dns::ServiceName::Nexus,
                    )
                    .await?;
                let addr = addrs.into_iter().next().expect(
                    "expected at least one Nexus address from \
                    successful DNS lookup",
                );
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
                command: BackgroundTasksCommands::Show,
            }) => cmd_nexus_background_tasks_show(&client).await,

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
                command: BlueprintsCommands::GenerateFromCollection(args),
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_nexus_blueprints_generate_from_collection(
                    &client, args, token,
                )
                .await
            }
            NexusCommands::Blueprints(BlueprintsArgs {
                command: BlueprintsCommands::Import(args),
            }) => {
                let token = omdb.check_allow_destructive()?;
                cmd_nexus_blueprints_import(&client, token, args).await
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
    let tasks = response.into_inner();
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
) -> Result<(), anyhow::Error> {
    let response =
        client.bgtask_list().await.context("listing background tasks")?;
    // Convert the HashMap to a BTreeMap because we want the keys in sorted
    // order.
    let mut tasks =
        response.into_inner().into_iter().collect::<BTreeMap<_, _>>();

    // We want to pick the order that we print some tasks intentionally.  Then
    // we want to print anything else that we find.
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
        } else {
            eprintln!("warning: expected to find background task {:?}", name);
        }
    }

    for (_, bgtask) in &tasks {
        print_task(bgtask);
    }

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

                if server_results.len() != 0 {
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
                if tls_cert_rows.len() > 0 {
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
        #[derive(Deserialize)]
        struct TaskSuccess {
            /// how many region replacements were started ok
            region_replacement_started_ok: usize,

            /// how many region replacements could not be started
            region_replacement_started_err: usize,
        }

        match serde_json::from_value::<TaskSuccess>(details.clone()) {
            Err(error) => eprintln!(
                "warning: failed to interpret task details: {:?}: {:?}",
                error, details
            ),
            Ok(success) => {
                println!(
                    "    number of region replacements started ok: {}",
                    success.region_replacement_started_ok
                );
                println!(
                    "    number of region replacement start errors: {}",
                    success.region_replacement_started_err
                );
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

    let rows: Vec<BlueprintRow> = client
        .blueprint_list_stream(None, None)
        .try_collect::<Vec<_>>()
        .await
        .context("listing blueprints")?
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
    let blueprint = client
        .blueprint_view(&args.blueprint_id)
        .await
        .with_context(|| format!("fetching blueprint {}", args.blueprint_id))?;
    println!("{}", blueprint.display());
    Ok(())
}

async fn cmd_nexus_blueprints_diff(
    client: &nexus_client::Client,
    args: &BlueprintIdsArgs,
) -> Result<(), anyhow::Error> {
    let b1 = client.blueprint_view(&args.blueprint1_id).await.with_context(
        || format!("fetching blueprint {}", args.blueprint1_id),
    )?;
    let b2 = client.blueprint_view(&args.blueprint2_id).await.with_context(
        || format!("fetching blueprint {}", args.blueprint2_id),
    )?;
    let diff = b2.diff_since_blueprint(&b1).context("diffing blueprints")?;
    println!("{}", diff.display());
    Ok(())
}

async fn cmd_nexus_blueprints_delete(
    client: &nexus_client::Client,
    args: &BlueprintIdArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let _ = client
        .blueprint_delete(&args.blueprint_id)
        .await
        .with_context(|| format!("deleting blueprint {}", args.blueprint_id))?;
    println!("blueprint {} deleted", args.blueprint_id);
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
        BlueprintTargetSetEnabled::Inherit => client
            .blueprint_target_view()
            .await
            .map(|current| current.into_inner().enabled)
            .context("failed to fetch current target blueprint")?,
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
    let description = if enabled { "enabled" } else { "disabled" };
    client
        .blueprint_target_set_enabled(
            &nexus_client::types::BlueprintTargetSet {
                target_id: args.blueprint_id,
                enabled,
            },
        )
        .await
        .with_context(|| {
            format!("setting blueprint {} to {description}", args.blueprint_id)
        })?;
    eprintln!("set target blueprint {} to {description}", args.blueprint_id);
    Ok(())
}

async fn cmd_nexus_blueprints_generate_from_collection(
    client: &nexus_client::Client,
    args: &CollectionIdArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let blueprint = client
        .blueprint_generate_from_collection(
            &nexus_client::types::CollectionId {
                collection_id: args.collection_id,
            },
        )
        .await
        .context("creating blueprint from collection id")?;
    eprintln!(
        "created blueprint {} from collection id {}",
        blueprint.id, args.collection_id
    );
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
        revision: i64,
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
    client
        .sled_add(&UninitializedSledId {
            part: args.part.clone(),
            serial: args.serial.clone(),
        })
        .await
        .context("adding sled")?;
    eprintln!("added sled {} ({})", args.serial, args.part);
    Ok(())
}

/// Runs `omdb nexus sleds expunge`
async fn cmd_nexus_sled_expunge(
    client: &nexus_client::Client,
    args: &SledExpungeArgs,
    omdb: &Omdb,
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

    let datastore = args.db_url_opts.connect(omdb, log).await?;
    let opctx = OpContext::for_tests(log.clone(), datastore.clone());
    let opctx = &opctx;

    // First, we need to look up the sled so we know its serial number.
    let (_authz_sled, sled) = LookupPath::new(opctx, &datastore)
        .sled_id(args.sled_id)
        .fetch()
        .await
        .with_context(|| format!("failed to find sled {}", args.sled_id))?;

    // Helper to get confirmation messages from the user.
    let mut line_editor = Reedline::create();
    let mut read_with_prompt = move |message: &str| {
        let prompt = DefaultPrompt::new(
            DefaultPromptSegment::Basic(message.to_string()),
            DefaultPromptSegment::Empty,
        );
        if let Ok(reedline::Signal::Success(input)) =
            line_editor.read_line(&prompt)
        {
            Ok(input)
        } else {
            bail!("expungement aborted")
        }
    };

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
                let confirm = read_with_prompt("y/N")?;
                if confirm != "y" {
                    eprintln!("expungement not confirmed: aborting");
                    return Ok(());
                }
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
    let confirm = read_with_prompt("sled serial number")?;
    if confirm != sled.serial_number() {
        eprintln!("sled serial number not confirmed: aborting");
        return Ok(());
    }

    let old_policy = client
        .sled_expunge(&SledSelector { sled: args.sled_id })
        .await
        .context("expunging sled")?
        .into_inner();
    eprintln!(
        "expunged sled {} (previous policy: {old_policy:?})",
        args.sled_id
    );
    Ok(())
}
