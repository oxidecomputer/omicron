// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db webhook` subcommands

use super::DbFetchOptions;
use super::check_limit;
use super::first_page;
use crate::helpers::const_max_len;
use crate::helpers::datetime_opt_rfc3339_concise;
use crate::helpers::datetime_rfc3339_concise;
use crate::helpers::display_option_blank;

use anyhow::Context;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use clap::Args;
use clap::Subcommand;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::expression::SelectableHelper;
use diesel::query_dsl::QueryDsl;
use nexus_db_model::WebhookDelivery;
use nexus_db_model::WebhookEvent;
use nexus_db_model::WebhookEventClass;
use nexus_db_model::WebhookReceiver;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_db_schema::schema::webhook_delivery::dsl as delivery_dsl;
use nexus_db_schema::schema::webhook_delivery_attempt::dsl as attempt_dsl;
use nexus_db_schema::schema::webhook_event::dsl as event_dsl;
use nexus_types::identity::Resource;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::WebhookEventUuid;
use tabled::Tabled;
use uuid::Uuid;

#[derive(Debug, Args, Clone)]
pub(super) struct WebhookArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand, Clone)]
enum Commands {
    /// Get information on webhook receivers
    #[clap(alias = "rx")]
    Receiver {
        #[command(subcommand)]
        command: RxCommands,
    },
    /// Get information on webhook events
    Event {
        #[command(subcommand)]
        command: EventCommands,
    },
    /// Get information on webhook delivieries
    Delivery {
        #[command(subcommand)]
        command: DeliveryCommands,
    },
}

#[derive(Debug, Subcommand, Clone)]
enum RxCommands {
    /// List webhook receivers
    #[clap(alias = "ls")]
    List(RxListArgs),

    #[clap(alias = "show")]
    Info(RxInfoArgs),
}

#[derive(Debug, Args, Clone)]
struct RxInfoArgs {
    receiver: NameOrId,
}

#[derive(Debug, Args, Clone)]
struct RxListArgs {
    #[clap(long, short = 'a')]
    start_at: Option<Uuid>,
}

#[derive(Debug, Subcommand, Clone)]
enum DeliveryCommands {
    /// List webhook deliveries
    #[clap(alias = "ls")]
    List(DeliveryListArgs),

    /// Show details on a webhook delivery, including its payload and attempt history.
    #[clap(alias = "show")]
    Info(DeliveryInfoArgs),
}

#[derive(Debug, Args, Clone)]
struct DeliveryListArgs {
    /// If present, show only deliveries to this receiver.
    #[clap(long, short, alias = "rx")]
    receiver: Option<NameOrId>,

    /// If present, select only deliveries for the given event.
    #[clap(long, short)]
    event: Option<WebhookEventUuid>,

    /// If present, select only deliveries in the provided state(s)
    #[clap(long = "state", short)]
    states: Vec<db::model::WebhookDeliveryState>,

    /// If present, select only deliveries with the provided trigger(s)
    #[clap(long = "trigger", short)]
    triggers: Vec<db::model::WebhookDeliveryTrigger>,

    /// Include only delivery entries created before this timestamp
    #[clap(long, short)]
    before: Option<DateTime<Utc>>,

    /// Include only delivery entries created after this timestamp
    #[clap(long, short)]
    after: Option<DateTime<Utc>>,
}

#[derive(Debug, Args, Clone)]
struct DeliveryInfoArgs {
    /// The ID of the delivery to show.
    delivery_id: Uuid,
}

#[derive(Debug, Subcommand, Clone)]
enum EventCommands {
    /// List webhook events
    #[clap(alias = "ls")]
    List(EventListArgs),

    /// Show details on a webhook event
    #[clap(alias = "show")]
    Info(EventInfoArgs),
}

#[derive(Debug, Args, Clone)]
struct EventListArgs {
    /// If set, include event JSON payloads in the output.
    ///
    /// Note that this results in very wide output.
    #[clap(long, short)]
    payload: bool,

    /// Include only events created before this timestamp
    #[clap(long, short)]
    before: Option<DateTime<Utc>>,

    /// Include only events created after this timestamp
    #[clap(long, short)]
    after: Option<DateTime<Utc>>,

    /// Include only events fully dispatched before this timestamp
    #[clap(long)]
    dispatched_before: Option<DateTime<Utc>>,

    /// Include only events fully dispatched after this timestamp
    #[clap(long)]
    dispatched_after: Option<DateTime<Utc>>,

    /// If `true`, include only events that have been fully dispatched.
    /// If `false`, include only events that have not been fully dispatched.
    ///
    /// If this argument is not provided, both dispatched and un-dispatched
    /// events are included.
    #[clap(long, short)]
    dispatched: Option<bool>,
}

#[derive(Debug, Args, Clone)]
struct EventInfoArgs {
    /// The ID of the event to show
    event_id: WebhookEventUuid,
}

pub(super) async fn cmd_db_webhook(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &WebhookArgs,
) -> anyhow::Result<()> {
    match &args.command {
        Commands::Receiver { command: RxCommands::List(args) } => {
            cmd_db_webhook_rx_list(opctx, datastore, fetch_opts, args).await
        }
        Commands::Receiver { command: RxCommands::Info(args) } => {
            cmd_db_webhook_rx_info(datastore, fetch_opts, args).await
        }
        Commands::Delivery { command: DeliveryCommands::List(args) } => {
            cmd_db_webhook_delivery_list(datastore, fetch_opts, args).await
        }
        Commands::Delivery { command: DeliveryCommands::Info(args) } => {
            cmd_db_webhook_delivery_info(datastore, fetch_opts, args).await
        }
        Commands::Event { command: EventCommands::Info(args) } => {
            cmd_db_webhook_event_info(datastore, fetch_opts, args).await
        }
        Commands::Event { command: EventCommands::List(args) } => {
            cmd_db_webhook_event_list(datastore, fetch_opts, args).await
        }
    }
}

const ID: &'static str = "ID";
const TIME_CREATED: &'static str = "created at";
const TIME_DELETED: &'static str = "deleted at";
const TIME_MODIFIED: &'static str = "modified at";

async fn cmd_db_webhook_rx_list(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &RxListArgs,
) -> anyhow::Result<()> {
    let ctx = || {
        if let Some(starting_at) = args.start_at {
            format!("listing webhook receivers (starting at {starting_at})")
        } else {
            "listing webhook_receivers".to_string()
        }
    };
    let pagparams = DataPageParams {
        marker: args.start_at.as_ref(),
        ..first_page(fetch_opts.fetch_limit)
    };
    let rxs = datastore
        .webhook_rx_list(opctx, &PaginatedBy::Id(pagparams))
        .await
        .with_context(ctx)?;

    check_limit(&rxs, fetch_opts.fetch_limit, ctx);

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct RxRow {
        id: Uuid,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        created: chrono::DateTime<Utc>,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        modified: chrono::DateTime<Utc>,
        secrets: usize,
        subscriptions: usize,
        name: String,
        endpoint: String,
    }

    let rows = rxs.into_iter().map(
        |db::model::WebhookReceiverConfig { rx, secrets, subscriptions }| {
            RxRow {
                id: rx.id().into_untyped_uuid(),
                name: rx.identity.name.to_string(),
                created: rx.time_created(),
                modified: rx.time_modified(),
                secrets: secrets.len(),
                subscriptions: subscriptions.len(),
                endpoint: rx.endpoint,
            }
        },
    );

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{table}");

    Ok(())
}

async fn cmd_db_webhook_rx_info(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &RxInfoArgs,
) -> anyhow::Result<()> {
    use nexus_db_schema::schema::webhook_rx_event_glob::dsl as glob_dsl;
    use nexus_db_schema::schema::webhook_rx_subscription::dsl as subscription_dsl;
    use nexus_db_schema::schema::webhook_secret::dsl as secret_dsl;

    let conn = datastore.pool_connection_for_tests().await?;
    let rx = lookup_webhook_rx(datastore, &args.receiver)
        .await
        .with_context(|| format!("loading webhook receiver {}", args.receiver))?
        .ok_or_else(|| {
            anyhow::anyhow!("no webhook receiver {} exists", args.receiver)
        })?;

    const NAME: &'static str = "name";
    const DESCRIPTION: &'static str = "description";
    const ENDPOINT: &'static str = "endpoint";
    const GEN: &'static str = "generation";
    const EXACT: &'static str = "exact subscriptions";
    const GLOBS: &'static str = "glob subscriptions";
    const GLOB_REGEX: &'static str = "  regex";
    const GLOB_SCHEMA_VERSION: &'static str = "  schema version";
    const GLOB_CREATED: &'static str = "  created at";
    const GLOB_EXACT: &'static str = "  exact subscriptions";
    const WIDTH: usize = const_max_len(&[
        ID,
        NAME,
        DESCRIPTION,
        TIME_CREATED,
        TIME_DELETED,
        TIME_MODIFIED,
        ENDPOINT,
        GEN,
        EXACT,
        GLOBS,
        GLOB_REGEX,
        GLOB_SCHEMA_VERSION,
        GLOB_CREATED,
        GLOB_EXACT,
    ]);

    let WebhookReceiver {
        identity:
            nexus_db_model::WebhookReceiverIdentity {
                id,
                name,
                description,
                time_created,
                time_modified,
                time_deleted,
            },
        endpoint,
        secret_gen,
        subscription_gen,
    } = rx;

    println!("\n{:=<80}", "== RECEIVER ");
    println!("    {NAME:>WIDTH$}: {name}");
    println!("    {ID:>WIDTH$}: {id}");
    println!("    {DESCRIPTION:>WIDTH$}: {description}");
    println!("    {ENDPOINT:>WIDTH$}: {endpoint}");
    println!();
    println!("    {TIME_CREATED:>WIDTH$}: {time_created}");
    println!("    {TIME_MODIFIED:>WIDTH$}: {time_modified}");
    if let Some(deleted) = time_deleted {
        println!("    {TIME_DELETED:>WIDTH$}: {deleted}");
    }

    println!("\n{:=<80}", "== SECRETS ");
    println!("    {GEN:>WIDTH$}: {}", secret_gen.0);

    let query = secret_dsl::webhook_secret
        .filter(secret_dsl::rx_id.eq(id.into_untyped_uuid()))
        .select(db::model::WebhookSecret::as_select());
    let secrets = if fetch_opts.include_deleted {
        query.load_async(&*conn).await
    } else {
        query
            .filter(secret_dsl::time_deleted.is_null())
            .load_async(&*conn)
            .await
    };

    match secrets {
        Ok(secrets) => {
            #[derive(Tabled)]
            struct SecretRow {
                id: Uuid,

                #[tabled(display_with = "datetime_rfc3339_concise")]
                created: chrono::DateTime<Utc>,

                #[tabled(display_with = "datetime_opt_rfc3339_concise")]
                deleted: Option<chrono::DateTime<Utc>>,
            }
            let rows = secrets.into_iter().map(
                |db::model::WebhookSecret {
                     identity:
                         db::model::WebhookSecretIdentity {
                             id,
                             time_modified: _,
                             time_created,
                         },
                     webhook_receiver_id: _,
                     secret: _,
                     time_deleted,
                 }| SecretRow {
                    id: id.into_untyped_uuid(),
                    created: time_created,
                    deleted: time_deleted,
                },
            );

            let table = tabled::Table::new(rows)
                .with(tabled::settings::Style::empty())
                .with(tabled::settings::Padding::new(0, 1, 0, 0))
                .to_string();
            println!("{table}");
        }
        Err(e) => eprintln!("failed to list secrets: {e}"),
    }

    println!("\n{:=<80}", "== SUBSCRIPTIONS ");
    println!("    {GEN:>WIDTH$}: {}", subscription_gen.0);

    let exact = subscription_dsl::webhook_rx_subscription
        .filter(subscription_dsl::rx_id.eq(id.into_untyped_uuid()))
        .filter(subscription_dsl::glob.is_null())
        .select(subscription_dsl::event_class)
        .load_async::<WebhookEventClass>(&*conn)
        .await;
    match exact {
        Ok(exact) => {
            println!("    {EXACT:>WIDTH$}: {}", exact.len());
            for event_class in exact {
                println!("    - {event_class}");
            }
        }
        Err(e) => {
            eprintln!("failed to list exact subscriptions: {e}");
        }
    }

    let globs = glob_dsl::webhook_rx_event_glob
        .filter(glob_dsl::rx_id.eq(id.into_untyped_uuid()))
        .select(db::model::WebhookRxEventGlob::as_select())
        .load_async::<db::model::WebhookRxEventGlob>(&*conn)
        .await;
    match globs {
        Ok(globs) => {
            println!("    {GLOBS:>WIDTH$}: {}", globs.len());
            for glob in globs {
                let db::model::WebhookRxEventGlob {
                    rx_id: _,
                    glob: db::model::WebhookGlob { glob, regex },
                    time_created,
                    schema_version,
                } = glob;
                println!("    - {glob}");
                println!("    {GLOB_CREATED:>WIDTH$}: {time_created}");
                if let Some(v) = schema_version {
                    println!("    {GLOB_SCHEMA_VERSION:>WIDTH$}: {v}")
                } else {
                    println!(
                        "(i) {GLOB_SCHEMA_VERSION:>WIDTH$}: <not yet processed>",
                    )
                }

                println!("    {GLOB_REGEX:>WIDTH$}: {regex}");
                let exact = subscription_dsl::webhook_rx_subscription
                    .filter(subscription_dsl::rx_id.eq(id.into_untyped_uuid()))
                    .filter(subscription_dsl::glob.eq(glob))
                    .select(subscription_dsl::event_class)
                    .load_async::<WebhookEventClass>(&*conn)
                    .await;
                match exact {
                    Ok(exact) => {
                        println!("    {GLOB_EXACT:>WIDTH$}: {}", exact.len());
                        for event_class in exact {
                            println!("      - {event_class}")
                        }
                    }
                    Err(e) => eprintln!(
                        "failed to list exact subscriptions for glob: {e}"
                    ),
                }
            }
        }
        Err(e) => {
            eprintln!("failed to list glob subscriptions: {e}");
        }
    }

    Ok(())
}

async fn cmd_db_webhook_delivery_list(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &DeliveryListArgs,
) -> anyhow::Result<()> {
    let DeliveryListArgs { before, after, receiver, states, triggers, event } =
        args;
    let conn = datastore.pool_connection_for_tests().await?;
    let mut query = delivery_dsl::webhook_delivery
        .limit(fetch_opts.fetch_limit.get().into())
        .order_by(delivery_dsl::time_created.desc())
        .into_boxed();

    if let (Some(before), Some(after)) = (before, after) {
        anyhow::ensure!(
            after < before,
            "if both `--after` and `--before` are included, after must be
             earlier than before"
        );
    }

    if let Some(before) = *before {
        query = query.filter(delivery_dsl::time_created.lt(before));
    }

    if let Some(after) = *after {
        query = query.filter(delivery_dsl::time_created.gt(after));
    }

    if let Some(ref receiver) = receiver {
        let rx =
            lookup_webhook_rx(datastore, receiver).await?.ok_or_else(|| {
                anyhow::anyhow!("no webhook receiver {receiver} found")
            })?;
        query = query.filter(delivery_dsl::rx_id.eq(rx.identity.id));
    }

    if !states.is_empty() {
        query = query.filter(delivery_dsl::state.eq_any(states.clone()));
    }

    if !triggers.is_empty() {
        query =
            query.filter(delivery_dsl::triggered_by.eq_any(triggers.clone()));
    }

    if let Some(id) = event {
        query = query.filter(delivery_dsl::event_id.eq(id.into_untyped_uuid()));
    }

    let ctx = || "listing webhook deliveries";

    let deliveries = query
        .select(WebhookDelivery::as_select())
        .load_async(&*conn)
        .await
        .with_context(ctx)?;

    check_limit(&deliveries, fetch_opts.fetch_limit, ctx);

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct WithEventId<T: Tabled> {
        #[tabled(inline)]
        inner: T,
        event_id: Uuid,
    }

    impl<'d, T> From<&'d WebhookDelivery> for WithEventId<T>
    where
        T: From<&'d WebhookDelivery> + Tabled,
    {
        fn from(d: &'d WebhookDelivery) -> Self {
            Self { event_id: d.event_id.into_untyped_uuid(), inner: T::from(d) }
        }
    }

    let mut table = match (args.receiver.as_ref(), args.event) {
        // Filtered by both receiver and event, so don't display either.
        (Some(_), Some(_)) => {
            tabled::Table::new(deliveries.iter().map(DeliveryRow::from))
        }
        // Filtered by neither receiver nor event, so include both.
        (None, None) => tabled::Table::new(
            deliveries
                .iter()
                .map(DeliveryRowWithRxId::<WithEventId<DeliveryRow>>::from),
        ),
        // Filtered by receiver ID only
        (Some(_), None) => tabled::Table::new(
            deliveries.iter().map(WithEventId::<DeliveryRow>::from),
        ),
        // Filtered by event ID only
        (None, Some(_)) => tabled::Table::new(
            deliveries.iter().map(DeliveryRowWithRxId::<DeliveryRow>::from),
        ),
    };
    table
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0));

    println!("{table}");
    Ok(())
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct DeliveryRow {
    id: Uuid,
    trigger: nexus_db_model::WebhookDeliveryTrigger,
    state: nexus_db_model::WebhookDeliveryState,
    attempts: u8,
    #[tabled(display_with = "datetime_rfc3339_concise")]
    time_created: DateTime<Utc>,
    #[tabled(display_with = "datetime_opt_rfc3339_concise")]
    time_completed: Option<DateTime<Utc>>,
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct DeliveryRowWithRxId<T: Tabled> {
    #[tabled(inline)]
    inner: T,
    receiver_id: Uuid,
}

impl From<&'_ WebhookDelivery> for DeliveryRow {
    fn from(d: &WebhookDelivery) -> Self {
        let WebhookDelivery {
            id,
            // event and receiver UUIDs are toggled on and off based on
            // whether or not we are filtering by receiver and event, so
            // ignore them here.
            event_id: _,
            rx_id: _,
            attempts,
            state,
            time_created,
            time_completed,
            // ignore these as they are used for runtime coordination and
            // aren't very useful for showing delivery history
            deliverator_id: _,
            time_leased: _,
            triggered_by,
        } = d;
        Self {
            id: id.into_untyped_uuid(),
            trigger: *triggered_by,
            state: *state,
            attempts: attempts.0,
            time_created: *time_created,
            time_completed: *time_completed,
        }
    }
}

impl<'d, T> From<&'d WebhookDelivery> for DeliveryRowWithRxId<T>
where
    T: From<&'d WebhookDelivery> + Tabled,
{
    fn from(d: &'d WebhookDelivery) -> Self {
        Self { receiver_id: d.rx_id.into_untyped_uuid(), inner: T::from(d) }
    }
}

/// Helper function to look up a webhook receiver with the given name or ID
async fn lookup_webhook_rx(
    datastore: &DataStore,
    name_or_id: &NameOrId,
) -> anyhow::Result<Option<WebhookReceiver>> {
    use nexus_db_schema::schema::webhook_receiver::dsl;

    let conn = datastore.pool_connection_for_tests().await?;
    match name_or_id {
        NameOrId::Id(id) => {
            dsl::webhook_receiver
                .filter(dsl::id.eq(*id))
                .limit(1)
                .select(WebhookReceiver::as_select())
                .get_result_async(&*conn)
                .await
        }
        NameOrId::Name(ref name) => {
            dsl::webhook_receiver
                .filter(dsl::name.eq(name.to_string()))
                .limit(1)
                .select(WebhookReceiver::as_select())
                .get_result_async(&*conn)
                .await
        }
    }
    .optional()
    .with_context(|| format!("loading webhook_receiver {name_or_id}"))
}

async fn cmd_db_webhook_delivery_info(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &DeliveryInfoArgs,
) -> anyhow::Result<()> {
    use db::model::WebhookDeliveryAttempt;

    let DeliveryInfoArgs { delivery_id } = args;
    let conn = datastore.pool_connection_for_tests().await?;
    let delivery = delivery_dsl::webhook_delivery
        .filter(delivery_dsl::id.eq(*delivery_id))
        .limit(1)
        .select(WebhookDelivery::as_select())
        .get_result_async(&*conn)
        .await
        .optional()
        .with_context(|| format!("loading webhook delivery {delivery_id}"))?
        .ok_or_else(|| {
            anyhow::anyhow!("no webhook delivery {delivery_id} exists")
        })?;

    const EVENT_ID: &'static str = "event ID";
    const RECEIVER_ID: &'static str = "receiver ID";
    const STATE: &'static str = "state";
    const TRIGGER: &'static str = "triggered by";
    const ATTEMPTS: &'static str = "attempts";
    const TIME_COMPLETED: &'static str = "completed at";

    const DELIVERATOR_ID: &'static str = "by Nexus";
    const TIME_LEASED: &'static str = "leased at";

    const WIDTH: usize = const_max_len(&[
        ID,
        EVENT_ID,
        RECEIVER_ID,
        TRIGGER,
        STATE,
        TIME_CREATED,
        TIME_COMPLETED,
        DELIVERATOR_ID,
        TIME_LEASED,
        ATTEMPTS,
    ]);

    let WebhookDelivery {
        id,
        event_id,
        rx_id,
        triggered_by,
        attempts,
        time_created,
        time_completed,
        state,
        deliverator_id,
        time_leased,
    } = delivery;
    println!("\n{:=<80}", "== DELIVERY ");
    println!("    {ID:>WIDTH$}: {id}");
    println!("    {EVENT_ID:>WIDTH$}: {event_id}");
    println!("    {RECEIVER_ID:>WIDTH$}: {rx_id}");
    println!("    {STATE:>WIDTH$}: {state}");
    println!("    {TRIGGER:>WIDTH$}: {triggered_by}");
    println!("    {TIME_CREATED:>WIDTH$}: {time_created}");
    println!("    {ATTEMPTS}: {}", attempts.0);

    if let Some(completed) = time_completed {
        println!("\n{:=<80}", "== DELIVERY COMPLETED ");
        println!("    {TIME_COMPLETED:>WIDTH$}: {completed}");
        if let Some(leased) = time_leased {
            println!("    {TIME_LEASED:>WIDTH$}: {leased}");
        } else {
            println!(
                "/!\\ WEIRD: delivery is completed but has no start timestamp?"
            );
        }
        if let Some(nexus) = deliverator_id {
            println!("    {DELIVERATOR_ID:>WIDTH$}: {nexus}");
        } else {
            println!("/!\\ WEIRD: delivery is completed but has no Nexus ID?");
        }
    } else if let Some(leased) = time_leased {
        println!("\n{:=<80}", "== DELIVERY IN PROGRESS ");
        println!("    {TIME_LEASED:>WIDTH$}: {leased}");

        if let Some(nexus) = deliverator_id {
            println!("    {DELIVERATOR_ID:>WIDTH$}: {nexus}");
        } else {
            println!(
                "/!\\ WEIRD: delivery is in progress but has no Nexus ID?"
            );
        }
    } else if let Some(deliverator) = deliverator_id {
        println!(
            "/!\\ WEIRD: delivery is not completed or in progress but has \
             Nexus ID {deliverator:?}"
        );
    }

    // Okay, now go get attempts for this delivery.
    let ctx = || format!("listing delivery attempts for {delivery_id}");
    let attempts = attempt_dsl::webhook_delivery_attempt
        .filter(attempt_dsl::delivery_id.eq(*delivery_id))
        .order_by(attempt_dsl::attempt.desc())
        .limit(fetch_opts.fetch_limit.get().into())
        .select(WebhookDeliveryAttempt::as_select())
        .load_async(&*conn)
        .await
        .with_context(ctx)?;

    check_limit(&attempts, fetch_opts.fetch_limit, ctx);

    if !attempts.is_empty() {
        println!("\n{:=<80}", "== DELIVERY ATTEMPT HISTORY ");

        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct DeliveryAttemptRow {
            id: Uuid,
            #[tabled(rename = "#")]
            attempt: u8,
            #[tabled(display_with = "datetime_rfc3339_concise")]
            time_created: DateTime<Utc>,
            nexus_id: Uuid,
            result: db::model::WebhookDeliveryAttemptResult,
            #[tabled(display_with = "display_option_blank")]
            status: Option<u16>,
            #[tabled(display_with = "display_option_blank")]
            duration: Option<chrono::TimeDelta>,
        }

        let rows = attempts.into_iter().map(
            |WebhookDeliveryAttempt {
                 id,
                 delivery_id: _,
                 rx_id: _,
                 attempt,
                 result,
                 response_status,
                 response_duration,
                 time_created,
                 deliverator_id,
             }| DeliveryAttemptRow {
                id: id.into_untyped_uuid(),
                attempt: attempt.0,
                time_created,
                nexus_id: deliverator_id.into_untyped_uuid(),
                result,
                status: response_status.map(|u| u.into()),
                duration: response_duration,
            },
        );
        let mut table = tabled::Table::new(rows);
        table
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0));
        println!("{table}");
    }

    Ok(())
}

async fn cmd_db_webhook_event_list(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &EventListArgs,
) -> anyhow::Result<()> {
    let EventListArgs {
        payload,
        before,
        after,
        dispatched_before,
        dispatched_after,
        dispatched,
    } = args;

    if let (Some(before), Some(after)) = (before, after) {
        anyhow::ensure!(
            after < before,
            "if both `--after` and `--before` are included, after must be
             earlier than before"
        );
    }

    if let (Some(before), Some(after)) = (dispatched_before, dispatched_after) {
        anyhow::ensure!(
            after < before,
            "if both `--dispatched-after` and `--dispatched-before` are
             included, after must be earlier than before"
        );
    }

    let conn = datastore.pool_connection_for_tests().await?;

    let mut query = event_dsl::webhook_event
        .limit(fetch_opts.fetch_limit.get().into())
        .order_by(event_dsl::time_created.asc())
        .select(WebhookEvent::as_select())
        .into_boxed();

    if let Some(before) = before {
        query = query.filter(event_dsl::time_created.lt(*before));
    }

    if let Some(after) = after {
        query = query.filter(event_dsl::time_created.gt(*after));
    }

    if let Some(before) = dispatched_before {
        query = query.filter(event_dsl::time_dispatched.lt(*before));
    }

    if let Some(after) = dispatched_after {
        query = query.filter(event_dsl::time_dispatched.gt(*after));
    }

    if let Some(dispatched) = dispatched {
        if *dispatched {
            query = query.filter(event_dsl::time_dispatched.is_not_null());
        } else {
            query = query.filter(event_dsl::time_dispatched.is_null());
        }
    }

    let ctx = || "loading webhook events";
    let events = query.load_async(&*conn).await.with_context(ctx)?;

    check_limit(&events, fetch_opts.fetch_limit, ctx);

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct EventRow {
        id: Uuid,
        class: WebhookEventClass,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        time_created: DateTime<Utc>,
        #[tabled(display_with = "datetime_opt_rfc3339_concise")]
        time_dispatched: Option<DateTime<Utc>>,
        dispatched: i64,
    }

    impl From<&'_ WebhookEvent> for EventRow {
        fn from(event: &'_ WebhookEvent) -> Self {
            Self {
                id: event.identity.id.into_untyped_uuid(),
                class: event.event_class,
                time_created: event.identity.time_created,
                time_dispatched: event.time_dispatched,
                dispatched: event.num_dispatched,
            }
        }
    }

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct EventRowWithPayload {
        #[tabled(inline)]
        row: EventRow,
        payload: String,
    }

    let mut table = if *payload {
        let rows = events.iter().map(|event| {
            let payload = match serde_json::to_string(&event.event) {
                Ok(payload) => payload,
                Err(e) => {
                    eprintln!(
                        "/!\\ failed to serialize payload for {:?}: {e}",
                        event.identity.id
                    );
                    "<error>".to_string()
                }
            };
            EventRowWithPayload { row: event.into(), payload }
        });
        tabled::Table::new(rows)
    } else {
        let rows = events.iter().map(EventRow::from);
        tabled::Table::new(rows)
    };
    table
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0));
    println!("{table}");

    Ok(())
}

async fn cmd_db_webhook_event_info(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &EventInfoArgs,
) -> anyhow::Result<()> {
    let EventInfoArgs { event_id } = args;
    let conn = datastore.pool_connection_for_tests().await?;

    let event = event_dsl::webhook_event
        .filter(event_dsl::id.eq(event_id.into_untyped_uuid()))
        .select(WebhookEvent::as_select())
        .limit(1)
        .get_result_async(&*conn)
        .await
        .optional()
        .with_context(|| format!("loading webhook event {event_id}"))?
        .ok_or_else(|| anyhow::anyhow!("no webhook event {event_id} exists"))?;

    let WebhookEvent {
        identity:
            db::model::WebhookEventIdentity { id, time_created, time_modified },
        time_dispatched,
        event_class,
        event,
        num_dispatched,
    } = event;

    const CLASS: &str = "class";
    const TIME_DISPATCHED: &str = "fully dispatched at";
    const NUM_DISPATCHED: &str = "deliveries dispatched";

    const WIDTH: usize = const_max_len(&[
        ID,
        TIME_CREATED,
        TIME_MODIFIED,
        TIME_DISPATCHED,
        NUM_DISPATCHED,
        CLASS,
    ]);

    println!("\n{:=<80}", "== EVENT ");
    println!("    {ID:>WIDTH$}: {id:?}");
    println!("    {CLASS:>WIDTH$}: {event_class}");
    println!("    {TIME_CREATED:>WIDTH$}: {time_created}");
    println!("    {TIME_MODIFIED:>WIDTH$}: {time_modified}");
    println!();
    println!("    {NUM_DISPATCHED:>WIDTH$}: {num_dispatched}");
    if let Some(t) = time_dispatched {
        println!("    {TIME_DISPATCHED:>WIDTH$}: {t}")
    }

    println!("\n{:=<80}", "== EVENT PAYLOAD ");
    serde_json::to_writer_pretty(std::io::stdout(), &event).with_context(
        || format!("failed to serialize event payload: {event:?}"),
    )?;

    let ctx = || format!("listing deliveries for event {event_id:?}");
    let deliveries = delivery_dsl::webhook_delivery
        .limit(fetch_opts.fetch_limit.get().into())
        .order_by(delivery_dsl::time_created.desc())
        .select(WebhookDelivery::as_select())
        .load_async(&*conn)
        .await
        .with_context(ctx)?;

    check_limit(&deliveries, fetch_opts.fetch_limit, ctx);

    if !deliveries.is_empty() {
        println!("\n{:=<80}", "== DELIVERIES ");
        let mut table = tabled::Table::new(
            deliveries.iter().map(DeliveryRowWithRxId::<DeliveryRow>::from),
        );
        table
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0));
        println!("{table}")
    } else if num_dispatched > 0 {
        println!(
            "/!\\ WEIRD: event claims to have {num_dispatched} deliveries \
             dispatched, but no delivery records were found"
        )
    }

    Ok(())
}
