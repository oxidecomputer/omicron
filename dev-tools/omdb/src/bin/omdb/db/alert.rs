// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db alert` subcommands

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
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use nexus_db_lookup::DbConnection;
use nexus_db_lookup::LookupPath;
use nexus_db_model::Alert;
use nexus_db_model::AlertClass;
use nexus_db_model::AlertDeliveryState;
use nexus_db_model::AlertDeliveryTrigger;
use nexus_db_model::AlertReceiver;
use nexus_db_model::WebhookDelivery;
use nexus_db_model::fm::RendezvousAlertCreated;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::WebhookDeliveryFilters;
use nexus_db_queries::db::pagination::paginated;
use nexus_db_schema::schema::alert_glob::dsl as glob_dsl;
use nexus_db_schema::schema::alert_subscription::dsl as subscription_dsl;
use nexus_db_schema::schema::webhook_delivery::dsl as delivery_dsl;
use nexus_db_schema::schema::webhook_delivery_attempt::dsl as attempt_dsl;
use nexus_types::external_api::alert as types;
use nexus_types::identity::Resource;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Generation;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::AlertReceiverUuid;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::GenericUuid;
use std::sync::LazyLock;
use tabled::Tabled;
use uuid::Uuid;

#[derive(Debug, Args, Clone)]
pub(super) struct AlertArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand, Clone)]
enum Commands {
    /// List alerts
    #[clap(alias = "ls")]
    List(AlertListArgs),

    /// Show details on an alert
    #[clap(alias = "show")]
    Info(AlertInfoArgs),

    /// Commands relating to webhook alerts.
    Webhook(WebhookArgs),
}

#[derive(Debug, Args, Clone)]
struct AlertListArgs {
    /// If set, include alert JSON payloads in the output.
    ///
    /// Note that this results in very wide output.
    #[clap(long, short)]
    payload: bool,

    /// Include only alerts created before this timestamp
    #[clap(long, short)]
    before: Option<DateTime<Utc>>,

    /// Include only alerts created after this timestamp
    #[clap(long, short)]
    after: Option<DateTime<Utc>>,

    /// Include only alerts fully dispatched before this timestamp
    #[clap(long)]
    dispatched_before: Option<DateTime<Utc>>,

    /// Include only alerts fully dispatched after this timestamp
    #[clap(long)]
    dispatched_after: Option<DateTime<Utc>>,

    /// Include only alerts requested by the fault management case(s) with the
    /// specified UUIDs.
    ///
    /// If multiple case IDs are provided, alerts requested by any of those
    /// cases will be included in the output.
    ///
    /// Note that not all alerts are requested by fault management cases.
    #[clap(long, num_args(1..))]
    cases: Vec<Uuid>,

    /// Include only alerts with the specified alert classes.
    ///
    /// If multiple values are provided, alerts with any of those classes will
    /// be included in the output.
    #[clap(
        long,
        num_args(1..),
        value_enum,
    )]
    classes: Vec<ClapAlertClass>,

    /// If `true`, include only alerts that have been fully dispatched.
    /// If `false`, include only alerts that have not been fully dispatched.
    ///
    /// If this argument is not provided, both dispatched and un-dispatched
    /// events are included.
    #[clap(long, short)]
    dispatched: Option<bool>,
}

// A workaround for not being able to derive `clap::ValueEnum` on
// `nexus_types::alert::AlertClass`, due to it living in the `nexus_types`
// crate(which doesn't really want a `clap` dependency).
#[derive(Debug, Copy, Clone)]
struct ClapAlertClass(nexus_types::alert::AlertClass);

impl clap::ValueEnum for ClapAlertClass {
    fn value_variants<'a>() -> &'a [Self] {
        static VALUE_VARIANTS: LazyLock<Vec<ClapAlertClass>> =
            LazyLock::new(|| {
                nexus_types::alert::AlertClass::ALL_CLASSES
                    .iter()
                    .copied()
                    .map(ClapAlertClass)
                    .collect()
            });
        &VALUE_VARIANTS
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(
            clap::builder::PossibleValue::new(self.0.as_str())
                .help(self.0.description()),
        )
    }
}

impl From<&'_ ClapAlertClass> for AlertClass {
    fn from(&ClapAlertClass(class): &ClapAlertClass) -> AlertClass {
        class.into()
    }
}

#[derive(Debug, Args, Clone)]
struct AlertInfoArgs {
    /// The ID of the alert to show
    id: AlertUuid,
}

#[derive(Debug, Args, Clone)]
struct WebhookArgs {
    #[clap(subcommand)]
    command: WebhookCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum WebhookCommands {
    /// Get information on webhook alert receivers
    #[clap(alias = "rx")]
    Receiver(WebhookRxArgs),

    /// Get information on webhook alert deliveries
    Delivery(WebhookDeliveryArgs),
}

#[derive(Debug, Args, Clone)]
struct WebhookRxArgs {
    #[clap(subcommand)]
    command: WebhookRxCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum WebhookRxCommands {
    /// List webhook alert receivers
    #[clap(alias = "ls")]
    List(WebhookRxListArgs),

    /// Get details on a webhook alert receiver
    #[clap(alias = "show")]
    Info(WebhookRxInfoArgs),
}

#[derive(Debug, Args, Clone)]
struct WebhookRxInfoArgs {
    receiver: NameOrId,
}

#[derive(Debug, Args, Clone)]
struct WebhookRxListArgs {
    #[clap(long, short = 'a')]
    start_at: Option<Uuid>,
}

#[derive(Debug, Args, Clone)]
struct WebhookDeliveryArgs {
    #[clap(subcommand)]
    command: WebhookDeliveryCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum WebhookDeliveryCommands {
    /// List webhook deliveries
    #[clap(alias = "ls")]
    List(WebhookDeliveryListArgs),

    /// Show details on a webhook delivery, including its payload and attempt history.
    #[clap(alias = "show")]
    Info(WebhookDeliveryInfoArgs),
}

#[derive(Debug, Args, Clone)]
struct WebhookDeliveryListArgs {
    /// If present, show only deliveries to this receiver.
    #[clap(long, short, alias = "rx")]
    receiver: Option<NameOrId>,

    /// If present, select only deliveries for the given alert.
    #[clap(
        long, short = 'A',
        alias = "event", // backwards compatibility
    )]
    alert: Option<AlertUuid>,

    /// If present, select only deliveries in the provided state(s)
    ///
    /// If multiple values are provided, deliveries in any of those states will
    /// be included in the output.
    #[clap(long = "state", short, num_args(1..), value_enum)]
    states: Vec<ClapAlertDeliveryState>,

    /// If present, select only deliveries with the provided trigger(s)
    ///
    /// If multiple values are provided, deliveries with any of those triggers
    /// will be included in the output.
    #[clap(long = "trigger", short, num_args(1..), value_enum)]
    triggers: Vec<ClapAlertDeliveryTrigger>,

    /// Include only delivery entries created before this timestamp
    #[clap(long, short)]
    before: Option<DateTime<Utc>>,

    /// Include only delivery entries created after this timestamp
    #[clap(long, short)]
    after: Option<DateTime<Utc>>,
}

// Wrapper for implementing `clap::ValueEnum` for `nexus_types`'s version of
// `AlertDeliveryState`.
#[derive(Debug, Copy, Clone)]
struct ClapAlertDeliveryState(types::AlertDeliveryState);

impl clap::ValueEnum for ClapAlertDeliveryState {
    fn value_variants<'a>() -> &'a [Self] {
        static VALUE_VARIANTS: LazyLock<Vec<ClapAlertDeliveryState>> =
            LazyLock::new(|| {
                types::AlertDeliveryState::ALL
                    .iter()
                    .copied()
                    .map(ClapAlertDeliveryState)
                    .collect()
            });
        &VALUE_VARIANTS
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(clap::builder::PossibleValue::new(self.0.as_str()))
    }

    fn from_str(input: &str, _ignore_case: bool) -> Result<Self, String> {
        input
            .parse::<types::AlertDeliveryState>()
            .map(ClapAlertDeliveryState)
            .map_err(|e| e.to_string())
    }
}

impl From<&'_ ClapAlertDeliveryState> for AlertDeliveryState {
    fn from(&ClapAlertDeliveryState(state): &ClapAlertDeliveryState) -> Self {
        state.into()
    }
}

#[derive(Debug, Copy, Clone)]
struct ClapAlertDeliveryTrigger(types::AlertDeliveryTrigger);

impl clap::ValueEnum for ClapAlertDeliveryTrigger {
    fn value_variants<'a>() -> &'a [Self] {
        static VALUE_VARIANTS: LazyLock<Vec<ClapAlertDeliveryTrigger>> =
            LazyLock::new(|| {
                types::AlertDeliveryTrigger::ALL
                    .iter()
                    .copied()
                    .map(ClapAlertDeliveryTrigger)
                    .collect()
            });
        &VALUE_VARIANTS
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(clap::builder::PossibleValue::new(self.0.as_str()))
    }

    fn from_str(input: &str, _ignore_case: bool) -> Result<Self, String> {
        input
            .parse::<types::AlertDeliveryTrigger>()
            .map(ClapAlertDeliveryTrigger)
            .map_err(|e| e.to_string())
    }
}

impl From<&'_ ClapAlertDeliveryTrigger> for AlertDeliveryTrigger {
    fn from(
        &ClapAlertDeliveryTrigger(trigger): &ClapAlertDeliveryTrigger,
    ) -> Self {
        trigger.into()
    }
}

#[derive(Debug, Args, Clone)]
struct WebhookDeliveryInfoArgs {
    /// The ID of the delivery to show.
    delivery_id: Uuid,
}

pub(super) async fn cmd_db_alert(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &AlertArgs,
) -> anyhow::Result<()> {
    match &args.command {
        Commands::Info(args) => {
            cmd_db_alert_info(opctx, datastore, fetch_opts, args).await
        }
        Commands::List(args) => {
            cmd_db_alert_list(opctx, datastore, fetch_opts, args).await
        }
        Commands::Webhook(args) => {
            cmd_db_webhook(opctx, datastore, fetch_opts, args).await
        }
    }
}

async fn cmd_db_webhook(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &WebhookArgs,
) -> anyhow::Result<()> {
    match &args.command {
        WebhookCommands::Receiver(WebhookRxArgs {
            command: WebhookRxCommands::Info(args),
        }) => cmd_db_webhook_rx_info(opctx, datastore, fetch_opts, args).await,
        WebhookCommands::Receiver(WebhookRxArgs {
            command: WebhookRxCommands::List(args),
        }) => cmd_db_webhook_rx_list(opctx, datastore, fetch_opts, args).await,
        WebhookCommands::Delivery(WebhookDeliveryArgs {
            command: WebhookDeliveryCommands::List(args),
        }) => {
            cmd_db_webhook_delivery_list(opctx, datastore, fetch_opts, args)
                .await
        }
        WebhookCommands::Delivery(WebhookDeliveryArgs {
            command: WebhookDeliveryCommands::Info(args),
        }) => cmd_db_webhook_delivery_info(datastore, fetch_opts, args).await,
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
    args: &WebhookRxListArgs,
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
        .alert_rx_list(opctx, &PaginatedBy::Id(pagparams))
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
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &WebhookRxInfoArgs,
) -> anyhow::Result<()> {
    use nexus_db_schema::schema::webhook_secret::dsl as secret_dsl;

    let conn = datastore.pool_connection_for_tests().await?;
    let (authz_rx, rx) =
        lookup_webhook_rx(opctx, datastore, &args.receiver).await?;

    const NAME: &'static str = "name";
    const DESCRIPTION: &'static str = "description";
    const ENDPOINT: &'static str = "endpoint";
    const WIDTH: usize = const_max_len(&[
        ID,
        NAME,
        DESCRIPTION,
        TIME_CREATED,
        TIME_DELETED,
        TIME_MODIFIED,
        ENDPOINT,
    ]);

    let AlertReceiver {
        identity:
            nexus_db_model::AlertReceiverIdentity {
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
    println!("generation: {}", secret_gen.0);

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
                     alert_receiver_id: _,
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
    println!("generation: {}", subscription_gen.0);
    println!();
    display_rx_subscriptions(&authz_rx, fetch_opts, &conn).await?;

    Ok(())
}

async fn cmd_db_webhook_delivery_list(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &WebhookDeliveryListArgs,
) -> anyhow::Result<()> {
    let WebhookDeliveryListArgs {
        before,
        after,
        receiver,
        states,
        triggers,
        alert,
    } = args;
    let filters = {
        let mut filters = WebhookDeliveryFilters::new();
        if let &Some(before) = before {
            filters = filters.before(before)?;
        }
        if let &Some(after) = after {
            filters = filters.after(after)?;
        }
        if let Some(receiver) = receiver {
            // Resolve (and authorize access to) the receiver before filtering
            // deliveries by it; `for_receiver` requires an `authz` resource.
            let lookup = LookupPath::new(opctx, datastore);
            let rx_lookup = match receiver {
                NameOrId::Id(id) => lookup.alert_receiver_id(
                    AlertReceiverUuid::from_untyped_uuid(*id),
                ),
                NameOrId::Name(name) => {
                    lookup.alert_receiver_name_owned(name.clone().into())
                }
            };
            let (authz_rx,) =
                rx_lookup.lookup_for(authz::Action::Read).await.with_context(
                    || format!("looking up alert receiver {receiver} failed"),
                )?;
            filters = filters.for_receiver(&authz_rx);
        }
        if let &Some(alert) = alert {
            let (authz_alert,) = LookupPath::new(opctx, datastore)
                .alert_id(alert)
                .lookup_for(authz::Action::Read)
                .await
                .with_context(|| format!("looking up alert {alert} failed"))?;
            filters = filters.for_alert(&authz_alert);
        }
        if !states.is_empty() {
            filters = filters
                .with_states(states.iter().map(AlertDeliveryState::from));
        }
        if !triggers.is_empty() {
            filters = filters
                .with_triggers(triggers.iter().map(AlertDeliveryTrigger::from));
        }
        filters
    };

    let ctx = || "listing webhook deliveries";

    let pagparams = DataPageParams {
        // Descending order shows the most recent delivery first
        direction: dropshot::PaginationOrder::Descending,
        ..first_page(fetch_opts.fetch_limit)
    };

    let deliveries = datastore
        .webhook_delivery_list(opctx, &filters, &pagparams)
        .await
        .with_context(ctx)?;

    check_limit(&deliveries, fetch_opts.fetch_limit, ctx);

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct WithEventId<T: Tabled> {
        #[tabled(inline)]
        inner: T,
        alert_id: Uuid,
    }

    impl<'d, T> From<&'d WebhookDelivery> for WithEventId<T>
    where
        T: From<&'d WebhookDelivery> + Tabled,
    {
        fn from(d: &'d WebhookDelivery) -> Self {
            Self { alert_id: d.alert_id.into_untyped_uuid(), inner: T::from(d) }
        }
    }

    let mut table = match (args.receiver.as_ref(), args.alert) {
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
    trigger: nexus_db_model::AlertDeliveryTrigger,
    state: nexus_db_model::AlertDeliveryState,
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
            // alert and receiver UUIDs are toggled on and off based on
            // whether or not we are filtering by receiver and alert, so
            // ignore them here.
            alert_id: _,
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
    opctx: &OpContext,
    datastore: &DataStore,
    name_or_id: &NameOrId,
) -> anyhow::Result<(authz::AlertReceiver, AlertReceiver)> {
    let lookup = LookupPath::new(opctx, datastore);
    let rx_lookup = match name_or_id {
        NameOrId::Id(id) => {
            lookup.alert_receiver_id(AlertReceiverUuid::from_untyped_uuid(*id))
        }
        NameOrId::Name(name) => {
            lookup.alert_receiver_name_owned(name.clone().into())
        }
    };
    rx_lookup
        .fetch()
        .await
        .with_context(|| format!("loading webhook receiver {name_or_id}"))
}

async fn cmd_db_webhook_delivery_info(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &WebhookDeliveryInfoArgs,
) -> anyhow::Result<()> {
    use db::model::WebhookDeliveryAttempt;

    let WebhookDeliveryInfoArgs { delivery_id } = args;
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
        alert_id,
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
    println!("    {EVENT_ID:>WIDTH$}: {alert_id}");
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

async fn cmd_db_alert_list(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &AlertListArgs,
) -> anyhow::Result<()> {
    let AlertListArgs {
        payload,
        before,
        after,
        cases,
        dispatched_before,
        dispatched_after,
        dispatched,
        classes,
    } = args;

    let filters = {
        let mut filters = nexus_db_queries::db::datastore::AlertFilters::new();
        if let &Some(before) = before {
            filters = filters.before(before)?;
        }
        if let &Some(after) = after {
            filters = filters.after(after)?;
        }
        if let &Some(dispatched_before) = dispatched_before {
            filters = filters.dispatched_before(dispatched_before)?;
        }
        if let &Some(dispatched_after) = dispatched_after {
            filters = filters.dispatched_after(dispatched_after)?;
        }
        if let &Some(dispatched) = dispatched {
            filters = filters.dispatched(dispatched)?;
        }
        if !cases.is_empty() {
            filters = filters.for_fm_cases(
                cases.iter().copied().map(CaseUuid::from_untyped_uuid),
            );
        }
        if !classes.is_empty() {
            filters = filters.with_classes(classes);
        }
        filters
    };

    let pagparams = DataPageParams {
        // Descending order shows the newest alerts first
        direction: dropshot::PaginationOrder::Descending,
        ..first_page(fetch_opts.fetch_limit)
    };

    let ctx = || "loading alerts";
    let alerts: Vec<(Alert, Option<RendezvousAlertCreated>)> = datastore
        .alert_list_matching(opctx, &filters, &pagparams)
        .await
        .with_context(ctx)?;
    check_limit(&alerts, fetch_opts.fetch_limit, ctx);

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct AlertRow {
        id: Uuid,
        class: AlertClass,
        #[tabled(rename = "V")]
        version: u32,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        time_created: DateTime<Utc>,
        #[tabled(display_with = "datetime_opt_rfc3339_concise")]
        time_dispatched: Option<DateTime<Utc>>,
        dispatched: i64,
        #[tabled(display_with = "display_option_blank")]
        fm_case_id: Option<Uuid>,
        #[tabled(display_with = "display_option_blank")]
        fm_gen: Option<Generation>,
    }

    let make_row =
        |alert: &Alert, marker: &Option<RendezvousAlertCreated>| AlertRow {
            id: alert.identity.id.into_untyped_uuid(),
            class: alert.class,
            version: u32::from(alert.version),
            time_created: alert.identity.time_created,
            time_dispatched: alert.time_dispatched,
            dispatched: alert.num_dispatched,
            fm_case_id: alert.case_id.map(GenericUuid::into_untyped_uuid),
            fm_gen: marker
                .as_ref()
                .map(|marker| marker.created_at_generation.0),
        };

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct AlertRowWithPayload {
        #[tabled(inline)]
        row: AlertRow,
        payload: String,
    }

    let mut table = if *payload {
        let rows = alerts.iter().map(|(alert, marker)| {
            let payload = match serde_json::to_string(&alert.payload) {
                Ok(payload) => payload,
                Err(e) => {
                    eprintln!(
                        "/!\\ failed to serialize payload for {:?}: {e}",
                        alert.identity.id
                    );
                    "<error>".to_string()
                }
            };
            AlertRowWithPayload { row: make_row(alert, marker), payload }
        });
        tabled::Table::new(rows)
    } else {
        let rows = alerts.iter().map(|(alert, marker)| make_row(alert, marker));
        tabled::Table::new(rows)
    };
    table
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0));
    println!("{table}");

    Ok(())
}

async fn cmd_db_alert_info(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &AlertInfoArgs,
) -> anyhow::Result<()> {
    let AlertInfoArgs { id } = args;
    let (authz_alert, alert) = LookupPath::new(opctx, datastore)
        .alert_id(*id)
        .fetch()
        .await
        .with_context(|| format!("failed to look up alert {id}"))?;

    // Fetch any corresponding `rendezvous_alert_created` marker so we can
    // display the generation at which the alert was created. Note, all
    // FM-created alerts can be distinguished by their fm_case_id, but won't
    // necessarily have a creation marker: GC will clean up markers that are no
    // longer needed.
    let rendezvous_created =
        datastore.alert_fetch_fm_rendezvous_gen(opctx, &authz_alert).await;
    if let Err(ref e) = rendezvous_created {
        eprintln!("error: failed to fetch FM rendezvous marker: {e}");
    }

    let Alert {
        identity: db::model::AlertIdentity { time_created, time_modified, .. },
        time_dispatched,
        class,
        payload,
        num_dispatched,
        case_id,
        version,
    } = alert;

    const CLASS: &str = "class";
    const TIME_DISPATCHED: &str = "fully dispatched at";
    const NUM_DISPATCHED: &str = "deliveries dispatched";
    const CASE_ID: &str = "requested by FM case";
    const PROVENANCE: &str = "provenance";
    const FM_GENERATION: &str = "created at FM generation";

    const WIDTH: usize = const_max_len(&[
        ID,
        TIME_CREATED,
        TIME_MODIFIED,
        TIME_DISPATCHED,
        NUM_DISPATCHED,
        CLASS,
        CASE_ID,
        PROVENANCE,
        FM_GENERATION,
    ]);

    println!("\n{:=<80}", "== ALERT ");
    println!("    {ID:>WIDTH$}: {id:?}");
    println!("    {CLASS:>WIDTH$}: {class}, v{}", u32::from(version));
    println!("    {TIME_CREATED:>WIDTH$}: {time_created}");
    println!("    {TIME_MODIFIED:>WIDTH$}: {time_modified}");
    println!();
    println!("    {NUM_DISPATCHED:>WIDTH$}: {num_dispatched}");
    if let Some(t) = time_dispatched {
        println!("    {TIME_DISPATCHED:>WIDTH$}: {t}")
    }
    // We have two indicators that an alert was created by fault management:
    //   - The `fm_case_id` field, referencing the FM case.
    //   - The `rendezvous_alert_created` marker row referencing the alert,
    //     which also carries the generation at which it was created.
    // An FM-created alert will always have an `fm_case_id`, but may not have a
    // `rendezvous_alert_created` marker (these are GC'ed periodically when
    // they're no longer needed). It would be a bug, though, to have a marker
    // for an alert that's missing a `fm_case_id`.
    match (case_id, rendezvous_created) {
        (Some(case_id), marker) => {
            println!("    {PROVENANCE:>WIDTH$}: created by fault management");
            println!("    {CASE_ID:>WIDTH$}: {case_id:?}");
            match marker {
                Ok(Some(marker)) => {
                    println!(
                        "    {FM_GENERATION:>WIDTH$}: {}",
                        marker.created_at_generation.0
                    );
                }
                Ok(None) => {} // may have been GCed...
                Err(_) => {
                    println!(
                        "    {FM_GENERATION:>WIDTH$}: /!\\ UNKNOWN (failed to \
                         fetch marker record)"
                    );
                }
            }
        }
        (None, Ok(Some(marker))) => {
            println!(
                "    {PROVENANCE:>WIDTH$}: /!\\ WEIRD: creation marker present \
                 but no FM case (possible bug)"
            );
            println!(
                "    {FM_GENERATION:>WIDTH$}: {}",
                marker.created_at_generation.0
            );
        }
        // Note that this includes both cases where we successfully fetched an
        // `Ok(None)` marker record *and* cases where there was a database
        // error. We already printed the db error earlier, so we need not print
        // it again here, and we know the alert is not supposed to have one
        // regardless.
        (None, _) => {
            println!(
                "    {PROVENANCE:>WIDTH$}: not created by fault management"
            );
        }
    }

    println!("\n{:=<80}", "== ALERT PAYLOAD ");
    serde_json::to_writer_pretty(std::io::stdout(), &payload).with_context(
        || format!("failed to serialize alert payload: {payload:?}"),
    )?;

    let ctx = || format!("listing deliveries for alert {id:?}");

    let pagparams = DataPageParams {
        // Descending order shows the most recent delivery first
        direction: dropshot::PaginationOrder::Descending,
        ..first_page(fetch_opts.fetch_limit)
    };
    let deliveries = datastore
        .webhook_delivery_list(
            opctx,
            &WebhookDeliveryFilters::default().for_alert(&authz_alert),
            &pagparams,
        )
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
            "/!\\ WEIRD: alert claims to have {num_dispatched} deliveries \
             dispatched, but no delivery records were found"
        )
    }

    Ok(())
}

async fn display_rx_subscriptions(
    authz_rx: &authz::AlertReceiver,
    fetch_opts: &DbFetchOptions,
    conn: &async_bb8_diesel::Connection<DbConnection>,
) -> Result<(), anyhow::Error> {
    let rx_id = authz_rx.id();

    let ctx =
        || format!("loading glob subscriptions for alert receiver {rx_id}");

    let pagparams: DataPageParams<'_, String> =
        first_page(fetch_opts.fetch_limit);
    let globs = paginated(glob_dsl::alert_glob, glob_dsl::glob, &pagparams)
        .filter(glob_dsl::rx_id.eq(rx_id.into_untyped_uuid()))
        .select(db::model::AlertRxGlob::as_select())
        .load_async::<db::model::AlertRxGlob>(conn)
        .await
        .with_context(ctx)?;
    check_limit(&globs, fetch_opts.fetch_limit, ctx);

    // Load the exact subscriptions from the `alert_subscription` table. These
    // include both subscriptions added directly by the user and the exact
    // subscriptions expanded from glob subscriptions.
    let ctx =
        || format!("loading exact subscriptions for alert receiver {rx_id}");

    let pagparams: DataPageParams<'_, AlertClass> =
        first_page(fetch_opts.fetch_limit);
    let subscriptions = paginated(
        subscription_dsl::alert_subscription,
        subscription_dsl::alert_class,
        &pagparams,
    )
    .filter(subscription_dsl::rx_id.eq(rx_id.into_untyped_uuid()))
    .select(db::model::AlertRxSubscription::as_select())
    .load_async::<db::model::AlertRxSubscription>(conn)
    .await
    .with_context(ctx)?;
    check_limit(&subscriptions, fetch_opts.fetch_limit, ctx);

    #[derive(Clone, Copy, Tabled)]
    struct GlobRow<'a> {
        glob: &'a str,
        regex: &'a str,
        #[tabled(display_with = "display_reprocessed_at")]
        reprocessed_at: Option<&'a nexus_db_model::SemverVersion>,
        // Number of exact subscriptions generated by expanding this glob,
        // tallied from the `alert_subscription` table below.
        n_exact: usize,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        time_created: &'a DateTime<Utc>,
    }

    impl<'a> IdOrdItem for GlobRow<'a> {
        type Key<'k>
            = &'k str
        where
            Self: 'k;

        fn key(&self) -> Self::Key<'_> {
            self.glob
        }

        id_upcast!();
    }

    fn display_reprocessed_at(
        version: &Option<&nexus_db_model::SemverVersion>,
    ) -> std::borrow::Cow<'static, str> {
        if let Some(version) = version {
            std::borrow::Cow::Owned(version.to_string())
        } else {
            std::borrow::Cow::Borrowed("<not yet reprocessed>")
        }
    }

    let mut glob_rows: IdOrdMap<GlobRow<'_>> = globs
        .iter()
        .map(
            |db::model::AlertRxGlob {
                 rx_id: _,
                 glob: db::model::AlertGlob { glob, regex },
                 schema_version,
                 time_created,
             }| GlobRow {
                glob,
                regex,
                reprocessed_at: schema_version.as_ref(),
                n_exact: 0,
                time_created,
            },
        )
        .collect();

    // Count the number of exact subscriptions generated from each glob.
    let mut total_from_globs = 0;
    for glob in subscriptions.iter().filter_map(|s| s.glob.as_deref()) {
        if let Some(mut row) = glob_rows.get_mut(glob) {
            row.n_exact += 1;
        }
        total_from_globs += 1;
    }

    if !glob_rows.is_empty() {
        println!("glob subscriptions: {}", glob_rows.len());
        let mut table = tabled::Table::new(glob_rows.iter().copied());
        table
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0));
        println!("{table}\n");
    } else {
        println!("glob subscriptions: <none>");
    }

    #[derive(Tabled)]
    struct SubscriptionRow<'a> {
        alert_class: &'a AlertClass,
        #[tabled(display_with = "display_option_blank")]
        from_glob: Option<&'a str>,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        time_created: &'a DateTime<Utc>,
    }

    if !subscriptions.is_empty() {
        println!("exact subscriptions:  {}", subscriptions.len());
        println!("  generated by globs: {total_from_globs}");
        let mut table = tabled::Table::new(subscriptions.iter().map(
            |db::model::AlertRxSubscription {
                 rx_id: _,
                 class,
                 glob,
                 time_created,
             }| SubscriptionRow {
                alert_class: class,
                from_glob: glob.as_deref(),
                time_created,
            },
        ));
        table
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0));
        println!("{table}");
    } else {
        println!("exact subscriptions: <none>");
    }

    Ok(())
}
