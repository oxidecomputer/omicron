// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db multicast` subcommands.
//!
//! Inspect external multicast groups and their underlay groups (see [RFD 488]).
//! Underlay IPs and VNIs are not exposed by the external API. See
//! [`ExternalMulticastGroup`] and [`UnderlayMulticastGroup`] for the group
//! definitions and default VNI.
//!
//! # Command Outputs
//!
//! ## `omdb db multicast pools`
//!
//! | Column          | Description                                           |
//! |-----------------|-------------------------------------------------------|
//! | `POOL_ID`       | Pool UUID                                             |
//! | `POOL_NAME`     | Pool name                                             |
//! | `FIRST_ADDRESS` | Range start IP or `-` if the pool lacks one           |
//! | `LAST_ADDRESS`  | Range end IP or `-` if the pool lacks one             |
//! | `CREATED`       | Range creation time or `-` (empty pools, `...` rows)  |
//! | `TIME_DELETED`  | Deletion timestamp (with `--include-deleted`)         |
//!
//! Each range gets its own row. A pool without ranges gets one row.
//! A pool whose ranges hit `--fetch-limit` ends in a `...` row.
//! Use `--include-deleted` to include deleted pools and ranges.
//!
//! ## `omdb db multicast groups`
//!
//! | Column         | Description                                   |
//! |----------------|-----------------------------------------------|
//! | `ID`           | Group UUID                                    |
//! | `NAME`         | Group name                                    |
//! | `STATE`        | `Creating`, `Active`, or `Deleting`           |
//! | `MULTICAST_IP` | Stored external group IP                      |
//! | `RANGE`        | `ASM` or `SSM`                                |
//! | `UNDERLAY_IP`  | Underlay group IP (blank if not mapped)       |
//! | `SOURCES`      | Member source summary (see below)             |
//! | `MEMBERS`      | Member list (see below) or `-` if empty       |
//! | `VNI`          | Virtual network ID                            |
//! | `CREATED`      | Group creation timestamp                      |
//! | `TIME_DELETED` | Deletion timestamp (with `--include-deleted`) |
//!
//! `RANGE` uses [`is_ssm_address`] from `dpd-types` to classify the stored
//! address without validating it. Group states are defined in
//! [`MulticastGroupState`].
//!
//! `SOURCES` is composed of the union of source IPs from both non-deleted
//! instance and probe members in any state. It displays `-` when a member has
//! an empty source list or when there are no members at all. Deleted members
//! are excluded from this summary even with `--include-deleted` flagged.
//!
//! Each `MEMBERS` entry is displayed as `parent@sled`: the instance or
//! probe name (UUID if unavailable) followed by the sled serial number or `-`.
//!
//! When `--include-deleted` is provided, deleted members are marked with a
//! `(deleted)` suffix. If a trailing `...` is shown, this means that the
//! member fetch reached its `--fetch-limit` across the listed groups.
//!
//! `--state deleting` doesn't rely on providing `--include-deleted` as well
//! (groups pending cleanup already have `time_deleted` set).
//!
//! Filters: `--state`, `--pool`
//!
//! ## `omdb db multicast members`
//!
//! | Column         | Description                                   |
//! |----------------|-----------------------------------------------|
//! | `ID`           | Member UUID                                   |
//! | `GROUP_NAME`   | Parent group name                             |
//! | `PARENT_KIND`  | `instance` or `probe`                         |
//! | `PARENT_ID`    | Instance or probe UUID (per `PARENT_KIND`)    |
//! | `STATE`        | `Joining`, `Joined`, or `Left`                |
//! | `MULTICAST_IP` | Group multicast IP                            |
//! | `SOURCES`      | Source allowlist or `-` (any)                 |
//! | `SLED_ID`      | Assigned sled UUID (blank if not mapped)      |
//! | `CREATED`      | Creation timestamp                            |
//! | `TIME_DELETED` | Deletion timestamp (with `--include-deleted`) |
//!
//! Filters: `--group-id`, `--group-ip`, `--group-name`, `--parent-kind`,
//!          `--state`, `--sled-id`, `--source-ip`
//!
//! ## `omdb db multicast info`
//!
//! Detailed view of a single group with these sections:
//!
//! **MULTICAST GROUP**
//! - `id`, `name`, `state`, `multicast_ip`, `vni`, `source_ips` (union over
//!   members, same as `SOURCES` in `groups`)
//! - `ip_pool` (name + ID), `underlay_group`, `tag`, `created`,
//!   `deleted` (if set)
//!
//! **UNDERLAY GROUP** (if present)
//! - `id`, `multicast_ip`, `tag`, `created`, `deleted` (if set)
//!
//! **MEMBERS** (table)
//!
//! | Column         | Description                                   |
//! |----------------|-----------------------------------------------|
//! | `ID`           | Member UUID                                   |
//! | `PARENT_KIND`  | `instance` or `probe`                         |
//! | `PARENT`       | Instance or probe name (UUID if unavailable)  |
//! | `STATE`        | Member state                                  |
//! | `MULTICAST_IP` | Group multicast IP                            |
//! | `SOURCES`      | Allowlist or `-` (any)                        |
//! | `SLED`         | Sled serial number, or `-`                    |
//! | `CREATED`      | Creation timestamp                            |
//! | `TIME_DELETED` | Deletion timestamp (with `--include-deleted`) |
//!
//! Lookup: `--group-id`, `--ip`, `--name` (exactly one required)
//!
//! [RFD 488]: https://rfd.shared.oxide.computer/rfd/488
//! [`ExternalMulticastGroup`]: nexus_db_model::ExternalMulticastGroup
//! [`UnderlayMulticastGroup`]: nexus_db_model::UnderlayMulticastGroup
//! [`MulticastGroupState`]: nexus_db_model::MulticastGroupState

use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU32;

use anyhow::Context;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::AsyncSimpleConnection;
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand, ValueEnum};
use diesel::prelude::*;
use dpd_types::mcast::is_ssm_address;
use itertools::Itertools;
use tabled::Tabled;
use uuid::Uuid;

use nexus_db_lookup::DataStoreConnection;
use nexus_db_model::{
    ExternalMulticastGroup, IpPool, IpPoolRange, IpPoolType, MemberParentRef,
    MulticastGroupMember, MulticastGroupMemberParentKind,
    MulticastGroupMemberState, MulticastGroupState, UnderlayMulticastGroup,
};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::multicast::members::SourceFilterState;
use nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
use nexus_types::identity::Resource;
use omicron_uuid_kinds::{
    GenericUuid, InstanceUuid, MulticastGroupUuid, ProbeUuid, SledUuid,
};

use crate::db::{DbFetchOptions, check_limit};
use crate::helpers::{
    const_max_len, datetime_opt_rfc3339_concise, datetime_rfc3339_concise,
    display_option_blank,
};

/// CLI wrapper for MulticastGroupState to support clap ValueEnum
#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliGroupState {
    Creating,
    Active,
    Deleting,
}

impl From<CliGroupState> for MulticastGroupState {
    fn from(cli: CliGroupState) -> Self {
        match cli {
            CliGroupState::Creating => MulticastGroupState::Creating,
            CliGroupState::Active => MulticastGroupState::Active,
            CliGroupState::Deleting => MulticastGroupState::Deleting,
        }
    }
}

/// CLI wrapper for MulticastGroupMemberState to support clap ValueEnum
#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliMemberState {
    Joining,
    Joined,
    Left,
}

impl From<CliMemberState> for MulticastGroupMemberState {
    fn from(cli: CliMemberState) -> Self {
        match cli {
            CliMemberState::Joining => MulticastGroupMemberState::Joining,
            CliMemberState::Joined => MulticastGroupMemberState::Joined,
            CliMemberState::Left => MulticastGroupMemberState::Left,
        }
    }
}

/// CLI wrapper for MulticastGroupMemberParentKind to support clap ValueEnum
#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliParentKind {
    Instance,
    Probe,
}

impl From<CliParentKind> for MulticastGroupMemberParentKind {
    fn from(cli: CliParentKind) -> Self {
        match cli {
            CliParentKind::Instance => MulticastGroupMemberParentKind::Instance,
            CliParentKind::Probe => MulticastGroupMemberParentKind::Probe,
        }
    }
}

const RANGE_SSM: &str = "SSM";
const RANGE_ASM: &str = "ASM";

fn parent_kind_label(kind: MulticastGroupMemberParentKind) -> &'static str {
    match kind {
        MulticastGroupMemberParentKind::Instance => "instance",
        MulticastGroupMemberParentKind::Probe => "probe",
    }
}

/// `omdb db multicast` subcommand
#[derive(Debug, Args, Clone)]
pub(super) struct MulticastArgs {
    #[command(subcommand)]
    command: MulticastCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum MulticastCommands {
    /// List all multicast groups.
    ///
    /// Columns: ID, NAME, STATE, MULTICAST_IP, RANGE (ASM/SSM), UNDERLAY_IP,
    /// SOURCES (union over members), MEMBERS, VNI, CREATED, and TIME_DELETED
    /// with --include-deleted.
    #[clap(alias = "ls")]
    Groups(MulticastGroupsArgs),

    /// List all multicast group members, of any parent kind.
    ///
    /// Columns: ID, GROUP_NAME, PARENT_KIND, PARENT_ID, STATE, MULTICAST_IP,
    /// SOURCES, SLED_ID, CREATED, and TIME_DELETED with --include-deleted.
    Members(MulticastMembersArgs),

    /// List multicast IP pools and their ranges.
    ///
    /// Shows pool ID, name, first/last addresses, and creation time, and
    /// TIME_DELETED with --include-deleted provided.
    Pools,

    /// Get detailed info for a multicast group.
    ///
    /// Prints the group, its underlay group if one exists, then its members.
    #[clap(alias = "show")]
    Info(MulticastInfoArgs),
}

#[derive(Debug, Args, Clone)]
struct MulticastGroupsArgs {
    /// Filter by state
    #[arg(long, ignore_case = true, value_enum)]
    state: Option<CliGroupState>,
    /// Filter by pool name
    #[arg(long)]
    pool: Option<String>,
}

#[derive(Debug, Args, Clone)]
#[command(group(clap::ArgGroup::new("group_selector").multiple(false)))]
struct MulticastMembersArgs {
    /// Filter by group ID
    #[arg(long, group = "group_selector")]
    group_id: Option<MulticastGroupUuid>,
    /// Filter by group IP address (e.g., 239.1.2.3)
    #[arg(long, group = "group_selector")]
    group_ip: Option<std::net::IpAddr>,
    /// Filter by group name
    #[arg(long, group = "group_selector")]
    group_name: Option<String>,
    /// Filter by parent kind
    #[arg(long, ignore_case = true, value_enum)]
    parent_kind: Option<CliParentKind>,
    /// Filter by state
    #[arg(long, ignore_case = true, value_enum)]
    state: Option<CliMemberState>,
    /// Filter by sled ID
    #[arg(long)]
    sled_id: Option<SledUuid>,
    /// Filter by source IP (members subscribed to this source)
    #[arg(long)]
    source_ip: Option<std::net::IpAddr>,
}

#[derive(Debug, Args, Clone)]
#[group(required = true, multiple = false)]
struct MulticastInfoArgs {
    /// Multicast group ID
    #[arg(long)]
    group_id: Option<MulticastGroupUuid>,
    /// Multicast IP address (e.g., 239.1.2.3)
    #[arg(long)]
    ip: Option<std::net::IpAddr>,
    /// Multicast group name
    #[arg(long)]
    name: Option<String>,
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct MulticastGroupRow {
    id: Uuid,
    name: String,
    state: MulticastGroupState,
    multicast_ip: std::net::IpAddr,
    /// ASM (any-source) or SSM (source-specific) based on IP range
    range: &'static str,
    #[tabled(display_with = "display_option_blank")]
    underlay_ip: Option<std::net::IpAddr>,
    /// Source IPs union from members ("-" = any source)
    sources: String,
    /// Members formatted as "parent_1@sled, ..., parent_n@sled"
    members: String,
    vni: u32,
    #[tabled(display_with = "datetime_rfc3339_concise")]
    created: DateTime<Utc>,
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct MulticastMemberRow {
    id: Uuid,
    group_name: String,
    parent_kind: &'static str,
    parent_id: Uuid,
    state: MulticastGroupMemberState,
    multicast_ip: std::net::IpAddr,
    /// Source IPs for source filtering ("-" = any source)
    sources: String,
    #[tabled(display_with = "display_option_blank")]
    sled_id: Option<SledUuid>,
    #[tabled(display_with = "datetime_rfc3339_concise")]
    created: DateTime<Utc>,
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct MulticastInfoMemberRow {
    id: Uuid,
    parent_kind: &'static str,
    parent: String,
    state: MulticastGroupMemberState,
    multicast_ip: std::net::IpAddr,
    /// Source IPs for source filtering ("-" = any source)
    sources: String,
    sled: String,
    #[tabled(display_with = "datetime_rfc3339_concise")]
    created: DateTime<Utc>,
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct WithDeleted<T: Tabled> {
    #[tabled(inline)]
    inner: T,
    #[tabled(display_with = "datetime_opt_rfc3339_concise")]
    time_deleted: Option<DateTime<Utc>>,
}

impl<T: Tabled> WithDeleted<T> {
    fn table(rows: &[Self], include_deleted: bool) -> tabled::Table {
        let mut table = if include_deleted {
            tabled::Table::new(rows)
        } else {
            tabled::Table::new(rows.iter().map(|row| &row.inner))
        };
        table
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0));
        table
    }
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct MulticastPoolRow {
    pool_id: Uuid,
    pool_name: String,
    first_address: String,
    last_address: String,
    #[tabled(display_with = "datetime_opt_rfc3339_concise")]
    created: Option<DateTime<Utc>>,
}

struct MulticastInfoDisplay<'a> {
    group: &'a ExternalMulticastGroup,
    underlay: Option<&'a UnderlayMulticastGroup>,
    pool_name: &'a str,
    source_ips: &'a str,
    members: &'a [WithDeleted<MulticastInfoMemberRow>],
    include_deleted: bool,
}

impl fmt::Display for MulticastInfoDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const ID: &str = "id";
        const NAME: &str = "name";
        const STATE: &str = "state";
        const MULTICAST_IP: &str = "multicast_ip";
        const VNI: &str = "vni";
        const SOURCE_IPS: &str = "source_ips";
        const IP_POOL: &str = "ip_pool";
        const UNDERLAY_GROUP: &str = "underlay_group";
        const TAG: &str = "tag";
        const CREATED: &str = "created";
        const DELETED: &str = "deleted";
        const WIDTH: usize = const_max_len(&[
            ID,
            NAME,
            STATE,
            MULTICAST_IP,
            VNI,
            SOURCE_IPS,
            IP_POOL,
            UNDERLAY_GROUP,
            TAG,
            CREATED,
            DELETED,
        ]);

        let group = self.group;
        writeln!(f, "MULTICAST GROUP")?;
        writeln!(f, "    {ID:>WIDTH$}: {}", group.identity.id)?;
        writeln!(f, "    {NAME:>WIDTH$}: {}", group.identity.name)?;
        writeln!(f, "    {STATE:>WIDTH$}: {}", group.state)?;
        writeln!(f, "    {MULTICAST_IP:>WIDTH$}: {}", group.multicast_ip.ip())?;
        writeln!(f, "    {VNI:>WIDTH$}: {}", u32::from(group.vni.0))?;
        writeln!(f, "    {SOURCE_IPS:>WIDTH$}: {}", self.source_ips)?;
        writeln!(
            f,
            "    {IP_POOL:>WIDTH$}: {} ({})",
            self.pool_name, group.ip_pool_id
        )?;
        writeln!(
            f,
            "    {UNDERLAY_GROUP:>WIDTH$}: {}",
            group
                .underlay_group_id
                .map_or_else(|| String::from("-"), |id| id.to_string())
        )?;
        writeln!(f, "    {TAG:>WIDTH$}: {}", display_option_blank(&group.tag))?;
        writeln!(
            f,
            "    {CREATED:>WIDTH$}: {}",
            datetime_rfc3339_concise(&group.identity.time_created)
        )?;
        if let Some(deleted) = group.identity.time_deleted {
            writeln!(
                f,
                "    {DELETED:>WIDTH$}: {}",
                datetime_rfc3339_concise(&deleted)
            )?;
        }

        if let Some(underlay_group) = self.underlay {
            writeln!(f, "\nUNDERLAY GROUP")?;
            writeln!(f, "    {ID:>WIDTH$}: {}", underlay_group.id)?;
            writeln!(
                f,
                "    {MULTICAST_IP:>WIDTH$}: {}",
                underlay_group.multicast_ip.ip()
            )?;
            writeln!(
                f,
                "    {TAG:>WIDTH$}: {}",
                display_option_blank(&underlay_group.tag)
            )?;
            writeln!(
                f,
                "    {CREATED:>WIDTH$}: {}",
                datetime_rfc3339_concise(&underlay_group.time_created)
            )?;
            if let Some(deleted) = underlay_group.time_deleted {
                writeln!(
                    f,
                    "    {DELETED:>WIDTH$}: {}",
                    datetime_rfc3339_concise(&deleted)
                )?;
            }
        }

        if self.members.is_empty() {
            writeln!(f, "\nMEMBERS: (none)")
        } else {
            writeln!(f, "\nMEMBERS ({}):", self.members.len())?;
            let table = WithDeleted::table(self.members, self.include_deleted);
            writeln!(f, "{table}")
        }
    }
}

/// Batch lookup of parent names for a set of members.
///
/// `parent_id` refers to either the `instance` or the `probe` table, as given
/// by `parent_kind`.
async fn fetch_parent_names(
    conn: &DataStoreConnection,
    members: &[MulticastGroupMember],
) -> Result<HashMap<MemberParentRef, String>, anyhow::Error> {
    use nexus_db_schema::schema::instance::dsl as instance_dsl;
    use nexus_db_schema::schema::probe::dsl as probe_dsl;

    let mut instance_ids = Vec::new();
    let mut probe_ids = Vec::new();
    for member in members {
        match member.parent_kind {
            MulticastGroupMemberParentKind::Instance => {
                instance_ids.push(member.parent_id)
            }
            MulticastGroupMemberParentKind::Probe => {
                probe_ids.push(member.parent_id)
            }
        }
    }

    let mut names = HashMap::new();

    if !instance_ids.is_empty() {
        let instances: Vec<(Uuid, String)> = instance_dsl::instance
            .filter(instance_dsl::id.eq_any(instance_ids))
            .select((instance_dsl::id, instance_dsl::name))
            .get_results_async(&**conn)
            .await
            .context("fetching instance names")?;
        for (id, name) in instances {
            names.insert(
                MemberParentRef::Instance(InstanceUuid::from_untyped_uuid(id)),
                name,
            );
        }
    }

    if !probe_ids.is_empty() {
        let probes: Vec<(Uuid, String)> = probe_dsl::probe
            .filter(probe_dsl::id.eq_any(probe_ids))
            .select((probe_dsl::id, probe_dsl::name))
            .get_results_async(&**conn)
            .await
            .context("fetching probe names")?;
        for (id, name) in probes {
            names.insert(
                MemberParentRef::Probe(ProbeUuid::from_untyped_uuid(id)),
                name,
            );
        }
    }

    Ok(names)
}

/// Render the source summary for `groups` and `info`.
///
/// Displays `-` if any member has an empty source list or the source union is
/// empty. Otherwise, we display the sorted source IPs, separated by commas.
fn source_filter_display(state: Option<&SourceFilterState>) -> String {
    match state {
        Some(state)
            if !state.has_any_source_member
                && !state.specific_sources.is_empty() =>
        {
            state
                .specific_sources
                .iter()
                .map(|ip| ip.to_string())
                .collect::<Vec<_>>()
                .join(",")
        }
        _ => "-".to_string(),
    }
}

/// Look up a single multicast group by IP or name.
///
/// With the `--include-deleted` flag designated, more than one row can match.
/// We return an error when this occurs so the caller can disambiguate with
/// the group ID.
async fn lookup_group(
    conn: &DataStoreConnection,
    fetch_opts: &DbFetchOptions,
    ip: Option<std::net::IpAddr>,
    name: Option<&str>,
) -> Result<Option<ExternalMulticastGroup>, anyhow::Error> {
    use nexus_db_schema::schema::multicast_group::dsl as group_dsl;

    let mut query = group_dsl::multicast_group.into_boxed();
    let selector = match (ip, name) {
        (Some(ip), _) => {
            query = query.filter(
                group_dsl::multicast_ip.eq(ipnetwork::IpNetwork::from(ip)),
            );
            format!("IP {ip}")
        }
        (None, Some(name)) => {
            query = query.filter(group_dsl::name.eq(name.to_string()));
            format!("name \"{name}\"")
        }
        (None, None) => return Ok(None),
    };

    if !fetch_opts.include_deleted {
        query = query.filter(group_dsl::time_deleted.is_null());
    }

    let groups: Vec<ExternalMulticastGroup> = conn
        .transaction_async(async move |conn| {
            if fetch_opts.include_deleted {
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
            }
            query
                .order_by(group_dsl::time_created.desc())
                .limit(2)
                .select(ExternalMulticastGroup::as_select())
                .get_results_async(&conn)
                .await
        })
        .await
        .with_context(|| {
            format!("looking up multicast group with {selector}")
        })?;

    match groups.len() {
        0 if fetch_opts.include_deleted => {
            anyhow::bail!("no multicast group found with {selector}")
        }
        0 => anyhow::bail!(
            "no multicast group found with {selector} \
             (use --include-deleted to search deleted groups)"
        ),
        1 => Ok(groups.into_iter().next()),
        _ => anyhow::bail!(
            "more than one multicast group matches {selector} \
             (including deleted groups), use --group-id"
        ),
    }
}

pub(super) async fn cmd_db_multicast(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &MulticastArgs,
) -> Result<(), anyhow::Error> {
    match &args.command {
        MulticastCommands::Groups(args) => {
            cmd_db_multicast_groups(opctx, datastore, fetch_opts, args).await
        }
        MulticastCommands::Members(args) => {
            cmd_db_multicast_members(datastore, fetch_opts, args).await
        }
        MulticastCommands::Pools => {
            cmd_db_multicast_pools(datastore, fetch_opts).await
        }
        MulticastCommands::Info(args) => {
            cmd_db_multicast_info(opctx, datastore, fetch_opts, args).await
        }
    }
}

async fn cmd_db_multicast_groups(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &MulticastGroupsArgs,
) -> Result<(), anyhow::Error> {
    use nexus_db_schema::schema::ip_pool::dsl as pool_dsl;
    use nexus_db_schema::schema::multicast_group::dsl;
    use nexus_db_schema::schema::multicast_group_member::dsl as member_dsl;
    use nexus_db_schema::schema::sled::dsl as sled_dsl;
    use nexus_db_schema::schema::underlay_multicast_group::dsl as underlay_dsl;

    let conn = datastore.pool_connection_for_tests().await?;

    let groups: Vec<ExternalMulticastGroup> = conn
        .transaction_async(async move |conn| {
            if fetch_opts.include_deleted {
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
            }
            let state = args.state.map(MulticastGroupState::from);
            let mut query = dsl::multicast_group.into_boxed();
            if !fetch_opts.include_deleted
                && state != Some(MulticastGroupState::Deleting)
            {
                query = query.filter(dsl::time_deleted.is_null());
            }
            if let Some(state) = state {
                query = query.filter(dsl::state.eq(state));
            }
            if let Some(ref pool_name) = args.pool {
                let mut pool_query = pool_dsl::ip_pool
                    .filter(pool_dsl::name.eq(pool_name.clone()))
                    .filter(pool_dsl::pool_type.eq(IpPoolType::Multicast))
                    .into_boxed();
                if !fetch_opts.include_deleted {
                    pool_query =
                        pool_query.filter(pool_dsl::time_deleted.is_null());
                }
                let pool_ids: Vec<Uuid> = pool_query
                    .select(pool_dsl::id)
                    .get_results_async(&conn)
                    .await
                    .context("looking up multicast pools")?;
                if pool_ids.is_empty() {
                    anyhow::bail!(
                        "no multicast pool found with name \"{pool_name}\""
                    );
                }
                query = query.filter(dsl::ip_pool_id.eq_any(pool_ids));
            }

            query
                .order_by(dsl::time_created.desc())
                .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
                .select(ExternalMulticastGroup::as_select())
                .get_results_async(&conn)
                .await
                .context("listing multicast groups")
        })
        .await?;

    check_limit(&groups, fetch_opts.fetch_limit, || {
        String::from("listing multicast groups")
    });

    let underlay_ids: Vec<Uuid> =
        groups.iter().filter_map(|group| group.underlay_group_id).collect();
    let underlay_ips: HashMap<Uuid, ipnetwork::IpNetwork> =
        lookup_by_ids(underlay_ids, async |ids| {
            underlay_dsl::underlay_multicast_group
                .filter(underlay_dsl::id.eq_any(ids))
                .select((underlay_dsl::id, underlay_dsl::multicast_ip))
                .get_results_async(&*conn)
                .await
                .context("fetching underlay groups")
        })
        .await?;

    // Fetch members for all groups
    let group_ids: Vec<Uuid> = groups.iter().map(|g| g.identity.id).collect();
    let mut members: Vec<MulticastGroupMember> = if group_ids.is_empty() {
        Vec::new()
    } else {
        let mut mq = member_dsl::multicast_group_member
            .filter(member_dsl::external_group_id.eq_any(group_ids))
            .into_boxed();
        if !fetch_opts.include_deleted {
            mq = mq.filter(member_dsl::time_deleted.is_null());
        }
        mq.order_by(member_dsl::external_group_id.asc())
            .then_order_by(member_dsl::parent_id.asc())
            .then_order_by(member_dsl::id.asc())
            .limit(i64::from(u32::from(fetch_opts.fetch_limit)) + 1)
            .select(MulticastGroupMember::as_select())
            .get_results_async(&*conn)
            .await
            .context("fetching multicast group members")?
    };
    let members_trunc_from =
        trunc_at_limit(&mut members, fetch_opts.fetch_limit, |m| {
            m.external_group_id
        });
    if members_trunc_from.is_some() {
        trunc_warning(
            fetch_opts.fetch_limit,
            "listing members of the listed multicast groups",
        );
    }

    let typed_group_ids: Vec<MulticastGroupUuid> = groups
        .iter()
        .map(|g| MulticastGroupUuid::from_untyped_uuid(g.identity.id))
        .collect();
    let source_states = datastore
        .multicast_groups_source_filter_state(opctx, &typed_group_ids)
        .await
        .context("computing multicast group source filters")?;

    let parent_names = fetch_parent_names(&conn, &members).await?;

    let sled_ids: Vec<Uuid> = members
        .iter()
        .filter_map(|m| m.sled_id.map(|s| s.into_untyped_uuid()))
        .collect();
    let sled_serials: HashMap<Uuid, String> =
        lookup_by_ids(sled_ids, async |ids| {
            sled_dsl::sled
                .filter(sled_dsl::id.eq_any(ids))
                .select((sled_dsl::id, sled_dsl::serial_number))
                .get_results_async(&*conn)
                .await
                .context("fetching sled serials")
        })
        .await?;

    let members_by_group: HashMap<Uuid, Vec<String>> = members
        .iter()
        .map(|member| {
            let entry = member_entry(
                &member_parent_name(member, &parent_names),
                &member_sled_serial(member, &sled_serials),
                member.time_deleted.is_some(),
            );
            (member.external_group_id, entry)
        })
        .sorted()
        .into_group_map();

    let rows: Vec<WithDeleted<MulticastGroupRow>> = groups
        .into_iter()
        .map(|group| {
            let mcast_ip = group.multicast_ip.ip();
            let range = range_label(mcast_ip);
            let sources =
                source_filter_display(source_states.get(&group.identity.id));
            let underlay_ip = match group.underlay_group_id {
                Some(id) => match underlay_ips.get(&id) {
                    Some(ip) => Some(ip.ip()),
                    None => {
                        eprintln!(
                            "WARN: multicast group {} references missing \
                             underlay group {id}",
                            group.identity.id,
                        );
                        None
                    }
                },
                None => None,
            };
            let trunc = past_boundary(group.identity.id, members_trunc_from);
            let members = members_cell(
                members_by_group.get(&group.identity.id).map(Vec::as_slice),
                trunc,
            );
            WithDeleted {
                inner: MulticastGroupRow {
                    id: group.identity.id,
                    name: group.identity.name.to_string(),
                    state: group.state,
                    multicast_ip: mcast_ip,
                    range,
                    underlay_ip,
                    sources,
                    members,
                    vni: u32::from(group.vni.0),
                    created: group.identity.time_created,
                },
                time_deleted: group.identity.time_deleted,
            }
        })
        .collect();

    print!(
        "{}",
        MulticastGroupsDisplay {
            rows: &rows,
            include_deleted: fetch_opts.include_deleted,
        }
    );

    Ok(())
}

fn trunc_at_limit<T>(
    rows: &mut Vec<T>,
    limit: NonZeroU32,
    key: impl Fn(&T) -> Uuid,
) -> Option<Uuid> {
    let limit = usize::try_from(limit.get()).unwrap();
    let boundary = rows.get(limit).map(key);
    rows.truncate(limit);
    boundary
}

async fn lookup_by_ids<V>(
    mut ids: Vec<Uuid>,
    lookup: impl AsyncFnOnce(Vec<Uuid>) -> anyhow::Result<Vec<(Uuid, V)>>,
) -> anyhow::Result<HashMap<Uuid, V>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(lookup(ids).await?.into_iter().collect())
}

fn past_boundary(id: Uuid, boundary: Option<Uuid>) -> bool {
    boundary.is_some_and(|first| id >= first)
}

fn trunc_warning(limit: NonZeroU32, context: &str) {
    eprintln!(
        "WARN: {context}: found more than {limit} items; output truncated.  \
         Increase --fetch-limit to see more items."
    );
}

fn range_label(ip: std::net::IpAddr) -> &'static str {
    if is_ssm_address(ip) { RANGE_SSM } else { RANGE_ASM }
}

fn member_sources(member: &MulticastGroupMember) -> String {
    if member.source_ips.is_empty() {
        String::from("-")
    } else {
        member.source_ips.iter().map(|ip| ip.ip()).join(",")
    }
}

fn member_parent_name(
    member: &MulticastGroupMember,
    parent_names: &HashMap<MemberParentRef, String>,
) -> String {
    parent_names
        .get(&member.parent_ref())
        .cloned()
        .unwrap_or_else(|| member.parent_id.to_string())
}

fn member_sled_serial(
    member: &MulticastGroupMember,
    sled_serials: &HashMap<Uuid, String>,
) -> String {
    member
        .sled_id
        .and_then(|s| sled_serials.get(&s.into_untyped_uuid()).cloned())
        .unwrap_or_else(|| String::from("-"))
}

fn member_entry(parent: &str, sled: &str, deleted: bool) -> String {
    if deleted {
        format!("{parent}@{sled} (deleted)")
    } else {
        format!("{parent}@{sled}")
    }
}

fn members_cell(entries: Option<&[String]>, trunc: bool) -> String {
    match (entries, trunc) {
        (Some(v), false) => v.join(", "),
        (Some(v), true) => format!("{}, ...", v.join(", ")),
        (None, false) => String::from("-"),
        (None, true) => String::from("..."),
    }
}

struct MulticastGroupsDisplay<'a> {
    rows: &'a [WithDeleted<MulticastGroupRow>],
    include_deleted: bool,
}

impl fmt::Display for MulticastGroupsDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table = WithDeleted::table(self.rows, self.include_deleted);
        writeln!(f, "{table}")
    }
}

async fn cmd_db_multicast_members(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &MulticastMembersArgs,
) -> Result<(), anyhow::Error> {
    use nexus_db_schema::schema::multicast_group::dsl as group_dsl;
    use nexus_db_schema::schema::multicast_group_member::dsl;

    let conn = datastore.pool_connection_for_tests().await?;

    let resolved_group_id = match lookup_group(
        &conn,
        fetch_opts,
        args.group_ip,
        args.group_name.as_deref(),
    )
    .await?
    {
        Some(group) => Some(MulticastGroupUuid::from_untyped_uuid(group.id())),
        None => args.group_id,
    };

    let mut query = dsl::multicast_group_member.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());
    }
    if let Some(group_id) = resolved_group_id {
        query = query
            .filter(dsl::external_group_id.eq(group_id.into_untyped_uuid()));
    }
    if let Some(kind) =
        args.parent_kind.map(MulticastGroupMemberParentKind::from)
    {
        query = query.filter(dsl::parent_kind.eq(kind));
    }
    if let Some(state) = args.state.map(MulticastGroupMemberState::from) {
        query = query.filter(dsl::state.eq(state));
    }
    if let Some(sled_id) = args.sled_id {
        query = query.filter(dsl::sled_id.eq(sled_id.into_untyped_uuid()));
    }
    if let Some(source_ip) = args.source_ip {
        let ip_network = ipnetwork::IpNetwork::from(source_ip);
        query = query.filter(dsl::source_ips.contains(vec![ip_network]));
    }

    let members: Vec<MulticastGroupMember> = conn
        .transaction_async(async move |conn| {
            if fetch_opts.include_deleted {
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
            }
            query
                .order_by(dsl::time_created.desc())
                .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
                .select(MulticastGroupMember::as_select())
                .get_results_async(&conn)
                .await
        })
        .await?;

    check_limit(&members, fetch_opts.fetch_limit, || {
        String::from("listing multicast group members")
    });

    let group_ids: Vec<Uuid> =
        members.iter().map(|member| member.external_group_id).collect();
    let group_names: HashMap<Uuid, String> =
        lookup_by_ids(group_ids, async |ids| {
            group_dsl::multicast_group
                .filter(group_dsl::id.eq_any(ids))
                .select((group_dsl::id, group_dsl::name))
                .get_results_async(&*conn)
                .await
                .context("fetching group names")
        })
        .await?;

    let rows: Vec<WithDeleted<MulticastMemberRow>> = members
        .into_iter()
        .map(|member| {
            let group_name = group_names
                .get(&member.external_group_id)
                .cloned()
                .unwrap_or_else(|| member.external_group_id.to_string());
            let sources = member_sources(&member);
            WithDeleted {
                inner: MulticastMemberRow {
                    id: member.id,
                    group_name,
                    parent_kind: parent_kind_label(member.parent_kind),
                    parent_id: member.parent_id,
                    state: member.state,
                    multicast_ip: member.multicast_ip.ip(),
                    sources,
                    sled_id: member.sled_id.map(SledUuid::from),
                    created: member.time_created,
                },
                time_deleted: member.time_deleted,
            }
        })
        .collect();

    println!("{}", WithDeleted::table(&rows, fetch_opts.include_deleted));

    Ok(())
}

async fn cmd_db_multicast_pools(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
) -> Result<(), anyhow::Error> {
    use nexus_db_schema::schema::ip_pool::dsl as pool_dsl;
    use nexus_db_schema::schema::ip_pool_range::dsl as range_dsl;

    let conn = datastore.pool_connection_for_tests().await?;

    let mut query = pool_dsl::ip_pool.into_boxed();
    query = query.filter(pool_dsl::pool_type.eq(IpPoolType::Multicast));
    if !fetch_opts.include_deleted {
        query = query.filter(pool_dsl::time_deleted.is_null());
    }

    let pools: Vec<IpPool> = conn
        .transaction_async(async move |conn| {
            if fetch_opts.include_deleted {
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
            }
            query
                .order_by(pool_dsl::time_created.desc())
                .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
                .select(IpPool::as_select())
                .get_results_async(&conn)
                .await
        })
        .await?;

    check_limit(&pools, fetch_opts.fetch_limit, || {
        String::from("listing multicast pools")
    });

    if pools.is_empty() {
        println!("no multicast IP pools found");
        return Ok(());
    }

    let pool_ids: Vec<Uuid> = pools.iter().map(|pool| pool.id()).collect();

    let mut range_query = range_dsl::ip_pool_range.into_boxed();
    range_query =
        range_query.filter(range_dsl::ip_pool_id.eq_any(pool_ids.clone()));
    if !fetch_opts.include_deleted {
        range_query = range_query.filter(range_dsl::time_deleted.is_null());
    }

    let mut ranges: Vec<IpPoolRange> = conn
        .transaction_async(async move |conn| {
            if fetch_opts.include_deleted {
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
            }
            range_query
                .order_by(range_dsl::ip_pool_id)
                .then_order_by(range_dsl::first_address)
                .limit(i64::from(u32::from(fetch_opts.fetch_limit)) + 1)
                .select(IpPoolRange::as_select())
                .get_results_async(&conn)
                .await
        })
        .await?;
    let ranges_trunc_from =
        trunc_at_limit(&mut ranges, fetch_opts.fetch_limit, |r| r.ip_pool_id);
    if ranges_trunc_from.is_some() {
        trunc_warning(
            fetch_opts.fetch_limit,
            "listing ranges of the listed multicast IP pools",
        );
    }

    let mut ranges_by_pool: HashMap<Uuid, Vec<IpPoolRange>> = HashMap::new();
    for range in ranges {
        ranges_by_pool.entry(range.ip_pool_id).or_default().push(range);
    }

    let pool_row = |pool: &IpPool, first: String, last: String| WithDeleted {
        inner: MulticastPoolRow {
            pool_id: pool.id(),
            pool_name: pool.name().to_string(),
            first_address: first,
            last_address: last,
            created: None,
        },
        time_deleted: pool.identity.time_deleted,
    };

    let mut rows: Vec<WithDeleted<MulticastPoolRow>> = Vec::new();
    for pool in &pools {
        let trunc = past_boundary(pool.id(), ranges_trunc_from);
        match ranges_by_pool.remove(&pool.id()) {
            Some(pool_ranges) => {
                rows.extend(pool_ranges.into_iter().map(|range| WithDeleted {
                    inner: MulticastPoolRow {
                        pool_id: pool.id(),
                        pool_name: pool.name().to_string(),
                        first_address: range.first_address.ip().to_string(),
                        last_address: range.last_address.ip().to_string(),
                        created: Some(range.time_created),
                    },
                    time_deleted:
                        range.time_deleted.or(pool.identity.time_deleted),
                }))
            }
            None if !trunc => {
                rows.push(pool_row(pool, String::from("-"), String::from("-")))
            }
            None => {}
        }
        if trunc {
            rows.push(pool_row(pool, String::from("..."), String::from("...")));
        }
    }

    println!("{}", WithDeleted::table(&rows, fetch_opts.include_deleted));

    Ok(())
}

async fn cmd_db_multicast_info(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &MulticastInfoArgs,
) -> Result<(), anyhow::Error> {
    use nexus_db_schema::schema::ip_pool::dsl as pool_dsl;
    use nexus_db_schema::schema::multicast_group::dsl as group_dsl;
    use nexus_db_schema::schema::multicast_group_member::dsl as member_dsl;
    use nexus_db_schema::schema::sled::dsl as sled_dsl;
    use nexus_db_schema::schema::underlay_multicast_group::dsl as underlay_dsl;

    let conn = datastore.pool_connection_for_tests().await?;

    let group_id = match args.group_id {
        Some(id) => id,
        None => lookup_group(&conn, fetch_opts, args.ip, args.name.as_deref())
            .await?
            .map(|group| MulticastGroupUuid::from_untyped_uuid(group.id()))
            .expect("clap requires one of --group-id, --ip, or --name"),
    };

    let mut query = group_dsl::multicast_group
        .filter(group_dsl::id.eq(group_id.into_untyped_uuid()))
        .into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(group_dsl::time_deleted.is_null());
    }

    let row: Option<(ExternalMulticastGroup, Option<UnderlayMulticastGroup>)> =
        query
            .left_join(underlay_dsl::underlay_multicast_group.on(
                underlay_dsl::id.nullable().eq(group_dsl::underlay_group_id),
            ))
            .select((
                ExternalMulticastGroup::as_select(),
                Option::<UnderlayMulticastGroup>::as_select(),
            ))
            .first_async(&*conn)
            .await
            .optional()
            .context("fetching multicast group")?;

    let Some((group, underlay)) = row else {
        if fetch_opts.include_deleted {
            anyhow::bail!("no multicast group found with ID {group_id}");
        }
        anyhow::bail!(
            "no multicast group found with ID {group_id} \
             (use --include-deleted to search deleted groups)"
        );
    };

    let pool_name: String = match pool_dsl::ip_pool
        .filter(pool_dsl::id.eq(group.ip_pool_id))
        .select(pool_dsl::name)
        .first_async(&*conn)
        .await
        .optional()
        .context("fetching pool name")?
    {
        Some(name) => name,
        None => {
            eprintln!("WARN: no pool found for ID {}", group.ip_pool_id);
            "<unknown>".into()
        }
    };

    let mut member_query = member_dsl::multicast_group_member.into_boxed();
    member_query = member_query
        .filter(member_dsl::external_group_id.eq(group.identity.id));
    if !fetch_opts.include_deleted {
        member_query = member_query.filter(member_dsl::time_deleted.is_null());
    }

    let members: Vec<MulticastGroupMember> = member_query
        .order_by(member_dsl::time_created.desc())
        .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
        .select(MulticastGroupMember::as_select())
        .get_results_async(&*conn)
        .await?;
    check_limit(&members, fetch_opts.fetch_limit, || {
        String::from("listing multicast group members")
    });

    let source_states = datastore
        .multicast_groups_source_filter_state(
            opctx,
            &[MulticastGroupUuid::from_untyped_uuid(group.identity.id)],
        )
        .await
        .context("computing multicast group source filter")?;
    let source_ips_display =
        source_filter_display(source_states.get(&group.identity.id));

    if let (Some(underlay_id), None) =
        (group.underlay_group_id, underlay.as_ref())
    {
        eprintln!(
            "WARN: multicast group {} references missing underlay \
             group {underlay_id}",
            group.identity.id,
        );
    }

    let parent_names = fetch_parent_names(&conn, &members).await?;

    let sled_ids: Vec<Uuid> = members
        .iter()
        .filter_map(|member| member.sled_id.map(|s| s.into_untyped_uuid()))
        .collect();
    let sled_serials: HashMap<Uuid, String> =
        lookup_by_ids(sled_ids, async |ids| {
            sled_dsl::sled
                .filter(sled_dsl::id.eq_any(ids))
                .select((sled_dsl::id, sled_dsl::serial_number))
                .get_results_async(&*conn)
                .await
                .context("fetching sled serials")
        })
        .await?;

    let rows: Vec<WithDeleted<MulticastInfoMemberRow>> = members
        .into_iter()
        .map(|member| {
            let parent_name = member_parent_name(&member, &parent_names);
            let sled_serial = member_sled_serial(&member, &sled_serials);
            let sources = member_sources(&member);
            WithDeleted {
                inner: MulticastInfoMemberRow {
                    id: member.id,
                    parent_kind: parent_kind_label(member.parent_kind),
                    parent: parent_name,
                    state: member.state,
                    multicast_ip: member.multicast_ip.ip(),
                    sources,
                    sled: sled_serial,
                    created: member.time_created,
                },
                time_deleted: member.time_deleted,
            }
        })
        .collect();

    print!(
        "{}",
        MulticastInfoDisplay {
            group: &group,
            underlay: underlay.as_ref(),
            pool_name: &pool_name,
            source_ips: &source_ips_display,
            members: &rows,
            include_deleted: fetch_opts.include_deleted,
        }
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_db_model::{
        ExternalMulticastGroupIdentity, Generation, Name, Vni,
    };
    use omicron_common::api::external;
    use omicron_test_utils::dev::test_cmds::OutputSnapshot;

    fn time(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn group(
        state: MulticastGroupState,
        underlay_group_id: Option<Uuid>,
        time_deleted: Option<DateTime<Utc>>,
    ) -> ExternalMulticastGroup {
        let id: Uuid = "1b2b4c4f-42a5-4e3c-8b28-a1c1f2f3a0b1".parse().unwrap();
        let multicast_ip = "232.1.2.3".parse().unwrap();
        ExternalMulticastGroup {
            identity: ExternalMulticastGroupIdentity {
                id,
                name: Name("test-group".parse().unwrap()),
                description: String::from("test multicast group"),
                time_created: time("2026-01-02T03:04:05Z"),
                time_modified: time("2026-01-02T03:04:05Z"),
                time_deleted,
            },
            vni: Vni(external::Vni::DEFAULT_MULTICAST_VNI),
            ip_pool_id: "9d8e2f61-7c0d-4b6a-9e5f-3a4b5c6d7e8f".parse().unwrap(),
            ip_pool_range_id: "5e4d3c2b-1a09-4f8e-8d7c-6b5a49382716"
                .parse()
                .unwrap(),
            multicast_ip,
            underlay_group_id,
            tag: Some(format!("{id}:232.1.2.3")),
            version_added: Generation::new(),
            version_removed: None,
            underlay_salt: None,
            state,
        }
    }

    fn underlay(
        id: Uuid,
        time_deleted: Option<DateTime<Utc>>,
    ) -> UnderlayMulticastGroup {
        UnderlayMulticastGroup {
            id,
            time_created: time("2026-01-02T03:04:06Z"),
            time_modified: time("2026-01-02T03:04:06Z"),
            time_deleted,
            multicast_ip: "ff04::e801:203".parse().unwrap(),
            tag: Some(String::from(
                "1b2b4c4f-42a5-4e3c-8b28-a1c1f2f3a0b1:232.1.2.3",
            )),
            version_added: Generation::new(),
            version_removed: None,
        }
    }

    fn members(
        time_deleted: Option<DateTime<Utc>>,
    ) -> Vec<WithDeleted<MulticastInfoMemberRow>> {
        vec![
            WithDeleted {
                inner: MulticastInfoMemberRow {
                    id: "0a1b2c3d-4e5f-4a6b-8c7d-8e9fa0b1c2d3".parse().unwrap(),
                    parent_kind: parent_kind_label(
                        MulticastGroupMemberParentKind::Instance,
                    ),
                    parent: String::from("web-0"),
                    state: MulticastGroupMemberState::Joined,
                    multicast_ip: "232.1.2.3".parse().unwrap(),
                    sources: String::from("10.0.0.1,10.0.0.2"),
                    sled: String::from("BRM42220001"),
                    created: time("2026-01-02T03:05:00Z"),
                },
                time_deleted: None,
            },
            WithDeleted {
                inner: MulticastInfoMemberRow {
                    id: "f0e1d2c3-b4a5-4968-8776-655443322110".parse().unwrap(),
                    parent_kind: parent_kind_label(
                        MulticastGroupMemberParentKind::Probe,
                    ),
                    parent: String::from("probe-0"),
                    state: MulticastGroupMemberState::Left,
                    multicast_ip: "232.1.2.3".parse().unwrap(),
                    sources: String::from("-"),
                    sled: String::from("-"),
                    created: time("2026-01-02T03:06:00Z"),
                },
                time_deleted,
            },
        ]
    }

    #[test]
    fn test_multicast_info_display() {
        let underlay_id: Uuid =
            "3c4d5e6f-7a8b-4c9d-8e0f-1a2b3c4d5e6f".parse().unwrap();
        let deleted = Some(time("2026-01-03T00:00:00Z"));

        let active =
            group(MulticastGroupState::Active, Some(underlay_id), None);
        let active_underlay = underlay(underlay_id, None);
        let mut active_members = members(None);
        active_members[1].inner.sources = String::from("10.0.0.2");

        let creating = group(MulticastGroupState::Creating, None, None);

        let removed =
            group(MulticastGroupState::Deleting, Some(underlay_id), deleted);
        let removed_underlay = underlay(underlay_id, deleted);
        let removed_members = members(deleted);

        let asm_ip: std::net::IpAddr = "224.1.2.4".parse().unwrap();
        let mut asm = group(MulticastGroupState::Active, None, None);
        asm.multicast_ip = asm_ip.into();
        asm.tag = Some(format!("{}:{asm_ip}", asm.identity.id));
        let mut asm_members = members(None);
        for row in &mut asm_members {
            row.inner.multicast_ip = asm_ip;
        }
        asm_members[1].inner.sources = String::from("10.0.0.2");
        let asm_sources = source_filter_display(Some(&SourceFilterState {
            specific_sources: ["10.0.0.1", "10.0.0.2"]
                .iter()
                .map(|s| s.parse().unwrap())
                .collect(),
            has_any_source_member: false,
        }));

        let cases = [
            (
                "active group with underlay and members",
                MulticastInfoDisplay {
                    group: &active,
                    underlay: Some(&active_underlay),
                    pool_name: "mcast-pool",
                    source_ips: "10.0.0.1,10.0.0.2",
                    members: &active_members,
                    include_deleted: false,
                },
            ),
            (
                "creating group without underlay or members",
                MulticastInfoDisplay {
                    group: &creating,
                    underlay: None,
                    pool_name: "mcast-pool",
                    source_ips: "-",
                    members: &[],
                    include_deleted: false,
                },
            ),
            (
                "deleted group with --include-deleted",
                MulticastInfoDisplay {
                    group: &removed,
                    underlay: Some(&removed_underlay),
                    pool_name: "mcast-pool",
                    source_ips: "10.0.0.1,10.0.0.2",
                    members: &removed_members,
                    include_deleted: true,
                },
            ),
            (
                "ASM group whose members all list sources",
                MulticastInfoDisplay {
                    group: &asm,
                    underlay: None,
                    pool_name: "mcast-pool",
                    source_ips: &asm_sources,
                    members: &asm_members,
                    include_deleted: false,
                },
            ),
        ];

        let mut snapshot = OutputSnapshot::new();
        for (name, display) in cases {
            snapshot.push(format_args!("CASE: {name}"), display, "");
        }
        snapshot.assert_contents("tests/output/multicast-info.txt");
    }

    fn group_row(
        id: &str,
        name: &str,
        state: MulticastGroupState,
        members: String,
        time_deleted: Option<DateTime<Utc>>,
    ) -> WithDeleted<MulticastGroupRow> {
        WithDeleted {
            inner: MulticastGroupRow {
                id: id.parse().unwrap(),
                name: String::from(name),
                state,
                multicast_ip: "232.1.2.3".parse().unwrap(),
                range: RANGE_SSM,
                underlay_ip: Some("ff04::e801:203".parse().unwrap()),
                sources: String::from("10.0.0.1"),
                members,
                vni: u32::from(external::Vni::DEFAULT_MULTICAST_VNI),
                created: time("2026-01-02T03:04:05Z"),
            },
            time_deleted,
        }
    }

    fn range_row(
        id: &str,
        name: &str,
        ip: &str,
        sources: &[&str],
        any_source_member: bool,
    ) -> WithDeleted<MulticastGroupRow> {
        let multicast_ip: std::net::IpAddr = ip.parse().unwrap();
        let state = SourceFilterState {
            specific_sources: sources
                .iter()
                .map(|s| s.parse().unwrap())
                .collect(),
            has_any_source_member: any_source_member,
        };
        WithDeleted {
            inner: MulticastGroupRow {
                id: id.parse().unwrap(),
                name: String::from(name),
                state: MulticastGroupState::Active,
                multicast_ip,
                range: range_label(multicast_ip),
                underlay_ip: None,
                sources: source_filter_display(Some(&state)),
                members: String::from("-"),
                vni: u32::from(external::Vni::DEFAULT_MULTICAST_VNI),
                created: time("2026-01-02T03:04:05Z"),
            },
            time_deleted: None,
        }
    }

    #[test]
    fn test_multicast_groups_display() {
        let live = [
            member_entry("web-0", "BRM42220001", false),
            member_entry("web-1", "-", false),
        ];
        let with_deleted = [
            member_entry("probe-0", "-", true),
            member_entry("web-0", "BRM42220001", false),
        ];

        let listed = [
            group_row(
                "1b2b4c4f-42a5-4e3c-8b28-a1c1f2f3a0b1",
                "group-a",
                MulticastGroupState::Active,
                members_cell(Some(&live), false),
                None,
            ),
            group_row(
                "2c3d4e5f-6a7b-4c8d-9e0f-1a2b3c4d5e6f",
                "group-b",
                MulticastGroupState::Creating,
                members_cell(None, false),
                None,
            ),
        ];
        let trunc = [
            group_row(
                "1b2b4c4f-42a5-4e3c-8b28-a1c1f2f3a0b1",
                "group-a",
                MulticastGroupState::Active,
                members_cell(Some(&live), true),
                None,
            ),
            group_row(
                "2c3d4e5f-6a7b-4c8d-9e0f-1a2b3c4d5e6f",
                "group-b",
                MulticastGroupState::Active,
                members_cell(None, true),
                None,
            ),
        ];
        let deleted = [
            group_row(
                "1b2b4c4f-42a5-4e3c-8b28-a1c1f2f3a0b1",
                "group-a",
                MulticastGroupState::Active,
                members_cell(Some(&with_deleted), false),
                None,
            ),
            group_row(
                "2c3d4e5f-6a7b-4c8d-9e0f-1a2b3c4d5e6f",
                "group-b",
                MulticastGroupState::Deleting,
                members_cell(None, false),
                Some(time("2026-01-03T00:00:00Z")),
            ),
        ];
        let ranges = [
            range_row(
                "3a4b5c6d-7e8f-4a0b-9c1d-2e3f4a5b6c7d",
                "ssm-sources",
                "232.1.2.3",
                &["10.0.0.1", "10.0.0.2"],
                false,
            ),
            range_row(
                "4b5c6d7e-8f9a-4b1c-8d2e-3f4a5b6c7d8e",
                "asm-any",
                "224.1.2.3",
                &[],
                true,
            ),
            range_row(
                "5c6d7e8f-9a0b-4c2d-9e3f-4a5b6c7d8e9f",
                "asm-sources",
                "224.1.2.4",
                &["10.1.0.1", "10.1.0.2"],
                false,
            ),
            range_row(
                "6d7e8f9a-0b1c-4d3e-8f4a-5b6c7d8e9fa0",
                "asm-mixed",
                "224.1.2.5",
                &["10.1.0.1"],
                true,
            ),
            range_row(
                "7e8f9a0b-1c2d-4e4f-9a5b-6c7d8e9fa0b1",
                "asm-v6",
                "ff0e::1:2",
                &["fd00::1"],
                false,
            ),
        ];

        let cases = [
            (
                "groups with and without members",
                MulticastGroupsDisplay {
                    rows: &listed,
                    include_deleted: false,
                },
            ),
            (
                "member fetch hit --fetch-limit",
                MulticastGroupsDisplay { rows: &trunc, include_deleted: false },
            ),
            (
                "deleted group and member with --include-deleted",
                MulticastGroupsDisplay {
                    rows: &deleted,
                    include_deleted: true,
                },
            ),
            (
                "ASM and SSM source summaries",
                MulticastGroupsDisplay {
                    rows: &ranges,
                    include_deleted: false,
                },
            ),
        ];

        let mut snapshot = OutputSnapshot::new();
        for (name, display) in cases {
            snapshot.push(format_args!("CASE: {name}"), display, "");
        }
        snapshot.assert_contents("tests/output/multicast-groups.txt");
    }

    #[test]
    fn test_trunc_at_limit() {
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        let c = Uuid::from_u128(3);
        let limit = NonZeroU32::new(3).unwrap();

        let mut under = vec![a, a];
        assert_eq!(trunc_at_limit(&mut under, limit, |id| *id), None);
        assert_eq!(under, [a, a]);

        let mut at = vec![a, a, b];
        assert_eq!(trunc_at_limit(&mut at, limit, |id| *id), None);
        assert_eq!(at, [a, a, b]);

        let mut mid_group = vec![a, b, b, b];
        assert_eq!(trunc_at_limit(&mut mid_group, limit, |id| *id), Some(b));
        assert_eq!(mid_group, [a, b, b]);

        let mut next_group = vec![a, a, b, c];
        assert_eq!(trunc_at_limit(&mut next_group, limit, |id| *id), Some(c));
        assert_eq!(next_group, [a, a, b]);

        assert!(!past_boundary(a, Some(b)));
        assert!(past_boundary(b, Some(b)));
        assert!(past_boundary(c, Some(b)));
        assert!(!past_boundary(c, None));
    }
}
