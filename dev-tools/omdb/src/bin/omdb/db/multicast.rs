// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db multicast` subcommands
//!
//! # Command Outputs
//!
//! ## `omdb db multicast pools`
//!
//! | Column        | Description                |
//! |---------------|----------------------------|
//! | POOL_ID       | Pool UUID                  |
//! | POOL_NAME     | Pool name                  |
//! | FIRST_ADDRESS | Range start IP             |
//! | LAST_ADDRESS  | Range end IP               |
//! | CREATED       | Creation timestamp         |
//!
//! ## `omdb db multicast groups`
//!
//! | Column       | Description                          |
//! |--------------|--------------------------------------|
//! | ID           | Group UUID                           |
//! | NAME         | Group name                           |
//! | STATE        | Group state ("Active"/"Creating")    |
//! | MULTICAST_IP | Allocated multicast IP               |
//! | RANGE        | ASM or SSM based on IP range         |
//! | UNDERLAY_IP  | Underlay group IP (blank if none)    |
//! | SOURCES      | Union of member source IPs, or "-"   |
//! | MEMBERS      | Comma-separated "instance@sled" list |
//! | VNI          | Virtual network ID                   |
//! | CREATED      | Creation timestamp                   |
//!
//! Filters: `--state`, `--pool`
//!
//! ## `omdb db multicast members`
//!
//! | Column       | Description                              |
//! |--------------|------------------------------------------|
//! | ID           | Member UUID                              |
//! | GROUP_NAME   | Parent group name                        |
//! | PARENT_ID    | Instance UUID                            |
//! | STATE        | Member state ("Joining"/"Joined"/"Left") |
//! | MULTICAST_IP | Group multicast IP                       |
//! | SOURCES      | SSM source IPs, or "-" for ASM           |
//! | SLED_ID      | Assigned sled UUID (blank if none)       |
//! | CREATED      | Creation timestamp                       |
//!
//! Filters: `--group-id`, `--group-ip`, `--group-name`, `--state`,
//!          `--sled-id`, `--source-ip`
//!
//! ## `omdb db multicast info`
//!
//! Detailed view of a single group with sections:
//!
//! **MULTICAST GROUP**
//! - id, name, state, multicast_ip, vni, source_ips
//! - ip_pool (name + ID), underlay_group, tag, created
//!
//! **UNDERLAY GROUP** (if present)
//! - id, multicast_ip, tag, created
//!
//! **MEMBERS** (table)
//!
//! | Column       | Description                    |
//! |--------------|--------------------------------|
//! | ID           | Member UUID                    |
//! | INSTANCE     | Instance name                  |
//! | STATE        | Member state                   |
//! | MULTICAST_IP | Group multicast IP             |
//! | SOURCES      | SSM source IPs, or "-" for ASM |
//! | SLED         | Sled serial number, or "-"     |
//! | CREATED      | Creation timestamp             |
//!
//! Lookup: `--group-id`, `--ip`, `--name` (exactly one required)

use std::collections::{BTreeSet, HashMap};

use anyhow::Context;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{DateTime, Utc};
use clap::builder::{PossibleValue, PossibleValuesParser, TypedValueParser};
use clap::{Args, Subcommand};
use diesel::prelude::*;
use tabled::Tabled;
use uuid::Uuid;

use nexus_db_model::{
    ExternalMulticastGroup, IpPool, IpPoolRange, IpPoolType,
    MulticastGroupMember, MulticastGroupMemberState, MulticastGroupState,
    UnderlayMulticastGroup,
};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Resource;
use omicron_common::address::is_ssm_address;
use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid, SledUuid};

use crate::db::{DbFetchOptions, check_limit};
use crate::helpers::{datetime_rfc3339_concise, display_option_blank};

// Display labels for multicast address range classification
const RANGE_SSM: &str = "SSM";
const RANGE_ASM: &str = "ASM";

/// `omdb db multicast` subcommand
#[derive(Debug, Args, Clone)]
pub(super) struct MulticastArgs {
    #[command(subcommand)]
    pub command: MulticastCommands,
}

#[derive(Debug, Subcommand, Clone)]
pub(super) enum MulticastCommands {
    /// List all multicast groups.
    ///
    /// Shows ID, name, state, multicast IP, address range type (ASM/SSM),
    /// underlay IP, source IPs union, VNI, and creation time.
    #[clap(alias = "ls")]
    Groups(MulticastGroupsArgs),

    /// List all multicast group members.
    ///
    /// Shows member ID, group name, parent instance ID, state, multicast IP,
    /// source IPs, sled ID, and creation time.
    Members(MulticastMembersArgs),

    /// List multicast IP pools and their ranges.
    ///
    /// Shows pool ID, name, first/last addresses, and creation time.
    Pools,

    /// Get detailed info for a multicast group.
    ///
    /// Shows group details, associated underlay group, and all members.
    #[clap(alias = "show")]
    Info(MulticastInfoArgs),
}

#[derive(Debug, Args, Clone)]
pub(super) struct MulticastGroupsArgs {
    /// Filter by state
    #[arg(
        long,
        ignore_case = true,
        value_parser = PossibleValuesParser::new(
            MulticastGroupState::ALL_STATES
                .iter()
                .map(|v| PossibleValue::new(v.label()))
        ).try_map(|s| s.parse::<MulticastGroupState>()),
    )]
    state: Option<MulticastGroupState>,
    /// Filter by pool name
    #[arg(long)]
    pool: Option<String>,
}

#[derive(Debug, Args, Clone)]
pub(super) struct MulticastMembersArgs {
    /// Filter by group ID
    #[arg(long)]
    group_id: Option<Uuid>,
    /// Filter by group IP address (e.g., 239.1.2.3)
    #[arg(long)]
    group_ip: Option<std::net::IpAddr>,
    /// Filter by group name
    #[arg(long)]
    group_name: Option<String>,
    /// Filter by state
    #[arg(
        long,
        ignore_case = true,
        value_parser = PossibleValuesParser::new(
            MulticastGroupMemberState::ALL_STATES
                .iter()
                .map(|v| PossibleValue::new(v.label()))
        ).try_map(|s| s.parse::<MulticastGroupMemberState>()),
    )]
    state: Option<MulticastGroupMemberState>,
    /// Filter by sled ID
    #[arg(long)]
    sled_id: Option<SledUuid>,
    /// Filter by source IP (members subscribed to this source)
    #[arg(long)]
    source_ip: Option<std::net::IpAddr>,
}

#[derive(Debug, Args, Clone)]
#[group(required = true, multiple = false)]
pub(super) struct MulticastInfoArgs {
    /// Multicast group ID
    #[arg(long)]
    group_id: Option<Uuid>,
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
    /// Members formatted as "inst_1@sled, ..., inst_n@sled"
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
    instance: String,
    state: MulticastGroupMemberState,
    multicast_ip: std::net::IpAddr,
    /// Source IPs for source filtering ("-" = any source)
    sources: String,
    sled: String,
    #[tabled(display_with = "datetime_rfc3339_concise")]
    created: DateTime<Utc>,
}

// Build output combining pools and ranges
#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct MulticastPoolRow {
    pool_id: Uuid,
    pool_name: String,
    first_address: std::net::IpAddr,
    last_address: std::net::IpAddr,
    #[tabled(display_with = "datetime_rfc3339_concise")]
    created: chrono::DateTime<Utc>,
}

pub(super) async fn cmd_db_multicast_groups(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &MulticastGroupsArgs,
) -> Result<(), anyhow::Error> {
    use nexus_db_schema::schema::instance::dsl as instance_dsl;
    use nexus_db_schema::schema::ip_pool::dsl as pool_dsl;
    use nexus_db_schema::schema::multicast_group::dsl;
    use nexus_db_schema::schema::multicast_group_member::dsl as member_dsl;
    use nexus_db_schema::schema::sled::dsl as sled_dsl;
    use nexus_db_schema::schema::underlay_multicast_group::dsl as underlay_dsl;

    let conn = datastore.pool_connection_for_tests().await?;

    let mut query = dsl::multicast_group.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());
    }
    if let Some(state) = args.state {
        query = query.filter(dsl::state.eq(state));
    }
    if let Some(ref pool_name) = args.pool {
        let pool_id: Uuid = pool_dsl::ip_pool
            .filter(pool_dsl::name.eq(pool_name.clone()))
            .filter(pool_dsl::time_deleted.is_null())
            .select(pool_dsl::id)
            .first_async(&*conn)
            .await
            .with_context(|| {
                format!("no pool found with name '{pool_name}'")
            })?;
        query = query.filter(dsl::ip_pool_id.eq(pool_id));
    }

    let groups: Vec<ExternalMulticastGroup> = query
        .order_by(dsl::time_created.desc())
        .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
        .select(ExternalMulticastGroup::as_select())
        .get_results_async(&*conn)
        .await?;

    check_limit(&groups, fetch_opts.fetch_limit, || {
        String::from("listing multicast groups")
    });

    // Batch lookup underlay IPs for groups
    let underlay_ids: Vec<Uuid> =
        groups.iter().filter_map(|group| group.underlay_group_id).collect();
    let underlay_map: HashMap<Uuid, ipnetwork::IpNetwork> =
        if underlay_ids.is_empty() {
            HashMap::new()
        } else {
            underlay_dsl::underlay_multicast_group
                .filter(underlay_dsl::id.eq_any(underlay_ids))
                .select((underlay_dsl::id, underlay_dsl::multicast_ip))
                .get_results_async::<(Uuid, ipnetwork::IpNetwork)>(&*conn)
                .await
                .unwrap_or_default()
                .into_iter()
                .collect()
        };

    // Derive source IPs union from members for each group
    // Source IPs are stored per-member; groups show the union of all member sources
    let group_uuids: Vec<MulticastGroupUuid> = groups
        .iter()
        .map(|group| MulticastGroupUuid::from_untyped_uuid(group.identity.id))
        .collect();
    let source_ips_raw = datastore
        .multicast_groups_source_ips_union(opctx, &group_uuids)
        .await
        .context("failed to fetch source IPs union")?;
    let source_ips_map: HashMap<Uuid, BTreeSet<std::net::IpAddr>> =
        source_ips_raw
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().collect()))
            .collect();

    // Fetch members for all groups
    let group_ids: Vec<Uuid> = groups.iter().map(|g| g.identity.id).collect();
    let members: Vec<MulticastGroupMember> = if group_ids.is_empty() {
        Vec::new()
    } else {
        let mut mq = member_dsl::multicast_group_member
            .filter(member_dsl::external_group_id.eq_any(group_ids))
            .into_boxed();
        if !fetch_opts.include_deleted {
            mq = mq.filter(member_dsl::time_deleted.is_null());
        }
        mq.select(MulticastGroupMember::as_select())
            .get_results_async(&*conn)
            .await
            .unwrap_or_default()
    };

    // Batch lookup instance names
    let parent_ids: Vec<Uuid> = members.iter().map(|m| m.parent_id).collect();
    let instance_names: HashMap<Uuid, String> = if parent_ids.is_empty() {
        HashMap::new()
    } else {
        instance_dsl::instance
            .filter(instance_dsl::id.eq_any(parent_ids))
            .select((instance_dsl::id, instance_dsl::name))
            .get_results_async::<(Uuid, String)>(&*conn)
            .await
            .unwrap_or_default()
            .into_iter()
            .collect()
    };

    // Batch lookup sled serials
    let sled_ids: Vec<Uuid> = members
        .iter()
        .filter_map(|m| m.sled_id.map(|s| s.into_untyped_uuid()))
        .collect();
    let sled_serials: HashMap<Uuid, String> = if sled_ids.is_empty() {
        HashMap::new()
    } else {
        sled_dsl::sled
            .filter(sled_dsl::id.eq_any(sled_ids))
            .select((sled_dsl::id, sled_dsl::serial_number))
            .get_results_async::<(Uuid, String)>(&*conn)
            .await
            .unwrap_or_default()
            .into_iter()
            .collect()
    };

    // Build group_id -> formatted members string
    let mut members_map: HashMap<Uuid, Vec<String>> = HashMap::new();
    for member in &members {
        let inst_name = instance_names
            .get(&member.parent_id)
            .cloned()
            .unwrap_or_else(|| member.parent_id.to_string());
        let sled_serial = member
            .sled_id
            .and_then(|s| sled_serials.get(&s.into_untyped_uuid()).cloned())
            .unwrap_or_else(|| "-".to_string());
        let formatted = format!("{inst_name}@{sled_serial}");
        members_map
            .entry(member.external_group_id)
            .or_default()
            .push(formatted);
    }

    let rows: Vec<MulticastGroupRow> = groups
        .into_iter()
        .map(|group| {
            let mcast_ip = group.multicast_ip.ip();
            let range =
                if is_ssm_address(mcast_ip) { RANGE_SSM } else { RANGE_ASM };
            // Format source IPs union (derived from members)
            let sources = source_ips_map
                .get(&group.identity.id)
                .filter(|source_ips| !source_ips.is_empty())
                .map(|source_ips| {
                    source_ips
                        .iter()
                        .map(|ip| ip.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_else(|| "-".to_string());
            let underlay_ip = group
                .underlay_group_id
                .and_then(|id| underlay_map.get(&id))
                .map(|ip| ip.ip());
            let members = members_map
                .get(&group.identity.id)
                .map(|v| v.join(", "))
                .unwrap_or_else(|| "-".to_string());
            MulticastGroupRow {
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
            }
        })
        .collect();

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{table}");

    Ok(())
}

pub(super) async fn cmd_db_multicast_members(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &MulticastMembersArgs,
) -> Result<(), anyhow::Error> {
    use nexus_db_schema::schema::multicast_group::dsl as group_dsl;
    use nexus_db_schema::schema::multicast_group_member::dsl;

    let conn = datastore.pool_connection_for_tests().await?;

    // Resolve group_ip or group_name to a group_id if specified
    let resolved_group_id = match (&args.group_ip, &args.group_name) {
        (Some(ip), _) => {
            let group: ExternalMulticastGroup = group_dsl::multicast_group
                .filter(group_dsl::time_deleted.is_null())
                .filter(
                    group_dsl::multicast_ip.eq(ipnetwork::IpNetwork::from(*ip)),
                )
                .select(ExternalMulticastGroup::as_select())
                .first_async(&*conn)
                .await
                .with_context(|| format!("no multicast group with IP {ip}"))?;
            Some(group.id())
        }
        (None, Some(name)) => {
            let group: ExternalMulticastGroup = group_dsl::multicast_group
                .filter(group_dsl::time_deleted.is_null())
                .filter(group_dsl::name.eq(name.clone()))
                .select(ExternalMulticastGroup::as_select())
                .first_async(&*conn)
                .await
                .with_context(|| {
                    format!("no multicast group with name '{name}'")
                })?;
            Some(group.id())
        }
        (None, None) => args.group_id,
    };

    let mut query = dsl::multicast_group_member.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());
    }
    if let Some(group_id) = resolved_group_id {
        query = query.filter(dsl::external_group_id.eq(group_id));
    }
    if let Some(state) = args.state {
        query = query.filter(dsl::state.eq(state));
    }
    if let Some(sled_id) = args.sled_id {
        query = query.filter(dsl::sled_id.eq(sled_id.into_untyped_uuid()));
    }
    if let Some(source_ip) = args.source_ip {
        let ip_network = ipnetwork::IpNetwork::from(source_ip);
        query = query.filter(dsl::source_ips.contains(vec![ip_network]));
    }

    let members: Vec<MulticastGroupMember> = query
        .order_by(dsl::time_created.desc())
        .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
        .select(MulticastGroupMember::as_select())
        .get_results_async(&*conn)
        .await?;

    check_limit(&members, fetch_opts.fetch_limit, || {
        String::from("listing multicast group members")
    });

    // Batch lookup group names
    let group_ids: Vec<Uuid> =
        members.iter().map(|member| member.external_group_id).collect();
    let group_names: HashMap<Uuid, String> = if group_ids.is_empty() {
        HashMap::new()
    } else {
        group_dsl::multicast_group
            .filter(group_dsl::id.eq_any(group_ids))
            .select((group_dsl::id, group_dsl::name))
            .get_results_async::<(Uuid, String)>(&*conn)
            .await
            .unwrap_or_default()
            .into_iter()
            .collect()
    };

    let rows: Vec<MulticastMemberRow> = members
        .into_iter()
        .map(|member| {
            let group_name = group_names
                .get(&member.external_group_id)
                .cloned()
                .unwrap_or_else(|| member.external_group_id.to_string());
            let sources = Some(&member.source_ips)
                .filter(|ips| !ips.is_empty())
                .map(|ips| {
                    ips.iter()
                        .map(|ip| ip.ip().to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_else(|| "-".to_string());
            MulticastMemberRow {
                id: member.id,
                group_name,
                parent_id: member.parent_id,
                state: member.state,
                multicast_ip: member.multicast_ip.ip(),
                sources,
                sled_id: member.sled_id.map(SledUuid::from),
                created: member.time_created,
            }
        })
        .collect();

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{table}");

    Ok(())
}

pub(super) async fn cmd_db_multicast_pools(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
) -> Result<(), anyhow::Error> {
    use nexus_db_schema::schema::ip_pool::dsl as pool_dsl;
    use nexus_db_schema::schema::ip_pool_range::dsl as range_dsl;

    let conn = datastore.pool_connection_for_tests().await?;

    // Get multicast pools
    let mut query = pool_dsl::ip_pool.into_boxed();
    query = query.filter(pool_dsl::pool_type.eq(IpPoolType::Multicast));
    if !fetch_opts.include_deleted {
        query = query.filter(pool_dsl::time_deleted.is_null());
    }

    let pools: Vec<IpPool> = query
        .order_by(pool_dsl::time_created.desc())
        .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
        .select(IpPool::as_select())
        .get_results_async(&*conn)
        .await?;

    check_limit(&pools, fetch_opts.fetch_limit, || {
        String::from("listing multicast pools")
    });

    if pools.is_empty() {
        println!("no multicast IP pools found");
        return Ok(());
    }

    // Get ranges for each pool
    let pool_ids: Vec<Uuid> = pools.iter().map(|pool| pool.id()).collect();

    let mut range_query = range_dsl::ip_pool_range.into_boxed();
    range_query =
        range_query.filter(range_dsl::ip_pool_id.eq_any(pool_ids.clone()));
    if !fetch_opts.include_deleted {
        range_query = range_query.filter(range_dsl::time_deleted.is_null());
    }

    let ranges: Vec<IpPoolRange> = range_query
        .order_by(range_dsl::first_address)
        .select(IpPoolRange::as_select())
        .get_results_async(&*conn)
        .await?;

    let pool_map: HashMap<Uuid, &IpPool> =
        pools.iter().map(|pool| (pool.id(), pool)).collect();

    let rows: Vec<MulticastPoolRow> = ranges
        .into_iter()
        .filter_map(|range| {
            pool_map.get(&range.ip_pool_id).map(|pool| MulticastPoolRow {
                pool_id: pool.id(),
                pool_name: pool.name().to_string(),
                first_address: range.first_address.ip(),
                last_address: range.last_address.ip(),
                created: range.time_created,
            })
        })
        .collect();

    if rows.is_empty() {
        println!("no multicast IP pool ranges found");
        return Ok(());
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{table}");

    Ok(())
}

pub(super) async fn cmd_db_multicast_info(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &MulticastInfoArgs,
) -> Result<(), anyhow::Error> {
    use nexus_db_schema::schema::instance::dsl as instance_dsl;
    use nexus_db_schema::schema::ip_pool::dsl as pool_dsl;
    use nexus_db_schema::schema::multicast_group::dsl as group_dsl;
    use nexus_db_schema::schema::multicast_group_member::dsl as member_dsl;
    use nexus_db_schema::schema::sled::dsl as sled_dsl;
    use nexus_db_schema::schema::underlay_multicast_group::dsl as underlay_dsl;

    let conn = datastore.pool_connection_for_tests().await?;

    // Find the group by ID, IP, or name, pairing filter with error message
    let (mut query, not_found_msg) =
        match (&args.group_id, &args.ip, &args.name) {
            (Some(id), _, _) => (
                group_dsl::multicast_group
                    .filter(group_dsl::id.eq(*id))
                    .into_boxed(),
                format!("no multicast group found with ID {id}"),
            ),
            (None, Some(ip), _) => (
                group_dsl::multicast_group
                    .filter(
                        group_dsl::multicast_ip
                            .eq(ipnetwork::IpNetwork::from(*ip)),
                    )
                    .into_boxed(),
                format!("no multicast group found with IP {ip}"),
            ),
            (None, None, Some(name)) => (
                group_dsl::multicast_group
                    .filter(group_dsl::name.eq(name.clone()))
                    .into_boxed(),
                format!("no multicast group found with name \"{name}\""),
            ),
            (None, None, None) => {
                anyhow::bail!("must specify --group-id, --ip, or --name")
            }
        };

    if !fetch_opts.include_deleted {
        query = query.filter(group_dsl::time_deleted.is_null());
    }

    // Fetch group with underlay in single query using LEFT JOIN
    let result: Option<(
        ExternalMulticastGroup,
        Option<UnderlayMulticastGroup>,
    )> =
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
            .optional()?;

    let (group, underlay) = match result {
        Some((grp, ulay)) => (grp, ulay),
        None => {
            println!("{not_found_msg}");
            return Ok(());
        }
    };

    // Look up the pool name
    let pool_name: String = pool_dsl::ip_pool
        .filter(pool_dsl::id.eq(group.ip_pool_id))
        .select(pool_dsl::name)
        .first_async(&*conn)
        .await
        .unwrap_or_else(|_| "<unknown>".into());

    // Derive source_ips union from members (source_ips is per-member, not per-group)
    let group_uuid = MulticastGroupUuid::from_untyped_uuid(group.identity.id);
    let source_ips_map = datastore
        .multicast_groups_source_ips_union(opctx, &[group_uuid])
        .await
        .context("failed to fetch source IPs union")?;
    let source_ips_display = source_ips_map
        .get(&group.identity.id)
        .filter(|ips| !ips.is_empty())
        .map(|ips| {
            ips.iter().map(|ip| ip.to_string()).collect::<Vec<_>>().join(",")
        })
        .unwrap_or_else(|| "-".to_string());

    // Print group details
    println!("MULTICAST GROUP");
    println!("  id:              {}", group.identity.id);
    println!("  name:            {}", group.identity.name);
    println!("  state:           {:?}", group.state);
    println!("  multicast_ip:    {}", group.multicast_ip);
    println!("  vni:             {}", u32::from(group.vni.0));
    println!("  source_ips:      {source_ips_display}");
    println!("  ip_pool:         {pool_name} ({})", group.ip_pool_id);
    println!("  underlay_group:  {:?}", group.underlay_group_id);
    println!("  tag:             {:?}", group.tag);
    println!("  created:         {}", group.identity.time_created);
    if let Some(deleted) = group.identity.time_deleted {
        println!("  deleted:         {deleted}");
    }

    // Display underlay group if present
    if let Some(underlay_group) = underlay {
        println!("\nUNDERLAY GROUP");
        println!("  id:              {}", underlay_group.id);
        println!("  multicast_ip:    {}", underlay_group.multicast_ip);
        println!("  tag:             {:?}", underlay_group.tag);
        println!("  created:         {}", underlay_group.time_created);
        if let Some(deleted) = underlay_group.time_deleted {
            println!("  deleted:         {deleted}");
        }
    }

    // Find members for this group
    let mut member_query = member_dsl::multicast_group_member.into_boxed();
    member_query = member_query
        .filter(member_dsl::external_group_id.eq(group.identity.id));
    if !fetch_opts.include_deleted {
        member_query = member_query.filter(member_dsl::time_deleted.is_null());
    }

    let members: Vec<MulticastGroupMember> = member_query
        .order_by(member_dsl::time_created.desc())
        .select(MulticastGroupMember::as_select())
        .get_results_async(&*conn)
        .await?;

    if members.is_empty() {
        println!("\nMEMBERS: (none)");
    } else {
        println!("\nMEMBERS ({}):", members.len());

        // Batch lookup instance names (parent_id references instances)
        let parent_ids: Vec<Uuid> =
            members.iter().map(|member| member.parent_id).collect();
        let instances: Vec<(Uuid, String)> = instance_dsl::instance
            .filter(instance_dsl::id.eq_any(parent_ids))
            .select((instance_dsl::id, instance_dsl::name))
            .get_results_async(&*conn)
            .await
            .unwrap_or_default();
        let instance_map: HashMap<Uuid, String> =
            instances.into_iter().collect();

        // Batch lookup sled serials
        let sled_ids: Vec<Uuid> = members
            .iter()
            .filter_map(|member| member.sled_id.map(|s| s.into_untyped_uuid()))
            .collect();
        let sleds: Vec<(Uuid, String)> = if sled_ids.is_empty() {
            Vec::new()
        } else {
            sled_dsl::sled
                .filter(sled_dsl::id.eq_any(sled_ids))
                .select((sled_dsl::id, sled_dsl::serial_number))
                .get_results_async(&*conn)
                .await
                .unwrap_or_default()
        };
        let sled_map: HashMap<Uuid, String> = sleds.into_iter().collect();

        let rows: Vec<MulticastInfoMemberRow> = members
            .into_iter()
            .map(|member| {
                let instance_name = instance_map
                    .get(&member.parent_id)
                    .cloned()
                    .unwrap_or_else(|| member.parent_id.to_string());
                let sled_serial = member
                    .sled_id
                    .and_then(|s| sled_map.get(&s.into_untyped_uuid()).cloned())
                    .unwrap_or_else(|| "-".to_string());
                let sources = Some(&member.source_ips)
                    .filter(|ips| !ips.is_empty())
                    .map(|ips| {
                        ips.iter()
                            .map(|ip| ip.ip().to_string())
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .unwrap_or_else(|| "-".to_string());
                MulticastInfoMemberRow {
                    id: member.id,
                    instance: instance_name,
                    state: member.state,
                    multicast_ip: member.multicast_ip.ip(),
                    sources,
                    sled: sled_serial,
                    created: member.time_created,
                }
            })
            .collect();

        let table = tabled::Table::new(rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();

        println!("{table}");
    }

    Ok(())
}
