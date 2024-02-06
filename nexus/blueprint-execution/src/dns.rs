// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Propagates internal DNS changes in a given blueprint

use crate::Sled;
use anyhow::ensure;
use internal_dns::DnsConfigBuilder;
use internal_dns::ServiceName;
use nexus_db_model::DnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::DnsVersionUpdateBuilder;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::OmicronZoneType;
use nexus_types::internal_api::params::DnsConfigParams;
use nexus_types::internal_api::params::DnsRecord;
use omicron_common::address::get_switch_zone_address;
use omicron_common::address::CLICKHOUSE_KEEPER_PORT;
use omicron_common::address::CLICKHOUSE_PORT;
use omicron_common::address::COCKROACH_PORT;
use omicron_common::address::CRUCIBLE_PANTRY_PORT;
use omicron_common::address::CRUCIBLE_PORT;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::DNS_HTTP_PORT;
use omicron_common::address::MGD_PORT;
use omicron_common::address::MGS_PORT;
use omicron_common::address::NEXUS_INTERNAL_PORT;
use omicron_common::address::NTP_PORT;
use omicron_common::address::OXIMETER_PORT;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::InternalContext;
use slog::{debug, info};
use std::collections::BTreeMap;
use std::collections::HashMap;
use uuid::Uuid;

pub async fn deploy_dns<S>(
    opctx: &OpContext,
    datastore: &DataStore,
    creator: S,
    blueprint: &Blueprint,
    sleds_by_id: &BTreeMap<Uuid, Sled>,
) -> Result<(), Error>
where
    String: From<S>,
{
    // First, fetch the current DNS config.
    let dns_config_current = datastore
        .dns_config_read(opctx, DnsGroup::Internal)
        .await
        .internal_context("reading current DNS")?;

    // We could check here that the DNS version we found isn't newer than when
    // the blueprint was generated.  But we have to check later when we try to
    // update the database anyway.  And we're not wasting much effort allowing
    // this proceed for now.  This way, we have only one code path for this and
    // we know it's being hit when we exercise this condition.

    // Next, construct the DNS config represented by the blueprint.
    let dns_config_blueprint = blueprint_dns_config(blueprint, sleds_by_id);

    // Diff these configurations and use the result to prepare an update.
    let comment = format!("blueprint {} ({})", blueprint.id, blueprint.comment);
    let mut update = DnsVersionUpdateBuilder::new(
        DnsGroup::Internal,
        comment,
        String::from(creator),
    );

    let diff = DnsDiff::new(&dns_config_current, &dns_config_blueprint)
        .map_err(|e| Error::internal_error(&format!("{:#}", e)))?;
    if !diff.is_empty() {
        info!(opctx.log, "blueprint execution: DNS: no changes");
        return Ok(());
    }

    for (name, new_records) in diff.names_added() {
        debug!(
            opctx.log,
            "blueprint dns update: adding name";
            "name" => name,
            "new_records" => ?new_records,
        );
        update.add_name(
            name.to_string(),
            new_records.into_iter().cloned().collect(),
        )?;
    }

    for (name, old_records) in diff.names_removed() {
        debug!(
            opctx.log,
            "blueprint dns update: removing name";
            "name" => name,
            "old_records" => ?old_records,
        );
        update.remove_name(name.to_string())?;
    }

    for (name, old_records, new_records) in diff.names_changed() {
        debug!(
            opctx.log,
            "blueprint dns update: updating name";
            "name" => name,
            "old_records" => ?old_records,
            "new_records" => ?new_records,
        );
        update.remove_name(name.to_string())?;
        update.add_name(
            name.to_string(),
            new_records.into_iter().cloned().collect(),
        )?;
    }

    info!(
        opctx.log,
        "blueprint execution: DNS: attempting to update from generation {}",
        dns_config_blueprint.generation,
    );
    let generation_u32 = u32::try_from(dns_config_blueprint.generation)
        .map_err(|e| {
            Error::internal_error(&format!(
                "internal DNS generation got too large: {}",
                e,
            ))
        })?;
    let generation =
        nexus_db_model::Generation::from(Generation::from(generation_u32));
    datastore.dns_update_from_version(opctx, update, generation).await
}

pub fn blueprint_dns_config(
    blueprint: &Blueprint,
    sleds_by_id: &BTreeMap<Uuid, Sled>,
) -> DnsConfigParams {
    // The DNS names configured here should match what RSS configures for the
    // same zones.  It's tricky to have RSS share the same code because it uses
    // Sled Agent's _internal_ `OmicronZoneConfig` (and friends), whereas we're
    // using `sled-agent-client`'s version of that type.  However, the
    // DnsConfigBuilder's interface is high-level enough that it handles most of
    // the details.
    let mut dns_builder = DnsConfigBuilder::new();

    // The code below assumes that all zones are using the default port numbers.
    // That should be true, as those are the only ports ever used today.
    // In an ideal world, the correct port would be pulled out of the
    // `OmicronZoneType` variant instead.  Although that information is present,
    // it's irritatingly non-trivial to do right now because SocketAddrs are
    // represented as strings, so we'd need to parse all of them and handle all
    // the errors, even though they should never happen.
    // See oxidecomputer/omicron#4988.
    for (_, omicron_zone) in blueprint.all_omicron_zones() {
        let (service_name, port) = match omicron_zone.zone_type {
            OmicronZoneType::BoundaryNtp { .. } => {
                (ServiceName::BoundaryNtp, NTP_PORT)
            }
            OmicronZoneType::InternalNtp { .. } => {
                (ServiceName::InternalNtp, NTP_PORT)
            }
            OmicronZoneType::Clickhouse { .. } => {
                (ServiceName::Clickhouse, CLICKHOUSE_PORT)
            }
            OmicronZoneType::ClickhouseKeeper { .. } => {
                (ServiceName::ClickhouseKeeper, CLICKHOUSE_KEEPER_PORT)
            }
            OmicronZoneType::CockroachDb { .. } => {
                (ServiceName::Cockroach, COCKROACH_PORT)
            }
            OmicronZoneType::Nexus { .. } => {
                (ServiceName::Nexus, NEXUS_INTERNAL_PORT)
            }
            OmicronZoneType::Crucible { .. } => {
                (ServiceName::Crucible(omicron_zone.id), CRUCIBLE_PORT)
            }
            OmicronZoneType::CruciblePantry { .. } => {
                (ServiceName::CruciblePantry, CRUCIBLE_PANTRY_PORT)
            }
            OmicronZoneType::Oximeter { .. } => {
                (ServiceName::Oximeter, OXIMETER_PORT)
            }
            OmicronZoneType::ExternalDns { .. } => {
                (ServiceName::ExternalDns, DNS_HTTP_PORT)
            }
            OmicronZoneType::InternalDns { .. } => {
                (ServiceName::InternalDns, DNS_HTTP_PORT)
            }
        };

        // This unwrap is safe because this function only fails if we provide
        // the same zone id twice, which should not be possible here.
        dns_builder
            .host_zone_with_one_backend(
                omicron_zone.id,
                omicron_zone.underlay_address,
                service_name,
                port,
            )
            .unwrap();
    }

    let scrimlets = sleds_by_id.values().filter(|sled| sled.is_scrimlet);
    for scrimlet in scrimlets {
        let sled_subnet = scrimlet.subnet();
        let switch_zone_ip = get_switch_zone_address(sled_subnet);
        // unwrap(): see above.
        dns_builder
            .host_zone_switch(
                scrimlet.id,
                switch_zone_ip,
                DENDRITE_PORT,
                MGS_PORT,
                MGD_PORT,
            )
            .unwrap();
    }

    dns_builder.generation(blueprint.internal_dns_version.next());
    dns_builder.build()
}

type DnsRecords = HashMap<String, Vec<DnsRecord>>;
pub struct DnsDiff<'a> {
    left: &'a DnsRecords,
    right: &'a DnsRecords,
}

impl<'a> DnsDiff<'a> {
    pub fn new(
        left: &'a DnsConfigParams,
        right: &'a DnsConfigParams,
    ) -> Result<DnsDiff<'a>, anyhow::Error> {
        let left_zone = {
            ensure!(
                left.zones.len() == 1,
                "left side of diff: expected exactly one \
                DNS zone, but found {}",
                left.zones.len(),
            );

            &left.zones[0]
        };

        let right_zone = {
            ensure!(
                right.zones.len() == 1,
                "right side of diff: expected exactly one \
                DNS zone, but found {}",
                right.zones.len(),
            );

            &right.zones[0]
        };

        ensure!(
            left_zone.zone_name == right_zone.zone_name,
            "cannot compare DNS configuration from zones with different names: \
            {:?} vs. {:?}", left_zone.zone_name, right_zone.zone_name,
        );

        Ok(DnsDiff { left: &left_zone.records, right: &right_zone.records })
    }

    pub fn names_added(&self) -> impl Iterator<Item = (&str, &[DnsRecord])> {
        self.right
            .iter()
            .filter(|(k, _)| !self.left.contains_key(*k))
            .map(|(k, v)| (k.as_ref(), v.as_ref()))
    }

    pub fn names_removed(&self) -> impl Iterator<Item = (&str, &[DnsRecord])> {
        self.left
            .iter()
            .filter(|(k, _)| !self.right.contains_key(*k))
            .map(|(k, v)| (k.as_ref(), v.as_ref()))
    }

    pub fn names_changed(
        &self,
    ) -> impl Iterator<Item = (&str, &[DnsRecord], &[DnsRecord])> {
        self.left.iter().filter_map(|(k, v1)| match self.right.get(k) {
            Some(v2) if v1 != v2 => {
                Some((k.as_ref(), v1.as_ref(), v2.as_ref()))
            }
            _ => None,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.names_added().next().is_none()
            && self.names_removed().next().is_none()
            && self.names_changed().next().is_none()
    }
}
