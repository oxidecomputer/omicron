// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciliation of Omicron service NAT entries.
//!
//! Does not modify non-service NAT entries.

use daft::Diffable;
use dpd_client::Client;
use futures::Stream;
use futures::TryStreamExt;
use macaddr::MacAddr6;
use omicron_common::api::external::MacAddr;
use omicron_common::api::external::Vni;
use omicron_uuid_kinds::OmicronZoneUuid;
use sled_agent_types::system_networking::ServiceZoneNatEntries;
use sled_agent_types::system_networking::ServiceZoneNatEntry;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::num::NonZeroU32;

type DpdClientError = dpd_client::Error<dpd_client::types::Error>;

const SINGLE_REQUEST_LIMIT: Option<NonZeroU32> =
    Some(NonZeroU32::new(128).expect("128 is not 0"));

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DpdNatReconcilerStatusNatEntry {
    pub external_ip: IpAddr,
    pub first_port: u16,
    pub last_port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DpdNatReconcilerStatusNatEntryFailure {
    pub entry: DpdNatReconcilerStatusNatEntry,
    pub error: String,
}

#[derive(Debug, Clone)]
pub enum DpdNatReconcilerStatus {
    NoNatEntriesConfig,
    FailedReadingCurrentDpdNatEntries(String),
    InvalidSystemNetworkingConfig(String),
    Reconciled {
        unchanged: BTreeSet<OmicronZoneUuid>,
        remove_success: Vec<DpdNatReconcilerStatusNatEntry>,
        remove_failure: Vec<DpdNatReconcilerStatusNatEntryFailure>,
        create_success:
            BTreeMap<OmicronZoneUuid, DpdNatReconcilerStatusNatEntry>,
        create_failure:
            BTreeMap<OmicronZoneUuid, DpdNatReconcilerStatusNatEntryFailure>,
    },
}

impl slog::KV for DpdNatReconcilerStatus {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        let skipped_key = "nat-reconciler-skipped";
        match self {
            DpdNatReconcilerStatus::NoNatEntriesConfig => serializer.emit_str(
                skipped_key.into(),
                "no NAT entries present in config",
            ),
            DpdNatReconcilerStatus::FailedReadingCurrentDpdNatEntries(
                reason,
            ) => serializer.emit_arguments(
                skipped_key.into(),
                &format_args!(
                    "failed to read current entries from dpd: {reason}"
                ),
            ),
            DpdNatReconcilerStatus::InvalidSystemNetworkingConfig(reason) => {
                serializer.emit_arguments(
                    skipped_key.into(),
                    &format_args!("invalid system networking config: {reason}"),
                )
            }
            DpdNatReconcilerStatus::Reconciled {
                unchanged,
                remove_success,
                remove_failure,
                create_success,
                create_failure,
            } => {
                // Only show a summary count; we have individual log statements
                // for each create/remove.
                for (key, val) in [
                    ("nat-entries-unchanged", unchanged.len()),
                    ("nat-entries-successfully-removed", remove_success.len()),
                    ("nat-entries-failed-to-remove", remove_failure.len()),
                    ("nat-entries-successfully-created", create_success.len()),
                    ("nat-entries-failed-to-create", create_failure.len()),
                ] {
                    serializer.emit_usize(key.into(), val)?;
                }
                Ok(())
            }
        }
    }
}

pub(super) async fn reconcile(
    client: &Client,
    desired_nat_entries: &ServiceZoneNatEntries,
    log: &Logger,
) -> DpdNatReconcilerStatus {
    let dpd_current_entries = match CurrentDpdEntriesAssembler::assemble(
        client,
        desired_nat_entries,
        log,
    )
    .await
    {
        Ok(entries) => entries,
        Err(err) => {
            return DpdNatReconcilerStatus::FailedReadingCurrentDpdNatEntries(
                format!(
                    "failed to read current NAT entries from dpd: {}",
                    InlineErrorChain::new(&err),
                ),
            );
        }
    };

    let plan = match ReconciliationPlan::new(
        &dpd_current_entries,
        desired_nat_entries,
        log,
    ) {
        Ok(plan) => plan,
        Err(err) => {
            return DpdNatReconcilerStatus::InvalidSystemNetworkingConfig(err);
        }
    };

    apply_plan(client, plan, log).await
}

/// Apply the contents of `plan` to dpd via `client`.
///
/// This requires `plan.to_remove.len() + plan.to_create.len()` independent
/// calls to `dpd`. We do not short circuit on failure: we'll always attempt to
/// make every call required. This may not be the right choice, but some
/// arguments in favor:
///
/// * We'd like to eventually replace this with fewer calls, if we add different
///   APIs to `dpd` for applying NAT settings in bulk. (TODO: file an issue in
///   dendrite describing what we want.)
/// * In practice we expect the number of calls here to be small. On startup we
///   expect ~10 `to_create` calls (one for each service, which is typically 2
///   boundary NTP, 3 Nexus, and 1-5 external DNS), and for every reconciliation
///   attempt after that we expect 0-1 (either no changes, or a single new
///   service has been added; it's possible we'll see multiple, but unlikely
///   given we re-reconcile on every networking config change).
/// * We always want to report the status of every step described by `plan`, and
///   implementing stop-on-first-failure means we'd need to record a "didn't
///   attempt because of an earlier failure" status for some steps. That's
///   doable but annoying.
async fn apply_plan(
    client: &Client,
    plan: ReconciliationPlan,
    log: &Logger,
) -> DpdNatReconcilerStatus {
    let ReconciliationPlan { unchanged, to_remove, to_create } = plan;

    let mut remove_success = Vec::new();
    let mut remove_failure = Vec::new();
    for entry in to_remove {
        let result = match entry.target_ip {
            IpAddr::V4(ip) => {
                client.nat_ipv4_delete(&ip, entry.first_port).await
            }
            IpAddr::V6(ip) => {
                client.nat_ipv6_delete(&ip, entry.first_port).await
            }
        };

        match result {
            Ok(_) => {
                info!(log, "successfully removed NAT entry"; "entry" => ?entry);
                remove_success.push(entry.into());
            }
            Err(err) => {
                let err = InlineErrorChain::new(&err);
                warn!(
                    log, "failed to remove NAT entry";
                    "entry" => ?entry,
                    &err,
                );
                remove_failure.push(DpdNatReconcilerStatusNatEntryFailure {
                    entry: entry.into(),
                    error: err.to_string(),
                });
            }
        }
    }

    let mut create_success = BTreeMap::new();
    let mut create_failure = BTreeMap::new();
    for (zone_id, entry) in to_create {
        let nat_target = dpd_client::types::NatTarget {
            inner_mac: dpd_client::types::MacAddr {
                a: entry.nic_mac.into_array(),
            },
            internal_ip: entry.sled_underlay_ip,
            vni: entry.vni.as_u32().into(),
        };
        let result = match entry.target_ip {
            IpAddr::V4(ip) => {
                client
                    .nat_ipv4_create(
                        &ip,
                        entry.first_port,
                        entry.last_port,
                        &nat_target,
                    )
                    .await
            }
            IpAddr::V6(ip) => {
                client
                    .nat_ipv6_create(
                        &ip,
                        entry.first_port,
                        entry.last_port,
                        &nat_target,
                    )
                    .await
            }
        };

        match result {
            Ok(_) => {
                info!(
                    log, "successfully created NAT entry";
                    "zone_id" => %zone_id,
                    "entry" => ?entry,
                );
                create_success.insert(zone_id, entry.into());
            }
            Err(err) => {
                let err = InlineErrorChain::new(&err);
                warn!(
                    log, "failed to create NAT entry";
                    "zone_id" => %zone_id,
                    "entry" => ?entry,
                    &err,
                );
                create_failure.insert(
                    zone_id,
                    DpdNatReconcilerStatusNatEntryFailure {
                        entry: entry.into(),
                        error: err.to_string(),
                    },
                );
            }
        }
    }

    DpdNatReconcilerStatus::Reconciled {
        unchanged,
        remove_success,
        remove_failure,
        create_success,
        create_failure,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ReconciliationPlan {
    // Set of zones whose NAT entries already exist in DPD.
    unchanged: BTreeSet<OmicronZoneUuid>,

    // Set of NAT entries that exist in DPD but not our desired set; each of
    // these should be removed.
    to_remove: BTreeSet<NatEntry>,

    // Set of NAT entries that don't exist in DPD but are in our desired set;
    // each of these should be created.
    to_create: BTreeMap<OmicronZoneUuid, NatEntry>,
}

impl ReconciliationPlan {
    fn new(
        dpd_current_entries: &BTreeSet<NatEntry>,
        service_nat_entries: &ServiceZoneNatEntries,
        log: &Logger,
    ) -> Result<Self, String> {
        // Convert `service_nat_entries` into both a set of `NatEntry`s (so we
        // can diff it against `dpd_current_entries` via `daft`) and a map of
        // `NatEntry` back to the zone ID that needs it (for our status
        // reporting).
        let mut desired_nat_entries = BTreeSet::new();
        let mut nat_to_zone_id = BTreeMap::new();
        for entry in service_nat_entries.iter() {
            let zone_id = entry.zone_id;
            let entry = NatEntry::from(entry);

            // We should have no duplicates; if we do, we have two different
            // zones that want the same NAT entry. Refuse to reconcile. This
            // should be impossible by construction: `ServiceZoneNatEntries`
            // rejects overlapping entries. We double-check here in case that
            // changes or is buggy.
            if !desired_nat_entries.insert(entry) {
                let prev_zone_id = nat_to_zone_id
                    .get(&entry)
                    .expect("nat_to_zone_id has a value for every prior entry");
                return Err(format!(
                    "invalid SystemNetworkingConfig: zones {zone_id} and \
                     {prev_zone_id} want the same NAT entry: {entry:?}",
                ));
            }

            // We don't have to check again for duplicates here; we just
            // confirmed every `entry` is unique.
            nat_to_zone_id.insert(entry, zone_id);
        }

        let nat_entry_diff = dpd_current_entries.diff(&desired_nat_entries);

        let unchanged = nat_entry_diff
            .common
            .into_iter()
            .map(|entry| {
                nat_to_zone_id
                    .get(entry)
                    .copied()
                    .expect("nat_to_zone_id has a value for every common entry")
            })
            .collect::<BTreeSet<_>>();
        let to_remove = nat_entry_diff
            .removed
            .into_iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let to_create = nat_entry_diff
            .added
            .into_iter()
            .map(|entry| {
                let zone_id = nat_to_zone_id
                    .get(entry)
                    .copied()
                    .expect("nat_to_zone_id has a value for every added entry");
                (zone_id, *entry)
            })
            .collect::<BTreeMap<_, _>>();

        info!(
            log,
            "generated NAT reconciliation plan";
            "entries_unchanged" => unchanged.len(),
            "entries_to_remove" => to_remove.len(),
            "entries_to_create" => to_create.len(),
        );

        Ok(Self { unchanged, to_remove, to_create })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Diffable)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
struct NatEntry {
    sled_underlay_ip: Ipv6Addr,
    target_ip: IpAddr,
    first_port: u16,
    last_port: u16,
    nic_mac: MacAddr,
    vni: Vni,
}

impl From<NatEntry> for DpdNatReconcilerStatusNatEntry {
    fn from(value: NatEntry) -> Self {
        Self {
            external_ip: value.target_ip,
            first_port: value.first_port,
            last_port: value.last_port,
        }
    }
}

impl From<&'_ ServiceZoneNatEntry> for NatEntry {
    fn from(value: &'_ ServiceZoneNatEntry) -> Self {
        let (first_port, last_port) = value.kind.nat_port_range();
        Self {
            sled_underlay_ip: value.sled_underlay_ip,
            target_ip: value.kind.external_ip(),
            first_port,
            last_port,
            nic_mac: value.nic_mac,
            vni: value.vni,
        }
    }
}

struct BadVni;

impl TryFrom<dpd_client::types::Ipv4Nat> for NatEntry {
    type Error = BadVni;

    fn try_from(
        value: dpd_client::types::Ipv4Nat,
    ) -> Result<Self, Self::Error> {
        let vni = Vni::try_from(value.target.vni.0).map_err(|_| BadVni)?;
        Ok(Self {
            sled_underlay_ip: value.target.internal_ip,
            target_ip: value.external.into(),
            nic_mac: MacAddr6::from(value.target.inner_mac.a).into(),
            first_port: value.low,
            last_port: value.high,
            vni,
        })
    }
}

impl TryFrom<dpd_client::types::Ipv6Nat> for NatEntry {
    type Error = BadVni;

    fn try_from(
        value: dpd_client::types::Ipv6Nat,
    ) -> Result<Self, Self::Error> {
        let vni = Vni::try_from(value.target.vni.0).map_err(|_| BadVni)?;
        Ok(Self {
            sled_underlay_ip: value.target.internal_ip,
            target_ip: value.external.into(),
            nic_mac: MacAddr6::from(value.target.inner_mac.a).into(),
            first_port: value.low,
            last_port: value.high,
            vni,
        })
    }
}

struct CurrentDpdEntriesAssembler<'a> {
    client: &'a Client,
    service_vnis: BTreeSet<Vni>,
    current_entries: BTreeSet<NatEntry>,
}

struct RelevantEntryCount {
    service_vni: u64,
    non_service_vni: u64,
}

impl<'a> CurrentDpdEntriesAssembler<'a> {
    async fn assemble(
        client: &'a Client,
        desired_nat_entries: &ServiceZoneNatEntries,
        log: &Logger,
    ) -> Result<BTreeSet<NatEntry>, DpdClientError> {
        // We want to reconcile "all service NAT entries", but
        // `desired_nat_entries` only tells us what should exist, not what needs
        // to be removed. Build a set of the `Vni`s used in all our services; in
        // practice, we expect this to always be a set of length one containing
        // the `Vni::SERVICES_VNI` constant, because all services use that Vni.
        // But we've written this as we have so that if we decide to split each
        // kind of service into a separate Vni, this reconciliation still works.
        //
        // We assume there are never any service NAT entries in `dpd` that have
        // a Vni other than one of the ones present in `desired_nat_entries`.
        let service_vnis: BTreeSet<Vni> =
            desired_nat_entries.iter().map(|entry| entry.vni).collect();
        let mut builder =
            Self { client, service_vnis, current_entries: BTreeSet::new() };
        builder.read_ipv4_entries(log).await?;
        builder.read_ipv6_entries(log).await?;
        Ok(builder.current_entries)
    }

    async fn read_ipv4_entries(
        &mut self,
        log: &Logger,
    ) -> Result<(), DpdClientError> {
        let mut stream_addresses =
            self.client.nat_ipv4_addresses_list_stream(SINGLE_REQUEST_LIMIT);

        let mut counts =
            RelevantEntryCount { service_vni: 0, non_service_vni: 0 };

        while let Some(ip) = stream_addresses.try_next().await? {
            self.assemble_entries_from_stream(
                self.client.nat_ipv4_list_stream(&ip, SINGLE_REQUEST_LIMIT),
                &mut counts,
            )
            .await?;
        }

        info!(
            log,
            "finished fetching current ipv4 NAT entries from dpd";
            "service_nat_entries" => counts.service_vni,
            "non_service_nat_entries" => counts.non_service_vni,
            "service_vnis" => ?self.service_vnis,
        );

        Ok(())
    }

    async fn read_ipv6_entries(
        &mut self,
        log: &Logger,
    ) -> Result<(), DpdClientError> {
        let mut stream_addresses =
            self.client.nat_ipv6_addresses_list_stream(SINGLE_REQUEST_LIMIT);

        let mut counts =
            RelevantEntryCount { service_vni: 0, non_service_vni: 0 };

        while let Some(ip) = stream_addresses.try_next().await? {
            self.assemble_entries_from_stream(
                self.client.nat_ipv6_list_stream(&ip, SINGLE_REQUEST_LIMIT),
                &mut counts,
            )
            .await?;
        }

        info!(
            log,
            "finished fetching current ipv6 NAT entries from dpd";
            "service_nat_entries" => counts.service_vni,
            "non_service_nat_entries" => counts.non_service_vni,
            "service_vnis" => ?self.service_vnis,
        );

        Ok(())
    }

    async fn assemble_entries_from_stream<S, T>(
        &mut self,
        mut stream: S,
        counts: &mut RelevantEntryCount,
    ) -> Result<(), DpdClientError>
    where
        S: Stream<Item = Result<T, DpdClientError>> + Unpin,
        T: TryInto<NatEntry, Error = BadVni>,
    {
        while let Some(entry) = stream.try_next().await? {
            // The only way we can fail to convert a dpd `NatEntry` is if the
            // Vni from dpd isn't a valid omicron Vni (the `BadVni` in our
            // generic bound). This should never happen, but if it does, we know
            // this isn't an entry we care about: we're only looking for entries
            // that match our services' Vni(s).
            match entry.try_into() {
                Ok(entry) if self.service_vnis.contains(&entry.vni) => {
                    self.current_entries.insert(entry);
                    counts.service_vni += 1;
                }
                Ok(_) | Err(BadVni) => {
                    counts.non_service_vni += 1;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
