// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for building inventory [`Collection`] dynamically
//!
//! This separates the concerns of _collection_ (literally just fetching data
//! from sources like MGS) from assembling a representation of what was
//! collected.

use anyhow::Context;
use anyhow::anyhow;
use chrono::DateTime;
use chrono::Utc;
use clickhouse_admin_types::ClickhouseKeeperClusterMembership;
use cockroach_admin_types::NodeId;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpState;
use iddqd::IdOrdMap;
use nexus_sled_agent_shared::inventory::Baseboard;
use nexus_sled_agent_shared::inventory::Inventory;
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::Caboose;
use nexus_types::inventory::CabooseFound;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::CockroachStatus;
use nexus_types::inventory::Collection;
use nexus_types::inventory::HostPhase1ActiveSlot;
use nexus_types::inventory::HostPhase1FlashHash;
use nexus_types::inventory::InternalDnsGenerationStatus;
use nexus_types::inventory::RotPage;
use nexus_types::inventory::RotPageFound;
use nexus_types::inventory::RotPageWhich;
use nexus_types::inventory::RotState;
use nexus_types::inventory::ServiceProcessor;
use nexus_types::inventory::SledAgent;
use nexus_types::inventory::SpType;
use nexus_types::inventory::TimeSync;
use nexus_types::inventory::Zpool;
use omicron_cockroach_metrics::CockroachMetric;
use omicron_cockroach_metrics::PrometheusMetrics;
use omicron_common::disk::M2Slot;
use omicron_uuid_kinds::CollectionKind;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::hash::Hash;
use std::sync::Arc;
use thiserror::Error;
use tufaceous_artifact::ArtifactHash;
use typed_rng::TypedUuidRng;

/// Describes an operational error encountered during the collection process
///
/// Examples include a down MGS instance, failure to parse a response from some
/// other service, etc.  We currently don't need to distinguish these
/// programmatically.
#[derive(Debug, Error)]
pub struct InventoryError(#[from] anyhow::Error);

impl std::fmt::Display for InventoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}

/// Describes a mis-use of the [`CollectionBuilder`] object
///
/// Example: reporting information about a caboose when the caller has not
/// already reported information about the corresopnding baseboard.
///
/// Unlike `InventoryError`s, which can always happen in a real system, these
/// errors are not ever expected.  Ideally, all of these problems would be
/// compile errors.
#[derive(Debug, Error)]
pub struct CollectorBug(#[from] anyhow::Error);

impl std::fmt::Display for CollectorBug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}

/// Random generator of UUIDs for a [`CollectionBuilder`].
#[derive(Debug, Clone)]
pub struct CollectionBuilderRng {
    // We just generate one UUID for each collection.
    id_rng: TypedUuidRng<CollectionKind>,
}

impl CollectionBuilderRng {
    pub fn from_entropy() -> Self {
        CollectionBuilderRng { id_rng: TypedUuidRng::from_entropy() }
    }

    pub fn from_seed<H: Hash>(seed: H) -> Self {
        // Important to add some more bytes here, so that builders with the
        // same seed but different purposes don't end up with the same UUIDs.
        const SEED_EXTRA: &str = "collection-builder";
        CollectionBuilderRng {
            id_rng: TypedUuidRng::from_seed(seed, SEED_EXTRA),
        }
    }
}

/// Build an inventory [`Collection`]
///
/// This interface is oriented around the interfaces used by an actual
/// collector.  Where possible, it accepts types directly provided by the data
/// sources (e.g., `gateway_client`).
#[derive(Debug)]
pub struct CollectionBuilder {
    // For field documentation, see the corresponding fields in `Collection`.
    errors: Vec<InventoryError>,
    time_started: DateTime<Utc>,
    collector: String,
    baseboards: BTreeSet<Arc<BaseboardId>>,
    cabooses: BTreeSet<Arc<Caboose>>,
    rot_pages: BTreeSet<Arc<RotPage>>,
    sps: BTreeMap<Arc<BaseboardId>, ServiceProcessor>,
    host_phase_1_active_slots: BTreeMap<Arc<BaseboardId>, HostPhase1ActiveSlot>,
    host_phase_1_flash_hashes:
        BTreeMap<M2Slot, BTreeMap<Arc<BaseboardId>, HostPhase1FlashHash>>,
    rots: BTreeMap<Arc<BaseboardId>, RotState>,
    cabooses_found:
        BTreeMap<CabooseWhich, BTreeMap<Arc<BaseboardId>, CabooseFound>>,
    rot_pages_found:
        BTreeMap<RotPageWhich, BTreeMap<Arc<BaseboardId>, RotPageFound>>,
    sleds: IdOrdMap<SledAgent>,
    clickhouse_keeper_cluster_membership:
        BTreeSet<ClickhouseKeeperClusterMembership>,
    cockroach_status: BTreeMap<NodeId, CockroachStatus>,
    ntp_timesync: IdOrdMap<TimeSync>,
    internal_dns_generation_status: IdOrdMap<InternalDnsGenerationStatus>,
    // CollectionBuilderRng is taken by value, rather than passed in as a
    // mutable ref, to encourage a tree-like structure where each RNG is
    // generally independent.
    rng: CollectionBuilderRng,
}

impl CollectionBuilder {
    /// Start building a new `Collection`
    ///
    /// `collector` is an arbitrary string describing the agent that collected
    /// this data.  It's generally a Nexus instance uuid but it can be anything.
    /// It's just for debugging.
    pub fn new<S>(collector: S) -> Self
    where
        String: From<S>,
    {
        CollectionBuilder {
            errors: vec![],
            time_started: now_db_precision(),
            collector: String::from(collector),
            baseboards: BTreeSet::new(),
            cabooses: BTreeSet::new(),
            rot_pages: BTreeSet::new(),
            sps: BTreeMap::new(),
            host_phase_1_active_slots: BTreeMap::new(),
            host_phase_1_flash_hashes: BTreeMap::new(),
            rots: BTreeMap::new(),
            cabooses_found: BTreeMap::new(),
            rot_pages_found: BTreeMap::new(),
            sleds: IdOrdMap::new(),
            clickhouse_keeper_cluster_membership: BTreeSet::new(),
            cockroach_status: BTreeMap::new(),
            ntp_timesync: IdOrdMap::new(),
            internal_dns_generation_status: IdOrdMap::new(),
            rng: CollectionBuilderRng::from_entropy(),
        }
    }

    /// Assemble a complete `Collection` representation
    pub fn build(mut self) -> Collection {
        Collection {
            id: self.rng.id_rng.next(),
            errors: self.errors.into_iter().map(|e| e.to_string()).collect(),
            time_started: self.time_started,
            time_done: now_db_precision(),
            collector: self.collector,
            baseboards: self.baseboards,
            cabooses: self.cabooses,
            rot_pages: self.rot_pages,
            sps: self.sps,
            host_phase_1_active_slots: self.host_phase_1_active_slots,
            host_phase_1_flash_hashes: self.host_phase_1_flash_hashes,
            rots: self.rots,
            cabooses_found: self.cabooses_found,
            rot_pages_found: self.rot_pages_found,
            sled_agents: self.sleds,
            clickhouse_keeper_cluster_membership: self
                .clickhouse_keeper_cluster_membership,
            cockroach_status: self.cockroach_status,
            ntp_timesync: self.ntp_timesync,
            internal_dns_generation_status: self.internal_dns_generation_status,
        }
    }

    /// Within tests, set an RNG for deterministic results.
    ///
    /// This will ensure that tests that use this builder will produce the same
    /// results each time they are run.
    pub fn set_rng(&mut self, rng: CollectionBuilderRng) -> &mut Self {
        self.rng = rng;
        self
    }

    /// Record service processor state `sp_state` reported by MGS
    ///
    /// `sp_type` and `slot` identify which SP this was.
    ///
    /// `source` is an arbitrary string for debugging that describes the MGS
    /// that reported this data (generally a URL string).
    pub fn found_sp_state(
        &mut self,
        source: &str,
        sp_type: SpType,
        sp_slot: u16,
        sp_state: SpState,
    ) -> Option<Arc<BaseboardId>> {
        // Normalize the baseboard id: i.e., if we've seen this baseboard
        // before, use the same baseboard id record.  Otherwise, make a new one.
        let baseboard = Self::normalize_item(
            &mut self.baseboards,
            BaseboardId {
                serial_number: sp_state.serial_number,
                part_number: sp_state.model,
            },
        );

        // Separate the SP state into the SP-specific state and the RoT state,
        // if any.
        let now = now_db_precision();
        let _ = self.sps.entry(baseboard.clone()).or_insert_with(|| {
            ServiceProcessor {
                time_collected: now,
                source: source.to_owned(),

                sp_type,
                sp_slot,

                baseboard_revision: sp_state.revision,
                hubris_archive: sp_state.hubris_archive_id,
                power_state: sp_state.power_state,
            }
        });

        match sp_state.rot {
            gateway_client::types::RotState::V2 {
                active,
                pending_persistent_boot_preference,
                persistent_boot_preference,
                slot_a_sha3_256_digest,
                slot_b_sha3_256_digest,
                transient_boot_preference,
            } => {
                let _ =
                    self.rots.entry(baseboard.clone()).or_insert_with(|| {
                        RotState {
                            time_collected: now,
                            source: source.to_owned(),
                            active_slot: active,
                            persistent_boot_preference,
                            pending_persistent_boot_preference,
                            transient_boot_preference,
                            slot_a_sha3_256_digest,
                            slot_b_sha3_256_digest,
                            stage0_digest: None,
                            stage0next_digest: None,
                            slot_a_error: None,
                            slot_b_error: None,
                            stage0_error: None,
                            stage0next_error: None,
                        }
                    });
            }
            gateway_client::types::RotState::CommunicationFailed {
                message,
            } => {
                self.found_error(InventoryError::from(anyhow!(
                    "MGS {:?}: reading RoT state for {:?}: {}",
                    source,
                    baseboard,
                    message
                )));
            }
            gateway_client::types::RotState::V3 {
                active,
                pending_persistent_boot_preference,
                persistent_boot_preference,
                slot_a_fwid,
                slot_b_fwid,
                stage0_fwid,
                stage0next_fwid,
                transient_boot_preference,
                slot_a_error,
                slot_b_error,
                stage0_error,
                stage0next_error,
            } => {
                let _ =
                    self.rots.entry(baseboard.clone()).or_insert_with(|| {
                        RotState {
                            time_collected: now,
                            source: source.to_owned(),
                            active_slot: active,
                            persistent_boot_preference,
                            pending_persistent_boot_preference,
                            transient_boot_preference,
                            slot_a_sha3_256_digest: Some(slot_a_fwid),
                            slot_b_sha3_256_digest: Some(slot_b_fwid),
                            stage0_digest: Some(stage0_fwid),
                            stage0next_digest: Some(stage0next_fwid),
                            slot_a_error,
                            slot_b_error,
                            stage0_error,
                            stage0next_error,
                        }
                    });
            }
        }

        Some(baseboard)
    }

    /// Returns true if we already found the active host phase 1 flash slot for
    /// baseboard `baseboard`
    ///
    /// This is used to avoid requesting it multiple times (from multiple MGS
    /// instances).
    pub fn found_host_phase_1_active_slot_already(
        &self,
        baseboard: &BaseboardId,
    ) -> bool {
        self.host_phase_1_active_slots.contains_key(baseboard)
    }

    /// Record the given host phase 1 active slot found for the given baseboard
    ///
    /// The baseboard must previously have been reported using
    /// `found_sp_state()`.
    ///
    /// `source` is an arbitrary string for debugging that describes the MGS
    /// that reported this data (generally a URL string).
    pub fn found_host_phase_1_active_slot(
        &mut self,
        baseboard: &BaseboardId,
        source: &str,
        slot: M2Slot,
    ) -> Result<(), CollectorBug> {
        let (baseboard, _) =
            self.sps.get_key_value(baseboard).ok_or_else(|| {
                anyhow!(
                    "reporting host phase 1 active slot for unknown baseboard: \
                    {baseboard:?} ({slot:?})",
                )
            })?;
        if let Some(previous) = self.host_phase_1_active_slots.insert(
            baseboard.clone(),
            HostPhase1ActiveSlot {
                time_collected: now_db_precision(),
                source: source.to_owned(),
                slot,
            },
        ) {
            let error = if previous.slot == slot {
                anyhow!("reported multiple times (same value)")
            } else {
                anyhow!(
                    "reported host phase 1 flash hash \
                     (previously {}, now {slot})",
                    previous.slot,
                )
            };
            Err(CollectorBug::from(
                error.context(format!("baseboard {baseboard:?}")),
            ))
        } else {
            Ok(())
        }
    }

    /// Returns true if we already found the host phase 1 flash hash for `slot`
    /// for baseboard `baseboard`
    ///
    /// This is used to avoid requesting it multiple times (from multiple MGS
    /// instances).
    pub fn found_host_phase_1_flash_hash_already(
        &self,
        baseboard: &BaseboardId,
        slot: M2Slot,
    ) -> bool {
        self.host_phase_1_flash_hashes
            .get(&slot)
            .map(|map| map.contains_key(baseboard))
            .unwrap_or(false)
    }

    /// Record the given host phase 1 flash hash found for the given baseboard
    ///
    /// The baseboard must previously have been reported using
    /// `found_sp_state()`.
    ///
    /// `source` is an arbitrary string for debugging that describes the MGS
    /// that reported this data (generally a URL string).
    pub fn found_host_phase_1_flash_hash(
        &mut self,
        baseboard: &BaseboardId,
        slot: M2Slot,
        source: &str,
        hash: ArtifactHash,
    ) -> Result<(), CollectorBug> {
        let (baseboard, _) =
            self.sps.get_key_value(baseboard).ok_or_else(|| {
                anyhow!(
                    "reporting host phase 1 flash hash for unknown baseboard: \
                    {baseboard:?} ({slot:?}: {hash})",
                )
            })?;
        let by_id = self
            .host_phase_1_flash_hashes
            .entry(slot)
            .or_insert_with(BTreeMap::new);
        if let Some(previous) = by_id.insert(
            baseboard.clone(),
            HostPhase1FlashHash {
                time_collected: now_db_precision(),
                source: source.to_owned(),
                slot,
                hash,
            },
        ) {
            let error = if previous.hash == hash {
                anyhow!("reported multiple times (same value)")
            } else {
                anyhow!(
                    "reported host phase 1 flash hash \
                     (previously {}, now {hash})",
                    previous.hash,
                )
            };
            Err(CollectorBug::from(
                error.context(format!("baseboard {baseboard:?} slot {slot:?}")),
            ))
        } else {
            Ok(())
        }
    }

    /// Returns true if we already found the caboose for `which` for baseboard
    /// `baseboard`
    ///
    /// This is used to avoid requesting it multiple times (from multiple MGS
    /// instances).
    pub fn found_caboose_already(
        &self,
        baseboard: &BaseboardId,
        which: CabooseWhich,
    ) -> bool {
        self.cabooses_found
            .get(&which)
            .map(|map| map.contains_key(baseboard))
            .unwrap_or(false)
    }

    /// Record the given caboose information found for the given baseboard
    ///
    /// The baseboard must previously have been reported using
    /// `found_sp_state()`.
    ///
    /// `source` is an arbitrary string for debugging that describes the MGS
    /// that reported this data (generally a URL string).
    pub fn found_caboose(
        &mut self,
        baseboard: &BaseboardId,
        which: CabooseWhich,
        source: &str,
        caboose: SpComponentCaboose,
    ) -> Result<(), CollectorBug> {
        // Normalize the caboose contents: i.e., if we've seen this exact
        // caboose contents before, use the same record from before.  Otherwise,
        // make a new one.
        let sw_caboose =
            Self::normalize_item(&mut self.cabooses, Caboose::from(caboose));
        let (baseboard, _) =
            self.sps.get_key_value(baseboard).ok_or_else(|| {
                anyhow!(
                    "reporting caboose for unknown baseboard: {:?} ({:?})",
                    baseboard,
                    sw_caboose
                )
            })?;
        let by_id =
            self.cabooses_found.entry(which).or_insert_with(|| BTreeMap::new());
        if let Some(previous) = by_id.insert(
            baseboard.clone(),
            CabooseFound {
                time_collected: now_db_precision(),
                source: source.to_owned(),
                caboose: sw_caboose.clone(),
            },
        ) {
            let error = if *previous.caboose == *sw_caboose {
                anyhow!("reported multiple times (same value)")
            } else {
                anyhow!(
                    "reported caboose multiple times (previously {:?}, \
                    now {:?})",
                    previous,
                    sw_caboose
                )
            };
            Err(CollectorBug::from(error.context(format!(
                "baseboard {:?} caboose {:?}",
                baseboard, which
            ))))
        } else {
            Ok(())
        }
    }

    /// Returns true if we already found the root of trust page for `which` for
    /// baseboard `baseboard`
    ///
    /// This is used to avoid requesting it multiple times (from multiple MGS
    /// instances).
    pub fn found_rot_page_already(
        &self,
        baseboard: &BaseboardId,
        which: RotPageWhich,
    ) -> bool {
        self.rot_pages_found
            .get(&which)
            .map(|map| map.contains_key(baseboard))
            .unwrap_or(false)
    }

    /// Record the given root of trust page found for the given baseboard
    ///
    /// The baseboard must previously have been reported using
    /// `found_sp_state()`.
    ///
    /// `source` is an arbitrary string for debugging that describes the MGS
    /// that reported this data (generally a URL string).
    pub fn found_rot_page(
        &mut self,
        baseboard: &BaseboardId,
        which: RotPageWhich,
        source: &str,
        page: RotPage,
    ) -> Result<(), CollectorBug> {
        // Normalize the page contents: i.e., if we've seen this exact page
        // before, use the same record from before.  Otherwise, make a new one.
        let sw_rot_page = Self::normalize_item(&mut self.rot_pages, page);
        let (baseboard, _) =
            self.sps.get_key_value(baseboard).ok_or_else(|| {
                anyhow!(
                    "reporting rot page for unknown baseboard: {:?} ({:?})",
                    baseboard,
                    sw_rot_page
                )
            })?;
        let by_id = self.rot_pages_found.entry(which).or_default();
        if let Some(previous) = by_id.insert(
            baseboard.clone(),
            RotPageFound {
                time_collected: now_db_precision(),
                source: source.to_owned(),
                page: sw_rot_page.clone(),
            },
        ) {
            let error = if *previous.page == *sw_rot_page {
                anyhow!("reported multiple times (same value)",)
            } else {
                anyhow!(
                    "reported rot page multiple times (previously {:?}, \
                    now {:?})",
                    previous,
                    sw_rot_page
                )
            };
            Err(CollectorBug::from(error.context(format!(
                "baseboard {:?} rot page {:?}",
                baseboard, which
            ))))
        } else {
            Ok(())
        }
    }

    /// Helper function for normalizing items
    ///
    /// If `item` (or its equivalent) is not already in `items`, insert it.
    /// Either way, return the item from `items`.  (This will either be `item`
    /// itself or whatever was already in `items`.)
    fn normalize_item<T: Clone + Ord>(
        items: &mut BTreeSet<Arc<T>>,
        item: T,
    ) -> Arc<T> {
        match items.get(&item) {
            Some(found_item) => found_item.clone(),
            None => {
                let new_item = Arc::new(item);
                items.insert(new_item.clone());
                new_item
            }
        }
    }

    /// Record a collection error
    ///
    /// See [`InventoryError`] for more on what kinds of errors are reported
    /// this way.  These errors are stored as part of the collection so that
    /// future readers can see what problems might make the collection
    /// incomplete.  By contrast, [`CollectorBug`]s are not reported and stored
    /// this way.
    pub fn found_error(&mut self, error: InventoryError) {
        self.errors.push(error);
    }

    /// Record information about a sled that's part of the control plane
    pub fn found_sled_inventory(
        &mut self,
        source: &str,
        inventory: Inventory,
    ) -> Result<(), anyhow::Error> {
        let sled_id = inventory.sled_id;

        let baseboard_id = match inventory.baseboard {
            Baseboard::Pc { .. } => None,
            Baseboard::Gimlet { identifier, model, .. } => {
                Some(Self::normalize_item(
                    &mut self.baseboards,
                    BaseboardId {
                        serial_number: identifier,
                        part_number: model,
                    },
                ))
            }
            Baseboard::Unknown => {
                self.found_error(InventoryError::from(anyhow!(
                    "sled {sled_id}: reported unknown baseboard",
                )));
                None
            }
        };

        // Socket addresses come through the OpenAPI spec as strings, which
        // means they don't get validated when everything else does.  This
        // error is an operational error in collecting the data, not a collector
        // bug.
        let time_collected = now_db_precision();
        let sled = SledAgent {
            source: source.to_string(),
            sled_agent_address: inventory.sled_agent_address,
            sled_role: inventory.sled_role,
            baseboard_id,
            usable_hardware_threads: inventory.usable_hardware_threads,
            usable_physical_ram: inventory.usable_physical_ram,
            cpu_family: inventory.cpu_family,
            reservoir_size: inventory.reservoir_size,
            time_collected,
            sled_id,
            disks: inventory.disks.into_iter().map(|d| d.into()).collect(),
            zpools: inventory
                .zpools
                .into_iter()
                .map(|z| Zpool::new(time_collected, z))
                .collect(),
            datasets: inventory
                .datasets
                .into_iter()
                .map(|d| d.into())
                .collect(),
            ledgered_sled_config: inventory.ledgered_sled_config,
            reconciler_status: inventory.reconciler_status,
            last_reconciliation: inventory.last_reconciliation,
            zone_image_resolver: inventory.zone_image_resolver,
        };

        self.sleds
            .insert_unique(sled)
            .map_err(|error| error.into_owned())
            .with_context(|| {
                anyhow!("sled {sled_id}: reported sled multiple times")
            })
    }

    /// Record information about Keeper cluster membership learned from the
    /// clickhouse-admin service running in the keeper zones.
    pub fn found_clickhouse_keeper_cluster_membership(
        &mut self,
        membership: ClickhouseKeeperClusterMembership,
    ) {
        self.clickhouse_keeper_cluster_membership.insert(membership);
    }

    /// Record information about timesync
    pub fn found_ntp_timesync(
        &mut self,
        timesync: TimeSync,
    ) -> Result<(), anyhow::Error> {
        self.ntp_timesync
            .insert_unique(timesync)
            .map_err(|err| err.into_owned())
            .context("NTP service reported time multiple times")
    }

    /// Record metrics from a CockroachDB node
    pub fn found_cockroach_metrics(
        &mut self,
        node_id: NodeId,
        metrics: PrometheusMetrics,
    ) {
        let mut status = CockroachStatus::default();
        status.ranges_underreplicated =
            metrics.get_metric_unsigned(CockroachMetric::RangesUnderreplicated);
        status.liveness_live_nodes =
            metrics.get_metric_unsigned(CockroachMetric::LivenessLiveNodes);
        self.cockroach_status.insert(node_id, status);
    }

    /// Record information about internal DNS generation status
    pub fn found_internal_dns_generation_status(
        &mut self,
        status: InternalDnsGenerationStatus,
    ) -> Result<(), anyhow::Error> {
        self.internal_dns_generation_status
            .insert_unique(status)
            .map_err(|err| err.into_owned())
            .context(
                "Internal DNS server reported generation status multiple times",
            )
    }

    /// Returns all zones of a kind from the ledgers of observed sleds
    pub fn ledgered_zones_of_kind(
        &self,
        kind: nexus_sled_agent_shared::inventory::ZoneKind,
    ) -> impl Iterator<
        Item = &nexus_sled_agent_shared::inventory::OmicronZoneConfig,
    > + '_ {
        self.sleds.iter().flat_map(move |sled| {
            sled.ledgered_sled_config.as_ref().into_iter().flat_map(
                move |sled_config| {
                    sled_config.zones.iter().filter(move |zone_config| {
                        zone_config.zone_type.kind() == kind
                    })
                },
            )
        })
    }

    /// Returns zones from the last reconciled ledger of observed sleds
    ///
    /// Does not actually consider whether or not the zone was successfully
    /// created by the sled reconciliation process
    pub fn last_reconciled_zones_of_kind(
        &self,
        kind: nexus_sled_agent_shared::inventory::ZoneKind,
    ) -> impl Iterator<
        Item = &nexus_sled_agent_shared::inventory::OmicronZoneConfig,
    > + '_ {
        self.sleds.iter().flat_map(move |sled| {
            sled.last_reconciliation.as_ref().into_iter().flat_map(
                move |sled_config| {
                    sled_config.last_reconciled_config.zones.iter().filter(
                        move |zone_config| zone_config.zone_type.kind() == kind,
                    )
                },
            )
        })
    }
}

/// Returns the current time, truncated to the previous microsecond.
///
/// This exists because the database doesn't store nanosecond-precision, so if
/// we store nanosecond-precision timestamps, then DateTime conversion is lossy
/// when round-tripping through the database.  That's rather inconvenient.
pub fn now_db_precision() -> DateTime<Utc> {
    let ts = Utc::now();
    let nanosecs = ts.timestamp_subsec_nanos();
    let micros = ts.timestamp_subsec_micros();
    let only_nanos = nanosecs - micros * 1000;
    ts - std::time::Duration::from_nanos(u64::from(only_nanos))
}

#[cfg(test)]
mod test {
    use super::CollectionBuilder;
    use super::now_db_precision;
    use crate::examples::Representative;
    use crate::examples::representative;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use gateway_client::types::PowerState;
    use gateway_client::types::RotState;
    use gateway_client::types::SpComponentCaboose;
    use gateway_client::types::SpState;
    use gateway_types::rot::RotSlot;
    use nexus_sled_agent_shared::inventory::SledRole;
    use nexus_types::inventory::BaseboardId;
    use nexus_types::inventory::Caboose;
    use nexus_types::inventory::CabooseWhich;
    use nexus_types::inventory::RotPage;
    use nexus_types::inventory::RotPageWhich;
    use nexus_types::inventory::SpType;
    use omicron_common::api::external::ByteCount;

    // Verify the contents of an empty collection.
    #[test]
    fn test_empty() {
        let time_before = now_db_precision();
        let builder = CollectionBuilder::new("test_empty");
        let collection = builder.build();
        let time_after = now_db_precision();

        assert!(collection.errors.is_empty());
        assert!(time_before <= collection.time_started);
        assert!(collection.time_started <= collection.time_done);
        assert!(collection.time_done <= time_after);
        assert_eq!(collection.collector, "test_empty");
        assert!(collection.baseboards.is_empty());
        assert!(collection.cabooses.is_empty());
        assert!(collection.rot_pages.is_empty());
        assert!(collection.sps.is_empty());
        assert!(collection.rots.is_empty());
        assert!(collection.cabooses_found.is_empty());
        assert!(collection.rot_pages_found.is_empty());
        assert!(collection.clickhouse_keeper_cluster_membership.is_empty());
        assert!(collection.cockroach_status.is_empty());
        assert!(collection.ntp_timesync.is_empty());
        assert!(collection.internal_dns_generation_status.is_empty());
    }

    // Simple test of a single, fairly typical collection that contains just
    // about all kinds of valid data.  That includes exercising:
    //
    // - all three baseboard types (switch, sled, PSC)
    // - various valid values for all fields (sources, slot numbers, power
    //   states, baseboard revisions, cabooses, etc.)
    // - some empty slots
    // - some missing cabooses
    // - some cabooses common to multiple baseboards; others not
    // - serial number reused across different model numbers
    // - sled agent inventory
    // - omicron zone inventory
    //
    // This test is admittedly pretty tedious and maybe not worthwhile but it's
    // a useful quick check.
    #[test]
    fn test_basic() {
        let time_before = now_db_precision();
        let Representative {
            builder,
            sleds: [sled1_bb, sled2_bb, sled3_bb, sled4_bb],
            switch,
            psc,
            sled_agents:
                [
                    sled_agent_id_basic,
                    sled_agent_id_extra,
                    sled_agent_id_pc,
                    sled_agent_id_unknown,
                ],
        } = representative();
        let collection = builder.build();
        let time_after = now_db_precision();
        println!("{:#?}", collection);
        assert!(time_before <= collection.time_started);
        assert!(collection.time_started <= collection.time_done);
        assert!(collection.time_done <= time_after);
        assert_eq!(collection.collector, "example");

        // Verify the one error that ought to have been produced for the SP with
        // no RoT information.
        assert_eq!(
            collection.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            [
                "MGS \"fake MGS 1\": reading RoT state for BaseboardId \
                { part_number: \"model1\", serial_number: \"s2\" }: test suite \
                injected error",
                "sled 5c5b4cf9-3e13-45fd-871c-f177d6537510: reported unknown \
                baseboard"
            ]
        );

        // Verify the baseboard ids found.
        let expected_baseboards =
            &[&sled1_bb, &sled2_bb, &sled3_bb, &sled4_bb, &switch, &psc];
        for bb in expected_baseboards {
            assert!(collection.baseboards.contains(*bb));
        }
        assert_eq!(collection.baseboards.len(), expected_baseboards.len());

        // Verify the stuff that's easy to verify for all SPs: timestamps.
        // There will be one more baseboard than SP because of the one added for
        // the extra sled agent.
        assert_eq!(collection.sps.len() + 1, collection.baseboards.len());
        for (bb, sp) in collection.sps.iter() {
            assert!(collection.time_started <= sp.time_collected);
            assert!(sp.time_collected <= collection.time_done);

            if let Some(rot) = collection.rots.get(bb) {
                assert_eq!(rot.source, sp.source);
                assert_eq!(rot.time_collected, sp.time_collected);
            }

            for which in [CabooseWhich::SpSlot0, CabooseWhich::SpSlot1] {
                let caboose = collection.caboose_for(which, bb);
                if let Some(c) = caboose {
                    assert!(collection.time_started <= c.time_collected);
                    assert!(c.time_collected <= collection.time_done);
                    assert!(collection.cabooses.contains(&c.caboose));
                }
            }
        }

        // Verify the common caboose.
        let common_caboose_baseboards = [&sled1_bb, &sled2_bb, &switch];
        let common_caboose = Caboose {
            board: String::from("board_1"),
            git_commit: String::from("git_commit_1"),
            name: String::from("name_1"),
            version: String::from("version_1"),
            sign: Some(String::from("sign_1")),
        };
        for bb in &common_caboose_baseboards {
            let _ = collection.sps.get(*bb).unwrap();
            let c0 = collection.caboose_for(CabooseWhich::SpSlot0, bb).unwrap();
            let c1 = collection.caboose_for(CabooseWhich::SpSlot1, bb).unwrap();
            assert_eq!(c0.source, "test suite");
            assert_eq!(*c0.caboose, common_caboose);
            assert_eq!(c1.source, "test suite");
            assert_eq!(*c1.caboose, common_caboose);

            let _ = collection.rots.get(*bb).unwrap();
            let c0 =
                collection.caboose_for(CabooseWhich::RotSlotA, bb).unwrap();
            let c1 =
                collection.caboose_for(CabooseWhich::RotSlotB, bb).unwrap();
            assert_eq!(c0.source, "test suite");
            assert_eq!(*c0.caboose, common_caboose);
            assert_eq!(c1.source, "test suite");
            assert_eq!(*c1.caboose, common_caboose);
        }
        assert!(collection.cabooses.contains(&common_caboose));

        // Verify the common RoT page data.
        let common_rot_page_baseboards = [&sled1_bb, &sled3_bb, &switch];
        let common_rot_page = nexus_types::inventory::RotPage {
            // base64("1") == "MQ=="
            data_base64: "MQ==".to_string(),
        };
        for bb in &common_rot_page_baseboards {
            let _ = collection.sps.get(*bb).unwrap();
            let p0 = collection.rot_page_for(RotPageWhich::Cmpa, bb).unwrap();
            let p1 =
                collection.rot_page_for(RotPageWhich::CfpaActive, bb).unwrap();
            let p2 = collection
                .rot_page_for(RotPageWhich::CfpaInactive, bb)
                .unwrap();
            let p3 =
                collection.rot_page_for(RotPageWhich::CfpaScratch, bb).unwrap();
            assert_eq!(p0.source, "test suite");
            assert_eq!(*p0.page, common_rot_page);
            assert_eq!(p1.source, "test suite");
            assert_eq!(*p1.page, common_rot_page);
            assert_eq!(p2.source, "test suite");
            assert_eq!(*p2.page, common_rot_page);
            assert_eq!(p3.source, "test suite");
            assert_eq!(*p3.page, common_rot_page);
        }
        assert!(collection.rot_pages.contains(&common_rot_page));

        // Verify the specific, different data for the healthy SPs and RoTs that
        // we reported.
        // sled1
        let sp = collection.sps.get(&sled1_bb).unwrap();
        assert_eq!(sp.source, "fake MGS 1");
        assert_eq!(sp.sp_type, SpType::Sled);
        assert_eq!(sp.sp_slot, 3);
        assert_eq!(sp.baseboard_revision, 0);
        assert_eq!(sp.hubris_archive, "hubris1");
        assert_eq!(sp.power_state, PowerState::A0);
        let rot = collection.rots.get(&sled1_bb).unwrap();
        assert_eq!(rot.active_slot, RotSlot::A);
        assert_eq!(rot.pending_persistent_boot_preference, None);
        assert_eq!(rot.persistent_boot_preference, RotSlot::A);
        assert_eq!(
            rot.slot_a_sha3_256_digest.as_ref().unwrap(),
            "slotAdigest1"
        );
        assert_eq!(
            rot.slot_b_sha3_256_digest.as_ref().unwrap(),
            "slotBdigest1"
        );
        assert_eq!(rot.transient_boot_preference, None);

        // sled2
        let sp = collection.sps.get(&sled2_bb).unwrap();
        assert_eq!(sp.source, "fake MGS 2");
        assert_eq!(sp.sp_type, SpType::Sled);
        assert_eq!(sp.sp_slot, 4);
        assert_eq!(sp.baseboard_revision, 1);
        assert_eq!(sp.hubris_archive, "hubris2");
        assert_eq!(sp.power_state, PowerState::A2);
        let rot = collection.rots.get(&sled2_bb).unwrap();
        assert_eq!(rot.active_slot, RotSlot::B);
        assert_eq!(rot.pending_persistent_boot_preference, Some(RotSlot::A));
        assert_eq!(rot.persistent_boot_preference, RotSlot::A);
        assert_eq!(
            rot.slot_a_sha3_256_digest.as_ref().unwrap(),
            "slotAdigest2"
        );
        assert_eq!(
            rot.slot_b_sha3_256_digest.as_ref().unwrap(),
            "slotBdigest2"
        );
        assert_eq!(rot.transient_boot_preference, Some(RotSlot::B));

        // sled 2 did not have any RoT pages reported
        assert!(
            collection.rot_page_for(RotPageWhich::Cmpa, &sled2_bb).is_none()
        );
        assert!(
            collection
                .rot_page_for(RotPageWhich::CfpaActive, &sled2_bb)
                .is_none()
        );
        assert!(
            collection
                .rot_page_for(RotPageWhich::CfpaInactive, &sled2_bb)
                .is_none()
        );
        assert!(
            collection
                .rot_page_for(RotPageWhich::CfpaScratch, &sled2_bb)
                .is_none()
        );

        // switch
        let sp = collection.sps.get(&switch).unwrap();
        assert_eq!(sp.source, "fake MGS 2");
        assert_eq!(sp.sp_type, SpType::Switch);
        assert_eq!(sp.sp_slot, 0);
        assert_eq!(sp.baseboard_revision, 2);
        assert_eq!(sp.hubris_archive, "hubris3");
        assert_eq!(sp.power_state, PowerState::A1);
        let rot = collection.rots.get(&switch).unwrap();
        assert_eq!(rot.active_slot, RotSlot::B);
        assert_eq!(rot.pending_persistent_boot_preference, None);
        assert_eq!(rot.persistent_boot_preference, RotSlot::A);
        assert_eq!(
            rot.slot_a_sha3_256_digest.as_ref().unwrap(),
            "slotAdigest3"
        );
        assert_eq!(
            rot.slot_b_sha3_256_digest.as_ref().unwrap(),
            "slotBdigest3"
        );
        assert_eq!(rot.transient_boot_preference, None);

        // PSC
        let sp = collection.sps.get(&psc).unwrap();
        assert_eq!(sp.source, "fake MGS 1");
        assert_eq!(sp.sp_type, SpType::Power);
        assert_eq!(sp.sp_slot, 1);
        assert_eq!(sp.baseboard_revision, 3);
        assert_eq!(sp.hubris_archive, "hubris4");
        assert_eq!(sp.power_state, PowerState::A2);
        let rot = collection.rots.get(&psc).unwrap();
        assert_eq!(rot.active_slot, RotSlot::B);
        assert_eq!(rot.pending_persistent_boot_preference, None);
        assert_eq!(rot.persistent_boot_preference, RotSlot::A);
        assert_eq!(
            rot.slot_a_sha3_256_digest.as_ref().unwrap(),
            "slotAdigest4"
        );
        assert_eq!(
            rot.slot_b_sha3_256_digest.as_ref().unwrap(),
            "slotBdigest4"
        );
        assert_eq!(rot.transient_boot_preference, None);

        // The PSC has four different cabooses!
        let c = &collection
            .caboose_for(CabooseWhich::SpSlot0, &psc)
            .unwrap()
            .caboose;
        assert_eq!(c.board, "board_psc_sp_0");
        assert!(collection.cabooses.contains(c));
        let c = &collection
            .caboose_for(CabooseWhich::SpSlot1, &psc)
            .unwrap()
            .caboose;
        assert!(collection.cabooses.contains(c));
        assert_eq!(c.board, "board_psc_sp_1");
        let c = &collection
            .caboose_for(CabooseWhich::RotSlotA, &psc)
            .unwrap()
            .caboose;
        assert!(collection.cabooses.contains(c));
        assert_eq!(c.board, "board_psc_rot_a");
        let c = &collection
            .caboose_for(CabooseWhich::RotSlotB, &psc)
            .unwrap()
            .caboose;
        assert!(collection.cabooses.contains(c));
        assert_eq!(c.board, "board_psc_rot_b");

        // The PSC also has four different RoT pages!
        let p =
            &collection.rot_page_for(RotPageWhich::Cmpa, &psc).unwrap().page;
        assert_eq!(
            BASE64_STANDARD.decode(&p.data_base64).unwrap(),
            b"psc cmpa"
        );
        let p = &collection
            .rot_page_for(RotPageWhich::CfpaActive, &psc)
            .unwrap()
            .page;
        assert_eq!(
            BASE64_STANDARD.decode(&p.data_base64).unwrap(),
            b"psc cfpa active"
        );
        let p = &collection
            .rot_page_for(RotPageWhich::CfpaInactive, &psc)
            .unwrap()
            .page;
        assert_eq!(
            BASE64_STANDARD.decode(&p.data_base64).unwrap(),
            b"psc cfpa inactive"
        );
        let p = &collection
            .rot_page_for(RotPageWhich::CfpaScratch, &psc)
            .unwrap()
            .page;
        assert_eq!(
            BASE64_STANDARD.decode(&p.data_base64).unwrap(),
            b"psc cfpa scratch"
        );

        // Verify the reported SP state for sled3, which did not have a healthy
        // RoT, nor any cabooses.
        let sp = collection.sps.get(&sled3_bb).unwrap();
        assert_eq!(sp.source, "fake MGS 1");
        assert_eq!(sp.sp_type, SpType::Sled);
        assert_eq!(sp.sp_slot, 5);
        assert_eq!(sp.baseboard_revision, 1);
        assert_eq!(sp.hubris_archive, "hubris5");
        assert_eq!(sp.power_state, PowerState::A2);
        assert!(
            collection.caboose_for(CabooseWhich::SpSlot0, &sled3_bb).is_none()
        );
        assert!(
            collection.caboose_for(CabooseWhich::SpSlot1, &sled3_bb).is_none()
        );
        assert!(!collection.rots.contains_key(&sled3_bb));

        // There shouldn't be any other RoTs.
        assert_eq!(collection.sps.len(), collection.rots.len() + 1);

        // There should be five cabooses: the four used for the PSC (see above),
        // plus the common one; same for RoT pages.
        assert_eq!(collection.cabooses.len(), 5);
        assert_eq!(collection.rot_pages.len(), 5);

        // Verify that we found the sled agents.
        assert_eq!(collection.sled_agents.len(), 4);
        for sled_agent in &collection.sled_agents {
            if sled_agent.sled_id == sled_agent_id_extra {
                assert_eq!(sled_agent.sled_role, SledRole::Scrimlet);
            } else {
                assert_eq!(sled_agent.sled_role, SledRole::Gimlet);
            }

            assert_eq!(
                sled_agent.sled_agent_address,
                "[::1]:56792".parse().unwrap()
            );
            assert_eq!(sled_agent.usable_hardware_threads, 10);
            assert_eq!(
                sled_agent.usable_physical_ram,
                ByteCount::from(1024 * 1024)
            );
            assert_eq!(sled_agent.reservoir_size, ByteCount::from(1024));
        }

        let sled1_agent =
            collection.sled_agents.get(&sled_agent_id_basic).unwrap();
        let sled1_bb = sled1_agent.baseboard_id.as_ref().unwrap();
        assert_eq!(sled1_bb.part_number, "model1");
        assert_eq!(sled1_bb.serial_number, "s1");
        assert_eq!(sled1_agent.disks.len(), 4);
        assert_eq!(sled1_agent.disks[0].identity.vendor, "macrohard");
        assert_eq!(sled1_agent.disks[0].identity.model, "box");
        assert_eq!(sled1_agent.disks[0].identity.serial, "XXIV");

        let sled4_agent =
            collection.sled_agents.get(&sled_agent_id_extra).unwrap();
        let sled4_bb = sled4_agent.baseboard_id.as_ref().unwrap();
        assert_eq!(sled4_bb.serial_number, "s4");
        assert!(
            collection
                .sled_agents
                .get(&sled_agent_id_pc)
                .unwrap()
                .baseboard_id
                .is_none()
        );
        assert!(
            collection
                .sled_agents
                .get(&sled_agent_id_unknown)
                .unwrap()
                .baseboard_id
                .is_none()
        );
    }

    // Exercises all the failure cases that shouldn't happen in real systems.
    // Despite all of these failures, we should get a valid collection at the
    // end.
    #[test]
    fn test_problems() {
        let mut builder = CollectionBuilder::new("test_problems");

        let sled1_bb = builder
            .found_sp_state(
                "fake MGS 1",
                SpType::Sled,
                3,
                SpState {
                    base_mac_address: [0; 6],
                    hubris_archive_id: String::from("hubris1"),
                    model: String::from("model1"),
                    power_state: PowerState::A0,
                    revision: 0,
                    rot: RotState::V2 {
                        active: RotSlot::A,
                        pending_persistent_boot_preference: None,
                        persistent_boot_preference: RotSlot::A,
                        slot_a_sha3_256_digest: None,
                        slot_b_sha3_256_digest: None,
                        transient_boot_preference: None,
                    },
                    serial_number: String::from("s1"),
                },
            )
            .unwrap();

        // report the same SP again with the same contents
        let sled1_bb_dup = builder
            .found_sp_state(
                "fake MGS 1",
                SpType::Sled,
                3,
                SpState {
                    base_mac_address: [0; 6],
                    hubris_archive_id: String::from("hubris1"),
                    model: String::from("model1"),
                    power_state: PowerState::A0,
                    revision: 0,
                    rot: RotState::V2 {
                        active: RotSlot::A,
                        pending_persistent_boot_preference: None,
                        persistent_boot_preference: RotSlot::A,
                        slot_a_sha3_256_digest: None,
                        slot_b_sha3_256_digest: None,
                        transient_boot_preference: None,
                    },
                    serial_number: String::from("s1"),
                },
            )
            .unwrap();
        assert_eq!(sled1_bb, sled1_bb_dup);

        // report the same SP again with different contents
        let sled1_bb_dup = builder
            .found_sp_state(
                "fake MGS 1",
                SpType::Sled,
                3,
                SpState {
                    base_mac_address: [0; 6],
                    hubris_archive_id: String::from("hubris1"),
                    model: String::from("model1"),
                    power_state: PowerState::A0,
                    revision: 1,
                    rot: RotState::V2 {
                        active: RotSlot::A,
                        pending_persistent_boot_preference: None,
                        persistent_boot_preference: RotSlot::A,
                        slot_a_sha3_256_digest: None,
                        slot_b_sha3_256_digest: None,
                        transient_boot_preference: None,
                    },
                    serial_number: String::from("s1"),
                },
            )
            .unwrap();
        assert_eq!(sled1_bb, sled1_bb_dup);

        // report SP caboose for an unknown baseboard
        let bogus_baseboard = BaseboardId {
            part_number: String::from("p1"),
            serial_number: String::from("bogus"),
        };
        let caboose1 = SpComponentCaboose {
            board: String::from("board1"),
            git_commit: String::from("git_commit1"),
            name: String::from("name1"),
            version: String::from("version1"),
            sign: Some(String::from("sign1")),
            epoch: None,
        };
        assert!(
            !builder
                .found_caboose_already(&bogus_baseboard, CabooseWhich::SpSlot0)
        );
        let error = builder
            .found_caboose(
                &bogus_baseboard,
                CabooseWhich::SpSlot0,
                "dummy",
                caboose1.clone(),
            )
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "reporting caboose for unknown baseboard: \
            BaseboardId { part_number: \"p1\", serial_number: \"bogus\" } \
            (Caboose { board: \"board1\", git_commit: \"git_commit1\", \
            name: \"name1\", version: \"version1\", sign: Some(\"sign1\") })"
        );
        assert!(
            !builder
                .found_caboose_already(&bogus_baseboard, CabooseWhich::SpSlot0)
        );

        // report RoT caboose for an unknown baseboard
        let error2 = builder
            .found_caboose(
                &bogus_baseboard,
                CabooseWhich::RotSlotA,
                "dummy",
                caboose1.clone(),
            )
            .unwrap_err();
        assert_eq!(error.to_string(), error2.to_string(),);

        // report the same caboose twice with the same contents
        builder
            .found_caboose(
                &sled1_bb,
                CabooseWhich::SpSlot0,
                "dummy",
                caboose1.clone(),
            )
            .unwrap();
        let error = builder
            .found_caboose(
                &sled1_bb,
                CabooseWhich::SpSlot0,
                "dummy",
                caboose1.clone(),
            )
            .unwrap_err();
        assert_eq!(
            format!("{:#}", error),
            "baseboard BaseboardId { part_number: \"model1\", \
            serial_number: \"s1\" } caboose SpSlot0: reported multiple \
            times (same value)"
        );
        // report the same caboose again with different contents
        let error = builder
            .found_caboose(
                &sled1_bb,
                CabooseWhich::SpSlot0,
                "dummy",
                SpComponentCaboose {
                    board: String::from("board2"),
                    git_commit: String::from("git_commit2"),
                    name: String::from("name2"),
                    version: String::from("version2"),
                    sign: Some(String::from("sign2")),
                    epoch: None,
                },
            )
            .unwrap_err();
        let message = format!("{:#}", error);
        println!("found error: {}", message);
        assert!(message.contains(
            "caboose SpSlot0: reported caboose multiple times (previously"
        ));
        assert!(message.contains(", now "));

        // report RoT page for an unknown baseboard
        let rot_page1 = RotPage { data_base64: "page1".to_string() };
        let rot_page2 = RotPage { data_base64: "page2".to_string() };
        assert!(
            !builder
                .found_rot_page_already(&bogus_baseboard, RotPageWhich::Cmpa)
        );
        let error = builder
            .found_rot_page(
                &bogus_baseboard,
                RotPageWhich::Cmpa,
                "dummy",
                rot_page1.clone(),
            )
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "reporting rot page for unknown baseboard: \
            BaseboardId { part_number: \"p1\", serial_number: \"bogus\" } \
            (RotPage { data_base64: \"page1\" })"
        );
        assert!(
            !builder
                .found_rot_page_already(&bogus_baseboard, RotPageWhich::Cmpa)
        );

        // report the same rot page twice with the same contents
        builder
            .found_rot_page(
                &sled1_bb,
                RotPageWhich::Cmpa,
                "dummy",
                rot_page1.clone(),
            )
            .unwrap();
        let error = builder
            .found_rot_page(
                &sled1_bb,
                RotPageWhich::Cmpa,
                "dummy",
                rot_page1.clone(),
            )
            .unwrap_err();
        assert_eq!(
            format!("{:#}", error),
            "baseboard BaseboardId { part_number: \"model1\", \
            serial_number: \"s1\" } rot page Cmpa: reported multiple \
            times (same value)"
        );
        // report the same rot page again with different contents
        let error = builder
            .found_rot_page(
                &sled1_bb,
                RotPageWhich::Cmpa,
                "dummy",
                rot_page2.clone(),
            )
            .unwrap_err();
        let message = format!("{:#}", error);
        println!("found error: {}", message);
        assert!(message.contains(
            "rot page Cmpa: reported rot page multiple times (previously"
        ));
        assert!(message.contains(", now RotPage { data_base64: \"page2\" }"));

        // We should still get a valid collection.
        let collection = builder.build();
        println!("{:#?}", collection);
        assert_eq!(collection.collector, "test_problems");

        // We should still have the one sled, its SP slot0 caboose, and its Cmpa
        // RoT page.
        assert!(collection.baseboards.contains(&sled1_bb));
        let _ = collection.sps.get(&sled1_bb).unwrap();
        let caboose =
            collection.caboose_for(CabooseWhich::SpSlot0, &sled1_bb).unwrap();
        assert_eq!(caboose.caboose.board, "board2");
        assert!(collection.cabooses.contains(&caboose.caboose));
        assert!(
            collection.caboose_for(CabooseWhich::SpSlot1, &sled1_bb).is_none()
        );
        let _ = collection.rots.get(&sled1_bb).unwrap();
        assert!(
            collection.caboose_for(CabooseWhich::RotSlotA, &sled1_bb).is_none()
        );
        assert!(
            collection.caboose_for(CabooseWhich::RotSlotB, &sled1_bb).is_none()
        );
        let rot_page =
            collection.rot_page_for(RotPageWhich::Cmpa, &sled1_bb).unwrap();
        assert!(collection.rot_pages.contains(&rot_page.page));

        // TODO-correctness Is this test correct? We reported the same RoT page
        // with different data (rot_page1, then rot_page2). The second
        // `found_rot_page` returned an error, but we overwrote the original
        // data and did not record the error in `collection.errors`. Should we
        // either have kept the original data or returned Ok while returning an
        // error? It seems a little strange we returned Err but accepted the new
        // data.
        assert_eq!(rot_page.page.data_base64, rot_page2.data_base64);

        assert!(
            collection
                .rot_page_for(RotPageWhich::CfpaActive, &sled1_bb)
                .is_none()
        );
        assert!(
            collection
                .rot_page_for(RotPageWhich::CfpaInactive, &sled1_bb)
                .is_none()
        );
        assert!(
            collection
                .rot_page_for(RotPageWhich::CfpaScratch, &sled1_bb)
                .is_none()
        );

        // We should see no errors.
        assert!(collection.errors.is_empty());
    }
}
