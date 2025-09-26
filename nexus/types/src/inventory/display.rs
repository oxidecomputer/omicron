// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code to display inventory collections.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Write},
};

use chrono::SecondsFormat;
use clap::Subcommand;
use gateway_types::component::SpType;
use iddqd::IdOrdMap;
use indent_write::fmt::IndentWriter;
use itertools::Itertools;
use nexus_sled_agent_shared::inventory::{
    BootImageHeader, BootPartitionContents, BootPartitionDetails,
    ConfigReconcilerInventory, ConfigReconcilerInventoryResult,
    ConfigReconcilerInventoryStatus, HostPhase2DesiredContents,
    OmicronSledConfig, OmicronZoneImageSource, OrphanedDataset,
    RemoveMupdateOverrideBootSuccessInventory,
};
use omicron_common::disk::M2Slot;
use omicron_uuid_kinds::{
    DatasetUuid, OmicronZoneUuid, PhysicalDiskUuid, ZpoolUuid,
};
use std::collections::HashMap;
use strum::IntoEnumIterator;
use tabled::Tabled;
use tufaceous_artifact::ArtifactHash;
use uuid::Uuid;

use crate::inventory::{
    CabooseWhich, Collection, Dataset, InternalDnsGenerationStatus,
    PhysicalDisk, RotPageWhich, SledAgent, TimeSync, Zpool,
};

/// Code to display inventory collections.
pub struct CollectionDisplay<'a> {
    collection: &'a Collection,
    include_sps: CollectionDisplayIncludeSps,
    include_sleds: bool,
    include_orphaned_datasets: bool,
    include_clickhouse_keeper_membership: bool,
    include_cockroach_status: bool,
    include_ntp_status: bool,
    include_internal_dns_status: bool,
    long_string_formatter: LongStringFormatter,
}

impl<'a> CollectionDisplay<'a> {
    pub(super) fn new(collection: &'a Collection) -> Self {
        Self {
            collection,
            // Display all items by default.
            include_sps: CollectionDisplayIncludeSps::None,
            include_sleds: true,
            include_orphaned_datasets: true,
            include_clickhouse_keeper_membership: true,
            include_cockroach_status: true,
            include_ntp_status: true,
            include_internal_dns_status: true,
            long_string_formatter: LongStringFormatter::new(),
        }
    }

    /// Control display of SP information (defaults to
    /// [`CollectionDisplayIncludeSps::All`]).
    pub fn include_sps(
        &mut self,
        include_sps: CollectionDisplayIncludeSps,
    ) -> &mut Self {
        self.include_sps = include_sps;
        self
    }

    /// Control display of sled information (defaults to true).
    pub fn include_sleds(&mut self, include_sleds: bool) -> &mut Self {
        self.include_sleds = include_sleds;
        self
    }

    /// Control display of orphaned datasets (defaults to true).
    pub fn include_orphaned_datasets(
        &mut self,
        include_orphaned_datasets: bool,
    ) -> &mut Self {
        self.include_orphaned_datasets = include_orphaned_datasets;
        self
    }

    /// Control display of ClickHouse keeper membership information (defaults to
    /// true).
    pub fn include_clickhouse_keeper_membership(
        &mut self,
        include_clickhouse_keeper_membership: bool,
    ) -> &mut Self {
        self.include_clickhouse_keeper_membership =
            include_clickhouse_keeper_membership;
        self
    }

    /// Control display of Cockroach cluster information (defaults to true).
    pub fn include_cockroach_status(
        &mut self,
        include_cockroach_status: bool,
    ) -> &mut Self {
        self.include_cockroach_status = include_cockroach_status;
        self
    }

    /// Control display of NTP timesync information (defaults to true).
    pub fn include_ntp_status(
        &mut self,
        include_ntp_status: bool,
    ) -> &mut Self {
        self.include_ntp_status = include_ntp_status;
        self
    }

    /// Control display of Internal DNS generation information (defaults to true).
    pub fn include_internal_dns_status(
        &mut self,
        include_internal_dns_status: bool,
    ) -> &mut Self {
        self.include_internal_dns_status = include_internal_dns_status;
        self
    }

    /// Show long strings (defaults to false).
    pub fn show_long_strings(&mut self, show_long_strings: bool) -> &mut Self {
        self.long_string_formatter.show_long_strings = show_long_strings;
        self
    }

    /// Apply a [`CollectionDisplayCliFilter`] to this displayer.
    pub fn apply_cli_filter(
        &mut self,
        filter: &CollectionDisplayCliFilter,
    ) -> &mut Self {
        self.include_sps(filter.to_include_sps())
            .include_sleds(filter.include_sleds())
            .include_orphaned_datasets(filter.include_orphaned_datasets())
            .include_clickhouse_keeper_membership(
                filter.include_keeper_membership(),
            )
            .include_cockroach_status(filter.include_cockroach_status())
            .include_ntp_status(filter.include_ntp_status())
            .include_internal_dns_status(filter.include_internal_dns_status())
    }
}

impl fmt::Display for CollectionDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        display_header(self.collection, f)?;
        let nerrors = display_errors(self.collection, f)?;
        display_devices(
            &self.collection,
            &self.include_sps,
            &self.long_string_formatter,
            f,
        )?;
        if self.include_sleds {
            display_sleds(&self.collection, f)?;
        } else if self.include_orphaned_datasets {
            // display_sleds already includes orphaned datasets, hence "else if
            // self.include_orphaned_datasets" rather than just "if".
            display_orphaned_datasets(&self.collection, f)?;
        }
        if self.include_clickhouse_keeper_membership {
            display_keeper_membership(&self.collection, f)?;
        }
        if self.include_cockroach_status {
            display_cockroach_status(&self.collection, f)?;
        }
        if self.include_ntp_status {
            display_ntp_status(&self.collection.ntp_timesync, f)?;
        }
        if self.include_internal_dns_status {
            display_internal_dns_status(
                &self.collection.internal_dns_generation_status,
                f,
            )?;
        }

        if nerrors > 0 {
            writeln!(
                f,
                "warning: {} collection error{} {} reported above",
                nerrors,
                if nerrors == 1 { "" } else { "s" },
                if nerrors == 1 { "was" } else { "were" },
            )?;
        }

        Ok(())
    }
}

/// A command-line friendly representation of filters for a [`CollectionDisplay`].
#[derive(Clone, Debug, Subcommand)]
pub enum CollectionDisplayCliFilter {
    /// show all information from the collection
    All,
    /// show information about service processors (baseboard)
    Sp {
        /// show only information about one SP
        serial: Option<String>,
    },
    /// show orphaned datasets
    OrphanedDatasets,
}

impl CollectionDisplayCliFilter {
    fn to_include_sps(&self) -> CollectionDisplayIncludeSps {
        match self {
            Self::All | Self::Sp { serial: None } => {
                CollectionDisplayIncludeSps::All
            }
            Self::Sp { serial: Some(serial) } => {
                CollectionDisplayIncludeSps::Serial(serial.clone())
            }
            Self::OrphanedDatasets => CollectionDisplayIncludeSps::None,
        }
    }

    fn include_sleds(&self) -> bool {
        match self {
            Self::All => true,
            Self::Sp { .. } => false,
            Self::OrphanedDatasets => false,
        }
    }

    fn include_orphaned_datasets(&self) -> bool {
        match self {
            Self::All => true,
            Self::Sp { .. } => false,
            Self::OrphanedDatasets => true,
        }
    }

    fn include_keeper_membership(&self) -> bool {
        match self {
            Self::All => true,
            Self::Sp { .. } => false,
            Self::OrphanedDatasets => false,
        }
    }

    fn include_cockroach_status(&self) -> bool {
        match self {
            Self::All => true,
            Self::Sp { .. } => false,
            Self::OrphanedDatasets => false,
        }
    }

    fn include_ntp_status(&self) -> bool {
        match self {
            Self::All => true,
            Self::Sp { .. } => false,
            Self::OrphanedDatasets => false,
        }
    }

    fn include_internal_dns_status(&self) -> bool {
        match self {
            Self::All => true,
            Self::Sp { .. } => false,
            Self::OrphanedDatasets => false,
        }
    }
}

/// Which SPs within a collection to display.
pub enum CollectionDisplayIncludeSps {
    /// Display all SPs.
    All,

    /// Display only the SP with the given serial number.
    ///
    /// Currently, this is limited to one SP. We may want to show more than one
    /// SP in the future.
    Serial(String),

    /// Do not display SPs.
    None,
}

impl CollectionDisplayIncludeSps {
    fn include_sp_unknown_serial(&self) -> bool {
        match self {
            CollectionDisplayIncludeSps::All => true,
            CollectionDisplayIncludeSps::Serial(_) => false,
            CollectionDisplayIncludeSps::None => false,
        }
    }

    fn include_sp(&self, serial: &str) -> bool {
        match self {
            CollectionDisplayIncludeSps::All => true,
            CollectionDisplayIncludeSps::Serial(s) => s == serial,
            CollectionDisplayIncludeSps::None => false,
        }
    }
}

fn display_header(
    collection: &Collection,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    writeln!(f, "collection: {}", collection.id)?;
    writeln!(
        f,
        "collector:  {}{}",
        collection.collector,
        if collection.collector.parse::<Uuid>().is_ok() {
            " (likely a Nexus instance)"
        } else {
            ""
        }
    )?;
    writeln!(
        f,
        "started:    {}",
        humantime::format_rfc3339_millis(collection.time_started.into())
    )?;
    writeln!(
        f,
        "done:       {}",
        humantime::format_rfc3339_millis(collection.time_done.into())
    )?;

    Ok(())
}

fn display_errors(
    collection: &Collection,
    f: &mut dyn fmt::Write,
) -> Result<u32, fmt::Error> {
    writeln!(f, "errors:     {}", collection.errors.len())?;
    for (index, message) in collection.errors.iter().enumerate() {
        writeln!(f, "  error {}: {}", index, message)?;
    }

    Ok(collection
        .errors
        .len()
        .try_into()
        .expect("could not convert error count into u32 (yikes)"))
}

fn display_devices(
    collection: &Collection,
    include_sps: &CollectionDisplayIncludeSps,
    long_string_formatter: &LongStringFormatter,
    f: &mut dyn fmt::Write,
) -> fmt::Result {
    // Assemble a list of baseboard ids, sorted first by device type (sled,
    // switch, power), then by slot number.  This is the order in which we will
    // print everything out.
    let mut sorted_baseboard_ids: Vec<_> =
        collection.sps.keys().cloned().collect();
    sorted_baseboard_ids.sort_by(|s1, s2| {
        let sp1 = collection.sps.get(s1).unwrap();
        let sp2 = collection.sps.get(s2).unwrap();
        sp1.sp_type.cmp(&sp2.sp_type).then(sp1.sp_slot.cmp(&sp2.sp_slot))
    });

    // Now print them.
    for baseboard_id in &sorted_baseboard_ids {
        // This unwrap should not fail because the collection we're iterating
        // over came from the one we're looking into now.
        let sp = collection.sps.get(baseboard_id).unwrap();
        let baseboard = collection.baseboards.get(baseboard_id);
        let rot = collection.rots.get(baseboard_id);

        match baseboard {
            None => {
                // It should be impossible to find an SP whose baseboard
                // information we didn't previously fetch.  That's either a bug
                // in this tool (for failing to fetch or find the right
                // baseboard information) or the inventory system (for failing
                // to insert a record into the hw_baseboard_id table).
                if !include_sps.include_sp_unknown_serial() {
                    continue;
                }

                writeln!(f, "")?;
                writeln!(
                    f,
                    "{:?} (serial number unknown -- this is a bug)",
                    sp.sp_type
                )?;
                writeln!(f, "    part number: unknown")?;
            }
            Some(baseboard) => {
                if !include_sps.include_sp(&baseboard.serial_number) {
                    continue;
                }

                writeln!(f, "")?;
                writeln!(f, "{:?} {}", sp.sp_type, baseboard.serial_number)?;
                writeln!(f, "    part number: {}", baseboard.part_number)?;
            }
        };

        writeln!(f, "    power:    {:?}", sp.power_state)?;
        writeln!(f, "    revision: {}", sp.baseboard_revision)?;
        write!(f, "    MGS slot: {:?} {}", sp.sp_type, sp.sp_slot)?;
        writeln!(f, "")?;
        writeln!(
            f,
            "    found at: {} from {}",
            sp.time_collected
                .to_rfc3339_opts(SecondsFormat::Millis, /* use_z */ true),
            sp.source
        )?;

        if sp.sp_type == SpType::Sled {
            #[derive(Tabled)]
            #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
            struct HostPhase1FlashHashRow {
                slot: String,
                hash: String,
            }

            let active_slot =
                match collection.host_phase_1_active_slot_for(baseboard_id) {
                    Some(s) => Cow::Owned(format!("{:?}", s.slot)),
                    None => Cow::Borrowed("unknown (not collected)"),
                };
            writeln!(f, "    host phase 1 active slot: {active_slot}")?;
            writeln!(f, "    host phase 1 hashes:")?;
            let host_phase1_hash_rows: Vec<_> = M2Slot::iter()
                .filter_map(|s| {
                    collection
                        .host_phase_1_flash_hash_for(s, baseboard_id)
                        .map(|h| (s, h))
                })
                .map(|(slot, phase1)| HostPhase1FlashHashRow {
                    slot: format!("{slot:?}"),
                    hash: phase1.hash.to_string(),
                })
                .collect();
            let table = tabled::Table::new(host_phase1_hash_rows)
                .with(tabled::settings::Style::empty())
                .with(tabled::settings::Padding::new(0, 1, 0, 0))
                .to_string();
            writeln!(
                f,
                "{}",
                textwrap::indent(&table.to_string(), "        ")
            )?;
        }

        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct CabooseRow<'a> {
            slot: String,
            board: &'a str,
            name: &'a str,
            version: &'a str,
            git_commit: &'a str,
            #[tabled(display_with = "option_impl_display")]
            sign: &'a Option<String>,
        }

        writeln!(f, "    cabooses:")?;
        let caboose_rows: Vec<_> = CabooseWhich::iter()
            .filter_map(|c| {
                collection.caboose_for(c, baseboard_id).map(|d| (c, d))
            })
            .map(|(c, found_caboose)| CabooseRow {
                slot: format!("{:?}", c),
                board: &found_caboose.caboose.board,
                name: &found_caboose.caboose.name,
                version: &found_caboose.caboose.version,
                git_commit: &found_caboose.caboose.git_commit,
                sign: &found_caboose.caboose.sign,
            })
            .collect();
        let table = tabled::Table::new(caboose_rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();
        writeln!(f, "{}", textwrap::indent(&table.to_string(), "        "))?;

        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct RotPageRow<'a> {
            slot: String,
            data_base64: Cow<'a, str>,
        }

        writeln!(f, "    RoT pages:")?;
        let rot_page_rows: Vec<_> = RotPageWhich::iter()
            .filter_map(|which| {
                collection.rot_page_for(which, baseboard_id).map(|d| (which, d))
            })
            .map(|(which, found_page)| RotPageRow {
                slot: format!("{which:?}"),
                data_base64: long_string_formatter
                    .maybe_truncate(&found_page.page.data_base64),
            })
            .collect();
        let table = tabled::Table::new(rot_page_rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();
        writeln!(f, "{}", textwrap::indent(&table.to_string(), "        "))?;

        if let Some(rot) = rot {
            writeln!(f, "    RoT: active slot: slot {:?}", rot.active_slot)?;
            writeln!(
                f,
                "    RoT: persistent boot preference: slot {:?}",
                rot.persistent_boot_preference,
            )?;
            writeln!(
                f,
                "    RoT: pending persistent boot preference: {}",
                rot.pending_persistent_boot_preference
                    .map(|s| format!("slot {:?}", s))
                    .unwrap_or_else(|| String::from("-"))
            )?;
            writeln!(
                f,
                "    RoT: transient boot preference: {}",
                rot.transient_boot_preference
                    .map(|s| format!("slot {:?}", s))
                    .unwrap_or_else(|| String::from("-"))
            )?;

            writeln!(
                f,
                "    RoT: slot A SHA3-256: {}",
                rot.slot_a_sha3_256_digest
                    .clone()
                    .unwrap_or_else(|| String::from("-"))
            )?;

            writeln!(
                f,
                "    RoT: slot B SHA3-256: {}",
                rot.slot_b_sha3_256_digest
                    .clone()
                    .unwrap_or_else(|| String::from("-"))
            )?;
        } else {
            writeln!(f, "    RoT: no information found")?;
        }
    }

    writeln!(f, "")?;
    for sp_missing_rot in collection
        .sps
        .keys()
        .collect::<BTreeSet<_>>()
        .difference(&collection.rots.keys().collect::<BTreeSet<_>>())
    {
        // It's not a bug in either omdb or the inventory system to find an SP
        // with no RoT.  It just means that when we collected inventory from the
        // SP, it couldn't communicate with its RoT.
        let sp = collection.sps.get(*sp_missing_rot).unwrap();
        writeln!(
            f,
            "warning: found SP with no RoT: {:?} slot {}",
            sp.sp_type, sp.sp_slot
        )?;
    }

    for rot_missing_sp in collection
        .rots
        .keys()
        .collect::<BTreeSet<_>>()
        .difference(&collection.sps.keys().collect::<BTreeSet<_>>())
    {
        // It *is* a bug in the inventory system (or omdb) to find an RoT with
        // no SP, since we get the RoT information from the SP in the first
        // place.
        writeln!(
            f,
            "error: found RoT with no SP: \
            hw_baseboard_id {:?} -- this is a bug",
            rot_missing_sp
        )?;
    }

    Ok(())
}

fn display_sleds(
    collection: &Collection,
    f: &mut dyn fmt::Write,
) -> fmt::Result {
    let mut f = f;
    writeln!(f, "SLED AGENTS")?;
    for sled in &collection.sled_agents {
        let SledAgent {
            time_collected,
            source,
            sled_id,
            baseboard_id,
            sled_agent_address,
            sled_role,
            usable_hardware_threads,
            usable_physical_ram,
            cpu_family,
            reservoir_size,
            disks,
            zpools,
            datasets,
            ledgered_sled_config,
            reconciler_status,
            last_reconciliation,
            zone_image_resolver,
            measurement_resolver,
        } = sled;

        writeln!(
            f,
            "\nsled {} (role = {:?}, serial {})",
            sled_id,
            sled_role,
            match &baseboard_id {
                Some(baseboard_id) => &baseboard_id.serial_number,
                None => "unknown",
            },
        )?;

        let mut indented = IndentWriter::new("    ", f);

        writeln!(
            indented,
            "found at:    {} from {}",
            time_collected
                .to_rfc3339_opts(SecondsFormat::Millis, /* use_z */ true),
            source
        )?;
        writeln!(indented, "address:     {}", sled_agent_address)?;
        writeln!(indented, "usable hw threads:   {}", usable_hardware_threads)?;
        writeln!(indented, "CPU family:          {}", cpu_family)?;
        writeln!(
            indented,
            "usable memory (GiB): {}",
            usable_physical_ram.to_whole_gibibytes()
        )?;
        writeln!(
            indented,
            "reservoir (GiB):     {}",
            reservoir_size.to_whole_gibibytes()
        )?;

        if !zpools.is_empty() {
            writeln!(indented, "physical disks:")?;
        }
        for disk in disks {
            let PhysicalDisk { identity, variant, slot, .. } = disk;
            let mut indent2 = IndentWriter::new("  ", &mut indented);
            writeln!(indent2, "{variant:?}: {identity:?} in {slot}")?;
        }

        if !zpools.is_empty() {
            writeln!(indented, "zpools")?;
        }
        for zpool in zpools {
            let Zpool { id, total_size, .. } = zpool;
            let mut indent2 = IndentWriter::new("  ", &mut indented);
            writeln!(indent2, "{id}: total size: {total_size}")?;
        }

        if !datasets.is_empty() {
            writeln!(indented, "datasets:")?;
        }
        for dataset in datasets {
            let Dataset {
                id,
                name,
                available,
                used,
                quota,
                reservation,
                compression,
            } = dataset;

            let id = if let Some(id) = id {
                id.to_string()
            } else {
                String::from("none")
            };

            {
                let mut indent2 = IndentWriter::new("  ", &mut indented);
                writeln!(
                    indent2,
                    "{name} - id: {id}, compression: {compression}"
                )?;
                let mut indent3 = IndentWriter::new("  ", &mut indent2);
                writeln!(indent3, "available: {available}, used: {used}")?;
                writeln!(
                    indent3,
                    "reservation: {reservation:?}, quota: {quota:?}"
                )?;
            }
        }

        f = indented.into_inner();

        if let Some(config) = &ledgered_sled_config {
            display_sled_config("LEDGERED", config, f)?;
        } else {
            writeln!(f, "    no ledgered sled config")?;
        }

        let mut indented = IndentWriter::new("    ", f);

        writeln!(indented, "zone image resolver status:")?;
        {
            let mut indent2 = IndentWriter::new("    ", &mut indented);
            // Use write! rather than writeln! since zone_image_resolver.display()
            // always produces a newline at the end.
            write!(indent2, "{}", zone_image_resolver.display())?;
        }

        writeln!(indented, "measurement resolver status:")?;
        {
            let mut indent2 = IndentWriter::new("    ", &mut indented);
            // Use write! rather than writeln! since zone_image_resolver.display()
            // always produces a newline at the end.
            write!(indent2, "{}", measurement_resolver.display())?;
        }

        if let Some(last_reconciliation) = &last_reconciliation {
            let ConfigReconcilerInventory {
                last_reconciled_config,
                external_disks,
                datasets,
                orphaned_datasets,
                zones,
                boot_partitions,
                remove_mupdate_override,
            } = last_reconciliation;

            display_boot_partition_contents(boot_partitions, &mut indented)?;

            if Some(last_reconciled_config) == ledgered_sled_config.as_ref() {
                writeln!(
                    indented,
                    "last reconciled config: matches ledgered config"
                )?;
            } else {
                let f = indented.into_inner();
                display_sled_config(
                    "LAST RECONCILED CONFIG",
                    &last_reconciled_config,
                    f,
                )?;
                indented = IndentWriter::new("    ", f);
            }

            {
                let mut indent2 = IndentWriter::new("    ", &mut indented);

                if let Some(remove_mupdate_override) = remove_mupdate_override {
                    match &remove_mupdate_override.boot_disk_result {
                        Ok(RemoveMupdateOverrideBootSuccessInventory::Removed) => {
                            writeln!(
                                indent2,
                                "removed mupdate override on boot disk",
                            )?;
                        }
                        Ok(
                            RemoveMupdateOverrideBootSuccessInventory::NoOverride,
                        ) => {
                            writeln!(
                                indent2,
                                "attempted to remove mupdate override \
                                 on boot disk, but no override was set",
                            )?;
                        }
                        Err(message) => {
                            writeln!(
                                indent2,
                                "failed to remove mupdate override \
                                 on boot disk: {message}",
                            )?;
                        }
                    }
                    writeln!(
                        indent2,
                        "remove mupdate override on non-boot disk:"
                    )?;

                    let mut indent3 = IndentWriter::new("  ", &mut indent2);
                    writeln!(
                        indent3,
                        "{}",
                        remove_mupdate_override.non_boot_message
                    )?;
                } else {
                    match &zone_image_resolver.mupdate_override.boot_override {
                        Ok(Some(_)) => {
                            writeln!(
                                indent2,
                                "mupdate override present, but sled agent was not \
                                 instructed to clear it"
                            )?;
                        }
                        Ok(None) => {
                            writeln!(indent2, "no mupdate override to clear")?;
                        }
                        Err(_) => {
                            writeln!(
                                indent2,
                                "error reading mupdate override, so sled agent \
                                 didn't attempt to clear it"
                            )?;
                        }
                    }
                }

                if orphaned_datasets.is_empty() {
                    writeln!(indent2, "no orphaned datasets")?;
                } else {
                    writeln!(
                        indent2,
                        "{} orphaned dataset(s):",
                        orphaned_datasets.len()
                    )?;
                    let mut indent3 = IndentWriter::new("    ", &mut indent2);
                    for orphan in orphaned_datasets {
                        display_one_orphaned_dataset(orphan, &mut indent3)?;
                    }
                }
                let disk_errs =
                    collect_config_reconciler_errors(&external_disks);
                let dataset_errs = collect_config_reconciler_errors(&datasets);
                let zone_errs = collect_config_reconciler_errors(&zones);
                for (label, errs) in [
                    ("disk", disk_errs),
                    ("dataset", dataset_errs),
                    ("zone", zone_errs),
                ] {
                    if errs.is_empty() {
                        writeln!(
                            indent2,
                            "all {label}s reconciled successfully"
                        )?;
                    } else {
                        writeln!(
                            indent2,
                            "{} {label} reconciliation errors:",
                            errs.len()
                        )?;
                        let mut indent3 = IndentWriter::new("  ", &mut indent2);
                        for err in errs {
                            writeln!(indent3, "{err}")?;
                        }
                    }
                }
            }
        }

        write!(indented, "reconciler task status: ")?;
        match &reconciler_status {
            ConfigReconcilerInventoryStatus::NotYetRun => {
                writeln!(indented, "not yet run")?;
            }
            ConfigReconcilerInventoryStatus::Running {
                config,
                started_at,
                running_for,
            } => {
                writeln!(
                    indented,
                    "running for {running_for:?} (since {started_at})"
                )?;
                if Some(config) == ledgered_sled_config.as_ref() {
                    writeln!(
                        indented,
                        "reconciling currently-ledgered config"
                    )?;
                } else {
                    let f = indented.into_inner();
                    display_sled_config("RECONCILING CONFIG", config, f)?;
                    indented = IndentWriter::new("    ", f);
                }
            }
            ConfigReconcilerInventoryStatus::Idle { completed_at, ran_for } => {
                writeln!(
                    indented,
                    "idle (finished at {} \
                         after running for {ran_for:?})",
                    completed_at.to_rfc3339_opts(
                        SecondsFormat::Millis,
                        /* use_z */ true,
                    )
                )?;
            }
        }

        f = indented.into_inner();
    }
    Ok(())
}

fn display_ntp_status(
    ntp_timesync: &IdOrdMap<TimeSync>,
    f: &mut dyn fmt::Write,
) -> fmt::Result {
    writeln!(f, "\nNTP STATUS")?;
    let mut f = IndentWriter::new("    ", f);

    let mut ntp_found = false;
    for ts in ntp_timesync {
        ntp_found = true;
        let zone_id = ts.zone_id;
        if ts.synced {
            writeln!(f, "Zone {zone_id}: NTP reports that time is synced")?;
        } else {
            writeln!(f, "Zone {zone_id}: NTP reports that time is NOT synced")?;
        }
    }

    if !ntp_found {
        writeln!(f, "No NTP zones reported timesync information")?;
    }

    Ok(())
}

fn display_boot_partition_contents(
    boot_partitions: &BootPartitionContents,
    f: &mut dyn fmt::Write,
) -> fmt::Result {
    let mut f = f;

    let BootPartitionContents { boot_disk, slot_a, slot_b } = &boot_partitions;
    write!(f, "boot disk slot: ")?;
    match boot_disk {
        Ok(slot) => writeln!(f, "{slot:?}")?,
        Err(err) => writeln!(f, "FAILED TO DETERMINE: {err}")?,
    }
    match slot_a {
        Ok(details) => {
            writeln!(f, "slot A details:")?;
            let mut indented = IndentWriter::new("    ", f);
            display_boot_partition_details(details, &mut indented)?;
            f = indented.into_inner();
        }
        Err(err) => {
            writeln!(f, "slot A details UNAVAILABLE: {err}")?;
        }
    }
    match slot_b {
        Ok(details) => {
            writeln!(f, "slot B details:")?;
            let mut indented = IndentWriter::new("    ", f);
            display_boot_partition_details(details, &mut indented)?;
        }
        Err(err) => {
            writeln!(f, "slot B details UNAVAILABLE: {err}")?;
        }
    }

    Ok(())
}

fn display_internal_dns_status(
    internal_dns_generation_status: &IdOrdMap<InternalDnsGenerationStatus>,
    f: &mut dyn fmt::Write,
) -> fmt::Result {
    writeln!(f, "\nINTERNAL DNS STATUS")?;
    let mut f = IndentWriter::new("    ", f);

    let mut internal_dns_found = false;
    for st in internal_dns_generation_status {
        internal_dns_found = true;
        writeln!(
            f,
            "Zone {}: Internal DNS generation @ {}",
            st.zone_id, st.generation
        )?
    }

    if !internal_dns_found {
        writeln!(f, "No Internal DNS zones found which reported a generation")?;
    }

    Ok(())
}

fn display_boot_partition_details(
    details: &BootPartitionDetails,
    f: &mut dyn fmt::Write,
) -> fmt::Result {
    let BootPartitionDetails { header, artifact_hash, artifact_size } = details;

    // Not sure it's useful to print all the header details? We'll omit for now.
    let BootImageHeader {
        flags: _,
        data_size: _,
        image_size: _,
        target_size: _,
        sha256,
        image_name,
    } = header;

    writeln!(f, "artifact: {artifact_hash} ({artifact_size} bytes)")?;
    writeln!(f, "image name: {image_name}")?;
    writeln!(f, "phase 2 hash: {}", ArtifactHash(*sha256))?;

    Ok(())
}

fn display_orphaned_datasets(
    collection: &Collection,
    f: &mut dyn fmt::Write,
) -> fmt::Result {
    // Helper for `unwrap_or()` passing borrow check below
    static EMPTY_SET: IdOrdMap<OrphanedDataset> = IdOrdMap::new();

    let mut f = f;
    writeln!(f, "ORPHANED DATASETS")?;
    for sled in &collection.sled_agents {
        writeln!(
            f,
            "\nsled {} (serial {})",
            sled.sled_id,
            match &sled.baseboard_id {
                Some(baseboard_id) => &baseboard_id.serial_number,
                None => "unknown",
            },
        )?;
        let orphaned_datasets = sled
            .last_reconciliation
            .as_ref()
            .map(|r| &r.orphaned_datasets)
            .unwrap_or(&EMPTY_SET);

        let mut indented = IndentWriter::new("    ", f);
        if orphaned_datasets.is_empty() {
            writeln!(indented, "no orphaned datasets")?;
        } else {
            writeln!(
                indented,
                "{} orphaned dataset(s):",
                orphaned_datasets.len()
            )?;
            let mut indent2 = IndentWriter::new("    ", &mut indented);
            for orphan in orphaned_datasets {
                display_one_orphaned_dataset(orphan, &mut indent2)?;
            }
        }

        f = indented.into_inner();
    }
    Ok(())
}

fn display_one_orphaned_dataset(
    orphan: &OrphanedDataset,
    f: &mut dyn fmt::Write,
) -> fmt::Result {
    let OrphanedDataset { name, reason, id, mounted, available, used } = orphan;
    let id = match id {
        Some(id) => id as &dyn fmt::Display,
        None => &"none (this is unexpected!)",
    };
    writeln!(f, "{}", name.full_name())?;
    let mut indented = IndentWriter::new("    ", f);
    writeln!(indented, "reason: {reason}")?;
    writeln!(indented, "dataset ID: {id}")?;
    writeln!(indented, "mounted: {mounted}")?;
    writeln!(indented, "available: {available}")?;
    writeln!(indented, "used: {used}")?;
    Ok(())
}

fn collect_config_reconciler_errors<T: Ord + fmt::Display>(
    results: &BTreeMap<T, ConfigReconcilerInventoryResult>,
) -> Vec<String> {
    results
        .iter()
        .filter_map(|(id, result)| match result {
            ConfigReconcilerInventoryResult::Ok => None,
            ConfigReconcilerInventoryResult::Err { message } => {
                Some(format!("{id}: {message}"))
            }
        })
        .collect()
}

fn display_sled_config(
    label: &str,
    config: &OmicronSledConfig,
    f: &mut dyn fmt::Write,
) -> fmt::Result {
    let OmicronSledConfig {
        generation,
        disks,
        datasets,
        zones,
        remove_mupdate_override,
        host_phase_2,
        // XXX FIXME
        measurements: _,
    } = config;

    writeln!(f, "\n{label} SLED CONFIG")?;
    let mut indented = IndentWriter::new("    ", f);

    writeln!(indented, "generation: {}", generation)?;
    writeln!(indented, "remove_mupdate_override: {remove_mupdate_override:?}")?;

    let display_host_phase_2_desired = |desired| match desired {
        HostPhase2DesiredContents::CurrentContents => {
            Cow::Borrowed("keep current contents")
        }
        HostPhase2DesiredContents::Artifact { hash } => {
            Cow::Owned(format!("artifact {hash}"))
        }
    };
    writeln!(
        indented,
        "desired host phase 2 slot a: {}",
        display_host_phase_2_desired(host_phase_2.slot_a)
    )?;
    writeln!(
        indented,
        "desired host phase 2 slot b: {}",
        display_host_phase_2_desired(host_phase_2.slot_b)
    )?;

    if disks.is_empty() {
        writeln!(indented, "disk config empty")?;
    } else {
        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct DiskRow {
            id: PhysicalDiskUuid,
            zpool_id: ZpoolUuid,
            vendor: String,
            model: String,
            serial: String,
        }

        let rows = disks.iter().map(|d| DiskRow {
            id: d.id,
            zpool_id: d.pool_id,
            vendor: d.identity.vendor.clone(),
            model: d.identity.model.clone(),
            serial: d.identity.serial.clone(),
        });
        let table = tabled::Table::new(rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(4, 1, 0, 0))
            .to_string();
        writeln!(indented, "DISKS: {}", disks.len())?;
        writeln!(indented, "{table}")?;
    }

    if datasets.is_empty() {
        writeln!(indented, "dataset config empty")?;
    } else {
        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct DatasetRow {
            id: DatasetUuid,
            name: String,
            compression: String,
            quota: String,
            reservation: String,
        }

        let rows = datasets.iter().map(|d| DatasetRow {
            id: d.id,
            name: d.name.full_name(),
            compression: d.inner.compression.to_string(),
            quota: d
                .inner
                .quota
                .map(|q| q.to_string())
                .unwrap_or_else(|| "none".to_string()),
            reservation: d
                .inner
                .reservation
                .map(|r| r.to_string())
                .unwrap_or_else(|| "none".to_string()),
        });
        let table = tabled::Table::new(rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(4, 1, 0, 0))
            .to_string();
        writeln!(indented, "DATASETS: {}", datasets.len())?;
        writeln!(indented, "{table}")?;
    }

    if zones.is_empty() {
        writeln!(indented, "zone config empty")?;
    } else {
        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct ZoneRow {
            id: OmicronZoneUuid,
            kind: &'static str,
            image_source: String,
        }

        let rows = zones.iter().map(|z| ZoneRow {
            id: z.id,
            kind: z.zone_type.kind().report_str(),
            image_source: match &z.image_source {
                OmicronZoneImageSource::InstallDataset => {
                    "install-dataset".to_string()
                }
                OmicronZoneImageSource::Artifact { hash } => {
                    format!("artifact: {hash}")
                }
            },
        });
        let table = tabled::Table::new(rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(4, 1, 0, 0))
            .to_string();
        writeln!(indented, "ZONES: {}", zones.len())?;
        writeln!(indented, "{table}")?;
    }

    Ok(())
}

fn display_keeper_membership(
    collection: &Collection,
    f: &mut dyn fmt::Write,
) -> fmt::Result {
    writeln!(f, "\nKEEPER MEMBERSHIP")?;
    for k in &collection.clickhouse_keeper_cluster_membership {
        writeln!(f, "\n    queried keeper: {}", k.queried_keeper)?;
        writeln!(
            f,
            "    leader_committed_log_index: {}",
            k.leader_committed_log_index
        )?;

        let s = k.raft_config.iter().join(", ");
        writeln!(f, "    raft config: {s}")?;
    }
    if collection.clickhouse_keeper_cluster_membership.is_empty() {
        writeln!(f, "    no membership retrieved")?;
    }
    writeln!(f, "")?;

    Ok(())
}

fn display_cockroach_status(
    collection: &Collection,
    f: &mut dyn fmt::Write,
) -> fmt::Result {
    writeln!(f, "\nCOCKROACH STATUS")?;
    let mut f = IndentWriter::new("    ", f);

    // Under normal conditions, cockroach nodes will report the same data. For
    // brevity, we will map "status" -> "nodes reporting that status", to avoid
    // emitting the same information repeatedly for each node.
    let mut status_to_node: HashMap<_, Vec<_>> = HashMap::new();

    for (node, status) in &collection.cockroach_status {
        status_to_node.entry(status).or_default().push(node);
    }

    for (status, nodes) in &status_to_node {
        writeln!(
            f,
            "status from nodes: {}",
            nodes.iter().map(|n| n.to_string()).join(", ")
        )?;

        writeln!(
            f,
            "ranges underreplicated: {}",
            status
                .ranges_underreplicated
                .map(|r| r.to_string())
                .unwrap_or_else(|| "<COULD NOT BE PARSED>".to_string())
        )?;
        writeln!(
            f,
            "live nodes: {}",
            status
                .liveness_live_nodes
                .map(|r| r.to_string())
                .unwrap_or_else(|| "<COULD NOT BE PARSED>".to_string())
        )?;
    }
    if status_to_node.is_empty() {
        writeln!(f, "no cockroach status retrieved")?;
    }
    writeln!(f)?;

    Ok(())
}

#[derive(Debug)]
struct LongStringFormatter {
    show_long_strings: bool,
}

impl LongStringFormatter {
    fn new() -> Self {
        LongStringFormatter { show_long_strings: false }
    }

    fn maybe_truncate<'a>(&self, s: &'a str) -> Cow<'a, str> {
        use unicode_width::UnicodeWidthChar;

        // pick an arbitrary width at which we'll truncate, knowing that these
        // strings are probably contained in tables with other columns
        const TRUNCATE_AT_WIDTH: usize = 32;

        // quick check for short strings or if we should show long strings in
        // their entirety
        if self.show_long_strings || s.len() <= TRUNCATE_AT_WIDTH {
            return s.into();
        }

        // longer check; we'll do the proper thing here and check the unicode
        // width, and we don't really care about speed, so we can just iterate
        // over chars
        let mut width = 0;
        for (pos, ch) in s.char_indices() {
            let ch_width = UnicodeWidthChar::width(ch).unwrap_or(0);
            if width + ch_width > TRUNCATE_AT_WIDTH {
                let (prefix, _) = s.split_at(pos);
                return format!("{prefix}...").into();
            }
            width += ch_width;
        }

        // if we didn't break out of the loop, `s` in its entirety is not too
        // wide, so return it as-is
        s.into()
    }
}

fn option_impl_display<T: fmt::Display>(t: &Option<T>) -> String {
    match t {
        Some(v) => format!("{v}"),
        None => String::from("n/a"),
    }
}
