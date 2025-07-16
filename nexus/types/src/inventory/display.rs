// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code to display inventory collections.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::LazyLock,
};

use chrono::SecondsFormat;
use clap::Subcommand;
use gateway_client::types::SpType;
use iddqd::IdOrdMap;
use itertools::Itertools;
use nexus_sled_agent_shared::inventory::{
    BootImageHeader, BootPartitionContents, BootPartitionDetails,
    ConfigReconcilerInventory, ConfigReconcilerInventoryResult,
    ConfigReconcilerInventoryStatus, HostPhase2DesiredContents,
    OmicronSledConfig, OmicronZoneImageSource, OrphanedDataset,
};
use omicron_uuid_kinds::{
    DatasetUuid, OmicronZoneUuid, PhysicalDiskUuid, ZpoolUuid,
};
use strum::IntoEnumIterator;
use tabled::Tabled;
use tufaceous_artifact::ArtifactHash;
use uuid::Uuid;

use crate::inventory::{
    CabooseWhich, Collection, Dataset, PhysicalDisk, RotPageWhich, Zpool,
};

/// Code to display inventory collections.
pub struct CollectionDisplay<'a> {
    collection: &'a Collection,
    include_sps: CollectionDisplayIncludeSps,
    include_sleds: bool,
    include_orphaned_datasets: bool,
    include_clickhouse_keeper_membership: bool,
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
        }
        if self.include_orphaned_datasets {
            display_orphaned_datasets(&self.collection, f)?;
        }
        if self.include_clickhouse_keeper_membership {
            display_keeper_membership(&self.collection, f)?;
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
        if let SpType::Sled = sp.sp_type {
            write!(f, " (cubby {})", sp.sp_slot)?;
        }
        writeln!(f, "")?;
        writeln!(f, "    found at: {} from {}", sp.time_collected, sp.source)?;

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
    writeln!(f, "SLED AGENTS")?;
    for sled in &collection.sled_agents {
        writeln!(
            f,
            "\nsled {} (role = {:?}, serial {})",
            sled.sled_id,
            sled.sled_role,
            match &sled.baseboard_id {
                Some(baseboard_id) => &baseboard_id.serial_number,
                None => "unknown",
            },
        )?;
        writeln!(
            f,
            "    found at:    {} from {}",
            sled.time_collected
                .to_rfc3339_opts(SecondsFormat::Millis, /* use_z */ true),
            sled.source
        )?;
        writeln!(f, "    address:     {}", sled.sled_agent_address)?;
        writeln!(
            f,
            "    usable hw threads:   {}",
            sled.usable_hardware_threads
        )?;
        writeln!(
            f,
            "    usable memory (GiB): {}",
            sled.usable_physical_ram.to_whole_gibibytes()
        )?;
        writeln!(
            f,
            "    reservoir (GiB):     {}",
            sled.reservoir_size.to_whole_gibibytes()
        )?;

        if !sled.zpools.is_empty() {
            writeln!(f, "    physical disks:")?;
        }
        for disk in &sled.disks {
            let PhysicalDisk { identity, variant, slot, .. } = disk;
            writeln!(f, "      {variant:?}: {identity:?} in {slot}")?;
        }

        if !sled.zpools.is_empty() {
            writeln!(f, "    zpools")?;
        }
        for zpool in &sled.zpools {
            let Zpool { id, total_size, .. } = zpool;
            writeln!(f, "      {id}: total size: {total_size}")?;
        }

        if !sled.datasets.is_empty() {
            writeln!(f, "    datasets:")?;
        }
        for dataset in &sled.datasets {
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

            writeln!(f, "      {name} - id: {id}, compression: {compression}")?;
            writeln!(f, "        available: {available}, used: {used}")?;
            writeln!(
                f,
                "        reservation: {reservation:?}, quota: {quota:?}"
            )?;
        }

        if let Some(config) = &sled.ledgered_sled_config {
            display_sled_config("LEDGERED", config, f)?;
        } else {
            writeln!(f, "    no ledgered sled config")?;
        }

        if let Some(last_reconciliation) = &sled.last_reconciliation {
            let ConfigReconcilerInventory {
                last_reconciled_config,
                external_disks,
                datasets,
                orphaned_datasets,
                zones,
                boot_partitions,
            } = last_reconciliation;

            display_boot_partition_contents("    ", boot_partitions, f)?;

            if Some(last_reconciled_config)
                == sled.ledgered_sled_config.as_ref()
            {
                writeln!(
                    f,
                    "    last reconciled config: matches ledgered config"
                )?;
            } else {
                display_sled_config(
                    "LAST RECONCILED CONFIG",
                    &last_reconciled_config,
                    f,
                )?;
            }
            if orphaned_datasets.is_empty() {
                writeln!(f, "        no orphaned datasets")?;
            } else {
                writeln!(
                    f,
                    "        {} orphaned dataset(s):",
                    orphaned_datasets.len()
                )?;
                for orphan in orphaned_datasets {
                    display_one_orphaned_dataset("            ", orphan, f)?;
                }
            }
            let disk_errs = collect_config_reconciler_errors(&external_disks);
            let dataset_errs = collect_config_reconciler_errors(&datasets);
            let zone_errs = collect_config_reconciler_errors(&zones);
            for (label, errs) in [
                ("disk", disk_errs),
                ("dataset", dataset_errs),
                ("zone", zone_errs),
            ] {
                if errs.is_empty() {
                    writeln!(
                        f,
                        "        all {label}s reconciled successfully"
                    )?;
                } else {
                    writeln!(
                        f,
                        "        {} {label} reconciliation errors:",
                        errs.len()
                    )?;
                    for err in errs {
                        writeln!(f, "          {err}")?;
                    }
                }
            }
        }

        write!(f, "    reconciler task status: ")?;
        match &sled.reconciler_status {
            ConfigReconcilerInventoryStatus::NotYetRun => {
                writeln!(f, "not yet run")?;
            }
            ConfigReconcilerInventoryStatus::Running {
                config,
                started_at,
                running_for,
            } => {
                writeln!(
                    f,
                    "running for {running_for:?} (since {started_at})"
                )?;
                if Some(config) == sled.ledgered_sled_config.as_ref() {
                    writeln!(f, "    reconciling currently-ledgered config")?;
                } else {
                    display_sled_config("RECONCILING CONFIG", config, f)?;
                }
            }
            ConfigReconcilerInventoryStatus::Idle { completed_at, ran_for } => {
                writeln!(
                    f,
                    "idle (finished at {} \
                     after running for {ran_for:?})",
                    completed_at.to_rfc3339_opts(
                        SecondsFormat::Millis,
                        /* use_z */ true,
                    )
                )?;
            }
        }
    }
    Ok(())
}

fn display_boot_partition_contents(
    indent: &str,
    boot_partitions: &BootPartitionContents,
    f: &mut dyn fmt::Write,
) -> fmt::Result {
    let BootPartitionContents { boot_disk, slot_a, slot_b } = &boot_partitions;
    write!(f, "{indent}boot disk slot: ")?;
    match boot_disk {
        Ok(slot) => writeln!(f, "{slot:?}")?,
        Err(err) => writeln!(f, "FAILED TO DETERMINE: {err}")?,
    }
    match slot_a {
        Ok(details) => {
            writeln!(f, "{indent}slot A details:")?;
            display_boot_partition_details(
                &format!("{indent}    "),
                details,
                f,
            )?;
        }
        Err(err) => {
            writeln!(f, "{indent}slot A details UNAVAILABLE: {err}")?;
        }
    }
    match slot_b {
        Ok(details) => {
            writeln!(f, "{indent}slot B details:")?;
            display_boot_partition_details(
                &format!("{indent}    "),
                details,
                f,
            )?;
        }
        Err(err) => {
            writeln!(f, "{indent}slot B details UNAVAILABLE: {err}")?;
        }
    }

    Ok(())
}

fn display_boot_partition_details(
    indent: &str,
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

    writeln!(f, "{indent}artifact: {artifact_hash} ({artifact_size} bytes)")?;
    writeln!(f, "{indent}image name: {image_name}")?;
    writeln!(f, "{indent}phase 2 hash: {}", ArtifactHash(*sha256))?;

    Ok(())
}

fn display_orphaned_datasets(
    collection: &Collection,
    f: &mut dyn fmt::Write,
) -> fmt::Result {
    // Helper for `unwrap_or()` passing borrow check below
    static EMPTY_SET: LazyLock<IdOrdMap<OrphanedDataset>> =
        LazyLock::new(IdOrdMap::new);

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
            .unwrap_or(&*EMPTY_SET);
        if orphaned_datasets.is_empty() {
            writeln!(f, "    no orphaned datasets")?;
        } else {
            writeln!(
                f,
                "    {} orphaned dataset(s):",
                orphaned_datasets.len()
            )?;
            for orphan in orphaned_datasets {
                display_one_orphaned_dataset("        ", orphan, f)?;
            }
        }
    }
    Ok(())
}

fn display_one_orphaned_dataset(
    indent: &str,
    orphan: &OrphanedDataset,
    f: &mut dyn fmt::Write,
) -> fmt::Result {
    let OrphanedDataset { name, reason, id, mounted, available, used } = orphan;
    let id = match id {
        Some(id) => id as &dyn fmt::Display,
        None => &"none (this is unexpected!)",
    };
    writeln!(f, "{indent}{}", name.full_name())?;
    writeln!(f, "{indent}    reason: {reason}")?;
    writeln!(f, "{indent}    dataset ID: {id}")?;
    writeln!(f, "{indent}    mounted: {mounted}")?;
    writeln!(f, "{indent}    available: {available}")?;
    writeln!(f, "{indent}    used: {used}")?;
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
    } = config;

    writeln!(f, "\n{label} SLED CONFIG")?;
    writeln!(f, "    generation: {}", generation)?;
    writeln!(f, "    remove_mupdate_override: {remove_mupdate_override:?}")?;

    let display_host_phase_2_desired = |desired| match desired {
        HostPhase2DesiredContents::CurrentContents => {
            Cow::Borrowed("keep current contents")
        }
        HostPhase2DesiredContents::Artifact { hash } => {
            Cow::Owned(format!("artifact {hash}"))
        }
    };
    writeln!(
        f,
        "    desired host phase 2 slot a: {}",
        display_host_phase_2_desired(host_phase_2.slot_a)
    )?;
    writeln!(
        f,
        "    desired host phase 2 slot b: {}",
        display_host_phase_2_desired(host_phase_2.slot_b)
    )?;

    if disks.is_empty() {
        writeln!(f, "    disk config empty")?;
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
            .with(tabled::settings::Padding::new(8, 1, 0, 0))
            .to_string();
        writeln!(f, "    DISKS: {}", disks.len())?;
        writeln!(f, "{table}")?;
    }

    if datasets.is_empty() {
        writeln!(f, "    dataset config empty")?;
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
            .with(tabled::settings::Padding::new(8, 1, 0, 0))
            .to_string();
        writeln!(f, "    DATASETS: {}", datasets.len())?;
        writeln!(f, "{table}")?;
    }

    if zones.is_empty() {
        writeln!(f, "    zone config empty")?;
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
            .with(tabled::settings::Padding::new(8, 1, 0, 0))
            .to_string();
        writeln!(f, "    ZONES: {}", zones.len())?;
        writeln!(f, "{table}")?;
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
        writeln!(f, "No membership retrieved.")?;
    }
    writeln!(f, "")?;

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
