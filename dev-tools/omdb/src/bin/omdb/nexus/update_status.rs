// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands related to update status

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    iter,
};

use super::UpdateStatusArgs;
use anyhow::Context;
use gateway_types::rot::RotSlot;
use nexus_types::internal_api::views::{
    HostPhase1Status, HostPhase2Status, MgsDrivenUpdateStatus,
    RotBootloaderStatus, RotStatus, SledAgentUpdateStatus, SpStatus,
    TufRepoVersion, UpdateStatus, ZoneStatus,
};
use omicron_common::disk::M2Slot;
use omicron_uuid_kinds::SledUuid;
use strum::IntoEnumIterator;
use tabled::Tabled;

/// Runs `omdb nexus update-status`
pub async fn cmd_nexus_update_status(
    client: &nexus_lockstep_client::Client,
    args: &UpdateStatusArgs,
) -> Result<(), anyhow::Error> {
    let UpdateStatusArgs { details } = args;

    let status = client
        .update_status()
        .await
        .context("retrieving update status")?
        .into_inner();

    if *details {
        print_status_details(status);
    } else {
        print_status_summary(status);
    }

    Ok(())
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, strum::EnumIter,
)]
enum Component {
    RotBootloader,
    Rot,
    Sp,
    HostPhase1,
    HostPhase2,
    Zone,
}

#[derive(Debug, Default)]
struct UpdateStatusSummaryBuilder {
    all_versions: BTreeSet<Cow<'static, str>>,
    counts: BTreeMap<Component, BTreeMap<Cow<'static, str>, usize>>,
}

impl UpdateStatusSummaryBuilder {
    fn insert(&mut self, component: Component, version: TufRepoVersion) {
        let version = match version {
            TufRepoVersion::Unknown => Cow::Borrowed("unknown"),
            TufRepoVersion::InstallDataset => Cow::Borrowed("install-dataset"),
            TufRepoVersion::Error(_) => Cow::Borrowed("error"),
            TufRepoVersion::Version(v) => Cow::Owned(v.to_string()),
        };

        self.all_versions.insert(version.clone());
        *self
            .counts
            .entry(component)
            .or_default()
            .entry(version)
            .or_default() += 1;
    }
}

fn print_status_summary(status: UpdateStatus) {
    let mut builder = UpdateStatusSummaryBuilder::default();

    let UpdateStatus { mgs_driven, sleds } = status;

    for sled in sleds {
        let SledAgentUpdateStatus { host_phase_2, sled_id: _, zones } = sled;

        builder.insert(Component::HostPhase2, host_phase_2.boot_disk_version());
        for z in zones {
            builder.insert(Component::Zone, z.version);
        }
    }

    for mgs in mgs_driven {
        let MgsDrivenUpdateStatus {
            baseboard_description: _,
            rot_bootloader,
            rot,
            sp,
            host_os_phase_1,
        } = mgs;

        builder.insert(Component::RotBootloader, rot_bootloader.stage0_version);
        builder.insert(Component::Sp, sp.slot0_version);
        builder.insert(Component::Rot, rot.active_slot_version());

        if let Some(version) = host_os_phase_1.active_slot_version() {
            builder.insert(Component::HostPhase1, version);
        }
    }

    let rows = Component::iter().map(|c| {
        let name = match c {
            Component::RotBootloader => "RoT bootloader",
            Component::Rot => "RoT",
            Component::Sp => "SP",
            Component::HostPhase1 => "Host OS (phase 1)",
            Component::HostPhase2 => "Host OS (phase 2)",
            Component::Zone => "Zone",
        };
        let builder = &builder;
        iter::once(Cow::Borrowed(name)).chain(builder.all_versions.iter().map(
            move |v| {
                let count = builder
                    .counts
                    .get(&c)
                    .and_then(|by_version| by_version.get(v))
                    .unwrap_or(&0);
                Cow::Owned(count.to_string())
            },
        ))
    });
    let columns =
        iter::once(Cow::Borrowed("")).chain(builder.all_versions.clone());

    let mut table = tabled::builder::Builder::new();
    table.push_record(columns);
    for row in rows {
        table.push_record(row);
    }
    let table = table
        .build()
        .with(tabled::settings::Style::psql())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("Count of each component type by system version:");
    println!();
    println!("{table}");
    println!();
    println!("To see each individual component, rerun with `--details`.");
}

fn print_status_details(status: UpdateStatus) {
    print_rot_bootloaders(
        status
            .mgs_driven
            .iter()
            .map(|s| (s.baseboard_description.clone(), &s.rot_bootloader)),
    );
    println!();
    print_rots(
        status
            .mgs_driven
            .iter()
            .map(|s| (s.baseboard_description.clone(), &s.rot)),
    );
    println!();
    print_sps(
        status
            .mgs_driven
            .iter()
            .map(|s| (s.baseboard_description.clone(), &s.sp)),
    );
    println!();
    print_host_phase_1s(
        status
            .mgs_driven
            .iter()
            .map(|s| (s.baseboard_description.clone(), &s.host_os_phase_1)),
    );
    println!();
    print_host_phase_2s(
        status.sleds.iter().map(|s| (s.sled_id, &s.host_phase_2)),
    );
    println!();
    print_zones(
        status
            .sleds
            .iter()
            .map(|s| (s.sled_id, s.zones.iter().cloned().collect())),
    );
}

fn print_zones(zones: impl Iterator<Item = (SledUuid, Vec<ZoneStatus>)>) {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct ZoneRow {
        sled_id: String,
        zone_type: String,
        zone_id: String,
        version: String,
    }

    let mut rows = Vec::new();
    for (sled_id, mut statuses) in zones {
        statuses.sort_unstable_by_key(|s| {
            (s.zone_type.kind(), s.zone_id, s.version.clone())
        });
        for status in statuses {
            rows.push(ZoneRow {
                sled_id: sled_id.to_string(),
                zone_type: status.zone_type.kind().name_prefix().into(),
                zone_id: status.zone_id.to_string(),
                version: status.version.to_string(),
            });
        }
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("Running Zones");
    println!("{}", table);
}

fn print_rot_bootloaders<'a>(
    bootloaders: impl Iterator<Item = (String, &'a RotBootloaderStatus)>,
) {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct BootloaderRow {
        baseboard_id: String,
        stage0_version: String,
        stage0_next_version: String,
    }

    let mut rows = Vec::new();
    for (baseboard_id, status) in bootloaders {
        let RotBootloaderStatus { stage0_version, stage0_next_version } =
            status;
        rows.push(BootloaderRow {
            baseboard_id,
            stage0_version: stage0_version.to_string(),
            stage0_next_version: stage0_next_version.to_string(),
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("Installed RoT Bootloader Software");
    println!("{}", table);
}

fn print_rots<'a>(rots: impl Iterator<Item = (String, &'a RotStatus)>) {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct RotRow {
        baseboard_id: String,
        slot_a_version: String,
        slot_b_version: String,
    }

    let mut rows = Vec::new();
    for (baseboard_id, status) in rots {
        let RotStatus { active_slot, slot_a_version, slot_b_version } = status;
        let (slot_a_suffix, slot_b_suffix) = match active_slot {
            Some(RotSlot::A) => (" (active)", ""),
            Some(RotSlot::B) => ("", " (active)"),
            // This is not expected! Be louder.
            None => ("", " (ACTIVE SLOT UNKNOWN)"),
        };
        rows.push(RotRow {
            baseboard_id,
            slot_a_version: format!("{slot_a_version}{slot_a_suffix}"),
            slot_b_version: format!("{slot_b_version}{slot_b_suffix}"),
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("Installed RoT Software");
    println!("{}", table);
}

fn print_sps<'a>(sps: impl Iterator<Item = (String, &'a SpStatus)>) {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct SpRow {
        baseboard_id: String,
        slot0_version: String,
        slot1_version: String,
    }

    let mut rows = Vec::new();
    for (baseboard_id, status) in sps {
        let SpStatus { slot0_version, slot1_version } = status;
        rows.push(SpRow {
            baseboard_id,
            slot0_version: slot0_version.to_string(),
            slot1_version: slot1_version.to_string(),
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("Installed SP Software");
    println!("{}", table);
}

fn print_host_phase_1s<'a>(
    phase_1s: impl Iterator<Item = (String, &'a HostPhase1Status)>,
) {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct HostPhase1Row {
        baseboard_id: String,
        sled_id: String,
        slot_a_version: String,
        slot_b_version: String,
    }

    let mut rows = Vec::new();
    for (baseboard_id, status) in phase_1s {
        match status {
            HostPhase1Status::NotASled => continue,
            HostPhase1Status::Sled {
                sled_id,
                active_slot,
                slot_a_version,
                slot_b_version,
            } => {
                let (slot_a_suffix, slot_b_suffix) = match active_slot {
                    Some(M2Slot::A) => (" (active)", ""),
                    Some(M2Slot::B) => ("", " (active)"),
                    // This is not expected! Be louder.
                    None => ("", " (ACTIVE SLOT UNKNOWN)"),
                };
                rows.push(HostPhase1Row {
                    baseboard_id,
                    sled_id: sled_id.to_string(),
                    slot_a_version: format!("{slot_a_version}{slot_a_suffix}"),
                    slot_b_version: format!("{slot_b_version}{slot_b_suffix}"),
                });
            }
        }
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("Installed Host Phase 1 Software");
    println!("{}", table);
}

fn print_host_phase_2s<'a>(
    sleds: impl Iterator<Item = (SledUuid, &'a HostPhase2Status)>,
) {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct HostPhase2Row {
        sled_id: String,
        slot_a_version: String,
        slot_b_version: String,
    }

    let mut rows = Vec::new();
    for (sled_id, status) in sleds {
        let HostPhase2Status { boot_disk, slot_a_version, slot_b_version } =
            status;
        let (slot_a_suffix, slot_b_suffix) = match boot_disk {
            Ok(M2Slot::A) => (" (boot disk)", "".to_string()),
            Ok(M2Slot::B) => ("", " (boot disk)".to_string()),
            // This is not expected! Be louder.
            Err(err) => ("", format!(" (BOOT DISK UNKNOWN: {err})")),
        };
        rows.push(HostPhase2Row {
            sled_id: sled_id.to_string(),
            slot_a_version: format!("{slot_a_version}{slot_a_suffix}"),
            slot_b_version: format!("{slot_b_version}{slot_b_suffix}"),
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("Installed Host Phase 2 Software");
    println!("{}", table);
}
