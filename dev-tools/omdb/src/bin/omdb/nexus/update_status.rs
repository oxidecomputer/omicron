// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands related to update status

use anyhow::Context;
use nexus_types::internal_api::views::{
    RotBootloaderStatus, SpStatus, ZoneStatus,
};
use omicron_uuid_kinds::SledUuid;
use tabled::Tabled;

/// Runs `omdb nexus update-status`
pub async fn cmd_nexus_update_status(
    client: &nexus_client::Client,
) -> Result<(), anyhow::Error> {
    let status = client
        .update_status()
        .await
        .context("retrieving update status")?
        .into_inner();

    print_zones(
        status
            .sleds
            .iter()
            .map(|s| (s.sled_id, s.zones.iter().cloned().collect())),
    );
    print_rot_bootloaders(status.mgs_driven.iter().map(|s| {
        (s.baseboard_description.clone(), s.sled_id, &s.rot_bootloader)
    }));
    print_sps(
        status
            .mgs_driven
            .iter()
            .map(|s| (s.baseboard_description.clone(), s.sled_id, &s.sp)),
    );

    Ok(())
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
    sps: impl Iterator<Item = (String, Option<SledUuid>, &'a RotBootloaderStatus)>,
) {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct BootloaderRow {
        baseboard_id: String,
        sled_id: String,
        stage0_version: String,
        stage0_next_version: String,
    }

    let mut rows = Vec::new();
    for (baseboard_id, sled_id, status) in sps {
        let RotBootloaderStatus { stage0_version, stage0_next_version } =
            status;
        rows.push(BootloaderRow {
            baseboard_id,
            sled_id: sled_id.map_or("".to_string(), |id| id.to_string()),
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

fn print_sps<'a>(
    sps: impl Iterator<Item = (String, Option<SledUuid>, &'a SpStatus)>,
) {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct SpRow {
        baseboard_id: String,
        sled_id: String,
        slot0_version: String,
        slot1_version: String,
    }

    let mut rows = Vec::new();
    for (baseboard_id, sled_id, status) in sps {
        let SpStatus { slot0_version, slot1_version } = status;
        rows.push(SpRow {
            baseboard_id,
            sled_id: sled_id.map_or("".to_string(), |id| id.to_string()),
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
