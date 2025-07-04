// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands related to update status

use anyhow::Context;
use nexus_types::internal_api::views::{SpStatus, ZoneStatus};
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

    print_zones(status.zones.into_iter());
    print_sps(status.sps.into_iter());

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

fn print_sps(sps: impl Iterator<Item = (String, SpStatus)>) {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct SpRow {
        baseboard_id: String,
        sled_id: String,
        slot0_version: String,
        slot0_git_commit: String,
        slot1_version: String,
        slot1_git_commit: String,
    }

    let mut rows = Vec::new();
    for (baseboard_id, status) in sps {
        let SpStatus {
            sled_id,
            slot0_version,
            slot0_git_commit,
            slot1_version,
            slot1_git_commit,
        } = status;
        rows.push(SpRow {
            baseboard_id,
            sled_id: sled_id.map_or("".to_string(), |id| id.to_string()),
            slot0_version,
            slot0_git_commit,
            slot1_version,
            slot1_git_commit,
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("Installed SP Software");
    println!("{}", table);
}
