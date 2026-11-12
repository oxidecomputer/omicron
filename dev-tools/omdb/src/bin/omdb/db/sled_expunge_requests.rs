// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::helpers::const_max_len;
use crate::helpers::datetime_opt_rfc3339_concise;
use crate::helpers::datetime_rfc3339_concise;
use crate::helpers::display_option_dash;
use crate::helpers::sled_cubby_with_source_text;
use anyhow::Context;
use chrono::DateTime;
use chrono::Utc;
use clap::Args;
use clap::Subcommand;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::sled_expunge::SledExpungePowerOff;
use nexus_types::sled_expunge::SledExpungePowerOffProgress;
use nexus_types::sled_expunge::SledExpungeRequest;
use nexus_types::sled_expunge::SledExpungeRequestState;
use nexus_types::sled_expunge::SledExpungeRequestStateKind;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledUuid;
use swrite::SWrite as _;
use swrite::swriteln;
use tabled::Tabled;
use trust_quorum_types::types::Epoch;

#[derive(Debug, Args, Clone)]
pub(super) struct SledExpungeRequestsArgs {
    #[command(subcommand)]
    command: SledExpungeRequestsCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum SledExpungeRequestsCommands {
    #[command(about = "List sled expunge requests, oldest first")]
    List,
    #[command(about = "Show every recorded field of one sled expunge request")]
    Show(SledExpungeRequestShowArgs),
}

#[derive(Debug, Args, Clone)]
struct SledExpungeRequestShowArgs {
    #[arg(help = "The ID of the sled expunge request")]
    request_id: SledExpungeRequestUuid,
}

pub(super) async fn cmd_db_sled_expunge_requests(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &SledExpungeRequestsArgs,
) -> Result<(), anyhow::Error> {
    match &args.command {
        SledExpungeRequestsCommands::List => {
            cmd_db_sled_expunge_requests_list(opctx, datastore).await
        }
        SledExpungeRequestsCommands::Show(args) => {
            cmd_db_sled_expunge_requests_show(opctx, datastore, args).await
        }
    }
}

async fn cmd_db_sled_expunge_requests_list(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    let mut requests = Vec::new();
    let mut paginator =
        Paginator::new(SQL_BATCH_SIZE, dropshot::PaginationOrder::Ascending);
    while let Some(p) = paginator.next() {
        let batch = datastore
            .sled_expunge_request_list(opctx, &p.current_pagparams())
            .await
            .context("listing sled expunge requests")?;
        paginator = p.found_batch(&batch, &|r: &SledExpungeRequest| {
            (r.time_created, r.id.into_untyped_uuid())
        });
        requests.extend(batch);
    }
    print!("{}", sled_expunge_requests_list_text(&requests));
    Ok(())
}

async fn cmd_db_sled_expunge_requests_show(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &SledExpungeRequestShowArgs,
) -> Result<(), anyhow::Error> {
    let request_id = args.request_id;
    let request = datastore
        .sled_expunge_request_get(opctx, request_id)
        .await
        .with_context(|| {
        format!("fetching sled expunge request {request_id}")
    })?;
    print!("{}", sled_expunge_request_show_text(&request));
    Ok(())
}

fn is_set_label<T>(value: &Option<T>) -> &'static str {
    match value {
        Some(_) => "yes",
        None => "no",
    }
}

fn recorded_cubby_text(state: &SledExpungeRequestState) -> String {
    match state.recorded_cubby() {
        Some(cubby) => sled_cubby_with_source_text(cubby.slot, cubby.source),
        None => "-".to_string(),
    }
}

struct StateFields<'a> {
    power_off: Option<SledExpungePowerOff>,
    last_error: Option<&'a str>,
    failure_reason: Option<&'a str>,
    time_completed: Option<DateTime<Utc>>,
}

impl<'a> StateFields<'a> {
    fn new(state: &'a SledExpungeRequestState) -> Self {
        match state {
            SledExpungeRequestState::WaitingForTrustQuorum {
                cubby: _,
                last_error,
            }
            | SledExpungeRequestState::PoweringOff { cubby: _, last_error } => {
                Self {
                    power_off: None,
                    last_error: last_error.as_deref(),
                    failure_reason: None,
                    time_completed: None,
                }
            }
            SledExpungeRequestState::Expunging { power_off, last_error } => {
                Self {
                    power_off: Some(*power_off),
                    last_error: last_error.as_deref(),
                    failure_reason: None,
                    time_completed: None,
                }
            }
            SledExpungeRequestState::Done { power_off, time_completed } => {
                Self {
                    power_off: Some(*power_off),
                    last_error: None,
                    failure_reason: None,
                    time_completed: Some(*time_completed),
                }
            }
            SledExpungeRequestState::Failed {
                power_off,
                failure_reason,
                time_completed,
            } => Self {
                power_off: match power_off {
                    SledExpungePowerOffProgress::Pending { cubby: _ } => None,
                    SledExpungePowerOffProgress::Settled(power_off) => {
                        Some(*power_off)
                    }
                },
                last_error: None,
                failure_reason: Some(failure_reason),
                time_completed: Some(*time_completed),
            },
        }
    }
}

fn sled_expunge_requests_list_text(requests: &[SledExpungeRequest]) -> String {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct SledExpungeRequestRow<'a> {
        id: SledExpungeRequestUuid,
        rack_id: RackUuid,
        sled_id: SledUuid,
        serial: &'a str,
        cubby: String,
        state: SledExpungeRequestStateKind,
        tq_epoch: Epoch,
        last_error: &'static str,
        failure_reason: &'static str,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        time_created: DateTime<Utc>,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        time_modified: DateTime<Utc>,
    }

    let rows = requests.iter().map(|request| {
        let fields = StateFields::new(&request.state);
        SledExpungeRequestRow {
            id: request.id,
            rack_id: request.rack_id,
            sled_id: request.sled_id,
            serial: &request.baseboard_id.serial_number,
            cubby: recorded_cubby_text(&request.state),
            state: request.state.kind(),
            tq_epoch: request.tq_epoch,
            last_error: is_set_label(&fields.last_error),
            failure_reason: is_set_label(&fields.failure_reason),
            time_created: request.time_created,
            time_modified: request.time_modified,
        }
    });
    let mut table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    table.push('\n');
    table
}

fn sled_expunge_request_show_text(request: &SledExpungeRequest) -> String {
    const ID: &str = "ID";
    const RACK_ID: &str = "rack ID";
    const SLED_ID: &str = "sled ID";
    const PART_NUMBER: &str = "baseboard part number";
    const SERIAL_NUMBER: &str = "baseboard serial number";
    const SP_SLOT: &str = "last known cubby";
    const STATE: &str = "state";
    const TQ_EPOCH: &str = "trust quorum epoch (rack membership version)";
    const POWER_OFF_OUTCOME: &str = "power-off outcome";
    const TIME_CREATED: &str = "created at";
    const TIME_MODIFIED: &str = "last modified at";
    const TIME_COMPLETED: &str = "completed at";
    const LAST_ERROR: &str = "last error";
    const FAILURE_REASON: &str = "failure reason";
    const WIDTH: usize = const_max_len(&[
        ID,
        RACK_ID,
        SLED_ID,
        PART_NUMBER,
        SERIAL_NUMBER,
        SP_SLOT,
        STATE,
        TQ_EPOCH,
        POWER_OFF_OUTCOME,
        TIME_CREATED,
        TIME_MODIFIED,
        TIME_COMPLETED,
        LAST_ERROR,
        FAILURE_REASON,
    ]);

    let SledExpungeRequest {
        id,
        rack_id,
        sled_id,
        baseboard_id,
        tq_epoch,
        state,
        time_created,
        time_modified,
    } = request;
    let part_number = &baseboard_id.part_number;
    let serial_number = &baseboard_id.serial_number;
    let sp_slot = recorded_cubby_text(state);
    let StateFields { power_off, last_error, failure_reason, time_completed } =
        StateFields::new(state);
    let state = state.kind();
    let power_off_outcome =
        display_option_dash(&power_off.map(|power_off| power_off.label()));
    let time_created = datetime_rfc3339_concise(time_created);
    let time_modified = datetime_rfc3339_concise(time_modified);
    let time_completed = datetime_opt_rfc3339_concise(&time_completed);
    let last_error = display_option_dash(&last_error);
    let failure_reason = display_option_dash(&failure_reason);

    let mut text = String::new();
    swriteln!(text, "    {ID:>WIDTH$}: {id}");
    swriteln!(text, "    {RACK_ID:>WIDTH$}: {rack_id}");
    swriteln!(text, "    {SLED_ID:>WIDTH$}: {sled_id}");
    swriteln!(text, "    {PART_NUMBER:>WIDTH$}: {part_number}");
    swriteln!(text, "    {SERIAL_NUMBER:>WIDTH$}: {serial_number}");
    swriteln!(text, "    {SP_SLOT:>WIDTH$}: {sp_slot}");
    swriteln!(text, "    {STATE:>WIDTH$}: {state}");
    swriteln!(text, "    {TQ_EPOCH:>WIDTH$}: {tq_epoch}");
    swriteln!(text, "    {POWER_OFF_OUTCOME:>WIDTH$}: {power_off_outcome}");
    swriteln!(text, "    {TIME_CREATED:>WIDTH$}: {time_created}");
    swriteln!(text, "    {TIME_MODIFIED:>WIDTH$}: {time_modified}");
    swriteln!(text, "    {TIME_COMPLETED:>WIDTH$}: {time_completed}");
    swriteln!(text, "    {LAST_ERROR:>WIDTH$}: {last_error}");
    swriteln!(text, "    {FAILURE_REASON:>WIDTH$}: {failure_reason}");
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use gateway_test_utils::setup::sim_gimlet_serial;
    use nexus_types::sled_expunge::SledExpungeCubbyPowerOffOutcome;
    use nexus_types::sled_expunge::SledExpungeRecordedCubby;
    use nexus_types::sled_expunge::SledExpungeSpSlotSource;
    use sled_hardware_types::BaseboardId;
    use std::collections::BTreeSet;
    use strum::VariantArray;

    fn at_second(second: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + second, 0)
            .expect("the test timestamp is in range")
    }

    fn cubby(
        index: u8,
        source: SledExpungeSpSlotSource,
    ) -> SledExpungeRecordedCubby {
        SledExpungeRecordedCubby { slot: u16::from(index), source }
    }

    fn cubby_power_off(
        index: u8,
        source: SledExpungeSpSlotSource,
        outcome: SledExpungeCubbyPowerOffOutcome,
    ) -> SledExpungePowerOff {
        SledExpungePowerOff::Cubby { cubby: cubby(index, source), outcome }
    }

    fn request(
        index: u8,
        state: SledExpungeRequestState,
    ) -> SledExpungeRequest {
        let seconds = i64::from(index) * 100;
        SledExpungeRequest {
            id: SledExpungeRequestUuid::from_u128(
                0x5e1e_0000 + u128::from(index),
            ),
            rack_id: RackUuid::from_u128(0xc0ffee),
            sled_id: SledUuid::from_u128(0x51ed_0000 + u128::from(index)),
            baseboard_id: BaseboardId {
                part_number: String::from("913-0000019"),
                serial_number: sim_gimlet_serial(u16::from(index)),
            },
            tq_epoch: Epoch(u64::from(index) + 1),
            state,
            time_created: at_second(seconds),
            time_modified: at_second(seconds + 30),
        }
    }

    fn sample_requests() -> Vec<SledExpungeRequest> {
        use SledExpungeCubbyPowerOffOutcome as Outcome;
        use SledExpungeSpSlotSource as Source;
        vec![
            request(
                1,
                SledExpungeRequestState::WaitingForTrustQuorum {
                    cubby: Some(cubby(1, Source::Inventory)),
                    last_error: None,
                },
            ),
            request(
                2,
                SledExpungeRequestState::PoweringOff {
                    cubby: Some(cubby(2, Source::LiveScan)),
                    last_error: Some(String::from(
                        "test last error: the sled's power state is unknown",
                    )),
                },
            ),
            request(
                3,
                SledExpungeRequestState::Expunging {
                    power_off: cubby_power_off(
                        3,
                        Source::FoundAfterAbsent,
                        Outcome::PoweredOff,
                    ),
                    last_error: None,
                },
            ),
            request(
                4,
                SledExpungeRequestState::Done {
                    power_off: cubby_power_off(
                        4,
                        Source::Inventory,
                        Outcome::PoweredOff,
                    ),
                    time_completed: at_second(460),
                },
            ),
            request(
                5,
                SledExpungeRequestState::Failed {
                    power_off: SledExpungePowerOffProgress::Pending {
                        cubby: Some(cubby(5, Source::Inventory)),
                    },
                    failure_reason: String::from(
                        "test failure reason: the removal was aborted",
                    ),
                    time_completed: at_second(560),
                },
            ),
            request(
                6,
                SledExpungeRequestState::Expunging {
                    power_off: cubby_power_off(
                        6,
                        Source::Relocated,
                        Outcome::AlreadyOff,
                    ),
                    last_error: None,
                },
            ),
            request(
                7,
                SledExpungeRequestState::Expunging {
                    power_off: cubby_power_off(
                        7,
                        Source::Inventory,
                        Outcome::OtherBoardPresent,
                    ),
                    last_error: None,
                },
            ),
            request(
                8,
                SledExpungeRequestState::Expunging {
                    power_off: cubby_power_off(
                        8,
                        Source::Inventory,
                        Outcome::NotPresent,
                    ),
                    last_error: None,
                },
            ),
            request(
                9,
                SledExpungeRequestState::Done {
                    power_off: SledExpungePowerOff::AbsentAtCreation,
                    time_completed: at_second(960),
                },
            ),
        ]
    }

    #[test]
    fn test_sled_expunge_requests_display() {
        let requests = sample_requests();

        let states: BTreeSet<&str> = requests
            .iter()
            .map(|request| request.state.kind().label())
            .collect();
        let all_states: BTreeSet<&str> = SledExpungeRequestStateKind::ALL
            .iter()
            .map(|state| state.label())
            .collect();
        assert_eq!(states, all_states, "the samples cover every state");
        let outcomes: BTreeSet<&str> = requests
            .iter()
            .filter_map(|request| request.state.settled_power_off())
            .map(|power_off| power_off.label())
            .collect();
        let all_outcomes: BTreeSet<&str> =
            SledExpungeCubbyPowerOffOutcome::VARIANTS
                .iter()
                .map(|outcome| outcome.label())
                .chain([SledExpungePowerOff::AbsentAtCreation.label()])
                .collect();
        assert_eq!(
            outcomes, all_outcomes,
            "the samples cover every power-off outcome",
        );

        let mut output = String::new();
        swriteln!(output, "=== list (empty) ===");
        output.push_str(&sled_expunge_requests_list_text(&[]));
        swriteln!(output, "=== list ===");
        output.push_str(&sled_expunge_requests_list_text(&requests));
        for request in [&requests[0], &requests[3], &requests[4], &requests[8]]
        {
            swriteln!(output, "=== show: {} ===", request.state.kind());
            output.push_str(&sled_expunge_request_show_text(request));
        }
        expectorate::assert_contents(
            "tests/output/sled-expunge-requests.txt",
            &output,
        );
    }
}
