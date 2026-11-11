// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DbFetchOptions;
use super::check_limit;
use super::first_page;
use crate::helpers::const_max_len;
use crate::helpers::datetime_opt_rfc3339_concise;
use crate::helpers::datetime_rfc3339_concise;
use crate::helpers::display_option_dash;
use anyhow::Context;
use chrono::DateTime;
use chrono::Utc;
use clap::Args;
use clap::Subcommand;
use nexus_db_model::SledExpungeRequest;
use nexus_db_model::SledExpungeRequestState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledUuid;
use std::fmt;
use swrite::SWrite as _;
use swrite::swriteln;
use tabled::Tabled;

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
    fetch_opts: &DbFetchOptions,
    args: &SledExpungeRequestsArgs,
) -> Result<(), anyhow::Error> {
    match &args.command {
        SledExpungeRequestsCommands::List => {
            cmd_db_sled_expunge_requests_list(opctx, datastore, fetch_opts)
                .await
        }
        SledExpungeRequestsCommands::Show(args) => {
            cmd_db_sled_expunge_requests_show(opctx, datastore, args).await
        }
    }
}

async fn cmd_db_sled_expunge_requests_list(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
) -> Result<(), anyhow::Error> {
    let limit = fetch_opts.fetch_limit;
    let requests = datastore
        .sled_expunge_request_list(opctx, &first_page(limit))
        .await
        .context("listing sled expunge requests")?;
    check_limit(&requests, limit, || {
        "listing sled expunge requests (oldest first, so the newest requests \
         are the ones omitted)"
    });
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

// TODO-RAINCLAUDE: support-facing reading of the row's send_uncertain column; Settled covers both a send the coordinator accepted and a send never made because its coordinator is not an in-service sled (trust_quorum_manager then aborts the config), the two outcomes after which creation clears the column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TrustQuorumSend {
    Uncertain,
    Settled,
}

// TODO-RAINCLAUDE: the sled_expunge task reads send_uncertain only in its waiting_for_trust_quorum gate (to pick the stuck-preparing text); once a request has left that state the flag no longer affects it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TrustQuorumSendRelevance {
    GatesProgress,
    NoLongerRelevant,
}

impl TrustQuorumSendRelevance {
    fn for_state(state: SledExpungeRequestState) -> Self {
        match state {
            SledExpungeRequestState::WaitingForTrustQuorum => {
                Self::GatesProgress
            }
            SledExpungeRequestState::PoweringOff
            | SledExpungeRequestState::Expunging
            | SledExpungeRequestState::Done
            | SledExpungeRequestState::Failed => Self::NoLongerRelevant,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TrustQuorumSendDisplay {
    send: TrustQuorumSend,
    relevance: TrustQuorumSendRelevance,
}

impl TrustQuorumSendDisplay {
    fn new(request: &SledExpungeRequest) -> Self {
        let send = if request.send_uncertain {
            TrustQuorumSend::Uncertain
        } else {
            TrustQuorumSend::Settled
        };
        let relevance = TrustQuorumSendRelevance::for_state(request.state);
        Self { send, relevance }
    }

    fn label(self) -> &'static str {
        match (self.send, self.relevance) {
            (
                TrustQuorumSend::Uncertain,
                TrustQuorumSendRelevance::GatesProgress,
            ) => "UNCERTAIN",
            (
                TrustQuorumSend::Uncertain,
                TrustQuorumSendRelevance::NoLongerRelevant,
            ) => "uncertain",
            (
                TrustQuorumSend::Settled,
                TrustQuorumSendRelevance::GatesProgress
                | TrustQuorumSendRelevance::NoLongerRelevant,
            ) => "settled",
        }
    }

    fn explanation(self, request_id: SledExpungeRequestUuid) -> Vec<String> {
        let mut lines = Vec::new();
        match self.send {
            TrustQuorumSend::Uncertain => {
                lines.push(String::from(
                    "Nexus has not recorded that the trust quorum change \
                     removing this sled reached its coordinator, or that it \
                     was not sent because its coordinator is no longer an \
                     in-service sled: the send may not have \
                     happened yet, may have failed or not been attempted, or \
                     may have succeeded without its result being saved.",
                ));
                lines.push(format!(
                    "To see what happened to the send, search the Nexus \
                     logs for request_id {request_id}",
                ));
                match self.relevance {
                    TrustQuorumSendRelevance::GatesProgress => {}
                    TrustQuorumSendRelevance::NoLongerRelevant => {
                        lines.push(String::from(
                            "This only matters while the request is \
                             waiting_for_trust_quorum.",
                        ));
                    }
                }
            }
            TrustQuorumSend::Settled => {
                lines.push(String::from(
                    "Nexus recorded that the trust quorum change removing \
                     this sled reached its coordinator, or that it was not \
                     sent because its coordinator is no longer an in-service \
                     sled (the trust quorum background task then aborts \
                     it).",
                ));
            }
        }
        lines
    }
}

impl fmt::Display for TrustQuorumSendDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

fn is_set_label<T>(value: &Option<T>) -> &'static str {
    match value {
        Some(_) => "yes",
        None => "no",
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
        state: SledExpungeRequestState,
        tq_epoch: i64,
        tq_send: TrustQuorumSendDisplay,
        last_error: &'static str,
        failure_reason: &'static str,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        time_created: DateTime<Utc>,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        time_modified: DateTime<Utc>,
    }

    let rows = requests.iter().map(|request| SledExpungeRequestRow {
        id: request.id(),
        rack_id: request.rack_id.into(),
        sled_id: request.sled_id.into(),
        serial: &request.serial_number,
        state: request.state,
        tq_epoch: request.tq_epoch,
        tq_send: TrustQuorumSendDisplay::new(request),
        last_error: is_set_label(&request.last_error),
        failure_reason: is_set_label(&request.failure_reason),
        time_created: request.time_created,
        time_modified: request.time_modified,
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
    const STATE: &str = "state";
    const TQ_EPOCH: &str = "trust quorum epoch (rack membership version)";
    const TQ_SEND: &str = "trust quorum send";
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
        STATE,
        TQ_EPOCH,
        TQ_SEND,
        POWER_OFF_OUTCOME,
        TIME_CREATED,
        TIME_MODIFIED,
        TIME_COMPLETED,
        LAST_ERROR,
        FAILURE_REASON,
    ]);
    const CONTINUATION: &str = "";

    let SledExpungeRequest {
        id,
        time_created,
        time_modified,
        rack_id,
        sled_id,
        part_number,
        serial_number,
        tq_epoch,
        state,
        power_off_outcome,
        send_uncertain: _,
        last_error,
        failure_reason,
        time_completed,
    } = request;
    let id = SledExpungeRequestUuid::from(*id);
    let rack_id = RackUuid::from(*rack_id);
    let sled_id = SledUuid::from(*sled_id);
    let tq_send = TrustQuorumSendDisplay::new(request);
    let power_off_outcome = display_option_dash(power_off_outcome);
    let time_created = datetime_rfc3339_concise(time_created);
    let time_modified = datetime_rfc3339_concise(time_modified);
    let time_completed = datetime_opt_rfc3339_concise(time_completed);
    let last_error = last_error.as_deref().unwrap_or("-");
    let failure_reason = failure_reason.as_deref().unwrap_or("-");

    let mut text = String::new();
    swriteln!(text, "    {ID:>WIDTH$}: {id}");
    swriteln!(text, "    {RACK_ID:>WIDTH$}: {rack_id}");
    swriteln!(text, "    {SLED_ID:>WIDTH$}: {sled_id}");
    swriteln!(text, "    {PART_NUMBER:>WIDTH$}: {part_number}");
    swriteln!(text, "    {SERIAL_NUMBER:>WIDTH$}: {serial_number}");
    swriteln!(text, "    {STATE:>WIDTH$}: {state}");
    swriteln!(text, "    {TQ_EPOCH:>WIDTH$}: {tq_epoch}");
    swriteln!(text, "    {TQ_SEND:>WIDTH$}: {tq_send}");
    for line in tq_send.explanation(id) {
        swriteln!(text, "    {CONTINUATION:>WIDTH$}  {line}");
    }
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
    use nexus_db_model::SledExpungePowerOffOutcome;
    use std::collections::BTreeSet;

    fn at_second(second: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + second, 0)
            .expect("the test timestamp is in range")
    }

    fn request(
        index: u8,
        state: SledExpungeRequestState,
    ) -> SledExpungeRequest {
        let seconds = i64::from(index) * 100;
        SledExpungeRequest {
            id: SledExpungeRequestUuid::from_u128(
                0x5e1e_0000 + u128::from(index),
            )
            .into(),
            time_created: at_second(seconds),
            time_modified: at_second(seconds + 30),
            rack_id: RackUuid::from_u128(0xc0ffee).into(),
            sled_id: SledUuid::from_u128(0x51ed_0000 + u128::from(index))
                .into(),
            part_number: String::from("913-0000019"),
            serial_number: format!("SimGimlet0{index}"),
            tq_epoch: i64::from(index) + 1,
            state,
            power_off_outcome: None,
            send_uncertain: false,
            last_error: None,
            failure_reason: None,
            time_completed: None,
        }
    }

    fn sample_requests() -> Vec<SledExpungeRequest> {
        vec![
            SledExpungeRequest {
                send_uncertain: true,
                ..request(1, SledExpungeRequestState::WaitingForTrustQuorum)
            },
            SledExpungeRequest {
                last_error: Some(String::from(
                    "test last error: the sled's power state is unknown",
                )),
                ..request(2, SledExpungeRequestState::PoweringOff)
            },
            SledExpungeRequest {
                power_off_outcome: Some(SledExpungePowerOffOutcome::PoweredOff),
                ..request(3, SledExpungeRequestState::Expunging)
            },
            SledExpungeRequest {
                power_off_outcome: Some(SledExpungePowerOffOutcome::PoweredOff),
                time_completed: Some(at_second(460)),
                ..request(4, SledExpungeRequestState::Done)
            },
            SledExpungeRequest {
                send_uncertain: true,
                failure_reason: Some(String::from(
                    "test failure reason: the removal was aborted",
                )),
                time_completed: Some(at_second(560)),
                ..request(5, SledExpungeRequestState::Failed)
            },
        ]
    }

    #[test]
    fn test_sled_expunge_requests_display() {
        let requests = sample_requests();

        let states: BTreeSet<&str> =
            requests.iter().map(|request| request.state.label()).collect();
        let all_states: BTreeSet<&str> = SledExpungeRequestState::ALL
            .iter()
            .map(|state| state.label())
            .collect();
        assert_eq!(states, all_states, "the samples cover every state");
        let outcomes: BTreeSet<&str> = requests
            .iter()
            .filter_map(|request| request.power_off_outcome)
            .map(|outcome| outcome.label())
            .collect();
        let all_outcomes: BTreeSet<&str> = SledExpungePowerOffOutcome::ALL
            .iter()
            .map(|outcome| outcome.label())
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
        for request in [&requests[0], &requests[3], &requests[4]] {
            swriteln!(output, "=== show: {} ===", request.state);
            output.push_str(&sled_expunge_request_show_text(request));
        }
        expectorate::assert_contents(
            "tests/output/sled-expunge-requests.txt",
            &output,
        );
    }
}
