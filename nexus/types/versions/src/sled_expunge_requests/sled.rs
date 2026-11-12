// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO-RAINCLAUDE: module doc: sled types for version SLED_EXPUNGE_REQUESTS.

use crate::v2026_01_21_00::rack::RackMembershipVersion;
use chrono::{DateTime, Utc};
use omicron_uuid_kinds::{RackUuid, SledExpungeRequestUuid, SledUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::BaseboardId;

// TODO-RAINCLAUDE: API doc: parameters for sled_expunge.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SledExpungeParams {
    // TODO-RAINCLAUDE: API doc: the serial number of the sled to expunge; it must exactly equal the serial number of the sled in the path, confirming the caller means that sled.
    pub serial: String,
}

// TODO-RAINCLAUDE: API doc: state of a sled expunge request.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum SledExpungeRequestState {
    // TODO-RAINCLAUDE: API doc: waiting for the rack membership change that removes the sled to start committing.
    WaitingForTrustQuorum,
    // TODO-RAINCLAUDE: API doc: powering the sled off.
    PoweringOff,
    // TODO-RAINCLAUDE: API doc: the sled was powered off and is being expunged.
    Expunging,
    // TODO-RAINCLAUDE: API doc: the sled is expunged; recovery from the expungement may not have finished.
    Done,
    // TODO-RAINCLAUDE: API doc: the request failed; see failure_reason.
    Failed,
}

// TODO-RAINCLAUDE: API doc: how powering off the sled of an expunge request was settled.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum SledExpungePowerOffOutcome {
    // TODO-RAINCLAUDE: API doc: the sled was found running in its cubby and its cubby's power was cut.
    PoweredOff,
    // TODO-RAINCLAUDE: API doc: the sled was found in its cubby already powered off (A2), so nothing was done.
    AlreadyOff,
    // TODO-RAINCLAUDE: API doc: another sled was found in the sled's cubby, so nothing was done; the sled had been moved or removed.
    OtherBoardPresent,
    // TODO-RAINCLAUDE: API doc: the sled's service processor could not be reached and its cubby is empty or unpowered.
    NotPresent,
    // TODO-RAINCLAUDE: API doc: when the request was created, the sled was in neither inventory nor any powered cubby (its cubby was absent or unpowered), and it was still absent from inventory after the sled left the trust quorum.
    AbsentAtCreation,
}

// TODO-RAINCLAUDE: API doc: a request to expunge a sled.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SledExpungeRequest {
    // TODO-RAINCLAUDE: API doc: unique, immutable, system-controlled identifier for the request.
    pub id: SledExpungeRequestUuid,
    // TODO-RAINCLAUDE: API doc: the sled's rack.
    pub rack_id: RackUuid,
    // TODO-RAINCLAUDE: API doc: the sled being expunged.
    pub sled_id: SledUuid,
    // TODO-RAINCLAUDE: API doc: part and serial number of the sled being expunged.
    pub baseboard: BaseboardId,
    // TODO-RAINCLAUDE: API doc: the cubby the sled was last known to be in: where its service processor was found when the request was created, or later if a sled that was gone at creation reappears; absent if no cubby is known (the sled was gone at creation).
    pub cubby: Option<u16>,
    // TODO-RAINCLAUDE: API doc: current state of the request.
    pub state: SledExpungeRequestState,
    // TODO-RAINCLAUDE: API doc: version of the rack membership change that removes the sled, for rack_membership_status and rack_membership_abort on the sled's rack.
    pub rack_membership_version: RackMembershipVersion,
    // TODO-RAINCLAUDE: API doc: how powering the sled off was settled, once it has been; absent before then, and for a request that failed before it.
    pub power_off_outcome: Option<SledExpungePowerOffOutcome>,
    // TODO-RAINCLAUDE: API doc: why the request needs attention, if it does; the system keeps retrying, and it is cleared when the request moves to its next state.
    pub last_error: Option<String>,
    // TODO-RAINCLAUDE: API doc: why the request failed, if its state is failed.
    pub failure_reason: Option<String>,
    // TODO-RAINCLAUDE: API doc: timestamp when the request was created.
    pub time_created: DateTime<Utc>,
    // TODO-RAINCLAUDE: API doc: timestamp when the request was last modified.
    pub time_modified: DateTime<Utc>,
    // TODO-RAINCLAUDE: API doc: timestamp when the request reached done or failed.
    pub time_completed: Option<DateTime<Utc>>,
}

// TODO-RAINCLAUDE: API doc: what sled_expunge did.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum SledExpungeRequestDisposition {
    // TODO-RAINCLAUDE: API doc: a new request was created and its rack membership change proposed; whether the change reached its coordinator is not reported.
    Created,
    // TODO-RAINCLAUDE: API doc: the sled already has an unfinished request, which is returned unchanged; nothing new was started.
    AlreadyOutstanding,
}

// TODO-RAINCLAUDE: API doc: response to sled_expunge.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SledExpungeResponse {
    // TODO-RAINCLAUDE: API doc: the sled's expunge request.
    pub request: SledExpungeRequest,
    // TODO-RAINCLAUDE: API doc: what this call did.
    pub disposition: SledExpungeRequestDisposition,
}
