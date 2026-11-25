// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2026_01_21_00::rack::RackMembershipVersion;
use chrono::{DateTime, Utc};
use omicron_uuid_kinds::{RackUuid, SledExpungeRequestUuid, SledUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware_types::BaseboardId;

// TODO-RAINCLAUDE: Parameters for expunging a sled
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SledExpungeParams {
    // TODO-RAINCLAUDE: Serial number of the sled to expunge
    //
    // This must exactly match the serial number of the sled in the path. It
    // confirms that the caller means that sled.
    pub serial: String,
}

// TODO-RAINCLAUDE: State of a sled expunge request, with the details specific
// to that state
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SledExpungeRequestState {
    // TODO-RAINCLAUDE: The request is waiting for the rack membership change
    // that removes the sled to start committing.
    WaitingForTrustQuorum {
        // TODO-RAINCLAUDE: Cubby where the sled's service processor was found
        // when the request was created
        //
        // This is absent if the sled was not found at that time.
        cubby: Option<SledExpungeCubby>,
        // TODO-RAINCLAUDE: Why the request needs attention, if it does
        //
        // The system keeps retrying. This is cleared when the request moves to
        // its next state.
        last_error: Option<String>,
    },
    // TODO-RAINCLAUDE: The sled is being powered off.
    PoweringOff {
        // TODO-RAINCLAUDE: Cubby the sled was last known to be in
        //
        // This is absent if no cubby is known.
        cubby: Option<SledExpungeCubby>,
        // TODO-RAINCLAUDE: Why the request needs attention, if it does
        //
        // The system keeps retrying. This is cleared when the request moves to
        // its next state.
        last_error: Option<String>,
    },
    // TODO-RAINCLAUDE: Powering the sled off has been settled, and the sled is
    // being expunged.
    Expunging {
        // TODO-RAINCLAUDE: How powering the sled off was settled
        power_off: SledExpungePowerOffOutcome,
        // TODO-RAINCLAUDE: Why the request needs attention, if it does
        //
        // The system keeps retrying. This is cleared when the request moves to
        // its next state.
        last_error: Option<String>,
    },
    // TODO-RAINCLAUDE: The sled has been expunged.
    //
    // Note that recovery from the expungement may not have finished.
    Done {
        // TODO-RAINCLAUDE: How powering the sled off was settled
        power_off: SledExpungePowerOffOutcome,
        // TODO-RAINCLAUDE: Timestamp when the request completed
        time_completed: DateTime<Utc>,
    },
    // TODO-RAINCLAUDE: The request failed.
    Failed {
        // TODO-RAINCLAUDE: Why the request failed
        failure_reason: String,
        // TODO-RAINCLAUDE: Whether powering the sled off was settled before the
        // request failed, and if so, how
        power_off: SledExpungePowerOffProgress,
        // TODO-RAINCLAUDE: Timestamp when the request failed
        time_completed: DateTime<Utc>,
    },
}

// TODO-RAINCLAUDE: Cubby the sled of an expunge request was last known to be
// in, and how the system learned that
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq,
)]
pub struct SledExpungeCubby {
    // TODO-RAINCLAUDE: Cubby number
    //
    // This uses the same numbering as `Sled.slot`. Both are the slot number
    // that the management gateway reports for the sled's service processor.
    pub slot: u16,
    // TODO-RAINCLAUDE: How the system learned that the sled is in this cubby
    pub source: SledExpungeCubbySource,
}

// TODO-RAINCLAUDE: How the system learned which cubby the sled of an expunge
// request is in
//
// Only the most recent source is kept. For example, if a sled is found in a
// different cubby after it was found in inventory, the source becomes
// `relocated`.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum SledExpungeCubbySource {
    // TODO-RAINCLAUDE: When the request was created, the sled was found in the
    // latest inventory collection.
    Inventory,
    // TODO-RAINCLAUDE: When the request was created, the sled was not in the
    // latest inventory collection, but a check of every powered sled cubby
    // found it.
    LiveScan,
    // TODO-RAINCLAUDE: When the request was created, the sled was not in the
    // latest inventory collection or in any powered sled cubby. It was found
    // later, while it was being powered off.
    FoundAfterAbsent,
    // TODO-RAINCLAUDE: While the sled was being powered off, it was found in a
    // different cubby than the one recorded before.
    Relocated,
}

// TODO-RAINCLAUDE: How powering off the sled of an expunge request was settled
//
// Every outcome except `absent_at_creation` names a cubby.
//
// Some outcomes say that the sled was not found in any powered sled cubby. Only
// powered cubbies are checked, so the sled may still be sitting unpowered in
// one.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SledExpungePowerOffOutcome {
    // TODO-RAINCLAUDE: The sled was found running in its cubby, and the cubby's
    // power was cut.
    PoweredOff {
        // TODO-RAINCLAUDE: The sled's cubby
        cubby: SledExpungeCubby,
    },
    // TODO-RAINCLAUDE: The sled was found in its cubby, already powered off, so
    // nothing was done.
    AlreadyOff {
        // TODO-RAINCLAUDE: The sled's cubby
        cubby: SledExpungeCubby,
    },
    // TODO-RAINCLAUDE: Another sled was found in the sled's recorded cubby, and
    // the sled was not found in any powered sled cubby, so nothing was done.
    OtherBoardPresent {
        // TODO-RAINCLAUDE: The sled's recorded cubby, which holds another sled
        cubby: SledExpungeCubby,
    },
    // TODO-RAINCLAUDE: The sled was not found in its recorded cubby or in any
    // powered sled cubby.
    //
    // In the recorded cubby, the service processor could not be reached or
    // identified.
    NotPresent {
        // TODO-RAINCLAUDE: The sled's recorded cubby
        cubby: SledExpungeCubby,
    },
    // TODO-RAINCLAUDE: The sled was never found, so no cubby was recorded.
    //
    // When the request was created, the sled was not in inventory or in any
    // powered sled cubby. It was looked for again after the rack membership
    // change that removes it began committing, and it was still not found.
    AbsentAtCreation,
}

// TODO-RAINCLAUDE: For a failed request, whether powering the sled off was
// settled before the request failed
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SledExpungePowerOffProgress {
    // TODO-RAINCLAUDE: The request failed before powering the sled off was
    // settled.
    Pending {
        // TODO-RAINCLAUDE: Cubby the sled was last known to be in
        //
        // This is absent if no cubby is known.
        cubby: Option<SledExpungeCubby>,
    },
    // TODO-RAINCLAUDE: Powering the sled off was settled before the request
    // failed.
    // TODO-RAINCLAUDE: This is a struct variant with an `outcome` field, and
    // not a newtype variant `Settled(SledExpungePowerOffOutcome)`, because both
    // enums are internally tagged with `type`.
    //
    // With a newtype variant, serde would put the outcome's fields directly
    // into this variant's JSON object. The two tags would then need the same
    // `type` key: `"settled"` for this enum, and (for example) `"powered_off"`
    // for the outcome. With the `outcome` field, the outcome is nested one
    // level down and keeps its own `type`:
    //
    //     {"type": "settled", "outcome": {"type": "powered_off", ...}}
    Settled {
        // TODO-RAINCLAUDE: How powering the sled off was settled
        outcome: SledExpungePowerOffOutcome,
    },
}

// TODO-RAINCLAUDE: A request to expunge a sled
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SledExpungeRequest {
    // TODO-RAINCLAUDE: Unique, immutable, system-controlled identifier for the
    // request
    pub id: SledExpungeRequestUuid,
    // TODO-RAINCLAUDE: The rack that the sled is in
    pub rack_id: RackUuid,
    // TODO-RAINCLAUDE: The sled being expunged
    pub sled_id: SledUuid,
    // TODO-RAINCLAUDE: Part number and serial number of the sled being expunged
    pub baseboard_id: BaseboardId,
    // TODO-RAINCLAUDE: Current state of the request
    pub status: SledExpungeRequestState,
    // TODO-RAINCLAUDE: Version of the rack membership change that removes the
    // sled
    //
    // Use this with `rack_membership_status` and `rack_membership_abort` to
    // check on that change or to abort it.
    pub rack_membership_version: RackMembershipVersion,
    // TODO-RAINCLAUDE: Timestamp when the request was created
    pub time_created: DateTime<Utc>,
    // TODO-RAINCLAUDE: Timestamp when the request was last modified
    pub time_modified: DateTime<Utc>,
}

// TODO-RAINCLAUDE: What a call to `sled_expunge` did
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum SledExpungeRequestDisposition {
    // TODO-RAINCLAUDE: A new request was created, and the rack membership
    // change that removes the sled was proposed.
    //
    // Note that this doesn't say whether the change has reached its
    // coordinator.
    Created,
    // TODO-RAINCLAUDE: The sled already had an unfinished request, which is
    // returned unchanged.
    AlreadyOutstanding,
}

// TODO-RAINCLAUDE: Response to a `sled_expunge` call
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SledExpungeResponse {
    // TODO-RAINCLAUDE: The sled's expunge request
    pub request: SledExpungeRequest,
    // TODO-RAINCLAUDE: What this call did
    pub disposition: SledExpungeRequestDisposition,
}
