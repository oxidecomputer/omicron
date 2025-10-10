// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! Interface for API requests to a Management Gateway Service (MGS) instance

pub use gateway_messages::SpComponent;
use std::time::Duration;
use std::time::Instant;
use types::ComponentFirmwareHashStatus;

// We specifically want to allow consumers, such as `wicketd`, to embed
// inventory datatypes into their own APIs, rather than recreate structs.
//
// We did not add this functionality to `omicron_common` because, in the common
// case, we want to prohibit users from accidentally exposing implementation
// specific, private types. As an example, we don't want to make it trivial
// to include propolis types that may change in the Oxide public API. For
// scenarios like this, forcing the user of a client API to generate a new type
// serves as a safety feature in the common case. The user of an underlying
// dropshot server can always choose to use this escape hatch.
//
// In this case, we choose to allow the escape hatch, because one of two
// primary consumers of MGS is wicketd, and wicketd wants to share inventory
// data, which is already in a useful format, directly with wicket for
// processing without having to perform unnecessary manipulation. In essence,
// wicketd is just proxying information for display. Furthermore, wicket
// itself is a TUI, and so it will not be forwarding these types to any public
// consumers. While MGS also has Nexus as a client, the use case is fairly
// constrained and it is unlikely we'd desire to expose MGS types directly. In
// this instance we'll want to be extra careful, but we shouldn't burden ourself
// unduly here to prevent an unlikely mistake.
//
// If the format of inventory data desired by wicket or nexus changes such that
// it is no longer useful to directly expose the JsonSchema types, we can go
// back to reusing `omicron_common`.
//
// As another alternative, since the `derives` and `patch` directives were
// introduced, these types have moved to living in gateway-types. This means
// that we can choose to use `replace` on them. That hasn't felt necessary so
// far, but it's an option if it becomes desirable in the future. (One reason
// to do that might be that currently, `nexus-types` depends on
// `gateway-client`. Generally we expect the `-client` layer to depend on the
// `-types` layer to avoid a circular dependency, and we've had to resolve a
// rather thorny one between Nexus and sled-agent. But Nexus and sled-agent
// call into each other. Since `gateway` is a lower-level service and never
// calls into Nexus, the current scheme is okay.)
progenitor::generate_api!(
    spec = "../../openapi/gateway/gateway-latest.json",
    interface = Positional,
    inner_type = slog::Logger,
    pre_hook = (|log: &slog::Logger, request: &reqwest::Request| {
        slog::debug!(log, "client request";
            "method" => %request.method(),
            "uri" => %request.url(),
            "body" => ?&request.body(),
        );
    }),
    post_hook = (|log: &slog::Logger, result: &Result<_, _>| {
        slog::debug!(log, "client response"; "result" => ?result);
    }),
    derives = [schemars::JsonSchema],
    patch = {
        ComponentFirmwareHashStatus = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        HostPhase2RecoveryImageId = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        ImageVersion = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        RotImageDetails = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        RotImageError = { derives = [ thiserror::Error, PartialEq, Eq, PartialOrd, Ord] },
        RotState = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        SpComponentCaboose = { derives = [PartialEq, Eq] },
        SpComponentInfo = { derives = [PartialEq, Eq] },
        SpIdentifier = { derives = [Copy, PartialEq, Hash, Eq] },
        SpIgnition = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        SpIgnitionSystemType = { derives = [Copy, PartialEq, Eq, PartialOrd, Ord] },
        SpState = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        SpUpdateStatus = { derives = [PartialEq, Hash, Eq] },
        UpdatePreparationProgress = { derives = [PartialEq, Hash, Eq] },
    },
    crates = {
        "omicron-uuid-kinds" = "*",
    },

    replace = {
        RotSlot = gateway_types::rot::RotSlot,
        Ena = ereport_types::Ena,
        Ereport = ereport_types::Ereport,
        Ereports = ereport_types::Ereports,
        SpType = gateway_types::component::SpType,
        TaskDump = gateway_types::task_dump::TaskDump,
    },
);

// Override the impl of Ord for SpIdentifier because the default one orders the
// fields in a different order than people are likely to want.
impl Ord for crate::types::SpIdentifier {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.type_.cmp(&other.type_).then(self.slot.cmp(&other.slot))
    }
}

impl PartialOrd for crate::types::SpIdentifier {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HostPhase1HashError {
    #[error("timed out after {0:?} waiting for hash calculation")]
    Timeout(Duration),
    #[error("hash calculation failed (phase1 written while hashing?)")]
    ContentsModifiedWhileHashing,
    #[error("failed to send request to {kind}")]
    RequestError {
        kind: &'static str,
        #[source]
        err: Error<types::Error>,
    },
}

impl Client {
    /// Get the hash of the host phase 1 flash contents in the given slot.
    ///
    /// This operation is implemented asynchronously on the SP: a client (us)
    /// must request the hash be calculated, then poll until the calculation is
    /// complete. This method takes care of the "start / poll" operation; the
    /// caller must provide a timeout for how long they're willing to wait for
    /// the calculation to complete. In practice, we expect this to take a
    /// handful of seconds on real hardware.
    pub async fn host_phase_1_flash_hash_calculate_with_timeout(
        &self,
        sp_type: gateway_types::component::SpType,
        sp_slot: u16,
        phase1_slot: u16,
        timeout: Duration,
    ) -> Result<[u8; 32], HostPhase1HashError> {
        // The most common cases of calling this function are:
        //
        // 1. The hash is already calculated; we get it in the first `get`
        //    operation below and return after a single request to MGS.
        // 2. The hash needs to be recalculated; we'll issue a "start hashing"
        //    request then go into the polling loop. We expect to sit in that
        //    loop for a handful of seconds.
        //
        // Given these, we could make this poll duration longer, since we know
        // the operation takes a little while. But there are two arguments for
        // polling somewhat more frequently:
        //
        // 1. Timeouts, timeouts, always wrong; if we believe hashing takes (by
        //    way of example) 7 seconds, so we set the timeout to something
        //    slightly larger than that (say 10 seconds), if a real device takes
        //    slightly longer than our timeout, we now wait 20 seconds.
        // 2. An uncommon case of calling this function is that our initial
        //    `get` returns `HashInProgress`; in this case we have no idea how
        //    long the hashing has already been running, so would not know how
        //    long to try to wait.
        //
        // It should be pretty cheap to poll the SP at 1 Hz, so we sidestep both
        // of those issues by doing so.
        const SLEEP_BETWEEN_POLLS: Duration = Duration::from_secs(1);
        const PHASE1_FLASH: &str =
            SpComponent::HOST_CPU_BOOT_FLASH.const_as_str();

        let need_to_start_hashing = match self
            .sp_component_hash_firmware_get(
                &sp_type,
                sp_slot,
                PHASE1_FLASH,
                phase1_slot,
            )
            .await
            .map_err(|err| HostPhase1HashError::RequestError {
                kind: "get hash",
                err,
            })?
            .into_inner()
        {
            ComponentFirmwareHashStatus::Hashed(hash) => return Ok(hash),
            ComponentFirmwareHashStatus::HashInProgress => false,
            ComponentFirmwareHashStatus::HashNotCalculated => true,
        };

        if need_to_start_hashing {
            // It's possible multiple Nexus instances race, all see
            // `HashNotCalculated` above, then all try to start hashing here.
            // The SP will accept the first request and return a
            // `HashInProgress` error for subsequent attempts, but MGS does its
            // best to make this operation idempotent; in particular, it will
            // catch a `HashInProgress` error here and return an HTTP success.
            // We'll return any other error.
            self.sp_component_hash_firmware_start(
                &sp_type,
                sp_slot,
                PHASE1_FLASH,
                phase1_slot,
            )
            .await
            .map_err(|err| HostPhase1HashError::RequestError {
                kind: "start hashing",
                err,
            })?;
        }

        let start = Instant::now();
        loop {
            tokio::time::sleep(SLEEP_BETWEEN_POLLS).await;
            if start.elapsed() > timeout {
                return Err(HostPhase1HashError::Timeout(timeout));
            }
            match self
                .sp_component_hash_firmware_get(
                    &sp_type,
                    sp_slot,
                    PHASE1_FLASH,
                    phase1_slot,
                )
                .await
                .map_err(|err| HostPhase1HashError::RequestError {
                    kind: "get hash",
                    err,
                })?
                .into_inner()
            {
                ComponentFirmwareHashStatus::Hashed(hash) => return Ok(hash),
                ComponentFirmwareHashStatus::HashInProgress => continue,
                ComponentFirmwareHashStatus::HashNotCalculated => {
                    return Err(
                        HostPhase1HashError::ContentsModifiedWhileHashing,
                    );
                }
            }
        }
    }
}
