// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::SledExpungeRequestCreateOutcome;
use nexus_db_queries::db::model::Sled;
use nexus_networking::GatewaysByRack;
use nexus_networking::SledPowerOffGateways;
use nexus_types::external_api::sled as views;
use nexus_types::inventory::Collection;
use nexus_types::sled_expunge::CubbyUnidentified;
use nexus_types::sled_expunge::SledExpungeCreationPresence;
use nexus_types::sled_expunge::SledExpungeCreationProbe;
use nexus_types::sled_expunge::SledExpungeCreationSpSlotSource;
use nexus_types::sled_expunge::SledExpungeInventoryPresence;
use nexus_types::sled_expunge::SledExpungeLivePresence;
use nexus_types::sled_expunge::SledExpungeRequest;
use nexus_types::sled_expunge::SledExpungeUnidentifiedCubbies;
use nexus_types::sled_expunge::check_sled_expunge_baseboard_recorded;
use nexus_types::sled_expunge::check_sled_expunge_policy_and_state;
use nexus_types::sled_expunge::check_sled_expunge_serial_acknowledged;
use nexus_types::sled_expunge::sled_expunge_collection_cubby_identities;
use nexus_types::sled_expunge::sled_expunge_inventory_presence;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use slog_error_chain::InlineErrorChain;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SledExpungeRequestCreated {
    pub(crate) request: SledExpungeRequest,
    pub(crate) disposition: views::SledExpungeRequestDisposition,
}

impl super::Nexus {
    pub(crate) async fn sled_expunge_request_create(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
        acknowledged_serial: &str,
    ) -> Result<SledExpungeRequestCreated, Error> {
        let (authz_sled, sled) = self
            .sled_lookup(opctx, &sled_id)?
            .fetch_for(authz::Action::Modify)
            .await?;
        let rack_id = sled.rack_id();
        let authz_tq = authz::TrustQuorumConfig::for_rack_id(rack_id);
        // TODO-RAINCLAUDE: A replay of the sled's outstanding request is
        // answered here, before we probe MGS, so that a replay works even when
        // MGS is unreachable.
        let outstanding = self
            .db_datastore
            .sled_expunge_request_find_outstanding(
                opctx,
                &authz_sled,
                &authz_tq,
                acknowledged_serial,
            )
            .await?;
        let outcome = match outstanding {
            Some(outstanding) => outstanding.into_outcome(),
            None => {
                self.sled_expunge_request_probe_and_create(
                    opctx,
                    &authz_sled,
                    &authz_tq,
                    &sled,
                    acknowledged_serial,
                )
                .await?
            }
        };

        let (request, removal_config) = match outcome {
            SledExpungeRequestCreateOutcome::AlreadyOutstanding { request } => {
                // TODO-RAINCLAUDE: A replay sends nothing to the trust quorum
                // coordinator, because the removal is sent only once, when the
                // request is first created. It still activates `sled_expunge`,
                // so that the request keeps advancing.
                self.background_tasks.task_sled_expunge.activate();
                return Ok(SledExpungeRequestCreated {
                    request,
                    disposition:
                        views::SledExpungeRequestDisposition::AlreadyOutstanding,
                });
            }
            SledExpungeRequestCreateOutcome::Created {
                request,
                removal_config,
            } => (request, removal_config),
        };

        // TODO-RAINCLAUDE: The request and its removal config are already
        // committed, so a failed send is logged rather than failing the call.
        // The removal then stays in `Preparing`, and the `sled_expunge` task
        // reports it once it has been there for too long.
        if let Err(error) =
            self.tq_send_reconfigure(opctx, &removal_config).await
        {
            warn!(
                opctx.log,
                "failed to send sled expunge removal config to its \
                 coordinator";
                "sled_id" => %sled_id,
                "rack_id" => %removal_config.rack_id,
                "request_id" => %request.id,
                "epoch" => %removal_config.epoch,
                "error" => InlineErrorChain::new(&error),
            );
        }
        self.background_tasks.task_trust_quorum_manager.activate();
        self.background_tasks.task_sled_expunge.activate();

        Ok(SledExpungeRequestCreated {
            request,
            disposition: views::SledExpungeRequestDisposition::Created,
        })
    }

    // TODO-RAINCLAUDE: Probes for where the sled is, then runs the creation
    // transaction. The checks that need neither inventory nor MGS run first, so
    // that a wrong serial number or an unrecorded baseboard is refused as such,
    // rather than being hidden behind a 503 from the probe.
    //
    // The transaction runs every check again and has the final say. A sled that
    // fails the policy and state check here skips the probe, because the
    // transaction refuses it before it looks at the probe.
    async fn sled_expunge_request_probe_and_create(
        &self,
        opctx: &OpContext,
        authz_sled: &authz::Sled,
        authz_tq: &authz::TrustQuorumConfig,
        sled: &Sled,
        acknowledged_serial: &str,
    ) -> Result<SledExpungeRequestCreateOutcome, Error> {
        let sled_id = authz_sled.id();
        let rack_id = sled.rack_id();
        let policy_and_state = check_sled_expunge_policy_and_state(
            sled_id,
            sled.policy(),
            sled.state().into(),
        );
        let baseboard_id = sled.baseboard_id();
        let (latest_collection, probe) = match policy_and_state {
            Err(_refused_by_transaction) => {
                (None, SledExpungeCreationProbe::NotProbed)
            }
            Ok(()) => {
                check_sled_expunge_baseboard_recorded(sled_id, &baseboard_id)?;
                check_sled_expunge_serial_acknowledged(
                    sled_id,
                    sled.serial_number(),
                    acknowledged_serial,
                )?;
                // TODO-RAINCLAUDE: The collection is read from the database
                // rather than from the inventory loader's watch channel, so
                // that the presence and scrimlet checks see the newest
                // collection.
                let latest_collection = self
                    .db_datastore
                    .inventory_get_latest_collection(opctx)
                    .await?;
                let presence = self
                    .sled_expunge_creation_presence(
                        opctx,
                        sled_id,
                        rack_id,
                        &baseboard_id,
                        latest_collection.as_ref(),
                    )
                    .await?;
                (latest_collection, SledExpungeCreationProbe::Probed(presence))
            }
        };
        self.db_datastore
            .sled_expunge_request_create(
                opctx,
                authz_sled,
                authz_tq,
                acknowledged_serial,
                latest_collection.as_ref(),
                probe,
            )
            .await
    }

    // TODO-RAINCLAUDE: Finds where the sled is when its expunge request is
    // created. The latest inventory collection is consulted first, and the
    // rack-wide check through MGS runs only when the sled is absent from that
    // collection.
    async fn sled_expunge_creation_presence(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
        rack_id: RackUuid,
        baseboard_id: &BaseboardId,
        latest_collection: Option<&Collection>,
    ) -> Result<SledExpungeCreationPresence, Error> {
        match sled_expunge_inventory_presence(
            latest_collection,
            sled_id,
            rack_id,
            baseboard_id,
        )? {
            SledExpungeInventoryPresence::InCubby { sp_slot } => {
                return Ok(SledExpungeCreationPresence::Present {
                    sp_slot,
                    source: SledExpungeCreationSpSlotSource::Inventory,
                });
            }
            SledExpungeInventoryPresence::NotInInventory => {}
        }

        let log = &opctx.log;
        // TODO-RAINCLAUDE: The external message of a 503 is always "Service
        // Unavailable", so the texts passed to `Error::unavail` below reach
        // only the Nexus log. They are written for engineers, and include MGS
        // addresses and errors.
        let gateways =
            GatewaysByRack::resolve_all_gateways(log, self.resolver())
                .await
                .map_err(|error| {
                    Error::unavail(&format!(
                        "sled expunge creation presence for sled {sled_id} \
                         (not in inventory): resolving MGS failed: {error:#}"
                    ))
                })?;
        let Some(rack_gateways) = gateways.for_rack(&rack_id) else {
            let unknown = gateways
                .unknown()
                .iter()
                .map(|(gateway, error)| {
                    format!(
                        "{}: {}",
                        gateway.addr,
                        InlineErrorChain::new(error)
                    )
                })
                .collect::<Vec<_>>();
            return Err(Error::unavail(&format!(
                "sled expunge creation presence for sled {sled_id} (not in \
                 inventory): no MGS reported rack {rack_id}; {} MGS failed \
                 to report a rack ID ({})",
                unknown.len(),
                unknown.join("; "),
            )));
        };
        // TODO-RAINCLAUDE: A cubby whose board the latest collection recorded
        // counts as identified without an `sp_get`, which avoids a spurious 409
        // when an SP was inventoried but has since stopped answering. A stale
        // collection is safe here, because once the removal reaches
        // `Committing`, the `sled_expunge` task checks again that the sled is
        // gone, using only what MGS reports at that time.
        let gateways = SledPowerOffGateways::new(
            rack_gateways,
            sled_expunge_collection_cubby_identities(
                latest_collection,
                rack_id,
            ),
        );
        let presence = gateways.live_sled_presence(log, baseboard_id).await;
        info!(
            log,
            "sled expunge creation: ran the rack-wide check for a sled that \
             is not in inventory";
            "sled_id" => %sled_id,
            "presence" => ?presence,
        );
        match presence {
            Ok(SledExpungeLivePresence::InCubby { slot }) => {
                Ok(SledExpungeCreationPresence::Present {
                    sp_slot: slot,
                    source: SledExpungeCreationSpSlotSource::LiveScan,
                })
            }
            Ok(SledExpungeLivePresence::Gone { absence: _ }) => {
                Ok(SledExpungeCreationPresence::Gone)
            }
            Ok(SledExpungeLivePresence::Unidentified { cubbies }) => {
                Err(unidentified_cubbies_refusal(sled_id, &cubbies))
            }
            Err(switches) => {
                warn!(
                    log,
                    "sled expunge creation: a switch's ignition state is \
                     unavailable";
                    "sled_id" => %sled_id,
                    "switch_status" => ?gateways.switch_status(),
                );
                Err(Error::unavail(&format!(
                    "sled expunge creation presence for sled {sled_id} (not \
                     in inventory): concluding that the sled is in no \
                     powered sled cubby needs the ignition state of both \
                     switches of rack {rack_id}, but it is unavailable: \
                     {switches:?}",
                )))
            }
        }
    }

    pub(crate) async fn sled_expunge_request_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (DateTime<Utc>, Uuid)>,
    ) -> ListResultVec<SledExpungeRequest> {
        self.db_datastore.sled_expunge_request_list(opctx, pagparams).await
    }

    pub(crate) async fn sled_expunge_request_view(
        &self,
        opctx: &OpContext,
        id: SledExpungeRequestUuid,
    ) -> LookupResult<SledExpungeRequest> {
        self.db_datastore.sled_expunge_request_get(opctx, id).await
    }
}

// TODO-RAINCLAUDE: Builds the 409 for sled cubbies whose boards could not be
// identified. Operators see this message, so it names only the cubby numbers
// and the kind of failure in each. The MGS addresses and client errors stay in
// the logs.
fn unidentified_cubbies_refusal(
    sled_id: SledUuid,
    cubbies: &SledExpungeUnidentifiedCubbies,
) -> Error {
    let unidentified = cubbies
        .iter()
        .map(|cubby| match &cubby.reason {
            CubbyUnidentified::EmptyIdentity { reported: _ } => format!(
                "cubby {}: its service processor reported an empty baseboard \
                 identity",
                cubby.slot,
            ),
            CubbyUnidentified::SpUnreachable { error: _ } => format!(
                "cubby {}: its service processor did not answer",
                cubby.slot,
            ),
            CubbyUnidentified::MgsRequestFailed { error: _ } => format!(
                "cubby {}: the request to identify its board through MGS \
                 failed",
                cubby.slot,
            ),
            CubbyUnidentified::UnexpectedSystemType { system_type: _ } => {
                format!(
                    "cubby {}: ignition reports a system type that does not \
                     belong in a sled cubby",
                    cubby.slot,
                )
            }
        })
        .collect::<Vec<_>>();
    Error::conflict(format!(
        "Cannot expunge sled {sled_id}: neither its service processor nor its \
         sled agent is in the latest inventory collection, but the boards in \
         some sled cubbies could not be identified, so the sled may still be \
         running in one of them ({}). Retry after the next inventory \
         collection; if this persists, contact Oxide support.",
        unidentified.join("; "),
    ))
}
