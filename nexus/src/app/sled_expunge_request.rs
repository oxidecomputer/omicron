// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use gateway_types::component::SpType;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::SledExpungeRequestCreateOutcome;
use nexus_db_queries::db::model::SledExpungeCreationPresence;
use nexus_db_queries::db::model::SledExpungeRequest;
use nexus_db_queries::db::model::SpMgsSlot;
use nexus_networking::GatewaysByRack;
use nexus_networking::SledIgnitionList;
use nexus_networking::SledPowerOffGateways;
use nexus_types::external_api::sled as views;
use nexus_types::inventory::Collection;
use nexus_types::sled_expunge::SledCubbyIdentity;
use nexus_types::sled_expunge::SledExpungeIgnitionPresence;
use nexus_types::sled_expunge::SledExpungeInventoryPresence;
use nexus_types::sled_expunge::check_sled_expunge_policy_and_state;
use nexus_types::sled_expunge::check_sled_expunge_serial_acknowledged;
use nexus_types::sled_expunge::sled_expunge_collection_cubby_identity;
use nexus_types::sled_expunge::sled_expunge_ignition_presence;
use nexus_types::sled_expunge::sled_expunge_inventory_presence;
use nexus_types::sled_expunge::sled_expunge_powered_sled_cubbies;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledExpungeRequestUuid;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SledExpungeRequestCreated {
    pub(crate) request: SledExpungeRequest,
    pub(crate) disposition: views::SledExpungeRequestDisposition,
}

impl super::Nexus {
    // TODO-RAINCLAUDE: RFD 747 request creation: a new removal is sent to its coordinator here, once; a replay of an outstanding request doesn't resend.
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
        // TODO-RAINCLAUDE: a replay of the sled's outstanding request is answered before probing MGS, so it doesn't depend on MGS being reachable.
        if let Some(request) = self
            .db_datastore
            .sled_expunge_request_find_outstanding(
                opctx,
                &authz_sled,
                &authz_tq,
                acknowledged_serial,
            )
            .await?
        {
            self.background_tasks.task_sled_expunge.activate();
            return Ok(SledExpungeRequestCreated {
                request,
                disposition:
                    views::SledExpungeRequestDisposition::AlreadyOutstanding,
            });
        }
        // TODO-RAINCLAUDE: the collection is read from the database rather than the inventory loader's watch channel, so the presence and scrimlet checks see the newest one; MGS is probed before the transaction, never inside it, and the collection is dropped before the send.
        let outcome = {
            let latest_collection = self
                .db_datastore
                .inventory_get_latest_collection(opctx)
                .await?;
            // TODO-RAINCLAUDE: the cheap refusals run before probing MGS, so a wrong serial or a sled that can't be expunged isn't masked by a 503; the transaction re-runs both and stays authoritative, and a policy/state refusal skips the probe so the transaction reports it with the sled's latest finished request; that refusal is permanent (expunged and decommissioned are terminal), so the Gone placeholder passed then is never recorded.
            check_sled_expunge_serial_acknowledged(
                sled_id,
                sled.serial_number(),
                acknowledged_serial,
            )?;
            let presence = match check_sled_expunge_policy_and_state(
                sled_id,
                sled.policy(),
                sled.state().into(),
            ) {
                Ok(()) => {
                    self.sled_expunge_creation_presence(
                        opctx,
                        sled_id,
                        rack_id,
                        &sled.baseboard_id(),
                        latest_collection.as_ref(),
                    )
                    .await?
                }
                Err(_refused_by_transaction) => {
                    SledExpungeCreationPresence::Gone
                }
            };
            self.db_datastore
                .sled_expunge_request_create(
                    opctx,
                    &authz_sled,
                    &authz_tq,
                    acknowledged_serial,
                    latest_collection.as_ref(),
                    presence,
                )
                .await?
        };

        let (request, removal_config) = match outcome {
            SledExpungeRequestCreateOutcome::AlreadyOutstanding { request } => {
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

        // TODO-RAINCLAUDE: the request and its removal config are already committed, so a failed send is logged rather than failing the call; the removal then stays preparing until it is aborted, and the sled_expunge task reports it as stuck.
        if let Err(error) =
            self.tq_send_reconfigure(opctx, &removal_config).await
        {
            warn!(
                opctx.log,
                "failed to send sled expunge removal config to its \
                 coordinator";
                "sled_id" => %sled_id,
                "request_id" => %request.id(),
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

    // TODO-RAINCLAUDE: RFD 747 creation presence: the sled's SP in the latest collection records its cubby; a sled agent without its SP is refused; with neither, one MGS's ignition list (each MGS tried in address order) decides whether the sled is in some powered cubby or gone, identifying each powered sled cubby from the collection or else an sp_get through the MGS that answered.
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
                    sp_slot: SpMgsSlot::from(sp_slot),
                });
            }
            SledExpungeInventoryPresence::NotInInventory => {}
        }

        let log = &opctx.log;
        let gateways =
            GatewaysByRack::resolve_all_gateways(log, self.resolver())
                .await
                .map_err(|error| {
                    Error::unavail(&format!(
                        "Cannot expunge sled {sled_id}: it is not in the \
                         latest inventory collection, and confirming that it \
                         is gone needs MGS, but no MGS could be found: \
                         {error:#}. Retry later."
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
                "Cannot expunge sled {sled_id}: it is not in the latest \
                 inventory collection, and confirming that it is gone needs \
                 an MGS of rack {rack_id}, but no MGS reported that rack; {} \
                 MGS could not report their rack ID ({}). Retry later.",
                unknown.len(),
                unknown.join("; "),
            )));
        };
        let gateways = SledPowerOffGateways::new(rack_gateways);
        let SledIgnitionList { mgs, sleds, failed_attempts: _ } = gateways
            .fetch_sled_ignition(log)
            .await
            .map_err(|failed_attempts| {
                let errors = failed_attempts
                    .iter()
                    .map(|failure| {
                        format!("{}: {}", failure.mgs, failure.error)
                    })
                    .collect::<Vec<_>>();
                Error::unavail(&format!(
                    "Cannot expunge sled {sled_id}: it is not in the latest \
                     inventory collection, and confirming that it is gone \
                     needs ignition state from an MGS of rack {rack_id}, but \
                     none answered ({}). Retry later.",
                    errors.join("; "),
                ))
            })?;

        let mut identities = BTreeMap::new();
        for slot in sled_expunge_powered_sled_cubbies(&sleds) {
            let identity = match sled_expunge_collection_cubby_identity(
                latest_collection,
                rack_id,
                slot,
            ) {
                Some(found) => SledCubbyIdentity::Known(found.clone()),
                None => match mgs.client.sp_get(&SpType::Sled, slot).await {
                    Ok(state) => {
                        let state = state.into_inner();
                        SledCubbyIdentity::Known(BaseboardId {
                            part_number: state.model,
                            serial_number: state.serial_number,
                        })
                    }
                    Err(error) => {
                        warn!(
                            log,
                            "sled expunge creation: could not identify a \
                             powered sled cubby";
                            "sled_id" => %sled_id,
                            "slot" => slot,
                            "mgs_addr" => %mgs.addr,
                            InlineErrorChain::new(&error),
                        );
                        SledCubbyIdentity::SpUnreachable
                    }
                },
            };
            identities.insert(slot, identity);
        }
        let presence =
            sled_expunge_ignition_presence(sled_id, baseboard_id, &identities)?;
        info!(
            log,
            "sled expunge creation: ignition scan placed a sled that is not \
             in inventory";
            "sled_id" => %sled_id,
            "mgs_addr" => %mgs.addr,
            "presence" => ?presence,
        );
        Ok(match presence {
            SledExpungeIgnitionPresence::InCubby { sp_slot } => {
                SledExpungeCreationPresence::Present {
                    sp_slot: SpMgsSlot::from(sp_slot),
                }
            }
            SledExpungeIgnitionPresence::Gone => {
                SledExpungeCreationPresence::Gone
            }
        })
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
