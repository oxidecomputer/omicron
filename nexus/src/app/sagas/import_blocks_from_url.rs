// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! For disks in state ImportReady, send a request to import blocks from a URL.
//! Note the Pantry they're attached to must have addressability to the URL!

use super::declare_saga_actions;
use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use super::SagaInitError;
use crate::app::sagas::retry_until_known_result;
use nexus_db_model::Generation;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::{authn, authz};
use nexus_types::external_api::params;
use omicron_common::api::external;
use omicron_common::api::external::Error;
use serde::Deserialize;
use serde::Serialize;
use std::net::SocketAddrV6;
use steno::ActionError;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub disk_id: Uuid,

    pub import_params: params::ImportBlocksFromUrl,
}

declare_saga_actions! {
    import_blocks_from_url;
    SET_IMPORTING_STATE -> "disk_generation_number" {
        + sibfu_get_importing_state
        - sibfu_get_importing_state_undo
    }
    GET_PANTRY_ADDRESS -> "pantry_address" {
        + sibfu_get_pantry_address
    }
    CALL_PANTRY_IMPORT_FROM_URL_FOR_DISK -> "call_pantry_import_from_url_for_disk" {
        + sibfu_call_pantry_import_from_url_for_disk
    }
    WAIT_FOR_IMPORT_FROM_URL -> "wait_for_import_from_url" {
        + sibfu_wait_for_import_from_url
    }
    SET_IMPORT_READY_STATE -> "set_import_ready_state" {
        + sibfu_get_import_ready_state
    }
}

#[derive(Debug)]
pub(crate) struct SagaImportBlocksFromUrl;
impl NexusSaga for SagaImportBlocksFromUrl {
    const NAME: &'static str = "import-blocks-from-url";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        import_blocks_from_url_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(set_importing_state_action());

        builder.append(get_pantry_address_action());

        // Call the Pantry's /import_from_url
        builder.append(call_pantry_import_from_url_for_disk_action());

        // Wait for import_from_url job to complete
        builder.append(wait_for_import_from_url_action());

        // Set ImportReady state
        builder.append(set_import_ready_state_action());

        Ok(builder.build()?)
    }
}

async fn sibfu_get_importing_state(
    sagactx: NexusActionContext,
) -> Result<Generation, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_disk, db_disk) =
        LookupPath::new(&opctx, &osagactx.datastore())
            .disk_id(params.disk_id)
            .fetch_for(authz::Action::Modify)
            .await
            .map_err(ActionError::action_failed)?;

    match db_disk.state().into() {
        external::DiskState::ImportReady => {
            info!(
                log,
                "setting disk {} to state importing_from_url",
                db_disk.id(),
            );

            osagactx
                .datastore()
                .disk_update_runtime(
                    &opctx,
                    &authz_disk,
                    &db_disk.runtime().importing_from_url(),
                )
                .await
                .map_err(ActionError::action_failed)?;

            // Record the disk's new generation number as this saga node's output. It
            // will be important later to *only* transition this disk out of maintenance
            // if the generation number matches what *this* saga is doing.
            let (.., db_disk) = LookupPath::new(&opctx, &osagactx.datastore())
                .disk_id(params.disk_id)
                .fetch_for(authz::Action::Read)
                .await
                .map_err(ActionError::action_failed)?;

            Ok(db_disk.runtime().gen)
        }

        _ => Err(ActionError::action_failed(Error::invalid_request(&format!(
            "cannot import blocks from a url into disk in state {:?}",
            db_disk.state()
        )))),
    }
}

async fn sibfu_get_importing_state_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_disk, db_disk) =
        LookupPath::new(&opctx, &osagactx.datastore())
            .disk_id(params.disk_id)
            .fetch_for(authz::Action::Modify)
            .await
            .map_err(ActionError::action_failed)?;

    let expected_disk_generation_number =
        sagactx.lookup::<Generation>("disk_generation_number")?;

    match db_disk.state().into() {
        external::DiskState::ImportingFromUrl => {
            // A previous execution of *this* saga may hav already transitioned this disk to
            // import_ready. Another saga racing with this one may have transitioned the disk to
            // importing - only set this disk to import_ready if the generation number matches this
            // saga.
            if expected_disk_generation_number == db_disk.runtime().gen {
                info!(
                    log,
                    "undo: setting disk {} state from importing_from_url to import_ready",
                    params.disk_id
                );

                osagactx
                    .datastore()
                    .disk_update_runtime(
                        &opctx,
                        &authz_disk,
                        &db_disk.runtime().import_ready(),
                    )
                    .await
                    .map_err(ActionError::action_failed)?;
            } else {
                info!(
                    log,
                    "disk {} has generation number {:?}, which doesn't match the expected {:?}: skip setting to import_ready",
                    params.disk_id,
                    db_disk.runtime().gen,
                    expected_disk_generation_number,
                );
            }
        }

        external::DiskState::ImportReady => {
            info!(log, "disk {} already import_ready", params.disk_id);
        }

        _ => {
            warn!(log, "disk is in state {:?}", db_disk.state());
        }
    }

    Ok(())
}

async fn sibfu_get_pantry_address(
    sagactx: NexusActionContext,
) -> Result<SocketAddrV6, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., db_disk) = LookupPath::new(&opctx, &osagactx.datastore())
        .disk_id(params.disk_id)
        .fetch_for(authz::Action::Modify)
        .await
        .map_err(ActionError::action_failed)?;

    // At any stage of executing this saga, if the disk moves from state
    // importing to detached, it will be detached from the corresponding Pantry.
    // Any subsequent saga nodes will fail because the pantry address is stored
    // as part of the saga state, and requests sent to that Pantry with the
    // disk's id will fail.
    let pantry_address = db_disk.pantry_address().ok_or_else(|| {
        ActionError::action_failed(String::from("disk not attached to pantry!"))
    })?;

    info!(log, "disk {} is using pantry at {}", db_disk.id(), pantry_address);

    Ok(pantry_address)
}

async fn sibfu_call_pantry_import_from_url_for_disk(
    sagactx: NexusActionContext,
) -> Result<String, ActionError> {
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;

    let pantry_address = sagactx.lookup::<SocketAddrV6>("pantry_address")?;
    let endpoint = format!("http://{}", pantry_address);

    info!(
        log,
        "sending import from url {} request for disk {} to pantry endpoint {}",
        params.import_params.url,
        params.disk_id,
        endpoint,
    );

    let disk_id = params.disk_id.to_string();

    let client = crucible_pantry_client::Client::new(&endpoint);

    let request = crucible_pantry_client::types::ImportFromUrlRequest {
        url: params.import_params.url,
        expected_digest: if let Some(expected_digest) =
            params.import_params.expected_digest
        {
            match expected_digest {
                nexus_types::external_api::params::ExpectedDigest::Sha256(
                    v,
                ) => Some(
                    crucible_pantry_client::types::ExpectedDigest::Sha256(v),
                ),
            }
        } else {
            None
        },
    };

    let response = retry_until_known_result(log, || async {
        client.import_from_url(&disk_id, &request).await
    })
    .await
    .map_err(|e| {
        ActionError::action_failed(format!(
            "import from url failed with {:?}",
            e
        ))
    })?;

    Ok(response.job_id.clone())
}

async fn sibfu_wait_for_import_from_url(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let params = sagactx.saga_params::<Params>()?;

    let pantry_address = sagactx.lookup::<SocketAddrV6>("pantry_address")?;
    let job_id =
        sagactx.lookup::<String>("call_pantry_import_from_url_for_disk")?;

    let endpoint = format!("http://{}", pantry_address);

    let client = crucible_pantry_client::Client::new(&endpoint);

    info!(
        log,
        "waiting for import from url job {} for disk {} to complete on pantry {}",
        job_id,
        params.disk_id,
        endpoint,
    );

    loop {
        let result = retry_until_known_result(log, || async {
            client.is_job_finished(&job_id).await
        })
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "is_job_finished failed with {:?}",
                e
            ))
        })?;

        if result.job_is_finished {
            break;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    info!(
        log,
        "import from url job {} for disk {} on pantry {} completed",
        job_id,
        params.disk_id,
        endpoint,
    );

    let response = retry_until_known_result(log, || async {
        client.job_result_ok(&job_id).await
    })
    .await
    .map_err(|e| {
        ActionError::action_failed(format!("job_result_ok failed with {:?}", e))
    })?;

    if !response.job_result_ok {
        return Err(ActionError::action_failed(format!("Job {job_id} failed")));
    }

    Ok(())
}

async fn sibfu_get_import_ready_state(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_disk, db_disk) =
        LookupPath::new(&opctx, &osagactx.datastore())
            .disk_id(params.disk_id)
            .fetch_for(authz::Action::Modify)
            .await
            .map_err(ActionError::action_failed)?;

    let expected_disk_generation_number =
        sagactx.lookup::<Generation>("disk_generation_number")?;

    match db_disk.state().into() {
        external::DiskState::ImportingFromUrl => {
            if expected_disk_generation_number == db_disk.runtime().gen {
                info!(
                    log,
                    "setting disk {} state from importing_from_url to import_ready",
                    params.disk_id
                );

                osagactx
                    .datastore()
                    .disk_update_runtime(
                        &opctx,
                        &authz_disk,
                        &db_disk.runtime().import_ready(),
                    )
                    .await
                    .map_err(ActionError::action_failed)?;
            } else {
                info!(
                    log,
                    "disk {} has generation number {:?}, which doesn't match the expected {:?}: skip setting to import_ready",
                    params.disk_id,
                    db_disk.runtime().gen,
                    expected_disk_generation_number,
                );
            }
        }

        external::DiskState::ImportReady => {
            info!(log, "disk {} already import_ready", params.disk_id);
        }

        _ => {
            warn!(log, "disk is in state {:?}", db_disk.state());
        }
    }

    Ok(())
}
