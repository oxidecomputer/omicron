// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Smoke test for the demo saga

use futures::TryStreamExt;
use nexus_client::types::Saga;
use nexus_client::types::SagaState;
use nexus_test_interface::NexusServer;
use nexus_test_utils_macros::nexus_test;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_test_utils::dev::poll::CondCheckError;
use std::time::Duration;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// Tests that we can create a demo saga, then mark it completed, and the actual
// saga's state matches what we expect along the way.
#[nexus_test]
async fn test_demo_saga(cptestctx: &ControlPlaneTestContext) {
    let log = &cptestctx.logctx.log;
    let nexus_internal_url = format!(
        "http://{}",
        cptestctx.server.get_http_server_internal_address().await
    );
    let nexus_client =
        nexus_client::Client::new(&nexus_internal_url, log.clone());

    let sagas_before = list_sagas(&nexus_client).await;
    eprintln!("found sagas (before): {:?}", sagas_before);
    let demo_saga = nexus_client.saga_demo_create().await.unwrap();
    let saga_id = demo_saga.saga_id;
    assert!(!sagas_before.into_iter().any(|s| s.id == saga_id));

    let sagas_after = list_sagas(&nexus_client).await;
    eprintln!("found sagas (after): {:?}", sagas_after);
    let found = sagas_after.into_iter().find(|s| s.id == saga_id).unwrap();
    assert!(matches!(found.state, SagaState::Running));

    // It is hard to verify that the saga is not going to complete by itself.
    // No matter how long we wait and make sure it didn't complete, it might
    // have completed after that.  And then we've made the test suite take that
    // much longer.   But we can at least make sure that completing the saga
    // does cause it to finish.
    //
    // Note that `saga_demo_complete` will not succeed until the saga has gotten
    // far enough through execution that it has registered itself as waiting for
    // a completion message.  As a result, we have to keep trying until this
    // works.  But it shouldn't take long -- the saga only has one action and
    // this is all that it does.
    wait_for_condition(
        || async {
            nexus_client
                .saga_demo_complete(&demo_saga.demo_saga_id)
                .await
                .map_err(|err| {
                    if let Some(status) = err.status() {
                        if status == http::StatusCode::NOT_FOUND {
                            eprintln!("transient 404 -- will try again");
                            return CondCheckError::NotYet;
                        }
                    }

                    CondCheckError::Failed(err)
                })
        },
        &std::time::Duration::from_millis(20),
        &std::time::Duration::from_secs(10),
    )
    .await
    .unwrap();

    // Completion is not synchronous -- that just unblocked the saga.  So we
    // need to poll a bit to wait for it to actually finish.
    let found = wait_for_condition(
        || async {
            let sagas = list_sagas(&nexus_client).await;
            eprintln!("found sagas (last): {:?}", sagas);
            let found = sagas.into_iter().find(|s| s.id == saga_id).unwrap();
            if matches!(found.state, SagaState::Succeeded) {
                Ok(found)
            } else {
                Err(CondCheckError::<()>::NotYet)
            }
        },
        &Duration::from_millis(50),
        &Duration::from_secs(30),
    )
    .await
    .unwrap();

    assert_eq!(found.id, saga_id);
    assert!(matches!(found.state, SagaState::Succeeded));
}

async fn list_sagas(client: &nexus_client::Client) -> Vec<Saga> {
    client.saga_list_stream(None, None).try_collect::<Vec<_>>().await.unwrap()
}
