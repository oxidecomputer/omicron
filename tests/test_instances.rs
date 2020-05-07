/*!
 * Tests basic instance support in the API.
 */

use http::method::Method;
use http::StatusCode;
use oxide_api_prototype::api_model::ApiByteCount;
use oxide_api_prototype::api_model::ApiIdentityMetadata;
use oxide_api_prototype::api_model::ApiIdentityMetadataCreateParams;
use oxide_api_prototype::api_model::ApiInstanceCpuCount;
use oxide_api_prototype::api_model::ApiInstanceCreateParams;
use oxide_api_prototype::api_model::ApiInstanceState;
use oxide_api_prototype::api_model::ApiInstanceView;
use oxide_api_prototype::api_model::ApiName;
use oxide_api_prototype::api_model::ApiProjectCreateParams;
use oxide_api_prototype::ApiContext;
use oxide_api_prototype::OxideController;
use oxide_api_prototype::OxideControllerTestInterfaces;
use oxide_api_prototype::ServerControllerTestInterfaces;
use std::convert::TryFrom;
use std::sync::Arc;
use uuid::Uuid;

use dropshot::test_util::read_json;
use dropshot::test_util::read_ndjson;
use dropshot::test_util::TestContext;

pub mod common;
use common::test_setup;

#[macro_use]
extern crate slog;

#[tokio::test]
async fn test_instances() {
    let testctx = test_setup("test_instances").await;
    let apictx = ApiContext::from_server(&testctx.server);
    let controller = &apictx.controller;

    /* Create a project that we'll use for testing. */
    let project_name = "springfield-squidport";
    let url_instances = format!("/projects/{}/instances", project_name);
    testctx
        .client_testctx
        .make_request(
            Method::POST,
            "/projects",
            Some(ApiProjectCreateParams {
                identity: ApiIdentityMetadataCreateParams {
                    name: ApiName::try_from(project_name).unwrap(),
                    description: "a pier".to_string(),
                },
            }),
            StatusCode::CREATED,
        )
        .await
        .unwrap();

    /* List instances.  There aren't any yet. */
    let mut response = testctx
        .client_testctx
        .make_request_with_body(
            Method::GET,
            &url_instances,
            "".into(),
            StatusCode::OK,
        )
        .await
        .unwrap();
    let instances: Vec<ApiInstanceView> = read_ndjson(&mut response).await;
    assert_eq!(instances.len(), 0);

    /* Make sure we get a 404 if we fetch one. */
    let instance_url = format!("{}/just-rainsticks", url_instances);
    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::GET,
            &instance_url,
            "".into(),
            StatusCode::NOT_FOUND,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.message,
        "not found: instance with name \"just-rainsticks\""
    );

    /* Ditto if we try to delete one. */
    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::DELETE,
            &instance_url,
            "".into(),
            StatusCode::NOT_FOUND,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.message,
        "not found: instance with name \"just-rainsticks\""
    );

    /* Create an instance. */
    let new_instance = ApiInstanceCreateParams {
        identity: ApiIdentityMetadataCreateParams {
            name: ApiName::try_from("just-rainsticks").unwrap(),
            description: String::from("sells rainsticks"),
        },
        ncpus: ApiInstanceCpuCount(4),
        memory: ApiByteCount::from_mebibytes(256),
        boot_disk_size: ApiByteCount::from_gibibytes(1),
        hostname: String::from("rainsticks"),
    };
    let mut response = testctx
        .client_testctx
        .make_request(
            Method::POST,
            &url_instances,
            Some(new_instance.clone()),
            StatusCode::CREATED,
        )
        .await
        .unwrap();
    let instance: ApiInstanceView = read_json(&mut response).await;
    assert_eq!(instance.identity.name, "just-rainsticks");
    assert_eq!(instance.identity.description, "sells rainsticks");
    let ApiInstanceCpuCount(nfoundcpus) = instance.ncpus;
    assert_eq!(nfoundcpus, 4);
    assert_eq!(instance.memory.to_whole_mebibytes(), 256);
    assert_eq!(instance.boot_disk_size.to_whole_mebibytes(), 1024);
    assert_eq!(instance.hostname, "rainsticks");
    assert_eq!(instance.runtime.run_state, ApiInstanceState::Starting);

    /* Attempt to create a second instance with a conflicting name. */
    let error = testctx
        .client_testctx
        .make_request(
            Method::POST,
            &url_instances,
            Some(new_instance),
            StatusCode::BAD_REQUEST,
        )
        .await
        .unwrap_err();
    assert_eq!(error.message, "already exists: instance \"just-rainsticks\"");

    /* List instances again and expect to find the one we just created. */
    let mut response = testctx
        .client_testctx
        .make_request_with_body(
            Method::GET,
            &url_instances,
            "".into(),
            StatusCode::OK,
        )
        .await
        .unwrap();
    let instances: Vec<ApiInstanceView> = read_ndjson(&mut response).await;
    assert_eq!(instances.len(), 1);
    instances_eq(&instances[0], &instance);

    /* Fetch the instance and expect it to match. */
    let instance = instance_get(&testctx, &instance_url).await;
    instances_eq(&instances[0], &instance);
    assert_eq!(instance.runtime.run_state, ApiInstanceState::Starting);

    /*
     * Now, simulate completion of instance boot and check the state reported.
     */
    instance_simulate(controller, &instance.identity.id).await;
    let instance_next = instance_get(&testctx, &instance_url).await;
    identity_eq(&instance.identity, &instance_next.identity);
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Running);
    assert!(
        instance_next.runtime.run_state_updated
            > instance.runtime.run_state_updated
    );

    /*
     * Request another boot.  This should succeed without changing the state,
     * not even the state timestamp.
     */
    let instance = instance_next;
    let mut response = testctx
        .client_testctx
        .make_request_with_body(
            Method::POST,
            &format!("{}/start", instance_url),
            "".into(),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let instance_next = read_json::<ApiInstanceView>(&mut response).await;
    instances_eq(&instance, &instance_next);
    let instance_next = instance_get(&testctx, &instance_url).await;
    instances_eq(&instance, &instance_next);

    /*
     * Request a halt and verify both the immediate state and the finished state.
     */
    let instance = instance_next;
    let mut response = testctx
        .client_testctx
        .make_request_with_body(
            Method::POST,
            &format!("{}/stop", instance_url),
            "".into(),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let instance_next = read_json::<ApiInstanceView>(&mut response).await;
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Stopping);
    assert!(
        instance_next.runtime.run_state_updated
            > instance.runtime.run_state_updated
    );

    let instance = instance_next;
    instance_simulate(controller, &instance.identity.id).await;
    let instance_next = instance_get(&testctx, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Stopped);
    assert!(
        instance_next.runtime.run_state_updated
            > instance.runtime.run_state_updated
    );

    /*
     * Request another halt.  This should succeed without changing the state,
     * not even the state timestamp.
     */
    let instance = instance_next;
    let mut response = testctx
        .client_testctx
        .make_request_with_body(
            Method::POST,
            &format!("{}/stop", instance_url),
            "".into(),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let instance_next = read_json::<ApiInstanceView>(&mut response).await;
    instances_eq(&instance, &instance_next);
    let instance_next = instance_get(&testctx, &instance_url).await;
    instances_eq(&instance, &instance_next);

    /* Delete the instance. */
    testctx
        .client_testctx
        .make_request_with_body(
            Method::DELETE,
            &instance_url,
            "".into(),
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    /*
     * TODO-coverage re-add tests that check the server-side state after
     * deleting.  We need to figure out how these actually get cleaned up from
     * the API namespace when this happens.
     */

    /*
     * The rest of these examples attempt to create invalid instances.  We don't
     * do exhaustive tests of the model here -- those are part of unit tests --
     * but we exercise a few different types of errors to make sure those get
     * passed through properly.
     */

    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::POST,
            &url_instances,
            "{".into(),
            StatusCode::BAD_REQUEST,
        )
        .await
        .unwrap_err();
    assert!(error
        .message
        .starts_with("unable to parse body: EOF while parsing an object"));

    let request_body = r##"
        {
            "name": "an-instance",
            "description": "will never exist",
            "ncpus": -3,
            "memory": 256,
            "boot_disk_size": 2048,
            "hostname": "localhost",
        }
    "##;
    let error = testctx
        .client_testctx
        .make_request_with_body(
            Method::POST,
            &url_instances,
            request_body.into(),
            StatusCode::BAD_REQUEST,
        )
        .await
        .unwrap_err();
    assert!(error
        .message
        .starts_with("unable to parse body: invalid value: integer `-3`"));

    testctx.teardown().await;
}

async fn instance_get(
    testctx: &TestContext,
    instance_url: &str,
) -> ApiInstanceView {
    let mut response = testctx
        .client_testctx
        .make_request_with_body(
            Method::GET,
            &instance_url,
            "".into(),
            StatusCode::OK,
        )
        .await
        .unwrap();
    read_json::<ApiInstanceView>(&mut response).await
}

fn instances_eq(instance1: &ApiInstanceView, instance2: &ApiInstanceView) {
    identity_eq(&instance1.identity, &instance2.identity);
    assert_eq!(instance1.project_id, instance2.project_id);

    let ApiInstanceCpuCount(nfoundcpus1) = instance1.ncpus;
    let ApiInstanceCpuCount(nfoundcpus2) = instance2.ncpus;
    assert_eq!(nfoundcpus1, nfoundcpus2);

    assert_eq!(instance1.memory.to_bytes(), instance2.memory.to_bytes());
    assert_eq!(
        instance1.boot_disk_size.to_bytes(),
        instance2.boot_disk_size.to_bytes()
    );
    assert_eq!(instance1.hostname, instance2.hostname);
    assert_eq!(instance1.runtime.run_state, instance2.runtime.run_state);
    assert_eq!(
        instance1.runtime.run_state_updated,
        instance2.runtime.run_state_updated
    );
}

fn identity_eq(ident1: &ApiIdentityMetadata, ident2: &ApiIdentityMetadata) {
    assert_eq!(ident1.id, ident2.id);
    assert_eq!(ident1.name, ident2.name);
    assert_eq!(ident1.description, ident2.description);
    assert_eq!(ident1.time_created, ident2.time_created);
    assert_eq!(ident1.time_modified, ident2.time_modified);
}

/**
 * Simulate completion of an ongoing instance state transition.  To do this, we
 * have to look up the instance, then get the server controller associated with
 * that instance, and then tell it to finish simulating whatever async
 * transition is going on.
 */
async fn instance_simulate(controller: &Arc<OxideController>, id: &Uuid) {
    let sc = controller.instance_server_by_id(id).await.unwrap();
    sc.instance_finish_transition(id.clone()).await;
}
