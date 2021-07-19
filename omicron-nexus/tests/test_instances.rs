/*!
 * Tests basic instance support in the API
 */

use http::method::Method;
use http::StatusCode;
use omicron_common::api::ApiByteCount;
use omicron_common::api::ApiIdentityMetadataCreateParams;
use omicron_common::api::ApiInstanceCpuCount;
use omicron_common::api::ApiInstanceCreateParams;
use omicron_common::api::ApiInstanceState;
use omicron_common::api::ApiInstanceView;
use omicron_common::api::ApiName;
use omicron_common::api::ApiProjectCreateParams;
use omicron_common::api::ApiProjectView;
use omicron_common::SledAgentTestInterfaces as _;
use omicron_nexus::Nexus;
use omicron_nexus::TestInterfaces as _;
use std::convert::TryFrom;
use std::sync::Arc;
use uuid::Uuid;

use dropshot::test_util::object_get;
use dropshot::test_util::objects_list_page;
use dropshot::test_util::objects_post;
use dropshot::test_util::read_json;
use dropshot::test_util::ClientTestContext;

pub mod common;
use common::identity_eq;
use common::test_setup;

#[macro_use]
extern crate slog;

#[tokio::test]
async fn test_instances() {
    let cptestctx = test_setup("test_instances").await;
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.apictx;
    let nexus = &apictx.nexus;

    /* Create a project that we'll use for testing. */
    let project_name = "springfield-squidport";
    let url_instances = format!("/projects/{}/instances", project_name);
    let _: ApiProjectView = objects_post(
        &client,
        "/projects",
        ApiProjectCreateParams {
            identity: ApiIdentityMetadataCreateParams {
                name: ApiName::try_from(project_name).unwrap(),
                description: "a pier".to_string(),
            },
        },
    )
    .await;

    /* List instances.  There aren't any yet. */
    let instances = instances_list(&client, &url_instances).await;
    assert_eq!(instances.len(), 0);

    /* Make sure we get a 404 if we fetch one. */
    let instance_url = format!("{}/just-rainsticks", url_instances);
    let error = client
        .make_request_error(Method::GET, &instance_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(
        error.message,
        "not found: instance with name \"just-rainsticks\""
    );

    /* Ditto if we try to delete one. */
    let error = client
        .make_request_error(
            Method::DELETE,
            &instance_url,
            StatusCode::NOT_FOUND,
        )
        .await;
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
        memory: ApiByteCount::from_mebibytes_u32(256),
        hostname: String::from("rainsticks"),
    };
    let instance: ApiInstanceView =
        objects_post(&client, &url_instances, new_instance.clone()).await;
    assert_eq!(instance.identity.name, "just-rainsticks");
    assert_eq!(instance.identity.description, "sells rainsticks");
    let ApiInstanceCpuCount(nfoundcpus) = instance.ncpus;
    assert_eq!(nfoundcpus, 4);
    assert_eq!(instance.memory.to_whole_mebibytes(), 256);
    assert_eq!(instance.hostname, "rainsticks");
    assert_eq!(instance.runtime.run_state, ApiInstanceState::Starting);

    /* Attempt to create a second instance with a conflicting name. */
    let error = client
        .make_request_error_body(
            Method::POST,
            &url_instances,
            new_instance,
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(error.message, "already exists: instance \"just-rainsticks\"");

    /* List instances again and expect to find the one we just created. */
    let instances = instances_list(&client, &url_instances).await;
    assert_eq!(instances.len(), 1);
    instances_eq(&instances[0], &instance);

    /* Fetch the instance and expect it to match. */
    let instance = instance_get(&client, &instance_url).await;
    instances_eq(&instances[0], &instance);
    assert_eq!(instance.runtime.run_state, ApiInstanceState::Starting);

    /*
     * Now, simulate completion of instance boot and check the state reported.
     */
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    identity_eq(&instance.identity, &instance_next.identity);
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Running);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    /*
     * Request another boot.  This should succeed without changing the state,
     * not even the state timestamp.
     */
    let instance = instance_next;
    let instance_next =
        instance_post(&client, &instance_url, InstanceOp::Start).await;
    instances_eq(&instance, &instance_next);
    let instance_next = instance_get(&client, &instance_url).await;
    instances_eq(&instance, &instance_next);

    /*
     * Reboot the instance.
     */
    let instance = instance_next;
    let instance_next =
        instance_post(&client, &instance_url, InstanceOp::Reboot).await;
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Rebooting);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Starting);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Running);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    /*
     * Request a halt and verify both the immediate state and the finished state.
     */
    let instance = instance_next;
    let instance_next =
        instance_post(&client, &instance_url, InstanceOp::Stop).await;
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Stopping);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Stopped);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    /*
     * Request another halt.  This should succeed without changing the state,
     * not even the state timestamp.
     */
    let instance = instance_next;
    let instance_next =
        instance_post(&client, &instance_url, InstanceOp::Stop).await;
    instances_eq(&instance, &instance_next);
    let instance_next = instance_get(&client, &instance_url).await;
    instances_eq(&instance, &instance_next);

    /*
     * Attempt to reboot the halted instance.  This should fail.
     */
    let error = client
        .make_request_error(
            Method::POST,
            &format!("{}/reboot", instance_url),
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(error.message, "cannot reboot instance in state \"stopped\"");

    /*
     * Start the instance.  While it's starting, issue a reboot.  This should
     * succeed, having stopped in between.
     */
    let instance = instance_next;
    let instance_next =
        instance_post(&client, &instance_url, InstanceOp::Start).await;
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Starting);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    let instance_next =
        instance_post(&client, &instance_url, InstanceOp::Reboot).await;
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Rebooting);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Starting);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Running);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    /*
     * Stop the instance.  While it's stopping, issue a reboot.  This should
     * fail because you cannot stop an instance that's en route to a stopped
     * state.
     */
    let instance = instance_next;
    let instance_next =
        instance_post(&client, &instance_url, InstanceOp::Stop).await;
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Stopping);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let error = client
        .make_request_error(
            Method::POST,
            &format!("{}/reboot", instance_url),
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(error.message, "cannot reboot instance in state \"stopping\"");
    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, ApiInstanceState::Stopped);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    /* TODO-coverage add a test to try to delete the project at this point. */

    /* Delete the instance. */
    client
        .make_request_no_body(
            Method::DELETE,
            &instance_url,
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
     * Once more, try to reboot it.  This should not work on a destroyed
     * instance.
     */
    client
        .make_request_error(
            Method::POST,
            &format!("{}/reboot", instance_url),
            StatusCode::NOT_FOUND,
        )
        .await;

    /*
     * Similarly, we should not be able to start or stop the instance.
     */
    client
        .make_request_error(
            Method::POST,
            &format!("{}/start", instance_url),
            StatusCode::NOT_FOUND,
        )
        .await;

    client
        .make_request_error(
            Method::POST,
            &format!("{}/stop", instance_url),
            StatusCode::NOT_FOUND,
        )
        .await;

    /*
     * The rest of these examples attempt to create invalid instances.  We don't
     * do exhaustive tests of the model here -- those are part of unit tests --
     * but we exercise a few different types of errors to make sure those get
     * passed through properly.
     */

    let error = client
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
            "hostname": "localhost",
        }
    "##;
    let error = client
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

    cptestctx.teardown().await;
}

async fn instance_get(
    client: &ClientTestContext,
    instance_url: &str,
) -> ApiInstanceView {
    object_get::<ApiInstanceView>(client, instance_url).await
}

async fn instances_list(
    client: &ClientTestContext,
    instances_url: &str,
) -> Vec<ApiInstanceView> {
    objects_list_page::<ApiInstanceView>(client, instances_url).await.items
}

/**
 * Convenience function for starting, stopping, or rebooting an instance.
 */
enum InstanceOp {
    Start,
    Stop,
    Reboot,
}
async fn instance_post(
    client: &ClientTestContext,
    instance_url: &str,
    which: InstanceOp,
) -> ApiInstanceView {
    let url = format!(
        "{}/{}",
        instance_url,
        match which {
            InstanceOp::Start => "start",
            InstanceOp::Stop => "stop",
            InstanceOp::Reboot => "reboot",
        }
    );
    let mut response = client
        .make_request_with_body(
            Method::POST,
            &url,
            "".into(),
            StatusCode::ACCEPTED,
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
    assert_eq!(instance1.hostname, instance2.hostname);
    assert_eq!(instance1.runtime.run_state, instance2.runtime.run_state);
    assert_eq!(
        instance1.runtime.time_run_state_updated,
        instance2.runtime.time_run_state_updated
    );
}

/**
 * Simulate completion of an ongoing instance state transition.  To do this, we
 * have to look up the instance, then get the sled agent associated with that
 * instance, and then tell it to finish simulating whatever async transition is
 * going on.
 */
async fn instance_simulate(nexus: &Arc<Nexus>, id: &Uuid) {
    let sa = nexus.instance_sled_by_id(id).await.unwrap();
    sa.instance_finish_transition(id.clone()).await;
}
