/*!
 * Tests basic instance support in the API
 */

use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::InstanceCreateParams;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::Name;
use omicron_common::SledAgentTestInterfaces as _;
use omicron_nexus::Nexus;
use omicron_nexus::TestInterfaces as _;
use std::convert::TryFrom;
use std::sync::Arc;
use uuid::Uuid;

use dropshot::test_util::object_delete;
use dropshot::test_util::object_get;
use dropshot::test_util::objects_list_page;
use dropshot::test_util::objects_post;
use dropshot::test_util::read_json;
use dropshot::test_util::ClientTestContext;

pub mod common;
use common::identity_eq;
use common::resource_helpers::{create_organization, create_project};
use common::test_setup;

static ORGANIZATION_NAME: &str = "test-org";
static PROJECT_NAME: &str = "springfield-squidport";

#[tokio::test]
async fn test_instances_access_before_create_returns_not_found() {
    let cptestctx =
        test_setup("test_instances_access_before_create_returns_not_found")
            .await;
    let client = &cptestctx.external_client;

    /* Create a project that we'll use for testing. */
    create_organization(&client, ORGANIZATION_NAME).await;
    let url_instances = format!(
        "/organizations/{}/projects/{}/instances",
        ORGANIZATION_NAME, PROJECT_NAME
    );
    let _ = create_project(&client, ORGANIZATION_NAME, PROJECT_NAME).await;

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
    cptestctx.teardown().await;
}

#[tokio::test]
async fn test_instances_create_reboot_halt() {
    let cptestctx = test_setup("test_instances_create_reboot_halt").await;
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.apictx;
    let nexus = &apictx.nexus;

    /* Create a project that we'll use for testing. */
    create_organization(&client, ORGANIZATION_NAME).await;
    let url_instances = format!(
        "/organizations/{}/projects/{}/instances",
        ORGANIZATION_NAME, PROJECT_NAME
    );
    let _ = create_project(&client, ORGANIZATION_NAME, PROJECT_NAME).await;

    /* Create an instance. */
    let instance_url = format!("{}/just-rainsticks", url_instances);
    let new_instance = InstanceCreateParams {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from("just-rainsticks").unwrap(),
            description: String::from("sells rainsticks"),
        },
        ncpus: InstanceCpuCount(4),
        memory: ByteCount::from_mebibytes_u32(256),
        hostname: String::from("rainsticks"),
    };
    let instance: Instance =
        objects_post(&client, &url_instances, new_instance.clone()).await;
    assert_eq!(instance.identity.name, "just-rainsticks");
    assert_eq!(instance.identity.description, "sells rainsticks");
    let InstanceCpuCount(nfoundcpus) = instance.ncpus;
    assert_eq!(nfoundcpus, 4);
    assert_eq!(instance.memory.to_whole_mebibytes(), 256);
    assert_eq!(instance.hostname, "rainsticks");
    assert_eq!(instance.runtime.run_state, InstanceState::Starting);

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
    assert_eq!(instance.runtime.run_state, InstanceState::Starting);

    /*
     * Now, simulate completion of instance boot and check the state reported.
     */
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    identity_eq(&instance.identity, &instance_next.identity);
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);
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
    assert_eq!(instance_next.runtime.run_state, InstanceState::Rebooting);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Starting);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);
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
    assert_eq!(instance_next.runtime.run_state, InstanceState::Stopping);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Stopped);
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
    assert_eq!(instance_next.runtime.run_state, InstanceState::Starting);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    let instance_next =
        instance_post(&client, &instance_url, InstanceOp::Reboot).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Rebooting);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Starting);
    assert!(
        instance_next.runtime.time_run_state_updated
            > instance.runtime.time_run_state_updated
    );

    let instance = instance_next;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);
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
    assert_eq!(instance_next.runtime.run_state, InstanceState::Stopping);
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
    assert_eq!(instance_next.runtime.run_state, InstanceState::Stopped);
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

    cptestctx.teardown().await;
}

#[tokio::test]
async fn test_instances_delete_fails_when_running_succeeds_when_stopped() {
    let cptestctx = test_setup(
        "test_instances_delete_fails_when_running_succeeds_when_stopped",
    )
    .await;
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.apictx;
    let nexus = &apictx.nexus;

    // Create a project that we'll use for testing.
    create_organization(&client, ORGANIZATION_NAME).await;
    let url_instances = format!(
        "/organizations/{}/projects/{}/instances",
        ORGANIZATION_NAME, PROJECT_NAME
    );
    let _ = create_project(&client, ORGANIZATION_NAME, PROJECT_NAME).await;

    // Create an instance.
    let instance_url = format!("{}/just-rainsticks", url_instances);
    let new_instance = InstanceCreateParams {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from("just-rainsticks").unwrap(),
            description: String::from("sells rainsticks"),
        },
        ncpus: InstanceCpuCount(4),
        memory: ByteCount::from_mebibytes_u32(256),
        hostname: String::from("rainsticks"),
    };
    let instance: Instance =
        objects_post(&client, &url_instances, new_instance.clone()).await;

    // Simulate the instance booting.
    instance_simulate(nexus, &instance.identity.id).await;
    let instance_next = instance_get(&client, &instance_url).await;
    identity_eq(&instance.identity, &instance_next.identity);
    assert_eq!(instance_next.runtime.run_state, InstanceState::Running);

    // Attempt to delete a running instance. This should fail.
    let error = client
        .make_request_error(
            Method::DELETE,
            &instance_url,
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(
        error.message,
        "instance cannot be deleted in state \"running\""
    );

    // Stop the instance
    let instance =
        instance_post(&client, &instance_url, InstanceOp::Stop).await;
    instance_simulate(nexus, &instance.identity.id).await;
    let instance = instance_get(&client, &instance_url).await;
    assert_eq!(instance.runtime.run_state, InstanceState::Stopped);

    // Now deletion should succeed.
    object_delete(&client, &instance_url).await;

    cptestctx.teardown().await;
}

#[tokio::test]
async fn test_instances_invalid_creation_returns_bad_request() {
    /*
     * The rest of these examples attempt to create invalid instances.  We don't
     * do exhaustive tests of the model here -- those are part of unit tests --
     * but we exercise a few different types of errors to make sure those get
     * passed through properly.
     */
    let cptestctx =
        test_setup("test_instances_invalid_creation_returns_bad_request").await;
    let client = &cptestctx.external_client;
    let url_instances = format!(
        "/organizations/{}/projects/{}/instances",
        ORGANIZATION_NAME, PROJECT_NAME
    );

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
) -> Instance {
    object_get::<Instance>(client, instance_url).await
}

async fn instances_list(
    client: &ClientTestContext,
    instances_url: &str,
) -> Vec<Instance> {
    objects_list_page::<Instance>(client, instances_url).await.items
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
) -> Instance {
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
    read_json::<Instance>(&mut response).await
}

fn instances_eq(instance1: &Instance, instance2: &Instance) {
    identity_eq(&instance1.identity, &instance2.identity);
    assert_eq!(instance1.project_id, instance2.project_id);

    let InstanceCpuCount(nfoundcpus1) = instance1.ncpus;
    let InstanceCpuCount(nfoundcpus2) = instance2.ncpus;
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
