/*!
 * Tests basic disk support in the API
 */

use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::DiskAttachment;
use omicron_common::api::external::DiskCreateParams;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::InstanceCreateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::Project;
use omicron_common::api::external::ProjectCreateParams;
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

/*
 * TODO-cleanup the mess of URLs used here and in test_instances.rs ought to
 * come from common code.
 */
#[tokio::test]
async fn test_disks() {
    let cptestctx = test_setup("test_disks").await;
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.apictx;
    let nexus = &apictx.nexus;

    /* Create a project for testing. */
    let project_name = "springfield-squidport-disks";
    let url_disks = format!("/projects/{}/disks", project_name);
    let project: Project = objects_post(
        &client,
        "/projects",
        ProjectCreateParams {
            identity: IdentityMetadataCreateParams {
                name: Name::try_from(project_name).unwrap(),
                description: "a pier".to_string(),
            },
        },
    )
    .await;

    /* List disks.  There aren't any yet. */
    let disks = disks_list(&client, &url_disks).await;
    assert_eq!(disks.len(), 0);

    /* Make sure we get a 404 if we fetch one. */
    let disk_url = format!("{}/just-rainsticks", url_disks);
    let error = client
        .make_request_error(Method::GET, &disk_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: disk with name \"just-rainsticks\"");

    /* We should also get a 404 if we delete one. */
    let error = client
        .make_request_error(Method::DELETE, &disk_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: disk with name \"just-rainsticks\"");

    /* Create a disk. */
    let new_disk = DiskCreateParams {
        identity: IdentityMetadataCreateParams {
            name: Name::try_from("just-rainsticks").unwrap(),
            description: String::from("sells rainsticks"),
        },
        snapshot_id: None,
        size: ByteCount::from_gibibytes_u32(1),
    };
    let disk: Disk = objects_post(&client, &url_disks, new_disk.clone()).await;
    assert_eq!(disk.identity.name, "just-rainsticks");
    assert_eq!(disk.identity.description, "sells rainsticks");
    assert_eq!(disk.project_id, project.identity.id);
    assert_eq!(disk.snapshot_id, None);
    assert_eq!(disk.size.to_whole_mebibytes(), 1024);
    assert_eq!(disk.state, DiskState::Creating);

    /*
     * Fetch the disk and expect it to match what we just created except that
     * the state will now be "Detached", as the server has simulated the create
     * process.
     */
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.identity.name, "just-rainsticks");
    assert_eq!(disk.identity.description, "sells rainsticks");
    assert_eq!(disk.project_id, project.identity.id);
    assert_eq!(disk.snapshot_id, None);
    assert_eq!(disk.size.to_whole_mebibytes(), 1024);
    assert_eq!(disk.state, DiskState::Detached);

    /* Attempt to create a second disk with a conflicting name. */
    let error = client
        .make_request_error_body(
            Method::POST,
            &url_disks,
            new_disk,
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(error.message, "already exists: disk \"just-rainsticks\"");

    /* List disks again and expect to find the one we just created. */
    let disks = disks_list(&client, &url_disks).await;
    assert_eq!(disks.len(), 1);
    disks_eq(&disks[0], &disk);

    /* Create an instance to attach the disk. */
    let url_instances = format!("/projects/{}/instances", project_name);
    let instance: Instance = objects_post(
        &client,
        &url_instances,
        InstanceCreateParams {
            identity: IdentityMetadataCreateParams {
                name: Name::try_from("just-rainsticks").unwrap(),
                description: String::from("sells rainsticks"),
            },
            ncpus: InstanceCpuCount(4),
            memory: ByteCount::from_mebibytes_u32(256),
            hostname: String::from("rainsticks"),
        },
    )
    .await;

    /*
     * Verify that there are no disks attached to the instance, and specifically
     * that our disk is not attached to this instance.
     */
    let url_instance_disks = format!(
        "/projects/{}/instances/{}/disks",
        project_name,
        instance.identity.name.as_str()
    );
    let url_instance_disk = format!(
        "/projects/{}/instances/{}/disks/{}",
        project_name,
        instance.identity.name.as_str(),
        disk.identity.name.as_str(),
    );
    let attachments =
        object_get::<Vec<DiskAttachment>>(&client, &url_instance_disks).await;
    assert_eq!(attachments.len(), 0);
    let error = client
        .make_request_error(
            Method::GET,
            &url_instance_disk,
            StatusCode::NOT_FOUND,
        )
        .await;
    assert_eq!(
        "disk \"just-rainsticks\" is not attached to instance \
         \"just-rainsticks\"",
        error.message
    );

    /* Start attaching the disk to the instance. */
    let mut response = client
        .make_request_no_body(
            Method::PUT,
            &url_instance_disk,
            StatusCode::CREATED,
        )
        .await
        .unwrap();
    let attachment: DiskAttachment = read_json(&mut response).await;
    let instance_id = &instance.identity.id;
    assert_eq!(attachment.instance_id, *instance_id);
    assert_eq!(attachment.disk_name, disk.identity.name);
    assert_eq!(attachment.disk_id, disk.identity.id);
    assert_eq!(
        attachment.disk_state,
        DiskState::Attaching(instance_id.clone())
    );

    let attachment: DiskAttachment =
        object_get(&client, &url_instance_disk).await;
    assert_eq!(attachment.instance_id, instance.identity.id);
    assert_eq!(attachment.disk_name, disk.identity.name);
    assert_eq!(attachment.disk_id, disk.identity.id);
    assert_eq!(
        attachment.disk_state,
        DiskState::Attaching(instance_id.clone())
    );

    /* Check the state of the disk, too. */
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Attaching(instance_id.clone()));

    /*
     * Finish simulation of the attachment and verify the new state, both on the
     * attachment and the disk itself.
     */
    disk_simulate(nexus, &disk.identity.id).await;
    let attachment: DiskAttachment =
        object_get(&client, &url_instance_disk).await;
    assert_eq!(attachment.instance_id, instance.identity.id);
    assert_eq!(attachment.disk_name, disk.identity.name);
    assert_eq!(attachment.disk_id, disk.identity.id);
    assert_eq!(attachment.disk_state, DiskState::Attached(instance_id.clone()));

    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Attached(instance_id.clone()));

    /*
     * Attach the disk to the same instance.  This should complete immediately
     * with no state change.
     */
    client
        .make_request_no_body(
            Method::PUT,
            &url_instance_disk,
            StatusCode::CREATED,
        )
        .await
        .unwrap();
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Attached(instance_id.clone()));

    /*
     * Create a second instance and try to attach the disk to that.  This should
     * fail and the disk should remain attached to the first instance.
     */
    let instance2: Instance = objects_post(
        &client,
        &url_instances,
        InstanceCreateParams {
            identity: IdentityMetadataCreateParams {
                name: Name::try_from("instance2").unwrap(),
                description: String::from("instance2"),
            },
            ncpus: InstanceCpuCount(4),
            memory: ByteCount::from_mebibytes_u32(256),
            hostname: String::from("instance2"),
        },
    )
    .await;
    let url_instance2_disk = format!(
        "/projects/{}/instances/{}/disks/{}",
        project_name,
        instance2.identity.name.as_str(),
        disk.identity.name.as_str()
    );
    let error = client
        .make_request_error(
            Method::PUT,
            &url_instance2_disk,
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(
        error.message,
        "cannot attach disk \"just-rainsticks\": disk is attached to another \
         instance"
    );

    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Attached(instance_id.clone()));

    let error = client
        .make_request_error(
            Method::GET,
            &url_instance2_disk,
            StatusCode::NOT_FOUND,
        )
        .await;
    assert_eq!(
        error.message,
        "disk \"just-rainsticks\" is not attached to instance \"instance2\""
    );

    /*
     * Begin detaching the disk.
     */
    client
        .make_request_no_body(
            Method::DELETE,
            &url_instance_disk,
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();
    let attachment: DiskAttachment =
        object_get(&client, &url_instance_disk).await;
    assert_eq!(
        attachment.disk_state,
        DiskState::Detaching(instance_id.clone())
    );

    /* Check the state of the disk, too. */
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detaching(instance_id.clone()));

    /* It's still illegal to attach this disk elsewhere. */
    let error = client
        .make_request_error(
            Method::PUT,
            &url_instance2_disk,
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(
        error.message,
        "cannot attach disk \"just-rainsticks\": disk is attached to another \
         instance"
    );

    /* It's even illegal to attach this disk back to the same instance. */
    let error = client
        .make_request_error(
            Method::PUT,
            &url_instance_disk,
            StatusCode::BAD_REQUEST,
        )
        .await;
    /* TODO-debug the error message here is misleading. */
    assert_eq!(
        error.message,
        "cannot attach disk \"just-rainsticks\": disk is attached to another \
         instance"
    );

    /* However, there's no problem attempting to detach it again. */
    client
        .make_request_no_body(
            Method::DELETE,
            &url_instance_disk,
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detaching(instance_id.clone()));

    /* Finish the detachment. */
    disk_simulate(nexus, &disk.identity.id).await;
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detached);
    let error = client
        .make_request_error(
            Method::GET,
            &url_instance_disk,
            StatusCode::NOT_FOUND,
        )
        .await;
    assert_eq!(
        error.message,
        "disk \"just-rainsticks\" is not attached to instance \
         \"just-rainsticks\""
    );

    /* Since delete is idempotent, we can detach it again -- from either one. */
    client
        .make_request_no_body(
            Method::DELETE,
            &url_instance_disk,
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();
    client
        .make_request_no_body(
            Method::DELETE,
            &url_instance2_disk,
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();

    /* Now, start attaching it again to the second instance. */
    let mut response = client
        .make_request_no_body(
            Method::PUT,
            &url_instance2_disk,
            StatusCode::CREATED,
        )
        .await
        .unwrap();
    let attachment: DiskAttachment = read_json(&mut response).await;
    let instance2_id = &instance2.identity.id;
    assert_eq!(attachment.instance_id, *instance2_id);
    assert_eq!(attachment.disk_name, disk.identity.name);
    assert_eq!(attachment.disk_id, disk.identity.id);
    assert_eq!(
        attachment.disk_state,
        DiskState::Attaching(instance2_id.clone())
    );

    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Attaching(instance2_id.clone()));

    /*
     * At this point, it's not legal to attempt to attach it to a different
     * instance (the first one).
     */
    let error = client
        .make_request_error(
            Method::PUT,
            &url_instance_disk,
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!(
        error.message,
        "cannot attach disk \"just-rainsticks\": disk is attached to another \
         instance"
    );

    /* It's fine to attempt another attachment to the same instance. */
    client
        .make_request_no_body(
            Method::PUT,
            &url_instance2_disk,
            StatusCode::CREATED,
        )
        .await
        .unwrap();
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Attaching(instance2_id.clone()));

    /* It's not allowed to delete a disk that's attaching. */
    let error = client
        .make_request_error(Method::DELETE, &disk_url, StatusCode::BAD_REQUEST)
        .await;
    assert_eq!(error.message, "disk is attached");

    /* Now, begin a detach while the disk is still being attached. */
    client
        .make_request_no_body(
            Method::DELETE,
            &url_instance2_disk,
            StatusCode::NO_CONTENT,
        )
        .await
        .unwrap();
    let attachment: DiskAttachment =
        object_get(&client, &url_instance2_disk).await;
    assert_eq!(
        attachment.disk_state,
        DiskState::Detaching(instance2_id.clone())
    );
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detaching(instance2_id.clone()));

    /* It's not allowed to delete a disk that's detaching, either. */
    let error = client
        .make_request_error(Method::DELETE, &disk_url, StatusCode::BAD_REQUEST)
        .await;
    assert_eq!(error.message, "disk is attached");

    /* Finish detachment. */
    disk_simulate(nexus, &disk.identity.id).await;
    let disk = disk_get(&client, &disk_url).await;
    assert_eq!(disk.state, DiskState::Detached);

    let error = client
        .make_request_error(
            Method::GET,
            &url_instance_disk,
            StatusCode::NOT_FOUND,
        )
        .await;
    assert_eq!(
        error.message,
        "disk \"just-rainsticks\" is not attached to instance \
         \"just-rainsticks\""
    );

    let error = client
        .make_request_error(
            Method::GET,
            &url_instance2_disk,
            StatusCode::NOT_FOUND,
        )
        .await;
    assert_eq!(
        error.message,
        "disk \"just-rainsticks\" is not attached to instance \"instance2\""
    );

    /* It's not allowed to delete a disk that's detaching, either. */
    client
        .make_request_no_body(Method::DELETE, &disk_url, StatusCode::NO_CONTENT)
        .await
        .unwrap();

    /* It should no longer be present in our list of disks. */
    assert_eq!(disks_list(&client, &url_disks).await.len(), 0);
    /* We shouldn't find it if we request it explicitly. */
    let error = client
        .make_request_error(Method::GET, &disk_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: disk with name \"just-rainsticks\"");

    cptestctx.teardown().await;
}

async fn disk_get(client: &ClientTestContext, disk_url: &str) -> Disk {
    object_get::<Disk>(client, disk_url).await
}

async fn disks_list(client: &ClientTestContext, list_url: &str) -> Vec<Disk> {
    objects_list_page::<Disk>(client, list_url).await.items
}

fn disks_eq(disk1: &Disk, disk2: &Disk) {
    identity_eq(&disk1.identity, &disk2.identity);
    assert_eq!(disk1.project_id, disk2.project_id);
    assert_eq!(disk1.snapshot_id, disk2.snapshot_id);
    assert_eq!(disk1.size.to_bytes(), disk2.size.to_bytes());
    assert_eq!(disk1.state, disk2.state);
    assert_eq!(disk1.device_path, disk2.device_path);
}

/**
 * Simulate completion of an ongoing disk state transition.
 */
async fn disk_simulate(nexus: &Arc<Nexus>, id: &Uuid) {
    let sa = nexus.disk_sled_by_id(id).await.unwrap();
    sa.disk_finish_transition(id.clone()).await;
}
