// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Tests basic disk support in the API
 */

use http::method::Method;
use http::StatusCode;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceCpuCount;
use omicron_nexus::TestInterfaces as _;
use omicron_nexus::{external_api::params, Nexus};
use sled_agent_client::TestInterfaces as _;
use std::sync::Arc;
use uuid::Uuid;

use dropshot::test_util::object_get;
use dropshot::test_util::objects_list_page;
use dropshot::test_util::objects_post;
use dropshot::test_util::read_json;
use dropshot::test_util::ClientTestContext;

use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::identity_eq;
use nexus_test_utils::resource_helpers::create_organization;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;

/*
 * TODO-cleanup the mess of URLs used here and in test_instances.rs ought to
 * come from common code.
 */
#[nexus_test]
async fn test_disks(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let apictx = &cptestctx.server.apictx;
    let nexus = &apictx.nexus;

    /* Create a project for testing. */
    let org_name = "test-org";
    create_organization(&client, &org_name).await;
    let project_name = "springfield-squidport-disks";
    let url_disks =
        format!("/organizations/{}/projects/{}/disks", org_name, project_name);
    let project = create_project(client, &org_name, &project_name).await;

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
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &disk_url)
            .expect_status(Some(StatusCode::NOT_FOUND)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success")
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(error.message, "not found: disk with name \"just-rainsticks\"");

    /* Create a disk. */
    let new_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: "just-rainsticks".parse().unwrap(),
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
    let url_instances = format!(
        "/organizations/{}/projects/{}/instances",
        org_name, project_name
    );
    let instance: Instance = objects_post(
        &client,
        &url_instances,
        params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: "just-rainsticks".parse().unwrap(),
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
        "/organizations/{}/projects/{}/instances/{}/disks",
        org_name,
        project_name,
        instance.identity.name.as_str()
    );
    let url_instance_disk = format!(
        "/organizations/{}/projects/{}/instances/{}/disks/{}",
        org_name,
        project_name,
        instance.identity.name.as_str(),
        disk.identity.name.as_str(),
    );
    let disks = objects_list_page::<Disk>(&client, &url_instance_disks).await;
    assert_eq!(disks.items.len(), 0);
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

    let url_instance_attach_disk = format!(
        "/organizations/{}/projects/{}/instances/{}/disks/attach",
        org_name,
        project_name,
        instance.identity.name.as_str(),
    );
    let url_instance_detach_disk = format!(
        "/organizations/{}/projects/{}/instances/{}/disks/detach",
        org_name,
        project_name,
        instance.identity.name.as_str(),
    );

    /* Start attaching the disk to the instance. */
    let mut response = client
        .make_request(
            Method::POST,
            &url_instance_attach_disk,
            Some(params::DiskReference { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let attached_disk: Disk = read_json(&mut response).await;
    let instance_id = &instance.identity.id;
    assert_eq!(attached_disk.identity.name, disk.identity.name);
    assert_eq!(attached_disk.identity.id, disk.identity.id);
    assert_eq!(attached_disk.state, DiskState::Attaching(instance_id.clone()));

    /*
     * Finish simulation of the attachment and verify the new state, both on the
     * attachment and the disk itself.
     */
    disk_simulate(nexus, &disk.identity.id).await;
    let attached_disk: Disk = object_get(&client, &url_instance_disk).await;
    assert_eq!(attached_disk.identity.name, disk.identity.name);
    assert_eq!(attached_disk.identity.id, disk.identity.id);
    assert_eq!(attached_disk.state, DiskState::Attached(instance_id.clone()));

    /*
     * Attach the disk to the same instance.  This should complete immediately
     * with no state change.
     */
    client
        .make_request(
            Method::POST,
            &url_instance_attach_disk,
            Some(params::DiskReference { disk: disk.identity.name }),
            StatusCode::ACCEPTED,
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
        params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: "instance2".parse().unwrap(),
                description: "instance2".to_string(),
            },
            ncpus: InstanceCpuCount(4),
            memory: ByteCount::from_mebibytes_u32(256),
            hostname: "instance2".to_string(),
        },
    )
    .await;
    let url_instance2_disks = format!(
        "/organizations/{}/projects/{}/instances/{}/disks",
        org_name,
        project_name,
        instance2.identity.name.as_str(),
    );
    let url_instance2_disk = format!(
        "/organizations/{}/projects/{}/instances/{}/disks/{}",
        org_name,
        project_name,
        instance2.identity.name.as_str(),
        disk.identity.name.as_str(),
    );
    let url_instance2_attach_disk = format!(
        "/organizations/{}/projects/{}/instances/{}/disks/attach",
        org_name,
        project_name,
        instance2.identity.name.as_str(),
    );
    let url_instance2_detach_disk = format!(
        "/organizations/{}/projects/{}/instances/{}/disks/detach",
        org_name,
        project_name,
        instance2.identity.name.as_str(),
    );
    let error = client
        .make_request_error_body(
            Method::POST,
            &url_instance2_disks,
            params::DiskReference { disk: disk.identity.name.clone() },
            StatusCode::CONFLICT,
        )
        .await;
    assert_eq!(
        error.message,
        "cannot attach disk \"just-rainsticks\": disk is attached to another \
         instance"
    );

    let attached_disk = disk_get(&client, &disk_url).await;
    assert_eq!(attached_disk.state, DiskState::Attached(instance_id.clone()));

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
        .make_request(
            Method::POST,
            &url_instance_detach_disk,
            Some(params::DiskReference { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let detached_disk: Disk = object_get(&client, &url_instance_disk).await;
    assert_eq!(detached_disk.state, DiskState::Detaching(instance_id.clone()));

    /* It's still illegal to attach this disk elsewhere. */
    let error = client
        .make_request_error_body(
            Method::POST,
            &url_instance2_attach_disk,
            Some(params::DiskReference { disk: disk.identity.name.clone() }),
            StatusCode::CONFLICT,
        )
        .await;
    assert_eq!(
        error.message,
        "cannot attach disk \"just-rainsticks\": disk is attached to another \
         instance"
    );

    /* It's even illegal to attach this disk back to the same instance. */
    let error = client
        .make_request_error_body(
            Method::POST,
            &url_instance_attach_disk,
            Some(params::DiskReference { disk: disk.identity.name.clone() }),
            StatusCode::CONFLICT,
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
        .make_request(
            Method::POST,
            &url_instance_detach_disk,
            Some(params::DiskReference { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
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
        .make_request(
            Method::POST,
            &url_instance_detach_disk,
            Some(params::DiskReference { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    client
        .make_request(
            Method::POST,
            &url_instance2_detach_disk,
            Some(params::DiskReference { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();

    /* Now, start attaching it again to the second instance. */
    let mut response = client
        .make_request(
            Method::POST,
            &url_instance2_attach_disk,
            Some(params::DiskReference { disk: disk.identity.name.clone() }),
            StatusCode::ACCEPTED,
        )
        .await
        .unwrap();
    let attached_disk: Disk = read_json(&mut response).await;
    let instance2_id = &instance2.identity.id;
    assert_eq!(attached_disk.identity.name, disk.identity.name);
    assert_eq!(attached_disk.identity.id, disk.identity.id);
    assert_eq!(attached_disk.state, DiskState::Attaching(instance2_id.clone()));

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
    let detached_disk: Disk = object_get(&client, &url_instance2_disk).await;
    assert_eq!(detached_disk.state, DiskState::Detaching(instance2_id.clone()));

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

    /*
     * If we're not authenticated, or authenticated as an unprivileged user, we
     * shouldn't be able to delete this disk.
     */
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &disk_url)
            .expect_status(Some(StatusCode::UNAUTHORIZED)),
    )
    .execute()
    .await
    .expect("expected request to fail");
    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &disk_url)
            .expect_status(Some(StatusCode::FORBIDDEN)),
    )
    .authn_as(AuthnMode::UnprivilegedUser)
    .execute()
    .await
    .expect("expected request to fail");
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    /* It should no longer be present in our list of disks. */
    assert_eq!(disks_list(&client, &url_disks).await.len(), 0);
    /* We shouldn't find it if we request it explicitly. */
    let error = client
        .make_request_error(Method::GET, &disk_url, StatusCode::NOT_FOUND)
        .await;
    assert_eq!(error.message, "not found: disk with name \"just-rainsticks\"");
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
