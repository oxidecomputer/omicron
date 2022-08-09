// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests basic snapshot support in the API

use chrono::Utc;
use dropshot::test_util::ClientTestContext;
use http::method::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_ip_pool;
use nexus_test_utils::resource_helpers::create_organization;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::create_silo;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::Name;
use omicron_nexus::authz;
use omicron_nexus::context::OpContext;
use omicron_nexus::db;
use omicron_nexus::db::identity::Resource;
use omicron_nexus::db::lookup::LookupPath;
use omicron_nexus::external_api::params;
use omicron_nexus::external_api::shared;
use omicron_nexus::external_api::views;
use uuid::Uuid;

use httptest::{matchers::*, responders::*, Expectation, ServerBuilder};

const ORG_NAME: &str = "test-org";
const PROJECT_NAME: &str = "springfield-squidport-disks";

fn get_project_url() -> String {
    format!("/organizations/{}/projects/{}", ORG_NAME, PROJECT_NAME)
}

fn get_disks_url() -> String {
    format!("{}/disks", get_project_url())
}

async fn create_org_and_project(client: &ClientTestContext) -> Uuid {
    create_organization(&client, ORG_NAME).await;
    let project = create_project(client, ORG_NAME, PROJECT_NAME).await;
    project.identity.id
}

#[nexus_test]
async fn test_snapshot(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_ip_pool(&client, "p0", None, None).await;
    create_org_and_project(client).await;
    let disks_url = get_disks_url();

    // Define a global image
    let server = ServerBuilder::new().run().unwrap();
    server.expect(
        Expectation::matching(request::method_path("HEAD", "/image.raw"))
            .times(1..)
            .respond_with(
                status_code(200).append_header(
                    "Content-Length",
                    format!("{}", 4096 * 1000),
                ),
            ),
    );

    let image_create_params = params::GlobalImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        source: params::ImageSource::Url {
            url: server.url("/image.raw").to_string(),
        },
        distribution: params::Distribution {
            name: "alpine".parse().unwrap(),
            version: "edge".into(),
        },
        block_size: params::BlockSize::try_from(512).unwrap(),
    };

    let global_image: views::GlobalImage =
        NexusRequest::objects_post(client, "/images", &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    // Create a disk from this image
    let disk_size = ByteCount::try_from(2u64 * 1024 * 1024 * 1024).unwrap();
    let base_disk_name: Name = "base-disk".parse().unwrap();
    let base_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: base_disk_name.clone(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::GlobalImage {
            image_id: global_image.identity.id,
        },
        size: disk_size,
    };

    let base_disk: Disk = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&base_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Boot instance with disk
    let instances_url = format!(
        "/organizations/{}/projects/{}/instances",
        ORG_NAME, PROJECT_NAME,
    );
    let instance_name = "base-instance";

    let _instance: Instance = object_create(
        client,
        &instances_url,
        &params::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: instance_name.parse().unwrap(),
                description: format!("instance {:?}", instance_name),
            },
            ncpus: InstanceCpuCount(2),
            memory: ByteCount::from_gibibytes_u32(1),
            hostname: String::from("base_instance"),
            user_data:
                b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                    .to_vec(),
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::None,
            disks: vec![params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach { name: base_disk_name.clone() },
            )],
            external_ips: vec![],
        },
    )
    .await;

    // Issue snapshot request
    let snapshots_url = format!(
        "/organizations/{}/projects/{}/snapshots",
        ORG_NAME, PROJECT_NAME
    );

    let snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: instance_name.parse().unwrap(),
                description: format!("instance {:?}", instance_name),
            },
            disk: base_disk_name,
        },
    )
    .await;

    assert_eq!(snapshot.disk_id, base_disk.identity.id);
    assert_eq!(snapshot.size, base_disk.size);
}

#[nexus_test]
async fn test_snapshot_without_instance(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_ip_pool(&client, "p0", None, None).await;
    create_org_and_project(client).await;
    let disks_url = get_disks_url();

    // Define a global image
    let server = ServerBuilder::new().run().unwrap();
    server.expect(
        Expectation::matching(request::method_path("HEAD", "/image.raw"))
            .times(1..)
            .respond_with(
                status_code(200).append_header(
                    "Content-Length",
                    format!("{}", 4096 * 1000),
                ),
            ),
    );

    let image_create_params = params::GlobalImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        source: params::ImageSource::Url {
            url: server.url("/image.raw").to_string(),
        },
        distribution: params::Distribution {
            name: "alpine".parse().unwrap(),
            version: "edge".into(),
        },
        block_size: params::BlockSize::try_from(512).unwrap(),
    };

    let global_image: views::GlobalImage =
        NexusRequest::objects_post(client, "/images", &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();

    // Create a disk from this image
    let disk_size = ByteCount::try_from(2u64 * 1024 * 1024 * 1024).unwrap();
    let base_disk_name: Name = "base-disk".parse().unwrap();
    let base_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: base_disk_name.clone(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::GlobalImage {
            image_id: global_image.identity.id,
        },
        size: disk_size,
    };

    let base_disk: Disk = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&base_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Issue snapshot request
    let snapshots_url = format!(
        "/organizations/{}/projects/{}/snapshots",
        ORG_NAME, PROJECT_NAME
    );

    let snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "not-attached".parse().unwrap(),
                description: "not attached to instance".into(),
            },
            disk: base_disk_name,
        },
    )
    .await;

    assert_eq!(snapshot.disk_id, base_disk.identity.id);
    assert_eq!(snapshot.size, base_disk.size);
}

#[nexus_test]
async fn test_delete_snapshot(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_ip_pool(&client, "p0", None, None).await;
    create_org_and_project(client).await;
    let disks_url = get_disks_url();

    // Create a blank disk
    let disk_size = ByteCount::try_from(2u64 * 1024 * 1024 * 1024).unwrap();
    let base_disk_name: Name = "base-disk".parse().unwrap();
    let base_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: base_disk_name.clone(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    let base_disk: Disk = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&base_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Issue snapshot request
    let snapshots_url = format!(
        "/organizations/{}/projects/{}/snapshots",
        ORG_NAME, PROJECT_NAME
    );

    let snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "not-attached".parse().unwrap(),
                description: "not attached to instance".into(),
            },
            disk: base_disk_name,
        },
    )
    .await;

    assert_eq!(snapshot.disk_id, base_disk.identity.id);
    assert_eq!(snapshot.size, base_disk.size);

    // Create a disk from this snapshot
    let disk_size = ByteCount::try_from(2u64 * 1024 * 1024 * 1024).unwrap();
    let snap_disk_name: Name = "snap-disk".parse().unwrap();
    let snap_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: snap_disk_name.clone(),
            description: String::from("snapshot of 'sells rainsticks'"),
        },
        disk_source: params::DiskSource::Blank {
            block_size: params::BlockSize::try_from(512).unwrap(),
        },
        size: disk_size,
    };

    let _snap_disk: Disk = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&snap_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Delete snapshot
    let snapshot_url = format!(
        "/organizations/{}/projects/{}/snapshots/not-attached",
        ORG_NAME, PROJECT_NAME,
    );

    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &snapshot_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}

// Test that the code that Saga nodes call is idempotent

#[nexus_test]
async fn test_create_snapshot_record_idempotent(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx.nexus;
    let datastore = nexus.datastore();

    const SILO_NAME: &str = "snapshot-silo";
    let silo =
        create_silo(&client, SILO_NAME, true, shared::UserProvisionType::Fixed)
            .await;

    let project_id = create_org_and_project(&client).await;

    let snapshot = db::model::Snapshot {
        identity: db::model::SnapshotIdentity {
            id: Uuid::new_v4(),
            name: external::Name::try_from("snapshot".to_string())
                .unwrap()
                .into(),
            description: "snapshot".into(),

            time_created: Utc::now(),
            time_modified: Utc::now(),
            time_deleted: None,
        },

        project_id,
        disk_id: Uuid::new_v4(),
        volume_id: Uuid::new_v4(),

        gen: db::model::Generation::new(),
        state: db::model::SnapshotState::Creating,
        block_size: db::model::BlockSize::Traditional,
        size: external::ByteCount::try_from(1024u32).unwrap().into(),
    };

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let (authz_silo, ..) = LookupPath::new(&opctx, &datastore)
        .silo_id(silo.identity.id)
        .fetch()
        .await
        .unwrap();

    // Test project_ensure_snapshot is idempotent

    let snapshot_created_1 = datastore
        .project_ensure_snapshot(&opctx, &authz_silo, snapshot.clone())
        .await
        .unwrap();

    let snapshot_created_2 = datastore
        .project_ensure_snapshot(&opctx, &authz_silo, snapshot)
        .await
        .unwrap();

    assert_eq!(snapshot_created_1.id(), snapshot_created_2.id());

    // Test project_delete_snapshot is idempotent

    let (.., authz_snapshot, db_snapshot) = LookupPath::new(&opctx, &datastore)
        .snapshot_id(snapshot_created_1.id())
        .fetch_for(authz::Action::Delete)
        .await
        .unwrap();

    datastore
        .project_delete_snapshot(&opctx, &authz_snapshot, &db_snapshot)
        .await
        .unwrap();

    datastore
        .project_delete_snapshot(&opctx, &authz_snapshot, &db_snapshot)
        .await
        .unwrap();
}
