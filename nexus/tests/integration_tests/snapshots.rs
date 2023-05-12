// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests basic snapshot support in the API

use crate::integration_tests::instances::instance_simulate;
use chrono::Utc;
use dropshot::test_util::ClientTestContext;
use http::method::Method;
use http::StatusCode;
use nexus_db_queries::context::OpContext;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::populate_ip_pool;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::Name;
use omicron_nexus::authz;
use omicron_nexus::db;
use omicron_nexus::db::identity::Resource;
use omicron_nexus::db::lookup::LookupPath;
use omicron_nexus::external_api::params;
use omicron_nexus::external_api::views;
use uuid::Uuid;

use httptest::{matchers::*, responders::*, Expectation, ServerBuilder};

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const PROJECT_NAME: &str = "springfield-squidport-disks";

fn get_disks_url() -> String {
    format!("/v1/disks?project={}", PROJECT_NAME)
}

fn get_disk_url(name: &str) -> String {
    format!("/v1/disks/{}?project={}", name, PROJECT_NAME)
}

async fn create_org_and_project(client: &ClientTestContext) -> Uuid {
    let project = create_project(client, PROJECT_NAME).await;
    project.identity.id
}

#[nexus_test]
async fn test_snapshot_basic(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    populate_ip_pool(&client, "default", None).await;
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

    let image_create_params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        source: params::ImageSource::Url {
            url: server.url("/image.raw").to_string(),
        },
        os: "alpine".to_string(),
        version: "edge".to_string(),
        block_size: params::BlockSize::try_from(512).unwrap(),
    };

    let images_url = format!("/v1/images?project={}", PROJECT_NAME);
    let image =
        NexusRequest::objects_post(client, &images_url, &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<views::Image>()
            .await;

    // Create a disk from this image
    let disk_size = ByteCount::from_gibibytes_u32(2);
    let base_disk_name: Name = "base-disk".parse().unwrap();
    let base_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: base_disk_name.clone(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Image { image_id: image.identity.id },
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
    let instances_url = format!("/v1/instances?project={}", PROJECT_NAME,);
    let instance_name = "base-instance";

    let instance: Instance = object_create(
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
            start: true,
        },
    )
    .await;

    // cannot snapshot attached disk for instance in state starting
    let nexus = &cptestctx.server.apictx().nexus;
    instance_simulate(nexus, &instance.identity.id).await;

    // Issue snapshot request
    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);

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
    populate_ip_pool(&client, "default", None).await;
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

    let image_create_params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        source: params::ImageSource::Url {
            url: server.url("/image.raw").to_string(),
        },
        os: "alpine".to_string(),
        version: "edge".to_string(),
        block_size: params::BlockSize::try_from(512).unwrap(),
    };

    let images_url = format!("/v1/images?project={}", PROJECT_NAME);
    let image =
        NexusRequest::objects_post(client, &images_url, &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<views::Image>()
            .await;

    // Create a disk from this image
    let disk_size = ByteCount::from_gibibytes_u32(2);
    let base_disk_name: Name = "base-disk".parse().unwrap();
    let base_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: base_disk_name.clone(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Image { image_id: image.identity.id },
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

    // Assert disk is detached
    let disk_url =
        format!("/v1/disks/{}?project={}", base_disk_name, PROJECT_NAME);
    let disk: Disk = NexusRequest::object_get(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk")
        .parsed_body()
        .unwrap();

    assert_eq!(disk.state, DiskState::Detached);

    // Issue snapshot request
    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);

    let snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "not-attached".parse().unwrap(),
                description: "not attached to instance".into(),
            },
            disk: base_disk_name.clone(),
        },
    )
    .await;

    assert_eq!(snapshot.disk_id, base_disk.identity.id);
    assert_eq!(snapshot.size, base_disk.size);

    // Assert disk is still detached
    let disk_url =
        format!("/v1/disks/{}?project={}", base_disk_name, PROJECT_NAME);
    let disk: Disk = NexusRequest::object_get(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk")
        .parsed_body()
        .unwrap();

    assert_eq!(disk.state, DiskState::Detached);
}

#[nexus_test]
async fn test_delete_snapshot(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();
    DiskTest::new(&cptestctx).await;
    populate_ip_pool(&client, "default", None).await;
    let project_id = create_org_and_project(client).await;
    let disks_url = get_disks_url();

    // Create a blank disk
    let disk_size = ByteCount::from_gibibytes_u32(2);
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

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let provision = datastore
        .virtual_provisioning_collection_get(&opctx, project_id)
        .await
        .unwrap();
    assert_eq!(provision.virtual_disk_bytes_provisioned.0, disk_size);

    // Issue snapshot request
    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);

    let snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "not-attached".parse().unwrap(),
                description: "not attached to instance".into(),
            },
            disk: base_disk_name.clone(),
        },
    )
    .await;

    assert_eq!(snapshot.disk_id, base_disk.identity.id);
    assert_eq!(snapshot.size, base_disk.size);
    let provision = datastore
        .virtual_provisioning_collection_get(&opctx, project_id)
        .await
        .unwrap();
    assert_eq!(
        provision.virtual_disk_bytes_provisioned.to_bytes(),
        2 * disk_size.to_bytes()
    );

    // Create a disk from this snapshot
    let disk_size = ByteCount::from_gibibytes_u32(2);
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

    let provision = datastore
        .virtual_provisioning_collection_get(&opctx, project_id)
        .await
        .unwrap();
    assert_eq!(
        provision.virtual_disk_bytes_provisioned.to_bytes(),
        3 * disk_size.to_bytes()
    );

    // Delete snapshot
    let snapshot_url =
        format!("/v1/snapshots/not-attached?project={}", PROJECT_NAME);

    NexusRequest::new(
        RequestBuilder::new(client, Method::DELETE, &snapshot_url)
            .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    let provision = datastore
        .virtual_provisioning_collection_get(&opctx, project_id)
        .await
        .unwrap();
    assert_eq!(
        provision.virtual_disk_bytes_provisioned.to_bytes(),
        2 * disk_size.to_bytes()
    );

    // Delete the disk using the snapshot
    let disk_url =
        format!("/v1/disks/{}?project={}", snap_disk_name, PROJECT_NAME);
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");
    let provision = datastore
        .virtual_provisioning_collection_get(&opctx, project_id)
        .await
        .unwrap();
    assert_eq!(provision.virtual_disk_bytes_provisioned.0, disk_size);

    // Delete the original base disk
    let disk_url =
        format!("/v1/disks/{}?project={}", base_disk_name, PROJECT_NAME);
    NexusRequest::object_delete(client, &disk_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");
    let provision = datastore
        .virtual_provisioning_collection_get(&opctx, project_id)
        .await
        .unwrap();
    assert_eq!(provision.virtual_disk_bytes_provisioned.to_bytes(), 0);
}

// Test the various ways Nexus can reject a disk created from a snapshot
#[nexus_test]
async fn test_reject_creating_disk_from_snapshot(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();

    let project_id = create_org_and_project(&client).await;

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let (.., authz_project) = LookupPath::new(&opctx, &datastore)
        .project_id(project_id)
        .lookup_for(authz::Action::CreateChild)
        .await
        .unwrap();

    let snapshot = datastore
        .project_ensure_snapshot(
            &opctx,
            &authz_project,
            db::model::Snapshot {
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
                destination_volume_id: Uuid::new_v4(),

                gen: db::model::Generation::new(),
                state: db::model::SnapshotState::Creating,
                block_size: db::model::BlockSize::AdvancedFormat,

                size: external::ByteCount::try_from(
                    2 * params::MIN_DISK_SIZE_BYTES,
                )
                .unwrap()
                .into(),
            },
        )
        .await
        .unwrap();

    let disks_url = get_disks_url();

    // Reject where block size doesn't evenly divide total size
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&params::DiskCreate {
                identity: IdentityMetadataCreateParams {
                    name: "bad-disk".parse().unwrap(),
                    description: String::from("bad disk"),
                },

                disk_source: params::DiskSource::Snapshot {
                    snapshot_id: snapshot.id(),
                },

                size: ByteCount::try_from(
                    2 * params::MIN_DISK_SIZE_BYTES
                        + db::model::BlockSize::Traditional.to_bytes(),
                )
                .unwrap(),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(
        error.message,
        "unsupported value for \"size and block_size\": total size must be a multiple of block size 4096",
    );

    // Reject where size of snapshot is greater than the disk's
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&params::DiskCreate {
                identity: IdentityMetadataCreateParams {
                    name: "bad-disk".parse().unwrap(),
                    description: String::from("bad disk"),
                },

                disk_source: params::DiskSource::Snapshot {
                    snapshot_id: snapshot.id(),
                },

                size: ByteCount::try_from(params::MIN_DISK_SIZE_BYTES).unwrap(),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(
        error.message,
        format!(
            "disk size {} must be greater than or equal to snapshot size {}",
            params::MIN_DISK_SIZE_BYTES,
            2 * params::MIN_DISK_SIZE_BYTES,
        )
    );

    // Reject disks where the MIN_DISK_SIZE_BYTES doesn't evenly divide
    // the size
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&params::DiskCreate {
                identity: IdentityMetadataCreateParams {
                    name: "bad-disk".parse().unwrap(),
                    description: String::from("bad disk"),
                },

                disk_source: params::DiskSource::Snapshot {
                    snapshot_id: snapshot.id(),
                },

                size: ByteCount::try_from(
                    2 * params::MIN_DISK_SIZE_BYTES
                        + db::model::BlockSize::AdvancedFormat.to_bytes(),
                )
                .unwrap(),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(
        error.message,
        "unsupported value for \"size\": total size must be a multiple of 1 GiB",
    );
}

#[nexus_test]
async fn test_reject_creating_disk_from_illegal_snapshot(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();

    let project_id = create_org_and_project(&client).await;

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let (.., authz_project) = LookupPath::new(&opctx, &datastore)
        .project_id(project_id)
        .lookup_for(authz::Action::CreateChild)
        .await
        .unwrap();

    let snapshot = datastore
        .project_ensure_snapshot(
            &opctx,
            &authz_project,
            db::model::Snapshot {
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
                destination_volume_id: Uuid::new_v4(),

                gen: db::model::Generation::new(),
                state: db::model::SnapshotState::Creating,
                block_size: db::model::BlockSize::AdvancedFormat,

                size: external::ByteCount::try_from(
                    db::model::BlockSize::AdvancedFormat.to_bytes(),
                )
                .unwrap()
                .into(),
            },
        )
        .await
        .unwrap();

    let disks_url = get_disks_url();

    // Reject where the size isn't at least MIN_DISK_SIZE_BYTES
    //
    // If there is a check that the disk must be larger than the snapshot, there
    // doesn't seem to be a scenario where this could happen - snapshots are
    // created from disks, and disks can't be less than MIN_DISK_SIZE_BYTES.
    // But it would be wrong to remove the check from Nexus, right? So put an
    // illegally sized snapshot in above, and make sure that we can't create
    // this anyway.
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&params::DiskCreate {
                identity: IdentityMetadataCreateParams {
                    name: "bad-disk".parse().unwrap(),
                    description: String::from("bad disk"),
                },

                disk_source: params::DiskSource::Snapshot {
                    snapshot_id: snapshot.id(),
                },

                size: ByteCount::try_from(
                    db::model::BlockSize::AdvancedFormat.to_bytes() * 2,
                )
                .unwrap(),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(
        error.message,
        "unsupported value for \"size\": total size must be at least 1 GiB",
    );
}

#[nexus_test]
async fn test_cannot_snapshot_if_no_space(cptestctx: &ControlPlaneTestContext) {
    // Test that snapshots cannot be created if there is no space for the blocks
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    populate_ip_pool(&client, "default", None).await;
    create_org_and_project(client).await;
    let disks_url = get_disks_url();

    // Create a disk at just over half the capacity of what DiskTest allocates
    let gibibytes: u64 = DiskTest::DEFAULT_ZPOOL_SIZE_GIB as u64 / 2 + 1;
    let disk_size =
        ByteCount::try_from(gibibytes * 1024 * 1024 * 1024).unwrap();
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

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&base_disk))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected error creating disk");

    // Issue snapshot request, expect it to fail
    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &snapshots_url)
            .body(Some(&params::SnapshotCreate {
                identity: IdentityMetadataCreateParams {
                    name: "not-attached".parse().unwrap(),
                    description: "not attached to instance".into(),
                },
                disk: base_disk_name,
            }))
            .expect_status(Some(StatusCode::SERVICE_UNAVAILABLE)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("unexpected success creating snapshot");
}

#[nexus_test]
async fn test_snapshot_unwind(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let disk_test = DiskTest::new(&cptestctx).await;
    populate_ip_pool(&client, "default", None).await;
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

    let image_create_params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        source: params::ImageSource::Url {
            url: server.url("/image.raw").to_string(),
        },
        os: "alpine".to_string(),
        version: "edge".to_string(),
        block_size: params::BlockSize::try_from(512).unwrap(),
    };

    let images_url = format!("/v1/images?project={}", PROJECT_NAME);
    let image =
        NexusRequest::objects_post(client, &images_url, &image_create_params)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute_and_parse_unwrap::<views::Image>()
            .await;

    // Create a disk from this image
    let disk_size = ByteCount::from_gibibytes_u32(2);
    let base_disk_name: Name = "base-disk".parse().unwrap();
    let base_disk = params::DiskCreate {
        identity: IdentityMetadataCreateParams {
            name: base_disk_name.clone(),
            description: String::from("sells rainsticks"),
        },
        disk_source: params::DiskSource::Image { image_id: image.identity.id },
        size: disk_size,
    };

    let _base_disk: Disk = NexusRequest::new(
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

    // Set the third region's running snapshot callback so it fails
    let zpool = &disk_test.zpools[2];
    let dataset = &zpool.datasets[0];
    disk_test
        .sled_agent
        .get_crucible_dataset(zpool.id, dataset.id)
        .await
        .set_creating_a_running_snapshot_should_fail()
        .await;

    // Issue snapshot request, expecting it to fail
    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);

    NexusRequest::expect_failure_with_body(
        client,
        StatusCode::INTERNAL_SERVER_ERROR,
        Method::POST,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "snapshot".parse().unwrap(),
                description: String::from("a snapshot"),
            },
            disk: base_disk_name.clone(),
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("snapshot request succeeded");

    // Delete the disk
    NexusRequest::object_delete(client, &get_disk_url(base_disk_name.as_str()))
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to delete disk");

    // Assert everything was cleaned up
    assert!(disk_test.crucible_resources_deleted().await);
}

// Test that the code that Saga nodes call is idempotent

#[nexus_test]
async fn test_create_snapshot_record_idempotent(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();

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
        destination_volume_id: Uuid::new_v4(),

        gen: db::model::Generation::new(),
        state: db::model::SnapshotState::Creating,
        block_size: db::model::BlockSize::Traditional,
        size: external::ByteCount::try_from(1024u32).unwrap().into(),
    };

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let (.., authz_project) = LookupPath::new(&opctx, &datastore)
        .project_id(project_id)
        .lookup_for(authz::Action::CreateChild)
        .await
        .unwrap();

    // Test project_ensure_snapshot is idempotent

    let snapshot_created_1 = datastore
        .project_ensure_snapshot(&opctx, &authz_project, snapshot.clone())
        .await
        .unwrap();

    let snapshot_created_2 = datastore
        .project_ensure_snapshot(&opctx, &authz_project, snapshot)
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

#[nexus_test]
async fn test_region_snapshot_create_idempotent(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.apictx().nexus;
    let datastore = nexus.datastore();

    let region_snapshot = db::model::RegionSnapshot {
        dataset_id: Uuid::new_v4(),
        region_id: Uuid::new_v4(),
        snapshot_id: Uuid::new_v4(),

        snapshot_addr: "[::]:12345".to_string(),

        volume_references: 1,
    };

    datastore.region_snapshot_create(region_snapshot.clone()).await.unwrap();

    datastore.region_snapshot_create(region_snapshot).await.unwrap();
}
