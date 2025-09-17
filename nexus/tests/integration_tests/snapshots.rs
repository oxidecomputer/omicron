// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests basic snapshot support in the API

use crate::integration_tests::instances::instance_simulate;
use chrono::Utc;
use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use http::method::Method;
use nexus_config::RegionAllocationStrategy;
use nexus_db_lookup::LookupPath;
use nexus_db_model::to_db_typed_uuid;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore::REGION_REDUNDANCY_THRESHOLD;
use nexus_db_queries::db::datastore::RegionAllocationFor;
use nexus_db_queries::db::datastore::RegionAllocationParameters;
use nexus_db_queries::db::identity::Resource;
use nexus_test_utils::SLED_AGENT_UUID;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_disk;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::views;
use omicron_common::api::external;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::Disk;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceCpuCount;
use omicron_common::api::external::Name;
use omicron_nexus::app::MIN_DISK_SIZE_BYTES;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::VolumeUuid;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;
type DiskTest<'a> =
    nexus_test_utils::resource_helpers::DiskTest<'a, omicron_nexus::Server>;
type DiskTestBuilder<'a> = nexus_test_utils::resource_helpers::DiskTestBuilder<
    'a,
    omicron_nexus::Server,
>;

const PROJECT_NAME: &str = "springfield-squidport-disks";

fn get_disks_url() -> String {
    format!("/v1/disks?project={}", PROJECT_NAME)
}

fn get_disk_url(name: &str) -> String {
    format!("/v1/disks/{}?project={}", name, PROJECT_NAME)
}

async fn create_project_and_pool(client: &ClientTestContext) -> Uuid {
    create_default_ip_pool(client).await;
    let project = create_project(client, PROJECT_NAME).await;
    project.identity.id
}

#[nexus_test]
async fn test_snapshot_basic(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;
    let disks_url = get_disks_url();

    // Define a global image
    let image_create_params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        source: params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
        os: "alpine".to_string(),
        version: "edge".to_string(),
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
            hostname: "base-instance".parse().unwrap(),
            user_data:
                b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                    .to_vec(),
            ssh_public_keys: Some(Vec::new()),
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::None,
            boot_disk: Some(params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach { name: base_disk_name.clone() },
            )),
            cpu_platform: None,
            disks: Vec::new(),
            external_ips: vec![],
            start: true,
            auto_restart_policy: Default::default(),
            anti_affinity_groups: Vec::new(),
        },
    )
    .await;
    let instance_id = InstanceUuid::from_untyped_uuid(instance.identity.id);

    // cannot snapshot attached disk for instance in state starting
    let nexus = &cptestctx.server.server_context().nexus;
    instance_simulate(nexus, &instance_id).await;

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
            disk: base_disk_name.into(),
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
    create_project_and_pool(client).await;
    let disks_url = get_disks_url();

    // Define a global image
    let image_create_params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        source: params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
        os: "alpine".to_string(),
        version: "edge".to_string(),
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
            disk: base_disk_name.clone().into(),
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
async fn test_snapshot_stopped_instance(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;
    let disks_url = get_disks_url();

    // Define a global image
    let image_create_params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        source: params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
        os: "alpine".to_string(),
        version: "edge".to_string(),
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

    // Create a stopped instance with attached disk
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
            hostname: "base-instance".parse().unwrap(),
            user_data:
                b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                    .to_vec(),
            ssh_public_keys: Some(Vec::new()),
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::None,
            boot_disk: Some(params::InstanceDiskAttachment::Attach(
                params::InstanceDiskAttach { name: base_disk_name.clone() },
            )),
            cpu_platform: None,
            disks: Vec::new(),
            external_ips: vec![],
            start: false,
            auto_restart_policy: Default::default(),
            anti_affinity_groups: Vec::new(),
        },
    )
    .await;

    assert_eq!(instance.runtime.run_state, external::InstanceState::Stopped);

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
            disk: base_disk_name.into(),
        },
    )
    .await;

    assert_eq!(snapshot.disk_id, base_disk.identity.id);
    assert_eq!(snapshot.size, base_disk.size);
}

#[nexus_test]
async fn test_delete_snapshot(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    DiskTest::new(&cptestctx).await;
    let project_id = create_project_and_pool(client).await;
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
            disk: base_disk_name.clone().into(),
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
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let project_id = create_project_and_pool(&client).await;

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let (.., authz_project) = LookupPath::new(&opctx, datastore)
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
                volume_id: VolumeUuid::new_v4().into(),
                destination_volume_id: VolumeUuid::new_v4().into(),

                gen: db::model::Generation::new(),
                state: db::model::SnapshotState::Creating,
                block_size: db::model::BlockSize::AdvancedFormat,

                size: external::ByteCount::try_from(2 * MIN_DISK_SIZE_BYTES)
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
                    2 * MIN_DISK_SIZE_BYTES
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

                size: ByteCount::try_from(MIN_DISK_SIZE_BYTES).unwrap(),
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
            MIN_DISK_SIZE_BYTES,
            2 * MIN_DISK_SIZE_BYTES,
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
                    2 * MIN_DISK_SIZE_BYTES
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
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let project_id = create_project_and_pool(&client).await;

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let (.., authz_project) = LookupPath::new(&opctx, datastore)
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
                volume_id: VolumeUuid::new_v4().into(),
                destination_volume_id: VolumeUuid::new_v4().into(),

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
async fn test_reject_creating_disk_from_other_project_snapshot(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let project_id = create_project_and_pool(&client).await;

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let (.., authz_project) = LookupPath::new(&opctx, datastore)
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
                volume_id: VolumeUuid::new_v4().into(),
                destination_volume_id: VolumeUuid::new_v4().into(),

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

    let second_project = create_project(client, "moes-tavern").await;
    let second_disks_url =
        format!("/v1/disks?project={}", second_project.identity.name);
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &second_disks_url)
            .body(Some(&params::DiskCreate {
                identity: IdentityMetadataCreateParams {
                    name: "stolen-disk".parse().unwrap(),
                    description: String::from("stolen disk"),
                },

                disk_source: params::DiskSource::Snapshot {
                    snapshot_id: snapshot.id(),
                },

                size: ByteCount::try_from(MIN_DISK_SIZE_BYTES).unwrap(),
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .unwrap();
    assert_eq!(error.message, "snapshot does not belong to this project");
}

#[nexus_test]
async fn test_cannot_snapshot_if_no_space(cptestctx: &ControlPlaneTestContext) {
    // Test that snapshots cannot be created if there is no space for the blocks
    let client = &cptestctx.external_client;
    DiskTest::new(&cptestctx).await;
    create_project_and_pool(client).await;
    let disks_url = get_disks_url();

    // Create a disk at just over half the capacity of what DiskTest allocates
    let gibibytes: u64 = u64::from(DiskTest::DEFAULT_ZPOOL_SIZE_GIB) / 2 + 1;
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
                disk: base_disk_name.into(),
            }))
            .expect_status(Some(StatusCode::INSUFFICIENT_STORAGE)),
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
    create_project_and_pool(client).await;
    let disks_url = get_disks_url();

    // Define a global image
    let image_create_params = params::ImageCreate {
        identity: IdentityMetadataCreateParams {
            name: "alpine-edge".parse().unwrap(),
            description: String::from(
                "you can boot any image, as long as it's alpine",
            ),
        },
        source: params::ImageSource::YouCanBootAnythingAsLongAsItsAlpine,
        os: "alpine".to_string(),
        version: "edge".to_string(),
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
    let zpool = disk_test.zpools().nth(2).expect("Not enough zpools");
    let dataset = zpool.crucible_dataset();
    cptestctx
        .first_sled_agent()
        .get_crucible_dataset(zpool.id, dataset.id)
        .set_creating_a_running_snapshot_should_fail();

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
            disk: base_disk_name.clone().into(),
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
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let project_id = create_project_and_pool(&client).await;
    let disk_id = Uuid::new_v4();
    let snapshot_name =
        external::Name::try_from("snapshot".to_string()).unwrap();

    let snapshot = db::model::Snapshot {
        identity: db::model::SnapshotIdentity {
            id: Uuid::new_v4(),
            name: snapshot_name.clone().into(),
            description: "snapshot".into(),

            time_created: Utc::now(),
            time_modified: Utc::now(),
            time_deleted: None,
        },

        project_id,
        disk_id,
        volume_id: VolumeUuid::new_v4().into(),
        destination_volume_id: VolumeUuid::new_v4().into(),

        gen: db::model::Generation::new(),
        state: db::model::SnapshotState::Creating,
        block_size: db::model::BlockSize::Traditional,
        size: external::ByteCount::try_from(1024u32).unwrap().into(),
    };

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let (.., authz_project) = LookupPath::new(&opctx, datastore)
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
        .project_ensure_snapshot(&opctx, &authz_project, snapshot.clone())
        .await
        .unwrap();

    assert_eq!(snapshot_created_1.id(), snapshot_created_2.id());

    // Test that attempting to ensure a snapshot with the same project + name
    // but a different ID will not work: if a user tries to fire off multiple
    // snapshot creations for the same project + name, they will get different
    // IDs, so this should be rejected, ensuring only one snapshot create saga
    // would apply for the user's multiple requests.

    let dupe_snapshot = db::model::Snapshot {
        identity: db::model::SnapshotIdentity {
            id: Uuid::new_v4(),
            name: snapshot_name.clone().into(),
            description: "snapshot".into(),

            time_created: Utc::now(),
            time_modified: Utc::now(),
            time_deleted: None,
        },

        project_id,
        disk_id,
        volume_id: VolumeUuid::new_v4().into(),
        destination_volume_id: VolumeUuid::new_v4().into(),

        gen: db::model::Generation::new(),
        state: db::model::SnapshotState::Creating,
        block_size: db::model::BlockSize::Traditional,
        size: external::ByteCount::try_from(1024u32).unwrap().into(),
    };

    let dupe_snapshot_created_err = datastore
        .project_ensure_snapshot(&opctx, &authz_project, dupe_snapshot)
        .await;

    assert!(matches!(
        dupe_snapshot_created_err.unwrap_err(),
        external::Error::ObjectAlreadyExists { .. },
    ));

    // Move snapshot from Creating to Ready

    let (.., authz_snapshot, db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot_created_1.id())
        .fetch_for(authz::Action::Modify)
        .await
        .unwrap();

    datastore
        .project_snapshot_update_state(
            &opctx,
            &authz_snapshot,
            db_snapshot.gen,
            db::model::SnapshotState::Ready,
        )
        .await
        .unwrap();

    // Grab the new snapshot (so generation number is updated)

    let (.., authz_snapshot, db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot_created_1.id())
        .fetch_for(authz::Action::Delete)
        .await
        .unwrap();

    // Test project_delete_snapshot is idempotent for the same input

    datastore
        .project_delete_snapshot(
            &opctx,
            &authz_snapshot,
            &db_snapshot,
            vec![
                // This must match what is in the snapshot_delete saga!
                db::model::SnapshotState::Ready,
                db::model::SnapshotState::Faulted,
                db::model::SnapshotState::Destroyed,
            ],
        )
        .await
        .unwrap();

    {
        // Ensure the snapshot is gone
        let r = LookupPath::new(&opctx, datastore)
            .snapshot_id(snapshot_created_1.id())
            .fetch_for(authz::Action::Read)
            .await;

        assert!(r.is_err());
    }

    datastore
        .project_delete_snapshot(
            &opctx,
            &authz_snapshot,
            &db_snapshot,
            vec![
                db::model::SnapshotState::Ready,
                db::model::SnapshotState::Faulted,
                db::model::SnapshotState::Destroyed,
            ],
        )
        .await
        .unwrap();

    // Test that a new snapshot can be added with the same name as a deleted
    // one.

    let new_snapshot = db::model::Snapshot {
        identity: db::model::SnapshotIdentity {
            id: Uuid::new_v4(),
            name: snapshot_name.into(),
            description: "snapshot".into(),

            time_created: Utc::now(),
            time_modified: Utc::now(),
            time_deleted: None,
        },

        project_id,
        disk_id,
        volume_id: VolumeUuid::new_v4().into(),
        destination_volume_id: VolumeUuid::new_v4().into(),

        gen: db::model::Generation::new(),
        state: db::model::SnapshotState::Creating,
        block_size: db::model::BlockSize::Traditional,
        size: external::ByteCount::try_from(1024u32).unwrap().into(),
    };

    let _ = datastore
        .project_ensure_snapshot(&opctx, &authz_project, new_snapshot)
        .await
        .unwrap();
}

#[nexus_test]
async fn test_region_snapshot_create_idempotent(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let region_snapshot = db::model::RegionSnapshot {
        dataset_id: to_db_typed_uuid(DatasetUuid::new_v4()),
        region_id: Uuid::new_v4(),
        snapshot_id: Uuid::new_v4(),

        snapshot_addr: "[::]:12345".to_string(),

        volume_references: 1,
        deleting: false,
    };

    datastore.region_snapshot_create(region_snapshot.clone()).await.unwrap();

    datastore.region_snapshot_create(region_snapshot).await.unwrap();
}

/// Test that multiple DELETE calls won't be sent to a Crucible agent for the
/// same read-only downstairs
#[nexus_test]
async fn test_multiple_deletes_not_sent(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    DiskTest::new(&cptestctx).await;
    let _project_id = create_project_and_pool(client).await;
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

    // Create three snapshots of this disk
    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);

    let snapshot_1: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "snapshot1".parse().unwrap(),
                description: "not attached to instance".into(),
            },
            disk: base_disk_name.clone().into(),
        },
    )
    .await;

    let snapshot_2: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "snapshot2".parse().unwrap(),
                description: "not attached to instance".into(),
            },
            disk: base_disk_name.clone().into(),
        },
    )
    .await;

    let snapshot_3: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "snapshot3".parse().unwrap(),
                description: "not attached to instance".into(),
            },
            disk: base_disk_name.clone().into(),
        },
    )
    .await;

    assert_eq!(snapshot_1.disk_id, base_disk.identity.id);
    assert_eq!(snapshot_2.disk_id, base_disk.identity.id);
    assert_eq!(snapshot_3.disk_id, base_disk.identity.id);

    // Simulate all three of these have snapshot delete sagas executing
    // concurrently. First, delete the snapshot record:

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    let (.., authz_snapshot_1, db_snapshot_1) =
        LookupPath::new(&opctx, datastore)
            .snapshot_id(snapshot_1.identity.id)
            .fetch_for(authz::Action::Delete)
            .await
            .unwrap();

    datastore
        .project_delete_snapshot(
            &opctx,
            &authz_snapshot_1,
            &db_snapshot_1,
            vec![
                // This must match what is in the snapshot_delete saga!
                db::model::SnapshotState::Ready,
                db::model::SnapshotState::Faulted,
                db::model::SnapshotState::Destroyed,
            ],
        )
        .await
        .unwrap();

    let (.., authz_snapshot_2, db_snapshot_2) =
        LookupPath::new(&opctx, datastore)
            .snapshot_id(snapshot_2.identity.id)
            .fetch_for(authz::Action::Delete)
            .await
            .unwrap();

    datastore
        .project_delete_snapshot(
            &opctx,
            &authz_snapshot_2,
            &db_snapshot_2,
            vec![
                // This must match what is in the snapshot_delete saga!
                db::model::SnapshotState::Ready,
                db::model::SnapshotState::Faulted,
                db::model::SnapshotState::Destroyed,
            ],
        )
        .await
        .unwrap();

    let (.., authz_snapshot_3, db_snapshot_3) =
        LookupPath::new(&opctx, datastore)
            .snapshot_id(snapshot_3.identity.id)
            .fetch_for(authz::Action::Delete)
            .await
            .unwrap();

    datastore
        .project_delete_snapshot(
            &opctx,
            &authz_snapshot_3,
            &db_snapshot_3,
            vec![
                // This must match what is in the snapshot_delete saga!
                db::model::SnapshotState::Ready,
                db::model::SnapshotState::Faulted,
                db::model::SnapshotState::Destroyed,
            ],
        )
        .await
        .unwrap();

    // Continue pretending that each saga is executing concurrently: call
    // `soft_delete_volume` back to back.  Make sure that each saga is deleting
    // a unique set of resources, else they will be sending identical DELETE
    // calls to Crucible agents. This is ok because the agents are idempotent,
    // but if someone issues a DELETE for a read-only downstairs (called a
    // "running snapshot") when the snapshot was deleted, they'll see a 404,
    // which will cause the saga to fail.

    let resources_1 =
        datastore.soft_delete_volume(db_snapshot_1.volume_id()).await.unwrap();

    let resources_2 =
        datastore.soft_delete_volume(db_snapshot_2.volume_id()).await.unwrap();

    let resources_3 =
        datastore.soft_delete_volume(db_snapshot_3.volume_id()).await.unwrap();

    let resources_1_datasets_and_regions =
        datastore.regions_to_delete(&resources_1).await.unwrap();
    let resources_1_datasets_and_snapshots =
        datastore.snapshots_to_delete(&resources_1).await.unwrap();

    let resources_2_datasets_and_regions =
        datastore.regions_to_delete(&resources_2).await.unwrap();
    let resources_2_datasets_and_snapshots =
        datastore.snapshots_to_delete(&resources_2).await.unwrap();

    let resources_3_datasets_and_regions =
        datastore.regions_to_delete(&resources_3).await.unwrap();
    let resources_3_datasets_and_snapshots =
        datastore.snapshots_to_delete(&resources_3).await.unwrap();

    // No region deletions yet, these are just snapshot deletes

    assert!(resources_1_datasets_and_regions.is_empty());
    assert!(resources_2_datasets_and_regions.is_empty());
    assert!(resources_3_datasets_and_regions.is_empty());

    // But there are snapshots to delete

    assert!(!resources_1_datasets_and_snapshots.is_empty());
    assert!(!resources_2_datasets_and_snapshots.is_empty());
    assert!(!resources_3_datasets_and_snapshots.is_empty());

    // Assert there are no overlaps in the snapshots_to_delete to delete.

    for tuple in &resources_1_datasets_and_snapshots {
        assert!(!resources_2_datasets_and_snapshots.contains(tuple));
        assert!(!resources_3_datasets_and_snapshots.contains(tuple));
    }

    for tuple in &resources_2_datasets_and_snapshots {
        assert!(!resources_1_datasets_and_snapshots.contains(tuple));
        assert!(!resources_3_datasets_and_snapshots.contains(tuple));
    }

    for tuple in &resources_3_datasets_and_snapshots {
        assert!(!resources_1_datasets_and_snapshots.contains(tuple));
        assert!(!resources_2_datasets_and_snapshots.contains(tuple));
    }
}

/// Ensure that allocating one replacement for a snapshot works
#[nexus_test]
async fn test_region_allocation_for_snapshot(
    cptestctx: &ControlPlaneTestContext,
) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create four zpools, each with one dataset.
    //
    // We add one more than the "three" default to avoid failing
    // with "not enough storage".
    let sled_id = cptestctx.first_sled_id();
    let mut disk_test = DiskTestBuilder::new(&cptestctx)
        .on_specific_sled(sled_id)
        .with_zpool_count(4)
        .build()
        .await;

    // Create a disk
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;

    // Assert disk has three allocated regions
    let disk_id = disk.identity.id;
    let (.., db_disk) = LookupPath::new(&opctx, datastore)
        .disk_id(disk_id)
        .fetch()
        .await
        .unwrap_or_else(|_| panic!("test disk {:?} should exist", disk_id));

    let allocated_regions =
        datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
    assert_eq!(allocated_regions.len(), REGION_REDUNDANCY_THRESHOLD);

    // Create a snapshot of the disk

    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);

    let snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "snapshot".parse().unwrap(),
                description: String::from("a snapshot"),
            },
            disk: disk_id.into(),
        },
    )
    .await;

    assert_eq!(snapshot.disk_id, disk.identity.id);
    assert_eq!(snapshot.size, disk.size);

    // There shouldn't be any regions for the snapshot volume

    let snapshot_id = snapshot.identity.id;
    let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
        .snapshot_id(snapshot_id)
        .fetch()
        .await
        .unwrap_or_else(|_| {
            panic!("test snapshot {:?} should exist", snapshot_id)
        });

    let allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    assert_eq!(allocated_regions.len(), 0);

    // Run region allocation for the snapshot volume, setting the redundancy to
    // 1 (aka one more than existing number of regions), and expect only _one_
    // region to be allocated.

    datastore
        .arbitrary_region_allocate(
            &opctx,
            RegionAllocationFor::SnapshotVolume {
                volume_id: db_snapshot.volume_id(),
                snapshot_id: snapshot.identity.id,
            },
            RegionAllocationParameters::FromDiskSource {
                disk_source: &params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(
                        disk.block_size.to_bytes() as u32,
                    )
                    .unwrap(),
                },
                size: disk.size,
            },
            &RegionAllocationStrategy::Random { seed: None },
            1,
        )
        .await
        .unwrap();

    let allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    assert_eq!(allocated_regions.len(), 1);

    // Assert that all regions are on separate datasets from the region
    // snapshots

    for (_, region) in allocated_regions {
        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();

            use async_bb8_diesel::AsyncRunQueryDsl;
            use diesel::ExpressionMethods;
            use diesel::QueryDsl;
            use diesel::SelectableHelper;
            use nexus_db_schema::schema::region_snapshot::dsl;

            let region_snapshots: Vec<db::model::RegionSnapshot> =
                dsl::region_snapshot
                    .filter(
                        dsl::dataset_id
                            .eq(to_db_typed_uuid(region.dataset_id())),
                    )
                    .filter(dsl::snapshot_id.eq(snapshot.identity.id))
                    .select(db::model::RegionSnapshot::as_select())
                    .load_async::<db::model::RegionSnapshot>(&*conn)
                    .await
                    .unwrap();

            assert!(region_snapshots.is_empty());
        }
    }

    // Ensure the function is idempotent

    datastore
        .arbitrary_region_allocate(
            &opctx,
            RegionAllocationFor::SnapshotVolume {
                volume_id: db_snapshot.volume_id(),
                snapshot_id: snapshot.identity.id,
            },
            RegionAllocationParameters::FromDiskSource {
                disk_source: &params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(
                        disk.block_size.to_bytes() as u32,
                    )
                    .unwrap(),
                },
                size: disk.size,
            },
            &RegionAllocationStrategy::Random { seed: None },
            1,
        )
        .await
        .unwrap();

    let allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    assert_eq!(allocated_regions.len(), 1);

    // If an additional region is required, make sure that works too.
    disk_test.add_zpool_with_datasets(sled_id).await;

    datastore
        .arbitrary_region_allocate(
            &opctx,
            RegionAllocationFor::SnapshotVolume {
                volume_id: db_snapshot.volume_id(),
                snapshot_id: snapshot.identity.id,
            },
            RegionAllocationParameters::FromDiskSource {
                disk_source: &params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(
                        disk.block_size.to_bytes() as u32,
                    )
                    .unwrap(),
                },
                size: disk.size,
            },
            &RegionAllocationStrategy::Random { seed: None },
            2,
        )
        .await
        .unwrap();

    let allocated_regions =
        datastore.get_allocated_regions(db_snapshot.volume_id()).await.unwrap();

    assert_eq!(allocated_regions.len(), 2);
}

#[nexus_test]
async fn test_snapshot_expunge(cptestctx: &ControlPlaneTestContext) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());

    // Create three zpools, each with one dataset.
    let _disk_test = DiskTest::new(&cptestctx).await;

    // Create a disk, then a snapshot of that disk
    let client = &cptestctx.external_client;
    let _project_id = create_project_and_pool(client).await;

    let disk = create_disk(&client, PROJECT_NAME, "disk").await;

    let snapshots_url = format!("/v1/snapshots?project={}", PROJECT_NAME);

    let snapshot: views::Snapshot = object_create(
        client,
        &snapshots_url,
        &params::SnapshotCreate {
            identity: IdentityMetadataCreateParams {
                name: "snapshot".parse().unwrap(),
                description: String::from("a snapshot"),
            },
            disk: disk.identity.name.into(),
        },
    )
    .await;

    // Expunge the sled
    cptestctx
        .lockstep_client
        .make_request(
            Method::POST,
            "/sleds/expunge",
            Some(params::SledSelector {
                sled: SLED_AGENT_UUID.parse().unwrap(),
            }),
            StatusCode::OK,
        )
        .await
        .unwrap();

    // All three region snapshots should be returned
    let expunged_region_snapshots = datastore
        .find_region_snapshots_on_expunged_physical_disks(&opctx)
        .await
        .unwrap();

    assert_eq!(expunged_region_snapshots.len(), 3);

    for expunged_region_snapshot in expunged_region_snapshots {
        assert_eq!(expunged_region_snapshot.snapshot_id, snapshot.identity.id);
    }
}
