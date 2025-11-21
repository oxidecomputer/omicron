// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests local storage support

use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use http::method::Method;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{params, views};
use omicron_common::api::external;
use omicron_nexus::app::MAX_DISK_SIZE_BYTES;
use omicron_nexus::app::MIN_DISK_SIZE_BYTES;
use std::convert::TryFrom;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

type DiskTest<'a> =
    nexus_test_utils::resource_helpers::DiskTest<'a, omicron_nexus::Server>;

static PROJECT_NAME: &str = "springfield-squidport";

fn get_project_selector() -> String {
    format!("project={PROJECT_NAME}")
}

fn get_disks_url() -> String {
    format!("/v1/disks?{}", get_project_selector())
}

pub async fn create_project_and_pool(
    client: &ClientTestContext,
) -> views::Project {
    create_default_ip_pool(client).await;
    create_project(client, PROJECT_NAME).await
}

// Test the various ways Nexus can reject a local storage disk based on sizes
#[nexus_test]
async fn test_reject_creating_local_storage_disk(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create some disks
    DiskTest::new(&cptestctx).await;

    create_project_and_pool(&client).await;

    let disks_url = get_disks_url();

    // Reject where block size doesn't evenly divide total size (note that all
    // local storage disks have a block size of 4096)
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&params::DiskCreate::LocalStorage {
                identity: external::IdentityMetadataCreateParams {
                    name: "bad-disk".parse().unwrap(),
                    description: String::from("bad disk"),
                },

                size: external::ByteCount::try_from(
                    2 * MIN_DISK_SIZE_BYTES + 512,
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

    // Reject disks where the MIN_DISK_SIZE_BYTES doesn't evenly divide
    // the size
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&params::DiskCreate::LocalStorage {
                identity: external::IdentityMetadataCreateParams {
                    name: "bad-disk".parse().unwrap(),
                    description: String::from("bad disk"),
                },

                size: external::ByteCount::try_from(
                    2 * MIN_DISK_SIZE_BYTES + 4096,
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

// Test creating a local storage disk larger than MAX_DISK_SIZE_BYTES
#[nexus_test]
async fn test_create_large_local_storage_disk(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create some giant disks

    let mut disk_test = DiskTest::new(&cptestctx).await;

    for sled_agent in cptestctx.all_sled_agents() {
        disk_test
            .add_sized_zpool_with_datasets(
                sled_agent.sled_agent.id,
                7 * 1024, // 7 TiB
            )
            .await;
    }

    disk_test.propagate_datasets_to_sleds().await;

    create_project_and_pool(&client).await;

    let disks_url = get_disks_url();

    // A 5 TiB disk!
    let large_disk_size = external::ByteCount::from_gibibytes_u32(5 * 1024);

    assert!(large_disk_size.to_bytes() > MAX_DISK_SIZE_BYTES);

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disks_url)
            .body(Some(&params::DiskCreate::LocalStorage {
                identity: external::IdentityMetadataCreateParams {
                    name: "chonk-disk".parse().unwrap(),
                    description: String::from("chonk"),
                },

                size: large_disk_size,
            }))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body::<external::Disk>()
    .unwrap();
}
