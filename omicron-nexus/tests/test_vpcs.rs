use omicron_common::api::IdentityMetadataCreateParams;
use omicron_common::api::Name;
use omicron_common::api::ProjectCreateParams;
use omicron_common::api::ProjectView;
use omicron_common::api::VPCView;
use std::convert::TryFrom;

use dropshot::test_util::objects_list_page;
use dropshot::test_util::objects_post;
use dropshot::test_util::ClientTestContext;

pub mod common;
use common::test_setup;

#[macro_use]
extern crate slog;

#[tokio::test]
async fn test_vpcs() {
    let cptestctx = test_setup("test_vpcs").await;
    let client = &cptestctx.external_client;
    // let apictx = &cptestctx.server.apictx;
    // let nexus = &apictx.nexus;

    /* Create a project that we'll use for testing. */
    let project_name = "springfield-squidport";
    let vpcs_url = format!("/projects/{}/vpcs", project_name);
    let _: ProjectView = objects_post(
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

    /* List vpcs.  There aren't any yet. */
    let vpcs = vpcs_list(&client, &vpcs_url).await;
    assert_eq!(vpcs.len(), 0);
}

async fn vpcs_list(client: &ClientTestContext, vpcs_url: &str) -> Vec<VPCView> {
    objects_list_page::<VPCView>(client, vpcs_url).await.items
}
