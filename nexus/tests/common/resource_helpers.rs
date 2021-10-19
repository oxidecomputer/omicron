use dropshot::test_util::objects_post;
use dropshot::test_util::ClientTestContext;
use dropshot::HttpErrorResponseBody;
use dropshot::Method;
use http::StatusCode;
use std::convert::TryFrom;

use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::Organization;
use omicron_common::api::external::OrganizationCreateParams;
use omicron_common::api::external::Project;
use omicron_common::api::external::ProjectCreateParams;
use omicron_common::api::external::Vpc;
use omicron_common::api::external::VpcCreateParams;

pub async fn create_organization(
    client: &ClientTestContext,
    organization_name: &str,
) -> Organization {
    objects_post(
        &client,
        "/organizations",
        OrganizationCreateParams {
            identity: IdentityMetadataCreateParams {
                name: Name::try_from(organization_name).unwrap(),
                description: "an org".to_string(),
            },
        },
    )
    .await
}

pub async fn create_project(
    client: &ClientTestContext,
    organization_name: &str,
    project_name: &str,
) -> Project {
    objects_post(
        &client,
        format!("/organizations/{}/projects", &organization_name).as_str(),
        ProjectCreateParams {
            identity: IdentityMetadataCreateParams {
                name: Name::try_from(project_name).unwrap(),
                description: "a pier".to_string(),
            },
        },
    )
    .await
}

pub async fn create_vpc(
    client: &ClientTestContext,
    organization_name: &str,
    project_name: &str,
    vpc_name: &str,
) -> Vpc {
    objects_post(
        &client,
        format!(
            "/organizations/{}/projects/{}/vpcs",
            &organization_name, &project_name
        )
        .as_str(),
        VpcCreateParams {
            identity: IdentityMetadataCreateParams {
                name: Name::try_from(vpc_name).unwrap(),
                description: String::from("vpc description"),
            },
            dns_name: Name::try_from("abc").unwrap(),
        },
    )
    .await
}

// TODO: probably would be cleaner to replace these helpers with something that
// just generates the create params since that's the noisiest part
pub async fn create_vpc_with_error(
    client: &ClientTestContext,
    organization_name: &str,
    project_name: &str,
    vpc_name: &str,
    status: StatusCode,
) -> HttpErrorResponseBody {
    client
        .make_request_error_body(
            Method::POST,
            format!(
                "/organizations/{}/projects/{}/vpcs",
                &organization_name, &project_name
            )
            .as_str(),
            VpcCreateParams {
                identity: IdentityMetadataCreateParams {
                    name: Name::try_from(vpc_name).unwrap(),
                    description: String::from("vpc description"),
                },
                dns_name: Name::try_from("abc").unwrap(),
            },
            status,
        )
        .await
}
