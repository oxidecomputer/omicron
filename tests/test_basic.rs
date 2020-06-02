/*!
 * Smoke tests against the API server.
 *
 * This file defines a very basic set of tests against the API.
 * TODO-coverage add test for racks, servers
 */

use http::method::Method;
use http::StatusCode;
use oxide_api_prototype::api_model::ApiIdentityMetadataCreateParams;
use oxide_api_prototype::api_model::ApiIdentityMetadataUpdateParams;
use oxide_api_prototype::api_model::ApiName;
use oxide_api_prototype::api_model::ApiProjectCreateParams;
use oxide_api_prototype::api_model::ApiProjectUpdateParams;
use oxide_api_prototype::api_model::ApiProjectView;
use std::convert::TryFrom;
use uuid::Uuid;

use dropshot::test_util::object_get;
use dropshot::test_util::objects_list;
use dropshot::test_util::objects_post;
use dropshot::test_util::read_json;
use dropshot::test_util::ClientTestContext;

pub mod common;
use common::test_setup;

#[macro_use]
extern crate slog;

#[tokio::test]
async fn test_basic_failures() {
    let testctx = test_setup("basic_failures").await;
    let client = &testctx.external_client;

    /* Error case: GET /nonexistent (a path with no route at all) */
    let error = client
        .make_request(
            Method::GET,
            "/nonexistent",
            None as Option<()>,
            StatusCode::NOT_FOUND,
        )
        .await
        .expect_err("expected error");
    assert_eq!("Not Found", error.message);

    /*
     * Error case: GET /projects/nonexistent (a possible value that does not
     * exist inside a collection that does exist)
     */
    let error = client
        .make_request(
            Method::GET,
            "/projects/nonexistent",
            None as Option<()>,
            StatusCode::NOT_FOUND,
        )
        .await
        .expect_err("expected error");
    assert_eq!("not found: project with name \"nonexistent\"", error.message);

    /*
     * Error case: GET /projects/-invalid-name
     * TODO-correctness is 400 the right error code here or is 404 more
     * appropriate?
     */
    let error = client
        .make_request(
            Method::GET,
            "/projects/-invalid-name",
            None as Option<()>,
            StatusCode::BAD_REQUEST,
        )
        .await
        .expect_err("expected error");
    assert_eq!(
        "bad parameter in URL path: name must begin with an ASCII lowercase \
         character",
        error.message
    );

    /* Error case: PUT /projects */
    let error = client
        .make_request(
            Method::PUT,
            "/projects",
            None as Option<()>,
            StatusCode::METHOD_NOT_ALLOWED,
        )
        .await
        .expect_err("expected error");
    assert_eq!("Method Not Allowed", error.message);

    /* Error case: DELETE /projects */
    let error = client
        .make_request(
            Method::DELETE,
            "/projects",
            None as Option<()>,
            StatusCode::METHOD_NOT_ALLOWED,
        )
        .await
        .expect_err("expected error");
    assert_eq!("Method Not Allowed", error.message);

    /* Error case: list instances in a nonexistent project. */
    let error = client
        .make_request_with_body(
            Method::GET,
            "/projects/nonexistent/instances",
            "".into(),
            StatusCode::NOT_FOUND,
        )
        .await
        .expect_err("expected error");
    assert_eq!("not found: project with name \"nonexistent\"", error.message);

    /* Error case: fetch an instance in a nonexistent project. */
    let error = client
        .make_request_with_body(
            Method::GET,
            "/projects/nonexistent/instances/my-instance",
            "".into(),
            StatusCode::NOT_FOUND,
        )
        .await
        .expect_err("expected error");
    assert_eq!("not found: project with name \"nonexistent\"", error.message);

    /* Error case: fetch an instance with an invalid name. */
    let error = client
        .make_request_with_body(
            Method::GET,
            "/projects/nonexistent/instances/my_instance",
            "".into(),
            StatusCode::BAD_REQUEST,
        )
        .await
        .expect_err("expected error");
    assert_eq!(
        "bad parameter in URL path: name contains invalid character: \"_\" \
         (allowed characters are lowercase ASCII, digits, and \"-\")",
        error.message
    );

    /* Error case: delete an instance with an invalid name. */
    let error = client
        .make_request_with_body(
            Method::DELETE,
            "/projects/nonexistent/instances/my_instance",
            "".into(),
            StatusCode::BAD_REQUEST,
        )
        .await
        .expect_err("expected error");
    assert_eq!(
        "bad parameter in URL path: name contains invalid character: \"_\" \
         (allowed characters are lowercase ASCII, digits, and \"-\")",
        error.message
    );
}

#[tokio::test]
async fn test_projects() {
    let testctx = test_setup("test_projects").await;
    let client = &testctx.external_client;

    /*
     * Verify that there are no projects to begin with.
     */
    let projects_url = "/projects";
    let projects = projects_list(&client, &projects_url).await;
    assert_eq!(0, projects.len());

    /*
     * Create three projects used by the rest of this test.
     */
    let projects_to_create = vec!["simproject1", "simproject2", "simproject3"];
    let new_project_ids = {
        let mut project_ids: Vec<Uuid> = Vec::new();
        for project_name in projects_to_create {
            let project_create = ApiProjectCreateParams {
                identity: ApiIdentityMetadataCreateParams {
                    name: ApiName::try_from(project_name.to_string()).unwrap(),
                    description: String::from("<auto-generated by test suite>"),
                },
            };
            let new_project: ApiProjectView =
                objects_post(&client, &projects_url, project_create).await;
            assert_eq!(String::from(new_project.identity.name), project_name);
            assert_eq!(
                new_project.identity.description,
                String::from("<auto-generated by test suite>")
            );
            project_ids.push(new_project.identity.id);
        }

        project_ids
    };

    /*
     * Error case: GET /projects/simproject1/nonexistent (a path that does not
     * exist beneath a resource that does exist)
     */
    let error = client
        .make_request_error(
            Method::GET,
            "/projects/simproject1/nonexistent",
            StatusCode::NOT_FOUND,
        )
        .await;
    assert_eq!("Not Found", error.message);

    /*
     * Basic GET /projects now that we've created a few.
     * TODO-coverage: pagination
     * TODO-coverage: marker even without pagination
     */
    let initial_projects = projects_list(&client, &projects_url).await;
    assert_eq!(initial_projects.len(), 3);
    assert_eq!(initial_projects[0].identity.id, new_project_ids[0]);
    assert_eq!(initial_projects[0].identity.name, "simproject1");
    assert!(initial_projects[0].identity.description.len() > 0);
    assert_eq!(initial_projects[1].identity.id, new_project_ids[1]);
    assert_eq!(initial_projects[1].identity.name, "simproject2");
    assert!(initial_projects[1].identity.description.len() > 0);
    assert_eq!(initial_projects[2].identity.id, new_project_ids[2]);
    assert_eq!(initial_projects[2].identity.name, "simproject3");
    assert!(initial_projects[2].identity.description.len() > 0);

    /*
     * Basic test of out-of-the-box GET /projects/simproject2
     */
    let project = project_get(&client, "/projects/simproject2").await;
    let expected = &initial_projects[1];
    assert_eq!(project.identity.id, expected.identity.id);
    assert_eq!(project.identity.name, expected.identity.name);
    assert_eq!(project.identity.description, expected.identity.description);
    assert!(project.identity.description.len() > 0);

    /*
     * Delete "simproject2".  We'll make sure that's reflected in the other
     * requests.
     */
    client
        .make_request_no_body(
            Method::DELETE,
            "/projects/simproject2",
            StatusCode::NO_CONTENT,
        )
        .await
        .expect("expected success");

    /*
     * Having deleted "simproject2", verify "GET", "PUT", and "DELETE" on
     * "/projects/simproject2".
     */
    client
        .make_request_error(
            Method::GET,
            "/projects/simproject2",
            StatusCode::NOT_FOUND,
        )
        .await;
    client
        .make_request_error(
            Method::DELETE,
            "/projects/simproject2",
            StatusCode::NOT_FOUND,
        )
        .await;
    client
        .make_request_error_body(
            Method::PUT,
            "/projects/simproject2",
            ApiProjectUpdateParams {
                identity: ApiIdentityMetadataUpdateParams {
                    name: None,
                    description: None,
                },
            },
            StatusCode::NOT_FOUND,
        )
        .await;

    /*
     * Similarly, verify "GET /projects"
     */
    let expected_projects: Vec<&ApiProjectView> = initial_projects
        .iter()
        .filter(|p| p.identity.name != "simproject2")
        .collect();
    let new_projects = projects_list(&client, "/projects").await;
    assert_eq!(new_projects.len(), expected_projects.len());
    assert_eq!(new_projects[0].identity.id, expected_projects[0].identity.id);
    assert_eq!(
        new_projects[0].identity.name,
        expected_projects[0].identity.name
    );
    assert_eq!(
        new_projects[0].identity.description,
        expected_projects[0].identity.description
    );
    assert_eq!(new_projects[1].identity.id, expected_projects[1].identity.id);
    assert_eq!(
        new_projects[1].identity.name,
        expected_projects[1].identity.name
    );
    assert_eq!(
        new_projects[1].identity.description,
        expected_projects[1].identity.description
    );

    /*
     * Update "simproject3".  We'll make sure that's reflected in the other
     * requests.
     */
    let project_update = ApiProjectUpdateParams {
        identity: ApiIdentityMetadataUpdateParams {
            name: None,
            description: Some("Li'l lightnin'".to_string()),
        },
    };
    let mut response = client
        .make_request(
            Method::PUT,
            "/projects/simproject3",
            Some(project_update),
            StatusCode::OK,
        )
        .await
        .expect("expected success");
    let project: ApiProjectView = read_json(&mut response).await;
    assert_eq!(project.identity.id, new_project_ids[2]);
    assert_eq!(project.identity.name, "simproject3");
    assert_eq!(project.identity.description, "Li'l lightnin'");

    let expected = project;
    let project = project_get(&client, "/projects/simproject3").await;
    assert_eq!(project.identity.name, expected.identity.name);
    assert_eq!(project.identity.description, expected.identity.description);
    assert_eq!(project.identity.description, "Li'l lightnin'");

    /*
     * Update "simproject3" in a way that changes its name.  This is a deeper
     * operation under the hood.  This case also exercises changes to multiple
     * fields in one request.
     */
    let project_update = ApiProjectUpdateParams {
        identity: ApiIdentityMetadataUpdateParams {
            name: Some(ApiName::try_from("lil-lightnin").unwrap()),
            description: Some("little lightning".to_string()),
        },
    };
    let mut response = client
        .make_request(
            Method::PUT,
            "/projects/simproject3",
            Some(project_update),
            StatusCode::OK,
        )
        .await
        .expect("failed to make request to server");
    let project: ApiProjectView = read_json(&mut response).await;
    assert_eq!(project.identity.id, new_project_ids[2]);
    assert_eq!(project.identity.name, "lil-lightnin");
    assert_eq!(project.identity.description, "little lightning");

    client
        .make_request_error(
            Method::GET,
            "/projects/simproject3",
            StatusCode::NOT_FOUND,
        )
        .await;

    /*
     * Try to create a project with a name that conflicts with an existing one.
     */
    let project_create = ApiProjectCreateParams {
        identity: ApiIdentityMetadataCreateParams {
            name: ApiName::try_from("simproject1".to_string()).unwrap(),
            description: "a duplicate of simproject1".to_string(),
        },
    };
    let error = client
        .make_request_error_body(
            Method::POST,
            "/projects",
            project_create,
            StatusCode::BAD_REQUEST,
        )
        .await;
    assert_eq!("already exists: project \"simproject1\"", error.message);

    /*
     * Try to create a project with an unsupported name.
     * TODO-polish why doesn't serde include the field name in this error?
     */
    let error = client
        .make_request_with_body(
            Method::POST,
            "/projects",
            "{\"name\": \"sim_project\", \"description\": \"underscore\"}"
                .into(),
            StatusCode::BAD_REQUEST,
        )
        .await
        .expect_err("expected failure");
    assert!(error.message.starts_with(
        "unable to parse body: name contains invalid character: \"_\" \
         (allowed characters are lowercase ASCII, digits, and \"-\""
    ));

    /*
     * Now, really do create another project.
     */
    let project_create = ApiProjectCreateParams {
        identity: ApiIdentityMetadataCreateParams {
            name: ApiName::try_from("honor-roller").unwrap(),
            description: "a soapbox racer".to_string(),
        },
    };
    let project: ApiProjectView =
        objects_post(&client, "/projects", project_create).await;
    assert_eq!(project.identity.name, "honor-roller");
    assert_eq!(project.identity.description, "a soapbox racer");

    /*
     * List projects again and verify all of our changes.  We should have:
     *
     * - "honor-roller" with description "a soapbox racer"
     * - "lil-lightnin" with description "little lightning"
     * - "simproject1", same as when it was created.
     */
    let projects = projects_list(&client, &projects_url).await;
    assert_eq!(projects.len(), 3);
    assert_eq!(projects[0].identity.name, "honor-roller");
    assert_eq!(projects[0].identity.description, "a soapbox racer");
    assert_eq!(projects[1].identity.name, "lil-lightnin");
    assert_eq!(projects[1].identity.description, "little lightning");
    assert_eq!(projects[2].identity.name, "simproject1");
    assert!(projects[2].identity.description.len() > 0);

    testctx.teardown().await;
}

async fn projects_list(
    client: &ClientTestContext,
    projects_url: &str,
) -> Vec<ApiProjectView> {
    objects_list::<ApiProjectView>(client, projects_url).await
}

async fn project_get(
    client: &ClientTestContext,
    project_url: &str,
) -> ApiProjectView {
    object_get::<ApiProjectView>(client, project_url).await
}
