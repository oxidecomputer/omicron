// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::test_util::ClientTestContext;
use http::{Method, StatusCode};
use nexus_test_utils::http_testing::{
    AuthnMode, NexusRequest, RequestBuilder, TestResponse,
};
use nexus_test_utils::resource_helpers::{
    create_local_user, create_silo, grant_iam, test_params,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::policy::SiloRole;
use nexus_types::external_api::silo::SiloIdentityMode;
use serde_json::{Value, json};
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;
const URL: &str = "/v1/service-accounts";

pub(super) async fn request(
    client: &ClientTestContext,
    authn: &AuthnMode,
    method: Method,
    url: &str,
    body: Option<&Value>,
    status: StatusCode,
) -> TestResponse {
    NexusRequest::new(
        RequestBuilder::new(client, method, url)
            .body(body)
            .expect_status(Some(status)),
    )
    .authn_as(authn.clone())
    .execute()
    .await
    .unwrap()
}

pub(super) async fn admin(
    client: &ClientTestContext,
    name: &str,
) -> (Uuid, AuthnMode) {
    let silo = create_silo(client, name, SiloIdentityMode::LocalOnly).await;
    let user = create_local_user(
        client,
        &silo,
        &"admin".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;
    grant_iam(
        client,
        &format!("/v1/system/silos/{name}"),
        SiloRole::Admin,
        user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;
    (silo.identity.id, AuthnMode::SiloUser(user.id))
}

pub(super) async fn project(
    client: &ClientTestContext,
    admin: &AuthnMode,
    name: &str,
) -> Uuid {
    let result: Value = request(
        client,
        admin,
        Method::POST,
        "/v1/projects",
        Some(&json!({"name": name, "description": "", "defaults": {}})),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    result["id"].as_str().unwrap().parse().unwrap()
}

fn params(kind: &str, resource_id: Uuid) -> Value {
    json!({"name": "reader", "description": "metrics reader",
        "grants": [{"resource_kind": kind, "resource_id": resource_id, "role_name": "viewer"}]})
}

#[nexus_test]
async fn test_service_account_namespaces(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let (silo, owner) = admin(client, "owner").await;
    let (_, other) = admin(client, "other").await;
    let p1 = project(client, &owner, "one").await;
    let p2 = project(client, &owner, "two").await;
    let mut ids = Vec::new();
    for (scope, parent) in [("silo", silo), ("project", p1), ("project", p2)] {
        let collection = format!("{URL}/{scope}?{scope}={parent}");
        let body = params(scope, parent);
        let created: Value = request(
            client,
            &owner,
            Method::POST,
            &collection,
            Some(&body),
            StatusCode::CREATED,
        )
        .await
        .parsed_body()
        .unwrap();
        let id = created["id"].as_str().unwrap();
        assert!(!ids.contains(&id.to_owned()));
        ids.push(id.to_owned());
        assert_eq!(created["scope"], scope);
        assert_eq!(created["resource_id"], parent.to_string());
        request(
            client,
            &owner,
            Method::POST,
            &collection,
            Some(&body),
            StatusCode::BAD_REQUEST,
        )
        .await;
        for url in [
            format!("{URL}/{scope}/{id}"),
            format!("{URL}/{scope}/reader?{scope}={parent}"),
        ] {
            let fetched: Value = request(
                client,
                &owner,
                Method::GET,
                &url,
                None,
                StatusCode::OK,
            )
            .await
            .parsed_body()
            .unwrap();
            assert_eq!(fetched["id"], id);
        }
        let listed: Value = request(
            client,
            &owner,
            Method::GET,
            &collection,
            None,
            StatusCode::OK,
        )
        .await
        .parsed_body()
        .unwrap();
        assert_eq!(listed["items"].as_array().unwrap().len(), 1);
        let url = format!("{URL}/{scope}/{id}");
        request(client, &other, Method::GET, &url, None, StatusCode::NOT_FOUND)
            .await;
        request(
            client,
            &other,
            Method::DELETE,
            &url,
            None,
            StatusCode::NOT_FOUND,
        )
        .await;
        let wrong_scope = if scope == "silo" { "project" } else { "silo" };
        request(
            client,
            &owner,
            Method::GET,
            &format!("{URL}/{wrong_scope}/{id}"),
            None,
            StatusCode::NOT_FOUND,
        )
        .await;
        request(
            client,
            &owner,
            Method::DELETE,
            &url,
            None,
            StatusCode::NO_CONTENT,
        )
        .await;
        request(client, &owner, Method::GET, &url, None, StatusCode::NOT_FOUND)
            .await;
        let replacement: Value = request(
            client,
            &owner,
            Method::POST,
            &collection,
            Some(&body),
            StatusCode::CREATED,
        )
        .await
        .parsed_body()
        .unwrap();
        assert_ne!(replacement["id"], id);
    }
}

#[nexus_test]
async fn test_service_account_grant_boundaries(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let (silo, owner) = admin(client, "owner").await;
    let (other_silo, other) = admin(client, "other").await;
    let p1 = project(client, &owner, "one").await;
    let p2 = project(client, &owner, "two").await;
    let outside = project(client, &other, "outside").await;
    for body in [
        params("silo", silo),
        params("project", p2),
        params("project", outside),
    ] {
        request(
            client,
            &owner,
            Method::POST,
            &format!("{URL}/project?project={p1}"),
            Some(&body),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    for body in [
        params("silo", other_silo),
        params("project", outside),
        params("project", Uuid::new_v4()),
    ] {
        request(
            client,
            &owner,
            Method::POST,
            &format!("{URL}/silo?silo={silo}"),
            Some(&body),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    let listed: Value = request(
        client,
        &owner,
        Method::GET,
        &format!("{URL}/project?project={p1}"),
        None,
        StatusCode::OK,
    )
    .await
    .parsed_body()
    .unwrap();
    assert!(listed["items"].as_array().unwrap().is_empty());
    let body = json!({"name": "both-projects", "description": "", "grants": [
        {"resource_kind": "project", "resource_id": p1, "role_name": "viewer"},
        {"resource_kind": "project", "resource_id": p2, "role_name": "collaborator"}
    ]});
    request(
        client,
        &owner,
        Method::POST,
        &format!("{URL}/silo?silo={silo}"),
        Some(&body),
        StatusCode::CREATED,
    )
    .await;
}

#[nexus_test]
async fn test_service_account_federation_patch(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let (silo, owner) = admin(client, "owner").await;
    let (_, other) = admin(client, "other").await;
    let idp_body = json!({"name": "gcp", "description": "", "issuer": "https://accounts.google.com",
        "audience": "oxide", "verification_type": "oidc_discovery"});
    let idp_url = "/v1/federation/inbound/identity-providers";
    let idp: Value = request(
        client,
        &owner,
        Method::POST,
        idp_url,
        Some(&idp_body),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    let outside: Value = request(
        client,
        &other,
        Method::POST,
        idp_url,
        Some(&idp_body),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    let collection = format!("{URL}/silo?silo={silo}");
    let mut body = params("silo", silo);
    body["federation"] = json!({"identity_provider": "gcp", "policy": "assume(jwt) if jwt.sub = \"builder\";"});
    let account: Value = request(
        client,
        &owner,
        Method::POST,
        &collection,
        Some(&body),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    let id = account["id"].as_str().unwrap();
    let url = format!("{URL}/silo/{id}");
    assert_eq!(account["federation"]["max_ttl_seconds"], 3600);
    assert_eq!(account["federation"]["identity_provider"], idp["id"]);
    assert_eq!(account["federation_generation"], 1);
    let changed: Value = request(
        client,
        &owner,
        Method::PATCH,
        &url,
        Some(
            &json!({"name": "renamed", "description": "updated", "grants": []}),
        ),
        StatusCode::OK,
    )
    .await
    .parsed_body()
    .unwrap();
    assert_eq!(changed["federation"], account["federation"]);
    assert_eq!(changed["federation_generation"], 1);
    assert_eq!(changed["grants"], json!([]));
    assert_eq!(changed["time_created"], account["time_created"]);
    for field in ["scope", "resource_id", "project", "silo"] {
        request(
            client,
            &owner,
            Method::PATCH,
            &url,
            Some(&json!({field: "changed"})),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }
    for (provider, status) in [
        (outside["id"].clone(), StatusCode::NOT_FOUND),
        (json!(Uuid::new_v4()), StatusCode::NOT_FOUND),
    ] {
        request(client, &owner, Method::PATCH, &url,
            Some(&json!({"federation": {"identity_provider": provider, "policy": "assume(_jwt);"}, "grants": body["grants"]})), status).await;
    }
    let unchanged: Value =
        request(client, &owner, Method::GET, &url, None, StatusCode::OK)
            .await
            .parsed_body()
            .unwrap();
    assert_eq!(unchanged, changed);
    body["federation"]["max_ttl_seconds"] = json!(120);
    let ttl_changed: Value = request(
        client,
        &owner,
        Method::PATCH,
        &url,
        Some(&json!({"federation": body["federation"]})),
        StatusCode::OK,
    )
    .await
    .parsed_body()
    .unwrap();
    assert_eq!(ttl_changed["federation_generation"], 1);
    body["federation"]["policy"] = json!("assume(jwt) if jwt.sub = \"other\";");
    let policy_changed: Value = request(
        client,
        &owner,
        Method::PATCH,
        &url,
        Some(&json!({"federation": body["federation"]})),
        StatusCode::OK,
    )
    .await
    .parsed_body()
    .unwrap();
    assert_eq!(policy_changed["federation_generation"], 2);
    let removed: Value = request(
        client,
        &owner,
        Method::PATCH,
        &url,
        Some(&json!({"federation": null})),
        StatusCode::OK,
    )
    .await
    .parsed_body()
    .unwrap();
    assert_eq!(removed["federation_generation"], 3);
    assert!(removed["federation"].is_null());
    let restored: Value = request(
        client,
        &owner,
        Method::PATCH,
        &url,
        Some(&json!({"federation": body["federation"]})),
        StatusCode::OK,
    )
    .await
    .parsed_body()
    .unwrap();
    assert_eq!(restored["federation_generation"], 4);
    request(
        client,
        &owner,
        Method::DELETE,
        &format!("{idp_url}/{}", idp["id"].as_str().unwrap()),
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    let mut new_body = body.clone();
    new_body["name"] = json!("second");
    request(
        client,
        &owner,
        Method::POST,
        &collection,
        Some(&new_body),
        StatusCode::NOT_FOUND,
    )
    .await;
}

#[nexus_test]
async fn test_service_account_parent_deletion(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let (silo, owner) = admin(client, "owner").await;
    let p = project(client, &owner, "one").await;
    let account: Value = request(
        client,
        &owner,
        Method::POST,
        &format!("{URL}/project?project={p}"),
        Some(&params("project", p)),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    request(
        client,
        &owner,
        Method::DELETE,
        &format!("/v1/projects/{p}"),
        None,
        StatusCode::BAD_REQUEST,
    )
    .await;
    request(
        client,
        &owner,
        Method::DELETE,
        &format!("{URL}/project/{}", account["id"].as_str().unwrap()),
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    request(
        client,
        &owner,
        Method::DELETE,
        &format!("/v1/projects/{p}"),
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    let account: Value = request(
        client,
        &owner,
        Method::POST,
        &format!("{URL}/silo?silo={silo}"),
        Some(&params("silo", silo)),
        StatusCode::CREATED,
    )
    .await
    .parsed_body()
    .unwrap();
    request(
        client,
        &AuthnMode::PrivilegedUser,
        Method::DELETE,
        &format!("/v1/system/silos/{silo}"),
        None,
        StatusCode::BAD_REQUEST,
    )
    .await;
    request(
        client,
        &owner,
        Method::DELETE,
        &format!("{URL}/silo/{}", account["id"].as_str().unwrap()),
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
    request(
        client,
        &AuthnMode::PrivilegedUser,
        Method::DELETE,
        &format!("/v1/system/silos/{silo}"),
        None,
        StatusCode::NO_CONTENT,
    )
    .await;
}

#[nexus_test]
async fn test_service_account_authorization(
    cptestctx: &ControlPlaneTestContext,
) {
    use nexus_types::external_api::policy::ProjectRole;
    use nexus_types::external_api::silo::Silo;
    let client = &cptestctx.external_client;
    let (silo_id, owner) = admin(client, "owner").await;
    let p = project(client, &owner, "one").await;
    let silo: Silo = request(
        client,
        &AuthnMode::PrivilegedUser,
        Method::GET,
        &format!("/v1/system/silos/{silo_id}"),
        None,
        StatusCode::OK,
    )
    .await
    .parsed_body()
    .unwrap();
    for (name, role, allowed) in [
        ("project-admin", ProjectRole::Admin, true),
        ("project-collaborator", ProjectRole::Collaborator, false),
        ("project-viewer", ProjectRole::Viewer, false),
    ] {
        let user = create_local_user(
            client,
            &silo,
            &name.parse().unwrap(),
            test_params::UserPassword::LoginDisallowed,
        )
        .await;
        grant_iam(
            client,
            &format!("/v1/projects/{p}"),
            role,
            user.id,
            owner.clone(),
        )
        .await;
        let authn = AuthnMode::SiloUser(user.id);
        let mut body = params("project", p);
        body["name"] = json!(name);
        request(
            client,
            &authn,
            Method::POST,
            &format!("{URL}/project?project={p}"),
            Some(&body),
            if allowed { StatusCode::CREATED } else { StatusCode::FORBIDDEN },
        )
        .await;
        request(
            client,
            &authn,
            Method::POST,
            &format!("{URL}/silo?silo={silo_id}"),
            Some(&params("silo", silo_id)),
            StatusCode::FORBIDDEN,
        )
        .await;
        for url in [
            format!("{URL}/project?project={p}"),
            format!("{URL}/project/project-admin?project={p}"),
        ] {
            request(client, &authn, Method::GET, &url, None, StatusCode::OK)
                .await;
        }
        if !allowed {
            let url = format!("{URL}/project/project-admin?project={p}");
            request(
                client,
                &authn,
                Method::PATCH,
                &url,
                Some(&json!({"grants": []})),
                StatusCode::FORBIDDEN,
            )
            .await;
            request(
                client,
                &authn,
                Method::DELETE,
                &url,
                None,
                StatusCode::FORBIDDEN,
            )
            .await;
        }
    }
    let user = create_local_user(
        client,
        &silo,
        &"silo-collaborator".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;
    grant_iam(
        client,
        &format!("/v1/system/silos/{silo_id}"),
        SiloRole::Collaborator,
        user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;
    let authn = AuthnMode::SiloUser(user.id);
    request(
        client,
        &authn,
        Method::POST,
        &format!("{URL}/project?project={p}"),
        Some(&params("project", p)),
        StatusCode::CREATED,
    )
    .await;
    request(
        client,
        &authn,
        Method::POST,
        &format!("{URL}/silo?silo={silo_id}"),
        Some(&params("silo", silo_id)),
        StatusCode::FORBIDDEN,
    )
    .await;
    request(
        client,
        &owner,
        Method::POST,
        &format!("{URL}/silo?silo={silo_id}"),
        Some(&params("silo", silo_id)),
        StatusCode::CREATED,
    )
    .await;
    let viewer = create_local_user(
        client,
        &silo,
        &"silo-viewer".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;
    grant_iam(
        client,
        &format!("/v1/system/silos/{silo_id}"),
        SiloRole::Viewer,
        viewer.id,
        AuthnMode::PrivilegedUser,
    )
    .await;
    let authn = AuthnMode::SiloUser(viewer.id);
    for url in [
        format!("{URL}/silo?silo={silo_id}"),
        format!("{URL}/silo/reader?silo={silo_id}"),
        format!("{URL}/project/project-admin?project={p}"),
    ] {
        request(client, &authn, Method::GET, &url, None, StatusCode::OK).await;
    }
    let url = format!("{URL}/silo/reader?silo={silo_id}");
    request(
        client,
        &authn,
        Method::PATCH,
        &url,
        Some(&json!({"grants": []})),
        StatusCode::FORBIDDEN,
    )
    .await;
    request(client, &authn, Method::DELETE, &url, None, StatusCode::FORBIDDEN)
        .await;
}

#[nexus_test]
async fn test_service_account_token_management(
    cptestctx: &ControlPlaneTestContext,
) {
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::{Duration, Utc};
    use diesel::prelude::*;
    use nexus_db_model::ServiceAccountToken;
    use nexus_db_schema::schema::service_account_token::dsl;
    use nexus_types::external_api::policy::ProjectRole;
    use nexus_types::external_api::silo::Silo;

    let client = &cptestctx.external_client;
    let (silo_id, owner) = admin(client, "owner").await;
    let (_, outsider) = admin(client, "other").await;
    let project_id = project(client, &owner, "one").await;
    let silo: Silo = request(
        client,
        &AuthnMode::PrivilegedUser,
        Method::GET,
        &format!("/v1/system/silos/{silo_id}"),
        None,
        StatusCode::OK,
    )
    .await
    .parsed_body()
    .unwrap();
    let conn = cptestctx
        .server
        .server_context()
        .nexus
        .datastore()
        .pool_connection_for_tests()
        .await
        .unwrap();

    for (scope, parent) in [("silo", silo_id), ("project", project_id)] {
        let mut actors = Vec::new();
        for is_admin in [false, true] {
            let name = format!(
                "{scope}-{}",
                if is_admin { "admin" } else { "viewer" }
            );
            let user = create_local_user(
                client,
                &silo,
                &name.parse().unwrap(),
                test_params::UserPassword::LoginDisallowed,
            )
            .await;
            if scope == "silo" {
                grant_iam(
                    client,
                    &format!("/v1/system/silos/{parent}"),
                    if is_admin { SiloRole::Admin } else { SiloRole::Viewer },
                    user.id,
                    AuthnMode::PrivilegedUser,
                )
                .await;
            } else {
                grant_iam(
                    client,
                    &format!("/v1/projects/{parent}"),
                    if is_admin {
                        ProjectRole::Admin
                    } else {
                        ProjectRole::Viewer
                    },
                    user.id,
                    owner.clone(),
                )
                .await;
            }
            actors.push(AuthnMode::SiloUser(user.id));
        }
        let viewer = &actors[0];
        let manager = &actors[1];
        let mut accounts = Vec::new();
        for name in ["reader", "other"] {
            let mut body = params(scope, parent);
            body["name"] = json!(name);
            let account: Value = request(
                client,
                &owner,
                Method::POST,
                &format!("{URL}/{scope}?{scope}={parent}"),
                Some(&body),
                StatusCode::CREATED,
            )
            .await
            .parsed_body()
            .unwrap();
            accounts
                .push(account["id"].as_str().unwrap().parse::<Uuid>().unwrap());
        }
        let account_id = accounts[0];
        let other_id = accounts[1];
        let now = Utc::now();
        let mut tokens = Vec::new();
        for (index, account) in [account_id, account_id, account_id, other_id]
            .into_iter()
            .enumerate()
        {
            let token_id = Uuid::new_v4();
            tokens.push(ServiceAccountToken {
                id: token_id,
                time_created: now,
                time_last_used: now,
                service_account_id: account,
                token: token_id.simple().to_string(),
                idp_id: Some(Uuid::new_v4()),
                federation_jwt_claims: Some(
                    json!({"sub": "external-subject", "custom": true}),
                ),
                federation_generation: Some(1),
                time_expires: Some(if index == 1 {
                    now - Duration::hours(1)
                } else {
                    now + Duration::hours(1)
                }),
                time_deleted: (index == 2).then_some(now),
            });
        }
        diesel::insert_into(dsl::service_account_token)
            .values(tokens.clone())
            .execute_async(&*conn)
            .await
            .unwrap();
        let collection = format!("{URL}/{scope}/{account_id}/tokens");
        let named = format!("{URL}/{scope}/reader/tokens?{scope}={parent}");
        for actor in [viewer, manager] {
            for url in [&collection, &named] {
                let page: Value = request(
                    client,
                    actor,
                    Method::GET,
                    url,
                    None,
                    StatusCode::OK,
                )
                .await
                .parsed_body()
                .unwrap();
                let items = page["items"].as_array().unwrap();
                assert_eq!(items.len(), 2);
                for token in &tokens[..2] {
                    let item = items
                        .iter()
                        .find(|item| item["id"] == token.id.to_string())
                        .unwrap();
                    assert_eq!(item.as_object().unwrap().len(), 4);
                    let fetched: Value = request(
                        client,
                        actor,
                        Method::GET,
                        &format!("{collection}/{}", token.id),
                        None,
                        StatusCode::OK,
                    )
                    .await
                    .parsed_body()
                    .unwrap();
                    assert_eq!(*item, fetched);
                }
            }
        }
        let first: Value = request(
            client,
            viewer,
            Method::GET,
            &format!("{named}&limit=1"),
            None,
            StatusCode::OK,
        )
        .await
        .parsed_body()
        .unwrap();
        assert_eq!(first["items"].as_array().unwrap().len(), 1);
        let cursor = first["next_page"].as_str().unwrap();
        let second: Value = request(
            client,
            viewer,
            Method::GET,
            &format!("{URL}/{scope}/reader/tokens?page_token={cursor}"),
            None,
            StatusCode::OK,
        )
        .await
        .parsed_body()
        .unwrap();
        assert_eq!(second["items"].as_array().unwrap().len(), 1);
        assert_ne!(first["items"][0]["id"], second["items"][0]["id"]);
        let token_url = format!("{collection}/{}", tokens[0].id);
        request(
            client,
            viewer,
            Method::DELETE,
            &token_url,
            None,
            StatusCode::FORBIDDEN,
        )
        .await;
        request(
            client,
            &outsider,
            Method::GET,
            &collection,
            None,
            StatusCode::NOT_FOUND,
        )
        .await;
        for method in [Method::GET, Method::DELETE] {
            request(
                client,
                &outsider,
                method.clone(),
                &token_url,
                None,
                StatusCode::NOT_FOUND,
            )
            .await;
            for token in &tokens[2..] {
                request(
                    client,
                    manager,
                    method.clone(),
                    &format!("{collection}/{}", token.id),
                    None,
                    StatusCode::NOT_FOUND,
                )
                .await;
            }
            let wrong_scope = if scope == "silo" { "project" } else { "silo" };
            request(
                client,
                manager,
                method,
                &format!(
                    "{URL}/{wrong_scope}/{account_id}/tokens/{}",
                    tokens[0].id
                ),
                None,
                StatusCode::NOT_FOUND,
            )
            .await;
        }
        request(
            client,
            manager,
            Method::DELETE,
            &token_url,
            None,
            StatusCode::NO_CONTENT,
        )
        .await;
        for method in [Method::GET, Method::DELETE] {
            request(
                client,
                manager,
                method,
                &token_url,
                None,
                StatusCode::NOT_FOUND,
            )
            .await;
        }
        let row = dsl::service_account_token
            .filter(dsl::id.eq(tokens[0].id))
            .select(ServiceAccountToken::as_select())
            .first_async(&*conn)
            .await
            .unwrap();
        assert!(row.time_deleted.is_some());
        assert_eq!(row.federation_jwt_claims, tokens[0].federation_jwt_claims);
        let page: Value = request(
            client,
            viewer,
            Method::GET,
            &collection,
            None,
            StatusCode::OK,
        )
        .await
        .parsed_body()
        .unwrap();
        assert_eq!(page["items"].as_array().unwrap().len(), 1);
        assert_eq!(page["items"][0]["id"], tokens[1].id.to_string());
        request(
            client,
            manager,
            Method::DELETE,
            &format!("{URL}/{scope}/{account_id}"),
            None,
            StatusCode::NO_CONTENT,
        )
        .await;
        request(
            client,
            manager,
            Method::GET,
            &collection,
            None,
            StatusCode::NOT_FOUND,
        )
        .await;
        for method in [Method::GET, Method::DELETE] {
            request(
                client,
                manager,
                method,
                &format!("{collection}/{}", tokens[1].id),
                None,
                StatusCode::NOT_FOUND,
            )
            .await;
        }
    }
}

#[nexus_test]
async fn test_service_account_token_issuance(
    cptestctx: &ControlPlaneTestContext,
) {
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::{DateTime, Duration, SubsecRound, Utc};
    use diesel::prelude::*;
    use nexus_db_model::{DeviceAccessToken, ServiceAccountToken};
    use nexus_db_schema::schema::{device_access_token, service_account_token};
    use nexus_types::external_api::policy::ProjectRole;
    use nexus_types::external_api::silo::Silo;

    let client = &cptestctx.external_client;
    let (silo_id, owner) = admin(client, "owner").await;
    let (_, outsider) = admin(client, "other").await;
    let project_id = project(client, &owner, "one").await;
    let silo: Silo = request(
        client,
        &AuthnMode::PrivilegedUser,
        Method::GET,
        &format!("/v1/system/silos/{silo_id}"),
        None,
        StatusCode::OK,
    )
    .await
    .parsed_body()
    .unwrap();
    let conn = cptestctx
        .server
        .server_context()
        .nexus
        .datastore()
        .pool_connection_for_tests()
        .await
        .unwrap();

    for (scope, parent) in [("silo", silo_id), ("project", project_id)] {
        let created: Value = request(
            client,
            &owner,
            Method::POST,
            &format!("{URL}/{scope}?{scope}={parent}"),
            Some(&params(scope, parent)),
            StatusCode::CREATED,
        )
        .await
        .parsed_body()
        .unwrap();
        let account_id = created["id"].as_str().unwrap();
        let url = format!("{URL}/{scope}/{account_id}/token");
        let mut admin_id = None;
        for (name, allowed) in
            [("viewer", false), ("collaborator", false), ("admin", true)]
        {
            let user = create_local_user(
                client,
                &silo,
                &format!("{scope}-{name}").parse().unwrap(),
                test_params::UserPassword::LoginDisallowed,
            )
            .await;
            if scope == "silo" {
                let role = match name {
                    "admin" => SiloRole::Admin,
                    "collaborator" => SiloRole::Collaborator,
                    _ => SiloRole::Viewer,
                };
                grant_iam(
                    client,
                    &format!("/v1/system/silos/{parent}"),
                    role,
                    user.id,
                    AuthnMode::PrivilegedUser,
                )
                .await;
            } else {
                let role = match name {
                    "admin" => ProjectRole::Admin,
                    "collaborator" => ProjectRole::Collaborator,
                    _ => ProjectRole::Viewer,
                };
                grant_iam(
                    client,
                    &format!("/v1/projects/{parent}"),
                    role,
                    user.id,
                    owner.clone(),
                )
                .await;
            }
            let response = request(
                client,
                &AuthnMode::SiloUser(user.id),
                Method::POST,
                &url,
                Some(&json!({})),
                if allowed {
                    StatusCode::CREATED
                } else {
                    StatusCode::FORBIDDEN
                },
            )
            .await;
            if allowed {
                admin_id = Some(user.id);
                let grant: Value = response.parsed_body().unwrap();
                assert!(grant["time_expires"].is_null());
                let id: Uuid = grant["id"].as_str().unwrap().parse().unwrap();
                let token = service_account_token::table
                    .filter(service_account_token::id.eq(id))
                    .select(ServiceAccountToken::as_select())
                    .first_async(&*conn)
                    .await
                    .unwrap();
                assert_eq!(
                    grant["token"],
                    format!("oxide-service-account-{}", token.token)
                );
                assert_eq!(token.token.len(), 40);
                assert_eq!(token.service_account_id.to_string(), account_id);
                assert!(token.idp_id.is_none());
                assert!(token.federation_jwt_claims.is_none());
                assert!(token.federation_generation.is_none());
                assert!(token.time_deleted.is_none());
                let metadata: Value = request(
                    client,
                    &owner,
                    Method::GET,
                    &format!("{URL}/{scope}/{account_id}/tokens/{id}"),
                    None,
                    StatusCode::OK,
                )
                .await
                .parsed_body()
                .unwrap();
                assert!(metadata.get("token").is_none());
            }
        }
        for ttl in [0, -1, 4294967296i64] {
            request(
                client,
                &owner,
                Method::POST,
                &url,
                Some(&json!({"ttl_seconds": ttl})),
                StatusCode::BAD_REQUEST,
            )
            .await;
        }
        request(
            client,
            &outsider,
            Method::POST,
            &url,
            Some(&json!({})),
            StatusCode::NOT_FOUND,
        )
        .await;
        let before = Utc::now();
        let grant: Value = request(
            client,
            &owner,
            Method::POST,
            &format!("{URL}/{scope}/reader/token?{scope}={parent}"),
            Some(&json!({"ttl_seconds": 30})),
            StatusCode::CREATED,
        )
        .await
        .parsed_body()
        .unwrap();
        let expiry: DateTime<Utc> =
            serde_json::from_value(grant["time_expires"].clone()).unwrap();
        assert!(expiry >= before + Duration::seconds(30));
        assert!(expiry <= Utc::now() + Duration::seconds(30));
        let device = DeviceAccessToken::new(
            Uuid::new_v4(),
            Uuid::new_v4().to_string(),
            Utc::now(),
            admin_id.unwrap(),
            Some(Utc::now() + Duration::minutes(5)),
        );
        let expected_expiry = device.time_expires;
        let authn =
            AuthnMode::DeviceToken(format!("oxide-token-{}", device.token));
        diesel::insert_into(device_access_token::table)
            .values(device)
            .execute_async(&*conn)
            .await
            .unwrap();
        let inherited: Value = request(
            client,
            &authn,
            Method::POST,
            &url,
            Some(&json!({})),
            StatusCode::CREATED,
        )
        .await
        .parsed_body()
        .unwrap();
        let inherited_expiry: Option<DateTime<Utc>> =
            serde_json::from_value(inherited["time_expires"].clone()).unwrap();
        assert_eq!(
            inherited_expiry,
            expected_expiry.map(|t| t.trunc_subsecs(6))
        );
        request(
            client,
            &authn,
            Method::POST,
            &url,
            Some(&json!({"ttl_seconds": 600})),
            StatusCode::BAD_REQUEST,
        )
        .await;
        request(
            client,
            &authn,
            Method::POST,
            &url,
            Some(&json!({"ttl_seconds": 30})),
            StatusCode::CREATED,
        )
        .await;
        request(
            client,
            &owner,
            Method::DELETE,
            &format!("{URL}/{scope}/{account_id}"),
            None,
            StatusCode::NO_CONTENT,
        )
        .await;
        request(
            client,
            &owner,
            Method::POST,
            &url,
            Some(&json!({})),
            StatusCode::NOT_FOUND,
        )
        .await;
    }
}
