// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! (Fairly) comprehensive test of authz by creating a hierarchy of resources
//! and a group of users with various roles on these resources and verifying
//! that each role grants the privileges we expect.
//! XXX-dap it looks like a bug that fleet admins can't see Organizations.  Is
//! silo.fleet not set up right in Polar?

use anyhow::anyhow;
use dropshot::test_util::ClientTestContext;
use futures::future::join_all;
use http::Method;
use http::StatusCode;
use lazy_static::lazy_static;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::InstanceCpuCount;
use omicron_nexus::app::test_interfaces::TestInterfaces;
use omicron_nexus::authz;
use omicron_nexus::external_api::params;
use omicron_nexus::external_api::params::InstanceNetworkInterfaceAttachment;
use omicron_nexus::external_api::shared;
use omicron_nexus::external_api::shared::IdentityType;
use std::collections::BTreeMap;
use std::convert::AsRef;
use strum::AsRefStr;
use uuid::Uuid;

struct World {
    users: BTreeMap<(String, String), Uuid>,
    resources: Vec<&'static Resource>,
}

#[nexus_test]
async fn test_authz_roles(cptestctx: &ControlPlaneTestContext) {
    let world = setup_hierarchy(cptestctx).await;

    // For each user that we've created, for each resource, for each possible
    // action, attempt the action.
    let results = test_operations(cptestctx, &world).await;

    let mut stream = std::io::Cursor::new(Vec::new());
    dump_results(&mut stream, &results)
        .expect("failed to write to in-memory buffer");
    let output = stream.into_inner();
    print!("{}", String::from_utf8_lossy(&output));

    // XXX-dap add expectorate
}

#[derive(Debug)]
struct Resource {
    resource_type: ResourceType,
}

impl Resource {
    fn full_name(&self) -> String {
        match self.resource_type {
            ResourceType::Fleet => String::from("fleet"),
            ResourceType::Silo { name } => format!("{}", name),
            ResourceType::Organization { name, parent_silo } => {
                format!("{}{}", parent_silo, name)
            }
            ResourceType::Project { name, parent_org, parent_silo } => {
                format!("{}{}{}", parent_silo, parent_org, name)
            }
            ResourceType::Instance {
                name,
                parent_project,
                parent_org,
                parent_silo,
            } => format!(
                "{}{}{}{}",
                parent_silo, parent_org, parent_project, name
            ),
        }
    }

    fn parent_full_name(&self) -> String {
        match self.resource_type {
            ResourceType::Fleet => unimplemented!(),
            ResourceType::Silo { .. } => unimplemented!(),
            ResourceType::Organization { parent_silo, .. } => {
                format!("{}", parent_silo)
            }
            ResourceType::Project { parent_org, parent_silo, .. } => {
                format!("{}{}", parent_silo, parent_org)
            }
            ResourceType::Instance {
                parent_project,
                parent_org,
                parent_silo,
                ..
            } => format!("{}{}{}", parent_silo, parent_org, parent_project),
        }
    }

    fn create_url(&self) -> String {
        match self.resource_type {
            ResourceType::Fleet => unimplemented!(),
            ResourceType::Silo { .. } => format!("/silos"),
            ResourceType::Organization { .. } => {
                format!("/organizations")
            }
            ResourceType::Project { parent_org, .. } => {
                format!("/organizations/{}/projects", parent_org)
            }
            ResourceType::Instance { parent_project, parent_org, .. } => {
                format!(
                    "/organizations/{}/projects/{}/instances",
                    parent_org, parent_project
                )
            }
        }
    }

    fn policy_url(&self) -> String {
        match self.resource_type {
            ResourceType::Fleet => format!("/policy"),
            ResourceType::Silo { name } => format!("/silos/{}/policy", name),
            ResourceType::Organization { name, .. } => {
                format!("/organizations/{}/policy", name)
            }
            ResourceType::Project { name, parent_org, .. } => format!(
                "/organizations/{}/projects/{}/policy",
                parent_org, name
            ),
            ResourceType::Instance { .. } => unimplemented!(),
        }
    }

    fn test_operations<'a>(
        &self,
        client: &'a ClientTestContext,
        username: &str,
    ) -> Vec<TestOperation<'a>> {
        // XXX-dap TODO examples:
        // - list children (various kinds)
        // - modify
        // - fetch
        // - delete?  this one seems hard to test because it might succeed!
        // - start/stop/halt -- hard to test because it's async
        // - migrate?
        // - disk attach/detach?
        // This is getting a little nuts.  In the limit, we kind of want to run
        // every test as a user with every role we think should grant that test
        // the privileges it needs (to make sure they grant enough).  Then we
        // kind of want to run every test again as every other role to make sure
        // it fails at some point.  Then we want to check coverage of the
        // endpoints/role combinations.  This seems really hard!
        match self.resource_type {
            // XXX-dap TODO
            ResourceType::Fleet => vec![],
            // XXX-dap TODO
            ResourceType::Silo { .. } => vec![],
            // XXX-dap TODO
            ResourceType::Organization { name, .. } => {
                let resource_url = format!("{}/{}", self.create_url(), name);
                let projects_url = format!("{}/projects", &resource_url);
                let new_project_name = username.parse().expect(
                    "invalid test project name (tried to use a username \
                    that we generated)",
                );
                let new_project_description =
                    format!("created by {}", username);
                let project_url =
                    format!("{}/{}", &projects_url, new_project_name);
                vec![
                    TestOperation {
                        label: "Fetch",
                        template: NexusRequest::new(RequestBuilder::new(
                            client,
                            Method::GET,
                            &resource_url,
                        )),
                        on_success: None,
                    },
                    TestOperation {
                        label: "ListProjects",
                        template: NexusRequest::new(RequestBuilder::new(
                            client,
                            Method::GET,
                            &projects_url,
                        )),
                        on_success: None,
                    },
                    TestOperation {
                        label: "CreateProject",
                        template: NexusRequest::new(
                            RequestBuilder::new(
                                client,
                                Method::POST,
                                &projects_url,
                            )
                            .body(Some(
                                &params::ProjectCreate {
                                    identity: IdentityMetadataCreateParams {
                                        name: new_project_name,
                                        description: new_project_description,
                                    },
                                },
                            )),
                        ),
                        on_success: Some(NexusRequest::object_delete(
                            client,
                            &project_url,
                        )),
                    },
                    TestOperation {
                        label: "ModifyDescription",
                        template: NexusRequest::new(
                            RequestBuilder::new(
                                client,
                                Method::PUT,
                                &resource_url,
                            )
                            .body(Some(
                                &params::OrganizationUpdate {
                                    identity: IdentityMetadataUpdateParams {
                                        name: None,
                                        description: Some(String::from(
                                            "updated!",
                                        )),
                                    },
                                },
                            )),
                        ),
                        on_success: None,
                    },
                ]
            }
            // XXX-dap TODO
            ResourceType::Project { .. } => vec![],
            // XXX-dap TODO
            ResourceType::Instance { .. } => vec![],
        }
    }
}

#[derive(Debug, AsRefStr)]
enum ResourceType {
    Fleet,
    Silo {
        name: &'static str,
    },
    Organization {
        name: &'static str,
        parent_silo: &'static str,
    },
    Project {
        name: &'static str,
        parent_org: &'static str,
        parent_silo: &'static str,
    },
    Instance {
        name: &'static str,
        parent_project: &'static str,
        parent_org: &'static str,
        parent_silo: &'static str,
    },
}

lazy_static! {
    // Hierarchy:
    // fleet
    // fleet/s1
    // fleet/s1/o1
    // fleet/s1/o1/p1
    // fleet/s1/o1/p1/i1
    // fleet/s1/o1/p2
    // fleet/s1/o1/p2/i1
    // fleet/s1/o2
    // fleet/s1/o2/p1
    // fleet/s1/o2/p1/i1
    // fleet/s2
    // fleet/s2/o3
    // fleet/s2/o3/p1
    // fleet/s2/o3/p1/i1
    //
    // Silo 2's organization is called "o3" because otherwise it appears as
    // though users from Silo 1 are successfully accessing it.
    // TODO-security TODO-coverage when we add support for looking up resources
    // by id, we should update this test to operate on things by-id as well.

    static ref FLEET: Resource = Resource { resource_type: ResourceType::Fleet };

    static ref SILO1: Resource =
        Resource { resource_type: ResourceType::Silo { name: "s1" } };
    static ref SILO1_ORG1: Resource = Resource {
        resource_type: ResourceType::Organization {
            name: "o1",
            parent_silo: "s1",
        },
    };
    static ref SILO1_ORG1_PROJ1: Resource = Resource {
        resource_type: ResourceType::Project {
            name: "p1",
            parent_org: "o1",
            parent_silo: "s1",
        },
    };
    static ref SILO1_ORG1_PROJ1_INST: Resource = Resource {
        resource_type: ResourceType::Instance {
            name: "i1",
            parent_project: "p1",
            parent_org: "o1",
            parent_silo: "s1",
        },
    };
    static ref SILO1_ORG1_PROJ2: Resource = Resource {
        resource_type: ResourceType::Project {
            name: "p2",
            parent_org: "o1",
            parent_silo: "s1",
        },
    };
    static ref SILO1_ORG1_PROJ2_INST: Resource = Resource {
        resource_type: ResourceType::Instance {
            name: "i1",
            parent_project: "p2",
            parent_org: "o1",
            parent_silo: "s1",
        },
    };
    static ref SILO1_ORG2: Resource = Resource {
        resource_type: ResourceType::Organization {
            name: "o2",
            parent_silo: "s1",
        },
    };
    static ref SILO1_ORG2_PROJ1: Resource = Resource {
        resource_type: ResourceType::Project {
            name: "p1",
            parent_org: "o2",
            parent_silo: "s1",
        },
    };
    static ref SILO1_ORG2_PROJ1_INST: Resource = Resource {
        resource_type: ResourceType::Instance {
            name: "i1",
            parent_project: "p1",
            parent_org: "o2",
            parent_silo: "s1",
        },
    };

    static ref SILO2: Resource =
        Resource { resource_type: ResourceType::Silo { name: "s2" } };
    static ref SILO2_ORG3: Resource = Resource {
        resource_type: ResourceType::Organization {
            name: "o3",
            parent_silo: "s2",
        },
    };
    static ref SILO2_ORG3_PROJ1: Resource = Resource {
        resource_type: ResourceType::Project {
            name: "p1",
            parent_org: "o3",
            parent_silo: "s2",
        },
    };
    static ref SILO2_ORG3_PROJ1_INST: Resource = Resource {
        resource_type: ResourceType::Instance {
            name: "i1",
            parent_project: "p1",
            parent_org: "o3",
            parent_silo: "s2",
        },
    };

    // XXX-dap TODO-doc These are the resources for which: for each role, we
    // will create a user having that role on this resource.  We will later test
    // that this user _can_ access the corresponding thing in the corresponding
    // way, and that they _cannot_ access any _other_ resources in any other
    // way.
    // Silos are skipped because we always create their users.
    static ref RESOURCES_WITH_USERS: Vec<&'static Resource> = vec![
        &*FLEET,
        &*SILO1_ORG1,
        &*SILO1_ORG1_PROJ1,
    ];

    // XXX-dap One idea to support testing DELETE:
    // - every resource has a "recreate()" function that recreates it (including
    //   the role assignments)
    // - this list is guaranteed to be topo-sorted
    // - we explicitly delete every resource when we're done testing it
    //
    // Then we can test these in reverse order.  By the time we get to any
    // resource, its children should be gone.
    static ref ALL_RESOURCES: Vec<&'static Resource> = vec![
        &*FLEET,
        &*SILO1,
        &*SILO1_ORG1,
        &*SILO1_ORG1_PROJ1,
        &*SILO1_ORG1_PROJ1_INST,
        &*SILO1_ORG1_PROJ2,
        &*SILO1_ORG1_PROJ2_INST,
        &*SILO1_ORG2,
        &*SILO1_ORG2_PROJ1,
        &*SILO1_ORG2_PROJ1_INST,
        &*SILO2,
        &*SILO2_ORG3,
        &*SILO2_ORG3_PROJ1,
        &*SILO2_ORG3_PROJ1_INST,
    ];
}

async fn setup_hierarchy(testctx: &ControlPlaneTestContext) -> World {
    let client = &testctx.external_client;
    let nexus = &*testctx.server.apictx.nexus;
    let log = &testctx.logctx.log.new(o!("component" => "SetupHierarchy"));

    // Mapping: (our resource name, role name) -> user id
    // where "user id" is the id of a user having role "role name" on resource
    // "our resource name".
    //
    // "our resource name" is a string like s1o1 (SILO1_ORG1).
    let mut users: BTreeMap<(String, String), Uuid> = BTreeMap::new();

    // Mapping: silo name -> silo id
    let mut silos: BTreeMap<String, Uuid> = BTreeMap::new();

    // We can't use the helpers in `resource_helpers` to create most of these
    // resources because they always use the "test-privileged" user in the
    // default Silo.  But in order to create things in other Silos, we need to
    // use different users.
    for resource in &*ALL_RESOURCES {
        let resource = *resource;
        debug!(log, "creating resource"; "resource" => ?resource);
        match resource.resource_type {
            ResourceType::Fleet => (),
            ResourceType::Silo { name } => {
                let silo =
                    resource_helpers::create_silo(client, name, false).await;
                silos.insert(name.to_string(), silo.identity.id);
                // We have to create the Silo users here so that we can use the
                // admin to create the other resources.
                create_users::<authz::SiloRoles>(
                    log,
                    nexus,
                    client,
                    name,
                    silo.identity.id,
                    &resource.policy_url(),
                    &mut users,
                    None,
                )
                .await;
            }
            ResourceType::Organization { name, parent_silo, .. } => {
                let caller_id = user_id(&users, &parent_silo, "admin");
                let full_name = resource.full_name();
                NexusRequest::objects_post(
                    client,
                    &resource.create_url(),
                    &params::OrganizationCreate {
                        identity: IdentityMetadataCreateParams {
                            name: name
                                .parse()
                                .expect("generated name was invalid"),
                            description: full_name.clone(),
                        },
                    },
                )
                .authn_as(AuthnMode::SiloUser(caller_id))
                .execute()
                .await
                .unwrap_or_else(|_| panic!("failed to create {}", full_name));
            }
            ResourceType::Project { name, parent_silo, .. } => {
                let caller_id = user_id(&users, &parent_silo, "admin");
                let full_name = resource.full_name();
                NexusRequest::objects_post(
                    client,
                    &resource.create_url(),
                    &params::ProjectCreate {
                        identity: IdentityMetadataCreateParams {
                            name: name
                                .parse()
                                .expect("generated name was invalid"),
                            description: full_name.clone(),
                        },
                    },
                )
                .authn_as(AuthnMode::SiloUser(caller_id))
                .execute()
                .await
                .unwrap_or_else(|_| panic!("failed to create {}", full_name));
            }
            ResourceType::Instance { name, parent_silo, .. } => {
                let caller_id = user_id(&users, &parent_silo, "admin");
                let full_name = resource.full_name();
                NexusRequest::objects_post(
                    client,
                    &resource.create_url(),
                    &params::InstanceCreate {
                        identity: IdentityMetadataCreateParams {
                            name: name
                                .parse()
                                .expect("generated name was invalid"),
                            description: full_name.clone(),
                        },
                        ncpus: InstanceCpuCount(1),
                        memory: ByteCount::from_gibibytes_u32(1),
                        hostname: full_name.clone(),
                        user_data: vec![],
                        network_interfaces:
                            InstanceNetworkInterfaceAttachment::Default,
                        disks: vec![],
                    },
                )
                .authn_as(AuthnMode::SiloUser(caller_id))
                .execute()
                .await
                .unwrap_or_else(|_| panic!("failed to create {}", full_name));
            }
        }
    }

    for resource in &*RESOURCES_WITH_USERS {
        match resource.resource_type {
            // XXX-dap we should be testing fleet users too
            // We don't create users for Instances.  We already created users
            // for Silos.
            ResourceType::Instance { .. } | ResourceType::Silo { .. } => {
                unimplemented!()
            }
            ResourceType::Fleet => {
                create_users::<authz::FleetRoles>(
                    log,
                    nexus,
                    client,
                    &resource.full_name(),
                    *omicron_nexus::db::fixed_data::silo::SILO_ID,
                    &resource.policy_url(),
                    &mut users,
                    None,
                )
                .await;
            }
            ResourceType::Organization { parent_silo, .. } => {
                let silo_id = silos.get(parent_silo).expect("missing silo");
                let caller_id =
                    user_id(&users, &resource.parent_full_name(), "admin");
                create_users::<authz::OrganizationRoles>(
                    log,
                    nexus,
                    client,
                    &resource.full_name(),
                    *silo_id,
                    &resource.policy_url(),
                    &mut users,
                    Some(caller_id),
                )
                .await;
            }
            ResourceType::Project { parent_silo, .. } => {
                let silo_id = silos.get(parent_silo).expect("missing silo");
                let caller_id =
                    user_id(&users, &resource.parent_full_name(), "admin");
                create_users::<authz::ProjectRoles>(
                    log,
                    nexus,
                    client,
                    &resource.full_name(),
                    *silo_id,
                    &resource.policy_url(),
                    &mut users,
                    Some(caller_id),
                )
                .await;
            }
        }
    }

    World { users, resources: ALL_RESOURCES.clone() }
}

fn user_id(
    users: &BTreeMap<(String, String), Uuid>,
    resource_full_name: &str,
    role_name: &str,
) -> Uuid {
    *users
        .get(&(resource_full_name.to_owned(), role_name.to_owned()))
        .unwrap_or_else(|| {
            panic!(
                "expected user to be created with role {:?} on {:?}",
                role_name, resource_full_name,
            )
        })
}

/// For a given resource, for each supported role, create a new user with that
/// role on this resource.
///
/// - `nexus`: needed to use the private interface for creating silo users
/// - `resource_name`: *our* identifier for the resource (e.g., s1o1).  This is
///   used as the basename of the user's name
/// - `policy_url`: the URL for the resource's IAM policy
/// - `users`: a map into which we'll insert the uuids of created users
/// - `run_as`: executes requests to update resource policy using the given silo
///   user id.  This lets us, say, create an Organization with a user having
///   "admin" on the parent Silo, and create a Project with a user having
///   "admin" on the parent Organization, etc.
async fn create_users<
    T: strum::IntoEnumIterator
        + serde::Serialize
        + serde::de::DeserializeOwned
        + omicron_nexus::db::model::DatabaseString,
>(
    log: &slog::Logger,
    nexus: &dyn TestInterfaces,
    client: &ClientTestContext,
    resource_name: &str,
    silo_id: Uuid,
    policy_url: &str,
    users: &mut BTreeMap<(String, String), Uuid>,
    run_as: Option<Uuid>,
) {
    for variant in T::iter() {
        let role_name = variant.to_database_string();
        // TODO when silo users get user names, this should go into it.
        let username = format!("{}-{}", resource_name, role_name);
        let user_id = Uuid::new_v4();
        debug!(
            log,
            "creating user";
            "username" => &username,
            "user_id" => user_id.to_string()
        );
        nexus
            .silo_user_create(silo_id, user_id)
            .await
            .unwrap_or_else(|_| panic!("failed to create user {:?}", username));
        users.insert((resource_name.to_owned(), role_name.to_owned()), user_id);

        debug!(
            log,
            "adding role";
            "username" => username,
            "user_id" => user_id.to_string(),
            "role" => role_name,
        );
        let authn_mode = run_as
            .map(|caller_id| AuthnMode::SiloUser(caller_id))
            .unwrap_or(AuthnMode::PrivilegedUser);

        let existing_policy: shared::Policy<T> =
            NexusRequest::object_get(client, policy_url)
                .authn_as(authn_mode.clone())
                .execute()
                .await
                .expect("failed to fetch policy")
                .parsed_body()
                .expect("failed to parse policy");

        let new_role_assignment = shared::RoleAssignment {
            identity_type: IdentityType::SiloUser,
            identity_id: user_id,
            role_name: variant,
        };
        let new_role_assignments = existing_policy
            .role_assignments
            .into_iter()
            .chain(std::iter::once(new_role_assignment))
            .collect();

        let new_policy =
            shared::Policy { role_assignments: new_role_assignments };

        NexusRequest::object_put(client, policy_url, Some(&new_policy))
            .authn_as(authn_mode)
            .execute()
            .await
            .expect("failed to update policy");
    }
}

struct TestOperation<'a> {
    label: &'static str,
    template: NexusRequest<'a>,
    on_success: Option<NexusRequest<'a>>,
}

enum OperationResult {
    Success,
    Denied,
    UnexpectedError(anyhow::Error),
}

struct ResourceResults {
    resource: &'static Resource,
    test_labels: Vec<&'static str>,
    results: Vec<ResourceUserResults>,
}

struct ResourceUserResults {
    username: String,
    results: Vec<OperationResult>,
}

async fn test_operations(
    cptestctx: &ControlPlaneTestContext,
    world: &World,
) -> Vec<ResourceResults> {
    let log = &cptestctx.logctx.log;
    let client = &cptestctx.external_client;
    let mut results = Vec::with_capacity(world.resources.len());
    for resource in &world.resources {
        let mut user_results = Vec::with_capacity(world.users.len());
        let mut test_labels: Option<Vec<&'static str>> = None;
        for ((user_resource_name, user_role_name), user_id) in &world.users {
            let username = format!("{}-{}", user_resource_name, user_role_name);
            let log = log.new(o!(
                "resource" => resource.full_name(),
                "user" => username.clone(),
            ));

            let operations = resource.test_operations(client, &username);
            if test_labels.is_none() {
                test_labels =
                    Some(operations.iter().map(|t| t.label).collect());
            }
            user_results.push(ResourceUserResults {
                username,
                results: join_all(operations.into_iter().map(
                    |test_operation| {
                        run_test_operation(&log, test_operation, *user_id)
                    },
                ))
                .await,
            });
        }

        let result = if let Some(test_labels) = test_labels {
            assert!(!user_results.is_empty());
            ResourceResults { resource, test_labels, results: user_results }
        } else {
            assert!(user_results.is_empty());
            ResourceResults {
                resource,
                test_labels: Vec::new(),
                results: Vec::new(),
            }
        };
        results.push(result);
    }

    results
}

async fn run_test_operation<'a>(
    log: &slog::Logger,
    to: TestOperation<'a>,
    user_id: Uuid,
) -> OperationResult {
    info!(log, "test operation"; "operation" => to.label);
    let request = to.template;
    let response = request
        .authn_as(AuthnMode::SiloUser(user_id))
        .execute()
        .await
        .expect("failed to execute request");
    if matches!(response.status, StatusCode::NOT_FOUND | StatusCode::FORBIDDEN)
    {
        OperationResult::Denied
    } else if response.status.is_success() {
        if let Some(request) = to.on_success {
            debug!(log, "on_success operation");
            request
                .authn_as(AuthnMode::SiloUser(user_id))
                .execute()
                .await
                .expect("failed to execute on-success request");
        }

        OperationResult::Success
    } else {
        let status = response.status;
        let error_response: dropshot::HttpErrorResponseBody =
            response.parsed_body().expect("failed to parse error response");
        OperationResult::UnexpectedError(anyhow!(
            "unexpected response: status code {}, message {:?}",
            status,
            error_response.message
        ))
    }
}

fn dump_results<W: std::io::Write>(
    mut out: W,
    results: &[ResourceResults],
) -> std::io::Result<()> {
    for resource_result in results {
        write!(
            out,
            "resource:  {} {:?}",
            resource_result.resource.resource_type.as_ref(),
            resource_result.resource.full_name()
        )?;

        if resource_result.test_labels.is_empty() {
            write!(out, ": no tests defined\n\n")?;
            continue;
        }

        write!(
            out,
            "\n  actions: {}\n\n",
            resource_result.test_labels.join(", ")
        )?;

        write!(out, "{:20} {}\n", "USER", "RESULTS FOR EACH ACTION")?;
        for user_result in &resource_result.results {
            write!(out, "{:20}", user_result.username)?;
            for op_result in &user_result.results {
                write!(
                    out,
                    " {}",
                    match op_result {
                        OperationResult::Success => '\u{2713}',
                        OperationResult::Denied => '\u{2717}',
                        OperationResult::UnexpectedError(_) => '\u{26a0}',
                    }
                )?;
            }
            write!(out, "\n")?;
        }

        write!(out, "\n")?;
    }

    Ok(())
}
