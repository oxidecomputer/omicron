// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! (Fairly) comprehensive test of authz by creating a hierarchy of resources
//! and a group of users with various roles on these resources and verifying
//! that each role grants the privileges we expect.

use anyhow::anyhow;
use dropshot::test_util::ClientTestContext;
use http::Method;
use http::StatusCode;
use lazy_static::lazy_static;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils_macros::nexus_test;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::IdentityMetadataUpdateParams;
use omicron_common::api::external::Ipv4Net;
use omicron_common::api::external::Name;
use omicron_nexus::app::test_interfaces::TestInterfaces;
use omicron_nexus::authz;
use omicron_nexus::external_api::params;
use omicron_nexus::external_api::shared;
use omicron_nexus::external_api::shared::IdentityType;
use std::collections::BTreeMap;
use std::convert::AsRef;
use std::io::Write;
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
    let mut cursor = std::io::Cursor::new(Vec::new());
    {
        let mut stdout = std::io::stdout();
        let mut stream = DumbTee::new(vec![&mut cursor, &mut stdout]);
        test_all_operations(cptestctx, &world, &mut stream)
            .await
            .expect("failed to write output");
    }

    let output = cursor.into_inner();
    expectorate::assert_contents(
        "tests/output/authz-roles-test.txt",
        &String::from_utf8_lossy(&output),
    );
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
            ResourceType::Vpc {
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
            ResourceType::Vpc {
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
            ResourceType::Vpc { parent_project, parent_org, .. } => {
                format!(
                    "/organizations/{}/projects/{}/vpcs",
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
            ResourceType::Vpc { .. } => unimplemented!(),
        }
    }

    fn test_operations<'a>(
        &self,
        client: &'a ClientTestContext,
        username: &str,
    ) -> ResourceTestOperations<'a> {
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
        // TODO-coverage what other interesting cases aren't covered?
        // - fetch policy (all resources)
        // - update policy (how do we do this?!)
        match self.resource_type {
            ResourceType::Fleet => {
                let silos_url = "/silos";
                let new_silo_name = username.parse().expect(
                    "invalid test Silo name (tried to use a username \
                    that we generated)",
                );
                let new_silo_description = format!("created by {}", username);
                let silo_url = format!("{}/{}", &silos_url, new_silo_name);
                ResourceTestOperations {
                    comment: "Fleet-level roles apply to everything in \
                            the system.  However, even fleet admins have no \
                            way to refer to resources in Silos other than \
                            the ones they exist in.  This test creates fleet \
                            admins in Silo \"s1\".",
                    operations: vec![
                        TestOperation {
                            label: "FetchPolicy",
                            template: NexusRequest::new(RequestBuilder::new(
                                client,
                                Method::GET,
                                "/policy",
                            )),
                            on_success: None,
                        },
                        TestOperation {
                            label: "ListSagas",
                            template: NexusRequest::new(RequestBuilder::new(
                                client,
                                Method::GET,
                                "/sagas",
                            )),
                            on_success: None,
                        },
                        TestOperation {
                            label: "ListSleds",
                            template: NexusRequest::new(RequestBuilder::new(
                                client,
                                Method::GET,
                                "/hardware/sleds",
                            )),
                            on_success: None,
                        },
                        TestOperation {
                            label: "ListRacks",
                            template: NexusRequest::new(RequestBuilder::new(
                                client,
                                Method::GET,
                                "/hardware/racks",
                            )),
                            on_success: None,
                        },
                        TestOperation {
                            label: "ListBuiltinUsers",
                            template: NexusRequest::new(RequestBuilder::new(
                                client,
                                Method::GET,
                                "/users",
                            )),
                            on_success: None,
                        },
                        TestOperation {
                            label: "ListBuiltinRoles",
                            template: NexusRequest::new(RequestBuilder::new(
                                client,
                                Method::GET,
                                "/roles",
                            )),
                            on_success: None,
                        },
                        TestOperation {
                            label: "ListSilos",
                            template: NexusRequest::new(RequestBuilder::new(
                                client,
                                Method::GET,
                                "/silos",
                            )),
                            on_success: None,
                        },
                        TestOperation {
                            label: "CreateSilo",
                            template: NexusRequest::new(
                                RequestBuilder::new(
                                    client,
                                    Method::POST,
                                    "/silos",
                                )
                                .body(Some(
                                    &params::SiloCreate {
                                        identity:
                                            IdentityMetadataCreateParams {
                                                name: new_silo_name,
                                                description:
                                                    new_silo_description,
                                            },
                                        discoverable: true,
                                    },
                                )),
                            ),
                            on_success: Some(NexusRequest::object_delete(
                                client, &silo_url,
                            )),
                        },
                    ],
                }
            }

            ResourceType::Silo { name } => {
                let resource_url = format!("{}/{}", self.create_url(), name);
                let orgs_url = "/organizations";
                let new_org_name = username.parse().expect(
                    "invalid test organization name (tried to use a username \
                    that we generated)",
                );
                let new_org_description = format!("created by {}", username);
                let org_url = format!("{}/{}", orgs_url, new_org_name);
                ResourceTestOperations {
                    comment: "All users can view their own Silo.  Roles are \
                        required to list and create Organizations.  Note \
                        that administrators on \"s2\" appear to be able to \
                        list and create Organizations in the output below, \
                        but those are Organizations in \"s2\".",
                    operations: vec![
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
                            label: "ListOrganizations",
                            template: NexusRequest::new(RequestBuilder::new(
                                client,
                                Method::GET,
                                orgs_url,
                            )),
                            on_success: None,
                        },
                        TestOperation {
                            label: "CreateOrganization",
                            template: NexusRequest::new(
                                RequestBuilder::new(
                                    client,
                                    Method::POST,
                                    orgs_url,
                                )
                                .body(Some(
                                    &params::OrganizationCreate {
                                        identity:
                                            IdentityMetadataCreateParams {
                                                name: new_org_name,
                                                description:
                                                    new_org_description,
                                            },
                                    },
                                )),
                            ),
                            on_success: Some(NexusRequest::object_delete(
                                client, &org_url,
                            )),
                        },
                    ],
                }
            }

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
                ResourceTestOperations {
                    comment: "",
                    operations: vec![
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
                                        identity:
                                            IdentityMetadataCreateParams {
                                                name: new_project_name,
                                                description:
                                                    new_project_description,
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
                                        identity:
                                            IdentityMetadataUpdateParams {
                                                name: None,
                                                description: Some(
                                                    String::from("updated!"),
                                                ),
                                            },
                                    },
                                )),
                            ),
                            on_success: None,
                        },
                    ],
                }
            }

            ResourceType::Project { name, .. } => {
                let resource_url = format!("{}/{}", self.create_url(), name);
                let vpcs_url = format!("{}/vpcs", &resource_url);
                let new_vpc_name: Name = username.parse().expect(
                    "invalid test VPC name (tried to use a username \
                    that we generated)",
                );
                let new_vpc_description = format!("created by {}", username);
                let vpc_url = format!("{}/{}", &vpcs_url, new_vpc_name);
                ResourceTestOperations {
                    comment: "",
                    operations: vec![
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
                            label: "ListVpcs",
                            template: NexusRequest::new(RequestBuilder::new(
                                client,
                                Method::GET,
                                &vpcs_url,
                            )),
                            on_success: None,
                        },
                        TestOperation {
                            label: "CreateVpc",
                            template: NexusRequest::new(
                                RequestBuilder::new(
                                    client,
                                    Method::POST,
                                    &vpcs_url,
                                )
                                .body(Some(
                                    &params::VpcCreate {
                                        identity:
                                            IdentityMetadataCreateParams {
                                                name: new_vpc_name.clone(),
                                                description:
                                                    new_vpc_description,
                                            },
                                        ipv6_prefix: None,
                                        dns_name: new_vpc_name,
                                    },
                                )),
                            ),
                            on_success: Some(NexusRequest::object_delete(
                                client, &vpc_url,
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
                                    &params::ProjectUpdate {
                                        identity:
                                            IdentityMetadataUpdateParams {
                                                name: None,
                                                description: Some(
                                                    String::from("updated!"),
                                                ),
                                            },
                                    },
                                )),
                            ),
                            on_success: None,
                        },
                    ],
                }
            }

            ResourceType::Vpc { name, .. } => {
                let resource_url = format!("{}/{}", self.create_url(), name);
                let subnets_url = format!("{}/subnets", &resource_url);
                let new_subnet_name: Name = username.parse().expect(
                    "invalid test VPC name (tried to use a username \
                    that we generated)",
                );
                let new_subnet_description = format!("created by {}", username);
                let subnet_url =
                    format!("{}/{}", &subnets_url, new_subnet_name);

                ResourceTestOperations {
                    comment: "",
                    operations: vec![
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
                            label: "ListSubnets",
                            template: NexusRequest::new(RequestBuilder::new(
                                client,
                                Method::GET,
                                &subnets_url,
                            )),
                            on_success: None,
                        },
                        TestOperation {
                            label: "CreateSubnet",
                            template: NexusRequest::new(
                                RequestBuilder::new(
                                    client,
                                    Method::POST,
                                    &subnets_url,
                                )
                                .body(Some(
                                    &params::VpcSubnetCreate {
                                        identity:
                                            IdentityMetadataCreateParams {
                                                name: new_subnet_name.clone(),
                                                description:
                                                    new_subnet_description,
                                            },
                                        ipv4_block: Ipv4Net(
                                            "192.168.1.0/24".parse().unwrap(),
                                        ),
                                        ipv6_block: None,
                                    },
                                )),
                            ),
                            on_success: Some(NexusRequest::object_delete(
                                client,
                                &subnet_url,
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
                                    &params::VpcUpdate {
                                        identity:
                                            IdentityMetadataUpdateParams {
                                                name: None,
                                                description: Some(
                                                    String::from("updated!"),
                                                ),
                                            },
                                        dns_name: None,
                                    },
                                )),
                            ),
                            on_success: None,
                        },
                    ],
                }
            }
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
    Vpc {
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
        resource_type: ResourceType::Vpc {
            name: "v1",
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
        resource_type: ResourceType::Vpc {
            name: "v1",
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
        resource_type: ResourceType::Vpc {
            name: "v1",
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
        resource_type: ResourceType::Vpc {
            name: "v1",
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
        println!("creating resource: {:?}", resource);
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
            ResourceType::Vpc { name, parent_silo, .. } => {
                let caller_id = user_id(&users, &parent_silo, "admin");
                let full_name = resource.full_name();
                NexusRequest::objects_post(
                    client,
                    &resource.create_url(),
                    &params::VpcCreate {
                        identity: IdentityMetadataCreateParams {
                            name: name
                                .parse()
                                .expect("generated name was invalid"),
                            description: full_name.clone(),
                        },
                        ipv6_prefix: None,
                        dns_name: full_name.parse().expect(
                            "expected resource name to be a valid DNS name",
                        ),
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
            // We don't create users for Vpcs.  We already created users for
            // Silos.
            ResourceType::Vpc { .. } | ResourceType::Silo { .. } => {
                unimplemented!()
            }
            ResourceType::Fleet => {
                let silo_id = silos.get("s1").expect("missing silo \"s1\"");
                create_users::<authz::FleetRoles>(
                    log,
                    nexus,
                    client,
                    &resource.full_name(),
                    *silo_id,
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

    println!("setup done\n");
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
        println!("creating user: {}", &username);
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

        println!("adding role {} for user {}", role_name, &username);
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

struct ResourceTestOperations<'a> {
    comment: &'static str,
    operations: Vec<TestOperation<'a>>,
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

async fn test_all_operations<W: Write>(
    cptestctx: &ControlPlaneTestContext,
    world: &World,
    mut out: W,
) -> std::io::Result<()> {
    let log = &cptestctx.logctx.log;
    let client = &cptestctx.external_client;
    for resource in &world.resources {
        write!(
            out,
            "resource:  {} {:?}\n",
            resource.resource_type.as_ref(),
            resource.full_name()
        )?;

        let mut first = true;
        for ((user_resource_name, user_role_name), user_id) in &world.users {
            let username = format!("{}-{}", user_resource_name, user_role_name);
            let log = log.new(o!(
                "resource" => resource.full_name(),
                "user" => username.clone(),
            ));

            let resource_operations =
                resource.test_operations(client, &username);
            let comment = resource_operations.comment;
            let operations = resource_operations.operations;
            if first {
                let test_labels =
                    operations.iter().map(|t| t.label).collect::<Vec<_>>();
                write!(out, "  actions: {}\n", test_labels.join(", "))?;
                write!(out, "  note:    {}\n\n", comment)?;
                write!(out, "{:20} {}\n", "USER", "RESULTS FOR EACH ACTION")?;
                first = false;
            }

            write!(out, "{:20}", username)?;
            for test_operation in operations.into_iter() {
                let op_result =
                    run_test_operation(&log, test_operation, *user_id).await;
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

async fn run_test_operation<'a>(
    log: &slog::Logger,
    to: TestOperation<'a>,
    user_id: Uuid,
) -> OperationResult {
    let log = log.new(o!("operation" => to.label));
    trace!(log, "test operation");
    let request = to.template;
    let response = request
        .authn_as(AuthnMode::SiloUser(user_id))
        .execute()
        .await
        .expect("failed to execute request");
    let req_id = response
        .headers
        .get(dropshot::HEADER_REQUEST_ID)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let log = log.new(o!(
        "status_code" => response.status.to_string(),
        "req_id" => req_id,
    ));
    if matches!(response.status, StatusCode::NOT_FOUND | StatusCode::FORBIDDEN)
    {
        info!(log, "test operation result"; "result" => "denied");
        OperationResult::Denied
    } else if response.status.is_success() {
        info!(log, "test operation result"; "result" => "success");
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
        info!(
            log,
            "test operation result";
            "result" => "unexpected failure",
            "message" => error_response.message.clone(),
        );
        OperationResult::UnexpectedError(anyhow!(
            "unexpected response: status code {}, message {:?}",
            status,
            error_response.message
        ))
    }
}

struct DumbTee<'a> {
    sinks: Vec<&'a mut dyn Write>,
}

impl<'a> DumbTee<'a> {
    fn new(sinks: Vec<&mut dyn Write>) -> DumbTee {
        DumbTee { sinks }
    }
}

impl<'a> Write for DumbTee<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        for sink in &mut self.sinks {
            let size = sink
                .write(buf)
                .expect("one side of the tee failed unexpectedly");
            assert_eq!(
                size,
                buf.len(),
                "tee can only be used with streams that always accept all data"
            );
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        for sink in &mut self.sinks {
            sink.flush().expect("one side of the tee failed to flush");
        }

        Ok(())
    }
}
