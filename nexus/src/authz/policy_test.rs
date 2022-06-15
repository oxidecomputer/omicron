// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Unit tests for the Oso policy
//!
//! This differs from the end-to-end integration tests for authz.  The tests
//! here verify:
//!
//! - for resources covered by RBAC: that the roles in the policy grant the
//!   permissions that we expect that they do
//!
//! - for other policies: that the policy reflects the privileges that we expect
//!   (e.g., ordinary users don't have internal roles)
//!
//! XXX-dap TODO:
//! - review above comment
//! - review remaining XXX-dap
//! - clean up, document test
//! - figure out what other types to add
//! - is there a way to verify coverage of all authz types?

use super::ApiResource;
use super::ApiResourceWithRoles;
use super::ApiResourceWithRolesType;
use crate::authn;
use crate::authz;
use crate::authz::AuthorizedResource;
use crate::context::OpContext;
use crate::db;
use crate::db::model::DatabaseString;
use crate::external_api::shared;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_test_utils::db::test_setup_database;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_test_utils::dev;
use std::fmt::Write;
use std::sync::Arc;
use strum::IntoEnumIterator;
use uuid::Uuid;

#[tokio::test]
async fn test_iam_roles() {
    let logctx = dev::test_setup_log("test_iam_roles");
    let mut db = test_setup_database(&logctx.log).await;
    let (opctx, datastore) = db::datastore::datastore_test(&logctx, &db).await;

    let test_resources = make_resources();
    let silo1_id = test_resources.silo1.resource_id();
    let mut users: Vec<(String, Uuid)> = Vec::new();

    create_users(
        &opctx,
        &*datastore,
        "fleet",
        silo1_id,
        &authz::FLEET,
        &mut users,
    )
    .await;

    create_users(
        &opctx,
        &*datastore,
        "silo1",
        silo1_id,
        &test_resources.silo1,
        &mut users,
    )
    .await;

    create_users(
        &opctx,
        &*datastore,
        "silo1-org1",
        silo1_id,
        &test_resources.silo1_org1,
        &mut users,
    )
    .await;

    create_users(
        &opctx,
        &*datastore,
        "silo1-org1-proj1",
        silo1_id,
        &test_resources.silo1_org1_proj1,
        &mut users,
    )
    .await;

    // Create an OpContext for each user for testing.
    let authz = Arc::new(authz::Authz::new(&logctx.log));
    let user_contexts: Vec<(String, Uuid, OpContext)> = users
        .iter()
        .map(|(username, user_id)| {
            let user_id = *user_id;
            let user_log = logctx.log.new(o!(
                "user_id" => user_id.to_string(),
                "username" => username.clone(),
            ));
            let opctx = OpContext::for_background(
                user_log,
                Arc::clone(&authz),
                authn::Context::for_test_user(user_id, silo1_id),
                Arc::clone(&datastore),
            );

            (username.clone(), user_id, opctx)
        })
        .collect();

    let mut buffer = String::new();
    {
        let mut tee = StdoutTee::new(&mut buffer);

        run_test_operations(
            &mut tee,
            &logctx.log,
            &user_contexts,
            &test_resources,
        )
        .await
        .unwrap();
    }

    expectorate::assert_contents("tests/output/authz-roles.out", &buffer);

    db.cleanup().await.unwrap();
    logctx.cleanup_successful();
}

async fn run_test_operations<W: Write>(
    mut out: W,
    log: &slog::Logger,
    user_contexts: &[(String, Uuid, OpContext)],
    test_resources: &Resources,
) -> std::fmt::Result {
    for resource in test_resources.all_resources() {
        write!(out, "resource: {}\n\n", resource_name(resource),)?;

        write!(out, "  {:31}", "USER")?;
        for action in authz::Action::iter() {
            write!(out, " {:>2}", action_abbreviation(action))?;
        }
        write!(out, "\n")?;

        for (username, _, opctx) in user_contexts.iter() {
            write!(out, "  {:31}", &username)?;
            for action in authz::Action::iter() {
                let result = resource.do_authorize(opctx, action).await;
                trace!(
                    log,
                    "do_authorize result";
                    "username" => username.clone(),
                    "resource" => ?resource,
                    "action" => ?action,
                    "result" => ?result,
                );
                let summary = match result {
                    Ok(_) => '\u{2713}',
                    Err(Error::Forbidden)
                    | Err(Error::ObjectNotFound { .. }) => '\u{2717}',
                    Err(_) => '\u{26a0}',
                };
                write!(out, " {:>2}", summary)?;
            }
            write!(out, "\n")?;
        }

        write!(out, "\n")?;
    }

    write!(out, "ACTIONS:\n\n")?;
    for action in authz::Action::iter() {
        write!(out, "  {:>2} = {:?}\n", action_abbreviation(action), action)?;
    }
    write!(out, "\n")?;

    Ok(())
}

/// Describes the hierarchy of resources used in our RBAC test
// The hierarchy looks like this:
// fleet
// fleet/s1
// fleet/s1/o1
// fleet/s1/o1/p1
// fleet/s1/o1/p1/vpc1
// fleet/s1/o1/p2
// fleet/s1/o1/p2/vpc1
// fleet/s1/o2
// fleet/s1/o2/p1
// fleet/s1/o2/p1/vpc1
// fleet/s2
// fleet/s2/o1
// fleet/s2/o1/p1
// fleet/s2/o1/p1/vpc1
struct Resources {
    silo1: authz::Silo,
    silo1_org1: authz::Organization,
    silo1_org1_proj1: authz::Project,
    silo1_org1_proj1_children: Vec<Box<dyn Authorizable>>,
    silo1_org1_proj2: authz::Project,
    silo1_org1_proj2_children: Vec<Box<dyn Authorizable>>,
    silo1_org2: authz::Organization,
    silo1_org2_proj1: authz::Project,
    silo1_org2_proj1_children: Vec<Box<dyn Authorizable>>,
    silo2: authz::Silo,
    silo2_org1: authz::Organization,
    silo2_org1_proj1: authz::Project,
    silo2_org1_proj1_children: Vec<Box<dyn Authorizable>>,
}

impl Resources {
    fn all_resources(
        &self,
    ) -> impl std::iter::Iterator<Item = &dyn Authorizable> {
        vec![
            &authz::FLEET as &dyn Authorizable,
            &self.silo1 as &dyn Authorizable,
            &self.silo1_org1 as &dyn Authorizable,
            &self.silo1_org1_proj1 as &dyn Authorizable,
        ]
        .into_iter()
        .chain(
            self.silo1_org1_proj1_children
                .iter()
                .map(|d: &Box<dyn Authorizable>| d.as_ref()),
        )
        .chain(std::iter::once(&self.silo1_org1_proj2 as &dyn Authorizable))
        .chain(
            self.silo1_org1_proj2_children
                .iter()
                .map(|d: &Box<dyn Authorizable>| d.as_ref()),
        )
        .chain(vec![
            &self.silo1_org2 as &dyn Authorizable,
            &self.silo1_org2_proj1 as &dyn Authorizable,
        ])
        .chain(
            self.silo1_org2_proj1_children
                .iter()
                .map(|d: &Box<dyn Authorizable>| d.as_ref()),
        )
        .chain(vec![
            &self.silo2 as &dyn Authorizable,
            &self.silo2_org1 as &dyn Authorizable,
            &self.silo2_org1_proj1 as &dyn Authorizable,
        ])
        .chain(
            self.silo2_org1_proj1_children
                .iter()
                .map(|d: &Box<dyn Authorizable>| d.as_ref()),
        )
    }
}

trait Authorizable: AuthorizedResource + ApiResource {
    fn do_authorize<'a, 'b>(
        &'a self,
        opctx: &'b OpContext,
        action: authz::Action,
    ) -> BoxFuture<'a, Result<(), Error>>
    where
        'b: 'a;
}

impl<T> Authorizable for T
where
    T: ApiResource + AuthorizedResource + Clone,
{
    fn do_authorize<'a, 'b>(
        &'a self,
        opctx: &'b OpContext,
        action: authz::Action,
    ) -> BoxFuture<'a, Result<(), Error>>
    where
        'b: 'a,
    {
        opctx.authorize(action, self).boxed()
    }
}

// XXX-dap make this deterministic
// Most of the uuids here are hardcoded rather than randomly generated for
// debuggability.
fn make_uuid() -> Uuid {
    Uuid::new_v4()
}

fn make_resources() -> Resources {
    let silo1_id = make_uuid();
    let silo1 = authz::Silo::new(
        authz::FLEET,
        silo1_id,
        LookupType::ByName(String::from("silo1")),
    );

    let silo1_org1 = authz::Organization::new(
        silo1.clone(),
        make_uuid(),
        LookupType::ByName(String::from("silo1-org1")),
    );
    let (silo1_org1_proj1, silo1_org1_proj1_children) =
        make_project(&silo1_org1, "silo1-org1-proj1");
    let (silo1_org1_proj2, silo1_org1_proj2_children) =
        make_project(&silo1_org1, "silo1-org1-proj2");

    let silo1_org2 = authz::Organization::new(
        silo1.clone(),
        make_uuid(),
        LookupType::ByName(String::from("silo1-org2")),
    );
    let (silo1_org2_proj1, silo1_org2_proj1_children) =
        make_project(&silo1_org2, "silo1-org2-proj1");

    let silo2_id = make_uuid();
    let silo2 = authz::Silo::new(
        authz::FLEET,
        silo2_id,
        LookupType::ByName(String::from("silo2")),
    );

    let silo2_org1 = authz::Organization::new(
        silo2.clone(),
        make_uuid(),
        LookupType::ByName(String::from("silo2-org1")),
    );
    let (silo2_org1_proj1, silo2_org1_proj1_children) =
        make_project(&silo2_org1, "silo2-org1-proj1");

    Resources {
        silo1,
        silo1_org1,
        silo1_org1_proj1,
        silo1_org1_proj1_children,
        silo1_org1_proj2,
        silo1_org1_proj2_children,
        silo1_org2,
        silo1_org2_proj1,
        silo1_org2_proj1_children,
        silo2,
        silo2_org1,
        silo2_org1_proj1,
        silo2_org1_proj1_children,
    }
}

fn make_project(
    organization: &authz::Organization,
    project_name: &str,
) -> (authz::Project, Vec<Box<dyn Authorizable>>) {
    let project = authz::Project::new(
        organization.clone(),
        make_uuid(),
        LookupType::ByName(project_name.to_string()),
    );

    let vpc1_name = format!("{}-vpc1", project_name);
    let vpc1 = authz::Vpc::new(
        project.clone(),
        make_uuid(),
        LookupType::ByName(vpc1_name.clone()),
    );
    let children: Vec<Box<dyn Authorizable>> = vec![
        // XXX-dap TODO-coverage add more different kinds of children
        Box::new(authz::Disk::new(
            project.clone(),
            make_uuid(),
            LookupType::ByName(format!("{}-disk1", project_name)),
        )),
        Box::new(authz::Instance::new(
            project.clone(),
            make_uuid(),
            LookupType::ByName(format!("{}-instance1", project_name)),
        )),
        Box::new(vpc1.clone()),
        // Test a resource nested two levels below Project
        Box::new(authz::VpcSubnet::new(
            vpc1,
            make_uuid(),
            LookupType::ByName(format!("{}-subnet1", vpc1_name)),
        )),
    ];

    (project, children)
}

async fn create_users<T>(
    opctx: &OpContext,
    datastore: &db::DataStore,
    resource_name: &str,
    silo_id: Uuid,
    authz_resource: &T,
    users: &mut Vec<(String, Uuid)>,
) where
    T: ApiResourceWithRolesType + Clone,
    T::AllowedRoles: IntoEnumIterator,
{
    for role in T::AllowedRoles::iter() {
        let role_name = role.to_database_string();
        let username = format!("{}-{}", resource_name, role_name);
        let user_id = make_uuid();
        println!("creating user: {}", &username);
        users.push((username, user_id));

        let silo_user = db::model::SiloUser::new(silo_id, user_id);
        datastore
            .silo_user_create(silo_user)
            .await
            .expect("failed to create silo user");

        let old_role_assignments = datastore
            .role_assignment_fetch_visible(opctx, authz_resource)
            .await
            .expect("fetching policy");
        let new_role_assignments = old_role_assignments
            .into_iter()
            .map(|r| r.try_into().unwrap())
            .chain(std::iter::once(shared::RoleAssignment {
                identity_type: shared::IdentityType::SiloUser,
                identity_id: user_id,
                role_name: role,
            }))
            .collect::<Vec<_>>();
        datastore
            .role_assignment_replace_visible(
                opctx,
                authz_resource,
                &new_role_assignments,
            )
            .await
            .expect("failed to assign role");
    }
}

fn action_abbreviation(action: authz::Action) -> &'static str {
    match action {
        authz::Action::Query => "Q",
        authz::Action::Read => "R",
        authz::Action::ListChildren => "LC",
        authz::Action::ReadPolicy => "RP",
        authz::Action::Modify => "M",
        authz::Action::ModifyPolicy => "MP",
        authz::Action::CreateChild => "CC",
        authz::Action::Delete => "D",
        authz::Action::ListIdentityProviders => "LP",
    }
}

fn resource_name(authz_resource: &dyn Authorizable) -> String {
    let my_ident = match authz_resource.lookup_type() {
        LookupType::ByName(name) => format!("{:?}", name),
        LookupType::ById(id) => format!("id {:?}", id.to_string()),
        LookupType::BySessionToken(_) | LookupType::ByCompositeId(_) => {
            unimplemented!()
        }
    };

    format!("{:?} {}", authz_resource.resource_type(), my_ident)
}

/// `Write` impl that writes everything it's given to both a destination `Write`
/// and stdout via `print!`.
///
/// It'd be nice if this were instead a generic `Tee` that took an arbitrary
/// number of `Write`s and wrote data to all of them.  That's possible, but it
/// wouldn't do what we want.  We need to use `print!` in order for output to be
/// captured by the test runner.  See rust-lang/rust#12309.
struct StdoutTee<W> {
    sink: W,
}

impl<W: Write> StdoutTee<W> {
    fn new(sink: W) -> StdoutTee<W> {
        StdoutTee { sink }
    }
}

impl<W: Write> Write for StdoutTee<W> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.sink.write_str(s)?;
        print!("{}", s);
        Ok(())
    }
}
