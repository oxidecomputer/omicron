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
use futures::StreamExt;
use nexus_test_utils::db::test_setup_database;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_test_utils::dev;
use oso::PolarClass;
use std::collections::BTreeSet;
use std::io::Cursor;
use std::io::Write;
use std::sync::Arc;
use strum::IntoEnumIterator;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread")]
async fn test_iam_roles_behavior() {
    let logctx = dev::test_setup_log("test_iam_roles");
    let mut db = test_setup_database(&logctx.log).await;
    let (opctx, datastore) = db::datastore::datastore_test(&logctx, &db).await;

    let mut coverage = Coverage::new(&logctx.log);
    let test_resources = make_resources(&mut coverage);
    coverage.verify();

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
    let mut user_contexts: Vec<Arc<(String, OpContext)>> = users
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

            Arc::new((username.clone(), opctx))
        })
        .collect();

    // Create and test an unauthenticated OpContext as well.
    //
    // We could test the "test-privileged" and "test-unprivileged" users, but it
    // wouldn't be very interesting: they're in a different Silo than the
    // resources that we're checking against so even "test-privileged" won't
    // have privileges here.   Anyway, they're composed of ordinary role
    // assignments so they're just a special case of what we're already testing.
    let user_log = logctx.log.new(o!(
        "username" => "unauthenticated",
    ));
    user_contexts.push(Arc::new((
        String::from("unauthenticated"),
        OpContext::for_background(
            user_log,
            Arc::clone(&authz),
            authn::Context::internal_unauthenticated(),
            Arc::clone(&datastore),
        ),
    )));

    let mut buffer = Vec::new();
    {
        let mut out = StdoutTee::new(&mut buffer);
        run_test_operations(
            &mut out,
            &logctx.log,
            &user_contexts,
            &test_resources,
        )
        .await
        .unwrap();
    }

    expectorate::assert_contents(
        "tests/output/authz-roles.out",
        &std::str::from_utf8(buffer.as_ref()).expect("non-UTF8 output"),
    );

    db.cleanup().await.unwrap();
    logctx.cleanup_successful();
}

struct Coverage {
    log: slog::Logger,
    class_names: BTreeSet<String>,
    exempted: BTreeSet<String>,
    covered: BTreeSet<String>,
}

impl Coverage {
    fn new(log: &slog::Logger) -> Coverage {
        let log = log.new(o!("component" => "IamTestCoverage"));
        let authz = authz::Authz::new(&log);
        let class_names = authz.into_class_names();

        // Class names should be added to this exemption list when their Polar
        // code snippets and authz behavior is identical to another class.  This
        // is primarily for performance reasons because this test takes a long
        // time.  But with every exemption comes the risk of a security issue!
        //
        // PLEASE: instead of adding a class to this list, consider updating
        // this test to create an instance of the class and then test it.
        let exempted = [
            // Non-resources
            super::Action::get_polar_class(),
            super::actor::AnyActor::get_polar_class(),
            super::actor::AuthenticatedActor::get_polar_class(),
            // XXX-dap TODO-coverage Not yet implemented, but not exempted for a
            // good reason.
            super::IpPoolList::get_polar_class(),
            super::GlobalImageList::get_polar_class(),
            super::ConsoleSessionList::get_polar_class(),
            super::DeviceAuthRequestList::get_polar_class(),
            super::IpPool::get_polar_class(),
            super::NetworkInterface::get_polar_class(),
            super::VpcRouter::get_polar_class(),
            super::RouterRoute::get_polar_class(),
            super::ConsoleSession::get_polar_class(),
            super::DeviceAuthRequest::get_polar_class(),
            super::DeviceAccessToken::get_polar_class(),
            super::Rack::get_polar_class(),
            super::RoleBuiltin::get_polar_class(),
            super::SshKey::get_polar_class(),
            super::SiloUser::get_polar_class(),
            super::SiloGroup::get_polar_class(),
            super::IdentityProvider::get_polar_class(),
            super::SamlIdentityProvider::get_polar_class(),
            super::Sled::get_polar_class(),
            super::UpdateAvailableArtifact::get_polar_class(),
            super::UserBuiltin::get_polar_class(),
            super::GlobalImage::get_polar_class(),
        ]
        .into_iter()
        .map(|c| c.name.clone())
        .collect();

        Coverage { log, class_names, exempted, covered: BTreeSet::new() }
    }

    fn covered<T: oso::PolarClass>(&mut self, _covered: &T) {
        self.covered_class(T::get_polar_class())
    }

    fn covered_class(&mut self, class: oso::Class) {
        let class_name = class.name.clone();
        debug!(&self.log, "covering"; "class_name" => &class_name);
        self.covered.insert(class_name);
    }

    fn verify(&self) {
        let mut uncovered = Vec::new();
        let mut bad_exemptions = Vec::new();

        for class_name in &self.class_names {
            let class_name = class_name.as_str();
            let exempted = self.exempted.contains(class_name);
            let covered = self.covered.contains(class_name);

            match (exempted, covered) {
                (true, false) => {
                    // XXX-dap consider checking whether the Polar snippet
                    // exactly matches that of another class?
                    debug!(&self.log, "exempt"; "class_name" => class_name);
                }
                (false, true) => {
                    debug!(&self.log, "covered"; "class_name" => class_name);
                }
                (true, true) => {
                    error!(
                        &self.log,
                        "bad exemption (class was covered)";
                        "class_name" => class_name
                    );
                    bad_exemptions.push(class_name);
                }
                (false, false) => {
                    error!(
                        &self.log,
                        "uncovered class";
                        "class_name" => class_name
                    );
                    uncovered.push(class_name);
                }
            };
        }

        if !bad_exemptions.is_empty() {
            panic!(
                "these classes were covered by the tests and should \
                    not have been part of the exemption list: {}",
                bad_exemptions.join(", ")
            );
        }

        if !uncovered.is_empty() {
            panic!(
                "these classes were not covered by the IAM role \
                    policy test: {}",
                uncovered.join(", ")
            );
        }
    }
}

async fn run_test_operations<W: Write>(
    mut out: W,
    log: &slog::Logger,
    user_contexts: &[Arc<(String, OpContext)>],
    test_resources: &Resources,
) -> std::io::Result<()> {
    let mut futures = futures::stream::FuturesOrdered::new();

    // Run the per-resource tests in parallel.
    for resource in test_resources.all_resources() {
        let log = log.new(o!("resource" => format!("{:?}", resource)));
        futures.push(test_one_resource(
            log,
            user_contexts.to_owned(),
            Arc::clone(&resource),
        ));
    }

    let outputs: Vec<String> = futures.collect().await;
    for o in outputs {
        write!(out, "{}", o)?;
    }

    write!(out, "ACTIONS:\n\n")?;
    for action in authz::Action::iter() {
        write!(out, "  {:>2} = {:?}\n", action_abbreviation(action), action)?;
    }
    write!(out, "\n")?;

    Ok(())
}

async fn test_one_resource(
    log: slog::Logger,
    user_contexts: Vec<Arc<(String, OpContext)>>,
    resource: Arc<dyn Authorizable>,
) -> String {
    let task = tokio::spawn(async move {
        let mut buffer = Vec::new();
        let mut out = Cursor::new(&mut buffer);
        write!(out, "resource: {}\n\n", resource.resource_name())?;

        write!(out, "  {:31}", "USER")?;
        for action in authz::Action::iter() {
            write!(out, " {:>2}", action_abbreviation(action))?;
        }
        write!(out, "\n")?;

        for ctx_tuple in user_contexts.iter() {
            let (ref username, ref opctx) = **ctx_tuple;
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
                    Ok(_) => '\u{2714}',
                    Err(Error::Forbidden)
                    | Err(Error::ObjectNotFound { .. }) => '\u{2718}',
                    Err(Error::Unauthenticated { .. }) => '!',
                    Err(_) => '\u{26a0}',
                };
                write!(out, " {:>2}", summary)?;
            }
            write!(out, "\n")?;
        }

        write!(out, "\n")?;
        Ok(buffer)
    });

    let result: std::io::Result<Vec<u8>> =
        task.await.expect("failed to wait for task");
    let result_str = result.expect("failed to write to string buffer");
    String::from_utf8(result_str).expect("unexpected non-UTF8 output")
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
    silo1_org1_proj1_children: Vec<Arc<dyn Authorizable>>,
    silo1_org1_proj2: authz::Project,
    silo1_org1_proj2_children: Vec<Arc<dyn Authorizable>>,
    silo1_org2: authz::Organization,
    silo1_org2_proj1: authz::Project,
    silo1_org2_proj1_children: Vec<Arc<dyn Authorizable>>,
    silo2: authz::Silo,
    silo2_org1: authz::Organization,
    silo2_org1_proj1: authz::Project,
    silo2_org1_proj1_children: Vec<Arc<dyn Authorizable>>,
}

impl Resources {
    fn all_resources(
        &self,
    ) -> impl std::iter::Iterator<Item = Arc<dyn Authorizable>> + '_ {
        vec![
            Arc::new(authz::DATABASE.clone()) as Arc<dyn Authorizable>,
            Arc::new(authz::FLEET.clone()) as Arc<dyn Authorizable>,
            Arc::new(self.silo1.clone()) as Arc<dyn Authorizable>,
            Arc::new(self.silo1_org1.clone()) as Arc<dyn Authorizable>,
            Arc::new(self.silo1_org1_proj1.clone()) as Arc<dyn Authorizable>,
        ]
        .into_iter()
        .chain(self.silo1_org1_proj1_children.iter().cloned())
        .chain(std::iter::once(
            Arc::new(self.silo1_org1_proj2.clone()) as Arc<dyn Authorizable>
        ))
        .chain(self.silo1_org1_proj2_children.iter().cloned())
        .chain(vec![
            Arc::new(self.silo1_org2.clone()) as Arc<dyn Authorizable>,
            Arc::new(self.silo1_org2_proj1.clone()) as Arc<dyn Authorizable>,
        ])
        .chain(self.silo1_org2_proj1_children.iter().cloned())
        .chain(vec![
            Arc::new(self.silo2.clone()) as Arc<dyn Authorizable>,
            Arc::new(self.silo2_org1.clone()) as Arc<dyn Authorizable>,
            Arc::new(self.silo2_org1_proj1.clone()) as Arc<dyn Authorizable>,
        ])
        .chain(self.silo2_org1_proj1_children.iter().cloned())
    }
}

trait Authorizable: AuthorizedResource + std::fmt::Debug {
    fn resource_name(&self) -> String;

    fn do_authorize<'a, 'b>(
        &'a self,
        opctx: &'b OpContext,
        action: authz::Action,
    ) -> BoxFuture<'a, Result<(), Error>>
    where
        'b: 'a;

    fn polar_class(&self) -> oso::Class;
}

impl<T> Authorizable for T
where
    T: ApiResource + AuthorizedResource + oso::PolarClass + Clone,
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

    fn resource_name(&self) -> String {
        let my_ident = match self.lookup_type() {
            LookupType::ByName(name) => format!("{:?}", name),
            LookupType::ById(id) => format!("id {:?}", id.to_string()),
            LookupType::BySessionToken(_) | LookupType::ByCompositeId(_) => {
                unimplemented!()
            }
        };

        format!("{:?} {}", self.resource_type(), my_ident)
    }

    fn polar_class(&self) -> oso::Class {
        T::get_polar_class()
    }
}

impl Authorizable for authz::oso_generic::Database {
    fn resource_name(&self) -> String {
        String::from("DATABASE")
    }

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

    fn polar_class(&self) -> oso::Class {
        authz::oso_generic::Database::get_polar_class()
    }
}

// XXX-dap make this deterministic
// Most of the uuids here are hardcoded rather than randomly generated for
// debuggability.
fn make_uuid() -> Uuid {
    Uuid::new_v4()
}

fn make_resources(coverage: &mut Coverage) -> Resources {
    // "Database" and "Fleet" are implicitly included by virtue of being
    // returned by the iterator.
    coverage.covered(&authz::DATABASE);
    coverage.covered(&authz::FLEET);

    let silo1_id = make_uuid();
    let silo1 = authz::Silo::new(
        authz::FLEET,
        silo1_id,
        LookupType::ByName(String::from("silo1")),
    );
    coverage.covered(&silo1);

    let silo1_org1 = authz::Organization::new(
        silo1.clone(),
        make_uuid(),
        LookupType::ByName(String::from("silo1-org1")),
    );
    coverage.covered(&silo1_org1);
    let (silo1_org1_proj1, silo1_org1_proj1_children) =
        make_project(coverage, &silo1_org1, "silo1-org1-proj1");
    let (silo1_org1_proj2, silo1_org1_proj2_children) =
        make_project(coverage, &silo1_org1, "silo1-org1-proj2");

    let silo1_org2 = authz::Organization::new(
        silo1.clone(),
        make_uuid(),
        LookupType::ByName(String::from("silo1-org2")),
    );
    let (silo1_org2_proj1, silo1_org2_proj1_children) =
        make_project(coverage, &silo1_org2, "silo1-org2-proj1");

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
        make_project(coverage, &silo2_org1, "silo2-org1-proj1");

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
    coverage: &mut Coverage,
    organization: &authz::Organization,
    project_name: &str,
) -> (authz::Project, Vec<Arc<dyn Authorizable>>) {
    let project = authz::Project::new(
        organization.clone(),
        make_uuid(),
        LookupType::ByName(project_name.to_string()),
    );
    coverage.covered(&project);

    let vpc1_name = format!("{}-vpc1", project_name);
    let vpc1 = authz::Vpc::new(
        project.clone(),
        make_uuid(),
        LookupType::ByName(vpc1_name.clone()),
    );
    coverage.covered(&vpc1);
    let children: Vec<Arc<dyn Authorizable>> = vec![
        // XXX-dap TODO-coverage add more different kinds of children
        Arc::new(authz::Disk::new(
            project.clone(),
            make_uuid(),
            LookupType::ByName(format!("{}-disk1", project_name)),
        )),
        Arc::new(authz::Instance::new(
            project.clone(),
            make_uuid(),
            LookupType::ByName(format!("{}-instance1", project_name)),
        )),
        Arc::new(vpc1.clone()),
        // Test a resource nested two levels below Project
        Arc::new(authz::VpcSubnet::new(
            vpc1,
            make_uuid(),
            LookupType::ByName(format!("{}-subnet1", vpc1_name)),
        )),
    ];
    for c in &children {
        coverage.covered_class(c.polar_class());
    }

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
    T: ApiResourceWithRolesType + oso::PolarClass + Clone,
    T::AllowedRoles: IntoEnumIterator,
{
    for role in T::AllowedRoles::iter() {
        let role_name = role.to_database_string();
        let username = format!("{}-{}", resource_name, role_name);
        let user_id = make_uuid();
        println!("creating user: {}", &username);
        users.push((username.clone(), user_id));

        let silo_user = db::model::SiloUser::new(silo_id, user_id, username);
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
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        print!("{}", std::str::from_utf8(buf).expect("non-UTF8 in stdout tee"));
        self.sink.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.sink.flush()
    }
}
