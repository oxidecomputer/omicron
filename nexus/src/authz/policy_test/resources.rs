// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Structures and functions related to the resources created for the IAM role
//! policy test

use super::coverage::Coverage;
use super::make_uuid;
use crate::authz;
use crate::authz::AuthorizedResource;
use crate::context::OpContext;
use authz::ApiResource;
use futures::future::BoxFuture;
use futures::FutureExt;
use lazy_static::lazy_static;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use oso::PolarClass;
use std::sync::Arc;
use uuid::Uuid;

lazy_static! {
    pub static ref SILO1_ID: Uuid = make_uuid();
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
pub struct Resources {
    // XXX-dap
    //all_resources: Vec<Arc<dyn Authorizable>>,
    //role_targets: Vec<Arc<dyn ApiResourceWithRoles>>,
    pub silo1: authz::Silo,
    pub silo1_org1: authz::Organization,
    pub silo1_org1_proj1: authz::Project,
    pub silo1_org1_proj1_children: Vec<Arc<dyn Authorizable>>,
    pub silo1_org1_proj2: authz::Project,
    pub silo1_org1_proj2_children: Vec<Arc<dyn Authorizable>>,
    pub silo1_org2: authz::Organization,
    pub silo1_org2_proj1: authz::Project,
    pub silo1_org2_proj1_children: Vec<Arc<dyn Authorizable>>,
    pub silo2: authz::Silo,
    pub silo2_org1: authz::Organization,
    pub silo2_org1_proj1: authz::Project,
    pub silo2_org1_proj1_children: Vec<Arc<dyn Authorizable>>,
}

impl Resources {
    pub fn all_resources(
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

pub trait Authorizable: AuthorizedResource + std::fmt::Debug {
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

pub fn make_resources(coverage: &mut Coverage) -> Resources {
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
