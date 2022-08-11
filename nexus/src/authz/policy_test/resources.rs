// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Concrete list of resources created for the IAM policy test

use super::resource_builder::ResourceBuilder;
use super::resource_builder::ResourceSet;
use crate::authz;
use omicron_common::api::external::LookupType;
use uuid::Uuid;

/// Assemble the set of resources that we'll test
// The main hierarchy looks like this:
//
//     fleet
//     fleet/s1
//     fleet/s1/o1
//     fleet/s1/o1/p1
//     fleet/s1/o1/p1/vpc1
//     fleet/s1/o1/p2
//     fleet/s1/o1/p2/vpc1
//     fleet/s1/o2
//     fleet/s1/o2/p1
//     fleet/s1/o2/p1/vpc1
//     fleet/s2
//     fleet/s2/o1
//     fleet/s2/o1/p1
//     fleet/s2/o1/p1/vpc1
//
// For one branch of the hierarchy, for each resource that supports roles, for
// each supported role, we will create one user with that role on that resource.
// Concretely, we'll create users like fleet-admin, silo1-admin,
// silo1-org1-viewer, silo1-org1-proj1-viewer, etc.  This is enough to check
// what privileges are granted by that role (i.e., privileges on that resource)
// as well as verify that those privileges are _not_ granted on resources in the
// other branches. We don't need to explicitly create users to test silo2 or
// silo1-org2 or silo1-org1-proj2 (for examples) because those cases are
// identical.
//
// IF YOU WANT TO ADD A NEW RESOURCE TO THIS TEST: the goal is to have this test
// show exactly what roles grant what permissions on your resource.  Generally,
// that means you'll need to create more than one instance of the resource, with
// different levels of access by different users.  This is probably easier than
// it sounds!
//
// - If your resource is NOT a collection, you only need to modify the function
//   that creates the parent collection to create an instance of your resource.
//   That's likely `make_project()`, `make_organization()`, `make_silo()`, etc.
//   If your resource is essentially a global singleton (like "Fleet"), you can
//   modify `make_resources()` directly.
//
// - If your resource is a collection, then you want to create a new function
//   similar to the other functions that make collections (`make_project()`,
//   `make_organization()`, etc.)  You'll likely need the `first_branch`
//   argument that says whether to create users and how many child hierarchies
//   to create.
pub async fn make_resources<'a>(
    mut builder: ResourceBuilder<'a>,
    main_silo_id: Uuid,
) -> ResourceSet {
    builder.new_resource(authz::DATABASE.clone());
    builder.new_resource_with_users(authz::FLEET.clone()).await;

    make_silo(&mut builder, "silo1", main_silo_id, true).await;
    make_silo(&mut builder, "silo2", Uuid::new_v4(), false).await;

    builder.build()
}

/// Helper for `make_resources()` that constructs a small Silo hierarchy
async fn make_silo(
    builder: &mut ResourceBuilder<'_>,
    silo_name: &str,
    silo_id: Uuid,
    first_branch: bool,
) {
    let silo1 = authz::Silo::new(
        authz::FLEET,
        silo_id,
        LookupType::ByName(silo_name.to_string()),
    );
    if first_branch {
        builder.new_resource_with_users(silo1.clone()).await;
    } else {
        builder.new_resource(silo1.clone());
    }

    let norganizations = if first_branch { 2 } else { 1 };
    for i in 0..norganizations {
        let organization_name = format!("{}-org{}", silo_name, i + 1);
        let org_first_branch = first_branch && i == 0;
        make_organization(
            builder,
            &silo1,
            &organization_name,
            org_first_branch,
        )
        .await;
    }
}

/// Helper for `make_resources()` that constructs a small Organization hierarchy
async fn make_organization(
    builder: &mut ResourceBuilder<'_>,
    silo: &authz::Silo,
    organization_name: &str,
    first_branch: bool,
) {
    let organization = authz::Organization::new(
        silo.clone(),
        Uuid::new_v4(),
        LookupType::ByName(organization_name.to_string()),
    );
    if first_branch {
        builder.new_resource_with_users(organization.clone()).await;
    } else {
        builder.new_resource(organization.clone());
    }

    let nprojects = if first_branch { 2 } else { 1 };
    for i in 0..nprojects {
        let project_name = format!("{}-proj{}", organization_name, i + 1);
        let create_project_users = first_branch && i == 0;
        make_project(
            builder,
            &organization,
            &project_name,
            create_project_users,
        )
        .await;
    }
}

/// Helper for `make_resources()` that constructs a small Project hierarchy
async fn make_project(
    builder: &mut ResourceBuilder<'_>,
    organization: &authz::Organization,
    project_name: &str,
    first_branch: bool,
) {
    let project = authz::Project::new(
        organization.clone(),
        Uuid::new_v4(),
        LookupType::ByName(project_name.to_string()),
    );
    if first_branch {
        builder.new_resource_with_users(project.clone()).await;
    } else {
        builder.new_resource(project.clone());
    }

    let vpc1_name = format!("{}-vpc1", project_name);
    let vpc1 = authz::Vpc::new(
        project.clone(),
        Uuid::new_v4(),
        LookupType::ByName(vpc1_name.clone()),
    );

    // XXX-dap TODO-coverage add more different kinds of children
    builder.new_resource(authz::Disk::new(
        project.clone(),
        Uuid::new_v4(),
        LookupType::ByName(format!("{}-disk1", project_name)),
    ));
    builder.new_resource(authz::Instance::new(
        project.clone(),
        Uuid::new_v4(),
        LookupType::ByName(format!("{}-instance1", project_name)),
    ));
    builder.new_resource(vpc1.clone());
    // Test a resource nested two levels below Project
    builder.new_resource(authz::VpcSubnet::new(
        vpc1,
        Uuid::new_v4(),
        LookupType::ByName(format!("{}-subnet1", vpc1_name)),
    ));
}
