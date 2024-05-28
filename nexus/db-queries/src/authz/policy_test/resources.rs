// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Concrete list of resources created for the IAM policy test

use super::resource_builder::ResourceBuilder;
use super::resource_builder::ResourceSet;
use crate::authz;
use crate::db::model::ArtifactId;
use nexus_db_model::SemverVersion;
use omicron_common::api::external::LookupType;
use omicron_uuid_kinds::GenericUuid;
use oso::PolarClass;
use std::collections::BTreeSet;
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
pub async fn make_resources(
    mut builder: ResourceBuilder<'_>,
    main_silo_id: Uuid,
) -> ResourceSet {
    // Global resources
    builder.new_resource(authz::DATABASE);
    builder.new_resource_with_users(authz::FLEET).await;
    builder.new_resource(authz::BLUEPRINT_CONFIG);
    builder.new_resource(authz::CONSOLE_SESSION_LIST);
    builder.new_resource(authz::DNS_CONFIG);
    builder.new_resource(authz::DEVICE_AUTH_REQUEST_LIST);
    builder.new_resource(authz::INVENTORY);
    builder.new_resource(authz::IP_POOL_LIST);

    // Silo/organization/project hierarchy
    make_silo(&mut builder, "silo1", main_silo_id, true).await;
    make_silo(&mut builder, "silo2", Uuid::new_v4(), false).await;

    // Various other resources
    let rack_id = "c037e882-8b6d-c8b5-bef4-97e848eb0a50".parse().unwrap();
    builder.new_resource(authz::Rack::new(
        authz::FLEET,
        rack_id,
        LookupType::ById(rack_id),
    ));

    let sled_id = "8a785566-adaf-c8d8-e886-bee7f9b73ca7".parse().unwrap();
    builder.new_resource(authz::Sled::new(
        authz::FLEET,
        sled_id,
        LookupType::ById(sled_id),
    ));

    let zpool_id = "aaaaaaaa-1233-af7d-9220-afe1d8090900".parse().unwrap();
    builder.new_resource(authz::Zpool::new(
        authz::FLEET,
        zpool_id,
        LookupType::ById(zpool_id),
    ));

    make_services(&mut builder).await;

    builder.new_resource(authz::PhysicalDisk::new(
        authz::FLEET,
        ("vendor".to_string(), "serial".to_string(), "model".to_string()),
        LookupType::ByCompositeId("vendor-serial-model".to_string()),
    ));

    let device_user_code = String::from("a-device-user-code");
    builder.new_resource(authz::DeviceAuthRequest::new(
        authz::FLEET,
        device_user_code.clone(),
        LookupType::ByName(device_user_code),
    ));

    let device_access_token = String::from("a-device-access-token");
    builder.new_resource(authz::DeviceAccessToken::new(
        authz::FLEET,
        device_access_token.clone(),
        LookupType::ByName(device_access_token),
    ));

    let blueprint_id = "b9e923f6-caf3-4c83-96f9-8ffe8c627dd2".parse().unwrap();
    builder.new_resource(authz::Blueprint::new(
        authz::FLEET,
        blueprint_id,
        LookupType::ById(blueprint_id),
    ));

    let tuf_repo_id = "3c52d72f-cbf7-4951-a62f-a4154e74da87".parse().unwrap();
    builder.new_resource(authz::TufRepo::new(
        authz::FLEET,
        tuf_repo_id,
        LookupType::ById(tuf_repo_id.into_untyped_uuid()),
    ));

    let artifact_id = ArtifactId {
        name: "a".to_owned(),
        version: SemverVersion("1.0.0".parse().unwrap()),
        kind: "b".to_owned(),
    };
    let artifact_id_desc = artifact_id.to_string();
    builder.new_resource(authz::TufArtifact::new(
        authz::FLEET,
        artifact_id,
        LookupType::ByCompositeId(artifact_id_desc),
    ));

    let address_lot_id =
        "43259fdc-c5c0-4a21-8b1d-2f673ad00d93".parse().unwrap();
    builder.new_resource(authz::AddressLot::new(
        authz::FLEET,
        address_lot_id,
        LookupType::ById(address_lot_id),
    ));

    let loopback_address_id =
        "9efbf1b1-16f9-45ab-864a-f7ebe501ae5b".parse().unwrap();
    builder.new_resource(authz::LoopbackAddress::new(
        authz::FLEET,
        loopback_address_id,
        LookupType::ById(loopback_address_id.into_untyped_uuid()),
    ));

    builder.build()
}

/// Helper for `make_resources()` that constructs some Services
async fn make_services(builder: &mut ResourceBuilder<'_>) {
    let nexus_service_id =
        "6b1f15ee-d6b3-424c-8436-94413a0b682d".parse().unwrap();
    builder.new_resource(authz::Service::new(
        authz::FLEET,
        nexus_service_id,
        LookupType::ById(nexus_service_id),
    ));

    let oximeter_service_id =
        "7f7bb301-5dc9-41f1-ab29-d369f4835079".parse().unwrap();
    builder.new_resource(authz::Service::new(
        authz::FLEET,
        oximeter_service_id,
        LookupType::ById(oximeter_service_id),
    ));
}

/// Helper for `make_resources()` that constructs a small Silo hierarchy
async fn make_silo(
    builder: &mut ResourceBuilder<'_>,
    silo_name: &str,
    silo_id: Uuid,
    first_branch: bool,
) {
    let silo = authz::Silo::new(
        authz::FLEET,
        silo_id,
        LookupType::ByName(silo_name.to_string()),
    );
    if first_branch {
        builder.new_resource_with_users(silo.clone()).await;
    } else {
        builder.new_resource(silo.clone());
    }

    builder.new_resource(authz::SiloCertificateList::new(silo.clone()));
    let certificate_id = Uuid::new_v4();
    builder.new_resource(authz::Certificate::new(
        silo.clone(),
        certificate_id,
        LookupType::ByName(format!("{}-certificate", silo_name)),
    ));

    builder.new_resource(authz::SiloIdentityProviderList::new(silo.clone()));
    let idp_id = Uuid::new_v4();
    builder.new_resource(authz::IdentityProvider::new(
        silo.clone(),
        idp_id,
        LookupType::ByName(format!("{}-identity-provider", silo_name)),
    ));
    builder.new_resource(authz::SamlIdentityProvider::new(
        silo.clone(),
        idp_id,
        LookupType::ByName(format!("{}-saml-identity-provider", silo_name)),
    ));

    builder.new_resource(authz::SiloUserList::new(silo.clone()));
    let silo_user_id = Uuid::new_v4();
    let silo_user = authz::SiloUser::new(
        silo.clone(),
        silo_user_id,
        LookupType::ByName(format!("{}-user", silo_name)),
    );
    builder.new_resource(silo_user.clone());
    let ssh_key_id = Uuid::new_v4();
    builder.new_resource(authz::SshKey::new(
        silo_user,
        ssh_key_id,
        LookupType::ByName(format!("{}-user-ssh-key", silo_name)),
    ));
    let silo_group_id = Uuid::new_v4();
    builder.new_resource(authz::SiloGroup::new(
        silo.clone(),
        silo_group_id,
        LookupType::ByName(format!("{}-group", silo_name)),
    ));
    let silo_image_id = Uuid::new_v4();
    builder.new_resource(authz::SiloImage::new(
        silo.clone(),
        silo_image_id,
        LookupType::ByName(format!("{}-silo-image", silo_name)),
    ));

    // Image is a special case in that this resource is technically just a
    // pass-through for `SiloImage` and `ProjectImage` resources.
    let image_id = Uuid::new_v4();
    builder.new_resource(authz::Image::new(
        silo.clone(),
        image_id,
        LookupType::ByName(format!("{}-image", silo_name)),
    ));

    let nprojects = if first_branch { 2 } else { 1 };
    for i in 0..nprojects {
        let project_name = format!("{}-proj{}", silo_name, i + 1);
        let create_project_users = first_branch && i == 0;
        make_project(builder, &silo, &project_name, create_project_users).await;
    }
}

/// Helper for `make_resources()` that constructs a small Project hierarchy
async fn make_project(
    builder: &mut ResourceBuilder<'_>,
    silo: &authz::Silo,
    project_name: &str,
    first_branch: bool,
) {
    let project = authz::Project::new(
        silo.clone(),
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

    let instance_name = format!("{}-instance1", project_name);
    let instance = authz::Instance::new(
        project.clone(),
        Uuid::new_v4(),
        LookupType::ByName(instance_name.clone()),
    );

    let disk_name = format!("{}-disk1", project_name);
    builder.new_resource(authz::Disk::new(
        project.clone(),
        Uuid::new_v4(),
        LookupType::ByName(disk_name.clone()),
    ));
    builder.new_resource(instance.clone());
    builder.new_resource(authz::InstanceNetworkInterface::new(
        instance,
        Uuid::new_v4(),
        LookupType::ByName(format!("{}-nic1", instance_name)),
    ));
    builder.new_resource(vpc1.clone());
    // Test a resource nested two levels below Project
    builder.new_resource(authz::VpcSubnet::new(
        vpc1,
        Uuid::new_v4(),
        LookupType::ByName(format!("{}-subnet1", vpc1_name)),
    ));

    builder.new_resource(authz::Snapshot::new(
        project.clone(),
        Uuid::new_v4(),
        LookupType::ByName(format!("{}-snapshot1", disk_name)),
    ));

    let image_name = format!("{}-image1", project_name);
    builder.new_resource(authz::ProjectImage::new(
        project.clone(),
        Uuid::new_v4(),
        LookupType::ByName(image_name),
    ));

    let floating_ip_name = format!("{project_name}-fip1");
    builder.new_resource(authz::FloatingIp::new(
        project.clone(),
        Uuid::new_v4(),
        LookupType::ByName(floating_ip_name),
    ));
}

/// Returns the set of authz classes exempted from the coverage test
pub fn exempted_authz_classes() -> BTreeSet<String> {
    // Exemption list for the coverage test
    //
    // There are two possible reasons for a resource to appear on this list:
    //
    // (1) because its behavior is identical to that of some other resource
    //     that we are testing (i.e., same Polar snippet and identical
    //     configuration for the authz type).  There aren't many examples of
    //     this today, but it might be reasonable to do this for resources
    //     that are indistinguishable to the authz subsystem (e.g., Disks,
    //     Instances, Vpcs, and other things nested directly below Project)
    //
    //     TODO-coverage It would be nice if we could verify that the Polar
    //     snippet and authz_resource! configuration were identical to that of
    //     an existing class.  Then it would be safer to exclude types that are
    //     truly duplicative of some other type.
    //
    // (2) because we have not yet gotten around to adding the type to this
    //     test.  We don't want to expand this list if we can avoid it!
    [
        // Non-resources:
        authz::Action::get_polar_class(),
        authz::actor::AnyActor::get_polar_class(),
        authz::actor::AuthenticatedActor::get_polar_class(),
        // Resources whose behavior should be identical to an existing type
        // and we don't want to do the test twice for performance reasons:
        // none yet.
        //
        // TODO-coverage Resources that we should test, but for which we
        // have not yet added a test.  PLEASE: instead of adding something
        // to this list, modify `make_resources()` to test it instead.  This
        // should be pretty straightforward in most cases.  Adding a new
        // class to this list makes it harder to catch security flaws!
        authz::IpPool::get_polar_class(),
        authz::VpcRouter::get_polar_class(),
        authz::RouterRoute::get_polar_class(),
        authz::ConsoleSession::get_polar_class(),
        authz::RoleBuiltin::get_polar_class(),
        authz::UserBuiltin::get_polar_class(),
    ]
    .into_iter()
    .map(|c| c.name)
    .collect()
}
