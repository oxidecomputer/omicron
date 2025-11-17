// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database test helpers. These are wrappers to make testing more compact.

use crate::authz;
use crate::context::OpContext;
use crate::db::DataStore;
use nexus_db_lookup::LookupPath;

use anyhow::Result;
use chrono::Utc;
use nexus_db_model::AffinityGroup;
use nexus_db_model::AntiAffinityGroup;
use nexus_db_model::BlockSize;
use nexus_db_model::ByteCount;
use nexus_db_model::Generation;
use nexus_db_model::Image;
use nexus_db_model::Instance;
use nexus_db_model::InstanceRuntimeState;
use nexus_db_model::InstanceState;
use nexus_db_model::Project;
use nexus_db_model::ProjectImage;
use nexus_db_model::ProjectImageIdentity;
use nexus_db_model::Resources;
use nexus_db_model::SledBaseboard;
use nexus_db_model::SledCpuFamily;
use nexus_db_model::SledSystemHardware;
use nexus_db_model::SledUpdate;
use nexus_db_model::Snapshot;
use nexus_db_model::SnapshotIdentity;
use nexus_db_model::SnapshotState;
use nexus_db_model::Vmm;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use omicron_common::api::external::{
    TufArtifactMeta, TufRepoDescription, TufRepoMeta,
};
use omicron_common::update::ArtifactId;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::TufRepoUuid;
use omicron_uuid_kinds::VolumeUuid;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::str::FromStr;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::{ArtifactKind, ArtifactVersion};
use uuid::Uuid;

/// Creates a project within the silo of "opctx".
pub async fn create_project(
    opctx: &OpContext,
    datastore: &DataStore,
    name: &'static str,
) -> (authz::Project, Project) {
    let authz_silo = opctx.authn.silo_required().unwrap();

    let project = Project::new(
        authz_silo.id(),
        params::ProjectCreate {
            identity: external::IdentityMetadataCreateParams {
                name: name.parse().unwrap(),
                description: "desc".to_string(),
            },
        },
    );
    datastore.project_create(&opctx, project).await.unwrap()
}

/// Creates a "fake" [`SledBaseboard`]
pub fn sled_baseboard_for_test() -> SledBaseboard {
    SledBaseboard {
        serial_number: Uuid::new_v4().to_string(),
        part_number: String::from("test-part"),
        revision: 1,
    }
}

/// A utility for creating a [`SledSystemHardware`]
pub struct SledSystemHardwareBuilder {
    is_scrimlet: bool,
    usable_hardware_threads: u32,
    usable_physical_ram: i64,
    reservoir_size: i64,
    cpu_family: SledCpuFamily,
}

impl Default for SledSystemHardwareBuilder {
    fn default() -> Self {
        Self {
            is_scrimlet: false,
            usable_hardware_threads: 4,
            usable_physical_ram: 1 << 40,
            reservoir_size: 1 << 39,
            cpu_family: SledCpuFamily::AmdMilan,
        }
    }
}

impl SledSystemHardwareBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_scrimlet(&mut self, is_scrimlet: bool) -> &mut Self {
        self.is_scrimlet = is_scrimlet;
        self
    }

    pub fn usable_hardware_threads(
        &mut self,
        usable_hardware_threads: u32,
    ) -> &mut Self {
        self.usable_hardware_threads = usable_hardware_threads;
        self
    }

    pub fn usable_physical_ram(
        &mut self,
        usable_physical_ram: i64,
    ) -> &mut Self {
        self.usable_physical_ram = usable_physical_ram;
        self
    }

    pub fn reservoir_size(&mut self, reservoir_size: i64) -> &mut Self {
        self.reservoir_size = reservoir_size;
        self
    }

    pub fn cpu_family(&mut self, family: SledCpuFamily) -> &mut Self {
        self.cpu_family = family;
        self
    }

    pub fn build(&self) -> SledSystemHardware {
        SledSystemHardware {
            is_scrimlet: self.is_scrimlet,
            usable_hardware_threads: self.usable_hardware_threads,
            usable_physical_ram: self.usable_physical_ram.try_into().unwrap(),
            reservoir_size: self.reservoir_size.try_into().unwrap(),
            cpu_family: self.cpu_family,
        }
    }
}

/// A utility for creating a [`SledUpdate`].
pub struct SledUpdateBuilder {
    sled_id: SledUuid,
    addr: SocketAddrV6,
    repo_depot_port: u16,
    rack_id: Uuid,
    sled_hardware: SledSystemHardwareBuilder,
}

impl Default for SledUpdateBuilder {
    fn default() -> Self {
        Self {
            sled_id: SledUuid::new_v4(),
            addr: SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
            repo_depot_port: 0,
            rack_id: Uuid::new_v4(),
            sled_hardware: SledSystemHardwareBuilder::default(),
        }
    }
}

impl SledUpdateBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sled_id(&mut self, sled_id: SledUuid) -> &mut Self {
        self.sled_id = sled_id;
        self
    }

    pub fn addr(&mut self, addr: SocketAddrV6) -> &mut Self {
        self.addr = addr;
        self
    }

    pub fn repo_depot_port(&mut self, repo_depot_port: u16) -> &mut Self {
        self.repo_depot_port = repo_depot_port;
        self
    }

    pub fn rack_id(&mut self, rack_id: Uuid) -> &mut Self {
        self.rack_id = rack_id;
        self
    }

    pub fn hardware(&mut self) -> &mut SledSystemHardwareBuilder {
        &mut self.sled_hardware
    }

    pub fn build(&self) -> SledUpdate {
        SledUpdate::new(
            self.sled_id,
            self.addr,
            self.repo_depot_port,
            sled_baseboard_for_test(),
            self.sled_hardware.build(),
            self.rack_id,
            Generation::new(),
        )
    }
}

pub fn small_resource_request() -> Resources {
    Resources::new(
        1,
        // Just require the bare non-zero amount of RAM.
        ByteCount::try_from(1024).unwrap(),
        ByteCount::try_from(1024).unwrap(),
    )
}

/// Helper function for creating an instance without a VMM.
pub async fn create_stopped_instance_record(
    opctx: &OpContext,
    datastore: &DataStore,
    authz_project: &authz::Project,
    name: &str,
) -> InstanceUuid {
    let instance = Instance::new(
        InstanceUuid::new_v4(),
        authz_project.id(),
        &params::InstanceCreate {
            identity: external::IdentityMetadataCreateParams {
                name: name.parse().unwrap(),
                description: "".to_string(),
            },
            ncpus: 2i64.try_into().unwrap(),
            memory: external::ByteCount::from_gibibytes_u32(16),
            hostname: "myhostname".try_into().unwrap(),
            user_data: Vec::new(),
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::None,
            external_ips: Vec::new(),
            disks: Vec::new(),
            boot_disk: None,
            cpu_platform: None,
            ssh_public_keys: None,
            start: false,
            auto_restart_policy: Default::default(),
            anti_affinity_groups: Vec::new(),
            multicast_groups: Vec::new(),
        },
    );

    let instance = datastore
        .project_create_instance(&opctx, &authz_project, instance)
        .await
        .unwrap();

    let id = InstanceUuid::from_untyped_uuid(instance.id());
    datastore
        .instance_update_runtime(
            &id,
            &InstanceRuntimeState {
                nexus_state: InstanceState::NoVmm,
                time_updated: Utc::now(),
                propolis_id: None,
                migration_id: None,
                dst_propolis_id: None,
                gen: Generation::from(Generation::new().0.next()),
                time_last_auto_restarted: None,
            },
        )
        .await
        .expect("Failed to update runtime state");

    id
}

pub async fn delete_instance_record(
    opctx: &OpContext,
    datastore: &DataStore,
    instance_id: InstanceUuid,
) {
    let (.., authz_instance) = LookupPath::new(opctx, datastore)
        .instance_id(instance_id.into_untyped_uuid())
        .lookup_for(authz::Action::Delete)
        .await
        .unwrap();
    datastore.project_delete_instance(&opctx, &authz_instance).await.unwrap();
}

pub async fn create_affinity_group(
    opctx: &OpContext,
    db: &DataStore,
    authz_project: &authz::Project,
    group_name: &'static str,
    policy: external::AffinityPolicy,
) -> AffinityGroup {
    db.affinity_group_create(
        &opctx,
        &authz_project,
        nexus_db_model::AffinityGroup::new(
            authz_project.id(),
            params::AffinityGroupCreate {
                identity: external::IdentityMetadataCreateParams {
                    name: group_name.parse().unwrap(),
                    description: "desc".to_string(),
                },
                policy,
                failure_domain: external::FailureDomain::Sled,
            },
        ),
    )
    .await
    .unwrap()
}

pub async fn delete_affinity_group(
    opctx: &OpContext,
    db: &DataStore,
    project_name: &'static str,
    group_name: &'static str,
) {
    let project = external::Name::from_str(project_name).unwrap();
    let group = external::Name::from_str(group_name).unwrap();
    let (.., authz_group) = LookupPath::new(opctx, db)
        .project_name_owned(project.into())
        .affinity_group_name_owned(group.into())
        .lookup_for(authz::Action::Delete)
        .await
        .unwrap();

    db.affinity_group_delete(&opctx, &authz_group).await.unwrap();
}

pub async fn create_anti_affinity_group(
    opctx: &OpContext,
    db: &DataStore,
    authz_project: &authz::Project,
    group_name: &'static str,
    policy: external::AffinityPolicy,
) -> AntiAffinityGroup {
    db.anti_affinity_group_create(
        &opctx,
        &authz_project,
        nexus_db_model::AntiAffinityGroup::new(
            authz_project.id(),
            params::AntiAffinityGroupCreate {
                identity: external::IdentityMetadataCreateParams {
                    name: group_name.parse().unwrap(),
                    description: "desc".to_string(),
                },
                policy,
                failure_domain: external::FailureDomain::Sled,
            },
        ),
    )
    .await
    .unwrap()
}

pub async fn delete_anti_affinity_group(
    opctx: &OpContext,
    db: &DataStore,
    project_name: &'static str,
    group_name: &'static str,
) {
    let project = external::Name::from_str(project_name).unwrap();
    let group = external::Name::from_str(group_name).unwrap();
    let (.., authz_group) = LookupPath::new(opctx, db)
        .project_name_owned(project.into())
        .anti_affinity_group_name_owned(group.into())
        .lookup_for(authz::Action::Delete)
        .await
        .unwrap();

    db.anti_affinity_group_delete(&opctx, &authz_group).await.unwrap();
}

pub async fn create_affinity_group_member(
    opctx: &OpContext,
    db: &DataStore,
    project_name: &'static str,
    group_name: &'static str,
    instance_id: InstanceUuid,
) -> Result<()> {
    let project = external::Name::from_str(project_name).unwrap();
    let group = external::Name::from_str(group_name).unwrap();
    let (.., authz_group) = LookupPath::new(opctx, db)
        .project_name_owned(project.into())
        .affinity_group_name_owned(group.into())
        .lookup_for(authz::Action::Modify)
        .await?;

    db.affinity_group_member_instance_add(opctx, &authz_group, instance_id)
        .await?;
    Ok(())
}

pub async fn create_anti_affinity_group_member(
    opctx: &OpContext,
    db: &DataStore,
    project_name: &'static str,
    group_name: &'static str,
    instance_id: InstanceUuid,
) -> Result<()> {
    let project = external::Name::from_str(project_name).unwrap();
    let group = external::Name::from_str(group_name).unwrap();
    let (.., authz_group) = LookupPath::new(opctx, db)
        .project_name_owned(project.into())
        .anti_affinity_group_name_owned(group.into())
        .lookup_for(authz::Action::Modify)
        .await?;

    db.anti_affinity_group_member_instance_add(
        opctx,
        &authz_group,
        instance_id,
    )
    .await?;
    Ok(())
}

pub async fn create_project_snapshot(
    opctx: &OpContext,
    datastore: &DataStore,
    authz_project: &authz::Project,
    disk_id: Uuid,
    name: &str,
) -> Snapshot {
    datastore
        .project_ensure_snapshot(
            &opctx,
            &authz_project,
            Snapshot {
                identity: SnapshotIdentity {
                    id: Uuid::new_v4(),
                    name: external::Name::try_from(name.to_string())
                        .unwrap()
                        .into(),
                    description: "snapshot".into(),

                    time_created: Utc::now(),
                    time_modified: Utc::now(),
                    time_deleted: None,
                },

                project_id: authz_project.id(),
                disk_id,
                volume_id: VolumeUuid::new_v4().into(),
                destination_volume_id: VolumeUuid::new_v4().into(),

                gen: Generation::new(),
                state: SnapshotState::Creating,
                block_size: BlockSize::AdvancedFormat,

                size: external::ByteCount::from_gibibytes_u32(2).into(),
            },
        )
        .await
        .unwrap()
}

pub async fn create_project_image(
    opctx: &OpContext,
    datastore: &DataStore,
    authz_project: &authz::Project,
    name: &str,
) -> Image {
    let authz_silo = opctx.authn.silo_required().unwrap();

    datastore
        .project_image_create(
            &opctx,
            &authz_project,
            ProjectImage {
                identity: ProjectImageIdentity {
                    id: Uuid::new_v4(),
                    name: external::Name::try_from(name.to_string())
                        .unwrap()
                        .into(),
                    description: "description".into(),

                    time_created: Utc::now(),
                    time_modified: Utc::now(),
                    time_deleted: None,
                },

                silo_id: authz_silo.id(),
                project_id: authz_project.id(),
                volume_id: VolumeUuid::new_v4().into(),

                url: None,
                os: String::from("debian"),
                version: String::from("12"),
                digest: None,
                block_size: BlockSize::Iso,

                size: external::ByteCount::from_gibibytes_u32(1).into(),
            },
        )
        .await
        .unwrap()
}

/// Create a VMM record for testing.
pub async fn create_vmm_for_instance(
    opctx: &OpContext,
    datastore: &DataStore,
    instance_id: InstanceUuid,
    sled_id: SledUuid,
) -> PropolisUuid {
    let vmm_id = PropolisUuid::new_v4();
    let vmm = Vmm::new(
        vmm_id,
        instance_id,
        sled_id,
        "127.0.0.1".parse().unwrap(), // Test IP
        12400,                        // Test port
        nexus_db_model::VmmCpuPlatform::SledDefault, // Test CPU platform
    );
    datastore.vmm_insert(opctx, vmm).await.expect("Should create VMM");
    vmm_id
}

/// Update instance runtime to point to a VMM.
pub async fn attach_instance_to_vmm(
    opctx: &OpContext,
    datastore: &DataStore,
    authz_project: &authz::Project,
    instance_id: InstanceUuid,
    vmm_id: PropolisUuid,
) {
    // Fetch current instance to get generation
    let authz_instance = authz::Instance::new(
        authz_project.clone(),
        instance_id.into_untyped_uuid(),
        external::LookupType::ById(instance_id.into_untyped_uuid()),
    );
    let instance = datastore
        .instance_refetch(opctx, &authz_instance)
        .await
        .expect("Should fetch instance");

    datastore
        .instance_update_runtime(
            &instance_id,
            &InstanceRuntimeState {
                nexus_state: InstanceState::Vmm,
                propolis_id: Some(vmm_id.into_untyped_uuid()),
                dst_propolis_id: None,
                migration_id: None,
                gen: Generation::from(instance.runtime().gen.next()),
                time_updated: Utc::now(),
                time_last_auto_restarted: None,
            },
        )
        .await
        .expect("Should update instance runtime state");
}

/// Create an instance with an associated VMM (convenience function).
pub async fn create_instance_with_vmm(
    opctx: &OpContext,
    datastore: &DataStore,
    authz_project: &authz::Project,
    instance_name: &str,
    sled_id: SledUuid,
) -> (InstanceUuid, PropolisUuid) {
    let instance_id = create_stopped_instance_record(
        opctx,
        datastore,
        authz_project,
        instance_name,
    )
    .await;

    let vmm_id =
        create_vmm_for_instance(opctx, datastore, instance_id, sled_id).await;

    attach_instance_to_vmm(
        opctx,
        datastore,
        authz_project,
        instance_id,
        vmm_id,
    )
    .await;

    (instance_id, vmm_id)
}

pub async fn insert_test_tuf_repo(
    opctx: &OpContext,
    datastore: &DataStore,
    version: u32,
) -> TufRepoUuid {
    let repo = make_test_repo(version);
    datastore
        .tuf_repo_insert(opctx, &repo)
        .await
        .expect("inserting repo")
        .recorded
        .repo
        .id()
}

fn make_test_repo(version: u32) -> TufRepoDescription {
    // We just need a unique hash for each repo.  We'll key it on the
    // version for determinism.
    let version_bytes = version.to_le_bytes();
    let hash_bytes: [u8; 32] = std::array::from_fn(|i| version_bytes[i % 4]);
    let hash = ArtifactHash(hash_bytes);
    let version = semver::Version::new(u64::from(version), 0, 0);
    let artifact_version = ArtifactVersion::new(version.to_string())
        .expect("valid artifact version");
    TufRepoDescription {
        repo: TufRepoMeta {
            hash,
            targets_role_version: 0,
            valid_until: chrono::Utc::now(),
            system_version: version,
            file_name: String::new(),
        },
        artifacts: vec![TufArtifactMeta {
            id: ArtifactId {
                name: String::new(),
                version: artifact_version,
                kind: ArtifactKind::from_static("empty"),
            },
            hash,
            size: 0,
            board: None,
            sign: None,
        }],
    }
}
