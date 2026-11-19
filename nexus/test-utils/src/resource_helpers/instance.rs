// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::resource_helpers::{object_create, object_create_error};

use dropshot::HttpErrorResponseBody;
use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use nexus_types::external_api::instance;
use nexus_types::external_api::instance::{
    InstanceAutoRestartPolicy, InstanceCpuCount,
};
use nexus_types::external_api::multicast;
use nexus_types_versions::latest::instance::Instance;
use nexus_types_versions::latest::instance::InstanceCpuPlatform;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::Nullable;

pub struct InstanceCreateBuilder {
    interim_body: instance::InstanceCreate,
}
impl InstanceCreateBuilder {
    pub fn new(instance_name: &str) -> Self {
        let interim_body = instance::InstanceCreate {
            identity: IdentityMetadataCreateParams {
                name: instance_name.parse().unwrap(),
                description: format!("instance {:?}", instance_name),
            },
            ncpus: InstanceCpuCount(4),
            memory: ByteCount::from_gibibytes_u32(1),
            hostname: "the-host".parse().unwrap(),
            user_data:
                b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                    .to_vec(),
            ssh_public_keys: Some(Vec::new()),
            network_interfaces:
                instance::InstanceNetworkInterfaceAttachment::DefaultIpv4,
            external_ips: Vec::new(),
            disks: Vec::new(),
            boot_disk: None,
            cpu_platform: None,
            start: true,
            auto_restart_policy: None,
            anti_affinity_groups: Vec::new(),
            multicast_groups: Vec::new(),
            enable_jumbo_frames: false,
        };

        Self { interim_body }
    }

    pub fn with_description(mut self, description: &str) -> Self {
        self.interim_body.identity.description = description.to_string();
        self
    }

    pub fn with_ncpus(mut self, ncpus: InstanceCpuCount) -> Self {
        self.interim_body.ncpus = ncpus;
        self
    }

    pub fn with_memory(mut self, memory: ByteCount) -> Self {
        self.interim_body.memory = memory;
        self
    }

    pub fn with_hostname(mut self, hostname: &str) -> Self {
        self.interim_body.hostname = hostname.parse().unwrap();
        self
    }

    pub fn with_user_data(mut self, user_data: &[u8]) -> Self {
        self.interim_body.user_data = user_data.to_vec();
        self
    }

    pub fn with_ssh_public_keys(
        mut self,
        ssh_public_keys: Option<Vec<NameOrId>>,
    ) -> Self {
        self.interim_body.ssh_public_keys = ssh_public_keys;
        self
    }

    pub fn with_nics(
        mut self,
        nics: instance::InstanceNetworkInterfaceAttachment,
    ) -> Self {
        self.interim_body.network_interfaces = nics;
        self
    }

    pub fn with_disks(
        mut self,
        disks: Vec<instance::InstanceDiskAttachment>,
    ) -> Self {
        self.interim_body.disks = disks;
        self
    }

    pub fn with_boot_disk(
        mut self,
        maybe_disk: Option<instance::InstanceDiskAttachment>,
    ) -> Self {
        self.interim_body.boot_disk = maybe_disk;
        self
    }

    pub fn with_external_ips(
        mut self,
        external_ips: Vec<instance::ExternalIpCreate>,
    ) -> Self {
        self.interim_body.external_ips = external_ips;
        self
    }

    pub fn with_start(mut self, start: bool) -> Self {
        self.interim_body.start = start;
        self
    }

    pub fn with_auto_restart_policy(
        mut self,
        auto_restart_policy: Option<InstanceAutoRestartPolicy>,
    ) -> Self {
        self.interim_body.auto_restart_policy = auto_restart_policy;
        self
    }

    pub fn with_cpu_platform(
        mut self,
        cpu_platform: Option<InstanceCpuPlatform>,
    ) -> Self {
        self.interim_body.cpu_platform = cpu_platform;
        self
    }

    pub fn with_anti_affinity_groups(
        mut self,
        anti_affinity_groups: Vec<NameOrId>,
    ) -> Self {
        self.interim_body.anti_affinity_groups = anti_affinity_groups;
        self
    }

    pub fn with_multicast_groups(
        mut self,
        multicast_groups: Vec<multicast::MulticastGroupJoinSpec>,
    ) -> Self {
        self.interim_body.multicast_groups = multicast_groups;
        self
    }

    pub fn with_enable_jumbo_frames(
        mut self,
        enable_jumbo_frames: bool,
    ) -> Self {
        self.interim_body.enable_jumbo_frames = enable_jumbo_frames;
        self
    }

    pub fn finish(self) -> instance::InstanceCreate {
        self.interim_body
    }
}

/// Creates an instance with all the settings set to "reasonable defaults". This
/// is mostly useful when a test needs an instance to exist for a test, but is
/// not opinionated about the properties of that specific instance.
///
/// The one instance setting which is well-defined by this helper is that the
/// instance is created with `start: true`.
pub async fn create_instance(
    client: &ClientTestContext,
    project_name: &str,
    instance_name: &str,
) -> Instance {
    let builder = InstanceCreateBuilder::new(instance_name).with_start(true);

    create_instance_from_body(client, project_name, builder.finish()).await
}

/// The same as [`create_instance()`], but with `start: false`.
///
/// This may be preferable when a test involves post-creation modification to an
/// instance, which would otherwise have to stop an instance to operate on it.
pub async fn create_instance_stopped(
    client: &ClientTestContext,
    project_name: &str,
    instance_name: &str,
) -> Instance {
    let builder = InstanceCreateBuilder::new(instance_name).with_start(false);

    create_instance_from_body(client, project_name, builder.finish()).await
}

/// Creates an instance with provided `InstanceCreate` payload.
///
/// Callers are expected to use [`InstanceCreateBuilder`] to specify what
/// properties of an instance are relevant to their tests, and leave unrelated
/// properties up to the default test instance settings.
pub async fn create_instance_from_body(
    client: &ClientTestContext,
    project_name: &str,
    create_body: instance::InstanceCreate,
) -> Instance {
    let url = format!("/v1/instances?project={}", project_name);

    object_create(client, &url, &create_body).await
}

/// Creates an instance, asserting a status code and returning the error.
///
/// Note that this accepts any serializable body, which allows users to create
/// invalid inputs to test our parameter validation.
pub async fn create_instance_with_error<T>(
    client: &ClientTestContext,
    project_name: &str,
    body: &T,
    status: StatusCode,
) -> HttpErrorResponseBody
where
    T: serde::Serialize,
{
    let url = format!("/v1/instances?project={project_name}");
    object_create_error(client, &url, body, status).await
}

/// Helper to compute and modifiy a no-op update on the provided `instance`.
pub fn instance_compute_update(
    instance: &Instance,
    edit_update: impl Fn(&mut instance::InstanceUpdate),
) -> instance::InstanceUpdate {
    // Construct an update which, if used to reconfigure `Instance`, would result
    // in no changes.
    let unchanged_boot_disk = instance.boot_disk_id.map(|id| NameOrId::Id(id));
    let mut update = instance::InstanceUpdate {
        boot_disk: Nullable(unchanged_boot_disk),
        auto_restart_policy: Nullable(instance.auto_restart_status.policy),
        cpu_platform: Nullable(instance.cpu_platform),
        ncpus: instance.ncpus,
        memory: instance.memory,
        // "If not provided, the instance's multicast group membership will not
        // be changed."
        multicast_groups: None,
        enable_jumbo_frames: instance.enable_jumbo_frames,
    };

    edit_update(&mut update);

    update
}
