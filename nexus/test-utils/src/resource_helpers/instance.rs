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

    pub fn with_multicast_groups(
        mut self,
        multicast_groups: Vec<multicast::MulticastGroupJoinSpec>,
    ) -> Self {
        self.interim_body.multicast_groups = multicast_groups;
        self
    }

    pub fn finish(self) -> instance::InstanceCreate {
        self.interim_body
    }
}

/// Creates an instance with an IPv4 NIC, no disks, and other settings at
/// defaults that most tests won't be concerned about.
///
/// Wrapper around [`create_instance`].
pub async fn create_instance(
    client: &ClientTestContext,
    project_name: &str,
    instance_name: &str,
) -> Instance {
    let builder = InstanceCreateBuilder::new(instance_name);

    create_instance_from_body(client, project_name, builder.finish()).await
}

/// Creates an instance with associated configuration.
///
/// TODO: this might be removable in favor of InstanceCreateBuilder.
// I know, Clippy. I don't like it either...
#[allow(clippy::too_many_arguments)]
pub async fn create_instance_with(
    client: &ClientTestContext,
    project_name: &str,
    instance_name: &str,
    nics: &instance::InstanceNetworkInterfaceAttachment,
    disks: Vec<instance::InstanceDiskAttachment>,
    external_ips: Vec<instance::ExternalIpCreate>,
    start: bool,
    auto_restart_policy: Option<InstanceAutoRestartPolicy>,
    cpu_platform: Option<InstanceCpuPlatform>,
    multicast_groups: Vec<multicast::MulticastGroupJoinSpec>,
) -> Instance {
    let body = InstanceCreateBuilder::new(instance_name)
        .with_nics(nics.clone())
        .with_disks(disks)
        .with_external_ips(external_ips)
        .with_start(start)
        .with_auto_restart_policy(auto_restart_policy)
        .with_cpu_platform(cpu_platform)
        .with_multicast_groups(multicast_groups)
        .finish();

    create_instance_from_body(client, project_name, body).await
}

/// Creates an instance with provided `InstanceCreate` payload.
///
/// See also [`InstanceCreateBuilder`] which can help construct test instance
/// settings.
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
