// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Params define the request bodies of API endpoints for creating or updating
//! resources.

use crate::external_api::shared;
use base64::Engine;
use chrono::{DateTime, Utc};
use http::Uri;
use omicron_common::api::external::{
    AddressLotKind, AffinityPolicy, AllowedSourceIps, BfdMode, BgpPeer,
    ByteCount, FailureDomain, Hostname, IdentityMetadataCreateParams,
    IdentityMetadataUpdateParams, InstanceAutoRestartPolicy, InstanceCpuCount,
    LinkFec, LinkSpeed, Name, NameOrId, Nullable, PaginationOrder,
    RouteDestination, RouteTarget, UserId,
};
use omicron_common::disk::DiskVariant;
use oxnet::{IpNet, Ipv4Net, Ipv6Net};
use parse_display::Display;
use schemars::JsonSchema;
use semver::Version;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, Visitor},
};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::{net::IpAddr, str::FromStr};
use url::Url;
use uuid::Uuid;

macro_rules! path_param {
    ($struct:ident, $param:ident, $name:tt) => {
        #[derive(Serialize, Deserialize, JsonSchema)]
        pub struct $struct {
            #[doc = "Name or ID of the "]
            #[doc = $name]
            pub $param: NameOrId,
        }
    };
}

macro_rules! id_path_param {
    ($struct:ident, $param:ident, $name:tt) => {
        #[derive(Serialize, Deserialize, JsonSchema)]
        pub struct $struct {
            #[doc = "ID of the "]
            #[doc = $name]
            pub $param: Uuid,
        }
    };
}

/// The unique hardware ID for a sled
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
)]
pub struct UninitializedSledId {
    pub serial: String,
    pub part: String,
}

path_param!(AffinityGroupPath, affinity_group, "affinity group");
path_param!(AntiAffinityGroupPath, anti_affinity_group, "anti affinity group");
path_param!(ProjectPath, project, "project");
path_param!(InstancePath, instance, "instance");
path_param!(NetworkInterfacePath, interface, "network interface");
path_param!(VpcPath, vpc, "VPC");
path_param!(SubnetPath, subnet, "subnet");
path_param!(RouterPath, router, "router");
path_param!(RoutePath, route, "route");
path_param!(InternetGatewayPath, gateway, "gateway");
path_param!(FloatingIpPath, floating_ip, "floating IP");
path_param!(DiskPath, disk, "disk");
path_param!(SnapshotPath, snapshot, "snapshot");
path_param!(ImagePath, image, "image");
path_param!(SiloPath, silo, "silo");
path_param!(ProviderPath, provider, "SAML identity provider");
path_param!(IpPoolPath, pool, "IP pool");
path_param!(IpAddressPath, address, "IP address");
path_param!(SshKeyPath, ssh_key, "SSH key");
path_param!(AddressLotPath, address_lot, "address lot");
path_param!(ProbePath, probe, "probe");
path_param!(CertificatePath, certificate, "certificate");

id_path_param!(SupportBundlePath, bundle_id, "support bundle");
id_path_param!(GroupPath, group_id, "group");
id_path_param!(UserPath, user_id, "user");
id_path_param!(TokenPath, token_id, "token");
id_path_param!(TufTrustRootPath, trust_root_id, "trust root");

// TODO: The hardware resources should be represented by its UUID or a hardware
// ID that can be used to deterministically generate the UUID.
id_path_param!(RackPath, rack_id, "rack");
id_path_param!(SledPath, sled_id, "sled");
id_path_param!(SwitchPath, switch_id, "switch");
id_path_param!(PhysicalDiskPath, disk_id, "physical disk");

// Internal API parameters
id_path_param!(BlueprintPath, blueprint_id, "blueprint");

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SledSelector {
    /// ID of the sled
    pub sled: Uuid,
}

/// Parameters for `sled_set_provision_policy`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SledProvisionPolicyParams {
    /// The provision state.
    pub state: super::views::SledProvisionPolicy,
}

/// Response to `sled_set_provision_policy`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SledProvisionPolicyResponse {
    /// The old provision state.
    pub old_state: super::views::SledProvisionPolicy,

    /// The new provision state.
    pub new_state: super::views::SledProvisionPolicy,
}

pub struct SwitchSelector {
    /// ID of the switch
    pub switch: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SiloSelector {
    /// Name or ID of the silo
    pub silo: NameOrId,
}

impl From<Name> for SiloSelector {
    fn from(name: Name) -> Self {
        SiloSelector { silo: name.into() }
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SupportBundleFilePath {
    #[serde(flatten)]
    pub bundle: SupportBundlePath,

    /// The file within the bundle to download
    pub file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SupportBundleCreate {
    /// User comment for the support bundle
    pub user_comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SupportBundleUpdate {
    /// User comment for the support bundle
    pub user_comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct OptionalSiloSelector {
    /// Name or ID of the silo
    pub silo: Option<NameOrId>,
}

/// Path parameters for Silo User requests
#[derive(Deserialize, JsonSchema)]
pub struct UserParam {
    /// The user's internal ID
    pub user_id: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SamlIdentityProviderSelector {
    /// Name or ID of the silo in which the SAML identity provider is associated
    pub silo: Option<NameOrId>,
    /// Name or ID of the SAML identity provider
    pub saml_identity_provider: NameOrId,
}

// The shape of this selector is slightly different than the others given that
// silos users can only be specified via ID and are automatically provided by
// the environment the user is authetnicated in
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SshKeySelector {
    /// ID of the silo user
    pub silo_user_id: Uuid,
    /// Name or ID of the SSH key
    pub ssh_key: NameOrId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ProjectSelector {
    /// Name or ID of the project
    pub project: NameOrId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct OptionalProjectSelector {
    /// Name or ID of the project
    pub project: Option<NameOrId>,
}

#[derive(Deserialize, JsonSchema, Clone)]
pub struct FloatingIpSelector {
    /// Name or ID of the project, only required if `floating_ip` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the Floating IP
    pub floating_ip: NameOrId,
}

#[derive(Deserialize, JsonSchema)]
pub struct DiskSelector {
    /// Name or ID of the project, only required if `disk` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the disk
    pub disk: NameOrId,
}

#[derive(Deserialize, JsonSchema)]
pub struct SnapshotSelector {
    /// Name or ID of the project, only required if `snapshot` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the snapshot
    pub snapshot: NameOrId,
}

#[derive(Deserialize, JsonSchema)]
pub struct ImageSelector {
    /// Name or ID of the project, only required if `image` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the image
    pub image: NameOrId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InstanceSelector {
    /// Name or ID of the project, only required if `instance` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the instance
    pub instance: NameOrId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct OptionalInstanceSelector {
    /// Name or ID of the project, only required if `instance` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the instance
    pub instance: Option<NameOrId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InstanceNetworkInterfaceSelector {
    /// Name or ID of the project, only required if `instance` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the instance, only required if `network_interface` is provided as a `Name`
    pub instance: Option<NameOrId>,
    /// Name or ID of the network interface
    pub network_interface: NameOrId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct VpcSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC
    pub vpc: NameOrId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct OptionalVpcSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC
    pub vpc: Option<NameOrId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InternetGatewayDeleteSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC
    pub vpc: Option<NameOrId>,
    /// Also delete routes targeting this gateway.
    #[serde(default)]
    pub cascade: bool,
}

#[derive(Deserialize, JsonSchema)]
pub struct SubnetSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `subnet` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the subnet
    pub subnet: NameOrId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct RouterSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `router` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the router
    pub router: NameOrId,
}

#[derive(Deserialize, JsonSchema)]
pub struct OptionalRouterSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `router` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the router
    pub router: Option<NameOrId>,
}

#[derive(Deserialize, JsonSchema)]
pub struct RouteSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `router` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the router, only required if `route` is provided as a `Name`
    pub router: Option<NameOrId>,
    /// Name or ID of the route
    pub route: NameOrId,
}

// Internet gateways

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InternetGatewaySelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `gateway` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the internet gateway
    pub gateway: NameOrId,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct OptionalInternetGatewaySelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `gateway` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the internet gateway
    pub gateway: Option<NameOrId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct DeleteInternetGatewayElementSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `gateway` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the internet gateway
    pub gateway: Option<NameOrId>,
    /// Also delete routes targeting this gateway element.
    #[serde(default)]
    pub cascade: bool,
}

#[derive(Deserialize, JsonSchema)]
pub struct InternetGatewayIpPoolSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `gateway` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the gateway, only required if `pool` is provided as a `Name`
    pub gateway: Option<NameOrId>,
    /// Name or ID of the pool
    pub pool: NameOrId,
}

#[derive(Deserialize, JsonSchema)]
pub struct InternetGatewayIpAddressSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `gateway` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the gateway, only required if `address` is provided as a `Name`
    pub gateway: Option<NameOrId>,
    /// Name or ID of the address
    pub address: NameOrId,
}

// Silos

/// Create-time parameters for a `Silo`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub discoverable: bool,

    pub identity_mode: shared::SiloIdentityMode,

    /// If set, this group will be created during Silo creation and granted the
    /// "Silo Admin" role. Identity providers can assert that users belong to
    /// this group and those users can log in and further initialize the Silo.
    ///
    /// Note that if configuring a SAML based identity provider,
    /// group_attribute_name must be set for users to be considered part of a
    /// group. See `SamlIdentityProviderCreate` for more information.
    pub admin_group_name: Option<String>,

    /// Initial TLS certificates to be used for the new Silo's console and API
    /// endpoints.  These should be valid for the Silo's DNS name(s).
    pub tls_certificates: Vec<CertificateCreate>,

    /// Limits the amount of provisionable CPU, memory, and storage in the Silo.
    /// CPU and memory are only consumed by running instances, while storage is
    /// consumed by any disk or snapshot. A value of 0 means that resource is
    /// *not* provisionable.
    pub quotas: SiloQuotasCreate,

    /// Mapping of which Fleet roles are conferred by each Silo role
    ///
    /// The default is that no Fleet roles are conferred by any Silo roles
    /// unless there's a corresponding entry in this map.
    #[serde(default)]
    pub mapped_fleet_roles:
        BTreeMap<shared::SiloRole, BTreeSet<shared::FleetRole>>,
}

/// The amount of provisionable resources for a Silo
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloQuotasCreate {
    /// The amount of virtual CPUs available for running instances in the Silo
    pub cpus: i64,
    /// The amount of RAM (in bytes) available for running instances in the Silo
    pub memory: ByteCount,
    /// The amount of storage (in bytes) available for disks or snapshots
    pub storage: ByteCount,
}

impl SiloQuotasCreate {
    /// All quotas set to 0
    pub fn empty() -> Self {
        Self {
            cpus: 0,
            memory: ByteCount::from(0),
            storage: ByteCount::from(0),
        }
    }

    /// An arbitrarily high but identifiable default for quotas
    /// that can be used for creating a Silo for testing
    ///
    /// The only silo that customers will see that this should be set on is the default
    /// silo. Ultimately the default silo should only be initialized with an empty quota,
    /// but as tests currently relying on it having a quota, we need to set something.
    pub fn arbitrarily_high_default() -> Self {
        Self {
            cpus: 9999999999,
            memory: ByteCount::try_from(999999999999999999_u64).unwrap(),
            storage: ByteCount::try_from(999999999999999999_u64).unwrap(),
        }
    }
}

// This conversion is mostly just useful for tests such that we can reuse
// empty() and arbitrarily_high_default() when testing utilization
impl From<SiloQuotasCreate> for super::views::VirtualResourceCounts {
    fn from(quota: SiloQuotasCreate) -> Self {
        Self { cpus: quota.cpus, memory: quota.memory, storage: quota.storage }
    }
}

/// Updateable properties of a Silo's resource limits.
/// If a value is omitted it will not be updated.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloQuotasUpdate {
    /// The amount of virtual CPUs available for running instances in the Silo
    pub cpus: Option<i64>,
    /// The amount of RAM (in bytes) available for running instances in the Silo
    pub memory: Option<ByteCount>,
    /// The amount of storage (in bytes) available for disks or snapshots
    pub storage: Option<ByteCount>,
}

// TODO: Unlike quota values, silo settings are nullable, so we need passing
// null to be meaningful here. But it's confusing for it to work that way here
// and differently for quotas. Maybe the best thing would be to make them all
// non-nullable on SiloQuotasUpdate. I vaguely remember the latter being the
// direction we wanted to go in general anyway. Can't find the issue where it
// was discussed.

/// Updateable properties of a silo's settings.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SiloAuthSettingsUpdate {
    /// Maximum lifetime of a device token in seconds. If set to null, users
    /// will be able to create tokens that do not expire.
    pub device_token_max_ttl_seconds: Nullable<NonZeroU32>,
}

/// Create-time parameters for a `User`
#[derive(Clone, Deserialize, JsonSchema)]
pub struct UserCreate {
    /// username used to log in
    pub external_id: UserId,
    /// how to set the user's login password
    pub password: UserPassword,
}

/// A password used for authenticating a local-only user
#[derive(Clone, Deserialize)]
#[serde(try_from = "String")]
// We store both the raw String and omicron_passwords::Password forms of the
// password.  That's because `omicron_passwords::Password` does not support
// getting the String back out (by design), but we may need to do that in order
// to impl Serialize.  See the `From<Password> for String` impl below.
pub struct Password(omicron_passwords::Password);

impl FromStr for Password {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Password::try_from(String::from(value))
    }
}

// Used to impl `Deserialize`
impl TryFrom<String> for Password {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let inner = omicron_passwords::Password::new(&value)
            .map_err(|e| format!("unsupported password: {:#}", e))?;
        // TODO-security If we want to apply password policy rules, this seems
        // like the place.  We presumably want to also document them in the
        // OpenAPI schema below.  See omicron#2307.
        Ok(Password(inner))
    }
}

impl JsonSchema for Password {
    fn schema_name() -> String {
        "Password".to_string()
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some(
                    "A password used to authenticate a user".to_string(),
                ),
                // TODO-doc If we apply password strength rules, they should
                // presumably be documented here.  See omicron#2307.
                description: Some(
                    "Passwords may be subject to additional constraints."
                        .to_string(),
                ),
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            string: Some(Box::new(schemars::schema::StringValidation {
                max_length: Some(
                    u32::try_from(omicron_passwords::MAX_PASSWORD_LENGTH)
                        .unwrap(),
                ),
                min_length: None,
                pattern: None,
            })),
            ..Default::default()
        }
        .into()
    }
}

impl AsRef<omicron_passwords::Password> for Password {
    fn as_ref(&self) -> &omicron_passwords::Password {
        &self.0
    }
}

/// Parameters for setting a user's password
#[derive(Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "mode", content = "value")]
pub enum UserPassword {
    /// Sets the user's password to the provided value
    Password(Password),
    /// Invalidates any current password (disabling password authentication)
    LoginDisallowed,
}

/// Credentials for local user login
#[derive(Clone, Deserialize, JsonSchema)]
pub struct UsernamePasswordCredentials {
    pub username: UserId,
    pub password: Password,
}

// Silo identity providers

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DerEncodedKeyPair {
    /// request signing public certificate (base64 encoded der file)
    #[serde(deserialize_with = "x509_cert_from_base64_encoded_der")]
    pub public_cert: String,

    /// request signing RSA private key in PKCS#1 format (base64 encoded der file)
    #[serde(deserialize_with = "key_from_base64_encoded_der")]
    pub private_key: String,
}

struct X509CertVisitor;

impl Visitor<'_> for X509CertVisitor {
    type Value = String;

    fn expecting(
        &self,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        formatter.write_str("a DER formatted X509 certificate as a string of base64 encoded bytes")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let raw_bytes = base64::engine::general_purpose::STANDARD
            .decode(&value.as_bytes())
            .map_err(|e| {
                de::Error::custom(format!(
                    "could not base64 decode public_cert: {}",
                    e
                ))
            })?;
        let _parsed =
            openssl::x509::X509::from_der(&raw_bytes).map_err(|e| {
                de::Error::custom(format!(
                    "public_cert is not recognized as a X509 certificate: {}",
                    e
                ))
            })?;

        Ok(value.to_string())
    }
}

fn x509_cert_from_base64_encoded_der<'de, D>(
    deserializer: D,
) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(X509CertVisitor)
}

struct KeyVisitor;

impl Visitor<'_> for KeyVisitor {
    type Value = String;

    fn expecting(
        &self,
        formatter: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        formatter.write_str(
            "a DER formatted key as a string of base64 encoded bytes",
        )
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let raw_bytes = base64::engine::general_purpose::STANDARD
            .decode(&value)
            .map_err(|e| {
                de::Error::custom(format!(
                    "could not base64 decode private_key: {}",
                    e
                ))
            })?;

        // TODO: samael does not support ECDSA, update to generic PKey type when it does
        //let _parsed = openssl::pkey::PKey::private_key_from_der(&raw_bytes)
        //    .map_err(|e| de::Error::custom(format!("could not base64 decode private_key: {}", e)))?;

        let parsed = openssl::rsa::Rsa::private_key_from_der(&raw_bytes)
            .map_err(|e| {
                de::Error::custom(format!(
                    "private_key is not recognized as a RSA private key: {}",
                    e
                ))
            })?;
        let _parsed = openssl::pkey::PKey::from_rsa(parsed).map_err(|e| {
            de::Error::custom(format!(
                "private_key is not recognized as a RSA private key: {}",
                e
            ))
        })?;

        Ok(value.to_string())
    }
}

fn key_from_base64_encoded_der<'de, D>(
    deserializer: D,
) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(KeyVisitor)
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IdpMetadataSource {
    Url { url: String },
    Base64EncodedXml { data: String },
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SamlIdentityProviderCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// the source of an identity provider metadata descriptor
    pub idp_metadata_source: IdpMetadataSource,

    /// idp's entity id
    pub idp_entity_id: String,

    /// sp's client id
    pub sp_client_id: String,

    /// service provider endpoint where the response will be sent
    pub acs_url: String,

    /// service provider endpoint where the idp should send log out requests
    pub slo_url: String,

    /// customer's technical contact for saml configuration
    pub technical_contact_email: String,

    /// request signing key pair
    #[serde(default)]
    #[serde(deserialize_with = "validate_key_pair")]
    pub signing_keypair: Option<DerEncodedKeyPair>,

    /// If set, SAML attributes with this name will be considered to denote a
    /// user's group membership, where the attribute value(s) should be a
    /// comma-separated list of group names.
    pub group_attribute_name: Option<String>,
}

/// sign some junk data and validate it with the key pair
fn sign_junk_data(key_pair: &DerEncodedKeyPair) -> Result<(), anyhow::Error> {
    let private_key = {
        let raw_bytes = base64::engine::general_purpose::STANDARD
            .decode(&key_pair.private_key)?;
        // TODO: samael does not support ECDSA, update to generic PKey type when it does
        //let parsed = openssl::pkey::PKey::private_key_from_der(&raw_bytes)?;
        let parsed = openssl::rsa::Rsa::private_key_from_der(&raw_bytes)?;
        let parsed = openssl::pkey::PKey::from_rsa(parsed)?;
        parsed
    };

    let public_key = {
        let raw_bytes = base64::engine::general_purpose::STANDARD
            .decode(&key_pair.public_cert)?;
        let parsed = openssl::x509::X509::from_der(&raw_bytes)?;
        parsed.public_key()?
    };

    let mut signer = openssl::sign::Signer::new(
        openssl::hash::MessageDigest::sha256(),
        &private_key.as_ref(),
    )?;

    let some_junk_data = b"this is some junk data";

    signer.update(some_junk_data)?;
    let signature = signer.sign_to_vec()?;

    let mut verifier = openssl::sign::Verifier::new(
        openssl::hash::MessageDigest::sha256(),
        &public_key,
    )?;

    verifier.update(some_junk_data)?;

    if !verifier.verify(&signature)? {
        anyhow::bail!("signature validation failed!");
    }

    Ok(())
}

fn validate_key_pair<'de, D>(
    deserializer: D,
) -> Result<Option<DerEncodedKeyPair>, D::Error>
where
    D: Deserializer<'de>,
{
    let v = Option::<DerEncodedKeyPair>::deserialize(deserializer)?;

    if let Some(ref key_pair) = v {
        if let Err(e) = sign_junk_data(&key_pair) {
            return Err(de::Error::custom(format!(
                "data signed with key not verified with certificate! {}",
                e
            )));
        }
    }

    Ok(v)
}

// AFFINITY GROUPS

/// Create-time parameters for an `AffinityGroup`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AffinityGroupCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub policy: AffinityPolicy,
    pub failure_domain: FailureDomain,
}

/// Updateable properties of an `AffinityGroup`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AffinityGroupUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct AffinityInstanceGroupMemberPath {
    pub affinity_group: NameOrId,
    pub instance: NameOrId,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct AntiAffinityInstanceGroupMemberPath {
    pub anti_affinity_group: NameOrId,
    pub instance: NameOrId,
}

/// Create-time parameters for an `AntiAffinityGroup`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AntiAffinityGroupCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub policy: AffinityPolicy,
    pub failure_domain: FailureDomain,
}

/// Updateable properties of an `AntiAffinityGroup`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AntiAffinityGroupUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

#[derive(Deserialize, JsonSchema, Clone)]
pub struct AffinityGroupSelector {
    /// Name or ID of the project, only required if `affinity_group` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the Affinity Group
    pub affinity_group: NameOrId,
}

#[derive(Deserialize, JsonSchema, Clone)]
pub struct AntiAffinityGroupSelector {
    /// Name or ID of the project, only required if `anti_affinity_group` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the Anti Affinity Group
    pub anti_affinity_group: NameOrId,
}

// PROJECTS

/// Create-time parameters for a `Project`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProjectCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// Updateable properties of a `Project`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProjectUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

// NETWORK INTERFACES

/// Create-time parameters for an `InstanceNetworkInterface`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceNetworkInterfaceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The VPC in which to create the interface.
    pub vpc_name: Name,
    /// The VPC Subnet in which to create the interface.
    pub subnet_name: Name,
    /// The IP address for the interface. One will be auto-assigned if not provided.
    pub ip: Option<IpAddr>,
}

/// Parameters for updating an `InstanceNetworkInterface`
///
/// Note that modifying IP addresses for an interface is not yet supported, a
/// new interface must be created instead.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceNetworkInterfaceUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,

    /// Make a secondary interface the instance's primary interface.
    ///
    /// If applied to a secondary interface, that interface will become the
    /// primary on the next reboot of the instance. Note that this may have
    /// implications for routing between instances, as the new primary interface
    /// will be on a distinct subnet from the previous primary interface.
    ///
    /// Note that this can only be used to select a new primary interface for an
    /// instance. Requests to change the primary interface into a secondary will
    /// return an error.
    // TODO-completeness TODO-doc When we get there, this should note that a
    // change in the primary interface will result in changes to the DNS records
    // for the instance, though not the name.
    #[serde(default)]
    pub primary: bool,

    /// A set of additional networks that this interface may send and
    /// receive traffic on.
    #[serde(default)]
    pub transit_ips: Vec<IpNet>,
}

// CERTIFICATES

/// Create-time parameters for a `Certificate`
#[derive(Clone, Deserialize, Serialize, JsonSchema)]
pub struct CertificateCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// PEM-formatted string containing public certificate chain
    pub cert: String,
    /// PEM-formatted string containing private key
    pub key: String,
    /// The service using this certificate
    pub service: shared::ServiceUsingCertificate,
}

impl std::fmt::Debug for CertificateCreate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertificateCreate")
            .field("identity", &self.identity)
            .field("cert", &self.cert)
            .field("key", &"<redacted>")
            .finish()
    }
}

// IP POOLS

/// Create-time parameters for an `IpPool`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// Parameters for updating an IP Pool
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolSiloPath {
    pub pool: NameOrId,
    pub silo: NameOrId,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolLinkSilo {
    pub silo: NameOrId,
    /// When a pool is the default for a silo, floating IPs and instance
    /// ephemeral IPs will come from that pool when no other pool is specified.
    /// There can be at most one default for a given silo.
    pub is_default: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IpPoolSiloUpdate {
    /// When a pool is the default for a silo, floating IPs and instance
    /// ephemeral IPs will come from that pool when no other pool is specified.
    /// There can be at most one default for a given silo, so when a pool is
    /// made default, an existing default will remain linked but will no longer
    /// be the default.
    pub is_default: bool,
}

// Floating IPs
/// Parameters for creating a new floating IP address for instances.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// An IP address to reserve for use as a floating IP. This field is
    /// optional: when not set, an address will be automatically chosen from
    /// `pool`. If set, then the IP must be available in the resolved `pool`.
    pub ip: Option<IpAddr>,

    /// The parent IP pool that a floating IP is pulled from. If unset, the
    /// default pool is selected.
    pub pool: Option<NameOrId>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/// The type of resource that a floating IP is attached to
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FloatingIpParentKind {
    Instance,
}

/// Parameters for attaching a floating IP address to another resource
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FloatingIpAttach {
    /// Name or ID of the resource that this IP address should be attached to
    pub parent: NameOrId,

    /// The type of `parent`'s resource
    pub kind: FloatingIpParentKind,
}

// INSTANCES

/// Describes an attachment of an `InstanceNetworkInterface` to an `Instance`,
/// at the time the instance is created.
// NOTE: VPC's are an organizing concept for networking resources, not for
// instances. It's true that all networking resources for an instance must
// belong to a single VPC, but we don't consider instances to be "scoped" to a
// VPC in the same way that they are scoped to projects, for example.
//
// This is slightly different than some other cloud providers, such as AWS,
// which use VPCs as both a networking concept, and a container more similar to
// our concept of a project. One example for why this is useful is that "moving"
// an instance to a new VPC can be done by detaching any interfaces in the
// original VPC and attaching interfaces in the new VPC.
//
// This type then requires the VPC identifiers, exactly because instances are
// _not_ scoped to a VPC, and so the VPC and/or VPC Subnet names are not present
// in the path of endpoints handling instance operations.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", content = "params", rename_all = "snake_case")]
pub enum InstanceNetworkInterfaceAttachment {
    /// Create one or more `InstanceNetworkInterface`s for the `Instance`.
    ///
    /// If more than one interface is provided, then the first will be
    /// designated the primary interface for the instance.
    Create(Vec<InstanceNetworkInterfaceCreate>),

    /// The default networking configuration for an instance is to create a
    /// single primary interface with an automatically-assigned IP address. The
    /// IP will be pulled from the Project's default VPC / VPC Subnet.
    Default,

    /// No network interfaces at all will be created for the instance.
    None,
}

impl Default for InstanceNetworkInterfaceAttachment {
    fn default() -> Self {
        Self::Default
    }
}

/// Describe the instance's disks at creation time
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InstanceDiskAttachment {
    /// During instance creation, create and attach disks
    Create(DiskCreate),

    /// During instance creation, attach this disk
    Attach(InstanceDiskAttach),
}

impl InstanceDiskAttachment {
    /// Get the name of the disk described by this attachment.
    pub fn name(&self) -> Name {
        match self {
            Self::Create(create) => create.identity.name.clone(),
            Self::Attach(InstanceDiskAttach { name }) => name.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceDiskAttach {
    /// A disk name to attach
    pub name: Name,
}

/// Parameters for creating an external IP address for instances.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalIpCreate {
    /// An IP address providing both inbound and outbound access. The address is
    /// automatically assigned from the provided IP pool or the default IP pool
    /// if not specified.
    Ephemeral { pool: Option<NameOrId> },
    /// An IP address providing both inbound and outbound access. The address is
    /// an existing floating IP object assigned to the current project.
    ///
    /// The floating IP must not be in use by another instance or service.
    Floating { floating_ip: NameOrId },
}

/// Parameters for creating an ephemeral IP address for an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub struct EphemeralIpCreate {
    /// Name or ID of the IP pool used to allocate an address. If unspecified,
    /// the default IP pool will be used.
    pub pool: Option<NameOrId>,
}

/// Parameters for detaching an external IP from an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalIpDetach {
    Ephemeral,
    Floating { floating_ip: NameOrId },
}

/// Create-time parameters for an `Instance`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The number of vCPUs to be allocated to the instance
    pub ncpus: InstanceCpuCount,
    /// The amount of RAM (in bytes) to be allocated to the instance
    pub memory: ByteCount,
    /// The hostname to be assigned to the instance
    pub hostname: Hostname,

    /// User data for instance initialization systems (such as cloud-init).
    /// Must be a Base64-encoded string, as specified in RFC 4648 § 4 (+ and /
    /// characters with padding). Maximum 32 KiB unencoded data.
    // While serde happily accepts #[serde(with = "<mod>")] as a shorthand for
    // specifing `serialize_with` and `deserialize_with`, schemars requires the
    // argument to `with` to be a type rather than merely a path prefix (i.e. a
    // mod or type). It's admittedly a bit tricky for schemars to address;
    // unlike `serialize` or `deserialize`, `JsonSchema` requires several
    // functions working together. It's unfortunate that schemars has this
    // built-in incompatibility, exacerbated by its glacial rate of progress
    // and immunity to offers of help.
    #[serde(default, with = "UserData")]
    pub user_data: Vec<u8>,

    /// The network interfaces to be created for this instance.
    #[serde(default)]
    pub network_interfaces: InstanceNetworkInterfaceAttachment,

    /// The external IP addresses provided to this instance.
    ///
    /// By default, all instances have outbound connectivity, but no inbound
    /// connectivity. These external addresses can be used to provide a fixed,
    /// known IP address for making inbound connections to the instance.
    #[serde(default)]
    pub external_ips: Vec<ExternalIpCreate>,

    /// A list of disks to be attached to the instance.
    ///
    /// Disk attachments of type "create" will be created, while those of type
    /// "attach" must already exist.
    ///
    /// The order of this list does not guarantee a boot order for the instance.
    /// Use the boot_disk attribute to specify a boot disk. When boot_disk is
    /// specified it will count against the disk attachment limit.
    #[serde(default)]
    pub disks: Vec<InstanceDiskAttachment>,

    /// The disk the instance is configured to boot from.
    ///
    /// This disk can either be attached if it already exists or created along
    /// with the instance.
    ///
    /// Specifying a boot disk is optional but recommended to ensure predictable
    /// boot behavior. The boot disk can be set during instance creation or
    /// later if the instance is stopped. The boot disk counts against the disk
    /// attachment limit.
    ///
    /// An instance that does not have a boot disk set will use the boot
    /// options specified in its UEFI settings, which are controlled by both the
    /// instance's UEFI firmware and the guest operating system. Boot options
    /// can change as disks are attached and detached, which may result in an
    /// instance that only boots to the EFI shell until a boot disk is set.
    #[serde(default)]
    pub boot_disk: Option<InstanceDiskAttachment>,

    /// An allowlist of SSH public keys to be transferred to the instance via
    /// cloud-init during instance creation.
    ///
    /// If not provided, all SSH public keys from the user's profile will be sent.
    /// If an empty list is provided, no public keys will be transmitted to the
    /// instance.
    pub ssh_public_keys: Option<Vec<NameOrId>>,

    /// Should this instance be started upon creation; true by default.
    #[serde(default = "bool_true")]
    pub start: bool,

    /// The auto-restart policy for this instance.
    ///
    /// This policy determines whether the instance should be automatically
    /// restarted by the control plane on failure. If this is `null`, no
    /// auto-restart policy will be explicitly configured for this instance, and
    /// the control plane will select the default policy when determining
    /// whether the instance can be automatically restarted.
    ///
    /// Currently, the global default auto-restart policy is "best-effort", so
    /// instances with `null` auto-restart policies will be automatically
    /// restarted. However, in the future, the default policy may be
    /// configurable through other mechanisms, such as on a per-project basis.
    /// In that case, any configured default policy will be used if this is
    /// `null`.
    #[serde(default)]
    pub auto_restart_policy: Option<InstanceAutoRestartPolicy>,

    /// Anti-Affinity groups which this instance should be added.
    #[serde(default)]
    pub anti_affinity_groups: Vec<NameOrId>,
}

/// Parameters of an `Instance` that can be reconfigured after creation.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceUpdate {
    /// The number of CPUs to assign to this instance.
    pub ncpus: InstanceCpuCount,

    /// The amount of memory to assign to this instance.
    pub memory: ByteCount,

    /// Name or ID of the disk the instance should be instructed to boot from.
    ///
    /// If not provided, unset the instance's boot disk.
    pub boot_disk: Option<NameOrId>,

    /// Sets the auto-restart policy for this instance.
    ///
    /// This policy determines whether the instance should be automatically
    /// restarted by the control plane on failure. If this is `null`, any
    /// explicitly configured auto-restart policy will be unset, and
    /// the control plane will select the default policy when determining
    /// whether the instance can be automatically restarted.
    ///
    /// Currently, the global default auto-restart policy is "best-effort", so
    /// instances with `null` auto-restart policies will be automatically
    /// restarted. However, in the future, the default policy may be
    /// configurable through other mechanisms, such as on a per-project basis.
    /// In that case, any configured default policy will be used if this is
    /// `null`.
    pub auto_restart_policy: Option<InstanceAutoRestartPolicy>,
}

#[inline]
fn bool_true() -> bool {
    true
}

// If you change this, also update the error message in
// `UserData::deserialize()` below.
pub const MAX_USER_DATA_BYTES: usize = 32 * 1024; // 32 KiB

struct UserData;
impl UserData {
    pub fn serialize<S>(
        data: &Vec<u8>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        base64::engine::general_purpose::STANDARD
            .encode(data)
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match base64::engine::general_purpose::STANDARD
            .decode(<String>::deserialize(deserializer)?)
        {
            Ok(buf) => {
                // if you change this, also update the stress test in crate::cidata
                if buf.len() > MAX_USER_DATA_BYTES {
                    Err(<D::Error as serde::de::Error>::invalid_length(
                        buf.len(),
                        &"less than 32 KiB",
                    ))
                } else {
                    Ok(buf)
                }
            }
            Err(_) => Err(<D::Error as serde::de::Error>::invalid_value(
                serde::de::Unexpected::Other("invalid base64 string"),
                &"a valid base64 string",
            )),
        }
    }
}

impl JsonSchema for UserData {
    fn schema_name() -> String {
        "String".to_string()
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            format: Some("byte".to_string()),
            ..Default::default()
        }
        .into()
    }

    fn is_referenceable() -> bool {
        false
    }
}

/// Forwarded to a propolis server to request the contents of an Instance's serial console.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct InstanceSerialConsoleRequest {
    /// Name or ID of the project, only required if `instance` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Character index in the serial buffer from which to read, counting the bytes output since
    /// instance start. If this is not provided, `most_recent` must be provided, and if this *is*
    /// provided, `most_recent` must *not* be provided.
    pub from_start: Option<u64>,
    /// Character index in the serial buffer from which to read, counting *backward* from the most
    /// recently buffered data retrieved from the instance. (See note on `from_start` about mutual
    /// exclusivity)
    pub most_recent: Option<u64>,
    /// Maximum number of bytes of buffered serial console contents to return. If the requested
    /// range runs to the end of the available buffer, the data returned will be shorter than
    /// `max_bytes`.
    pub max_bytes: Option<u64>,
}

/// Forwarded to a propolis server to request the contents of an Instance's serial console.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct InstanceSerialConsoleStreamRequest {
    /// Name or ID of the project, only required if `instance` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Character index in the serial buffer from which to read, counting *backward* from the most
    /// recently buffered data retrieved from the instance.
    pub most_recent: Option<u64>,
}

/// Contents of an Instance's serial console buffer.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceSerialConsoleData {
    /// The bytes starting from the requested offset up to either the end of the buffer or the
    /// request's `max_bytes`. Provided as a u8 array rather than a string, as it may not be UTF-8.
    pub data: Vec<u8>,
    /// The absolute offset since boot (suitable for use as `byte_offset` in a subsequent request)
    /// of the last byte returned in `data`.
    pub last_byte_offset: u64,
}

// VPCS

/// Create-time parameters for a `Vpc`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The IPv6 prefix for this VPC
    ///
    /// All IPv6 subnets created from this VPC must be taken from this range,
    /// which should be a Unique Local Address in the range `fd00::/48`. The
    /// default VPC Subnet will have the first `/64` range from this prefix.
    pub ipv6_prefix: Option<Ipv6Net>,

    pub dns_name: Name,
}

/// Updateable properties of a `Vpc`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    pub dns_name: Option<Name>,
}

/// Create-time parameters for a `VpcSubnet`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcSubnetCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The IPv4 address range for this subnet.
    ///
    /// It must be allocated from an RFC 1918 private address range, and must
    /// not overlap with any other existing subnet in the VPC.
    pub ipv4_block: Ipv4Net,

    /// The IPv6 address range for this subnet.
    ///
    /// It must be allocated from the RFC 4193 Unique Local Address range, with
    /// the prefix equal to the parent VPC's prefix. A random `/64` block will
    /// be assigned if one is not provided. It must not overlap with any
    /// existing subnet in the VPC.
    pub ipv6_block: Option<Ipv6Net>,

    /// An optional router, used to direct packets sent from hosts in this subnet
    /// to any destination address.
    ///
    /// Custom routers apply in addition to the VPC-wide *system* router, and have
    /// higher priority than the system router for an otherwise
    /// equal-prefix-length match.
    pub custom_router: Option<NameOrId>,
}

/// Updateable properties of a `VpcSubnet`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcSubnetUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,

    /// An optional router, used to direct packets sent from hosts in this subnet
    /// to any destination address.
    pub custom_router: Option<NameOrId>,
}

// VPC ROUTERS

/// Create-time parameters for a `VpcRouter`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcRouterCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// Updateable properties of a `VpcRouter`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcRouterUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

// VPC ROUTER ROUTES

/// Create-time parameters for a `RouterRoute`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RouterRouteCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The location that matched packets should be forwarded to.
    pub target: RouteTarget,
    /// Selects which traffic this routing rule will apply to.
    pub destination: RouteDestination,
}

/// Updateable properties of a `RouterRoute`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RouterRouteUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    /// The location that matched packets should be forwarded to.
    pub target: RouteTarget,
    /// Selects which traffic this routing rule will apply to.
    pub destination: RouteDestination,
}

// INTERNET GATEWAYS

/// Create-time parameters for an `InternetGateway`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InternetGatewayCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InternetGatewayIpPoolCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub ip_pool: NameOrId,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InternetGatewayIpAddressCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub address: IpAddr,
}

// DISKS

#[derive(Display, Serialize, Deserialize, JsonSchema)]
#[display(style = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum DiskMetricName {
    Activated,
    Flush,
    Read,
    ReadBytes,
    Write,
    WriteBytes,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct DiskMetricsPath {
    pub disk: NameOrId,
    pub metric: DiskMetricName,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "u32")] // invoke the try_from validation routine below
pub struct BlockSize(pub u32);

impl TryFrom<u32> for BlockSize {
    type Error = anyhow::Error;
    fn try_from(x: u32) -> Result<BlockSize, Self::Error> {
        if ![512, 2048, 4096].contains(&x) {
            anyhow::bail!("invalid block size {}", x);
        }

        Ok(BlockSize(x))
    }
}

impl Into<ByteCount> for BlockSize {
    fn into(self) -> ByteCount {
        ByteCount::from(self.0)
    }
}

impl From<BlockSize> for u64 {
    fn from(bs: BlockSize) -> u64 {
        u64::from(bs.0)
    }
}

impl JsonSchema for BlockSize {
    fn schema_name() -> String {
        "BlockSize".to_string()
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                id: None,
                title: Some("disk block size in bytes".to_string()),
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::InstanceType::Integer.into()),
            enum_values: Some(vec![
                serde_json::json!(512),
                serde_json::json!(2048),
                serde_json::json!(4096),
            ]),
            ..Default::default()
        })
    }
}

/// Describes the form factor of physical disks.
#[derive(
    Debug, Serialize, Deserialize, JsonSchema, Clone, Copy, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalDiskKind {
    M2,
    U2,
}

impl From<DiskVariant> for PhysicalDiskKind {
    fn from(dv: DiskVariant) -> Self {
        match dv {
            DiskVariant::M2 => PhysicalDiskKind::M2,
            DiskVariant::U2 => PhysicalDiskKind::U2,
        }
    }
}

/// Different sources for a disk
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DiskSource {
    /// Create a blank disk
    Blank {
        /// size of blocks for this Disk. valid values are: 512, 2048, or 4096
        block_size: BlockSize,
    },
    /// Create a disk from a disk snapshot
    Snapshot { snapshot_id: Uuid },
    /// Create a disk from an image
    Image { image_id: Uuid },
    /// Create a blank disk that will accept bulk writes or pull blocks from an
    /// external source.
    ImportingBlocks { block_size: BlockSize },
}

/// Create-time parameters for a `Disk`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DiskCreate {
    /// The common identifying metadata for the disk
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The initial source for this disk
    pub disk_source: DiskSource,
    /// The total size of the Disk (in bytes)
    pub size: ByteCount,
}

// equivalent to crucible_pantry_client::types::ExpectedDigest
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedDigest {
    Sha256(String),
}

/// Parameters for importing blocks with a bulk write
// equivalent to crucible_pantry_client::types::BulkWriteRequest
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ImportBlocksBulkWrite {
    pub offset: u64,
    pub base64_encoded_data: String,
}

/// Parameters for finalizing a disk
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct FinalizeDisk {
    /// If specified a snapshot of the disk will be created with the given name
    /// during finalization. If not specified, a snapshot for the disk will
    /// _not_ be created. A snapshot can be manually created once the disk
    /// transitions into the `Detached` state.
    pub snapshot_name: Option<Name>,
}

/// Select an address lot by an optional name or id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct AddressLotSelector {
    /// Name or id of the address lot to select
    pub address_lot: NameOrId,
}

/// Parameters for creating an address lot.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AddressLotCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The kind of address lot to create.
    pub kind: AddressLotKind,
    /// The blocks to add along with the new address lot.
    pub blocks: Vec<AddressLotBlockCreate>,
}

/// Parameters for creating an address lot block. Fist and last addresses are
/// inclusive.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AddressLotBlockCreate {
    /// The first address in the lot (inclusive).
    pub first_address: IpAddr,
    /// The last address in the lot (inclusive).
    pub last_address: IpAddr,
}

/// Parameters for creating a loopback address on a particular rack switch.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct LoopbackAddressCreate {
    /// The name or id of the address lot this loopback address will pull an
    /// address from.
    pub address_lot: NameOrId,

    /// The containing the switch this loopback address will be configured on.
    pub rack_id: Uuid,

    // TODO: #3604 Consider using `SwitchLocation` type instead of `Name` for `LoopbackAddressCreate.switch_location`
    /// The location of the switch within the rack this loopback address will be
    /// configured on.
    pub switch_location: Name,

    /// The address to create.
    pub address: IpAddr,

    /// The subnet mask to use for the address.
    pub mask: u8,

    /// Address is an anycast address.
    /// This allows the address to be assigned to multiple locations simultaneously.
    pub anycast: bool,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct LoopbackAddressPath {
    /// The rack to use when selecting the loopback address.
    pub rack_id: Uuid,

    /// The switch location to use when selecting the loopback address.
    pub switch_location: Name,

    /// The IP address and subnet mask to use when selecting the loopback
    /// address.
    pub address: IpAddr,

    /// The IP address and subnet mask to use when selecting the loopback
    /// address.
    pub subnet_mask: u8,
}

/// Parameters for creating a port settings group.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SwtichPortSettingsGroupCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// Switch port settings to associate with the settings group being created.
    pub settings: SwitchPortSettingsCreate,
}

/// Parameters for creating switch port settings. Switch port settings are the
/// central data structure for setting up external networking. Switch port
/// settings include link, interface, route, address and dynamic network
/// protocol configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SwitchPortSettingsCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub port_config: SwitchPortConfigCreate,

    #[serde(default)]
    pub groups: Vec<NameOrId>,

    /// Link configurations.
    pub links: Vec<LinkConfigCreate>,

    /// Interface configurations.
    #[serde(default)]
    pub interfaces: Vec<SwitchInterfaceConfigCreate>,

    /// Route configurations.
    #[serde(default)]
    pub routes: Vec<RouteConfig>,

    /// BGP peer configurations.
    #[serde(default)]
    pub bgp_peers: Vec<BgpPeerConfig>,

    /// Address configurations.
    pub addresses: Vec<AddressConfig>,
}

impl SwitchPortSettingsCreate {
    pub fn new(identity: IdentityMetadataCreateParams) -> Self {
        Self {
            identity,
            port_config: SwitchPortConfigCreate {
                geometry: SwitchPortGeometry::Qsfp28x1,
            },
            groups: Vec::new(),
            links: Vec::new(),
            interfaces: Vec::new(),
            routes: Vec::new(),
            bgp_peers: Vec::new(),
            addresses: Vec::new(),
        }
    }
}

/// Physical switch port configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SwitchPortConfigCreate {
    /// Link geometry for the switch port.
    pub geometry: SwitchPortGeometry,
}

/// The link geometry associated with a switch port.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SwitchPortGeometry {
    /// The port contains a single QSFP28 link with four lanes.
    Qsfp28x1,

    /// The port contains two QSFP28 links each with two lanes.
    Qsfp28x2,

    /// The port contains four SFP28 links each with one lane.
    Sfp28x4,
}

/// Switch link configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct LinkConfigCreate {
    /// Link name. On ports that are not broken out, this is always phy0.
    /// On a 2x breakout the options are phy0 and phy1, on 4x
    /// phy0-phy3, etc.
    pub link_name: Name,

    /// Maximum transmission unit for the link.
    pub mtu: u16,

    /// The link-layer discovery protocol (LLDP) configuration for the link.
    pub lldp: LldpLinkConfigCreate,

    /// The requested forward-error correction method.  If this is not
    /// specified, the standard FEC for the underlying media will be applied
    /// if it can be determined.
    pub fec: Option<LinkFec>,

    /// The speed of the link.
    pub speed: LinkSpeed,

    /// Whether or not to set autonegotiation.
    pub autoneg: bool,

    /// Optional tx_eq settings.
    pub tx_eq: Option<TxEqConfig>,
}

/// Per-port tx-eq overrides.  This can be used to fine-tune the transceiver
/// equalization settings to improve signal integrity.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct TxEqConfig {
    /// Pre-cursor tap1
    pub pre1: Option<i32>,
    /// Pre-cursor tap2
    pub pre2: Option<i32>,
    /// Main tap
    pub main: Option<i32>,
    /// Post-cursor tap2
    pub post2: Option<i32>,
    /// Post-cursor tap1
    pub post1: Option<i32>,
}

impl From<omicron_common::api::internal::shared::TxEqConfig> for TxEqConfig {
    fn from(
        x: omicron_common::api::internal::shared::TxEqConfig,
    ) -> TxEqConfig {
        TxEqConfig {
            pre1: x.pre1,
            pre2: x.pre2,
            main: x.main,
            post2: x.post2,
            post1: x.post1,
        }
    }
}

/// The LLDP configuration associated with a port.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct LldpLinkConfigCreate {
    /// Whether or not LLDP is enabled.
    pub enabled: bool,

    /// The LLDP link name TLV.
    pub link_name: Option<String>,

    /// The LLDP link description TLV.
    pub link_description: Option<String>,

    /// The LLDP chassis identifier TLV.
    pub chassis_id: Option<String>,

    /// The LLDP system name TLV.
    pub system_name: Option<String>,

    /// The LLDP system description TLV.
    pub system_description: Option<String>,

    /// The LLDP management IP TLV.
    pub management_ip: Option<IpAddr>,
}

impl PartialEq<LldpLinkConfigCreate>
    for omicron_common::api::external::LldpLinkConfig
{
    fn eq(&self, other: &LldpLinkConfigCreate) -> bool {
        self.enabled == other.enabled
            && self.link_name == other.link_name
            && self.link_description == other.link_description
            && self.chassis_id == other.chassis_id
            && self.system_name == other.system_name
            && self.system_description == other.system_description
            && self.management_ip == other.management_ip
    }
}

impl PartialEq<omicron_common::api::external::LldpLinkConfig>
    for LldpLinkConfigCreate
{
    fn eq(
        &self,
        other: &omicron_common::api::external::LldpLinkConfig,
    ) -> bool {
        self.enabled == other.enabled
            && self.link_name == other.link_name
            && self.link_description == other.link_description
            && self.chassis_id == other.chassis_id
            && self.system_name == other.system_name
            && self.system_description == other.system_description
            && self.management_ip == other.management_ip
    }
}

/// A layer-3 switch interface configuration. When IPv6 is enabled, a link local
/// address will be created for the interface.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SwitchInterfaceConfigCreate {
    /// Link name. On ports that are not broken out, this is always phy0.
    /// On a 2x breakout the options are phy0 and phy1, on 4x
    /// phy0-phy3, etc.
    pub link_name: Name,

    /// Whether or not IPv6 is enabled.
    pub v6_enabled: bool,

    /// What kind of switch interface this configuration represents.
    pub kind: SwitchInterfaceKind,
}

/// Indicates the kind for a switch interface.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SwitchInterfaceKind {
    /// Primary interfaces are associated with physical links. There is exactly
    /// one primary interface per physical link.
    Primary,

    /// VLAN interfaces allow physical interfaces to be multiplexed onto
    /// multiple logical links, each distinguished by a 12-bit 802.1Q Ethernet
    /// tag.
    Vlan(SwitchVlanInterface),

    /// Loopback interfaces are anchors for IP addresses that are not specific
    /// to any particular port.
    Loopback,
}

/// Configuration data associated with a switch VLAN interface. The VID
/// indicates a VLAN identifier. Must be between 1 and 4096.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SwitchVlanInterface {
    /// The virtual network id (VID) that distinguishes this interface and is
    /// used for producing and consuming 802.1Q Ethernet tags. This field has a
    /// maximum value of 4095 as 802.1Q tags are twelve bits.
    pub vid: u16,
}

/// Route configuration data associated with a switch port configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RouteConfig {
    /// Link name. On ports that are not broken out, this is always phy0.
    /// On a 2x breakout the options are phy0 and phy1, on 4x
    /// phy0-phy3, etc.
    pub link_name: Name,

    /// The set of routes assigned to a switch port.
    pub routes: Vec<Route>,
}

/// A route to a destination network through a gateway address.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Route {
    /// The route destination.
    pub dst: IpNet,

    /// The route gateway.
    pub gw: IpAddr,

    /// VLAN id the gateway is reachable over.
    pub vid: Option<u16>,

    /// Route RIB priority. Higher priority indicates precedence within and across
    /// protocols.
    pub rib_priority: Option<u8>,
}

/// Select a BGP config by a name or id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpConfigSelector {
    /// A name or id to use when selecting BGP config.
    pub name_or_id: NameOrId,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpPeerConfig {
    /// Link that the peer is reachable on.
    /// On ports that are not broken out, this is always phy0.
    /// On a 2x breakout the options are phy0 and phy1, on 4x
    /// phy0-phy3, etc.
    pub link_name: Name,

    pub peers: Vec<BgpPeer>,
}

/// Parameters for creating a named set of BGP announcements.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpAnnounceSetCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The announcements in this set.
    pub announcement: Vec<BgpAnnouncementCreate>,
}

/// Select a BGP announce set by a name or id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpAnnounceSetSelector {
    /// Name or ID of the announce set
    pub announce_set: NameOrId,
}

/// List BGP announce set with an optional name or id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpAnnounceListSelector {
    /// Name or ID of the announce set
    pub announce_set: Option<NameOrId>,
}

/// Selector used for querying imported BGP routes.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpRouteSelector {
    /// The ASN to filter on. Required.
    pub asn: u32,
}

/// A BGP announcement tied to a particular address lot block.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpAnnouncementCreate {
    /// Address lot this announcement is drawn from.
    pub address_lot_block: NameOrId,

    /// The network being announced.
    pub network: IpNet,
}

/// Parameters for creating a BGP configuration. This includes and autonomous
/// system number (ASN) and a virtual routing and forwarding (VRF) identifier.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpConfigCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The autonomous system number of this BGP configuration.
    pub asn: u32,

    pub bgp_announce_set_id: NameOrId,

    /// Optional virtual routing and forwarding identifier for this BGP
    /// configuration.
    pub vrf: Option<Name>,

    // Dynamic BGP policy is not yet available so we skip adding it to the API
    /// A shaper program to apply to outgoing open and update messages.
    #[serde(skip)]
    pub shaper: Option<String>,
    /// A checker program to apply to incoming open and update messages.
    #[serde(skip)]
    pub checker: Option<String>,
}

/// Select a BGP status information by BGP config id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpStatusSelector {
    /// A name or id of the BGP configuration to get status for
    pub name_or_id: NameOrId,
}

/// Information about a bidirectional forwarding detection (BFD) session.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BfdSessionEnable {
    /// Address the Oxide switch will listen on for BFD traffic. If `None` then
    /// the unspecified address (0.0.0.0 or ::) is used.
    pub local: Option<IpAddr>,

    /// Address of the remote peer to establish a BFD session with.
    pub remote: IpAddr,

    /// The negotiated Control packet transmission interval, multiplied by this
    /// variable, will be the Detection Time for this session (as seen by the
    /// remote system)
    pub detection_threshold: u8,

    /// The minimum interval, in microseconds, between received BFD
    /// Control packets that this system requires
    pub required_rx: u64,

    /// The switch to enable this session on. Must be `switch0` or `switch1`.
    pub switch: Name,

    /// Select either single-hop (RFC 5881) or multi-hop (RFC 5883)
    pub mode: BfdMode,
}

/// Information needed to disable a BFD session
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BfdSessionDisable {
    /// Address of the remote peer to disable a BFD session for.
    pub remote: IpAddr,

    /// The switch to enable this session on. Must be `switch0` or `switch1`.
    pub switch: Name,
}

/// A set of addresses associated with a port configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AddressConfig {
    /// Link to assign the addresses to.
    /// On ports that are not broken out, this is always phy0.
    /// On a 2x breakout the options are phy0 and phy1, on 4x
    /// phy0-phy3, etc.
    pub link_name: Name,

    /// The set of addresses assigned to the port configuration.
    pub addresses: Vec<Address>,
}

/// An address tied to an address lot.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Address {
    /// The address lot this address is drawn from.
    pub address_lot: NameOrId,

    /// The address and prefix length of this address.
    pub address: IpNet,

    /// Optional VLAN ID for this address
    pub vlan_id: Option<u16>,
}

/// Select a port settings object by an optional name or id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortSettingsSelector {
    /// An optional name or id to use when selecting port settings.
    pub port_settings: Option<NameOrId>,
}

/// Select a port settings info object by name or id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortSettingsInfoSelector {
    /// A name or id to use when selecting switch port settings info objects.
    pub port: NameOrId,
}

/// Select a switch port by name.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortPathSelector {
    /// A name to use when selecting switch ports.
    pub port: Name,
}

/// Select switch ports by rack id and location.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortSelector {
    /// A rack id to use when selecting switch ports.
    pub rack_id: Uuid,

    /// A switch location to use when selecting switch ports.
    pub switch_location: Name,
}

/// Select switch port interfaces by id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortPageSelector {
    /// An optional switch port id to use when listing switch ports.
    pub switch_port_id: Option<Uuid>,
}

/// Parameters for applying settings to switch ports.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct SwitchPortApplySettings {
    /// A name or id to use when applying switch port settings.
    pub port_settings: NameOrId,
}

/// Select an LLDP endpoint by rack/switch/port
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct LldpPortPathSelector {
    /// A rack id to use when selecting switch ports.
    pub rack_id: Uuid,

    /// A switch location to use when selecting switch ports.
    pub switch_location: Name,

    /// A name to use when selecting switch ports.
    pub port: Name,
}

// IMAGES

/// The source of the underlying image.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ImageSource {
    Snapshot {
        id: Uuid,
    },

    /// Boot the Alpine ISO that ships with the Propolis zone. Intended for
    /// development purposes only.
    #[schemars(skip)] // keep it out of the OpenAPI schema
    YouCanBootAnythingAsLongAsItsAlpine,
}

/// OS image distribution
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Distribution {
    /// The name of the distribution (e.g. "alpine" or "ubuntu")
    pub name: Name,
    /// The version of the distribution (e.g. "3.10" or "18.04")
    pub version: String,
}

/// Create-time parameters for an `Image`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ImageCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The family of the operating system (e.g. Debian, Ubuntu, etc.)
    pub os: String,

    /// The version of the operating system (e.g. 18.04, 20.04, etc.)
    pub version: String,

    /// The source of the image's contents.
    pub source: ImageSource,
}

// SNAPSHOTS

/// Create-time parameters for a `Snapshot`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SnapshotCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The disk to be snapshotted
    pub disk: NameOrId,
}

// USERS AND GROUPS

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct OptionalGroupSelector {
    pub group: Option<Uuid>,
}

// BUILT-IN USERS
//
// These cannot be created via the external API, but we use the same interfaces
// for creating them internally as we use for types that can be created in the
// external API.

/// Create-time parameters for a `UserBuiltin`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UserBuiltinCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct UserBuiltinSelector {
    pub user: NameOrId,
}

// SSH PUBLIC KEYS
//
// The SSH key mangement endpoints are currently under `/v1/me`,
// and so have an implicit silo user ID which must be passed seperately
// to the creation routine. Note that this disagrees with RFD 44.

/// Create-time parameters for an `SshKey`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SshKeyCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// SSH public key, e.g., `"ssh-ed25519 AAAAC3NzaC..."`
    pub public_key: String,
}

// METRICS

#[derive(Display, Deserialize, JsonSchema)]
#[display(style = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum SystemMetricName {
    VirtualDiskSpaceProvisioned,
    CpusProvisioned,
    RamProvisioned,
}

#[derive(Deserialize, JsonSchema)]
pub struct SystemMetricsPathParam {
    pub metric_name: SystemMetricName,
}

/// Query parameters common to resource metrics endpoints.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ResourceMetrics {
    /// An inclusive start time of metrics.
    pub start_time: DateTime<Utc>,
    /// An exclusive end time of metrics.
    pub end_time: DateTime<Utc>,
    /// Query result order
    pub order: Option<PaginationOrder>,
}

// SYSTEM UPDATE

/// Parameters for PUT requests for `/v1/system/update/repository`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UpdatesPutRepositoryParams {
    /// The name of the uploaded file.
    pub file_name: String,
}

/// Parameters for GET requests for `/v1/system/update/repository`.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct UpdatesGetRepositoryParams {
    /// The version to get.
    pub system_version: Version,
}

/// Parameters for PUT requests to `/v1/system/update/target-release`.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct SetTargetReleaseParams {
    /// Version of the system software to make the target release.
    pub system_version: Version,
}

// Probes

/// Create time parameters for probes.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProbeCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub sled: Uuid,
    pub ip_pool: Option<NameOrId>,
}

/// List probes with an optional name or id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct ProbeListSelector {
    /// A name or id to use when selecting a probe.
    pub name_or_id: Option<NameOrId>,
}

/// A timeseries query string, written in the Oximeter query language.
#[derive(Deserialize, JsonSchema, Serialize)]
pub struct TimeseriesQuery {
    /// A timeseries query string, written in the Oximeter query language.
    pub query: String,
}

// Allowed source IPs

/// Parameters for updating allowed source IPs
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AllowListUpdate {
    /// The new list of allowed source IPs.
    pub allowed_ips: AllowedSourceIps,
}

// Console API

#[derive(Deserialize, JsonSchema)]
pub struct RestPathParam {
    pub path: Vec<String>,
}

#[derive(Deserialize, JsonSchema)]
pub struct LoginToProviderPathParam {
    pub silo_name: Name,
    pub provider_name: Name,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct LoginUrlQuery {
    pub redirect_uri: Option<RelativeUri>,
}

#[derive(Deserialize, JsonSchema)]
pub struct LoginPath {
    pub silo_name: Name,
}

/// This is meant as a security feature. We want to ensure we never redirect to
/// a URI on a different host.
#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone, Display)]
#[serde(try_from = "String")]
#[display("{0}")]
pub struct RelativeUri(String);

impl FromStr for RelativeUri {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s.to_string())
    }
}

impl TryFrom<Uri> for RelativeUri {
    type Error = String;

    fn try_from(uri: Uri) -> Result<Self, Self::Error> {
        if uri.host().is_none() && uri.scheme().is_none() {
            Ok(Self(uri.to_string()))
        } else {
            Err(format!("\"{}\" is not a relative URI", uri))
        }
    }
}

impl TryFrom<String> for RelativeUri {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse::<Uri>()
            .map_err(|_| format!("\"{}\" is not a relative URI", s))
            .and_then(|uri| Self::try_from(uri))
    }
}

// Device auth

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeviceAuthRequest {
    pub client_id: Uuid,
    /// Optional lifetime for the access token in seconds. If not specified, the
    /// silo's max TTL will be used (if set).
    pub ttl_seconds: Option<NonZeroU32>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeviceAuthVerify {
    pub user_code: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeviceAccessTokenRequest {
    pub grant_type: String,
    pub device_code: String,
    pub client_id: Uuid,
}

// Alerts

/// Query params for listing alert classes.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertClassFilter {
    /// An optional glob pattern for filtering alert class names.
    ///
    /// If provided, only alert classes which match this glob pattern will be
    /// included in the response.
    pub filter: Option<shared::AlertSubscription>,
}

#[derive(Deserialize, JsonSchema)]
pub struct AlertSelector {
    /// UUID of the alert
    pub alert_id: Uuid,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertSubscriptionSelector {
    /// The webhook receiver that the subscription is attached to.
    #[serde(flatten)]
    pub receiver: AlertReceiverSelector,
    /// The event class subscription itself.
    pub subscription: shared::AlertSubscription,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertClassPage {
    /// The last webhook event class returned by a previous page.
    pub last_seen: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertReceiverSelector {
    /// The name or ID of the webhook receiver.
    pub receiver: NameOrId,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct WebhookCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The URL that webhook notification requests should be sent to
    pub endpoint: Url,

    /// A non-empty list of secret keys used to sign webhook payloads.
    pub secrets: Vec<String>,

    /// A list of webhook event class subscriptions.
    ///
    /// If this list is empty or is not included in the request body, the
    /// webhook will not be subscribed to any events.
    #[serde(default)]
    pub subscriptions: Vec<shared::AlertSubscription>,
}

/// Parameters to update a webhook configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct WebhookReceiverUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,

    /// The URL that webhook notification requests should be sent to
    pub endpoint: Option<Url>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertSubscriptionCreate {
    /// The event class pattern to subscribe to.
    pub subscription: shared::AlertSubscription,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct WebhookSecretCreate {
    /// The value of the shared secret key.
    pub secret: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct WebhookSecretSelector {
    /// ID of the secret.
    pub secret_id: Uuid,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertDeliveryStateFilter {
    /// If true, include deliveries which are currently in progress.
    ///
    /// If any of the "pending", "failed", or "delivered" query parameters are
    /// set to true, only deliveries matching those state(s) will be included in
    /// the response. If NO state filter parameters are set, then all deliveries
    /// are included.
    ///
    /// A delivery is considered "pending" if it has not yet been sent at all,
    /// or if a delivery attempt has failed but the delivery has retries
    /// remaining.
    pub pending: Option<bool>,
    /// If true, include deliveries which have failed permanently.
    ///
    /// If any of the "pending", "failed", or "delivered" query parameters are
    /// set to true, only deliveries matching those state(s) will be included in
    /// the response. If NO state filter parameters are set, then all deliveries
    /// are included.
    ///
    /// A delivery fails permanently when the retry limit of three total
    /// attempts is reached without a successful delivery.
    pub failed: Option<bool>,
    /// If true, include deliveries which have succeeded.
    ///
    /// If any of the "pending", "failed", or "delivered" query parameters are
    /// set to true, only deliveries matching those state(s) will be included in
    /// the response. If NO state filter parameters are set, then all deliveries
    /// are included.
    pub delivered: Option<bool>,
}

impl Default for AlertDeliveryStateFilter {
    fn default() -> Self {
        Self::ALL
    }
}

impl AlertDeliveryStateFilter {
    pub const ALL: Self =
        Self { pending: Some(true), failed: Some(true), delivered: Some(true) };

    pub fn include_pending(&self) -> bool {
        self.pending == Some(true) || self.is_all_none()
    }

    pub fn include_failed(&self) -> bool {
        self.failed == Some(true) || self.is_all_none()
    }

    pub fn include_delivered(&self) -> bool {
        self.delivered == Some(true) || self.is_all_none()
    }

    pub fn include_all(&self) -> bool {
        self.is_all_none()
            || (self.pending == Some(true)
                && self.failed == Some(true)
                && self.delivered == Some(true))
    }

    fn is_all_none(&self) -> bool {
        self.pending.is_none()
            && self.failed.is_none()
            && self.delivered.is_none()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertReceiverProbe {
    /// If true, resend all events that have not been delivered successfully if
    /// the probe request succeeds.
    #[serde(default)]
    pub resend: bool,
}
