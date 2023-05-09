// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Params define the request bodies of API endpoints for creating or updating resources.

use crate::external_api::shared;
use base64::Engine;
use chrono::{DateTime, Utc};
use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, IdentityMetadataUpdateParams,
    InstanceCpuCount, Ipv4Net, Ipv6Net, Name, NameOrId, RouteDestination,
    RouteTarget, SemverVersion,
};
use schemars::JsonSchema;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{net::IpAddr, str::FromStr};
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

path_param!(ProjectPath, project, "project");
path_param!(InstancePath, instance, "instance");
path_param!(NetworkInterfacePath, interface, "network interface");
path_param!(VpcPath, vpc, "VPC");
path_param!(SubnetPath, subnet, "subnet");
path_param!(RouterPath, router, "router");
path_param!(RoutePath, route, "route");
path_param!(DiskPath, disk, "disk");
path_param!(SnapshotPath, snapshot, "snapshot");
path_param!(ImagePath, image, "image");
path_param!(SiloPath, silo, "silo");
path_param!(ProviderPath, provider, "SAML identity provider");
path_param!(IpPoolPath, pool, "IP pool");
path_param!(SshKeyPath, ssh_key, "SSH key");

// Only by ID because groups have an `external_id` instead of a name and
// therefore don't implement `ObjectIdentity`, which makes lookup by name
// inconvenient. We should figure this out more generally, as there are several
// resources like this.
#[derive(Deserialize, JsonSchema)]
pub struct GroupPath {
    /// ID of the group
    pub group: Uuid,
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

/// A specialized selector for image list, it contains an extra field to indicate
/// if silo scoped images should be included when listing project images.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ImageListSelector {
    /// Name or ID of the project
    pub project: Option<NameOrId>,
    /// Flag used to indicate if silo scoped images should be included when
    /// listing project images. Only valid when `project` is provided.
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_optional_bool_from_string")]
    pub include_silo_images: Option<bool>,
}

// Unfortunately `include_silo_images` can't used the default `Deserialize`
// derive given the selector that uses it is embedded via `serde(flatten)` which
// causes it to attempt to deserialize all flattened values a string. Similar workarounds
// have been implemented here: https://github.com/oxidecomputer/omicron/blob/efb03b501d7febe961cc8793b4d72e8542d28eab/gateway/src/http_entrypoints.rs#L443
fn deserialize_optional_bool_from_string<'de, D>(
    deserializer: D,
) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Unexpected;

    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    enum StringOrOptionalBool {
        String(String),
        OptionalBool(Option<bool>),
    }

    match StringOrOptionalBool::deserialize(deserializer)? {
        StringOrOptionalBool::String(s) => match s.as_str() {
            "true" => Ok(Some(true)),
            "false" => Ok(None),
            "" => Ok(None),
            _ => {
                Err(de::Error::invalid_type(Unexpected::Str(&s), &"a boolean"))
            }
        },
        StringOrOptionalBool::OptionalBool(b) => Ok(b),
    }
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
    /// Name or ID of the VPC, only required if `subnet` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the router
    pub router: NameOrId,
}

#[derive(Deserialize, JsonSchema)]
pub struct OptionalRouterSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `subnet` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the router
    pub router: Option<NameOrId>,
}

#[derive(Deserialize, JsonSchema)]
pub struct RouteSelector {
    /// Name or ID of the project, only required if `vpc` is provided as a `Name`
    pub project: Option<NameOrId>,
    /// Name or ID of the VPC, only required if `subnet` is provided as a `Name`
    pub vpc: Option<NameOrId>,
    /// Name or ID of the router, only required if `route` is provided as a `Name`
    pub router: Option<NameOrId>,
    /// Name or ID of the route
    pub route: NameOrId,
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
}

/// Create-time parameters for a `User`
#[derive(Clone, Deserialize, Serialize, JsonSchema)]
pub struct UserCreate {
    /// username used to log in
    pub external_id: UserId,
    /// password used to log in
    pub password: UserPassword,
}

/// A username for a local-only user
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "String")]
pub struct UserId(String);

impl AsRef<str> for UserId {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl FromStr for UserId {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        UserId::try_from(String::from(value))
    }
}

/// Used to impl `Deserialize`
impl TryFrom<String> for UserId {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        // Mostly, this validation exists to cap the input size.  The specific
        // length is not critical here.  For convenience and consistency, we use
        // the same rules as `Name`.
        let _ = Name::try_from(value.clone())?;
        Ok(UserId(value))
    }
}

impl JsonSchema for UserId {
    fn schema_name() -> String {
        "UserId".to_string()
    }

    fn json_schema(
        gen: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        Name::json_schema(gen)
    }
}

/// A password used for authenticating a local-only user
#[derive(Clone, Deserialize, Serialize)]
#[serde(try_from = "String")]
#[serde(into = "String")]
// We store both the raw String and omicron_passwords::Password forms of the
// password.  That's because `omicron_passwords::Password` does not support
// getting the String back out (by design), but we may need to do that in order
// to impl Serialize.  See the `From<Password> for String` impl below.
pub struct Password(String, omicron_passwords::Password);

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
        Ok(Password(value, inner))
    }
}

// This "From" impl only exists to make it easier to derive `Serialize`.  That
// in turn is only to make this easier to use from the test suite.  (There's no
// other reason structs in this file should need to impl Serialize at all.)
impl From<Password> for String {
    fn from(password: Password) -> Self {
        password.0
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
        &self.1
    }
}

/// Parameters for setting a user's password
#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "user_password_value", content = "details")]
pub enum UserPassword {
    /// Sets the user's password to the provided value
    Password(Password),
    /// Invalidates any current password (disabling password authentication)
    InvalidPassword,
}

/// Credentials for local user login
#[derive(Clone, Deserialize, JsonSchema, Serialize)]
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

    /// request signing private key (base64 encoded der file)
    #[serde(deserialize_with = "key_from_base64_encoded_der")]
    pub private_key: String,
}

struct X509CertVisitor;

impl<'de> Visitor<'de> for X509CertVisitor {
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

impl<'de> Visitor<'de> for KeyVisitor {
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
}

// CERTIFICATES

/// Create-time parameters for a `Certificate`
#[derive(Clone, Deserialize, Serialize, JsonSchema)]
pub struct CertificateCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// PEM file containing public certificate chain
    pub cert: Vec<u8>,
    /// PEM file containing private key
    pub key: Vec<u8>,
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

// INSTANCES

pub const MIN_MEMORY_SIZE_BYTES: u32 = 1 << 30; // 1 GiB

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
    /// automatically-assigned from the provided IP Pool, or all available pools
    /// if not specified.
    Ephemeral { pool_name: Option<Name> },
    // TODO: Add floating IPs: https://github.com/oxidecomputer/omicron/issues/1334
}

/// Create-time parameters for an `Instance`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub ncpus: InstanceCpuCount,
    pub memory: ByteCount,
    pub hostname: String, // TODO-cleanup different type?

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

    /// The disks to be created or attached for this instance.
    #[serde(default)]
    pub disks: Vec<InstanceDiskAttachment>,

    /// Should this instance be started upon creation; true by default.
    #[serde(default = "bool_true")]
    pub start: bool,
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

/// Migration parameters for an `Instance`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMigrate {
    pub dst_sled_id: Uuid,
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
    /// This parameter is only useful for the non-streaming GET request for serial console data,
    /// and *ignored* by the streaming websocket endpoint.
    pub max_bytes: Option<u64>,
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
}

/// Updateable properties of a `VpcSubnet`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcSubnetUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
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
    pub target: RouteTarget,
    pub destination: RouteDestination,
}

/// Updateable properties of a `RouterRoute`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RouterRouteUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    pub target: RouteTarget,
    pub destination: RouteDestination,
}

// DISKS

pub const MIN_DISK_SIZE_BYTES: u32 = 1 << 30; // 1 GiB

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
        bs.0 as u64
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
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// initial source for this disk
    pub disk_source: DiskSource,
    /// total size of the Disk in bytes
    pub size: ByteCount,
}

// equivalent to crucible_pantry_client::types::ExpectedDigest
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedDigest {
    Sha256(String),
}

/// Parameters for importing blocks from a URL to a disk
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ImportBlocksFromUrl {
    /// the source to pull blocks from
    pub url: String,
    /// Expected digest of all blocks when importing from a URL
    pub expected_digest: Option<ExpectedDigest>,
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

// IMAGES

/// The source of the underlying image.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ImageSource {
    Url {
        url: String,
    },
    Snapshot {
        id: Uuid,
    },

    /// Boot the Alpine ISO that ships with the Propolis zone. Intended for
    /// development purposes only.
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

    /// block size in bytes
    pub block_size: BlockSize,

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

    /// The name of the disk to be snapshotted
    pub disk: Name,
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

/// Query parameters common to resource metrics endpoints.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ResourceMetrics {
    /// An inclusive start time of metrics.
    pub start_time: DateTime<Utc>,
    /// An exclusive end time of metrics.
    pub end_time: DateTime<Utc>,
}

// SYSTEM UPDATE

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SystemUpdatePath {
    pub version: SemverVersion,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SystemUpdateStart {
    pub version: SemverVersion,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SystemUpdateCreate {
    pub version: SemverVersion,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ComponentUpdateCreate {
    pub version: SemverVersion,
    pub component_type: shared::UpdateableComponentType,
    pub system_update_id: Uuid,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UpdateableComponentCreate {
    pub version: SemverVersion,
    pub system_version: SemverVersion,
    pub component_type: shared::UpdateableComponentType,
    pub device_id: String,
}
