// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Params define the request bodies of API endpoints for creating or updating resources.

use crate::external_api::shared;
use omicron_common::api::external::{
    ByteCount, IdentityMetadataCreateParams, IdentityMetadataUpdateParams,
    InstanceCpuCount, Ipv4Net, Ipv6Net, Name,
};
use schemars::JsonSchema;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::net::IpAddr;
use uuid::Uuid;

// Silos

/// Create-time parameters for a [`Silo`](crate::external_api::views::Silo)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SiloCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub discoverable: bool,

    pub user_provision_type: shared::UserProvisionType,
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
        let raw_bytes = base64::decode(&value.as_bytes()).map_err(|e| {
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
        let raw_bytes = base64::decode(&value).map_err(|e| {
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

    /// optional request signing key pair
    #[serde(deserialize_with = "validate_key_pair")]
    pub signing_keypair: Option<DerEncodedKeyPair>,
}

/// sign some junk data and validate it with the key pair
fn sign_junk_data(key_pair: &DerEncodedKeyPair) -> Result<(), anyhow::Error> {
    let private_key = {
        let raw_bytes = base64::decode(&key_pair.private_key)?;
        // TODO: samael does not support ECDSA, update to generic PKey type when it does
        //let parsed = openssl::pkey::PKey::private_key_from_der(&raw_bytes)?;
        let parsed = openssl::rsa::Rsa::private_key_from_der(&raw_bytes)?;
        let parsed = openssl::pkey::PKey::from_rsa(parsed)?;
        parsed
    };

    let public_key = {
        let raw_bytes = base64::decode(&key_pair.public_cert)?;
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

// ORGANIZATIONS

/// Create-time parameters for an [`Organization`](crate::external_api::views::Organization)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct OrganizationCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// Updateable properties of an [`Organization`](crate::external_api::views::Organization)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct OrganizationUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

// PROJECTS

/// Create-time parameters for a [`Project`](crate::external_api::views::Project)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProjectCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// Updateable properties of a [`Project`](crate::external_api::views::Project)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProjectUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

// NETWORK INTERFACES

/// Create-time parameters for a
/// [`NetworkInterface`](omicron_common::api::external::NetworkInterface)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct NetworkInterfaceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The VPC in which to create the interface.
    pub vpc_name: Name,
    /// The VPC Subnet in which to create the interface.
    pub subnet_name: Name,
    /// The IP address for the interface. One will be auto-assigned if not provided.
    pub ip: Option<IpAddr>,
}

/// Parameters for updating a
/// [`NetworkInterface`](omicron_common::api::external::NetworkInterface).
///
/// Note that modifying IP addresses for an interface is not yet supported, a
/// new interface must be created instead.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct NetworkInterfaceUpdate {
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
    // TODO-completeness TODO-docs: When we get there, this should note that a
    // change in the primary interface will result in changes to the DNS records
    // for the instance, though not the name.
    #[serde(default)]
    pub make_primary: bool,
}

// IP POOLS

/// Create-time parameters for an IP Pool.
///
/// See [`IpPool`](omicron_nexus::external_api::views::IpPool)
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

/// Describes an attachment of a `NetworkInterface` to an `Instance`, at the
/// time the instance is created.
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
    /// Create one or more `NetworkInterface`s for the `Instance`.
    ///
    /// If more than one interface is provided, then the first will be
    /// designated the primary interface for the instance.
    Create(Vec<NetworkInterfaceCreate>),

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

/// Create-time parameters for an [`Instance`](omicron_common::api::external::Instance)
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

    /// The disks to be created or attached for this instance.
    #[serde(default)]
    pub disks: Vec<InstanceDiskAttachment>,
}

struct UserData;
impl UserData {
    pub fn serialize<S>(
        data: &Vec<u8>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        base64::encode(data).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match base64::decode(<&str>::deserialize(deserializer)?) {
            Ok(buf) => {
                // if you change this, also update the stress test in crate::cidata
                if buf.len() > crate::cidata::MAX_USER_DATA_BYTES {
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

/// Migration parameters for an [`Instance`](omicron_common::api::external::Instance)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMigrate {
    pub dst_sled_id: Uuid,
}

/// Forwarded to a sled agent to request the contents of an Instance's serial console.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct InstanceSerialConsoleRequest {
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

/// Create-time parameters for a [`Vpc`](crate::external_api::views::Vpc)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The IPv6 prefix for this VPC.
    ///
    /// All IPv6 subnets created from this VPC must be taken from this range,
    /// which sould be a Unique Local Address in the range `fd00::/48`. The
    /// default VPC Subnet will have the first `/64` range from this prefix.
    pub ipv6_prefix: Option<Ipv6Net>,

    pub dns_name: Name,
}

/// Updateable properties of a [`Vpc`](crate::external_api::views::Vpc)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    pub dns_name: Option<Name>,
}

/// Create-time parameters for a [`VpcSubnet`](crate::external_api::views::VpcSubnet)
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

/// Updateable properties of a [`VpcSubnet`](crate::external_api::views::VpcSubnet)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcSubnetUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

// VPC ROUTERS

/// Create-time parameters for a [`VpcRouter`](crate::external_api::views::VpcRouter)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcRouterCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// Updateable properties of a [`VpcRouter`](crate::external_api::views::VpcRouter)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcRouterUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
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
    /// Create a disk from a project image
    Image { image_id: Uuid },
    /// Create a disk from a global image
    GlobalImage { image_id: Uuid },
}

/// Create-time parameters for a [`Disk`](omicron_common::api::external::Disk)
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

const EXTENT_SIZE: u32 = 64_u32 << 20; // 64 MiB

impl DiskCreate {
    pub fn extent_size(&self) -> i64 {
        EXTENT_SIZE as i64
    }

    pub fn extent_count(&self) -> i64 {
        let extent_size = EXTENT_SIZE as i64;
        let size = self.size.to_bytes() as i64;
        size / extent_size
            + ((size % extent_size) + extent_size - 1) / extent_size
    }
}

/// Parameters for the [`Disk`](omicron_common::api::external::Disk) to be
/// attached or detached to an instance
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DiskIdentifier {
    pub name: Name,
}

/// Parameters for the
/// [`NetworkInterface`](omicron_common::api::external::NetworkInterface) to be
/// attached or detached to an instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct NetworkInterfaceIdentifier {
    pub interface_name: Name,
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

/// Create-time parameters for an
/// [`GlobalImage`](omicron_common::api::external::GlobalImage)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct GlobalImageCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// OS image distribution
    pub distribution: Distribution,

    /// block size in bytes
    pub block_size: BlockSize,

    /// The source of the image's contents.
    pub source: ImageSource,
}

/// Create-time parameters for an
/// [`Image`](omicron_common::api::external::Image)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ImageCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// block size in bytes
    pub block_size: BlockSize,

    /// The source of the image's contents.
    pub source: ImageSource,
}

// SNAPSHOTS

/// Create-time parameters for a [`Snapshot`](omicron_common::api::external::Snapshot)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SnapshotCreate {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The name of the disk to be snapshotted
    pub disk: Name,
}

// BUILT-IN USERS
//
// These cannot be created via the external API, but we use the same interfaces
// for creating them internally as we use for types that can be created in the
// external API.

/// Create-time parameters for a [`UserBuiltin`](crate::db::model::UserBuiltin)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UserBuiltinCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

// SSH PUBLIC KEYS
//
// The SSH key mangement endpoints are currently under `/session/me`,
// and so have an implicit silo user ID which must be passed seperately
// to the creation routine. Note that this disagrees with RFD 44.

/// Create-time parameters for an [`SshKey`](crate::external_api::views::SshKey)
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SshKeyCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// SSH public key, e.g., `"ssh-ed25519 AAAAC3NzaC..."`
    pub public_key: String,
}

#[cfg(test)]
mod test {
    use super::*;
    use std::convert::TryFrom;

    const BLOCK_SIZE: u32 = 4096;

    fn new_disk_create_params(size: ByteCount) -> DiskCreate {
        DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: Name::try_from("myobject".to_string()).unwrap(),
                description: "desc".to_string(),
            },
            disk_source: DiskSource::Blank {
                block_size: BlockSize(BLOCK_SIZE),
            },
            size,
        }
    }

    #[test]
    fn test_extent_count() {
        let params = new_disk_create_params(ByteCount::try_from(0u64).unwrap());
        assert_eq!(0, params.extent_count());

        let params = new_disk_create_params(ByteCount::try_from(1u64).unwrap());
        assert_eq!(1, params.extent_count());
        let params = new_disk_create_params(
            ByteCount::try_from(EXTENT_SIZE - 1).unwrap(),
        );
        assert_eq!(1, params.extent_count());
        let params =
            new_disk_create_params(ByteCount::try_from(EXTENT_SIZE).unwrap());
        assert_eq!(1, params.extent_count());

        let params = new_disk_create_params(
            ByteCount::try_from(EXTENT_SIZE + 1).unwrap(),
        );
        assert_eq!(2, params.extent_count());

        // Mostly just checking we don't blow up on an unwrap here.
        let _params =
            new_disk_create_params(ByteCount::try_from(i64::MAX).unwrap());

        // Note that i64::MAX bytes is an invalid disk size as it's not
        // divisible by 4096.
        let max_disk_size = i64::MAX - (i64::MAX % (BLOCK_SIZE as i64));
        let params =
            new_disk_create_params(ByteCount::try_from(max_disk_size).unwrap());
        let blocks_per_extent: u64 =
            params.extent_size() as u64 / BLOCK_SIZE as u64;

        // We should still be rounding up to the nearest extent size.
        assert_eq!(
            params.extent_count() as u128 * EXTENT_SIZE as u128,
            i64::MAX as u128 + 1,
        );

        // Assert that the regions allocated will fit this disk
        assert!(
            params.size.to_bytes() as u64
                <= (params.extent_count() as u64)
                    * blocks_per_extent
                    * BLOCK_SIZE as u64
        );
    }
}
