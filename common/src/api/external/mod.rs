// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Data structures and related facilities for representing resources in the API
//!
//! This includes all representations over the wire for both the external and
//! internal APIs.  The contents here are all HTTP-agnostic.

mod error;
pub mod http_pagination;
use dropshot::HttpError;
pub use error::*;

use anyhow::anyhow;
use anyhow::Context;
use api_identity::ObjectIdentity;
use chrono::DateTime;
use chrono::Utc;
pub use dropshot::PaginationOrder;
use futures::future::ready;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use parse_display::Display;
use parse_display::FromStr;
use schemars::JsonSchema;
use semver;
use serde::Deserialize;
use serde::Serialize;
use serde_with::{DeserializeFromStr, SerializeDisplay};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FormatResult;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::num::{NonZeroU16, NonZeroU32};
use std::str::FromStr;
use uuid::Uuid;

// The type aliases below exist primarily to ensure consistency among return
// types for functions in the `nexus::Nexus` and `nexus::DataStore`.  The
// type argument `T` generally implements `Object`.

/// Result of a create operation for the specified type
pub type CreateResult<T> = Result<T, Error>;
/// Result of a delete operation for the specified type
pub type DeleteResult = Result<(), Error>;
/// Result of a list operation that returns an ObjectStream
pub type ListResult<T> = Result<ObjectStream<T>, Error>;
/// Result of a list operation that returns a vector
pub type ListResultVec<T> = Result<Vec<T>, Error>;
/// Result of a lookup operation for the specified type
pub type LookupResult<T> = Result<T, Error>;
/// Result of an update operation for the specified type
pub type UpdateResult<T> = Result<T, Error>;
/// Result of an optional lookup operation for the specified type
pub type OptionalLookupResult<T> = Result<Option<T>, Error>;

/// A stream of Results, each potentially representing an object in the API
pub type ObjectStream<T> = BoxStream<'static, Result<T, Error>>;

// General-purpose types used for client request parameters and return values.

/// Describes an `Object` that has its own identity metadata.  This is
/// currently used only for pagination.
pub trait ObjectIdentity {
    fn identity(&self) -> &IdentityMetadata;
}

/// Parameters used to request a specific page of results when listing a
/// collection of objects
///
/// This is logically analogous to Dropshot's `PageSelector` (plus the limit from
/// Dropshot's `PaginationParams).  However, this type is HTTP-agnostic.  More
/// importantly, by the time this struct is generated, we know the type of the
/// sort field and we can specialize `DataPageParams` to that type.  This makes
/// it considerably simpler to implement the backend for most of our paginated
/// APIs.
///
/// `NameType` is the type of the field used to sort the returned values and it's
/// usually `Name`.
#[derive(Debug)]
pub struct DataPageParams<'a, NameType> {
    /// If present, this is the value of the sort field for the last object seen
    pub marker: Option<&'a NameType>,

    /// Whether the sort is in ascending order
    pub direction: PaginationOrder,

    /// This identifies how many results should be returned on this page.
    /// Backend implementations must provide this many results unless we're at
    /// the end of the scan.  Dropshot assumes that if we provide fewer results
    /// than this number, then we're done with the scan.
    pub limit: NonZeroU32,
}

impl<'a, NameType> DataPageParams<'a, NameType> {
    /// Maps the marker type to a new type.
    ///
    /// Equivalent to [std::option::Option::map], because that's what it calls.
    pub fn map_name<OtherName, F>(&self, f: F) -> DataPageParams<'a, OtherName>
    where
        F: FnOnce(&'a NameType) -> &'a OtherName,
    {
        DataPageParams {
            marker: self.marker.map(f),
            direction: self.direction,
            limit: self.limit,
        }
    }
}

impl<'a> TryFrom<&DataPageParams<'a, NameOrId>> for DataPageParams<'a, Name> {
    type Error = HttpError;

    fn try_from(
        value: &DataPageParams<'a, NameOrId>,
    ) -> Result<Self, Self::Error> {
        match value.marker {
            Some(NameOrId::Name(name)) => Ok(DataPageParams {
                marker: Some(name),
                direction: value.direction,
                limit: value.limit,
            }),
            None => Ok(DataPageParams {
                marker: None,
                direction: value.direction,
                limit: value.limit,
            }),
            _ => Err(HttpError::for_bad_request(
                None,
                String::from("invalid pagination marker"),
            )),
        }
    }
}

impl<'a> TryFrom<&DataPageParams<'a, NameOrId>> for DataPageParams<'a, Uuid> {
    type Error = HttpError;

    fn try_from(
        value: &DataPageParams<'a, NameOrId>,
    ) -> Result<Self, Self::Error> {
        match value.marker {
            Some(NameOrId::Id(id)) => Ok(DataPageParams {
                marker: Some(id),
                direction: value.direction,
                limit: value.limit,
            }),
            None => Ok(DataPageParams {
                marker: None,
                direction: value.direction,
                limit: value.limit,
            }),
            _ => Err(HttpError::for_bad_request(
                None,
                String::from("invalid pagination marker"),
            )),
        }
    }
}

/// A name used in the API
///
/// Names are generally user-provided unique identifiers, highly constrained as
/// described in RFD 4.  An `Name` can only be constructed with a string
/// that's valid as a name.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[display("{0}")]
#[serde(try_from = "String")]
pub struct Name(String);

/// `Name::try_from(String)` is the primary method for constructing an Name
/// from an input string.  This validates the string according to our
/// requirements for a name.
/// TODO-cleanup why shouldn't callers use TryFrom<&str>?
impl TryFrom<String> for Name {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() > 63 {
            return Err(String::from("name may contain at most 63 characters"));
        }

        let mut iter = value.chars();

        let first = iter.next().ok_or_else(|| {
            String::from("name requires at least one character")
        })?;
        if !first.is_ascii_lowercase() {
            return Err(String::from(
                "name must begin with an ASCII lowercase character",
            ));
        }

        let mut last = first;
        for c in iter {
            last = c;

            if !c.is_ascii_lowercase() && !c.is_digit(10) && c != '-' {
                return Err(format!(
                    "name contains invalid character: \"{}\" (allowed \
                     characters are lowercase ASCII, digits, and \"-\")",
                    c
                ));
            }
        }

        if last == '-' {
            return Err(String::from("name cannot end with \"-\""));
        }

        if Uuid::parse_str(&value).is_ok() {
            return Err(String::from(
                "name cannot be a UUID to avoid ambiguity with IDs",
            ));
        }

        Ok(Name(value))
    }
}

impl FromStr for Name {
    // TODO: We should have better error types here.
    // See https://github.com/oxidecomputer/omicron/issues/347
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Name::try_from(String::from(value))
    }
}

impl<'a> From<&'a Name> for &'a str {
    fn from(n: &'a Name) -> Self {
        n.as_str()
    }
}

/// `Name` instances are comparable like Strings, primarily so that they can
/// be used as keys in trees.
impl<S> PartialEq<S> for Name
where
    S: AsRef<str>,
{
    fn eq(&self, other: &S) -> bool {
        self.0 == other.as_ref()
    }
}

/// Custom JsonSchema implementation to encode the constraints on Name.
impl JsonSchema for Name {
    fn schema_name() -> String {
        "Name".to_string()
    }
    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some(
                    "A name unique within the parent collection".to_string(),
                ),
                description: Some(
                    "Names must begin with a lower case ASCII letter, be \
                     composed exclusively of lowercase ASCII, uppercase \
                     ASCII, numbers, and '-', and may not end with a '-'. \
                     Names cannot be a UUID though they may contain a UUID."
                        .to_string(),
                ),
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            string: Some(Box::new(schemars::schema::StringValidation {
                max_length: Some(63),
                min_length: Some(1),
                pattern: Some(
                    concat!(
                        r#"^"#,
                        // Cannot match a UUID
                        r#"(?![0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$)"#,
                        r#"^[a-z][a-z0-9-]*[a-zA-Z0-9]*"#,
                        r#"$"#,
                    )
                    .to_string(),
                )
            })),
            ..Default::default()
        }
        .into()
    }
}

impl Name {
    /// Parse an `Name`.  This is a convenience wrapper around
    /// `Name::try_from(String)` that marshals any error into an appropriate
    /// `Error`.
    pub fn from_param(value: String, label: &str) -> Result<Name, Error> {
        value.parse().map_err(|e| Error::InvalidValue {
            label: String::from(label),
            message: e,
        })
    }

    /// Return the `&str` representing the actual name.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Serialize, Deserialize, Display, Clone, PartialEq)]
#[display("{0}")]
#[serde(untagged)]
pub enum NameOrId {
    Id(Uuid),
    Name(Name),
}

impl TryFrom<String> for NameOrId {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if let Ok(id) = Uuid::parse_str(&value) {
            Ok(NameOrId::Id(id))
        } else {
            Ok(NameOrId::Name(Name::try_from(value)?))
        }
    }
}

impl From<Name> for NameOrId {
    fn from(name: Name) -> Self {
        NameOrId::Name(name)
    }
}

impl From<Uuid> for NameOrId {
    fn from(id: Uuid) -> Self {
        NameOrId::Id(id)
    }
}

impl JsonSchema for NameOrId {
    fn schema_name() -> String {
        "NameOrId".to_string()
    }

    fn json_schema(
        gen: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            subschemas: Some(Box::new(schemars::schema::SubschemaValidation {
                one_of: Some(vec![
                    label_schema("id", gen.subschema_for::<Uuid>()),
                    label_schema("name", gen.subschema_for::<Name>()),
                ]),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

// TODO: remove wrapper for semver::Version once this PR goes through
// https://github.com/GREsau/schemars/pull/195
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Display,
    FromStr,
)]
#[display("{0}")]
#[serde(transparent)]
pub struct SemverVersion(pub semver::Version);

impl SemverVersion {
    pub fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self(semver::Version::new(major, minor, patch))
    }

    /// This is the official ECMAScript-compatible validation regex for
    /// semver:
    /// https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
    const VALIDATION_REGEX: &str = r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$";
}

impl JsonSchema for SemverVersion {
    fn schema_name() -> String {
        "SemverVersion".to_string()
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            string: Some(Box::new(schemars::schema::StringValidation {
                pattern: Some(Self::VALIDATION_REGEX.to_owned()),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

/// Name for a built-in role
#[derive(
    Clone,
    Debug,
    DeserializeFromStr,
    Display,
    Eq,
    FromStr,
    Ord,
    PartialEq,
    PartialOrd,
    SerializeDisplay,
)]
#[display("{resource_type}.{role_name}")]
pub struct RoleName {
    // "resource_type" is generally the String value of one of the
    // `ResourceType` variants.  We could store the parsed `ResourceType`
    // instead, but it's useful to be able to represent RoleNames for resource
    // types that we don't know about.  That could happen if we happen to find
    // them in the database, for example.
    #[from_str(regex = "[a-z-]+")]
    resource_type: String,
    #[from_str(regex = "[a-z-]+")]
    role_name: String,
}

impl RoleName {
    pub fn new(resource_type: &str, role_name: &str) -> RoleName {
        RoleName {
            resource_type: String::from(resource_type),
            role_name: String::from(role_name),
        }
    }
}

/// Custom JsonSchema implementation to encode the constraints on RoleName
impl JsonSchema for RoleName {
    fn schema_name() -> String {
        "RoleName".to_string()
    }
    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some("A name for a built-in role".to_string()),
                description: Some(
                    "Role names consist of two string components \
                     separated by dot (\".\")."
                        .to_string(),
                ),
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::SingleOrVec::Single(
                Box::new(schemars::schema::InstanceType::String),
            )),
            string: Some(Box::new(schemars::schema::StringValidation {
                max_length: Some(63),
                min_length: None,
                pattern: Some("[a-z-]+\\.[a-z-]+".to_string()),
            })),
            ..Default::default()
        })
    }
}

/// A count of bytes, typically used either for memory or storage capacity
///
/// The maximum supported byte count is [`i64::MAX`].  This makes it somewhat
/// inconvenient to define constructors: a u32 constructor can be infallible, but
/// an i64 constructor can fail (if the value is negative) and a u64 constructor
/// can fail (if the value is larger than i64::MAX).  We provide all of these for
/// consumers' convenience.
// TODO-cleanup This could benefit from a more complete implementation.
//
// The maximum byte count of i64::MAX comes from the fact that this is stored in
// the database as an i64.  Constraining it here ensures that we can't fail to
// serialize the value.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct ByteCount(u64);

#[allow(non_upper_case_globals)]
const KiB: u64 = 1024;
#[allow(non_upper_case_globals)]
const MiB: u64 = KiB * 1024;
#[allow(non_upper_case_globals)]
const GiB: u64 = MiB * 1024;
#[allow(non_upper_case_globals)]
const TiB: u64 = GiB * 1024;

impl ByteCount {
    pub fn from_kibibytes_u32(kibibytes: u32) -> ByteCount {
        ByteCount::try_from(KiB * u64::from(kibibytes)).unwrap()
    }

    pub fn from_mebibytes_u32(mebibytes: u32) -> ByteCount {
        ByteCount::try_from(MiB * u64::from(mebibytes)).unwrap()
    }

    pub fn from_gibibytes_u32(gibibytes: u32) -> ByteCount {
        ByteCount::try_from(GiB * u64::from(gibibytes)).unwrap()
    }

    pub fn to_bytes(&self) -> u64 {
        self.0
    }
    pub fn to_whole_kibibytes(&self) -> u64 {
        self.to_bytes() / KiB
    }
    pub fn to_whole_mebibytes(&self) -> u64 {
        self.to_bytes() / MiB
    }
    pub fn to_whole_gibibytes(&self) -> u64 {
        self.to_bytes() / GiB
    }
    pub fn to_whole_tebibytes(&self) -> u64 {
        self.to_bytes() / TiB
    }
}

impl Display for ByteCount {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        if self.to_bytes() >= TiB && self.to_bytes() % TiB == 0 {
            write!(f, "{} TiB", self.to_whole_tebibytes())
        } else if self.to_bytes() >= GiB && self.to_bytes() % GiB == 0 {
            write!(f, "{} GiB", self.to_whole_gibibytes())
        } else if self.to_bytes() >= MiB && self.to_bytes() % MiB == 0 {
            write!(f, "{} MiB", self.to_whole_mebibytes())
        } else if self.to_bytes() >= KiB && self.to_bytes() % KiB == 0 {
            write!(f, "{} KiB", self.to_whole_kibibytes())
        } else {
            write!(f, "{} B", self.to_bytes())
        }
    }
}

// TODO-cleanup This could use the experimental std::num::IntErrorKind.
#[derive(Debug, Eq, thiserror::Error, Ord, PartialEq, PartialOrd)]
pub enum ByteCountRangeError {
    #[error("value is too small for a byte count")]
    TooSmall,
    #[error("value is too large for a byte count")]
    TooLarge,
}
impl TryFrom<u64> for ByteCount {
    type Error = ByteCountRangeError;

    fn try_from(bytes: u64) -> Result<Self, Self::Error> {
        if i64::try_from(bytes).is_err() {
            Err(ByteCountRangeError::TooLarge)
        } else {
            Ok(ByteCount(bytes))
        }
    }
}

impl TryFrom<i64> for ByteCount {
    type Error = ByteCountRangeError;

    fn try_from(bytes: i64) -> Result<Self, Self::Error> {
        Ok(ByteCount(
            u64::try_from(bytes).map_err(|_| ByteCountRangeError::TooSmall)?,
        ))
    }
}

impl From<u32> for ByteCount {
    fn from(value: u32) -> Self {
        ByteCount(u64::from(value))
    }
}

impl From<ByteCount> for i64 {
    fn from(b: ByteCount) -> Self {
        // We have already validated that this value is in range.
        i64::try_from(b.0).unwrap()
    }
}

/// Generation numbers stored in the database, used for optimistic concurrency
/// control
// Because generation numbers are stored in the database, we represent them as
// i64.
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Eq,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
pub struct Generation(u64);

impl Generation {
    pub fn new() -> Generation {
        Generation(1)
    }

    pub fn next(&self) -> Generation {
        // It should technically be an operational error if this wraps or even
        // exceeds the value allowed by an i64.  But it seems unlikely enough to
        // happen in practice that we can probably feel safe with this.
        let next_gen = self.0 + 1;
        assert!(next_gen <= u64::try_from(i64::MAX).unwrap());
        Generation(next_gen)
    }
}

impl Display for Generation {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        f.write_str(&self.0.to_string())
    }
}

impl From<&Generation> for i64 {
    fn from(g: &Generation) -> Self {
        // We have already validated that the value is within range.
        // TODO-robustness We need to ensure that we don't deserialize a value
        // out of range here.
        i64::try_from(g.0).unwrap()
    }
}

impl TryFrom<i64> for Generation {
    type Error = anyhow::Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Ok(Generation(
            u64::try_from(value)
                .map_err(|_| anyhow!("generation number too large"))?,
        ))
    }
}

// General types used to implement API resources

/// Identifies a type of API resource
#[derive(
    Clone,
    Copy,
    Debug,
    DeserializeFromStr,
    Display,
    Eq,
    FromStr,
    Ord,
    PartialEq,
    PartialOrd,
    SerializeDisplay,
)]
#[display(style = "kebab-case")]
pub enum ResourceType {
    Fleet,
    Silo,
    SiloUser,
    SiloGroup,
    IdentityProvider,
    SamlIdentityProvider,
    SshKey,
    Certificate,
    ConsoleSession,
    DeviceAuthRequest,
    DeviceAccessToken,
    GlobalImage,
    Project,
    Dataset,
    Disk,
    Image,
    Instance,
    IpPool,
    NetworkInterface,
    PhysicalDisk,
    Rack,
    Service,
    Sled,
    SagaDbg,
    Snapshot,
    Volume,
    Vpc,
    VpcFirewallRule,
    VpcSubnet,
    VpcRouter,
    RouterRoute,
    Oximeter,
    MetricProducer,
    RoleBuiltin,
    UpdateAvailableArtifact,
    SystemUpdate,
    ComponentUpdate,
    SystemUpdateComponentUpdate,
    UpdateDeployment,
    UpdateableComponent,
    UserBuiltin,
    Zpool,
}

pub async fn to_list<T, U>(object_stream: ObjectStream<T>) -> Vec<U>
where
    T: Into<U>,
{
    object_stream
        .filter(|maybe_object| ready(maybe_object.is_ok()))
        .map(|maybe_object| maybe_object.unwrap().into())
        .collect::<Vec<U>>()
        .await
}

// IDENTITY METADATA

/// Identity-related metadata that's included in nearly all public API objects
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, JsonSchema)]
pub struct IdentityMetadata {
    /// unique, immutable, system-controlled identifier for each resource
    pub id: Uuid,
    /// unique, mutable, user-controlled identifier for each resource
    pub name: Name,
    /// human-readable free-form text about a resource
    pub description: String,
    /// timestamp when this resource was created
    pub time_created: DateTime<Utc>,
    /// timestamp when this resource was last modified
    pub time_modified: DateTime<Utc>,
}

/// Create-time identity-related parameters
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IdentityMetadataCreateParams {
    pub name: Name,
    pub description: String,
}

/// Updateable identity-related parameters
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct IdentityMetadataUpdateParams {
    pub name: Option<Name>,
    pub description: Option<String>,
}

// Specific API resources

// INSTANCES

/// Running state of an Instance (primarily: booted or stopped)
///
/// This typically reflects whether it's starting, running, stopping, or stopped,
/// but also includes states related to the Instance's lifecycle
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
// TODO-polish: RFD 315
pub enum InstanceState {
    /// The instance is being created.
    Creating,
    /// The instance is currently starting up.
    Starting,
    /// The instance is currently running.
    Running,
    /// The instance has been requested to stop and a transition to "Stopped" is imminent.
    Stopping,
    /// The instance is currently stopped.
    Stopped,
    /// The instance is in the process of rebooting - it will remain
    /// in the "rebooting" state until the VM is starting once more.
    Rebooting,
    /// The instance is in the process of migrating - it will remain
    /// in the "migrating" state until the migration process is complete
    /// and the destination propolis is ready to continue execution.
    Migrating,
    /// The instance is attempting to recover from a failure.
    Repairing,
    /// The instance has encountered a failure.
    Failed,
    /// The instance has been deleted.
    Destroyed,
}

impl Display for InstanceState {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(f, "{}", self.label())
    }
}

// TODO-cleanup why is this error type different from the one for Name?  The
// reason is probably that Name can be provided by the user, so we want a
// good validation error.  InstanceState cannot.  Still, is there a way to
// unify these?
impl TryFrom<&str> for InstanceState {
    type Error = String;

    fn try_from(variant: &str) -> Result<Self, Self::Error> {
        let r = match variant {
            "creating" => InstanceState::Creating,
            "starting" => InstanceState::Starting,
            "running" => InstanceState::Running,
            "stopping" => InstanceState::Stopping,
            "stopped" => InstanceState::Stopped,
            "rebooting" => InstanceState::Rebooting,
            "migrating" => InstanceState::Migrating,
            "repairing" => InstanceState::Repairing,
            "failed" => InstanceState::Failed,
            "destroyed" => InstanceState::Destroyed,
            _ => return Err(format!("Unexpected variant {}", variant)),
        };
        Ok(r)
    }
}

impl InstanceState {
    pub fn label(&self) -> &'static str {
        match self {
            InstanceState::Creating => "creating",
            InstanceState::Starting => "starting",
            InstanceState::Running => "running",
            InstanceState::Stopping => "stopping",
            InstanceState::Stopped => "stopped",
            InstanceState::Rebooting => "rebooting",
            InstanceState::Migrating => "migrating",
            InstanceState::Repairing => "repairing",
            InstanceState::Failed => "failed",
            InstanceState::Destroyed => "destroyed",
        }
    }

    /// Returns true if the given state represents a fully stopped Instance.
    /// This means that a transition from an !is_stopped() state must go
    /// through Stopping.
    pub fn is_stopped(&self) -> bool {
        match self {
            InstanceState::Starting => false,
            InstanceState::Running => false,
            InstanceState::Stopping => false,
            InstanceState::Rebooting => false,
            InstanceState::Migrating => false,

            InstanceState::Creating => true,
            InstanceState::Stopped => true,
            InstanceState::Repairing => true,
            InstanceState::Failed => true,
            InstanceState::Destroyed => true,
        }
    }
}

/// The number of CPUs in an Instance
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCpuCount(pub u16);

impl TryFrom<i64> for InstanceCpuCount {
    type Error = anyhow::Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Ok(InstanceCpuCount(u16::try_from(value).context("parsing CPU count")?))
    }
}

impl From<&InstanceCpuCount> for i64 {
    fn from(c: &InstanceCpuCount) -> Self {
        i64::from(c.0)
    }
}

/// Client view of an [`InstanceRuntimeState`]
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceRuntimeState {
    pub run_state: InstanceState,
    pub time_run_state_updated: DateTime<Utc>,
}

impl From<crate::api::internal::nexus::InstanceRuntimeState>
    for InstanceRuntimeState
{
    fn from(state: crate::api::internal::nexus::InstanceRuntimeState) -> Self {
        InstanceRuntimeState {
            run_state: state.run_state,
            time_run_state_updated: state.time_updated,
        }
    }
}

/// Client view of an [`Instance`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Instance {
    // TODO is flattening here the intent in RFD 4?
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// id for the project containing this Instance
    pub project_id: Uuid,

    /// number of CPUs allocated for this Instance
    pub ncpus: InstanceCpuCount,
    /// memory allocated for this Instance
    pub memory: ByteCount,
    /// RFC1035-compliant hostname for the Instance.
    pub hostname: String, // TODO-cleanup different type?

    #[serde(flatten)]
    pub runtime: InstanceRuntimeState,
}

// DISKS

/// Client view of a [`Disk`]
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Disk {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub project_id: Uuid,
    pub snapshot_id: Option<Uuid>,
    pub image_id: Option<Uuid>,
    pub size: ByteCount,
    pub block_size: ByteCount,
    pub state: DiskState,
    pub device_path: String,
}

/// State of a Disk (primarily: attached or not)
#[derive(
    Clone,
    Debug,
    Deserialize,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    JsonSchema,
)]
#[serde(tag = "state", content = "instance", rename_all = "snake_case")]
pub enum DiskState {
    /// Disk is being initialized
    Creating,
    /// Disk is ready but detached from any Instance
    Detached,
    /// Disk is ready to receive blocks from an external source
    ImportReady,
    /// Disk is importing blocks from a URL
    ImportingFromUrl,
    /// Disk is importing blocks from bulk writes
    ImportingFromBulkWrites,
    /// Disk is being finalized to state Detached
    Finalizing,
    /// Disk is undergoing maintenance
    Maintenance,
    /// Disk is being attached to the given Instance
    Attaching(Uuid), // attached Instance id
    /// Disk is attached to the given Instance
    Attached(Uuid), // attached Instance id
    /// Disk is being detached from the given Instance
    Detaching(Uuid), // attached Instance id
    /// Disk has been destroyed
    Destroyed,
    /// Disk is unavailable
    Faulted,
}

impl Display for DiskState {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(f, "{}", self.label())
    }
}

impl TryFrom<(&str, Option<Uuid>)> for DiskState {
    type Error = String;

    fn try_from(
        (s, maybe_id): (&str, Option<Uuid>),
    ) -> Result<Self, Self::Error> {
        match (s, maybe_id) {
            ("creating", None) => Ok(DiskState::Creating),
            ("detached", None) => Ok(DiskState::Detached),
            ("import_ready", None) => Ok(DiskState::ImportReady),
            ("importing_from_url", None) => Ok(DiskState::ImportingFromUrl),
            ("importing_from_bulk_writes", None) => {
                Ok(DiskState::ImportingFromBulkWrites)
            }
            ("finalizing", None) => Ok(DiskState::Finalizing),
            ("maintenance", None) => Ok(DiskState::Maintenance),
            ("destroyed", None) => Ok(DiskState::Destroyed),
            ("faulted", None) => Ok(DiskState::Faulted),
            ("attaching", Some(id)) => Ok(DiskState::Attaching(id)),
            ("attached", Some(id)) => Ok(DiskState::Attached(id)),
            ("detaching", Some(id)) => Ok(DiskState::Detaching(id)),
            _ => Err(format!(
                "unexpected value for disk state: {:?} with attached id {:?}",
                s, maybe_id
            )),
        }
    }
}

impl DiskState {
    /// Returns the string label for this disk state
    pub fn label(&self) -> &'static str {
        match self {
            DiskState::Creating => "creating",
            DiskState::Detached => "detached",
            DiskState::ImportReady => "import_ready",
            DiskState::ImportingFromUrl => "importing_from_url",
            DiskState::ImportingFromBulkWrites => "importing_from_bulk_writes",
            DiskState::Finalizing => "finalizing",
            DiskState::Maintenance => "maintenance",
            DiskState::Attaching(_) => "attaching",
            DiskState::Attached(_) => "attached",
            DiskState::Detaching(_) => "detaching",
            DiskState::Destroyed => "destroyed",
            DiskState::Faulted => "faulted",
        }
    }

    /// Returns whether the Disk is currently attached to, being attached to, or
    /// being detached from any Instance.
    pub fn is_attached(&self) -> bool {
        self.attached_instance_id().is_some()
    }

    /// If the Disk is attached to, being attached to, or being detached from an
    /// Instance, returns the id for that Instance.  Otherwise returns `None`.
    pub fn attached_instance_id(&self) -> Option<&Uuid> {
        match self {
            DiskState::Attaching(id) => Some(id),
            DiskState::Attached(id) => Some(id),
            DiskState::Detaching(id) => Some(id),

            DiskState::Creating => None,
            DiskState::Detached => None,
            DiskState::ImportReady => None,
            DiskState::ImportingFromUrl => None,
            DiskState::ImportingFromBulkWrites => None,
            DiskState::Finalizing => None,
            DiskState::Maintenance => None,
            DiskState::Destroyed => None,
            DiskState::Faulted => None,
        }
    }
}

// Sagas
//
// These are currently only intended for observability by developers.  We will
// eventually want to flesh this out into something more observable for end
// users.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct Saga {
    pub id: Uuid,
    pub state: SagaState,
}

impl From<steno::SagaView> for Saga {
    fn from(s: steno::SagaView) -> Self {
        Saga { id: Uuid::from(s.id), state: SagaState::from(s.state) }
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SagaState {
    Running,
    Succeeded,
    Failed { error_node_name: steno::NodeName, error_info: SagaErrorInfo },
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(tag = "error", rename_all = "snake_case")]
pub enum SagaErrorInfo {
    ActionFailed { source_error: serde_json::Value },
    DeserializeFailed { message: String },
    InjectedError,
    SerializeFailed { message: String },
    SubsagaCreateFailed { message: String },
}

impl From<steno::SagaStateView> for SagaState {
    fn from(st: steno::SagaStateView) -> Self {
        match st {
            steno::SagaStateView::Ready { .. } => SagaState::Running,
            steno::SagaStateView::Running { .. } => SagaState::Running,
            steno::SagaStateView::Done {
                result: steno::SagaResult { kind: Ok(_), .. },
                ..
            } => SagaState::Succeeded,
            steno::SagaStateView::Done {
                result: steno::SagaResult { kind: Err(e), .. },
                ..
            } => SagaState::Failed {
                error_node_name: e.error_node_name,
                error_info: match e.error_source {
                    steno::ActionError::ActionFailed { source_error } => {
                        SagaErrorInfo::ActionFailed { source_error }
                    }
                    steno::ActionError::DeserializeFailed { message } => {
                        SagaErrorInfo::DeserializeFailed { message }
                    }
                    steno::ActionError::InjectedError => {
                        SagaErrorInfo::InjectedError
                    }
                    steno::ActionError::SerializeFailed { message } => {
                        SagaErrorInfo::SerializeFailed { message }
                    }
                    steno::ActionError::SubsagaCreateFailed { message } => {
                        SagaErrorInfo::SubsagaCreateFailed { message }
                    }
                },
            },
        }
    }
}

/// An `Ipv4Net` represents a IPv4 subnetwork, including the address and network mask.
#[derive(Clone, Copy, Debug, Deserialize, Hash, PartialEq, Serialize)]
pub struct Ipv4Net(pub ipnetwork::Ipv4Network);

impl Ipv4Net {
    /// Return `true` if this IPv4 subnetwork is from an RFC 1918 private
    /// address space.
    pub fn is_private(&self) -> bool {
        self.0.network().is_private()
    }
}

impl std::ops::Deref for Ipv4Net {
    type Target = ipnetwork::Ipv4Network;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for Ipv4Net {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl JsonSchema for Ipv4Net {
    fn schema_name() -> String {
        "Ipv4Net".to_string()
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some("An IPv4 subnet".to_string()),
                description: Some(
                    "An IPv4 subnet, including prefix and subnet mask"
                        .to_string(),
                ),
                examples: vec!["192.168.1.0/24".into()],
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            string: Some(Box::new(schemars::schema::StringValidation {
                pattern: Some(
                    concat!(
                        r#"^(([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\.){3}"#,
                        r#"([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])"#,
                        r#"/([8-9]|1[0-9]|2[0-9]|3[0-2])$"#,
                    )
                    .to_string(),
                ),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

/// An `Ipv6Net` represents a IPv6 subnetwork, including the address and network mask.
#[derive(Clone, Copy, Debug, Deserialize, Hash, PartialEq, Eq, Serialize)]
pub struct Ipv6Net(pub ipnetwork::Ipv6Network);

impl Ipv6Net {
    /// The length for all VPC IPv6 prefixes
    pub const VPC_IPV6_PREFIX_LENGTH: u8 = 48;

    /// The prefix length for all VPC Sunets
    pub const VPC_SUBNET_IPV6_PREFIX_LENGTH: u8 = 64;

    /// Return `true` if this subnetwork is in the IPv6 Unique Local Address
    /// range defined in RFC 4193, e.g., `fd00:/8`
    pub fn is_unique_local(&self) -> bool {
        // TODO: Delegate to `Ipv6Addr::is_unique_local()` when stabilized.
        self.0.network().octets()[0] == 0xfd
    }

    /// Return `true` if this subnetwork is a valid VPC prefix.
    ///
    /// This checks that the subnet is a unique local address, and has the VPC
    /// prefix length required.
    pub fn is_vpc_prefix(&self) -> bool {
        self.is_unique_local()
            && self.0.prefix() == Self::VPC_IPV6_PREFIX_LENGTH
    }

    /// Return `true` if this subnetwork is a valid VPC Subnet, given the VPC's
    /// prefix.
    pub fn is_vpc_subnet(&self, vpc_prefix: &Ipv6Net) -> bool {
        self.is_unique_local()
            && self.is_subnet_of(vpc_prefix.0)
            && self.prefix() == Self::VPC_SUBNET_IPV6_PREFIX_LENGTH
    }
}

impl std::ops::Deref for Ipv6Net {
    type Target = ipnetwork::Ipv6Network;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for Ipv6Net {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<ipnetwork::Ipv6Network> for Ipv6Net {
    fn from(n: ipnetwork::Ipv6Network) -> Ipv6Net {
        Self(n)
    }
}

impl JsonSchema for Ipv6Net {
    fn schema_name() -> String {
        "Ipv6Net".to_string()
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some("An IPv6 subnet".to_string()),
                description: Some(
                    "An IPv6 subnet, including prefix and subnet mask"
                        .to_string(),
                ),
                examples: vec!["fd12:3456::/64".into()],
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            string: Some(Box::new(schemars::schema::StringValidation {
                pattern: Some(
                    // Conforming to unique local addressing scheme,
                    // `fd00::/8`.
                    concat!(
                        r#"^([fF][dD])[0-9a-fA-F]{2}:("#,
                        r#"([0-9a-fA-F]{1,4}:){6}[0-9a-fA-F]{1,4}"#,
                        r#"|([0-9a-fA-F]{1,4}:){1,6}:)"#,
                        r#"\/([1-9]|[1-9][0-9]|1[0-1][0-9]|12[0-8])$"#,
                    )
                    .to_string(),
                ),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

/// An `IpNet` represents an IP network, either IPv4 or IPv6.
#[derive(Clone, Copy, Debug, PartialEq, Hash)]
pub enum IpNet {
    V4(Ipv4Net),
    V6(Ipv6Net),
}

impl IpNet {
    /// Return the first address in this subnet
    pub fn first_address(&self) -> IpAddr {
        match self {
            IpNet::V4(inner) => IpAddr::from(inner.iter().next().unwrap()),
            IpNet::V6(inner) => IpAddr::from(inner.iter().next().unwrap()),
        }
    }

    /// Return the last address in this subnet.
    ///
    /// For a subnet of size 1, e.g., a /32, this is the same as the first
    /// address.
    // NOTE: This is a workaround for the fact that the `ipnetwork` crate's
    // iterator provides only the `Iterator::next()` method. That means that
    // finding the last address is linear in the size of the subnet, which is
    // completely untenable and totally avoidable with some addition. In the
    // long term, we should either put up a patch to the `ipnetwork` crate or
    // move the `ipnet` crate, which does provide an efficient iterator
    // implementation.
    pub fn last_address(&self) -> IpAddr {
        match self {
            IpNet::V4(inner) => {
                let base: u32 = inner.network().into();
                let size = inner.size() - 1;
                std::net::IpAddr::V4(std::net::Ipv4Addr::from(base + size))
            }
            IpNet::V6(inner) => {
                let base: u128 = inner.network().into();
                let size = inner.size() - 1;
                std::net::IpAddr::V6(std::net::Ipv6Addr::from(base + size))
            }
        }
    }
}

impl From<ipnetwork::IpNetwork> for IpNet {
    fn from(n: ipnetwork::IpNetwork) -> Self {
        match n {
            ipnetwork::IpNetwork::V4(v4) => IpNet::V4(Ipv4Net(v4)),
            ipnetwork::IpNetwork::V6(v6) => IpNet::V6(Ipv6Net(v6)),
        }
    }
}

impl From<Ipv4Net> for IpNet {
    fn from(n: Ipv4Net) -> IpNet {
        IpNet::V4(n)
    }
}

impl From<Ipv4Addr> for IpNet {
    fn from(n: Ipv4Addr) -> IpNet {
        IpNet::V4(Ipv4Net(ipnetwork::Ipv4Network::from(n)))
    }
}

impl From<Ipv6Net> for IpNet {
    fn from(n: Ipv6Net) -> IpNet {
        IpNet::V6(n)
    }
}

impl From<Ipv6Addr> for IpNet {
    fn from(n: Ipv6Addr) -> IpNet {
        IpNet::V6(Ipv6Net(ipnetwork::Ipv6Network::from(n)))
    }
}

impl From<IpAddr> for IpNet {
    fn from(n: IpAddr) -> IpNet {
        match n {
            IpAddr::V4(v4) => IpNet::from(v4),
            IpAddr::V6(v6) => IpNet::from(v6),
        }
    }
}

impl std::fmt::Display for IpNet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IpNet::V4(inner) => write!(f, "{}", inner),
            IpNet::V6(inner) => write!(f, "{}", inner),
        }
    }
}

impl FromStr for IpNet {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let net =
            s.parse::<ipnetwork::IpNetwork>().map_err(|e| e.to_string())?;
        match net {
            ipnetwork::IpNetwork::V4(net) => Ok(IpNet::from(Ipv4Net(net))),
            ipnetwork::IpNetwork::V6(net) => Ok(IpNet::from(Ipv6Net(net))),
        }
    }
}

impl From<IpNet> for ipnetwork::IpNetwork {
    fn from(net: IpNet) -> ipnetwork::IpNetwork {
        match net {
            IpNet::V4(net) => ipnetwork::IpNetwork::from(net.0),
            IpNet::V6(net) => ipnetwork::IpNetwork::from(net.0),
        }
    }
}

impl Serialize for IpNet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            IpNet::V4(v4) => v4.serialize(serializer),
            IpNet::V6(v6) => v6.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for IpNet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let net = ipnetwork::IpNetwork::deserialize(deserializer)?;
        match net {
            ipnetwork::IpNetwork::V4(net) => Ok(IpNet::from(Ipv4Net(net))),
            ipnetwork::IpNetwork::V6(net) => Ok(IpNet::from(Ipv6Net(net))),
        }
    }
}

impl JsonSchema for IpNet {
    fn schema_name() -> String {
        "IpNet".to_string()
    }

    fn json_schema(
        gen: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            subschemas: Some(Box::new(schemars::schema::SubschemaValidation {
                one_of: Some(vec![
                    label_schema("v4", gen.subschema_for::<Ipv4Net>()),
                    label_schema("v6", gen.subschema_for::<Ipv6Net>()),
                ]),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

/// Insert another level of schema indirection in order to provide an
/// additional title for a subschema. This allows generators to infer a better
/// variant name for an "untagged" enum.
// TODO-cleanup: We should move IpNet and this to
// `omicron_nexus::external_api::shared`. It's public now because `IpRange`,
// which is defined there, uses it.
pub fn label_schema(
    label: &str,
    schema: schemars::schema::Schema,
) -> schemars::schema::Schema {
    schemars::schema::SchemaObject {
        metadata: Some(
            schemars::schema::Metadata {
                title: Some(label.to_string()),
                ..Default::default()
            }
            .into(),
        ),
        subschemas: Some(
            schemars::schema::SubschemaValidation {
                all_of: Some(vec![schema]),
                ..Default::default()
            }
            .into(),
        ),
        ..Default::default()
    }
    .into()
}

/// A `RouteTarget` describes the possible locations that traffic matching a
/// route destination can be sent.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    FromStr,
    Serialize,
    PartialEq,
    JsonSchema,
)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
#[display("{}:{0}", style = "lowercase")]
pub enum RouteTarget {
    /// Forward traffic to a particular IP address.
    Ip(IpAddr),
    /// Forward traffic to a VPC
    Vpc(Name),
    /// Forward traffic to a VPC Subnet
    Subnet(Name),
    /// Forward traffic to a specific instance
    Instance(Name),
    #[display("inetgw:{0}")]
    /// Forward traffic to an internet gateway
    InternetGateway(Name),
}

/// A `RouteDestination` is used to match traffic with a routing rule, on the
/// destination of that traffic.
///
/// When traffic is to be sent to a destination that is within a given
/// `RouteDestination`, the corresponding [`RouterRoute`] applies, and traffic
/// will be forward to the [`RouteTarget`] for that rule.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    FromStr,
    Serialize,
    PartialEq,
    JsonSchema,
)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
#[display("{}:{0}", style = "lowercase")]
pub enum RouteDestination {
    /// Route applies to traffic destined for a specific IP address
    Ip(IpAddr),
    /// Route applies to traffic destined for a specific IP subnet
    IpNet(IpNet),
    /// Route applies to traffic destined for the given VPC.
    Vpc(Name),
    /// Route applies to traffic
    Subnet(Name),
}

/// The classification of a [`RouterRoute`] as defined by the system.
/// The kind determines certain attributes such as if the route is modifiable
/// and describes how or where the route was created.
///
/// See [RFD-21](https://rfd.shared.oxide.computer/rfd/0021#concept-router) for more context
#[derive(
    Clone, Copy, Debug, PartialEq, Deserialize, Serialize, Display, JsonSchema,
)]
#[display("{}")]
#[serde(rename_all = "snake_case")]
pub enum RouterRouteKind {
    /// Determines the default destination of traffic, such as whether it goes to the internet or not.
    ///
    /// `Destination: An Internet Gateway`
    /// `Modifiable: true`
    Default,
    /// Automatically added for each VPC Subnet in the VPC
    ///
    /// `Destination: A VPC Subnet`
    /// `Modifiable: false`
    VpcSubnet,
    /// Automatically added when VPC peering is established
    ///
    /// `Destination: A different VPC`
    /// `Modifiable: false`
    VpcPeering,
    /// Created by a user
    /// See [`RouteTarget`]
    ///
    /// `Destination: User defined`
    /// `Modifiable: true`
    Custom,
}

///  A route defines a rule that governs where traffic should be sent based on its destination.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RouterRoute {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The VPC Router to which the route belongs.
    pub vpc_router_id: Uuid,

    /// Describes the kind of router. Set at creation. `read-only`
    pub kind: RouterRouteKind,

    pub target: RouteTarget,
    pub destination: RouteDestination,
}

/// A single rule in a VPC firewall
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRule {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// whether this rule is in effect
    pub status: VpcFirewallRuleStatus,
    /// whether this rule is for incoming or outgoing traffic
    pub direction: VpcFirewallRuleDirection,
    /// list of sets of instances that the rule applies to
    pub targets: Vec<VpcFirewallRuleTarget>,
    /// reductions on the scope of the rule
    pub filters: VpcFirewallRuleFilter,
    /// whether traffic matching the rule should be allowed or dropped
    pub action: VpcFirewallRuleAction,
    /// the relative priority of this rule
    pub priority: VpcFirewallRulePriority,
    /// the VPC to which this rule belongs
    pub vpc_id: Uuid,
}

/// Collection of a Vpc's firewall rules
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRules {
    pub rules: Vec<VpcFirewallRule>,
}

/// A single rule in a VPC firewall
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, JsonSchema)]
pub struct VpcFirewallRuleUpdate {
    /// name of the rule, unique to this VPC
    pub name: Name,
    /// human-readable free-form text about a resource
    pub description: String,
    /// whether this rule is in effect
    pub status: VpcFirewallRuleStatus,
    /// whether this rule is for incoming or outgoing traffic
    pub direction: VpcFirewallRuleDirection,
    /// list of sets of instances that the rule applies to
    pub targets: Vec<VpcFirewallRuleTarget>,
    /// reductions on the scope of the rule
    pub filters: VpcFirewallRuleFilter,
    /// whether traffic matching the rule should be allowed or dropped
    pub action: VpcFirewallRuleAction,
    /// the relative priority of this rule
    pub priority: VpcFirewallRulePriority,
}

/// Updateable properties of a `Vpc`'s firewall
/// Note that VpcFirewallRules are implicitly created along with a Vpc,
/// so there is no explicit creation.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRuleUpdateParams {
    pub rules: Vec<VpcFirewallRuleUpdate>,
}

/// Firewall rule priority. This is a value from 0 to 65535, with rules with
/// lower values taking priority over higher values.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    PartialOrd,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(transparent)]
#[repr(transparent)]
pub struct VpcFirewallRulePriority(pub u16);

/// Filter for a firewall rule. A given packet must match every field that is
/// present for the rule to apply to it. A packet matches a field if any entry
/// in that field matches the packet.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRuleFilter {
    /// If present, the sources (if incoming) or destinations (if outgoing)
    /// this rule applies to.
    pub hosts: Option<Vec<VpcFirewallRuleHostFilter>>,

    /// If present, the networking protocols this rule applies to.
    pub protocols: Option<Vec<VpcFirewallRuleProtocol>>,

    /// If present, the destination ports this rule applies to.
    pub ports: Option<Vec<L4PortRange>>,
}

/// The protocols that may be specified in a firewall rule's filter
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum VpcFirewallRuleProtocol {
    Tcp,
    Udp,
    Icmp,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VpcFirewallRuleStatus {
    Disabled,
    Enabled,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VpcFirewallRuleDirection {
    Inbound,
    Outbound,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VpcFirewallRuleAction {
    Allow,
    Deny,
}

/// A `VpcFirewallRuleTarget` is used to specify the set of [`Instance`]s to
/// which a firewall rule applies.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    FromStr,
    Serialize,
    PartialEq,
    JsonSchema,
)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
#[display("{}:{0}", style = "lowercase")]
pub enum VpcFirewallRuleTarget {
    /// The rule applies to all instances in the VPC
    Vpc(Name),
    /// The rule applies to all instances in the VPC Subnet
    Subnet(Name),
    /// The rule applies to this specific instance
    Instance(Name),
    /// The rule applies to a specific IP address
    Ip(IpAddr),
    /// The rule applies to a specific IP subnet
    IpNet(IpNet),
    // Tags not yet implemented
    // Tag(Name),
}

/// The `VpcFirewallRuleHostFilter` is used to filter traffic on the basis of
/// its source or destination host.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    FromStr,
    Serialize,
    PartialEq,
    JsonSchema,
)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
#[display("{}:{0}", style = "lowercase")]
pub enum VpcFirewallRuleHostFilter {
    /// The rule applies to traffic from/to all instances in the VPC
    Vpc(Name),
    /// The rule applies to traffic from/to all instances in the VPC Subnet
    Subnet(Name),
    /// The rule applies to traffic from/to this specific instance
    Instance(Name),
    // Tags not yet implemented
    // Tag(Name),
    /// The rule applies to traffic from/to a specific IP address
    Ip(IpAddr),
    /// The rule applies to traffic from/to a specific IP subnet
    IpNet(IpNet),
    // TODO: Internet gateways not yet implemented
    // #[display("inetgw:{0}")]
    // InternetGateway(Name),
}

/// Port number used in a transport-layer protocol like TCP or UDP
/// Note that 0 is an invalid port number.
#[derive(
    Clone,
    Copy,
    Debug,
    Display,
    PartialEq,
    PartialOrd,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(transparent)]
#[repr(transparent)]
pub struct L4Port(pub NonZeroU16);

impl From<NonZeroU16> for L4Port {
    fn from(port: NonZeroU16) -> L4Port {
        L4Port(port)
    }
}

impl TryFrom<u16> for L4Port {
    type Error = <NonZeroU16 as TryFrom<u16>>::Error;
    fn try_from(port: u16) -> Result<L4Port, Self::Error> {
        NonZeroU16::try_from(port).map(L4Port)
    }
}

/// A range of transport layer ports. This range is inclusive on both ends.
#[derive(
    Clone, Copy, Debug, DeserializeFromStr, SerializeDisplay, PartialEq,
)]
pub struct L4PortRange {
    /// The first port in the range
    pub first: L4Port,
    /// The last port in the range
    pub last: L4Port,
}

impl FromStr for L4PortRange {
    type Err = String;
    fn from_str(range: &str) -> Result<Self, Self::Err> {
        const INVALID_PORT_NUMBER_MSG: &str = "invalid port number";

        match range.split_once('-') {
            None => {
                let port = range
                    .parse::<NonZeroU16>()
                    .map_err(|_| INVALID_PORT_NUMBER_MSG.to_string())?
                    .into();
                Ok(L4PortRange { first: port, last: port })
            }
            Some((left, right)) => {
                let first = left
                    .parse::<NonZeroU16>()
                    .map_err(|_| INVALID_PORT_NUMBER_MSG.to_string())?
                    .into();
                let last = right
                    .parse::<NonZeroU16>()
                    .map_err(|_| INVALID_PORT_NUMBER_MSG.to_string())?
                    .into();
                Ok(L4PortRange { first, last })
            }
        }
    }
}

impl TryFrom<String> for L4PortRange {
    type Error = <L4PortRange as FromStr>::Err;

    fn try_from(range: String) -> Result<Self, Self::Error> {
        range.parse()
    }
}

impl Display for L4PortRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.first == self.last {
            write!(f, "{}", self.first)
        } else {
            write!(f, "{}-{}", self.first, self.last)
        }
    }
}

impl JsonSchema for L4PortRange {
    fn schema_name() -> String {
        "L4PortRange".to_string()
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some("A range of IP ports".to_string()),
                description: Some(
                    "An inclusive-inclusive range of IP ports. The second port \
                    may be omitted to represent a single port"
                        .to_string(),
                ),
                examples: vec!["22".into(), "6667-7000".into()],
                ..Default::default()
            })),
            instance_type: Some(
                schemars::schema::InstanceType::String.into()
            ),
            string: Some(Box::new(schemars::schema::StringValidation {
                max_length: Some(11),  // 5 digits for each port and the dash
                min_length: Some(1),
                pattern: Some(
                    r#"^[0-9]{1,5}(-[0-9]{1,5})?$"#.to_string(),
                ),
            })),
            ..Default::default()
        }.into()
    }
}

/// The `MacAddr` represents a Media Access Control (MAC) address, used to uniquely identify
/// hardware devices on a network.
// NOTE: We're using the `macaddr` crate for the internal representation. But as with the `ipnet`,
// this crate does not implement `JsonSchema`.
#[derive(
    Clone, Copy, Debug, DeserializeFromStr, PartialEq, SerializeDisplay,
)]
pub struct MacAddr(pub macaddr::MacAddr6);

impl From<macaddr::MacAddr6> for MacAddr {
    fn from(mac: macaddr::MacAddr6) -> Self {
        Self(mac)
    }
}

impl FromStr for MacAddr {
    type Err = macaddr::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.split(':')
            .map(|b| format!("{:0>2}", b))
            .collect::<Vec<String>>()
            .join(":")
            .parse()
            .map(MacAddr)
    }
}

impl TryFrom<String> for MacAddr {
    type Error = <Self as FromStr>::Err;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        MacAddr::from_str(s.as_ref())
    }
}

impl std::ops::Deref for MacAddr {
    type Target = macaddr::MacAddr6;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for MacAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl JsonSchema for MacAddr {
    fn schema_name() -> String {
        "MacAddr".to_string()
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some("A MAC address".to_string()),
                description: Some(
                    "A Media Access Control address, in EUI-48 format"
                        .to_string(),
                ),
                examples: vec!["ff:ff:ff:ff:ff:ff".into()],
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            string: Some(Box::new(schemars::schema::StringValidation {
                max_length: Some(17), // 12 hex characters and 5 ":"-separators
                min_length: Some(5),  // Just 5 ":" separators
                pattern: Some(
                    r#"^([0-9a-fA-F]{0,2}:){5}[0-9a-fA-F]{0,2}$"#.to_string(),
                ),
            })),
            ..Default::default()
        }
        .into()
    }
}

/// A Geneve Virtual Network Identifier
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct Vni(u32);

impl Vni {
    /// Virtual Network Identifiers are constrained to be 24-bit values.
    pub const MAX_VNI: u32 = 0xFF_FFFF;

    /// Oxide reserves a slice of initial VNIs for its own use.
    pub const MIN_GUEST_VNI: u32 = 1024;

    /// Create a new random VNI.
    pub fn random() -> Self {
        use rand::Rng;
        Self(rand::thread_rng().gen_range(Self::MIN_GUEST_VNI..=Self::MAX_VNI))
    }
}

impl From<Vni> for u32 {
    fn from(vni: Vni) -> u32 {
        vni.0
    }
}

impl TryFrom<u32> for Vni {
    type Error = Error;

    fn try_from(x: u32) -> Result<Self, Error> {
        if x <= Self::MAX_VNI {
            Ok(Self(x))
        } else {
            Err(Error::internal_error(
                format!("Invalid Geneve VNI: {}", x).as_str(),
            ))
        }
    }
}

impl TryFrom<i32> for Vni {
    type Error = Error;

    fn try_from(x: i32) -> Result<Self, Error> {
        Self::try_from(u32::try_from(x).map_err(|_| {
            Error::internal_error(format!("Invalid Geneve VNI: {}", x).as_str())
        })?)
    }
}

/// A `NetworkInterface` represents a virtual network interface device.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct NetworkInterface {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The Instance to which the interface belongs.
    pub instance_id: Uuid,

    /// The VPC to which the interface belongs.
    pub vpc_id: Uuid,

    /// The subnet to which the interface belongs.
    pub subnet_id: Uuid,

    /// The MAC address assigned to this interface.
    pub mac: MacAddr,

    /// The IP address assigned to this interface.
    pub ip: IpAddr,
    // TODO-correctness: We need to split this into an optional V4 and optional
    // V6 address, at least one of which must be specified.
    /// True if this interface is the primary for the instance to which it's
    /// attached.
    pub primary: bool,
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum Digest {
    Sha256(String),
}

impl FromStr for Digest {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("sha256:") {
            let parts: Vec<&str> = s.split(':').collect();
            if parts.len() != 2 {
                anyhow::bail!("digest string {} should have two parts", s);
            }

            if parts[1].len() != 64 {
                anyhow::bail!("sha256 length must be 64");
            }

            return Ok(Digest::Sha256(parts[1].to_string()));
        }

        anyhow::bail!("invalid digest string {}", s);
    }
}

impl std::fmt::Display for Digest {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{}",
            match self {
                Digest::Sha256(value) => format!("sha256:{}", value),
            }
        )
    }
}

#[cfg(test)]
mod test {
    use serde::Deserialize;
    use serde::Serialize;

    use super::IpNet;
    use super::RouteDestination;
    use super::RouteTarget;
    use super::SemverVersion;
    use super::VpcFirewallRuleHostFilter;
    use super::VpcFirewallRuleTarget;
    use super::{
        ByteCount, Digest, L4Port, L4PortRange, Name, RoleName,
        VpcFirewallRuleAction, VpcFirewallRuleDirection, VpcFirewallRuleFilter,
        VpcFirewallRulePriority, VpcFirewallRuleProtocol,
        VpcFirewallRuleStatus, VpcFirewallRuleUpdate,
        VpcFirewallRuleUpdateParams,
    };
    use crate::api::external::Error;
    use crate::api::external::ResourceType;
    use std::convert::TryFrom;
    use std::str::FromStr;

    #[test]
    fn test_semver_validation() {
        // Examples copied from
        // https://github.com/dtolnay/semver/blob/cc2cfed67c17dfe6abae18726830bdb6d7cf740d/tests/test_version.rs#L13.
        let valid = [
            "1.2.3",
            "1.2.3-alpha1",
            "1.2.3+build5",
            "1.2.3+5build",
            "1.2.3-alpha1+build5",
            "1.2.3-1.alpha1.9+build5.7.3aedf",
            "1.2.3-0a.alpha1.9+05build.7.3aed",
            "0.4.0-beta.1+0851523",
            "1.1.0-beta-10",
        ];
        let invalid = [
            // These examples are rejected by the validation regex.
            "",
            "1",
            "1.2",
            "1.2.3-",
            "a.b.c",
            "1.2.3 abc",
            "1.2.3-01",
        ];

        let r = regress::Regex::new(SemverVersion::VALIDATION_REGEX)
            .expect("validation regex is valid");
        for input in valid {
            let m = r
                .find(input)
                .unwrap_or_else(|| panic!("input {input} did not match regex"));
            assert_eq!(m.start(), 0, "input {input} did not match start");
            assert_eq!(m.end(), input.len(), "input {input} did not match end");
        }

        for input in invalid {
            assert!(
                r.find(input).is_none(),
                "invalid input {input} should not match validation regex"
            );
        }
    }

    #[test]
    fn test_semver_serialize() {
        #[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
        struct MyStruct {
            version: SemverVersion,
        }

        let v = MyStruct { version: SemverVersion::new(1, 2, 3) };
        let expected = "{\"version\":\"1.2.3\"}";
        assert_eq!(serde_json::to_string(&v).unwrap(), expected);
        assert_eq!(serde_json::from_str::<MyStruct>(expected).unwrap(), v);
    }

    #[test]
    fn test_name_parse() {
        // Error cases
        let long_name =
            "a234567890123456789012345678901234567890123456789012345678901234";
        assert_eq!(long_name.len(), 64);
        let error_cases: Vec<(&str, &str)> = vec![
            ("", "name requires at least one character"),
            (long_name, "name may contain at most 63 characters"),
            ("123", "name must begin with an ASCII lowercase character"),
            ("-abc", "name must begin with an ASCII lowercase character"),
            ("abc-", "name cannot end with \"-\""),
            (
                "aBc",
                "name contains invalid character: \"B\" (allowed characters \
                 are lowercase ASCII, digits, and \"-\")",
            ),
            (
                "a_c",
                "name contains invalid character: \"_\" (allowed characters \
                 are lowercase ASCII, digits, and \"-\")",
            ),
            (
                "a\u{00e9}cc",
                "name contains invalid character: \"\u{00e9}\" (allowed \
                 characters are lowercase ASCII, digits, and \"-\")",
            ),
            (
                "a7e55044-10b1-426f-9247-bb680e5fe0c8",
                "name cannot be a UUID to avoid ambiguity with IDs",
            ),
        ];

        for (input, expected_message) in error_cases {
            eprintln!("check name \"{}\" (expecting error)", input);
            assert_eq!(input.parse::<Name>().unwrap_err(), expected_message);
        }

        // Success cases
        let valid_names: Vec<&str> = vec![
            "a",
            "abc",
            "abc-123",
            "a123",
            "ok-a7e55044-10b1-426f-9247-bb680e5fe0c8",
            "a7e55044-10b1-426f-9247-bb680e5fe0c8-ok",
            &long_name[0..63],
        ];

        for name in valid_names {
            eprintln!("check name \"{}\" (should be valid)", name);
            assert_eq!(name, name.parse::<Name>().unwrap().as_str());
        }
    }

    #[test]
    fn test_role_name_parse() {
        // Error cases
        let bad_inputs = vec![
            // empty string is always worth testing
            "",
            // missing dot
            "project",
            // extra dot (or, illegal character in the second component)
            "project.admin.super",
            // missing resource type (or, another bogus resource type)
            ".admin",
            // missing role name
            "project.",
            // illegal characters in role name
            "project.not_good",
        ];

        for input in bad_inputs {
            eprintln!("check name {:?} (expecting error)", input);
            let result =
                input.parse::<RoleName>().expect_err("unexpectedly succeeded");
            eprintln!("(expected) error: {:?}", result);
        }

        eprintln!("check name \"project.admin\" (expecting success)");
        let role_name =
            "project.admin".parse::<RoleName>().expect("failed to parse");
        assert_eq!(role_name.to_string(), "project.admin");
        assert_eq!(role_name.resource_type, "project");
        assert_eq!(role_name.role_name, "admin");

        eprintln!("check name \"barf.admin\" (expecting success)");
        let role_name =
            "barf.admin".parse::<RoleName>().expect("failed to parse");
        assert_eq!(role_name.to_string(), "barf.admin");
        assert_eq!(role_name.resource_type, "barf");
        assert_eq!(role_name.role_name, "admin");

        eprintln!("check name \"organization.super-user\" (expecting success)");
        let role_name = "organization.super-user"
            .parse::<RoleName>()
            .expect("failed to parse");
        assert_eq!(role_name.to_string(), "organization.super-user");
        assert_eq!(role_name.resource_type, "organization");
        assert_eq!(role_name.role_name, "super-user");
    }

    #[test]
    fn test_resource_name_parse() {
        let bad_inputs = vec![
            "bogus",
            "",
            "Project",
            "oRgAnIzAtIoN",
            "organisation",
            "vpc subnet",
            "vpc_subnet",
        ];
        for input in bad_inputs {
            eprintln!("check resource type {:?} (expecting error)", input);
            let result = input
                .parse::<ResourceType>()
                .expect_err("unexpectedly succeeded");
            eprintln!("(expected) error: {:?}", result);
        }

        assert_eq!(
            ResourceType::Project,
            "project".parse::<ResourceType>().unwrap()
        );
        assert_eq!(
            ResourceType::VpcSubnet,
            "vpc-subnet".parse::<ResourceType>().unwrap()
        );
    }

    #[test]
    fn test_name_parse_from_param() {
        let result = Name::from_param(String::from("my-name"), "the_name");
        assert!(result.is_ok());
        assert_eq!(result, Ok("my-name".parse().unwrap()));

        let result = Name::from_param(String::from(""), "the_name");
        assert!(result.is_err());
        assert_eq!(
            result,
            Err(Error::InvalidValue {
                label: "the_name".to_string(),
                message: "name requires at least one character".to_string()
            })
        );
    }

    #[test]
    fn test_bytecount() {
        // Smallest supported value: all constructors
        let zero = ByteCount::from(0u32);
        assert_eq!(0, zero.to_bytes());
        assert_eq!(0, zero.to_whole_kibibytes());
        assert_eq!(0, zero.to_whole_mebibytes());
        assert_eq!(0, zero.to_whole_gibibytes());
        assert_eq!(0, zero.to_whole_tebibytes());
        let zero = ByteCount::try_from(0i64).unwrap();
        assert_eq!(0, zero.to_bytes());
        let zero = ByteCount::try_from(0u64).unwrap();
        assert_eq!(0, zero.to_bytes());

        // Largest supported value: both constructors that support it.
        let max = ByteCount::try_from(i64::MAX).unwrap();
        assert_eq!(i64::MAX, max.to_bytes() as i64);
        assert_eq!(i64::MAX, i64::from(max));

        let maxu64 = u64::try_from(i64::MAX).unwrap();
        let max = ByteCount::try_from(maxu64).unwrap();
        assert_eq!(i64::MAX, max.to_bytes() as i64);
        assert_eq!(i64::MAX, i64::from(max));
        assert_eq!(
            (i64::MAX / 1024 / 1024 / 1024 / 1024) as u64,
            max.to_whole_tebibytes()
        );

        // Value too large (only one constructor can hit this)
        let bogus = ByteCount::try_from(maxu64 + 1).unwrap_err();
        assert_eq!(bogus.to_string(), "value is too large for a byte count");
        // Value too small (only one constructor can hit this)
        let bogus = ByteCount::try_from(-1i64).unwrap_err();
        assert_eq!(bogus.to_string(), "value is too small for a byte count");
        // For good measure, let's check i64::MIN
        let bogus = ByteCount::try_from(i64::MIN).unwrap_err();
        assert_eq!(bogus.to_string(), "value is too small for a byte count");

        // We've now exhaustively tested both sides of all boundary conditions
        // for all three constructors (to the extent that that's possible).
        // Check non-trivial cases for the various accessor functions.  This
        // means picking values in the middle of the range.
        let three_terabytes = 3_000_000_000_000u64;
        let tb3 = ByteCount::try_from(three_terabytes).unwrap();
        assert_eq!(three_terabytes, tb3.to_bytes());
        assert_eq!(2929687500, tb3.to_whole_kibibytes());
        assert_eq!(2861022, tb3.to_whole_mebibytes());
        assert_eq!(2793, tb3.to_whole_gibibytes());
        assert_eq!(2, tb3.to_whole_tebibytes());

        let three_tebibytes = 3u64 * 1024 * 1024 * 1024 * 1024;
        let tib3 = ByteCount::try_from(three_tebibytes).unwrap();
        assert_eq!(three_tebibytes, tib3.to_bytes());
        assert_eq!(3 * 1024 * 1024 * 1024, tib3.to_whole_kibibytes());
        assert_eq!(3 * 1024 * 1024, tib3.to_whole_mebibytes());
        assert_eq!(3 * 1024, tib3.to_whole_gibibytes());
        assert_eq!(3, tib3.to_whole_tebibytes());
    }

    #[test]
    fn test_bytecount_display() {
        assert_eq!(format!("{}", ByteCount::from(0u32)), "0 B".to_string());
        assert_eq!(format!("{}", ByteCount::from(1023)), "1023 B".to_string());
        assert_eq!(format!("{}", ByteCount::from(1024)), "1 KiB".to_string());
        assert_eq!(format!("{}", ByteCount::from(1025)), "1025 B".to_string());
        assert_eq!(
            format!("{}", ByteCount::from(1024 * 100)),
            "100 KiB".to_string()
        );
        assert_eq!(
            format!("{}", ByteCount::from_mebibytes_u32(1)),
            "1 MiB".to_string()
        );
        assert_eq!(
            format!("{}", ByteCount::from_gibibytes_u32(1)),
            "1 GiB".to_string()
        );
        assert_eq!(
            format!("{}", ByteCount::from_gibibytes_u32(1024)),
            "1 TiB".to_string()
        );
    }

    #[test]
    fn test_ip_port_range_from_str() {
        assert_eq!(
            L4PortRange::try_from("65532".to_string()),
            Ok(L4PortRange {
                first: L4Port::try_from(65532).unwrap(),
                last: L4Port::try_from(65532).unwrap()
            })
        );
        assert_eq!(
            L4PortRange::try_from("22-53".to_string()),
            Ok(L4PortRange {
                first: L4Port::try_from(22).unwrap(),
                last: L4Port::try_from(53).unwrap()
            })
        );

        assert_eq!(
            L4PortRange::try_from("".to_string()),
            Err("invalid port number".to_string())
        );
        assert_eq!(
            L4PortRange::try_from("65536".to_string()),
            Err("invalid port number".to_string())
        );
        assert_eq!(
            L4PortRange::try_from("65535-65536".to_string()),
            Err("invalid port number".to_string())
        );
        assert_eq!(
            L4PortRange::try_from("0x23".to_string()),
            Err("invalid port number".to_string())
        );
        assert_eq!(
            L4PortRange::try_from("0".to_string()),
            Err("invalid port number".to_string())
        );
        assert_eq!(
            L4PortRange::try_from("0-20".to_string()),
            Err("invalid port number".to_string())
        );
        assert_eq!(
            L4PortRange::try_from("-20".to_string()),
            Err("invalid port number".to_string())
        );
    }

    #[test]
    fn test_ip_port_range_into_str() {
        let range = L4PortRange {
            first: L4Port::try_from(12345).unwrap(),
            last: L4Port::try_from(12345).unwrap(),
        }
        .to_string();
        assert_eq!(range, "12345");

        let range: String = L4PortRange {
            first: L4Port::try_from(1).unwrap(),
            last: L4Port::try_from(1024).unwrap(),
        }
        .to_string();
        assert_eq!(range, "1-1024");
    }

    #[test]
    fn test_firewall_deserialization() {
        let json = r#"{
            "rules": [
              {
                "name": "allow-internal-inbound",
                "status": "enabled",
                "direction": "inbound",
                "targets": [ { "type": "vpc", "value": "default" } ],
                "filters": {"hosts": [ { "type": "vpc", "value": "default" } ]},
                "action": "allow",
                "priority": 65534,
                "description": "allow inbound traffic between instances"
              },
              {
                "name": "rule2",
                "status": "disabled",
                "direction": "outbound",
                "targets": [ { "type": "vpc", "value": "default" } ],
                "filters": {"ports": [ "22-25", "27" ], "protocols": [ "UDP" ]},
                "action": "deny",
                "priority": 65533,
                "description": "second rule"
              }
            ]
          }"#;
        let params =
            serde_json::from_str::<VpcFirewallRuleUpdateParams>(json).unwrap();
        assert_eq!(params.rules.len(), 2);
        assert_eq!(
            params.rules[0],
            VpcFirewallRuleUpdate {
                name: Name::try_from("allow-internal-inbound".to_string())
                    .unwrap(),
                status: VpcFirewallRuleStatus::Enabled,
                direction: VpcFirewallRuleDirection::Inbound,
                targets: vec![VpcFirewallRuleTarget::Vpc(
                    "default".parse().unwrap()
                )],
                filters: VpcFirewallRuleFilter {
                    hosts: Some(vec![VpcFirewallRuleHostFilter::Vpc(
                        "default".parse().unwrap()
                    )]),
                    ports: None,
                    protocols: None,
                },
                action: VpcFirewallRuleAction::Allow,
                priority: VpcFirewallRulePriority(65534),
                description: "allow inbound traffic between instances"
                    .to_string(),
            }
        );
        assert_eq!(
            params.rules[1],
            VpcFirewallRuleUpdate {
                name: Name::try_from("rule2".to_string()).unwrap(),
                status: VpcFirewallRuleStatus::Disabled,
                direction: VpcFirewallRuleDirection::Outbound,
                targets: vec![VpcFirewallRuleTarget::Vpc(
                    "default".parse().unwrap()
                )],
                filters: VpcFirewallRuleFilter {
                    hosts: None,
                    ports: Some(vec![
                        L4PortRange {
                            first: L4Port::try_from(22).unwrap(),
                            last: L4Port::try_from(25).unwrap()
                        },
                        L4PortRange {
                            first: L4Port::try_from(27).unwrap(),
                            last: L4Port::try_from(27).unwrap()
                        }
                    ]),
                    protocols: Some(vec![VpcFirewallRuleProtocol::Udp]),
                },
                action: VpcFirewallRuleAction::Deny,
                priority: VpcFirewallRulePriority(65533),
                description: "second rule".to_string(),
            }
        );
    }

    #[test]
    fn test_ipv6_net_operations() {
        use super::Ipv6Net;
        assert!(Ipv6Net("fd00::/8".parse().unwrap()).is_unique_local());
        assert!(!Ipv6Net("fe00::/8".parse().unwrap()).is_unique_local());

        assert!(Ipv6Net("fd00::/48".parse().unwrap()).is_vpc_prefix());
        assert!(!Ipv6Net("fe00::/48".parse().unwrap()).is_vpc_prefix());
        assert!(!Ipv6Net("fd00::/40".parse().unwrap()).is_vpc_prefix());

        let vpc_prefix = Ipv6Net("fd00::/48".parse().unwrap());
        assert!(
            Ipv6Net("fd00::/64".parse().unwrap()).is_vpc_subnet(&vpc_prefix)
        );
        assert!(
            !Ipv6Net("fd10::/64".parse().unwrap()).is_vpc_subnet(&vpc_prefix)
        );
        assert!(
            !Ipv6Net("fd00::/63".parse().unwrap()).is_vpc_subnet(&vpc_prefix)
        );
    }

    #[test]
    fn test_route_target_parse() {
        let name: Name = "foo".parse().unwrap();
        let address = "192.168.0.10".parse().unwrap();
        assert_eq!(RouteTarget::Vpc(name.clone()), "vpc:foo".parse().unwrap());
        assert_eq!(
            RouteTarget::Subnet(name.clone()),
            "subnet:foo".parse().unwrap()
        );
        assert_eq!(
            RouteTarget::Instance(name),
            "instance:foo".parse().unwrap()
        );
        assert_eq!(
            RouteTarget::Ip(address),
            "ip:192.168.0.10".parse().unwrap()
        );
        assert!("foo:foo".parse::<RouteTarget>().is_err());
        assert!("foo".parse::<RouteTarget>().is_err());
    }

    #[test]
    fn test_route_destination_parse() {
        let name: Name = "foo".parse().unwrap();
        let address = "192.168.0.10".parse().unwrap();
        let network = "fd00::/64".parse().unwrap();
        assert_eq!(
            RouteDestination::Vpc(name.clone()),
            "vpc:foo".parse().unwrap()
        );
        assert_eq!(
            RouteDestination::Subnet(name),
            "subnet:foo".parse().unwrap()
        );
        assert_eq!(
            RouteDestination::Ip(address),
            "ip:192.168.0.10".parse().unwrap()
        );
        assert_eq!(
            RouteDestination::IpNet(network),
            "ipnet:fd00::/64".parse().unwrap()
        );
        assert!("foo:foo".parse::<RouteDestination>().is_err());
        assert!("foo".parse::<RouteDestination>().is_err());
    }

    #[test]
    fn test_firewall_rule_target_parse() {
        let name: Name = "foo".parse().unwrap();
        let address = "192.168.0.10".parse().unwrap();
        let network = "fd00::/64".parse().unwrap();
        assert_eq!(
            VpcFirewallRuleTarget::Vpc(name.clone()),
            "vpc:foo".parse().unwrap()
        );
        assert_eq!(
            VpcFirewallRuleTarget::Subnet(name.clone()),
            "subnet:foo".parse().unwrap()
        );
        assert_eq!(
            VpcFirewallRuleTarget::Instance(name),
            "instance:foo".parse().unwrap()
        );
        assert_eq!(
            VpcFirewallRuleTarget::Ip(address),
            "ip:192.168.0.10".parse().unwrap()
        );
        assert_eq!(
            VpcFirewallRuleTarget::IpNet(network),
            "ipnet:fd00::/64".parse().unwrap()
        );
        assert!("foo:foo".parse::<VpcFirewallRuleTarget>().is_err());
        assert!("foo".parse::<VpcFirewallRuleTarget>().is_err());
    }

    #[test]
    fn test_firewall_rule_host_filter_parse() {
        let name: Name = "foo".parse().unwrap();
        let address = "192.168.0.10".parse().unwrap();
        let network = "fd00::/64".parse().unwrap();
        assert_eq!(
            VpcFirewallRuleHostFilter::Vpc(name.clone()),
            "vpc:foo".parse().unwrap()
        );
        assert_eq!(
            VpcFirewallRuleHostFilter::Subnet(name.clone()),
            "subnet:foo".parse().unwrap()
        );
        assert_eq!(
            VpcFirewallRuleHostFilter::Instance(name),
            "instance:foo".parse().unwrap()
        );
        assert_eq!(
            VpcFirewallRuleHostFilter::Ip(address),
            "ip:192.168.0.10".parse().unwrap()
        );
        assert_eq!(
            VpcFirewallRuleHostFilter::IpNet(network),
            "ipnet:fd00::/64".parse().unwrap()
        );
        assert!("foo:foo".parse::<VpcFirewallRuleHostFilter>().is_err());
        assert!("foo".parse::<VpcFirewallRuleHostFilter>().is_err());
    }

    #[test]
    fn test_digest() {
        // No prefix
        assert!(
            "5cc9d1620911c280b0b1dad1413603702baccf340a1e74ade9d0521bcd826acf"
                .parse::<Digest>()
                .is_err()
        );

        // Valid sha256
        let actual: Digest =
            "sha256:5cc9d1620911c280b0b1dad1413603702baccf340a1e74ade9d0521bcd826acf".to_string().parse().unwrap();
        assert_eq!(
            actual,
            Digest::Sha256("5cc9d1620911c280b0b1dad1413603702baccf340a1e74ade9d0521bcd826acf".to_string()),
        );

        // Too short for sha256
        assert!("sha256:5cc9d1620911c280b".parse::<Digest>().is_err());

        // Bad prefix
        assert!("hash:super_random".parse::<Digest>().is_err());
    }

    #[test]
    fn test_ipnet_serde() {
        let net_str = "fd00:2::/32";
        let net = IpNet::from_str(net_str).unwrap();
        let ser = serde_json::to_string(&net).unwrap();

        assert_eq!(format!(r#""{}""#, net_str), ser);
        let net_des = serde_json::from_str::<IpNet>(&ser).unwrap();
        assert_eq!(net, net_des);

        let net_str = "192.168.1.1/16";
        let net = IpNet::from_str(net_str).unwrap();
        let ser = serde_json::to_string(&net).unwrap();

        assert_eq!(format!(r#""{}""#, net_str), ser);
        let net_des = serde_json::from_str::<IpNet>(&ser).unwrap();
        assert_eq!(net, net_des);
    }

    #[test]
    fn test_ipnet_first_last_address() {
        use std::net::IpAddr;
        use std::net::Ipv4Addr;
        use std::net::Ipv6Addr;
        let net: IpNet = "fd00::/128".parse().unwrap();
        assert_eq!(
            net.first_address(),
            IpAddr::from(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0)),
        );
        assert_eq!(
            net.last_address(),
            IpAddr::from(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0)),
        );

        let net: IpNet = "fd00::/64".parse().unwrap();
        assert_eq!(
            net.first_address(),
            IpAddr::from(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0)),
        );
        assert_eq!(
            net.last_address(),
            IpAddr::from(Ipv6Addr::new(
                0xfd00, 0, 0, 0, 0xffff, 0xffff, 0xffff, 0xffff
            )),
        );

        let net: IpNet = "10.0.0.0/16".parse().unwrap();
        assert_eq!(
            net.first_address(),
            IpAddr::from(Ipv4Addr::new(10, 0, 0, 0)),
        );
        assert_eq!(
            net.last_address(),
            IpAddr::from(Ipv4Addr::new(10, 0, 255, 255)),
        );

        let net: IpNet = "10.0.0.0/32".parse().unwrap();
        assert_eq!(
            net.first_address(),
            IpAddr::from(Ipv4Addr::new(10, 0, 0, 0)),
        );
        assert_eq!(
            net.last_address(),
            IpAddr::from(Ipv4Addr::new(10, 0, 0, 0)),
        );
    }

    #[test]
    fn test_macaddr() {
        use super::MacAddr;
        let _ = MacAddr::from_str(":::::").unwrap();
        let _ = MacAddr::from_str("f:f:f:f:f:f").unwrap();
        let _ = MacAddr::from_str("ff:ff:ff:ff:ff:ff").unwrap();

        // Empty
        let _ = MacAddr::from_str("").unwrap_err();
        // Too few
        let _ = MacAddr::from_str("::::").unwrap_err();
        // Too many
        let _ = MacAddr::from_str("::::::").unwrap_err();
        // Not hex
        let _ = MacAddr::from_str("g:g:g:g:g:g").unwrap_err();
        // Too many characters
        let _ = MacAddr::from_str("fff:ff:ff:ff:ff:ff").unwrap_err();
    }
}
