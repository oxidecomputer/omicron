// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Data structures and related facilities for representing resources in the API
 *
 * This includes all representations over the wire for both the external and
 * internal APIs.  The contents here are all HTTP-agnostic.
 */

mod error;
pub mod http_pagination;
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
use serde::Deserialize;
use serde::Serialize;
use serde_with::{DeserializeFromStr, SerializeDisplay};
use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FormatResult;
use std::iter::FromIterator;
use std::net::{IpAddr, SocketAddr};
use std::num::{NonZeroU16, NonZeroU32};
use std::str::FromStr;
use uuid::Uuid;

/*
 * The type aliases below exist primarily to ensure consistency among return
 * types for functions in the `nexus::Nexus` and `nexus::DataStore`.  The
 * type argument `T` generally implements `Object`.
 */

/** Result of a create operation for the specified type */
pub type CreateResult<T> = Result<T, Error>;
/** Result of a delete operation for the specified type */
pub type DeleteResult = Result<(), Error>;
/** Result of a list operation that returns an ObjectStream */
pub type ListResult<T> = Result<ObjectStream<T>, Error>;
/** Result of a list operation that returns a vector */
pub type ListResultVec<T> = Result<Vec<T>, Error>;
/** Result of a lookup operation for the specified type */
pub type LookupResult<T> = Result<T, Error>;
/** Result of an update operation for the specified type */
pub type UpdateResult<T> = Result<T, Error>;

/**
 * A stream of Results, each potentially representing an object in the API
 */
pub type ObjectStream<T> = BoxStream<'static, Result<T, Error>>;

/*
 * General-purpose types used for client request parameters and return values.
 */

/**
 * Describes an `Object` that has its own identity metadata.  This is
 * currently used only for pagination.
 */
pub trait ObjectIdentity {
    fn identity(&self) -> &IdentityMetadata;
}

/**
 * Parameters used to request a specific page of results when listing a
 * collection of objects
 *
 * This is logically analogous to Dropshot's `PageSelector` (plus the limit from
 * Dropshot's `PaginationParams).  However, this type is HTTP-agnostic.  More
 * importantly, by the time this struct is generated, we know the type of the
 * sort field and we can specialize `DataPageParams` to that type.  This makes
 * it considerably simpler to implement the backend for most of our paginated
 * APIs.
 *
 * `NameType` is the type of the field used to sort the returned values and it's
 * usually `Name`.
 */
#[derive(Debug)]
pub struct DataPageParams<'a, NameType> {
    /**
     * If present, this is the value of the sort field for the last object seen
     */
    pub marker: Option<&'a NameType>,

    /**
     * Whether the sort is in ascending order
     */
    pub direction: PaginationOrder,

    /**
     * This identifies how many results should be returned on this page.
     * Backend implementations must provide this many results unless we're at
     * the end of the scan.  Dropshot assumes that if we provide fewer results
     * than this number, then we're done with the scan.
     */
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

/**
 * A name used in the API
 *
 * Names are generally user-provided unique identifiers, highly constrained as
 * described in RFD 4.  An `Name` can only be constructed with a string
 * that's valid as a name.
 */
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

/**
 * `Name::try_from(String)` is the primary method for constructing an Name
 * from an input string.  This validates the string according to our
 * requirements for a name.
 * TODO-cleanup why shouldn't callers use TryFrom<&str>?
 */
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

/**
 * `Name` instances are comparable like Strings, primarily so that they can
 * be used as keys in trees.
 */
impl<S> PartialEq<S> for Name
where
    S: AsRef<str>,
{
    fn eq(&self, other: &S) -> bool {
        self.0 == other.as_ref()
    }
}

/**
 * Custom JsonSchema implementation to encode the constraints on Name
 */
/*
 * TODO: 1. make this part of schemars w/ rename and maxlen annotations
 * TODO: 2. integrate the regex with `try_from`
 */
impl JsonSchema for Name {
    fn schema_name() -> String {
        "Name".to_string()
    }
    fn json_schema(
        _gen: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                id: None,
                title: Some("A name used in the API".to_string()),
                description: Some(
                    "Names must begin with a lower case ASCII letter, be \
                     composed exclusively of lowercase ASCII, uppercase \
                     ASCII, numbers, and '-', and may not end with a '-'."
                        .to_string(),
                ),
                default: None,
                deprecated: false,
                read_only: false,
                write_only: false,
                examples: vec![],
            })),
            instance_type: Some(schemars::schema::SingleOrVec::Single(
                Box::new(schemars::schema::InstanceType::String),
            )),
            format: None,
            enum_values: None,
            const_value: None,
            subschemas: None,
            number: None,
            string: Some(Box::new(schemars::schema::StringValidation {
                max_length: Some(63),
                min_length: None,
                pattern: Some("[a-z](|[a-zA-Z0-9-]*[a-zA-Z0-9])".to_string()),
            })),
            array: None,
            object: None,
            reference: None,
            extensions: BTreeMap::new(),
        })
    }
}

impl Name {
    /**
     * Parse an `Name`.  This is a convenience wrapper around
     * `Name::try_from(String)` that marshals any error into an appropriate
     * `Error`.
     */
    pub fn from_param(value: String, label: &str) -> Result<Name, Error> {
        value.parse().map_err(|e| Error::InvalidValue {
            label: String::from(label),
            message: e,
        })
    }

    /**
     * Return the `&str` representing the actual name.
     */
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/**
 * A count of bytes, typically used either for memory or storage capacity
 *
 * The maximum supported byte count is [`i64::MAX`].  This makes it somewhat
 * inconvenient to define constructors: a u32 constructor can be infallible, but
 * an i64 constructor can fail (if the value is negative) and a u64 constructor
 * can fail (if the value is larger than i64::MAX).  We provide all of these for
 * consumers' convenience.
 */
/*
 * TODO-cleanup This could benefit from a more complete implementation.
 * TODO-correctness RFD 4 requires that this be a multiple of 256 MiB.  We'll
 * need to write a validator for that.
 */
/*
 * The maximum byte count of i64::MAX comes from the fact that this is stored in
 * the database as an i64.  Constraining it here ensures that we can't fail to
 * serialize the value.
 */
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ByteCount(u64);

impl ByteCount {
    pub fn from_kibibytes_u32(kibibytes: u32) -> ByteCount {
        ByteCount::try_from(1024 * u64::from(kibibytes)).unwrap()
    }

    pub fn from_mebibytes_u32(mebibytes: u32) -> ByteCount {
        ByteCount::try_from(1024 * 1024 * u64::from(mebibytes)).unwrap()
    }

    pub fn from_gibibytes_u32(gibibytes: u32) -> ByteCount {
        ByteCount::try_from(1024 * 1024 * 1024 * u64::from(gibibytes)).unwrap()
    }

    pub fn to_bytes(&self) -> u64 {
        self.0
    }
    pub fn to_whole_kibibytes(&self) -> u64 {
        self.to_bytes() / 1024
    }
    pub fn to_whole_mebibytes(&self) -> u64 {
        self.to_bytes() / 1024 / 1024
    }
    pub fn to_whole_gibibytes(&self) -> u64 {
        self.to_bytes() / 1024 / 1024 / 1024
    }
    pub fn to_whole_tebibytes(&self) -> u64 {
        self.to_bytes() / 1024 / 1024 / 1024 / 1024
    }
}

/* TODO-cleanup This could use the experimental std::num::IntErrorKind. */
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

impl From<&ByteCount> for i64 {
    fn from(b: &ByteCount) -> Self {
        /* We have already validated that this value is in range. */
        i64::try_from(b.0).unwrap()
    }
}

/**
 * Generation numbers stored in the database, used for optimistic concurrency
 * control
 */
/*
 * Because generation numbers are stored in the database, we represent them as
 * i64.
 */
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
        /*
         * It should technically be an operational error if this wraps or even
         * exceeds the value allowed by an i64.  But it seems unlikely enough to
         * happen in practice that we can probably feel safe with this.
         */
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
        /* We have already validated that the value is within range. */
        /*
         * TODO-robustness We need to ensure that we don't deserialize a value
         * out of range here.
         */
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

/*
 * General types used to implement API resources
 */

/**
 * Identifies a type of API resource
 */
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ResourceType {
    Organization,
    Project,
    Disk,
    DiskAttachment,
    Instance,
    Rack,
    Sled,
    SagaDbg,
    Vpc,
    VpcFirewallRule,
    VpcSubnet,
    VpcRouter,
    RouterRoute,
    Oximeter,
    MetricProducer,
}

impl Display for ResourceType {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(
            f,
            "{}",
            match self {
                ResourceType::Organization => "organization",
                ResourceType::Project => "project",
                ResourceType::Disk => "disk",
                ResourceType::DiskAttachment => "disk attachment",
                ResourceType::Instance => "instance",
                ResourceType::Rack => "rack",
                ResourceType::Sled => "sled",
                ResourceType::SagaDbg => "saga_dbg",
                ResourceType::Vpc => "vpc",
                ResourceType::VpcFirewallRule => "vpc firewall rule",
                ResourceType::VpcSubnet => "vpc subnet",
                ResourceType::VpcRouter => "vpc router",
                ResourceType::RouterRoute => "vpc router route",
                ResourceType::Oximeter => "oximeter",
                ResourceType::MetricProducer => "metric producer",
            }
        )
    }
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

/*
 * IDENTITY METADATA
 */

/**
 * Identity-related metadata that's included in nearly all public API objects
 */
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IdentityMetadata {
    /** unique, immutable, system-controlled identifier for each resource */
    pub id: Uuid,
    /** unique, mutable, user-controlled identifier for each resource */
    pub name: Name,
    /** human-readable free-form text about a resource */
    pub description: String,
    /** timestamp when this resource was created */
    pub time_created: DateTime<Utc>,
    /** timestamp when this resource was last modified */
    pub time_modified: DateTime<Utc>,
}

/**
 * Create-time identity-related parameters
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IdentityMetadataCreateParams {
    pub name: Name,
    pub description: String,
}

/**
 * Updateable identity-related parameters
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IdentityMetadataUpdateParams {
    pub name: Option<Name>,
    pub description: Option<String>,
}

/*
 * Specific API resources
 */

/*
 * INSTANCES
 */

/**
 * Running state of an Instance (primarily: booted or stopped)
 *
 * This typically reflects whether it's starting, running, stopping, or stopped,
 * but also includes states related to the Instance's lifecycle
 */
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
#[serde(rename_all = "lowercase")]
pub enum InstanceState {
    Creating, /* TODO-polish: paper over Creating in the API with Starting? */
    Starting,
    Running,
    /// Implied that a transition to "Stopped" is imminent.
    Stopping,
    /// The instance is currently stopped.
    Stopped,
    /// The instance is in the process of rebooting - it will remain
    /// in the "rebooting" state until the VM is starting once more.
    Rebooting,
    Repairing,
    Failed,
    Destroyed,
}

impl Display for InstanceState {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(f, "{}", self.label())
    }
}

/*
 * TODO-cleanup why is this error type different from the one for Name?  The
 * reason is probably that Name can be provided by the user, so we want a
 * good validation error.  InstanceState cannot.  Still, is there a way to
 * unify these?
 */
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
            InstanceState::Repairing => "repairing",
            InstanceState::Failed => "failed",
            InstanceState::Destroyed => "destroyed",
        }
    }

    /**
     * Returns true if the given state represents a fully stopped Instance.
     * This means that a transition from an !is_stopped() state must go
     * through Stopping.
     */
    pub fn is_stopped(&self) -> bool {
        match self {
            InstanceState::Starting => false,
            InstanceState::Running => false,
            InstanceState::Stopping => false,
            InstanceState::Rebooting => false,

            InstanceState::Creating => true,
            InstanceState::Stopped => true,
            InstanceState::Repairing => true,
            InstanceState::Failed => true,
            InstanceState::Destroyed => true,
        }
    }
}

/** The number of CPUs in an Instance */
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

/**
 * Client view of an [`InstanceRuntimeState`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
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

/**
 * Client view of an [`Instance`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Instance {
    /* TODO is flattening here the intent in RFD 4? */
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /** id for the project containing this Instance */
    pub project_id: Uuid,

    /** number of CPUs allocated for this Instance */
    pub ncpus: InstanceCpuCount,
    /** memory allocated for this Instance */
    pub memory: ByteCount,
    /** RFC1035-compliant hostname for the Instance. */
    pub hostname: String, /* TODO-cleanup different type? */

    #[serde(flatten)]
    pub runtime: InstanceRuntimeState,
}

/**
 * Create-time parameters for an [`Instance`]
 */
/*
 * TODO We're ignoring "type" for now because no types are specified by the API.
 * Presumably this will need to be its own kind of API object that can be
 * created, modified, removed, etc.
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InstanceCreateParams {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub ncpus: InstanceCpuCount,
    pub memory: ByteCount,
    pub hostname: String, /* TODO-cleanup different type? */
}

/**
 * Updateable properties of an [`Instance`]
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceUpdateParams {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/*
 * DISKS
 */

/**
 * Client view of an [`Disk`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Disk {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub project_id: Uuid,
    pub snapshot_id: Option<Uuid>,
    pub size: ByteCount,
    pub state: DiskState,
    pub device_path: String,
}

/**
 * State of a Disk (primarily: attached or not)
 */
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
#[serde(rename_all = "lowercase")]
#[serde(tag = "state", content = "instance")]
pub enum DiskState {
    /** Disk is being initialized */
    Creating,
    /** Disk is ready but detached from any Instance */
    Detached,
    /** Disk is being attached to the given Instance */
    Attaching(Uuid), /* attached Instance id */
    /** Disk is attached to the given Instance */
    Attached(Uuid), /* attached Instance id */
    /** Disk is being detached from the given Instance */
    Detaching(Uuid), /* attached Instance id */
    /** Disk has been destroyed */
    Destroyed,
    /** Disk is unavailable */
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
    /**
     * Returns the string label for this disk state
     */
    pub fn label(&self) -> &'static str {
        match self {
            DiskState::Creating => "creating",
            DiskState::Detached => "detached",
            DiskState::Attaching(_) => "attaching",
            DiskState::Attached(_) => "attached",
            DiskState::Detaching(_) => "detaching",
            DiskState::Destroyed => "destroyed",
            DiskState::Faulted => "faulted",
        }
    }

    /**
     * Returns whether the Disk is currently attached to, being attached to, or
     * being detached from any Instance.
     */
    pub fn is_attached(&self) -> bool {
        self.attached_instance_id().is_some()
    }

    /**
     * If the Disk is attached to, being attached to, or being detached from an
     * Instance, returns the id for that Instance.  Otherwise returns `None`.
     */
    pub fn attached_instance_id(&self) -> Option<&Uuid> {
        match self {
            DiskState::Attaching(id) => Some(id),
            DiskState::Attached(id) => Some(id),
            DiskState::Detaching(id) => Some(id),

            DiskState::Creating => None,
            DiskState::Detached => None,
            DiskState::Destroyed => None,
            DiskState::Faulted => None,
        }
    }
}

/**
 * Create-time parameters for an [`Disk`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiskCreateParams {
    /** common identifying metadata */
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /** id for snapshot from which the Disk should be created, if any */
    pub snapshot_id: Option<Uuid>, /* TODO should be a name? */
    /** size of the Disk */
    pub size: ByteCount,
}

/**
 * Describes a Disk's attachment to an Instance
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiskAttachment {
    pub instance_id: Uuid,
    pub disk_id: Uuid,
    pub disk_name: Name,
    pub disk_state: DiskState,
}

/*
 * RACKS
 */

/**
 * Client view of an [`Rack`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Rack {
    pub identity: IdentityMetadata,
}

/*
 * SLEDS
 */

/**
 * Client view of an [`Sled`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Sled {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub service_address: SocketAddr,
}

/*
 * Sagas
 *
 * These are currently only intended for observability by developers.  We will
 * eventually want to flesh this out into something more observable for end
 * users.
 */
#[derive(ObjectIdentity, Clone, Debug, Serialize, JsonSchema)]
pub struct Saga {
    pub id: Uuid,
    pub state: SagaState,
    /*
     * TODO-cleanup This object contains a fake `IdentityMetadata`.  Why?  We
     * want to paginate these objects.  http_pagination.rs provides a bunch of
     * useful facilities -- notably `PaginatedById`.  `PaginatedById`
     * requires being able to take an arbitrary object in the result set and get
     * its id.  To do that, it uses the `ObjectIdentity` trait, which expects
     * to be able to return an `IdentityMetadata` reference from an object.
     * Finally, the pagination facilities just pull the `id` out of that.
     *
     * In this case (as well as others, like sleds and racks), we have ids, and
     * we want to be able to paginate by id, but we don't have full identity
     * metadata.  (Or we do, but it's similarly faked up.)  What we should
     * probably do is create a new trait, say `ObjectId`, that returns _just_
     * an id.  We can provide a blanket impl for anything that impls
     * IdentityMetadata.  We can define one-off impls for structs like this
     * one.  Then the id-only pagination interfaces can require just
     * `ObjectId`.
     */
    #[serde(skip)]
    pub identity: IdentityMetadata,
}

impl From<steno::SagaView> for Saga {
    fn from(s: steno::SagaView) -> Self {
        Saga {
            id: Uuid::from(s.id),
            state: SagaState::from(s.state),
            identity: IdentityMetadata {
                /* TODO-cleanup See the note in Saga above. */
                id: Uuid::from(s.id),
                name: format!("saga-{}", s.id).parse().unwrap(),
                description: format!("saga {}", s.id),
                time_created: Utc::now(),
                time_modified: Utc::now(),
            },
        }
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "state")]
pub enum SagaState {
    Running,
    Succeeded,
    Failed { error_node_name: String, error_info: SagaErrorInfo },
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "error")]
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
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct Ipv4Net(pub ipnetwork::Ipv4Network);

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
        schemars::schema::Schema::Object(
            schemars::schema::SchemaObject {
                metadata: Some(Box::new(schemars::schema::Metadata {
                    title: Some("An IPv4 subnet".to_string()),
                    description: Some("An IPv4 subnet, including prefix and subnet mask".to_string()),
                    examples: vec!["192.168.1.0/24".into()],
                    ..Default::default()
                })),
                instance_type: Some(schemars::schema::SingleOrVec::Single(Box::new(schemars::schema::InstanceType::String))),
                string: Some(Box::new(schemars::schema::StringValidation {
                    // Fully-specified IPv4 address. Up to 15 chars for address, plus slash and up to 2 subnet digits.
                    max_length: Some(18),
                    min_length: None,
                    // Addresses must be from an RFC 1918 private address space
                    pattern: Some(
                        concat!(
                            // 10.x.x.x/8
                            r#"^(10\.(25[0-5]|[1-2][0-4][0-9]|[1-9][0-9]|[0-9]\.){2}(25[0-5]|[1-2][0-4][0-9]|[1-9][0-9]|[0-9])/(1[0-9]|2[0-8]|[8-9]))$"#,
                            // 172.16.x.x/12
                            r#"^(172\.16\.(25[0-5]|[1-2][0-4][0-9]|[1-9][0-9]|[0-9])\.(25[0-5]|[1-2][0-4][0-9]|[1-9][0-9]|[0-9])/(1[2-9]|2[0-8]))$"#,
                            // 192.168.x.x/16
                            r#"^(192\.168\.(25[0-5]|[1-2][0-4][0-9]|[1-9][0-9]|[0-9])\.(25[0-5]|[1-2][0-4][0-9]|[1-9][0-9]|[0-9])/(1[6-9]|2[0-8]))$"#,
                        ).to_string(),
                    ),
                })),
                ..Default::default()
            }
        )
    }
}

/// An `Ipv6Net` represents a IPv6 subnetwork, including the address and network mask.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct Ipv6Net(pub ipnetwork::Ipv6Network);

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

impl JsonSchema for Ipv6Net {
    fn schema_name() -> String {
        "Ipv6Net".to_string()
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(
            schemars::schema::SchemaObject {
                metadata: Some(Box::new(schemars::schema::Metadata {
                    title: Some("An IPv6 subnet".to_string()),
                    description: Some("An IPv6 subnet, including prefix and subnet mask".to_string()),
                    examples: vec!["fd12:3456::/64".into()],
                    ..Default::default()
                })),
                instance_type: Some(schemars::schema::SingleOrVec::Single(Box::new(schemars::schema::InstanceType::String))),
                string: Some(Box::new(schemars::schema::StringValidation {
                    // Fully-specified IPv6 address. 4 hex chars per segment, 8 segments, 7
                    // ":"-separators, slash and up to 3 subnet digits
                    max_length: Some(43),
                    min_length: None,
                    pattern: Some(
                        // Conforming to unique local addressing scheme, `fd00::/8`
                        concat!(
                            r#"^(fd|FD)00:((([0-9a-fA-F]{1,4}\:){6}[0-9a-fA-F]{1,4})|(([0-9a-fA-F]{1,4}:){1,6}:))/(6[4-9]|[7-9][0-9]|1[0-1][0-9]|12[0-6])$"#,
                        ).to_string(),
                    ),
                })),
                ..Default::default()
            }
        )
    }
}

/// A VPC subnet represents a logical grouping for instances that allows network traffic between
/// them, within a IPv4 subnetwork or optionall an IPv6 subnetwork.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcSubnet {
    /** common identifying metadata */
    pub identity: IdentityMetadata,

    /** The VPC to which the subnet belongs. */
    pub vpc_id: Uuid,

    // TODO-design: RFD 21 says that V4 subnets are currently required, and V6 are optional. If a
    // V6 address is _not_ specified, one is created with a prefix that depends on the VPC and a
    // unique subnet-specific portion of the prefix (40 and 16 bits for each, respectively).
    //
    // We're leaving out the "view" types here for the external HTTP API for now, so it's not clear
    // how to do the validation of user-specified CIDR blocks, or how to create a block if one is
    // not given.
    /** The IPv4 subnet CIDR block. */
    pub ipv4_block: Option<Ipv4Net>,

    /** The IPv6 subnet CIDR block. */
    pub ipv6_block: Option<Ipv6Net>,
}

/**
 * Create-time parameters for a [`VpcSubnet`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VpcSubnetCreateParams {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub ipv4_block: Option<Ipv4Net>,
    pub ipv6_block: Option<Ipv6Net>,
}

/**
 * Updateable properties of a [`VpcSubnet`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VpcSubnetUpdateParams {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    pub ipv4_block: Option<Ipv4Net>,
    pub ipv6_block: Option<Ipv6Net>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum VpcRouterKind {
    System,
    Custom,
}

/// A VPC router defines a series of rules that indicate where traffic
/// should be sent depending on its destination.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcRouter {
    /// common identifying metadata
    pub identity: IdentityMetadata,

    pub kind: VpcRouterKind,

    /// The VPC to which the router belongs.
    pub vpc_id: Uuid,
}

/// Create-time parameters for a [`VpcRouter`]
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VpcRouterCreateParams {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/// Updateable properties of a [`VpcRouter`]
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VpcRouterUpdateParams {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

/// Represents all possible network target strings as defined in RFD-21
/// This enum itself isn't intended to be used directly but rather as a
/// delegate for subset enums to not have to re-implement all the base type conversions.
///
/// See https://rfd.shared.oxide.computer/rfd/0021#api-target-strings
#[derive(Debug, PartialEq, Display, FromStr)]
pub enum NetworkTarget {
    #[display("vpc:{0}")]
    Vpc(Name),
    #[display("subnet:{0}")]
    Subnet(Name),
    #[display("instance:{0}")]
    Instance(Name),
    #[display("tag:{0}")]
    Tag(Name),
    #[display("ip:{0}")]
    Ip(IpAddr),
    #[display("inetgw:{0}")]
    InternetGateway(Name),
    #[display("fip:{0}")]
    FloatingIp(Name),
}

/// A subset of [`NetworkTarget`], `RouteTarget` specifies all
/// possible targets that a route can forward to.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type", content = "value")]
pub enum RouteTarget {
    Ip(IpAddr),
    Vpc(Name),
    Subnet(Name),
    Instance(Name),
    InternetGateway(Name),
}

impl TryFrom<String> for RouteTarget {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        RouteTarget::try_from(
            value.parse::<NetworkTarget>().map_err(|e| e.to_string())?,
        )
    }
}

impl FromStr for RouteTarget {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        RouteTarget::try_from(String::from(value))
    }
}

impl From<RouteTarget> for NetworkTarget {
    fn from(target: RouteTarget) -> Self {
        match target {
            RouteTarget::Ip(ip) => NetworkTarget::Ip(ip),
            RouteTarget::Vpc(name) => NetworkTarget::Vpc(name),
            RouteTarget::Subnet(name) => NetworkTarget::Subnet(name),
            RouteTarget::Instance(name) => NetworkTarget::Instance(name),
            RouteTarget::InternetGateway(name) => {
                NetworkTarget::InternetGateway(name)
            }
        }
    }
}

impl TryFrom<NetworkTarget> for RouteTarget {
    type Error = String;

    fn try_from(value: NetworkTarget) -> Result<Self, Self::Error> {
        match value {
            NetworkTarget::Ip(ip) => Ok(RouteTarget::Ip(ip)),
            NetworkTarget::Vpc(name) => Ok(RouteTarget::Vpc(name)),
            NetworkTarget::Subnet(name) => Ok(RouteTarget::Subnet(name)),
            NetworkTarget::Instance(name) => Ok(RouteTarget::Instance(name)),
            NetworkTarget::InternetGateway(name) => {
                Ok(RouteTarget::InternetGateway(name))
            }
            _ => Err(format!(
                "Invalid RouteTarget {}, only ip, vpc, subnet, instance, and inetgw are allowed",
                value
            )),
        }
    }
}

impl Display for RouteTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        let target = NetworkTarget::from(self.clone());
        write!(f, "{}", target)
    }
}

/// A subset of [`NetworkTarget`], `RouteDestination` specifies
/// the kind of network traffic that will be matched to be forwarded
/// to the [`RouteTarget`].
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type", content = "value")]
pub enum RouteDestination {
    Ip(IpAddr),
    Vpc(Name),
    Subnet(Name),
}

impl TryFrom<NetworkTarget> for RouteDestination {
    type Error = String;

    fn try_from(value: NetworkTarget) -> Result<Self, Self::Error> {
        match value {
            NetworkTarget::Ip(ip) => Ok(RouteDestination::Ip(ip)),
            NetworkTarget::Vpc(name) => Ok(RouteDestination::Vpc(name)),
            NetworkTarget::Subnet(name) => Ok(RouteDestination::Subnet(name)),
            _ => Err(format!(
                "Invalid RouteTarget {}, only ip, vpc, and subnets are allowed",
                value
            )),
        }
    }
}

impl TryFrom<String> for RouteDestination {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        RouteDestination::try_from(
            value.parse::<NetworkTarget>().map_err(|e| e.to_string())?,
        )
    }
}

impl FromStr for RouteDestination {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        RouteDestination::try_from(String::from(value))
    }
}

impl From<RouteDestination> for NetworkTarget {
    fn from(target: RouteDestination) -> Self {
        match target {
            RouteDestination::Ip(ip) => NetworkTarget::Ip(ip),
            RouteDestination::Vpc(name) => NetworkTarget::Vpc(name),
            RouteDestination::Subnet(name) => NetworkTarget::Subnet(name),
        }
    }
}

impl Display for RouteDestination {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        let target = NetworkTarget::from(self.clone());
        write!(f, "{}", target)
    }
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
    pub identity: IdentityMetadata,

    /// The VPC Router to which the route belongs.
    pub router_id: Uuid,

    /// Describes the kind of router. Set at creation. `read-only`
    pub kind: RouterRouteKind,

    pub target: RouteTarget,
    pub destination: RouteDestination,
}

/// Create-time parameters for a [`RouterRoute`]
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RouterRouteCreateParams {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    pub target: RouteTarget,
    pub destination: RouteDestination,
}

/// Updateable properties of a [`RouterRoute`]
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RouterRouteUpdateParams {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
    pub target: RouteTarget,
    pub destination: RouteDestination,
}

/// A single rule in a VPC firewall
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRule {
    /// common identifying metadata
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
}

/// A single rule in a VPC firewall
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRuleUpdate {
    // In an update, the name is encoded as a key in the JSON object, so we
    // don't include one here
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

/**
 * Updateable properties of a [`Vpc`]'s firewall
 * Note that VpcFirewallRules are implicitly created along with a Vpc,
 * so there is no explicit creation.
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VpcFirewallRuleUpdateParams {
    #[serde(flatten)]
    pub rules: HashMap<Name, VpcFirewallRuleUpdate>,
}

/**
 * Response to an update replacing [`Vpc`]'s firewall
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VpcFirewallRuleUpdateResult {
    #[serde(flatten)]
    pub rules: HashMap<Name, VpcFirewallRule>,
}

impl FromIterator<VpcFirewallRule> for VpcFirewallRuleUpdateResult {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = VpcFirewallRule>,
    {
        Self {
            rules: iter
                .into_iter()
                .map(|rule| (rule.identity.name.clone(), rule))
                .collect(),
        }
    }
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
#[serde(rename_all = "camelCase")]
pub enum VpcFirewallRuleStatus {
    Disabled,
    Enabled,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum VpcFirewallRuleDirection {
    Inbound,
    Outbound,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum VpcFirewallRuleAction {
    Allow,
    Deny,
}

/// A subset of [`NetworkTarget`], `VpcFirewallRuleTarget` specifies all
/// possible targets that a firewall rule can be attached to.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type", content = "value")]
pub enum VpcFirewallRuleTarget {
    Vpc(Name),
    Subnet(Name),
    Instance(Name),
    // Tags not yet implemented
    //Tag(Name),
}

impl TryFrom<String> for VpcFirewallRuleTarget {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        VpcFirewallRuleTarget::try_from(
            value.parse::<NetworkTarget>().map_err(|e| e.to_string())?,
        )
    }
}

impl FromStr for VpcFirewallRuleTarget {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        VpcFirewallRuleTarget::try_from(String::from(value))
    }
}

impl From<VpcFirewallRuleTarget> for NetworkTarget {
    fn from(target: VpcFirewallRuleTarget) -> Self {
        match target {
            VpcFirewallRuleTarget::Vpc(name) => NetworkTarget::Vpc(name),
            VpcFirewallRuleTarget::Subnet(name) => NetworkTarget::Subnet(name),
            VpcFirewallRuleTarget::Instance(name) => {
                NetworkTarget::Instance(name)
            }
        }
    }
}

impl TryFrom<NetworkTarget> for VpcFirewallRuleTarget {
    type Error = String;

    fn try_from(value: NetworkTarget) -> Result<Self, Self::Error> {
        match value {
            NetworkTarget::Vpc(name) => Ok(VpcFirewallRuleTarget::Vpc(name)),
            NetworkTarget::Subnet(name) => {
                Ok(VpcFirewallRuleTarget::Subnet(name))
            }
            NetworkTarget::Instance(name) => {
                Ok(VpcFirewallRuleTarget::Instance(name))
            }
            _ => Err(format!(
                "Invalid VpcFirewallRuleTarget {}, only vpc, subnet, and instance, \
                are allowed",
                value
            )),
        }
    }
}

impl Display for VpcFirewallRuleTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        let target = NetworkTarget::from(self.clone());
        write!(f, "{}", target)
    }
}

/// A subset of [`NetworkTarget`], `VpcFirewallRuleHostFilter` specifies all
/// possible targets that a route can forward to.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type", content = "value")]
pub enum VpcFirewallRuleHostFilter {
    Vpc(Name),
    Subnet(Name),
    Instance(Name),
    // Tags not yet implemented
    // Tag(Name),
    Ip(IpAddr),
    InternetGateway(Name),
}

impl TryFrom<String> for VpcFirewallRuleHostFilter {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        VpcFirewallRuleHostFilter::try_from(
            value.parse::<NetworkTarget>().map_err(|e| e.to_string())?,
        )
    }
}

impl FromStr for VpcFirewallRuleHostFilter {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        VpcFirewallRuleHostFilter::try_from(String::from(value))
    }
}

impl From<VpcFirewallRuleHostFilter> for NetworkTarget {
    fn from(target: VpcFirewallRuleHostFilter) -> Self {
        match target {
            VpcFirewallRuleHostFilter::Vpc(name) => NetworkTarget::Vpc(name),
            VpcFirewallRuleHostFilter::Subnet(name) => {
                NetworkTarget::Subnet(name)
            }
            VpcFirewallRuleHostFilter::Instance(name) => {
                NetworkTarget::Instance(name)
            }
            VpcFirewallRuleHostFilter::Ip(ip) => NetworkTarget::Ip(ip),
            VpcFirewallRuleHostFilter::InternetGateway(name) => {
                NetworkTarget::InternetGateway(name)
            }
        }
    }
}

impl TryFrom<NetworkTarget> for VpcFirewallRuleHostFilter {
    type Error = String;

    fn try_from(value: NetworkTarget) -> Result<Self, Self::Error> {
        match value {
            NetworkTarget::Vpc(name) => {
                Ok(VpcFirewallRuleHostFilter::Vpc(name))
            }
            NetworkTarget::Subnet(name) => {
                Ok(VpcFirewallRuleHostFilter::Subnet(name))
            }
            NetworkTarget::Instance(name) => {
                Ok(VpcFirewallRuleHostFilter::Instance(name))
            }
            NetworkTarget::Ip(ip) => Ok(VpcFirewallRuleHostFilter::Ip(ip)),
            NetworkTarget::InternetGateway(name) => {
                Ok(VpcFirewallRuleHostFilter::InternetGateway(name))
            }
            _ => Err(format!(
                "Invalid VpcFirewallRuleHostFilter {}, only vpc, subnet, \
                instance, ip, and inetgw are allowed",
                value
            )),
        }
    }
}

impl Display for VpcFirewallRuleHostFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        let target = NetworkTarget::from(self.clone());
        write!(f, "{}", target)
    }
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
        const INVALID_PORT_NUMBER_MSG: &'static str = "invalid port number";

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
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
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
            instance_type: Some(schemars::schema::SingleOrVec::Single(
                Box::new(schemars::schema::InstanceType::String),
            )),
            string: Some(Box::new(schemars::schema::StringValidation {
                max_length: Some(11),  // 5 digits for each port and the dash
                min_length: Some(1),
                pattern: Some(
                    r#"^[0-9]{1,5}(-[0-9]{1,5})?$"#.to_string(),
                ),
            })),
            ..Default::default()
        })
    }
}

/// The `MacAddr` represents a Media Access Control (MAC) address, used to uniquely identify
/// hardware devices on a network.
// NOTE: We're using the `macaddr` crate for the internal representation. But as with the `ipnet`,
// this crate does not implement `JsonSchema`.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct MacAddr(pub macaddr::MacAddr6);

impl TryFrom<String> for MacAddr {
    type Error = macaddr::ParseError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse().map(|addr| MacAddr(addr))
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
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some("A MAC address".to_string()),
                description: Some(
                    "A Media Access Control address, in EUI-48 format"
                        .to_string(),
                ),
                examples: vec!["ff:ff:ff:ff:ff:ff".into()],
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::SingleOrVec::Single(
                Box::new(schemars::schema::InstanceType::String),
            )),
            string: Some(Box::new(schemars::schema::StringValidation {
                max_length: Some(17), // 12 hex characters and 5 ":"-separators
                min_length: Some(17),
                pattern: Some(
                    r#"^([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}$"#.to_string(),
                ),
            })),
            ..Default::default()
        })
    }
}

/// A `NetworkInterface` represents a virtual network interface device.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct NetworkInterface {
    /** common identifying metadata */
    pub identity: IdentityMetadata,

    /** The VPC to which the interface belongs. */
    pub vpc_id: Uuid,

    /** The subnet to which the interface belongs. */
    pub subnet_id: Uuid,

    /** The MAC address assigned to this interface. */
    pub mac: MacAddr,

    /** The IP address assigned to this interface. */
    pub ip: IpAddr,
}

#[cfg(test)]
mod test {
    use super::{
        ByteCount, L4Port, L4PortRange, Name, NetworkTarget,
        VpcFirewallRuleAction, VpcFirewallRuleDirection, VpcFirewallRuleFilter,
        VpcFirewallRuleHostFilter, VpcFirewallRulePriority,
        VpcFirewallRuleProtocol, VpcFirewallRuleStatus, VpcFirewallRuleTarget,
        VpcFirewallRuleUpdate, VpcFirewallRuleUpdateParams,
    };
    use crate::api::external::Error;
    use std::convert::TryFrom;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;

    #[test]
    fn test_name_parse() {
        /*
         * Error cases
         */
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
        ];

        for (input, expected_message) in error_cases {
            eprintln!("check name \"{}\" (expecting error)", input);
            assert_eq!(input.parse::<Name>().unwrap_err(), expected_message);
        }

        /*
         * Success cases
         */
        let valid_names: Vec<&str> =
            vec!["abc", "abc-123", "a123", &long_name[0..63]];

        for name in valid_names {
            eprintln!("check name \"{}\" (should be valid)", name);
            assert_eq!(name, name.parse::<Name>().unwrap().as_str());
        }
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
        /* Smallest supported value: all constructors */
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

        /* Largest supported value: both constructors that support it. */
        let max = ByteCount::try_from(i64::MAX).unwrap();
        assert_eq!(i64::MAX, max.to_bytes() as i64);
        assert_eq!(i64::MAX, i64::from(&max));

        let maxu64 = u64::try_from(i64::MAX).unwrap();
        let max = ByteCount::try_from(maxu64).unwrap();
        assert_eq!(i64::MAX, max.to_bytes() as i64);
        assert_eq!(i64::MAX, i64::from(&max));
        assert_eq!(
            (i64::MAX / 1024 / 1024 / 1024 / 1024) as u64,
            max.to_whole_tebibytes()
        );

        /* Value too large (only one constructor can hit this) */
        let bogus = ByteCount::try_from(maxu64 + 1).unwrap_err();
        assert_eq!(bogus.to_string(), "value is too large for a byte count");
        /* Value too small (only one constructor can hit this) */
        let bogus = ByteCount::try_from(-1i64).unwrap_err();
        assert_eq!(bogus.to_string(), "value is too small for a byte count");
        /* For good measure, let's check i64::MIN */
        let bogus = ByteCount::try_from(i64::MIN).unwrap_err();
        assert_eq!(bogus.to_string(), "value is too small for a byte count");

        /*
         * We've now exhaustively tested both sides of all boundary conditions
         * for all three constructors (to the extent that that's possible).
         * Check non-trivial cases for the various accessor functions.  This
         * means picking values in the middle of the range.
         */
        let three_terabytes = 3_000_000_000_000u64;
        let tb3 = ByteCount::try_from(three_terabytes).unwrap();
        assert_eq!(three_terabytes, tb3.to_bytes());
        assert_eq!(2929687500, tb3.to_whole_kibibytes());
        assert_eq!(2861022, tb3.to_whole_mebibytes());
        assert_eq!(2793, tb3.to_whole_gibibytes());
        assert_eq!(2, tb3.to_whole_tebibytes());

        let three_tebibytes = (3u64 * 1024 * 1024 * 1024 * 1024) as u64;
        let tib3 = ByteCount::try_from(three_tebibytes).unwrap();
        assert_eq!(three_tebibytes, tib3.to_bytes());
        assert_eq!(3 * 1024 * 1024 * 1024, tib3.to_whole_kibibytes());
        assert_eq!(3 * 1024 * 1024, tib3.to_whole_mebibytes());
        assert_eq!(3 * 1024, tib3.to_whole_gibibytes());
        assert_eq!(3, tib3.to_whole_tebibytes());
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
        let json = r#"
            {
            "allow-internal-inbound": {
                "status": "enabled",
                "direction": "inbound",
                "targets": [ { "type": "vpc", "value": "default" } ],
                "filters": {"hosts": [ { "type": "vpc", "value": "default" } ]},
                "action": "allow",
                "priority": 65534,
                "description": "allow inbound traffic between instances"
            },
            "rule2": {
                "status": "disabled",
                "direction": "outbound",
                "targets": [ { "type": "vpc", "value": "default" } ],
                "filters": {"ports": [ "22-25", "27" ], "protocols": [ "UDP" ]},
                "action": "deny",
                "priority": 65533,
                "description": "second rule"
            }
            }
            "#;
        let params =
            serde_json::from_str::<VpcFirewallRuleUpdateParams>(json).unwrap();
        assert_eq!(params.rules.len(), 2);
        assert_eq!(
            params.rules[&Name::try_from("allow-internal-inbound".to_string())
                .unwrap()],
            VpcFirewallRuleUpdate {
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
            params.rules[&Name::try_from("rule2".to_string()).unwrap()],
            VpcFirewallRuleUpdate {
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
    fn test_networktarget_parsing() {
        assert_eq!(
            "vpc:my-vital-vpc".parse(),
            Ok(NetworkTarget::Vpc("my-vital-vpc".parse().unwrap()))
        );
        assert_eq!(
            "subnet:my-slick-subnet".parse(),
            Ok(NetworkTarget::Subnet("my-slick-subnet".parse().unwrap()))
        );
        assert_eq!(
            "instance:my-intrepid-instance".parse(),
            Ok(NetworkTarget::Instance(
                "my-intrepid-instance".parse().unwrap()
            ))
        );
        assert_eq!(
            "tag:my-turbid-tag".parse(),
            Ok(NetworkTarget::Tag("my-turbid-tag".parse().unwrap()))
        );
        assert_eq!(
            "ip:127.0.0.1".parse(),
            Ok(NetworkTarget::Ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))))
        );
        assert_eq!(
            "inetgw:my-gregarious-internet-gateway".parse(),
            Ok(NetworkTarget::InternetGateway(
                "my-gregarious-internet-gateway".parse().unwrap()
            ))
        );
        assert_eq!(
            "fip:my-fickle-floating-ip".parse(),
            Ok(NetworkTarget::FloatingIp(
                "my-fickle-floating-ip".parse().unwrap()
            ))
        );
        assert_eq!(
            "nope:this-should-error".parse::<NetworkTarget>().unwrap_err(),
            parse_display::ParseError::new()
        );
    }
}
