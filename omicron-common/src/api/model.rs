/*!
 * Data structures and related facilities for representing resources in the API
 *
 * This includes all representations over the wire for both the external and
 * internal APIs.  The contents here are all HTTP-agnostic.
 */

use crate::api::Error;
use anyhow::anyhow;
use anyhow::Context;
use api_identity::ObjectIdentity;
use chrono::DateTime;
use chrono::Utc;
pub use dropshot::PaginationOrder;
use futures::future::ready;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FormatResult;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::time::Duration;
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

/**
 * A name used in the API
 *
 * Names are generally user-provided unique identifiers, highly constrained as
 * described in RFD 4.  An `Name` can only be constructed with a string
 * that's valid as a name.
 */
#[derive(
    Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize,
)]
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

/**
 * Convenience parse function for literal strings, primarily for the test suite.
 */
impl TryFrom<&str> for Name {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
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
        Name::try_from(value).map_err(|e| Error::InvalidValue {
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
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub enum ResourceType {
    Project,
    Disk,
    DiskAttachment,
    Instance,
    Rack,
    Sled,
    SagaDbg,
}

impl Display for ResourceType {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(
            f,
            "{}",
            match self {
                ResourceType::Project => "project",
                ResourceType::Disk => "disk",
                ResourceType::DiskAttachment => "disk attachment",
                ResourceType::Instance => "instance",
                ResourceType::Rack => "rack",
                ResourceType::Sled => "sled",
                ResourceType::SagaDbg => "saga_dbg",
            }
        )
    }
}

/**
 * Object represents a resource in the API and is implemented by concrete
 * types representing specific API resources.
 *
 * Consider a Project, which is about as simple a resource as we have.  The
 * `Project` struct represents a project as understood by the API.  It
 * contains all the fields necessary to implement a Project.  It has several
 * related types:
 *
 * * `ProjectView` is what gets emitted by the API when a user asks for a
 *   Project
 * * `ProjectCreateParams` is what must be provided to the API when a user
 *   wants to create a new project
 * * `ProjectUpdateParams` is what must be provided to the API when a user
 *   wants to update a project.
 *
 * We also have Instances, Disks, Racks, Sleds, and many related types, and we
 * expect to add many more types like images, networking abstractions,
 * organizations, teams, users, system components, and the like.  See RFD 4 for
 * details.  Some resources may not have analogs for all these types because
 * they're immutable (e.g., the `Rack` resource doesn't define a
 * "CreateParams" type).
 *
 * The only thing guaranteed by the `Object` trait is that the type can be
 * converted to a View, which is something that can be serialized.
 */
/*
 * TODO-coverage: each type could have unit tests for various invalid input
 * types?
 */
pub trait Object {
    type View: Serialize + Clone + Debug;
    fn to_view(&self) -> Self::View;
}

/**
 * Given an `ObjectStream<Object>` (for some specific `Object` type),
 * return a vector of the objects' views.  Any failures are ignored.
 */
/*
 * TODO-hardening: Consider how to better deal with these failures.  We should
 * probably at least log something.
 */
pub async fn to_view_list<T: Object>(
    object_stream: ObjectStream<T>,
) -> Vec<T::View> {
    object_stream
        .filter(|maybe_object| ready(maybe_object.is_ok()))
        .map(|maybe_object| maybe_object.unwrap().to_view())
        .collect::<Vec<T::View>>()
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
 * PROJECTS
 */

/**
 * A Project in the external API
 */
pub struct Project {
    /** common identifying metadata */
    pub identity: IdentityMetadata,
}

impl Object for Project {
    type View = ProjectView;
    fn to_view(&self) -> ProjectView {
        ProjectView { identity: self.identity.clone() }
    }
}

/**
 * Client view of an [`Project`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProjectView {
    /*
     * TODO-correctness is flattening here (and in all the other types) the
     * intent in RFD 4?
     */
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

/**
 * Create-time parameters for an [`Project`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProjectCreateParams {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

/**
 * Updateable properties of an [`Project`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProjectUpdateParams {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,
}

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

/**
 * Requestable running state of an Instance.
 *
 * A subset of [`InstanceState`].
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
pub enum InstanceStateRequested {
    Running,
    Stopped,
    // Issues a reset command to the instance, such that it should
    // stop and then immediately become running.
    Reboot,
    Destroyed,
}

impl Display for InstanceStateRequested {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(f, "{}", self.label())
    }
}

impl InstanceStateRequested {
    fn label(&self) -> &str {
        match self {
            InstanceStateRequested::Running => "running",
            InstanceStateRequested::Stopped => "stopped",
            InstanceStateRequested::Reboot => "reboot",
            InstanceStateRequested::Destroyed => "destroyed",
        }
    }

    /**
     * Returns true if the state represents a stopped Instance.
     */
    pub fn is_stopped(&self) -> bool {
        match self {
            InstanceStateRequested::Running => false,
            InstanceStateRequested::Stopped => true,
            InstanceStateRequested::Reboot => false,
            InstanceStateRequested::Destroyed => true,
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
 * An Instance (VM) in the external API
 */
#[derive(Clone, Debug)]
pub struct Instance {
    /** common identifying metadata */
    pub identity: IdentityMetadata,

    /** id for the project containing this Instance */
    pub project_id: Uuid,

    /** number of CPUs allocated for this Instance */
    pub ncpus: InstanceCpuCount,
    /** memory allocated for this Instance */
    pub memory: ByteCount,
    /** RFC1035-compliant hostname for the Instance. */
    pub hostname: String, /* TODO-cleanup different type? */

    /** state owned by the data plane */
    pub runtime: InstanceRuntimeState,
    /* TODO-completeness: add disks, network, tags, metrics */
}

impl Object for Instance {
    type View = InstanceView;
    fn to_view(&self) -> InstanceView {
        InstanceView {
            identity: self.identity.clone(),
            project_id: self.project_id,
            ncpus: self.ncpus,
            memory: self.memory,
            hostname: self.hostname.clone(),
            runtime: self.runtime.to_view(),
        }
    }
}

/**
 * Runtime state of the Instance, including the actual running state and minimal
 * metadata
 *
 * This state is owned by the sled agent running that Instance.
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceRuntimeState {
    /** runtime state of the Instance */
    pub run_state: InstanceState,
    /** which sled is running this Instance */
    pub sled_uuid: Uuid,
    /** generation number for this state */
    pub gen: Generation,
    /** timestamp for this information */
    pub time_updated: DateTime<Utc>,
}

/**
 * Used to request an Instance state change from a sled agent
 *
 * Right now, it's only the run state that can be changed, though we might want
 * to support changing properties like "ncpus" here.
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceRuntimeStateRequested {
    pub run_state: InstanceStateRequested,
}

/**
 * Client view of an [`InstanceRuntimeState`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InstanceRuntimeStateView {
    pub run_state: InstanceState,
    pub time_run_state_updated: DateTime<Utc>,
}

impl Object for InstanceRuntimeState {
    type View = InstanceRuntimeStateView;
    fn to_view(&self) -> InstanceRuntimeStateView {
        InstanceRuntimeStateView {
            run_state: self.run_state,
            time_run_state_updated: self.time_updated,
        }
    }
}

/**
 * Client view of an [`Instance`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InstanceView {
    /* TODO is flattening here the intent in RFD 4? */
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /** id for the project containing this Instance */
    pub project_id: Uuid,

    /** number of CPUs allocated for this Instance */
    pub ncpus: InstanceCpuCount,
    /** memory, in gigabytes, allocated for this Instance */
    pub memory: ByteCount,
    /** RFC1035-compliant hostname for the Instance. */
    pub hostname: String, /* TODO-cleanup different type? */

    #[serde(flatten)]
    pub runtime: InstanceRuntimeStateView,
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
 * A Disk (network block device) in the external API
 */
#[derive(Clone, Debug)]
pub struct Disk {
    /** common identifying metadata */
    pub identity: IdentityMetadata,
    /** id for the project containing this Disk */
    pub project_id: Uuid,
    /**
     * id for the snapshot from which this Disk was created (None means a blank
     * disk)
     */
    pub create_snapshot_id: Option<Uuid>,
    /** size of the Disk */
    pub size: ByteCount,
    /** runtime state of the Disk */
    pub runtime: DiskRuntimeState,
}

/**
 * Client view of an [`Disk`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DiskView {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub project_id: Uuid,
    pub snapshot_id: Option<Uuid>,
    pub size: ByteCount,
    pub state: DiskState,
    pub device_path: String,
}

impl Object for Disk {
    type View = DiskView;
    fn to_view(&self) -> DiskView {
        /*
         * TODO-correctness: can the name always be used as a path like this
         * or might it need to be sanitized?
         */
        let device_path = format!("/mnt/{}", self.identity.name.as_str());
        DiskView {
            identity: self.identity.clone(),
            project_id: self.project_id,
            snapshot_id: self.create_snapshot_id,
            size: self.size,
            state: self.runtime.disk_state.clone(),
            device_path,
        }
    }
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
 * Runtime state of the Disk, which includes its attach state and some minimal
 * metadata
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DiskRuntimeState {
    /** runtime state of the Disk */
    pub disk_state: DiskState,
    /** generation number for this state */
    pub gen: Generation,
    /** timestamp for this information */
    pub time_updated: DateTime<Utc>,
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

impl Object for DiskAttachment {
    type View = Self;
    fn to_view(&self) -> Self::View {
        self.clone()
    }
}

/**
 * Used to request a Disk state change
 */
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DiskStateRequested {
    Detached,
    Attached(Uuid),
    Destroyed,
    Faulted,
}

impl DiskStateRequested {
    /**
     * Returns whether the requested state is attached to an Instance or not.
     */
    pub fn is_attached(&self) -> bool {
        match self {
            DiskStateRequested::Detached => false,
            DiskStateRequested::Destroyed => false,
            DiskStateRequested::Faulted => false,

            DiskStateRequested::Attached(_) => true,
        }
    }
}

/*
 * RACKS
 */

/**
 * A Rack in the external API
 */
pub struct Rack {
    pub identity: IdentityMetadata,
}

impl Object for Rack {
    type View = RackView;
    fn to_view(&self) -> RackView {
        RackView { identity: self.identity.clone() }
    }
}

/**
 * Client view of an [`Rack`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RackView {
    pub identity: IdentityMetadata,
}

/*
 * SLEDS
 */

/**
 * A Sled in the external API
 */
pub struct Sled {
    pub identity: IdentityMetadata,
    pub service_address: SocketAddr,
}

impl Object for Sled {
    type View = SledView;
    fn to_view(&self) -> SledView {
        SledView {
            identity: self.identity.clone(),
            service_address: self.service_address,
        }
    }
}

/**
 * Client view of an [`Sled`]
 */
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SledView {
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
pub struct SagaView {
    pub id: Uuid,
    pub state: SagaStateView,
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

impl Object for SagaView {
    type View = Self;
    fn to_view(&self) -> Self::View {
        self.clone()
    }
}

impl From<steno::SagaView> for SagaView {
    fn from(s: steno::SagaView) -> Self {
        SagaView {
            id: Uuid::from(s.id),
            state: SagaStateView::from(s.state),
            identity: IdentityMetadata {
                /* TODO-cleanup See the note in SagaView above. */
                id: Uuid::from(s.id),
                name: Name::try_from(format!("saga-{}", s.id)).unwrap(),
                description: format!("saga {}", s.id),
                time_created: Utc::now(),
                time_modified: Utc::now(),
            },
        }
    }
}

/*
 * TODO-robustness This type is unnecessarily loosey-goosey.  For example, see
 * the use of Options in the "Done" variant.
 */
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SagaStateView {
    Running,
    #[serde(rename_all = "camelCase")]
    Done {
        failed: bool,
        error_node_name: Option<String>,
        error_info: Option<steno::ActionError>,
    },
}

impl From<steno::SagaStateView> for SagaStateView {
    fn from(st: steno::SagaStateView) -> Self {
        match st {
            steno::SagaStateView::Ready { .. } => SagaStateView::Running,
            steno::SagaStateView::Running { .. } => SagaStateView::Running,
            steno::SagaStateView::Done { result, .. } => match result.kind {
                Ok(_) => SagaStateView::Done {
                    failed: false,
                    error_node_name: None,
                    error_info: None,
                },
                Err(e) => SagaStateView::Done {
                    failed: true,
                    error_node_name: Some(e.error_node_name),
                    error_info: Some(e.error_source),
                },
            },
        }
    }
}

/// A Virtual Private Cloud (VPC) object.
#[derive(Clone, Debug)]
pub struct VPC {
    /** common identifying metadata */
    pub identity: IdentityMetadata,
    /** id for the project containing this Instance */
    pub project_id: Uuid,
}

/// An `Ipv4Net` represents a IPv4 subnetwork, including the address and network mask.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct Ipv4Net(pub ipnet::Ipv4Net);

impl std::ops::Deref for Ipv4Net {
    type Target = ipnet::Ipv4Net;
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
pub struct Ipv6Net(pub ipnet::Ipv6Net);

impl std::ops::Deref for Ipv6Net {
    type Target = ipnet::Ipv6Net;
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
                            r#"^(fd|FD)00:((([0-8a-fA-F]{1,4}\:){6}[0-8a-fA-F]{1,4})|(([0-8a-fA-F]{1,4}:){1,6}:))/(6[4-9]|[7-9][0-9]|1[0-1][0-9]|12[0-6])$"#,
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
#[derive(Clone, Debug)]
pub struct VPCSubnet {
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

/// The `MacAddr` represents a Media Access Control (MAC) address, used to uniquely identify
/// hardware devices on a network.
// NOTE: We're using the `macaddr` crate for the internal representation. But as with the `ipnet`,
// this crate does not implement `JsonSchema`, nor the the SQL conversion traits `FromSql` and
// `ToSql`.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct MacAddr(pub macaddr::MacAddr6);

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
                    r#"^([0-8a-fA-F]{2}:){5}[0-8a-fA-F]{2}$"#.to_string(),
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

/*
 * Internal Control Plane API objects
 */

/**
 * Sent by a sled agent on startup to Nexus to request further instruction
 */
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SledAgentStartupInfo {
    /** the address of the sled agent's API endpoint */
    pub sa_address: SocketAddr,
}

/**
 * Sent from Nexus to a sled agent to establish the runtime state of an Instance
 */
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstanceEnsureBody {
    /**
     * Last runtime state of the Instance known to Nexus (used if the agent
     * has never seen this Instance before).
     */
    pub initial_runtime: InstanceRuntimeState,
    /** requested runtime state of the Instance */
    pub target: InstanceRuntimeStateRequested,
}

/**
 * Sent from Nexus to a sled agent to establish the runtime state of a Disk
 */
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct DiskEnsureBody {
    /**
     * Last runtime state of the Disk known to Nexus (used if the agent has
     * never seen this Disk before).
     */
    pub initial_runtime: DiskRuntimeState,
    /** requested runtime state of the Disk */
    pub target: DiskStateRequested,
}

/*
 * Bootstrap Agent objects
 */

/**
 * Identity signed by local RoT and Oxide certificate chain.
 */
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct BootstrapAgentShareRequest {
    // TODO-completeness: format TBD; currently opaque.
    pub identity: Vec<u8>,
}

/**
 * Sent between bootstrap agents to establish trust quorum.
 */
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct BootstrapAgentShareResponse {
    // TODO-completeness: format TBD; currently opaque.
    pub shared_secret: Vec<u8>,
}

/*
 * Oximeter producer/collector objects.
 */

/**
 * Information announced by a metric server, used so that clients can contact it and collect
 * available metric data from it.
 */
#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct ProducerEndpoint {
    pub id: Uuid,
    pub address: SocketAddr,
    pub base_route: String,
    pub interval: Duration,
}

impl ProducerEndpoint {
    /**
     * Return the route that can be used to request metric data.
     */
    pub fn collection_route(&self) -> String {
        format!("{}/{}", &self.base_route, &self.id)
    }
}

/// Message used to notify Nexus that this oximeter instance is up and running.
#[derive(Debug, Clone, Copy, JsonSchema, Serialize, Deserialize)]
pub struct OximeterInfo {
    /// The ID for this oximeter instance.
    pub collector_id: Uuid,

    /// The address on which this oximeter instance listens for requests
    pub address: SocketAddr,
}

/// An assignment of an Oximeter instance to a metric producer for collection.
#[derive(Debug, Clone, Copy, JsonSchema, Serialize, Deserialize)]
pub struct OximeterAssignment {
    pub oximeter_id: Uuid,
    pub producer_id: Uuid,
}

#[cfg(test)]
mod test {
    use super::ByteCount;
    use super::Name;
    use crate::api::Error;
    use std::convert::TryFrom;

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
            assert_eq!(Name::try_from(input).unwrap_err(), expected_message);
        }

        /*
         * Success cases
         */
        let valid_names: Vec<&str> =
            vec!["abc", "abc-123", "a123", &long_name[0..63]];

        for name in valid_names {
            eprintln!("check name \"{}\" (should be valid)", name);
            assert_eq!(name, Name::try_from(name).unwrap().as_str());
        }
    }

    #[test]
    fn test_name_parse_from_param() {
        let result = Name::from_param(String::from("my-name"), "the_name");
        assert!(result.is_ok());
        assert_eq!(result, Ok(Name::try_from("my-name").unwrap()));

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
}
