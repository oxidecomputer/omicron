/*!
 * Data structures and related facilities for representing resources in the API
 *
 * This includes all representations over the wire for both the external and
 * internal APIs.  The contents here are all HTTP-agnostic.
 */

use crate::error::ApiError;
use anyhow::anyhow;
use anyhow::Context;
use api_identity::ApiObjectIdentity;
use chrono::DateTime;
use chrono::Utc;
pub use dropshot::PaginationOrder;
use futures::future::ready;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FormatResult;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use thiserror::Error;
use uuid::Uuid;

/*
 * The type aliases below exist primarily to ensure consistency among return
 * types for functions in the `nexus::Nexus` and `nexus::DataStore`.  The
 * type argument `T` generally implements `ApiObject`.
 */

/** Result of a create operation for the specified type */
pub type CreateResult<T> = Result<T, ApiError>;
/** Result of a delete operation for the specified type */
pub type DeleteResult = Result<(), ApiError>;
/** Result of a list operation that returns an ObjectStream */
pub type ListResult<T> = Result<ObjectStream<T>, ApiError>;
/** Result of a lookup operation for the specified type */
pub type LookupResult<T> = Result<T, ApiError>;
/** Result of an update operation for the specified type */
pub type UpdateResult<T> = Result<T, ApiError>;

/**
 * A stream of Results, each potentially representing an object in the API
 */
pub type ObjectStream<T> = BoxStream<'static, Result<T, ApiError>>;

/*
 * General-purpose types used for client request parameters and return values.
 */

/**
 * Describes an `ApiObject` that has its own identity metadata.  This is
 * currently used only for pagination.
 */
pub trait ApiObjectIdentity {
    fn identity(&self) -> &ApiIdentityMetadata;
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
 * usually `ApiName`.
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
 * described in RFD 4.  An `ApiName` can only be constructed with a string
 * that's valid as a name.
 */
#[derive(
    Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(try_from = "String")]
pub struct ApiName(String);

/**
 * `ApiName::try_from(String)` is the primary method for constructing an ApiName
 * from an input string.  This validates the string according to our
 * requirements for a name.
 * TODO-cleanup why shouldn't callers use TryFrom<&str>?
 */
impl TryFrom<String> for ApiName {
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

        Ok(ApiName(value))
    }
}

/**
 * Convenience parse function for literal strings, primarily for the test suite.
 */
impl TryFrom<&str> for ApiName {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        ApiName::try_from(String::from(value))
    }
}

impl<'a> From<&'a ApiName> for &'a str {
    fn from(n: &'a ApiName) -> Self {
        n.as_str()
    }
}

/**
 * `ApiName` instances are comparable like Strings, primarily so that they can
 * be used as keys in trees.
 */
impl<S> PartialEq<S> for ApiName
where
    S: AsRef<str>,
{
    fn eq(&self, other: &S) -> bool {
        self.0 == other.as_ref()
    }
}

/**
 * Custom JsonSchema implementation to encode the constraints on ApiName
 */
/*
 * TODO: 1. make this part of schemars w/ rename and maxlen annotations
 * TODO: 2. integrate the regex with `try_from`
 */
impl JsonSchema for ApiName {
    fn schema_name() -> String {
        "ApiName".to_string()
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

impl ApiName {
    /**
     * Parse an `ApiName`.  This is a convenience wrapper around
     * `ApiName::try_from(String)` that marshals any error into an appropriate
     * `ApiError`.
     */
    pub fn from_param(value: String, label: &str) -> Result<ApiName, ApiError> {
        ApiName::try_from(value).map_err(|e| ApiError::InvalidValue {
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
pub struct ApiByteCount(u64);
impl ApiByteCount {
    pub fn from_kibibytes_u32(kibibytes: u32) -> ApiByteCount {
        ApiByteCount::try_from(1024 * u64::from(kibibytes)).unwrap()
    }

    pub fn from_mebibytes_u32(mebibytes: u32) -> ApiByteCount {
        ApiByteCount::try_from(1024 * 1024 * u64::from(mebibytes)).unwrap()
    }

    pub fn from_gibibytes_u32(gibibytes: u32) -> ApiByteCount {
        ApiByteCount::try_from(1024 * 1024 * 1024 * u64::from(gibibytes))
            .unwrap()
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
#[derive(Debug, Eq, Error, Ord, PartialEq, PartialOrd)]
pub enum ByteCountRangeError {
    #[error("value is too small for a byte count")]
    TooSmall,
    #[error("value is too large for a byte count")]
    TooLarge,
}
impl TryFrom<u64> for ApiByteCount {
    type Error = ByteCountRangeError;

    fn try_from(bytes: u64) -> Result<Self, Self::Error> {
        if i64::try_from(bytes).is_err() {
            Err(ByteCountRangeError::TooLarge)
        } else {
            Ok(ApiByteCount(bytes))
        }
    }
}

impl TryFrom<i64> for ApiByteCount {
    type Error = ByteCountRangeError;

    fn try_from(bytes: i64) -> Result<Self, Self::Error> {
        Ok(ApiByteCount(
            u64::try_from(bytes).map_err(|_| ByteCountRangeError::TooSmall)?,
        ))
    }
}

impl From<u32> for ApiByteCount {
    fn from(value: u32) -> Self {
        ApiByteCount(u64::from(value))
    }
}

impl From<&ApiByteCount> for i64 {
    fn from(b: &ApiByteCount) -> Self {
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
pub struct ApiGeneration(u64);
impl ApiGeneration {
    pub fn new() -> ApiGeneration {
        ApiGeneration(1)
    }

    pub fn next(&self) -> ApiGeneration {
        /*
         * It should technically be an operational error if this wraps or even
         * exceeds the value allowed by an i64.  But it seems unlikely enough to
         * happen in practice that we can probably feel safe with this.
         */
        let next_gen = self.0 + 1;
        assert!(next_gen <= u64::try_from(i64::MAX).unwrap());
        ApiGeneration(next_gen)
    }
}

impl Display for ApiGeneration {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        f.write_str(&self.0.to_string())
    }
}

impl From<&ApiGeneration> for i64 {
    fn from(g: &ApiGeneration) -> Self {
        /* We have already validated that the value is within range. */
        /*
         * TODO-robustness We need to ensure that we don't deserialize a value
         * out of range here.
         */
        i64::try_from(g.0).unwrap()
    }
}

impl TryFrom<i64> for ApiGeneration {
    type Error = anyhow::Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Ok(ApiGeneration(
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
pub enum ApiResourceType {
    Project,
    Disk,
    DiskAttachment,
    Instance,
    Rack,
    Sled,
    SagaDbg,
}

impl Display for ApiResourceType {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(
            f,
            "{}",
            match self {
                ApiResourceType::Project => "project",
                ApiResourceType::Disk => "disk",
                ApiResourceType::DiskAttachment => "disk attachment",
                ApiResourceType::Instance => "instance",
                ApiResourceType::Rack => "rack",
                ApiResourceType::Sled => "sled",
                ApiResourceType::SagaDbg => "saga_dbg",
            }
        )
    }
}

/**
 * ApiObject represents a resource in the API and is implemented by concrete
 * types representing specific API resources.
 *
 * Consider a Project, which is about as simple a resource as we have.  The
 * `ApiProject` struct represents a project as understood by the API.  It
 * contains all the fields necessary to implement a Project.  It has several
 * related types:
 *
 * * `ApiProjectView` is what gets emitted by the API when a user asks for a
 *   Project
 * * `ApiProjectCreateParams` is what must be provided to the API when a user
 *   wants to create a new project
 * * `ApiProjectUpdateParams` is what must be provided to the API when a user
 *   wants to update a project.
 *
 * We also have Instances, Disks, Racks, Sleds, and many related types, and we
 * expect to add many more types like images, networking abstractions,
 * organizations, teams, users, system components, and the like.  See RFD 4 for
 * details.  Some resources may not have analogs for all these types because
 * they're immutable (e.g., the `ApiRack` resource doesn't define a
 * "CreateParams" type).
 *
 * The only thing guaranteed by the `ApiObject` trait is that the type can be
 * converted to a View, which is something that can be serialized.
 */
/*
 * TODO-coverage: each type could have unit tests for various invalid input
 * types?
 */
pub trait ApiObject {
    type View: Serialize + Clone + Debug;
    fn to_view(&self) -> Self::View;
}

/**
 * Given an `ObjectStream<ApiObject>` (for some specific `ApiObject` type),
 * return a vector of the objects' views.  Any failures are ignored.
 */
/*
 * TODO-hardening: Consider how to better deal with these failures.  We should
 * probably at least log something.
 */
pub async fn to_view_list<T: ApiObject>(
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
pub struct ApiIdentityMetadata {
    /** unique, immutable, system-controlled identifier for each resource */
    pub id: Uuid,
    /** unique, mutable, user-controlled identifier for each resource */
    pub name: ApiName,
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
pub struct ApiIdentityMetadataCreateParams {
    pub name: ApiName,
    pub description: String,
}

/**
 * Updateable identity-related parameters
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiIdentityMetadataUpdateParams {
    pub name: Option<ApiName>,
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
pub struct ApiProject {
    /** common identifying metadata */
    pub identity: ApiIdentityMetadata,
}

impl ApiObject for ApiProject {
    type View = ApiProjectView;
    fn to_view(&self) -> ApiProjectView {
        ApiProjectView { identity: self.identity.clone() }
    }
}

/**
 * Client view of an [`ApiProject`]
 */
#[derive(
    ApiObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct ApiProjectView {
    /*
     * TODO-correctness is flattening here (and in all the other types) the
     * intent in RFD 4?
     */
    #[serde(flatten)]
    pub identity: ApiIdentityMetadata,
}

/**
 * Create-time parameters for an [`ApiProject`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiProjectCreateParams {
    #[serde(flatten)]
    pub identity: ApiIdentityMetadataCreateParams,
}

/**
 * Updateable properties of an [`ApiProject`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiProjectUpdateParams {
    #[serde(flatten)]
    pub identity: ApiIdentityMetadataUpdateParams,
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
pub enum ApiInstanceState {
    Creating, /* TODO-polish: paper over Creating in the API with Starting? */
    Starting,
    Running,
    Stopping,
    Stopped,
    Repairing,
    Failed,
    Destroyed,
}

impl Display for ApiInstanceState {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(f, "{}", self.label())
    }
}

/*
 * TODO-cleanup why is this error type different from the one for ApiName?  The
 * reason is probably that ApiName can be provided by the user, so we want a
 * good validation error.  ApiInstanceState cannot.  Still, is there a way to
 * unify these?
 */
impl TryFrom<&str> for ApiInstanceState {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        parse_str_using_serde(value)
    }
}

impl<'a> From<&'a ApiInstanceState> for &'a str {
    fn from(s: &'a ApiInstanceState) -> &'a str {
        s.label()
    }
}

impl ApiInstanceState {
    fn label(&self) -> &str {
        match self {
            ApiInstanceState::Creating => "creating",
            ApiInstanceState::Starting => "starting",
            ApiInstanceState::Running => "running",
            ApiInstanceState::Stopping => "stopping",
            ApiInstanceState::Stopped => "stopped",
            ApiInstanceState::Repairing => "repairing",
            ApiInstanceState::Failed => "failed",
            ApiInstanceState::Destroyed => "destroyed",
        }
    }

    /**
     * Returns true if the given state represents a fully stopped Instance.
     * This means that a transition from an is_not_stopped() state must go
     * through Stopping.
     */
    pub fn is_stopped(&self) -> bool {
        match self {
            ApiInstanceState::Starting => false,
            ApiInstanceState::Running => false,
            ApiInstanceState::Stopping => false,

            ApiInstanceState::Creating => true,
            ApiInstanceState::Stopped => true,
            ApiInstanceState::Repairing => true,
            ApiInstanceState::Failed => true,
            ApiInstanceState::Destroyed => true,
        }
    }
}

/** The number of CPUs in an Instance */
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ApiInstanceCpuCount(pub u16);

impl TryFrom<i64> for ApiInstanceCpuCount {
    type Error = anyhow::Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Ok(ApiInstanceCpuCount(
            u16::try_from(value).context("parsing CPU count")?,
        ))
    }
}

impl From<&ApiInstanceCpuCount> for i64 {
    fn from(c: &ApiInstanceCpuCount) -> Self {
        i64::from(c.0)
    }
}

/**
 * An Instance (VM) in the external API
 */
#[derive(Clone, Debug)]
pub struct ApiInstance {
    /** common identifying metadata */
    pub identity: ApiIdentityMetadata,

    /** id for the project containing this Instance */
    pub project_id: Uuid,

    /** number of CPUs allocated for this Instance */
    pub ncpus: ApiInstanceCpuCount,
    /** memory allocated for this Instance */
    pub memory: ApiByteCount,
    /** RFC1035-compliant hostname for the Instance. */
    pub hostname: String, /* TODO-cleanup different type? */

    /** state owned by the data plane */
    pub runtime: ApiInstanceRuntimeState,
    /* TODO-completeness: add disks, network, tags, metrics */
}

impl ApiObject for ApiInstance {
    type View = ApiInstanceView;
    fn to_view(&self) -> ApiInstanceView {
        ApiInstanceView {
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
pub struct ApiInstanceRuntimeState {
    /** runtime state of the Instance */
    pub run_state: ApiInstanceState,
    /** indicates whether a reboot is currently in progress */
    pub reboot_in_progress: bool,
    /** which sled is running this Instance */
    pub sled_uuid: Uuid,
    /** generation number for this state */
    pub gen: ApiGeneration,
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
pub struct ApiInstanceRuntimeStateRequested {
    pub run_state: ApiInstanceState,
    pub reboot_wanted: bool,
}

/**
 * Client view of an [`ApiInstanceRuntimeState`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiInstanceRuntimeStateView {
    pub run_state: ApiInstanceState,
    pub time_run_state_updated: DateTime<Utc>,
}

impl ApiObject for ApiInstanceRuntimeState {
    type View = ApiInstanceRuntimeStateView;
    fn to_view(&self) -> ApiInstanceRuntimeStateView {
        ApiInstanceRuntimeStateView {
            run_state: self.run_state.clone(),
            time_run_state_updated: self.time_updated,
        }
    }
}

/**
 * Client view of an [`ApiInstance`]
 */
#[derive(
    ApiObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct ApiInstanceView {
    /* TODO is flattening here the intent in RFD 4? */
    #[serde(flatten)]
    pub identity: ApiIdentityMetadata,

    /** id for the project containing this Instance */
    pub project_id: Uuid,

    /** number of CPUs allocated for this Instance */
    pub ncpus: ApiInstanceCpuCount,
    /** memory, in gigabytes, allocated for this Instance */
    pub memory: ApiByteCount,
    /** RFC1035-compliant hostname for the Instance. */
    pub hostname: String, /* TODO-cleanup different type? */

    #[serde(flatten)]
    pub runtime: ApiInstanceRuntimeStateView,
}

/**
 * Create-time parameters for an [`ApiInstance`]
 */
/*
 * TODO We're ignoring "type" for now because no types are specified by the API.
 * Presumably this will need to be its own kind of API object that can be
 * created, modified, removed, etc.
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiInstanceCreateParams {
    #[serde(flatten)]
    pub identity: ApiIdentityMetadataCreateParams,
    pub ncpus: ApiInstanceCpuCount,
    pub memory: ApiByteCount,
    pub hostname: String, /* TODO-cleanup different type? */
}

/**
 * Updateable properties of an [`ApiInstance`]
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiInstanceUpdateParams {
    #[serde(flatten)]
    pub identity: ApiIdentityMetadataUpdateParams,
}

/*
 * DISKS
 */

/**
 * A Disk (network block device) in the external API
 */
#[derive(Clone, Debug)]
pub struct ApiDisk {
    /** common identifying metadata */
    pub identity: ApiIdentityMetadata,
    /** id for the project containing this Disk */
    pub project_id: Uuid,
    /**
     * id for the snapshot from which this Disk was created (None means a blank
     * disk)
     */
    pub create_snapshot_id: Option<Uuid>,
    /** size of the Disk */
    pub size: ApiByteCount,
    /** runtime state of the Disk */
    pub runtime: ApiDiskRuntimeState,
}

/**
 * Client view of an [`ApiDisk`]
 */
#[derive(
    ApiObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct ApiDiskView {
    #[serde(flatten)]
    pub identity: ApiIdentityMetadata,
    pub project_id: Uuid,
    pub snapshot_id: Option<Uuid>,
    pub size: ApiByteCount,
    pub state: ApiDiskState,
    pub device_path: String,
}

impl ApiObject for ApiDisk {
    type View = ApiDiskView;
    fn to_view(&self) -> ApiDiskView {
        /*
         * TODO-correctness: can the name always be used as a path like this
         * or might it need to be sanitized?
         */
        let device_path = format!("/mnt/{}", self.identity.name.as_str());
        ApiDiskView {
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
pub enum ApiDiskState {
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

impl Display for ApiDiskState {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(f, "{}", self.label())
    }
}

impl TryFrom<(&str, Option<Uuid>)> for ApiDiskState {
    type Error = String;

    fn try_from(
        (s, maybe_id): (&str, Option<Uuid>),
    ) -> Result<Self, Self::Error> {
        match (s, maybe_id) {
            ("creating", None) => Ok(ApiDiskState::Creating),
            ("detached", None) => Ok(ApiDiskState::Detached),
            ("destroyed", None) => Ok(ApiDiskState::Destroyed),
            ("faulted", None) => Ok(ApiDiskState::Faulted),
            ("attaching", Some(id)) => Ok(ApiDiskState::Attaching(id)),
            ("attached", Some(id)) => Ok(ApiDiskState::Attached(id)),
            ("detaching", Some(id)) => Ok(ApiDiskState::Detaching(id)),
            _ => Err(format!(
                "unexpected value for disk state: {:?} with attached id {:?}",
                s, maybe_id
            )),
        }
    }
}

pub fn parse_str_using_serde<T: Serialize + DeserializeOwned>(
    s: &str,
) -> Result<T, anyhow::Error> {
    /*
     * Round-tripping through serde is a little absurd, but has the benefit
     * of always staying in sync with the real definition.  (The initial
     * serialization is necessary to correctly handle any quotes or the like
     * in the input string.)
     */
    let json = serde_json::to_string(s).unwrap();
    serde_json::from_str(&json).context("parsing instance state")
}

impl ApiDiskState {
    /**
     * Returns the string label for this disk state
     */
    pub fn label(&self) -> &'static str {
        match self {
            ApiDiskState::Creating => "creating",
            ApiDiskState::Detached => "detached",
            ApiDiskState::Attaching(_) => "attaching",
            ApiDiskState::Attached(_) => "attached",
            ApiDiskState::Detaching(_) => "detaching",
            ApiDiskState::Destroyed => "destroyed",
            ApiDiskState::Faulted => "faulted",
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
            ApiDiskState::Attaching(id) => Some(id),
            ApiDiskState::Attached(id) => Some(id),
            ApiDiskState::Detaching(id) => Some(id),

            ApiDiskState::Creating => None,
            ApiDiskState::Detached => None,
            ApiDiskState::Destroyed => None,
            ApiDiskState::Faulted => None,
        }
    }
}

/**
 * Runtime state of the Disk, which includes its attach state and some minimal
 * metadata
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ApiDiskRuntimeState {
    /** runtime state of the Disk */
    pub disk_state: ApiDiskState,
    /** generation number for this state */
    pub gen: ApiGeneration,
    /** timestamp for this information */
    pub time_updated: DateTime<Utc>,
}

/**
 * Create-time parameters for an [`ApiDisk`]
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiDiskCreateParams {
    /** common identifying metadata */
    #[serde(flatten)]
    pub identity: ApiIdentityMetadataCreateParams,
    /** id for snapshot from which the Disk should be created, if any */
    pub snapshot_id: Option<Uuid>, /* TODO should be a name? */
    /** size of the Disk */
    pub size: ApiByteCount,
}

/**
 * Describes a Disk's attachment to an Instance
 */
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiDiskAttachment {
    pub instance_id: Uuid,
    pub disk_id: Uuid,
    pub disk_name: ApiName,
    pub disk_state: ApiDiskState,
}

impl ApiObject for ApiDiskAttachment {
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
pub enum ApiDiskStateRequested {
    Detached,
    Attached(Uuid),
    Destroyed,
    Faulted,
}

impl ApiDiskStateRequested {
    /**
     * Returns whether the requested state is attached to an Instance or not.
     */
    pub fn is_attached(&self) -> bool {
        match self {
            ApiDiskStateRequested::Detached => false,
            ApiDiskStateRequested::Destroyed => false,
            ApiDiskStateRequested::Faulted => false,

            ApiDiskStateRequested::Attached(_) => true,
        }
    }
}

/*
 * RACKS
 */

/**
 * A Rack in the external API
 */
pub struct ApiRack {
    pub identity: ApiIdentityMetadata,
}

impl ApiObject for ApiRack {
    type View = ApiRackView;
    fn to_view(&self) -> ApiRackView {
        ApiRackView { identity: self.identity.clone() }
    }
}

/**
 * Client view of an [`ApiRack`]
 */
#[derive(
    ApiObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct ApiRackView {
    pub identity: ApiIdentityMetadata,
}

/*
 * SLEDS
 */

/**
 * A Sled in the external API
 */
pub struct ApiSled {
    pub identity: ApiIdentityMetadata,
    pub service_address: SocketAddr,
}

impl ApiObject for ApiSled {
    type View = ApiSledView;
    fn to_view(&self) -> ApiSledView {
        ApiSledView {
            identity: self.identity.clone(),
            service_address: self.service_address,
        }
    }
}

/**
 * Client view of an [`ApiSled`]
 */
#[derive(
    ApiObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct ApiSledView {
    #[serde(flatten)]
    pub identity: ApiIdentityMetadata,
    pub service_address: SocketAddr,
}

/*
 * Sagas
 *
 * These are currently only intended for observability by developers.  We will
 * eventually want to flesh this out into something more observable for end
 * users.
 */
#[derive(ApiObjectIdentity, Clone, Debug, Serialize, JsonSchema)]
pub struct ApiSagaView {
    pub id: Uuid,
    pub template: ApiSagaTemplateView,
    pub state: ApiSagaStateView,
    /*
     * XXX ApiObjectIdentity should not be necessary, so it should not be faked
     * up.
     */
    #[serde(skip)]
    pub identity: ApiIdentityMetadata,
}

impl ApiObject for ApiSagaView {
    type View = Self;
    fn to_view(&self) -> Self::View {
        self.clone()
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiSagaTemplateView {
    pub name: String,
    pub nodes: Vec<ApiSagaTemplateNode>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiSagaTemplateNode {
    pub id: steno::SagaNodeId,
    pub name: String,
    pub label: String,
}

/*
 * TODO-robustness This type is unnecessarily loosey-goosey.
 */
#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiSagaStateView {
    Unloaded,
    Running,
    #[serde(rename_all = "camelCase")]
    Done {
        failed: bool,
        error_node_name: Option<String>,
        error_info: Option<steno::ActionError>,
    },
    #[serde(rename_all = "camelCase")]
    Abandoned {
        time_abandoned: DateTime<Utc>,
        reason: String,
    },
}

/*
 * Internal Control Plane API objects
 */

/**
 * Sent by a sled agent on startup to Nexus to request further instruction
 */
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ApiSledAgentStartupInfo {
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
    pub initial_runtime: ApiInstanceRuntimeState,
    /** requested runtime state of the Instance */
    pub target: ApiInstanceRuntimeStateRequested,
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
    pub initial_runtime: ApiDiskRuntimeState,
    /** requested runtime state of the Disk */
    pub target: ApiDiskStateRequested,
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

#[cfg(test)]
mod test {
    use super::ApiByteCount;
    use super::ApiName;
    use crate::error::ApiError;
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
            assert_eq!(ApiName::try_from(input).unwrap_err(), expected_message);
        }

        /*
         * Success cases
         */
        let valid_names: Vec<&str> =
            vec!["abc", "abc-123", "a123", &long_name[0..63]];

        for name in valid_names {
            eprintln!("check name \"{}\" (should be valid)", name);
            assert_eq!(name, ApiName::try_from(name).unwrap().as_str());
        }
    }

    #[test]
    fn test_name_parse_from_param() {
        let result = ApiName::from_param(String::from("my-name"), "the_name");
        assert!(result.is_ok());
        assert_eq!(result, Ok(ApiName::try_from("my-name").unwrap()));

        let result = ApiName::from_param(String::from(""), "the_name");
        assert!(result.is_err());
        assert_eq!(
            result,
            Err(ApiError::InvalidValue {
                label: "the_name".to_string(),
                message: "name requires at least one character".to_string()
            })
        );
    }

    #[test]
    fn test_bytecount() {
        /* Smallest supported value: all constructors */
        let zero = ApiByteCount::from(0u32);
        assert_eq!(0, zero.to_bytes());
        assert_eq!(0, zero.to_whole_kibibytes());
        assert_eq!(0, zero.to_whole_mebibytes());
        assert_eq!(0, zero.to_whole_gibibytes());
        assert_eq!(0, zero.to_whole_tebibytes());
        let zero = ApiByteCount::try_from(0i64).unwrap();
        assert_eq!(0, zero.to_bytes());
        let zero = ApiByteCount::try_from(0u64).unwrap();
        assert_eq!(0, zero.to_bytes());

        /* Largest supported value: both constructors that support it. */
        let max = ApiByteCount::try_from(i64::MAX).unwrap();
        assert_eq!(i64::MAX, max.to_bytes() as i64);
        assert_eq!(i64::MAX, i64::from(&max));

        let maxu64 = u64::try_from(i64::MAX).unwrap();
        let max = ApiByteCount::try_from(maxu64).unwrap();
        assert_eq!(i64::MAX, max.to_bytes() as i64);
        assert_eq!(i64::MAX, i64::from(&max));
        assert_eq!(
            (i64::MAX / 1024 / 1024 / 1024 / 1024) as u64,
            max.to_whole_tebibytes()
        );

        /* Value too large (only one constructor can hit this) */
        let bogus = ApiByteCount::try_from(maxu64 + 1).unwrap_err();
        assert_eq!(bogus.to_string(), "value is too large for a byte count");
        /* Value too small (only one constructor can hit this) */
        let bogus = ApiByteCount::try_from(-1i64).unwrap_err();
        assert_eq!(bogus.to_string(), "value is too small for a byte count");
        /* For good measure, let's check i64::MIN */
        let bogus = ApiByteCount::try_from(i64::MIN).unwrap_err();
        assert_eq!(bogus.to_string(), "value is too small for a byte count");

        /*
         * We've now exhaustively tested both sides of all boundary conditions
         * for all three constructors (to the extent that that's possible).
         * Check non-trivial cases for the various accessor functions.  This
         * means picking values in the middle of the range.
         */
        let three_terabytes = 3_000_000_000_000u64;
        let tb3 = ApiByteCount::try_from(three_terabytes).unwrap();
        assert_eq!(three_terabytes, tb3.to_bytes());
        assert_eq!(2929687500, tb3.to_whole_kibibytes());
        assert_eq!(2861022, tb3.to_whole_mebibytes());
        assert_eq!(2793, tb3.to_whole_gibibytes());
        assert_eq!(2, tb3.to_whole_tebibytes());

        let three_tebibytes = (3u64 * 1024 * 1024 * 1024 * 1024) as u64;
        let tib3 = ApiByteCount::try_from(three_tebibytes).unwrap();
        assert_eq!(three_tebibytes, tib3.to_bytes());
        assert_eq!(3 * 1024 * 1024 * 1024, tib3.to_whole_kibibytes());
        assert_eq!(3 * 1024 * 1024, tib3.to_whole_mebibytes());
        assert_eq!(3 * 1024, tib3.to_whole_gibibytes());
        assert_eq!(3, tib3.to_whole_tebibytes());
    }
}
