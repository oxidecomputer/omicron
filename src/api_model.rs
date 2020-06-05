/*!
 * Data structures and related facilities for representing resources in the API.
 * This includes all representations over the wire for both the external and
 * internal APIs.  The contents here are all HTTP-agnostic.
 */

use chrono::DateTime;
use chrono::Utc;
use futures::stream::Stream;
use serde::Deserialize;
use serde::Serialize;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FormatResult;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;

use crate::api_error::ApiError;

use dropshot::ExtractedParameter;

/*
 * The type aliases below exist primarily to ensure consistency among return
 * types for functions in the `OxideController` and `ControlDataStore`.  The
 * type argument `T` generally implements `ApiObject`.
 */

/** Result of a create operation for the specified type */
pub type CreateResult<T> = Result<Arc<T>, ApiError>;
/** Result of a delete operation for the specified type */
pub type DeleteResult = Result<(), ApiError>;
/** Result of a list operation that returns an ObjectStream */
pub type ListResult<T> = Result<ObjectStream<T>, ApiError>;
/** Result of a lookup operation for the specified type */
pub type LookupResult<T> = Result<Arc<T>, ApiError>;
/** Result of an update operation for the specified type */
pub type UpdateResult<T> = Result<Arc<T>, ApiError>;

/** A stream of Results, each potentially representing an object in the API */
pub type ObjectStream<T> =
    Pin<Box<dyn Stream<Item = Result<Arc<T>, ApiError>> + Send>>;

/*
 * General-purpose types used for client request parameters and return values.
 */

/**
 * Parameters for requesting a specific page of results when listing a
 * collection of objects
 *
 * All list operations in the API are paginated, meaning that there's a limit on
 * the number of objects returned in a single request and clients are expected
 * to make additional requests to fetch the next page of results until the end
 * of the list is reached or the client has found what it needs.  For any list
 * operation, objects are sorted by a particular field that is unique among
 * objects in the list (usually a UTF-8 name or a UUID).  For all requests after
 * the first, the client is expected to provide the value of this field for the
 * last object seen, called the _marker_.  The server will return a page of
 * objects that appear immediately after the object that has this marker.
 *
 * `NameType` is the type of the field used to sort the returned values and it's
 * usually `ApiName`.
 */
#[derive(Deserialize, ExtractedParameter)]
pub struct PaginationParams<NameType> {
    /**
     * If present, this is the value of the sort field for the last object seen
     */
    pub marker: Option<NameType>,

    /**
     * If present, this is an upper bound on how many objects the client wants
     * in this page of results.  The server may choose to use a lower limit.
     */
    pub limit: Option<usize>,
}

/** Default maximum number of items per page of "list" results */
pub const DEFAULT_LIST_PAGE_SIZE: usize = 100;

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
 */
impl TryFrom<String> for ApiName {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() > 63 {
            return Err(format!("name may contain at most 63 characters"));
        }

        let mut iter = value.chars();

        let first = iter
            .next()
            .ok_or_else(|| format!("name requires at least one character"))?;
        if !first.is_ascii_lowercase() {
            return Err(format!(
                "name must begin with an ASCII lowercase character"
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
            return Err(format!("name cannot end with \"-\""));
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

/**
 * Convert an `ApiName` into the `String` representing the actual name.
 */
impl From<ApiName> for String {
    fn from(value: ApiName) -> String {
        value.0
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
        &self.0 == other.as_ref()
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
}

/**
 * A count of bytes, typically used either for memory or storage capacity
 */
/*
 * TODO-cleanup This could benefit from a more complete implementation.
 * TODO-correctness RFD 4 requires that this be a multiple of 256 MiB.  We'll
 * need to write a validator for that.
 */
#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
pub struct ApiByteCount(u64);
impl ApiByteCount {
    pub fn from_bytes(bytes: u64) -> ApiByteCount {
        ApiByteCount(bytes)
    }
    pub fn from_kibibytes(kibibytes: u64) -> ApiByteCount {
        ApiByteCount::from_bytes(1024 * kibibytes)
    }
    pub fn from_mebibytes(mebibytes: u64) -> ApiByteCount {
        ApiByteCount::from_bytes(1024 * 1024 * mebibytes)
    }
    pub fn from_gibibytes(gibibytes: u64) -> ApiByteCount {
        ApiByteCount::from_bytes(1024 * 1024 * 1024 * gibibytes)
    }
    pub fn from_tebibytes(tebibytes: u64) -> ApiByteCount {
        ApiByteCount::from_bytes(1024 * 1024 * 1024 * 1024 * tebibytes)
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

/*
 * General types used to implement API resources
 */

/**
 * Identifies a type of API resource
 */
#[derive(Debug, PartialEq)]
pub enum ApiResourceType {
    Project,
    Disk,
    DiskAttachment,
    Instance,
    Rack,
    Server,
}

impl Display for ApiResourceType {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(f, "{}", match self {
            ApiResourceType::Project => "project",
            ApiResourceType::Disk => "disk",
            ApiResourceType::DiskAttachment => "disk attachment",
            ApiResourceType::Instance => "instance",
            ApiResourceType::Rack => "rack",
            ApiResourceType::Server => "server",
        })
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
 * We also have Instances, Disks, Racks, Servers, and many related types, and we
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

/*
 * IDENTITY METADATA
 */

/**
 * Identity-related metadata that's included in nearly all public API objects
 */
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize)]
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
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiIdentityMetadataCreateParams {
    pub name: ApiName,
    pub description: String,
}

/**
 * Updateable identity-related parameters
 */
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize)]
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

    /*
     * TODO We define a generation number here at the model layer so that in
     * theory the model layer can handle optimistic concurrency control (i.e.,
     * put-only-if-matches-etag and the like).  It's not yet clear if a
     * generation number is the right way to express this.
     */
    /** generation number for this version of the object. */
    pub generation: u64,
}

impl ApiObject for ApiProject {
    type View = ApiProjectView;
    fn to_view(&self) -> ApiProjectView {
        ApiProjectView {
            identity: self.identity.clone(),
        }
    }
}

/**
 * Client view of an [`ApiProject`]
 */
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize)]
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
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize, ExtractedParameter)]
pub struct ApiProjectCreateParams {
    #[serde(flatten)]
    pub identity: ApiIdentityMetadataCreateParams,
}

/**
 * Updateable properties of an [`ApiProject`]
 */
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize)]
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
    Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize,
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
        let label = match self {
            ApiInstanceState::Creating => "creating",
            ApiInstanceState::Starting => "starting",
            ApiInstanceState::Running => "running",
            ApiInstanceState::Stopping => "stopping",
            ApiInstanceState::Stopped => "stopped",
            ApiInstanceState::Repairing => "repairing",
            ApiInstanceState::Failed => "failed",
            ApiInstanceState::Destroyed => "destroyed",
        };

        write!(f, "{}", label)
    }
}

impl ApiInstanceState {
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
#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
pub struct ApiInstanceCpuCount(pub usize);

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
    /** size of the boot disk for the image */
    pub boot_disk_size: ApiByteCount,
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
            project_id: self.project_id.clone(),
            ncpus: self.ncpus,
            memory: self.memory,
            boot_disk_size: self.boot_disk_size,
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
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiInstanceRuntimeState {
    /** runtime state of the Instance */
    pub run_state: ApiInstanceState,
    /** indicates whether a reboot is currently in progress */
    pub reboot_in_progress: bool,
    /** which server is running this Instance */
    pub server_uuid: Uuid,
    /** generation number for this state */
    pub gen: u64,
    /** timestamp for this information */
    pub time_updated: DateTime<Utc>,
}

/**
 * Used to request an Instance state change from a sled agent
 *
 * Right now, it's only the run state that can be changed, though we might want
 * to support changing properties like "ncpus" here.
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiInstanceRuntimeStateRequested {
    pub run_state: ApiInstanceState,
    pub reboot_wanted: bool,
}

/**
 * Client view of an [`ApiInstanceRuntimeState`]
 */
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize)]
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
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize)]
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
    /** size of the boot disk for the image */
    pub boot_disk_size: ApiByteCount,
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
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize, ExtractedParameter)]
pub struct ApiInstanceCreateParams {
    #[serde(flatten)]
    pub identity: ApiIdentityMetadataCreateParams,
    pub ncpus: ApiInstanceCpuCount,
    pub memory: ApiByteCount,
    pub boot_disk_size: ApiByteCount,
    pub hostname: String, /* TODO-cleanup different type? */
}

/**
 * Updateable properties of an [`ApiInstance`]
 */
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize)]
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
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize)]
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
        let device_path =
            format!("/mnt/{}", String::from(self.identity.name.clone()),);
        ApiDiskView {
            identity: self.identity.clone(),
            project_id: self.project_id.clone(),
            snapshot_id: self.create_snapshot_id.clone(),
            size: self.size.clone(),
            state: self.runtime.disk_state.clone(),
            device_path,
        }
    }
}

/**
 * State of a Disk (primarily: attached or not)
 */
#[derive(
    Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize,
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
        let label = match self {
            ApiDiskState::Creating => "creating",
            ApiDiskState::Detached => "detached",
            ApiDiskState::Attaching(_) => "attaching",
            ApiDiskState::Attached(_) => "attached",
            ApiDiskState::Detaching(_) => "detaching",
            ApiDiskState::Destroyed => "destroyed",
            ApiDiskState::Faulted => "faulted",
        };

        write!(f, "{}", label)
    }
}

impl ApiDiskState {
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
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiDiskRuntimeState {
    /** runtime state of the Disk */
    pub disk_state: ApiDiskState,
    /** generation number for this state */
    pub gen: u64,
    /** timestamp for this information */
    pub time_updated: DateTime<Utc>,
}

/**
 * Create-time parameters for an [`ApiDisk`]
 */
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize)]
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
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiDiskAttachment {
    pub instance_name: ApiName,
    pub instance_id: Uuid,
    pub disk_name: ApiName,
    pub disk_id: Uuid,
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
    pub id: Uuid,
}

impl ApiObject for ApiRack {
    type View = ApiRackView;
    fn to_view(&self) -> ApiRackView {
        ApiRackView {
            id: self.id.clone(),
        }
    }
}

/**
 * Client view of an [`ApiRack`]
 */
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiRackView {
    pub id: Uuid,
}

/*
 * SERVERS
 */

/**
 * A Server in the external API
 */
pub struct ApiServer {
    pub id: Uuid,
    pub service_address: SocketAddr,
}

impl ApiObject for ApiServer {
    type View = ApiServerView;
    fn to_view(&self) -> ApiServerView {
        ApiServerView {
            id: self.id.clone(),
            service_address: self.service_address.clone(),
        }
    }
}

/**
 * Client view of an [`ApiServer`]
 */
#[serde(rename_all = "camelCase")]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiServerView {
    pub id: Uuid,
    pub service_address: SocketAddr,
}

/*
 * Internal Control Plane API objects
 */

/**
 * Sent by a sled agent on startup to OXC request further instruction
 */
#[derive(Serialize, Deserialize)]
pub struct ApiServerStartupInfo {
    /** the address of the sled agent's API endpoint */
    pub sa_address: SocketAddr,
}

/**
 * Sent from OXC to a sled agent to establish the runtime state of an Instance
 */
#[derive(Serialize, Deserialize)]
pub struct InstanceEnsureBody {
    /**
     * Last runtime state of the Instance known to OXC (used if the agent has
     * never seen this Instance before).
     */
    pub initial_runtime: ApiInstanceRuntimeState,
    /** requested runtime state of the Instance */
    pub target: ApiInstanceRuntimeStateRequested,
}

/**
 * Sent from OXC to a sled agent to establish the runtime state of a Disk
 */
#[derive(Serialize, Deserialize)]
pub struct DiskEnsureBody {
    /**
     * Last runtime state of the Disk known to OXC (used if the agent has never
     * seen this Disk before).
     */
    pub initial_runtime: ApiDiskRuntimeState,
    /** requested runtime state of the Disk */
    pub target: ApiDiskStateRequested,
}

#[cfg(test)]
mod test {
    use super::ApiByteCount;
    use super::ApiName;
    use crate::api_error::ApiError;
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
            assert_eq!(name, String::from(ApiName::try_from(name).unwrap()));
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
        let zero = ApiByteCount::from_bytes(0);
        assert_eq!(0, zero.to_bytes());
        assert_eq!(0, zero.to_whole_kibibytes());
        assert_eq!(0, zero.to_whole_mebibytes());
        assert_eq!(0, zero.to_whole_gibibytes());
        assert_eq!(0, zero.to_whole_tebibytes());

        let three_terabytes = 3_000_000_000_000;
        let tb3 = ApiByteCount::from_bytes(three_terabytes);
        assert_eq!(three_terabytes, tb3.to_bytes());
        assert_eq!(2929687500, tb3.to_whole_kibibytes());
        assert_eq!(2861022, tb3.to_whole_mebibytes());
        assert_eq!(2793, tb3.to_whole_gibibytes());
        assert_eq!(2, tb3.to_whole_tebibytes());

        let three_tebibytes = 3 * 1024 * 1024 * 1024 * 1024;
        let tib3 = ApiByteCount::from_bytes(three_tebibytes);
        assert_eq!(three_tebibytes, tib3.to_bytes());
        assert_eq!(3 * 1024 * 1024 * 1024, tib3.to_whole_kibibytes());
        assert_eq!(3 * 1024 * 1024, tib3.to_whole_mebibytes());
        assert_eq!(3 * 1024, tib3.to_whole_gibibytes());
        assert_eq!(3, tib3.to_whole_tebibytes());
    }
}
