/*!
 * Facilities for working with objects in the API (agnostic to both the HTTP
 * transport through which consumers interact with them and the backend
 * implementation (simulator or a real rack)).
 */

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FormatResult;
use uuid::Uuid;

use crate::api_error::ApiError;

use dropshot::ExtractedParameter;

/** Default maximum number of items per page of "list" results */
pub const DEFAULT_LIST_PAGE_SIZE: usize = 100;

/**
 * ApiObject is a trait implemented by the types used to represent objects in
 * the API.  It's helpful to start with a concrete example, so let's consider
 * a Project, which is about as simple a resource as we have.
 *
 * The `ApiProject` struct represents a project as understood by the API.  It
 * contains all the fields necessary to implement a Project.  It has several
 * associated types:
 *
 * * `ApiProjectView`, which is what gets emitted by the API when a user asks
 *    for a Project
 * * `ApiProjectCreateParams`, which is what must be provided to the API when a
 *   user wants to create a new project
 * * `ApiProjectUpdate`, which is what must be provided to the API when a user
 *   wants to update a project.
 *
 * We expect to add many more types to the API for things like instances, disks,
 * images, networking abstractions, organizations, teams, users, system
 * components, and the like.  See RFD 4 for details.  The current plan is to add
 * types and supporting functions for each of these resources.  However,
 * different types may support different operations.  For examples, instances
 * will have additional operations (like "boot" and "halt").  System component
 * resources may be immutable (i.e., they won't define a "CreateParams" type, an
 * "UpdateParams" type, nor create or update functions).
 *
 * The only thing guaranteed by the `ApiObject` trait is that the type can be
 * converted to a View, which is something that can be serialized.
 *
 * TODO-coverage: each type could have unit tests for various invalid input
 * types?
 */
pub trait ApiObject {
    type View: Serialize + Clone + Debug;
    fn to_view(&self) -> Self::View;
}

/**
 * List of API resource types
 */
#[derive(Debug, PartialEq)]
pub enum ApiResourceType {
    Project,
    Instance,
    Rack,
}

impl Display for ApiResourceType {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(f, "{}", match self {
            ApiResourceType::Project => "project",
            ApiResourceType::Instance => "instance",
            ApiResourceType::Rack => "rack",
        })
    }
}

/*
 * Data types used in the API
 */

/**
 * ApiName represents a "name" value in the API.  An ApiName can only be
 * constructed with a valid name string.
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
 * `ApiName::try_from(&str)` is a convenience primarily for the test suite and
 * other hardcoded names.
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

/*
 * IDENTITY METADATA
 * (shared by most API objects)
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiIdentityMetadataCreateParams {
    pub name: ApiName,
    pub description: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiIdentityMetadataUpdateParams {
    pub name: Option<ApiName>,
    pub description: Option<String>,
}

/*
 * PROJECTS
 */

/**
 * Represents a Project in the API.  See RFD for field details.
 */
pub struct ApiProject {
    /** common identifying metadata */
    pub identity: ApiIdentityMetadata,

    /*
     * TODO
     * We define a generation number here at the model layer so that in theory
     * the model layer can handle optimistic concurrency control (i.e.,
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
 * Represents the properties of an ApiProject that can be seen by end users.
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiProjectView {
    /* TODO is flattening here the intent in RFD 4? */
    #[serde(flatten)]
    pub identity: ApiIdentityMetadata,
}

/**
 * Represents the create-time parameters for an ApiProject.
 */
#[derive(Clone, Debug, Deserialize, Serialize, ExtractedParameter)]
pub struct ApiProjectCreateParams {
    #[serde(flatten)]
    pub identity: ApiIdentityMetadataCreateParams,
}

/**
 * Represents the properties of an ApiProject that can be updated by end users.
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiProjectUpdateParams {
    #[serde(flatten)]
    pub identity: ApiIdentityMetadataUpdateParams,
}

/*
 * INSTANCES
 */

/**
 * ApiInstanceState describes the runtime state of the instance (i.e., starting,
 * running, etc.)
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

/** Represents the number of CPUs in an instance. */
#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
pub struct ApiInstanceCpuCount(pub usize);

/**
 * Represents a count of bytes, typically used either for memory or storage.
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

/**
 * Represents an instance (VM) in the API
 */
#[derive(Clone, Debug)]
pub struct ApiInstance {
    /** common identifying metadata */
    pub identity: ApiIdentityMetadata,

    /** id for the project containing this instance */
    pub project_id: Uuid,

    /** number of CPUs allocated for this instance */
    pub ncpus: ApiInstanceCpuCount,
    /** memory, in gigabytes, allocated for this instance */
    pub memory: ApiByteCount,
    /** size of the boot disk for the image */
    pub boot_disk_size: ApiByteCount,
    /** RFC1035-compliant hostname for the instance. */
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
 * The runtime state of an Instance is owned by the server controller running
 * that Instance.
 */
#[derive(Clone, Debug)]
pub struct ApiInstanceRuntimeState {
    /** runtime state of the instance */
    pub run_state: ApiInstanceState,
    /** indicates whether a reboot is currently in progress */
    pub reboot_in_progress: bool,
    /** which server is running this instance */
    pub server_uuid: Uuid,
    /** generation number for this state */
    pub gen: u64,
    /** timestamp for this information */
    pub time_updated: DateTime<Utc>,
}

/**
 * RuntimeStateParams is used to request an Instance state change from a server
 * controller.  Right now, it's only the run state that can be changed, though
 * we could imagine supporting changing properties like "ncpus" here.  If we
 * allow other properties here, we may want to make them Options so that callers
 * don't have to know the prior state already.
 */
#[derive(Clone, Debug)]
pub struct ApiInstanceRuntimeStateParams {
    pub run_state: ApiInstanceState,
    pub reboot_wanted: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiInstanceRuntimeStateView {
    pub run_state: ApiInstanceState,
    pub run_state_updated: DateTime<Utc>,
}

impl ApiObject for ApiInstanceRuntimeState {
    type View = ApiInstanceRuntimeStateView;
    fn to_view(&self) -> ApiInstanceRuntimeStateView {
        ApiInstanceRuntimeStateView {
            run_state: self.run_state.clone(),
            run_state_updated: self.time_updated,
        }
    }
}

/**
 * Represents the properties of an `ApiInstance` that can be seen by end users.
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiInstanceView {
    /* TODO is flattening here the intent in RFD 4? */
    #[serde(flatten)]
    pub identity: ApiIdentityMetadata,

    /** id for the project containing this instance */
    pub project_id: Uuid,

    /** number of CPUs allocated for this instance */
    pub ncpus: ApiInstanceCpuCount,
    /** memory, in gigabytes, allocated for this instance */
    pub memory: ApiByteCount,
    /** size of the boot disk for the image */
    pub boot_disk_size: ApiByteCount,
    /** RFC1035-compliant hostname for the instance. */
    pub hostname: String, /* TODO-cleanup different type? */

    #[serde(flatten)]
    pub runtime: ApiInstanceRuntimeStateView,
}

/**
 * Represents the create-time parameters for an ApiInstance.
 * TODO We're ignoring "type" for now because no types are specified by the API.
 * Presumably this will need to be its own kind of API object that can be
 * created, modified, removed, etc.
 */
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
 * Represents the properties of an ApiInstance that can be updated by end users.
 * TODO Very little is updateable right now because it's not clear if we'll want
 * the key properties to be updated only by a separate "resize" API that would
 * be async.
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiInstanceUpdateParams {
    #[serde(flatten)]
    pub identity: ApiIdentityMetadataUpdateParams,
}

/*
 * RACKS
 */

/**
 * Concrete type for a Rack in the API.  Note that this type is not really used
 * for anything.  See `OxideRack` for details.
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
 * Represents a Rack in the API.  See RFD for field details.
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiRackView {
    pub id: Uuid,
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
