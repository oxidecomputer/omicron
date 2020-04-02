/*!
 * Facilities for working with objects in the API (agnostic to both the HTTP
 * transport through which consumers interact with them and the backend
 * implementation (simulator or a real rack)).
 */

use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use futures::future::ready;
use futures::stream::Stream;
use futures::stream::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use std::any::Any;
use std::convert::TryFrom;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FormatResult;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;

use crate::api_error::ApiError;

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
 * Recall that we intend to support two backends: one backed by a real Oxide
 * rack and the other backed by a simulator.  The interface to these backends is
 * defined by the `ApiBackend` trait, which provides functions for operating on
 * resources like projects.  For example, `ApiBackend` provides
 * `project_lookup(primary key)`,
 * `project_list(marker: Option, limit: usize)`,
 * `project_create(project, ApiProjectCreateParams)`,
 * `project_update(project, ApiProjectUpdateParams)`, and
 * `project_delete(project)`.  These are all `async` functions.
 *
 * We expect to add many more types to the API for things like instances, disks,
 * images, networking abstractions, organizations, teams, users, system
 * components, and the like.  See RFD 4 for details.  The current plan is to add
 * types and supporting backend functions for each of these resources.  However,
 * different types may support different operations.  For examples, instances
 * will have additional operations (like "boot" and "halt").  System component
 * resources may be immutable (i.e., they won't define a "CreateParams" type, an
 * "UpdateParams" type, nor create or update functions on the Backend).
 *
 * The only thing guaranteed by the `ApiObject` trait is that the type can be
 * converted to a View, which is something that can be serialized.
 */
pub trait ApiObject {
    type View: Serialize;
    fn to_view(&self) -> Self::View;
}

/**
 * List of API resource types
 */
#[derive(Debug, PartialEq)]
pub enum ApiResourceType {
    Project,
}

impl Display for ApiResourceType {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(f, "{}", match self {
            ApiResourceType::Project => "project",
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

/*
 * TODO-correctness: RFD 4 calls for an "id" here, but it's not clear yet how it
 * could be used.  At some point we'll need to resolve that and decide what we
 * want to do here.
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

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiIdentityMetadataCreateParams {
    pub name: ApiName,
    pub description: String,
}

#[derive(Debug, Deserialize, Serialize)]
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
    /** private data used by the backend implementation */
    pub backend_impl: Box<dyn Any + Send + Sync>,
    /** common identifying metadata */
    pub identity: ApiIdentityMetadata,

    /*
     * TODO
     * We define a generation number here at the model layer so that in theory
     * the model layer can handle optimistic concurrency control (i.e.,
     * put-only-if-matches-etag and the like).  It's not yet clear if this is
     * better handled in the backend or if a generation number is the right way
     * to express this.
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
#[derive(Debug, Deserialize, Serialize)]
pub struct ApiProjectView {
    /* TODO is flattening here the intent in RFD 4? */
    #[serde(flatten)]
    pub identity: ApiIdentityMetadata,
}

/**
 * Represents the create-time parameters for an ApiProject.
 */
#[derive(Debug, Deserialize, Serialize)]
pub struct ApiProjectCreateParams {
    #[serde(flatten)]
    pub identity: ApiIdentityMetadataCreateParams,
}

/**
 * Represents the properties of an ApiProject that can be updated by end users.
 */
#[derive(Debug, Deserialize, Serialize)]
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
    Starting,
    Running,
    Stopping,
    Stopped,
    Repairing,
    Failed,
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
pub struct ApiInstance {
    /** private data used by the backend implementation */
    pub backend_impl: Box<dyn Any + Send + Sync>,
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
    /** current runtime state of the instance */
    pub state: ApiInstanceState,
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
            state: self.state.clone(),
        }
    }
}

/**
 * Represents the properties of an `ApiInstance` that can be seen by end users.
 */
#[derive(Debug, Deserialize, Serialize)]
pub struct ApiInstanceView {
    /* TODO is flattening here the intent in RFD 4? */
    #[serde(flatten)]
    pub identity: ApiIdentityMetadata,

    /** id for the project containing this instance */
    pub project_id: Uuid,

    /** number of CPUs allocated for this instance */
    #[serde(flatten)]
    pub ncpus: ApiInstanceCpuCount,
    /** memory, in gigabytes, allocated for this instance */
    #[serde(flatten)]
    pub memory: ApiByteCount,
    /** size of the boot disk for the image */
    #[serde(flatten)]
    pub boot_disk_size: ApiByteCount,
    /** RFC1035-compliant hostname for the instance. */
    pub hostname: String, /* TODO-cleanup different type? */
    /** current runtime state of the instance */
    pub state: ApiInstanceState,
}

/**
 * Represents the create-time parameters for an ApiInstance.
 * TODO We're ignoring "type" for now because no types are specified by the API.
 * Presumably this will need to be its own kind of API object that can be
 * created, modified, removed, etc.
 */
#[derive(Debug, Deserialize, Serialize)]
pub struct ApiInstanceCreateParams {
    #[serde(flatten)]
    pub identity: ApiIdentityMetadataCreateParams,
    #[serde(flatten)]
    pub ncpus: ApiInstanceCpuCount,
    #[serde(flatten)]
    pub memory: ApiByteCount,
    #[serde(flatten)]
    pub boot_disk_size: ApiByteCount,
    pub hostname: String, /* TODO-cleanup different type? */
}

/**
 * Represents the properties of an ApiInstance that can be updated by end users.
 * TODO Very little is updateable right now because it's not clear if we'll want
 * the key properties to be updated only by a separate "resize" API that would
 * be async.
 */
#[derive(Debug, Deserialize, Serialize)]
pub struct ApiInstanceUpdateParams {
    #[serde(flatten)]
    pub identity: ApiIdentityMetadataUpdateParams,
}

/*
 * BACKEND INTERFACES
 *
 * TODO: Currently, the HTTP layer calls directly into the backend layer.
 * That's probably not what we want.  A good example where we don't really want
 * that is where the user requests to delete a project.  We need to go delete
 * everything _in_ that project first.  That's common code that ought to live
 * outside the backend.  It's also not HTTP-specific, if we were to throw some
 * other control interface on this server.  Hence, it belongs in this model
 * layer.
 */

/*
 * These type aliases exist primarily to make it easier to be consistent in the
 * way these functions look.
 */

/** Result of a create operation for the specified type. */
pub type CreateResult<T> = Result<Arc<T>, ApiError>;
/** Result of a delete operation for the specified type. */
pub type DeleteResult = Result<(), ApiError>;
/** Result of a list operation that returns an ObjectStream. */
pub type ListResult<T> = Result<ObjectStream<T>, ApiError>;
/** Result of a lookup operation for the specified type. */
pub type LookupResult<T> = Result<Arc<T>, ApiError>;
/** Result of an update operation for the specified type. */
pub type UpdateResult<T> = Result<Arc<T>, ApiError>;

/** A stream of Results, each potentially representing an object in the API. */
pub type ObjectStream<T> =
    Pin<Box<dyn Stream<Item = Result<Arc<T>, ApiError>> + Send>>;

#[derive(Deserialize)]
pub struct PaginationParams<NameType> {
    pub marker: Option<NameType>,
    pub limit: Option<usize>,
}

/**
 * Given an `ObjectStream<ApiObject>` (for some specific `ApiObject` type),
 * return a vector of the objects' views.  Any failures are ignored.
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

/**
 * Represents a backend implementation of the API.
 * TODO Is it possible to make some of these operations more generic?  A
 * particularly good example is probably list() (or even lookup()), where
 * with the right type parameters, generic code can be written to work on all
 * types.
 * TODO update and delete need to accommodate both with-etag and don't-care
 */
#[async_trait]
pub trait ApiBackend: Send + Sync {
    async fn project_create(
        &self,
        params: &ApiProjectCreateParams,
    ) -> CreateResult<ApiProject>;
    async fn project_lookup(&self, name: &ApiName) -> LookupResult<ApiProject>;
    async fn project_delete(&self, name: &ApiName) -> DeleteResult;
    async fn project_update(
        &self,
        name: &ApiName,
        params: &ApiProjectUpdateParams,
    ) -> UpdateResult<ApiProject>;
    async fn projects_list(
        &self,
        pagparams: &PaginationParams<ApiName>,
    ) -> ListResult<ApiProject>;
    async fn project_list_instances(
        &self,
        name: &ApiName,
        pagparams: &PaginationParams<ApiName>,
    ) -> ListResult<ApiInstance>;
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
