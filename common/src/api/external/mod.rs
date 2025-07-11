// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Data structures and related facilities for representing resources in the API
//!
//! This includes all representations over the wire for both the external and
//! internal APIs.  The contents here are all HTTP-agnostic.

mod error;
pub mod http_pagination;
pub use crate::api::internal::shared::AllowedSourceIps;
pub use crate::api::internal::shared::SwitchLocation;
use crate::update::ArtifactId;
use anyhow::Context;
use api_identity::ObjectIdentity;
use chrono::DateTime;
use chrono::Utc;
use daft::Diffable;
use dropshot::HttpError;
pub use dropshot::PaginationOrder;
pub use error::*;
use futures::stream::BoxStream;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use oxnet::IpNet;
use oxnet::Ipv4Net;
use parse_display::Display;
use parse_display::FromStr;
use rand::Rng;
use rand::thread_rng;
use schemars::JsonSchema;
use semver::Version;
use serde::Deserialize;
use serde::Serialize;
use serde_with::{DeserializeFromStr, SerializeDisplay};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FormatResult;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::num::ParseIntError;
use std::num::{NonZeroU16, NonZeroU32};
use std::ops::Deref;
use std::ops::RangeInclusive;
use std::str::FromStr;
use tufaceous_artifact::ArtifactHash;
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

/// Exists for types that don't properly implement `ObjectIdentity` but
/// still need to be paginated by id.
pub trait SimpleIdentity {
    fn id(&self) -> Uuid;
}

impl<T: ObjectIdentity> SimpleIdentity for T {
    fn id(&self) -> Uuid {
        self.identity().id
    }
}

/// Exists for types that don't properly implement `ObjectIdentity` but
/// still need to be paginated by name or id.
pub trait SimpleIdentityOrName {
    fn id(&self) -> Uuid;
    fn name(&self) -> &Name;
}

impl<T: ObjectIdentity> SimpleIdentityOrName for T {
    fn id(&self) -> Uuid {
        self.identity().id
    }

    fn name(&self) -> &Name {
        &self.identity().name
    }
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
#[derive(Clone, Debug)]
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
    pub fn max_page() -> Self {
        Self {
            marker: None,
            direction: dropshot::PaginationOrder::Ascending,
            limit: NonZeroU32::new(u32::MAX).unwrap(),
        }
    }
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
#[derive(Diffable)]
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

impl From<Name> for String {
    fn from(name: Name) -> Self {
        name.0
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
        name_schema(schemars::schema::Metadata {
            title: Some(
                "A name unique within the parent collection".to_string(),
            ),
            description: Some(
                "Names must begin with a lower case ASCII letter, be \
                 composed exclusively of lowercase ASCII, uppercase \
                 ASCII, numbers, and '-', and may not end with a '-'. \
                 Names cannot be a UUID, but they may contain a UUID. \
                 They can be at most 63 characters long."
                    .to_string(),
            ),
            ..Default::default()
        })
    }
}

impl Name {
    /// Parse an `Name`.  This is a convenience wrapper around
    /// `Name::try_from(String)` that marshals any error into an appropriate
    /// `Error`.
    pub fn from_param(value: String, label: &str) -> Result<Name, Error> {
        value.parse().map_err(|e| Error::invalid_value(label, e))
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

impl FromStr for NameOrId {
    // TODO: We should have better error types here.
    // See https://github.com/oxidecomputer/omicron/issues/347
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        NameOrId::try_from(String::from(value))
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

/// A username for a local-only user.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
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
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        name_schema(schemars::schema::Metadata {
            title: Some("A username for a local-only user".to_string()),
            description: Some(
                "Usernames must begin with a lower case ASCII letter, be \
                 composed exclusively of lowercase ASCII, uppercase ASCII, \
                 numbers, and '-', and may not end with a '-'. Usernames \
                 cannot be a UUID, but they may contain a UUID. They can be at \
                 most 63 characters long."
                    .to_string(),
            ),
            ..Default::default()
        })
    }
}

fn name_schema(
    metadata: schemars::schema::Metadata,
) -> schemars::schema::Schema {
    schemars::schema::SchemaObject {
        metadata: Some(Box::new(metadata)),
        instance_type: Some(schemars::schema::InstanceType::String.into()),
        string: Some(Box::new(schemars::schema::StringValidation {
            max_length: Some(63),
            min_length: Some(1),
            pattern: Some(
                concat!(
                    r#"^"#,
                    // Cannot match a UUID
                    concat!(
                        r#"(?![0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"#,
                        r#"-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$)"#,
                    ),
                    r#"^[a-z]([a-zA-Z0-9-]*[a-zA-Z0-9]+)?"#,
                    r#"$"#,
                )
                .to_string(),
            ),
        })),
        ..Default::default()
    }
    .into()
}

/// Byte count to express memory or storage capacity.
//
// The maximum supported byte count is [`i64::MAX`].  This makes it somewhat
// inconvenient to define constructors: a u32 constructor can be infallible,
// but an i64 constructor can fail (if the value is negative) and a u64
// constructor can fail (if the value is larger than i64::MAX).  We provide
// all of these for consumers' convenience.
//
// The maximum byte count of i64::MAX comes from the fact that this is stored
// in the database as an i64.  Constraining it here ensures that we can't fail
// to serialize the value.
//
// TODO: custom JsonSchema impl to describe i64::MAX limit; this is blocked by
// https://github.com/oxidecomputer/typify/issues/589
#[derive(
    Copy,
    Clone,
    Debug,
    Serialize,
    JsonSchema,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Diffable,
)]
pub struct ByteCount(u64);

impl<'de> Deserialize<'de> for ByteCount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = u64::deserialize(deserializer)?;
        ByteCount::try_from(bytes).map_err(serde::de::Error::custom)
    }
}

#[allow(non_upper_case_globals)]
const KiB: u64 = 1024;
#[allow(non_upper_case_globals)]
const MiB: u64 = KiB * 1024;
#[allow(non_upper_case_globals)]
const GiB: u64 = MiB * 1024;
#[allow(non_upper_case_globals)]
const TiB: u64 = GiB * 1024;

impl ByteCount {
    // None of these three constructors can create a value larger than
    // `i64::MAX`. (Note that a `from_tebibytes_u32` could overflow u64.)
    pub const fn from_kibibytes_u32(kibibytes: u32) -> ByteCount {
        ByteCount(KiB * kibibytes as u64)
    }
    pub const fn from_mebibytes_u32(mebibytes: u32) -> ByteCount {
        ByteCount(MiB * mebibytes as u64)
    }
    pub const fn from_gibibytes_u32(gibibytes: u32) -> ByteCount {
        ByteCount(GiB * gibibytes as u64)
    }

    pub const fn to_bytes(&self) -> u64 {
        self.0
    }
    pub const fn to_whole_kibibytes(&self) -> u64 {
        self.to_bytes() / KiB
    }
    pub const fn to_whole_mebibytes(&self) -> u64 {
        self.to_bytes() / MiB
    }
    pub const fn to_whole_gibibytes(&self) -> u64 {
        self.to_bytes() / GiB
    }
    pub const fn to_whole_tebibytes(&self) -> u64 {
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
//
// A generation is a value between 0 and 2**63-1, i.e. equivalent to a u63.
// The reason is that we store it as an i64 in the database, and we want to
// disallow negative values. (We could potentially use two's complement to
// store values greater than that as negative values, but surely 2**63 is
// enough.)
#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    Hash,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Diffable,
)]
#[daft(leaf)]
pub struct Generation(u64);

impl Generation {
    // `as` is a little distasteful because it allows lossy conversion, but we
    // know converting `i64::MAX` to `u64` will always succeed losslessly.
    const MAX: Generation = Generation(i64::MAX as u64);

    pub const fn new() -> Generation {
        Generation(1)
    }

    pub const fn from_u32(value: u32) -> Generation {
        // `as` is a little distasteful because it allows lossy conversion, but
        // (a) we know converting `u32` to `u64` will always succeed
        // losslessly, and (b) it allows to make this function `const`, unlike
        // if we were to use `u64::from(value)`.
        Generation(value as u64)
    }

    pub const fn next(&self) -> Generation {
        // It should technically be an operational error if this wraps or even
        // exceeds the value allowed by an i64.  But it seems unlikely enough to
        // happen in practice that we can probably feel safe with this.
        let next_gen = self.0 + 1;
        assert!(
            next_gen <= Generation::MAX.0,
            "attempt to overflow generation number"
        );
        Generation(next_gen)
    }

    pub const fn prev(&self) -> Option<Generation> {
        if self.0 > 1 { Some(Generation(self.0 - 1)) } else { None }
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for Generation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Generation::try_from(value).map_err(|GenerationOverflowError(_)| {
            serde::de::Error::invalid_value(
                serde::de::Unexpected::Unsigned(value),
                &"an integer between 0 and 9223372036854775807",
            )
        })
    }
}

// This is the equivalent of applying `#[serde(transparent)]`, but that has a
// side effect of changing the JsonSchema derive to no longer emit a schema.
impl Serialize for Generation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
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
        i64::try_from(g.0).unwrap()
    }
}

impl From<Generation> for u64 {
    fn from(g: Generation) -> Self {
        g.0
    }
}

impl From<u32> for Generation {
    fn from(value: u32) -> Self {
        Generation(u64::from(value))
    }
}

impl TryFrom<i64> for Generation {
    type Error = GenerationNegativeError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Ok(Generation(
            u64::try_from(value).map_err(|_| GenerationNegativeError(()))?,
        ))
    }
}

impl TryFrom<u64> for Generation {
    type Error = GenerationOverflowError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        i64::try_from(value).map_err(|_| GenerationOverflowError(()))?;
        Ok(Generation(value))
    }
}

impl FromStr for Generation {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Try to parse `s` as both an i64 and u64, returning the error from
        // either.
        let _ = i64::from_str(s)?;
        Ok(Generation(u64::from_str(s)?))
    }
}

impl slog::Value for Generation {
    fn serialize(
        &self,
        _rec: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_u64(key, self.0)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("negative generation number")]
pub struct GenerationNegativeError(());

#[derive(Debug, thiserror::Error)]
#[error("generation number too large")]
pub struct GenerationOverflowError(());

/// An RFC-1035-compliant hostname.
#[derive(
    Clone, Debug, Deserialize, Display, Eq, PartialEq, SerializeDisplay,
)]
#[display("{0}")]
#[serde(try_from = "String", into = "String")]
pub struct Hostname(String);

impl Hostname {
    /// Return the hostname as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

// Regular expression for hostnames.
//
// Each name is a dot-separated sequence of labels. Each label is supposed to
// be an "LDH": letter, dash, or hyphen. Hostnames can consist of one label, or
// many, separated by a `.`. While _domain_ names are allowed to end in a `.`,
// making them fully-qualified, hostnames are not.
//
// Note that labels are allowed to contain a hyphen, but may not start or end
// with one. See RFC 952, "Lexical grammar" section.
//
// Note that we need to use a regex engine capable of lookbehind to support
// this, since we need to check that labels don't end with a `-`.
const HOSTNAME_REGEX: &str = r#"^([a-zA-Z0-9]+[a-zA-Z0-9\-]*(?<!-))(\.[a-zA-Z0-9]+[a-zA-Z0-9\-]*(?<!-))*$"#;

// Labels need to be encoded on the wire, and prefixed with a signel length
// octet. They also need to end with a length octet of 0 when encoded. So the
// longest name is a single label of 253 characters, which will be encoded as
// `\xfd<the label>\x00`.
const HOSTNAME_MAX_LEN: u32 = 253;

impl FromStr for Hostname {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        anyhow::ensure!(
            s.len() <= HOSTNAME_MAX_LEN as usize,
            "Max hostname length is {HOSTNAME_MAX_LEN}"
        );
        let re = regress::Regex::new(HOSTNAME_REGEX).unwrap();
        if re.find(s).is_some() {
            Ok(Hostname(s.to_string()))
        } else {
            anyhow::bail!("Hostnames must comply with RFC 1035")
        }
    }
}

impl TryFrom<&str> for Hostname {
    type Error = <Hostname as FromStr>::Err;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl TryFrom<String> for Hostname {
    type Error = <Hostname as FromStr>::Err;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.as_str().parse()
    }
}

// Custom implementation of JsonSchema for Hostname to ensure RFC-1035-style
// validation
impl JsonSchema for Hostname {
    fn schema_name() -> String {
        "Hostname".to_string()
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some("An RFC-1035-compliant hostname".to_string()),
                description: Some(
                    "A hostname identifies a host on a network, and \
                    is usually a dot-delimited sequence of labels, \
                    where each label contains only letters, digits, \
                    or the hyphen. See RFCs 1035 and 952 for more details."
                        .to_string(),
                ),
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::SingleOrVec::Single(
                Box::new(schemars::schema::InstanceType::String),
            )),
            string: Some(Box::new(schemars::schema::StringValidation {
                max_length: Some(HOSTNAME_MAX_LEN),
                min_length: Some(1),
                pattern: Some(HOSTNAME_REGEX.to_string()),
            })),
            ..Default::default()
        })
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
    AddressLot,
    AddressLotBlock,
    AffinityGroup,
    AffinityGroupMember,
    AntiAffinityGroup,
    AntiAffinityGroupMember,
    Alert,
    AlertReceiver,
    AllowList,
    BackgroundTask,
    BgpConfig,
    BgpAnnounceSet,
    Blueprint,
    Fleet,
    Silo,
    SiloUser,
    SiloGroup,
    SiloQuotas,
    IdentityProvider,
    SamlIdentityProvider,
    SshKey,
    Certificate,
    ConsoleSession,
    DeviceAuthRequest,
    DeviceAccessToken,
    Project,
    Dataset,
    Disk,
    Image,
    SiloImage,
    ProjectImage,
    Instance,
    LoopbackAddress,
    SiloAuthSettings,
    SwitchPortSettings,
    SupportBundle,
    IpPool,
    IpPoolResource,
    InstanceNetworkInterface,
    InternetGateway,
    InternetGatewayIpPool,
    InternetGatewayIpAddress,
    PhysicalDisk,
    Rack,
    Service,
    ServiceNetworkInterface,
    Sled,
    SledInstance,
    SledLedger,
    Switch,
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
    TufRepo,
    TufArtifact,
    TufTrustRoot,
    SwitchPort,
    UserBuiltin,
    Zpool,
    Vmm,
    Ipv4NatEntry,
    FloatingIp,
    Probe,
    ProbeNetworkInterface,
    LldpLinkConfig,
    WebhookSecret,
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
    Hash,
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

impl From<crate::api::internal::nexus::VmmState> for InstanceState {
    fn from(state: crate::api::internal::nexus::VmmState) -> Self {
        use crate::api::internal::nexus::VmmState as InternalVmmState;
        match state {
            InternalVmmState::Starting => Self::Starting,
            InternalVmmState::Running => Self::Running,
            InternalVmmState::Stopping => Self::Stopping,
            InternalVmmState::Stopped => Self::Stopped,
            InternalVmmState::Rebooting => Self::Rebooting,
            InternalVmmState::Migrating => Self::Migrating,
            InternalVmmState::Failed => Self::Failed,
            InternalVmmState::Destroyed => Self::Destroyed,
        }
    }
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
}

/// The number of CPUs in an Instance
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
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

/// The state of an `Instance`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceRuntimeState {
    pub run_state: InstanceState,
    pub time_run_state_updated: DateTime<Utc>,
    /// The timestamp of the most recent time this instance was automatically
    /// restarted by the control plane.
    ///
    /// If this is not present, then this instance has not been automatically
    /// restarted.
    pub time_last_auto_restarted: Option<DateTime<Utc>>,
}

/// View of an Instance
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
    pub hostname: String,

    /// the ID of the disk used to boot this Instance, if a specific one is assigned.
    pub boot_disk_id: Option<Uuid>,

    #[serde(flatten)]
    pub runtime: InstanceRuntimeState,

    #[serde(flatten)]
    pub auto_restart_status: InstanceAutoRestartStatus,
}

/// Status of control-plane driven automatic failure recovery for this instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceAutoRestartStatus {
    /// `true` if this instance's auto-restart policy will permit the control
    /// plane to automatically restart it if it enters the `Failed` state.
    //
    // Rename this field, as the struct is `#[serde(flatten)]`ed into the
    // `Instance` type, and we would like the field to be prefixed with
    // `auto_restart`.
    #[serde(rename = "auto_restart_enabled")]
    pub enabled: bool,

    /// The auto-restart policy configured for this instance, or `null` if no
    /// explicit policy has been configured.
    ///
    /// This policy determines whether the instance should be automatically
    /// restarted by the control plane on failure. If this is `null`, the
    /// control plane will use the default policy when determining whether or
    /// not to automatically restart this instance, which may or may not allow
    /// it to be restarted. The value of the `auto_restart_enabled` field
    /// indicates whether the instance will be auto-restarted, based on its
    /// current policy or the default if it has no configured policy.
    //
    // Rename this field, as the struct is `#[serde(flatten)]`ed into the
    // `Instance` type, and we would like the field to be prefixed with
    // `auto_restart`.
    #[serde(rename = "auto_restart_policy")]
    pub policy: Option<InstanceAutoRestartPolicy>,

    /// The time at which the auto-restart cooldown period for this instance
    /// completes, permitting it to be automatically restarted again. If the
    /// instance enters the `Failed` state, it will not be restarted until after
    /// this time.
    ///
    /// If this is not present, then either the instance has never been
    /// automatically restarted, or the cooldown period has already expired,
    /// allowing the instance to be restarted immediately if it fails.
    //
    // Rename this field, as the struct is `#[serde(flatten)]`ed into the
    // `Instance` type, and we would like the field to be prefixed with
    // `auto_restart`.
    #[serde(rename = "auto_restart_cooldown_expiration")]
    pub cooldown_expiration: Option<DateTime<Utc>>,
}

/// A policy determining when an instance should be automatically restarted by
/// the control plane.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Eq, PartialEq,
)]
#[serde(rename_all = "snake_case")]
pub enum InstanceAutoRestartPolicy {
    /// The instance should not be automatically restarted by the control plane
    /// if it fails.
    Never,
    /// If this instance is running and unexpectedly fails (e.g. due to a host
    /// software crash or unexpected host reboot), the control plane will make a
    /// best-effort attempt to restart it. The control plane may choose not to
    /// restart the instance to preserve the overall availability of the system.
    BestEffort,
}

// AFFINITY GROUPS

/// Affinity policy used to describe "what to do when a request cannot be satisfied"
///
/// Used for both Affinity and Anti-Affinity Groups
#[derive(
    Clone, Copy, Debug, Deserialize, Hash, Eq, Serialize, PartialEq, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AffinityPolicy {
    /// If the affinity request cannot be satisfied, allow it anyway.
    ///
    /// This enables a "best-effort" attempt to satisfy the affinity policy.
    Allow,

    /// If the affinity request cannot be satisfied, fail explicitly.
    Fail,
}

/// Describes the scope of affinity for the purposes of co-location.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureDomain {
    /// Instances are considered co-located if they are on the same sled
    Sled,
}

/// A member of an Affinity Group
///
/// Membership in a group is not exclusive - members may belong to multiple
/// affinity / anti-affinity groups.
///
/// Affinity Groups can contain up to 32 members.
// See: AFFINITY_GROUP_MAX_MEMBERS
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum AffinityGroupMember {
    /// An instance belonging to this group
    ///
    /// Instances can belong to up to 16 affinity groups.
    // See: INSTANCE_MAX_AFFINITY_GROUPS
    Instance { id: InstanceUuid, name: Name, run_state: InstanceState },
}

impl SimpleIdentityOrName for AffinityGroupMember {
    fn id(&self) -> Uuid {
        match self {
            AffinityGroupMember::Instance { id, .. } => *id.as_untyped_uuid(),
        }
    }

    fn name(&self) -> &Name {
        match self {
            AffinityGroupMember::Instance { name, .. } => name,
        }
    }
}

/// A member of an Anti-Affinity Group
///
/// Membership in a group is not exclusive - members may belong to multiple
/// affinity / anti-affinity groups.
///
/// Anti-Affinity Groups can contain up to 32 members.
// See: ANTI_AFFINITY_GROUP_MAX_MEMBERS
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum AntiAffinityGroupMember {
    /// An instance belonging to this group
    ///
    /// Instances can belong to up to 16 anti-affinity groups.
    // See: INSTANCE_MAX_ANTI_AFFINITY_GROUPS
    Instance { id: InstanceUuid, name: Name, run_state: InstanceState },
}

impl SimpleIdentityOrName for AntiAffinityGroupMember {
    fn id(&self) -> Uuid {
        match self {
            AntiAffinityGroupMember::Instance { id, .. } => {
                *id.as_untyped_uuid()
            }
        }
    }

    fn name(&self) -> &Name {
        match self {
            AntiAffinityGroupMember::Instance { name, .. } => name,
        }
    }
}

// DISKS

/// View of a Disk
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Disk {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub project_id: Uuid,
    /// ID of snapshot from which disk was created, if any
    pub snapshot_id: Option<Uuid>,
    /// ID of image from which disk was created, if any
    pub image_id: Option<Uuid>,
    pub size: ByteCount,
    pub block_size: ByteCount,
    pub state: DiskState,
    pub device_path: String,
}

/// State of a Disk
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

pub trait Ipv6NetExt {
    /// The length for all VPC IPv6 prefixes
    const VPC_IPV6_PREFIX_LENGTH: u8 = 48;

    /// The prefix length for all VPC Subnets
    const VPC_SUBNET_IPV6_PREFIX_LENGTH: u8 = 64;

    /// Return `true` if this subnetwork is a valid VPC prefix.
    ///
    /// This checks that the subnet is a unique local address, and has the VPC
    /// prefix length required.
    fn is_vpc_prefix(&self) -> bool;

    /// Return `true` if this subnetwork is a valid VPC Subnet, given the VPC's
    /// prefix.
    fn is_vpc_subnet(&self, vpc_prefix: &Self) -> bool;
}

impl Ipv6NetExt for oxnet::Ipv6Net {
    fn is_vpc_prefix(&self) -> bool {
        self.is_unique_local() && self.width() == Self::VPC_IPV6_PREFIX_LENGTH
    }

    fn is_vpc_subnet(&self, vpc_prefix: &Self) -> bool {
        self.is_unique_local()
            && self.is_subnet_of(vpc_prefix)
            && self.width() == Self::VPC_SUBNET_IPV6_PREFIX_LENGTH
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
    #[display("drop")]
    /// Drop matching traffic
    Drop,
}

/// A `RouteDestination` is used to match traffic with a routing rule based on
/// the destination of that traffic.
///
/// When traffic is to be sent to a destination that is within a given
/// `RouteDestination`, the corresponding `RouterRoute` applies, and traffic
/// will be forward to the `RouteTarget` for that rule.
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
    /// Route applies to traffic destined for the specified IP address
    Ip(IpAddr),
    /// Route applies to traffic destined for the specified IP subnet
    IpNet(IpNet),
    /// Route applies to traffic destined for the specified VPC
    Vpc(Name),
    /// Route applies to traffic destined for the specified VPC subnet
    Subnet(Name),
}

/// The kind of a `RouterRoute`
///
/// The kind determines certain attributes such as if the route is modifiable
/// and describes how or where the route was created.
//
// See [RFD-21](https://rfd.shared.oxide.computer/rfd/0021#concept-router) for more context
#[derive(
    Clone, Copy, Debug, PartialEq, Deserialize, Serialize, Display, JsonSchema,
)]
#[display("{}")]
#[serde(rename_all = "snake_case")]
pub enum RouterRouteKind {
    /// Determines the default destination of traffic, such as whether it goes
    /// to the internet or not.
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
    /// Created by a user; see `RouteTarget`
    ///
    /// `Destination: User defined`
    /// `Modifiable: true`
    Custom,
}

/// A route defines a rule that governs where traffic should be sent based on
/// its destination.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct RouterRoute {
    /// Common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The ID of the VPC Router to which the route belongs
    pub vpc_router_id: Uuid,
    /// Describes the kind of router. Set at creation. `read-only`
    pub kind: RouterRouteKind,
    /// The location that matched packets should be forwarded to
    pub target: RouteTarget,
    /// Selects which traffic this routing rule will apply to
    pub destination: RouteDestination,
}

#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InternetGatewayIpPool {
    /// Common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The ID of the internet gateway to which the IP pool entry belongs
    pub internet_gateway_id: Uuid,
    /// The ID of the referenced IP pool
    pub ip_pool_id: Uuid,
}

#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InternetGatewayIp {
    /// Common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// The ID of the internet gateway to which the IP belongs
    pub internet_gateway_id: Uuid,
    /// The IP address
    pub address: IpAddr,
}

/// A single rule in a VPC firewall
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRule {
    /// Common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// Whether this rule is in effect
    pub status: VpcFirewallRuleStatus,
    /// Whether this rule is for incoming or outgoing traffic
    pub direction: VpcFirewallRuleDirection,
    /// Determine the set of instances that the rule applies to
    pub targets: Vec<VpcFirewallRuleTarget>,
    /// Reductions on the scope of the rule
    pub filters: VpcFirewallRuleFilter,
    /// Whether traffic matching the rule should be allowed or dropped
    pub action: VpcFirewallRuleAction,
    /// The relative priority of this rule
    pub priority: VpcFirewallRulePriority,
    /// The VPC to which this rule belongs
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
    /// Name of the rule, unique to this VPC
    pub name: Name,
    /// Human-readable free-form text about a resource
    pub description: String,
    /// Whether this rule is in effect
    pub status: VpcFirewallRuleStatus,
    /// Whether this rule is for incoming or outgoing traffic
    pub direction: VpcFirewallRuleDirection,
    /// Determine the set of instances that the rule applies to
    #[schemars(length(max = 256))]
    pub targets: Vec<VpcFirewallRuleTarget>,
    /// Reductions on the scope of the rule
    pub filters: VpcFirewallRuleFilter,
    /// Whether traffic matching the rule should be allowed or dropped
    pub action: VpcFirewallRuleAction,
    /// The relative priority of this rule
    pub priority: VpcFirewallRulePriority,
}

/// Updated list of firewall rules. Will replace all existing rules.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRuleUpdateParams {
    #[schemars(length(max = 1024))]
    #[serde(default)]
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

/// Filters reduce the scope of a firewall rule. Without filters, the rule
/// applies to all packets to the targets (or from the targets, if it's an
/// outbound rule). With multiple filters, the rule applies only to packets
/// matching ALL filters. The maximum number of each type of filter is 256.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRuleFilter {
    /// If present, host filters match the "other end" of traffic from the
    /// target’s perspective: for an inbound rule, they match the source of
    /// traffic. For an outbound rule, they match the destination.
    #[schemars(length(max = 256))]
    pub hosts: Option<Vec<VpcFirewallRuleHostFilter>>,

    /// If present, the networking protocols this rule applies to.
    #[schemars(length(max = 256))]
    pub protocols: Option<Vec<VpcFirewallRuleProtocol>>,

    /// If present, the destination ports or port ranges this rule applies to.
    #[schemars(length(max = 256))]
    pub ports: Option<Vec<L4PortRange>>,
}

/// The protocols that may be specified in a firewall rule's filter
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
pub enum VpcFirewallRuleProtocol {
    Tcp,
    Udp,
    Icmp(Option<VpcFirewallIcmpFilter>),
    // TODO: IPv6 not supported by instances.
    // Icmpv6(Option<VpcFirewallIcmpFilter>),
    // TODO: OPTE does not yet permit further L4 protocols. (opte#609)
    // Other(u16),
}

impl FromStr for VpcFirewallRuleProtocol {
    type Err = Error;

    fn from_str(proto: &str) -> Result<Self, Self::Err> {
        let (ty_str, content_str) = match proto.split_once(':') {
            None => (proto, None),
            Some((lhs, rhs)) => (lhs, Some(rhs)),
        };

        match (ty_str, content_str) {
            (lhs, None) if lhs.eq_ignore_ascii_case("tcp") => Ok(Self::Tcp),
            (lhs, None) if lhs.eq_ignore_ascii_case("udp") => Ok(Self::Udp),
            (lhs, None) if lhs.eq_ignore_ascii_case("icmp") => {
                Ok(Self::Icmp(None))
            }
            (lhs, Some(rhs)) if lhs.eq_ignore_ascii_case("icmp") => {
                Ok(Self::Icmp(Some(rhs.parse()?)))
            }
            (lhs, None) => Err(Error::invalid_value(
                "vpc_firewall_rule_protocol",
                format!("unrecognized protocol: {lhs}"),
            )),
            (lhs, Some(rhs)) => Err(Error::invalid_value(
                "vpc_firewall_rule_protocol",
                format!(
                    "cannot specify extra filters ({rhs}) for protocol \"{lhs}\""
                ),
            )),
        }
    }
}

impl TryFrom<String> for VpcFirewallRuleProtocol {
    type Error = <VpcFirewallRuleProtocol as FromStr>::Err;

    fn try_from(proto: String) -> Result<Self, Self::Error> {
        proto.parse()
    }
}

impl Display for VpcFirewallRuleProtocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        match self {
            VpcFirewallRuleProtocol::Tcp => write!(f, "tcp"),
            VpcFirewallRuleProtocol::Udp => write!(f, "udp"),
            VpcFirewallRuleProtocol::Icmp(None) => write!(f, "icmp"),
            VpcFirewallRuleProtocol::Icmp(Some(v)) => write!(f, "icmp:{v}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallIcmpFilter {
    pub icmp_type: u8,
    pub code: Option<IcmpParamRange>,
}

impl Display for VpcFirewallIcmpFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatResult {
        write!(f, "{}", self.icmp_type)?;
        if let Some(code) = self.code {
            write!(f, ",{code}")?;
        }
        Ok(())
    }
}

impl FromStr for VpcFirewallIcmpFilter {
    type Err = Error;

    fn from_str(filter: &str) -> Result<Self, Self::Err> {
        let (ty_str, code_str) = match filter.split_once(',') {
            None => (filter, None),
            Some((lhs, rhs)) => (lhs, Some(rhs)),
        };

        Ok(Self {
            icmp_type: ty_str.parse::<u8>().map_err(|e| {
                Error::invalid_value(
                    "icmp_type",
                    format!("{ty_str:?} unparsable for type: {e}"),
                )
            })?,
            code: code_str
                .map(|v| {
                    v.parse::<IcmpParamRange>().map_err(|e| {
                        Error::invalid_value("code", e.to_string())
                    })
                })
                .transpose()?,
        })
    }
}

impl From<u8> for IcmpParamRange {
    fn from(value: u8) -> Self {
        Self { first: value, last: value }
    }
}

impl TryFrom<RangeInclusive<u8>> for IcmpParamRange {
    type Error = IcmpParamRangeError;

    fn try_from(value: RangeInclusive<u8>) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Err(IcmpParamRangeError::EmptyRange)
        } else {
            let (first, last) = value.into_inner();
            Ok(Self { first, last })
        }
    }
}

impl From<IcmpParamRange> for RangeInclusive<u8> {
    fn from(value: IcmpParamRange) -> Self {
        value.first..=value.last
    }
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

/// A `VpcFirewallRuleTarget` is used to specify the set of instances to which
/// a firewall rule applies. You can target instances directly by name, or
/// specify a VPC, VPC subnet, IP, or IP subnet, which will apply the rule to
/// traffic going to all matching instances. Targets are additive: the rule
/// applies to instances matching ANY target.
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
    IpNet(oxnet::IpNet),
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
    IpNet(oxnet::IpNet),
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
    type Err = L4PortRangeError;
    fn from_str(range: &str) -> Result<Self, Self::Err> {
        match range.split_once('-') {
            None => {
                let port = range
                    .parse::<NonZeroU16>()
                    .map_err(|e| L4PortRangeError::Value(range.into(), e))?
                    .into();
                Ok(L4PortRange { first: port, last: port })
            }
            Some(("", _)) => Err(L4PortRangeError::MissingStart),
            Some((_, "")) => Err(L4PortRangeError::MissingEnd),
            Some((left, right)) => {
                let first = left
                    .parse::<NonZeroU16>()
                    .map_err(|e| L4PortRangeError::Value(left.into(), e))?
                    .into();
                let last = right
                    .parse::<NonZeroU16>()
                    .map_err(|e| L4PortRangeError::Value(right.into(), e))?
                    .into();

                if first > last {
                    Err(L4PortRangeError::EmptyRange)
                } else {
                    Ok(L4PortRange { first, last })
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum L4PortRangeError {
    #[error("range has no start value")]
    MissingStart,
    #[error("range has no end value")]
    MissingEnd,
    #[error("range has larger start value than end value")]
    EmptyRange,
    #[error("{0:?} unparsable for type: {1}")]
    Value(String, ParseIntError),
}

impl From<L4PortRangeError> for Error {
    fn from(value: L4PortRangeError) -> Self {
        Error::invalid_value("l4_port_range", value.to_string())
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
                    may be omitted to represent a single port."
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

impl From<L4Port> for L4PortRange {
    fn from(value: L4Port) -> Self {
        Self { first: value, last: value }
    }
}

impl TryFrom<RangeInclusive<L4Port>> for L4PortRange {
    type Error = L4PortRangeError;

    fn try_from(value: RangeInclusive<L4Port>) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Err(L4PortRangeError::EmptyRange)
        } else {
            let (first, last) = value.into_inner();
            Ok(Self { first, last })
        }
    }
}

/// A range of ICMP(v6) types or codes. This range is inclusive on both ends.
#[derive(
    Clone, Copy, Debug, DeserializeFromStr, SerializeDisplay, PartialEq,
)]
pub struct IcmpParamRange {
    /// The first number in the range
    pub first: u8,
    /// The last number in the range
    pub last: u8,
}

impl FromStr for IcmpParamRange {
    type Err = IcmpParamRangeError;
    fn from_str(range: &str) -> Result<Self, Self::Err> {
        match range.split_once('-') {
            None => {
                let param = range
                    .parse::<u8>()
                    .map_err(|e| IcmpParamRangeError::Value(range.into(), e))?;
                Ok(IcmpParamRange { first: param, last: param })
            }
            Some(("", _)) => Err(IcmpParamRangeError::MissingStart),
            Some((_, "")) => Err(IcmpParamRangeError::MissingEnd),
            Some((left, right)) => {
                let first = left
                    .parse::<u8>()
                    .map_err(|e| IcmpParamRangeError::Value(left.into(), e))?;
                let last = right
                    .parse::<u8>()
                    .map_err(|e| IcmpParamRangeError::Value(right.into(), e))?;

                if first > last {
                    Err(IcmpParamRangeError::EmptyRange)
                } else {
                    Ok(IcmpParamRange { first, last })
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IcmpParamRangeError {
    #[error("range has no start value")]
    MissingStart,
    #[error("range has no end value")]
    MissingEnd,
    #[error("range has larger start value than end value")]
    EmptyRange,
    #[error("{0:?} unparsable for type: {1}")]
    Value(String, ParseIntError),
}

impl TryFrom<String> for IcmpParamRange {
    type Error = <IcmpParamRange as FromStr>::Err;

    fn try_from(range: String) -> Result<Self, Self::Error> {
        range.parse()
    }
}

impl Display for IcmpParamRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.first == self.last {
            write!(f, "{}", self.first)
        } else {
            write!(f, "{}-{}", self.first, self.last)
        }
    }
}

impl JsonSchema for IcmpParamRange {
    fn schema_name() -> String {
        "IcmpParamRange".to_string()
    }

    fn json_schema(
        _: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some("A range of ICMP(v6) types or codes".to_string()),
                description: Some(
                    "An inclusive-inclusive range of ICMP(v6) types or codes. \
                    The second value may be omitted to represent a single parameter."
                        .to_string(),
                ),
                examples: vec!["3".into(), "20-120".into()],
                ..Default::default()
            })),
            instance_type: Some(
                schemars::schema::InstanceType::String.into()
            ),
            string: Some(Box::new(schemars::schema::StringValidation {
                max_length: Some(7),  // 3 digits for each value and the dash
                min_length: Some(1),
                pattern: Some(
                    r#"^[0-9]{1,3}(-[0-9]{1,3})?$"#.to_string(),
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
    Clone,
    Copy,
    Debug,
    DeserializeFromStr,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    SerializeDisplay,
    Hash,
    Diffable,
)]
#[daft(leaf)]
pub struct MacAddr(pub macaddr::MacAddr6);

impl MacAddr {
    // Guest MAC addresses begin with the Oxide OUI A8:40:25. Further, guest
    // address are constrained to be in the virtual address range
    // A8:40:25:F_:__:__. Even further, the range F0:00:00 - FE:FF:FF is
    // reserved for customer-visible addresses (FF:00:00-FF:FF:FF is for
    // system MAC addresses). See RFD 174 for the discussion of the virtual
    // range, and
    // https://github.com/oxidecomputer/omicron/pull/955#discussion_r856432498
    // for an initial discussion of the customer/system address range split.
    // The system range is further split between FF:00:00-FF:7F:FF for
    // fixed addresses (e.g., the OPTE virtual gateway MAC) and
    // FF:80:00-FF:FF:FF for dynamically allocated addresses (e.g., service
    // vNICs).
    //
    // F0:00:00 - FF:FF:FF    Oxide Virtual Address Range
    //     F0:00:00 - FE:FF:FF    Guest Addresses
    //     FF:00:00 - FF:FF:FF    System Addresses
    //         FF:00:00 - FF:7F:FF    Reserved Addresses
    //         FF:80:00 - FF:FF:FF    Runtime allocatable
    pub const MIN_GUEST_ADDR: i64 = 0xA8_40_25_F0_00_00;
    pub const MAX_GUEST_ADDR: i64 = 0xA8_40_25_FE_FF_FF;
    pub const MIN_SYSTEM_ADDR: i64 = 0xA8_40_25_FF_00_00;
    pub const MAX_SYSTEM_RESV: i64 = 0xA8_40_25_FF_7F_FF;
    pub const MAX_SYSTEM_ADDR: i64 = 0xA8_40_25_FF_FF_FF;

    /// Generate a random MAC address for a guest network interface
    pub fn random_guest() -> Self {
        let value =
            thread_rng().gen_range(Self::MIN_GUEST_ADDR..=Self::MAX_GUEST_ADDR);
        Self::from_i64(value)
    }

    /// Generate a random MAC address in the system address range
    pub fn random_system() -> Self {
        let value = thread_rng()
            .gen_range((Self::MAX_SYSTEM_RESV + 1)..=Self::MAX_SYSTEM_ADDR);
        Self::from_i64(value)
    }

    /// Iterate the MAC addresses in the system address range
    /// (used as an allocator in contexts where collisions are not expected and
    /// determinism is useful, like in the test suite)
    pub fn iter_system() -> impl Iterator<Item = MacAddr> + Send {
        ((Self::MAX_SYSTEM_RESV + 1)..=Self::MAX_SYSTEM_ADDR)
            .map(Self::from_i64)
    }

    /// Is this a MAC in the Guest Addresses range
    pub fn is_guest(&self) -> bool {
        let value = self.to_i64();
        value >= Self::MIN_GUEST_ADDR && value <= Self::MAX_GUEST_ADDR
    }

    /// Is this a MAC in the System Addresses range
    pub fn is_system(&self) -> bool {
        let value = self.to_i64();
        value >= Self::MIN_SYSTEM_ADDR && value <= Self::MAX_SYSTEM_ADDR
    }

    /// Construct a MAC address from its i64 big-endian byte representation.
    // NOTE: This is the representation used in the database.
    pub fn from_i64(value: i64) -> Self {
        let bytes = value.to_be_bytes();
        Self(macaddr::MacAddr6::new(
            bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ))
    }

    /// Convert a MAC address to its i64 big-endian byte representation
    // NOTE: This is the representation used in the database.
    pub fn to_i64(self) -> i64 {
        let bytes = self.0.as_bytes();
        i64::from_be_bytes([
            0, 0, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
        ])
    }
}

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
    Diffable,
)]
pub struct Vni(u32);

impl Vni {
    /// Virtual Network Identifiers are constrained to be 24-bit values.
    pub const MAX_VNI: u32 = 0xFF_FFFF;

    /// The VNI for the builtin services VPC.
    pub const SERVICES_VNI: Self = Self(100);

    /// Oxide reserves a slice of initial VNIs for its own use.
    pub const MIN_GUEST_VNI: u32 = 1024;

    /// Create a new random VNI.
    pub fn random() -> Self {
        Self(rand::thread_rng().gen_range(Self::MIN_GUEST_VNI..=Self::MAX_VNI))
    }

    /// Create a new random VNI in the Oxide-reserved space.
    pub fn random_system() -> Self {
        Self(rand::thread_rng().gen_range(0..Self::MIN_GUEST_VNI))
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

/// An `InstanceNetworkInterface` represents a virtual network interface device
/// attached to an instance.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InstanceNetworkInterface {
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
    // TODO-correctness: We need to split this into an optional V4 and optional
    // V6 address, at least one of which must be specified.
    pub ip: IpAddr,
    /// True if this interface is the primary for the instance to which it's
    /// attached.
    pub primary: bool,

    /// A set of additional networks that this interface may send and
    /// receive traffic on.
    #[serde(default)]
    pub transit_ips: Vec<IpNet>,
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

/// An address lot and associated blocks resulting from creating an address lot.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AddressLotCreateResponse {
    /// The address lot that was created.
    pub lot: AddressLot,

    /// The address lot blocks that were created.
    pub blocks: Vec<AddressLotBlock>,
}

/// Represents an address lot object, containing the id of the lot that can be
/// used in other API calls.
// TODO Add kind attribute to AddressLot
// https://github.com/oxidecomputer/omicron/issues/3064
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq,
)]
pub struct AddressLot {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// Desired use of `AddressLot`
    pub kind: AddressLotKind,
}

/// The kind associated with an address lot.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AddressLotKind {
    /// Infrastructure address lots are used for network infrastructure like
    /// addresses assigned to rack switches.
    Infra,

    /// Pool address lots are used by IP pools.
    Pool,
}

/// An address lot block is a part of an address lot and contains a range of
/// addresses. The range is inclusive.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct AddressLotBlock {
    /// The id of the address lot block.
    pub id: Uuid,

    /// The first address of the block (inclusive).
    pub first_address: IpAddr,

    /// The last address of the block (inclusive).
    pub last_address: IpAddr,
}

/// A loopback address is an address that is assigned to a rack switch but is
/// not associated with any particular port.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct LoopbackAddress {
    /// The id of the loopback address.
    pub id: Uuid,

    /// The address lot block this address came from.
    pub address_lot_block_id: Uuid,

    /// The id of the rack where this loopback address is assigned.
    pub rack_id: Uuid,

    /// Switch location where this loopback address is assigned.
    pub switch_location: String,

    /// The loopback IP address and prefix length.
    pub address: oxnet::IpNet,
}

/// A switch port represents a physical external port on a rack switch.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPort {
    /// The id of the switch port.
    pub id: Uuid,

    /// The rack this switch port belongs to.
    pub rack_id: Uuid,

    /// The switch location of this switch port.
    pub switch_location: String,

    /// The name of this switch port.
    pub port_name: Name,

    /// The primary settings group of this switch port. Will be `None` until
    /// this switch port is configured.
    pub port_settings_id: Option<Uuid>,
}

/// A switch port settings identity whose id may be used to view additional
/// details.
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq,
)]
pub struct SwitchPortSettingsIdentity {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

/// This structure contains all port settings information in one place. It's a
/// convenience data structure for getting a complete view of a particular
/// port's settings.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortSettings {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// Switch port settings included from other switch port settings groups.
    pub groups: Vec<SwitchPortSettingsGroups>,

    /// Layer 1 physical port settings.
    pub port: SwitchPortConfig,

    /// Layer 2 link settings.
    pub links: Vec<SwitchPortLinkConfig>,

    /// Layer 3 interface settings.
    pub interfaces: Vec<SwitchInterfaceConfig>,

    /// Vlan interface settings.
    pub vlan_interfaces: Vec<SwitchVlanInterfaceConfig>,

    /// IP route settings.
    pub routes: Vec<SwitchPortRouteConfig>,

    /// BGP peer settings.
    pub bgp_peers: Vec<BgpPeer>,

    /// Layer 3 IP address settings.
    pub addresses: Vec<SwitchPortAddressView>,
}

/// This structure maps a port settings object to a port settings groups. Port
/// settings objects may inherit settings from groups. This mapping defines the
/// relationship between settings objects and the groups they reference.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortSettingsGroups {
    /// The id of a port settings object referencing a port settings group.
    pub port_settings_id: Uuid,

    /// The id of a port settings group being referenced by a port settings
    /// object.
    pub port_settings_group_id: Uuid,
}

/// A port settings group is a named object that references a port settings
/// object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortSettingsGroup {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The port settings that comprise this group.
    pub port_settings_id: Uuid,
}

/// The link geometry associated with a switch port.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SwitchPortGeometry {
    /// The port contains a single QSFP28 link with four lanes.
    Qsfp28x1,

    /// The port contains two QSFP28 links each with two lanes.
    Qsfp28x2,

    /// The port contains four SFP28 links each with one lane.
    Sfp28x4,
}

/// A physical port configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortConfig {
    /// The id of the port settings object this configuration belongs to.
    pub port_settings_id: Uuid,

    /// The physical link geometry of the port.
    pub geometry: SwitchPortGeometry,
}

/// The speed of a link.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LinkSpeed {
    /// Zero gigabits per second.
    Speed0G,
    /// 1 gigabit per second.
    Speed1G,
    /// 10 gigabits per second.
    Speed10G,
    /// 25 gigabits per second.
    Speed25G,
    /// 40 gigabits per second.
    Speed40G,
    /// 50 gigabits per second.
    Speed50G,
    /// 100 gigabits per second.
    Speed100G,
    /// 200 gigabits per second.
    Speed200G,
    /// 400 gigabits per second.
    Speed400G,
}

/// The forward error correction mode of a link.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LinkFec {
    /// Firecode forward error correction.
    Firecode,
    /// No forward error correction.
    None,
    /// Reed-Solomon forward error correction.
    Rs,
}

impl From<crate::api::internal::shared::PortFec> for LinkFec {
    fn from(x: crate::api::internal::shared::PortFec) -> LinkFec {
        match x {
            crate::api::internal::shared::PortFec::Firecode => Self::Firecode,
            crate::api::internal::shared::PortFec::None => Self::None,
            crate::api::internal::shared::PortFec::Rs => Self::Rs,
        }
    }
}

impl From<crate::api::internal::shared::PortSpeed> for LinkSpeed {
    fn from(x: crate::api::internal::shared::PortSpeed) -> Self {
        match x {
            crate::api::internal::shared::PortSpeed::Speed0G => Self::Speed0G,
            crate::api::internal::shared::PortSpeed::Speed1G => Self::Speed1G,
            crate::api::internal::shared::PortSpeed::Speed10G => Self::Speed10G,
            crate::api::internal::shared::PortSpeed::Speed25G => Self::Speed25G,
            crate::api::internal::shared::PortSpeed::Speed40G => Self::Speed40G,
            crate::api::internal::shared::PortSpeed::Speed50G => Self::Speed50G,
            crate::api::internal::shared::PortSpeed::Speed100G => {
                Self::Speed100G
            }
            crate::api::internal::shared::PortSpeed::Speed200G => {
                Self::Speed200G
            }
            crate::api::internal::shared::PortSpeed::Speed400G => {
                Self::Speed400G
            }
        }
    }
}

/// A link configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortLinkConfig {
    /// The port settings this link configuration belongs to.
    pub port_settings_id: Uuid,

    /// The name of this link.
    pub link_name: Name,

    /// The maximum transmission unit for this link.
    pub mtu: u16,

    /// The requested forward-error correction method.  If this is not
    /// specified, the standard FEC for the underlying media will be applied
    /// if it can be determined.
    pub fec: Option<LinkFec>,

    /// The configured speed of the link.
    pub speed: LinkSpeed,

    /// Whether or not the link has autonegotiation enabled.
    pub autoneg: bool,

    /// The link-layer discovery protocol service configuration for this
    /// link.
    pub lldp_link_config: Option<LldpLinkConfig>,

    /// The tx_eq configuration for this link.
    pub tx_eq_config: Option<TxEqConfig>,
}

/// A link layer discovery protocol (LLDP) service configuration.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct LldpLinkConfig {
    /// The id of this LLDP service instance.
    pub id: Uuid,

    /// Whether or not the LLDP service is enabled.
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

/// Information about LLDP advertisements from other network entities directly
/// connected to a switch port.  This structure contains both metadata about
/// when and where the neighbor was seen, as well as the specific information
/// the neighbor was advertising.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct LldpNeighbor {
    // Unique ID assigned to this neighbor - only used for pagination
    #[serde(skip)]
    pub id: Uuid,

    /// The port on which the neighbor was seen
    pub local_port: String,

    /// Initial sighting of this LldpNeighbor
    pub first_seen: DateTime<Utc>,

    /// Most recent sighting of this LldpNeighbor
    pub last_seen: DateTime<Utc>,

    /// The LLDP link name advertised by the neighbor
    pub link_name: String,

    /// The LLDP link description advertised by the neighbor
    pub link_description: Option<String>,

    /// The LLDP chassis identifier advertised by the neighbor
    pub chassis_id: String,

    /// The LLDP system name advertised by the neighbor
    pub system_name: Option<String>,

    /// The LLDP system description advertised by the neighbor
    pub system_description: Option<String>,

    /// The LLDP management IP(s) advertised by the neighbor
    pub management_ip: Vec<lldp_protocol::types::ManagementAddress>,
}

impl SimpleIdentity for LldpNeighbor {
    fn id(&self) -> Uuid {
        self.id
    }
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

/// Describes the kind of an switch interface.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SwitchInterfaceKind {
    /// Primary interfaces are associated with physical links. There is exactly
    /// one primary interface per physical link.
    Primary,

    /// VLAN interfaces allow physical interfaces to be multiplexed onto
    /// multiple logical links, each distinguished by a 12-bit 802.1Q Ethernet
    /// tag.
    Vlan,

    /// Loopback interfaces are anchors for IP addresses that are not specific
    /// to any particular port.
    Loopback,
}

/// A switch port interface configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchInterfaceConfig {
    /// The port settings object this switch interface configuration belongs to.
    pub port_settings_id: Uuid,

    /// A unique identifier for this switch interface.
    pub id: Uuid,

    /// The name of this switch interface.
    pub interface_name: Name,

    /// Whether or not IPv6 is enabled on this interface.
    pub v6_enabled: bool,

    /// The switch interface kind.
    pub kind: SwitchInterfaceKind,
}

/// A switch port VLAN interface configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchVlanInterfaceConfig {
    /// The switch interface configuration this VLAN interface configuration
    /// belongs to.
    pub interface_config_id: Uuid,

    /// The virtual network id for this interface that is used for producing and
    /// consuming 802.1Q Ethernet tags. This field has a maximum value of 4095
    /// as 802.1Q tags are twelve bits.
    pub vlan_id: u16,
}

/// A route configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortRouteConfig {
    /// The port settings object this route configuration belongs to.
    pub port_settings_id: Uuid,

    /// The interface name this route configuration is assigned to.
    pub interface_name: Name,

    /// The route's destination network.
    pub dst: oxnet::IpNet,

    /// The route's gateway address.
    pub gw: IpAddr,

    /// The VLAN identifier for the route. Use this if the gateway is reachable
    /// over an 802.1Q tagged L2 segment.
    pub vlan_id: Option<u16>,

    /// RIB Priority indicating priority within and across protocols.
    pub rib_priority: Option<u8>,
}

/// A BGP peer configuration for an interface. Includes the set of announcements
/// that will be advertised to the peer identified by `addr`. The `bgp_config`
/// parameter is a reference to global BGP parameters. The `interface_name`
/// indicates what interface the peer should be contacted on.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpPeer {
    /// The global BGP configuration used for establishing a session with this
    /// peer.
    pub bgp_config: NameOrId,

    /// The name of interface to peer on. This is relative to the port
    /// configuration this BGP peer configuration is a part of. For example this
    /// value could be phy0 to refer to a primary physical interface. Or it
    /// could be vlan47 to refer to a VLAN interface.
    pub interface_name: Name,

    /// The address of the host to peer with.
    pub addr: IpAddr,

    /// How long to hold peer connections between keepalives (seconds).
    pub hold_time: u32,

    /// How long to hold a peer in idle before attempting a new session
    /// (seconds).
    pub idle_hold_time: u32,

    /// How long to delay sending an open request after establishing a TCP
    /// session (seconds).
    pub delay_open: u32,

    /// How long to to wait between TCP connection retries (seconds).
    pub connect_retry: u32,

    /// How often to send keepalive requests (seconds).
    pub keepalive: u32,

    /// Require that a peer has a specified ASN.
    pub remote_asn: Option<u32>,

    /// Require messages from a peer have a minimum IP time to live field.
    pub min_ttl: Option<u8>,

    /// Use the given key for TCP-MD5 authentication with the peer.
    pub md5_auth_key: Option<String>,

    /// Apply the provided multi-exit discriminator (MED) updates sent to the peer.
    pub multi_exit_discriminator: Option<u32>,

    /// Include the provided communities in updates sent to the peer.
    pub communities: Vec<u32>,

    /// Apply a local preference to routes received from this peer.
    pub local_pref: Option<u32>,

    /// Enforce that the first AS in paths received from this peer is the peer's AS.
    pub enforce_first_as: bool,

    /// Define import policy for a peer.
    pub allowed_import: ImportExportPolicy,

    /// Define export policy for a peer.
    pub allowed_export: ImportExportPolicy,

    /// Associate a VLAN ID with a peer.
    pub vlan_id: Option<u16>,
}

/// A base BGP configuration.
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq,
)]
pub struct BgpConfig {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The autonomous system number of this BGP configuration.
    pub asn: u32,

    /// Optional virtual routing and forwarding identifier for this BGP
    /// configuration.
    pub vrf: Option<String>,
}

/// Represents a BGP announce set by id. The id can be used with other API calls
/// to view and manage the announce set.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpAnnounceSet {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
}

/// A BGP announcement tied to an address lot block.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpAnnouncement {
    /// The id of the set this announcement is a part of.
    pub announce_set_id: Uuid,

    /// The address block the IP network being announced is drawn from.
    pub address_lot_block_id: Uuid,

    /// The IP network being announced.
    pub network: oxnet::IpNet,
}

/// An IP address configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortAddressConfig {
    /// The port settings object this address configuration belongs to.
    pub port_settings_id: Uuid,

    /// The id of the address lot block this address is drawn from.
    pub address_lot_block_id: Uuid,

    /// The IP address and prefix.
    pub address: oxnet::IpNet,

    /// An optional VLAN ID
    pub vlan_id: Option<u16>,

    /// The interface name this address belongs to.
    pub interface_name: Name,
}

/// An IP address configuration for a port settings object.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortAddressView {
    /// The port settings object this address configuration belongs to.
    pub port_settings_id: Uuid,

    /// The id of the address lot this address is drawn from.
    pub address_lot_id: Uuid,

    /// The name of the address lot this address is drawn from.
    pub address_lot_name: Name,

    /// The id of the address lot block this address is drawn from.
    pub address_lot_block_id: Uuid,

    /// The IP address and prefix.
    pub address: oxnet::IpNet,

    /// An optional VLAN ID
    pub vlan_id: Option<u16>,

    /// The interface name this address belongs to.
    pub interface_name: Name,
}

/// The current state of a BGP peer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BgpPeerState {
    /// Initial state. Refuse all incoming BGP connections. No resources
    /// allocated to peer.
    Idle,

    /// Waiting for the TCP connection to be completed.
    Connect,

    /// Trying to acquire peer by listening for and accepting a TCP connection.
    Active,

    /// Waiting for open message from peer.
    OpenSent,

    /// Waiting for keepaliave or notification from peer.
    OpenConfirm,

    /// Synchronizing with peer.
    SessionSetup,

    /// Session established. Able to exchange update, notification and keepalive
    /// messages with peers.
    Established,
}

impl From<mg_admin_client::types::FsmStateKind> for BgpPeerState {
    fn from(s: mg_admin_client::types::FsmStateKind) -> BgpPeerState {
        use mg_admin_client::types::FsmStateKind;
        match s {
            FsmStateKind::Idle => BgpPeerState::Idle,
            FsmStateKind::Connect => BgpPeerState::Connect,
            FsmStateKind::Active => BgpPeerState::Active,
            FsmStateKind::OpenSent => BgpPeerState::OpenSent,
            FsmStateKind::OpenConfirm => BgpPeerState::OpenConfirm,
            FsmStateKind::SessionSetup => BgpPeerState::SessionSetup,
            FsmStateKind::Established => BgpPeerState::Established,
        }
    }
}

/// The current status of a BGP peer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpPeerStatus {
    /// IP address of the peer.
    pub addr: IpAddr,

    /// Local autonomous system number.
    pub local_asn: u32,

    /// Remote autonomous system number.
    pub remote_asn: u32,

    /// State of the peer.
    pub state: BgpPeerState,

    /// Time of last state change.
    pub state_duration_millis: u64,

    /// Switch with the peer session.
    pub switch: SwitchLocation,
}

/// The current status of a BGP peer.
#[derive(
    Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, Default,
)]
pub struct BgpExported {
    /// Exported routes indexed by peer address.
    pub exports: HashMap<String, Vec<Ipv4Net>>,
}

/// Opaque object representing BGP message history for a given BGP peer. The
/// contents of this object are not yet stable.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BgpMessageHistory(mg_admin_client::types::MessageHistory);

impl BgpMessageHistory {
    pub fn new(arg: mg_admin_client::types::MessageHistory) -> Self {
        Self(arg)
    }
}

impl JsonSchema for BgpMessageHistory {
    fn json_schema(
        gen: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        let obj = schemars::schema::Schema::Object(
            schemars::schema::SchemaObject::default(),
        );
        gen.definitions_mut().insert(Self::schema_name(), obj.clone());
        obj
    }

    fn schema_name() -> String {
        "BgpMessageHistory".to_owned()
    }
}

/// BGP message history for a particular switch.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct SwitchBgpHistory {
    /// Switch this message history is associated with.
    pub switch: SwitchLocation,

    /// Message history indexed by peer address.
    pub history: HashMap<String, BgpMessageHistory>,
}

/// BGP message history for rack switches.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AggregateBgpMessageHistory {
    /// BGP history organized by switch.
    switch_histories: Vec<SwitchBgpHistory>,
}

impl AggregateBgpMessageHistory {
    pub fn new(switch_histories: Vec<SwitchBgpHistory>) -> Self {
        Self { switch_histories }
    }
}

/// A route imported from a BGP peer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpImportedRouteIpv4 {
    /// The destination network prefix.
    pub prefix: oxnet::Ipv4Net,

    /// The nexthop the prefix is reachable through.
    pub nexthop: Ipv4Addr,

    /// BGP identifier of the originating router.
    pub id: u32,

    /// Switch the route is imported into.
    pub switch: SwitchLocation,
}

/// BFD connection mode.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
)]
#[serde(rename_all = "snake_case")]
pub enum BfdMode {
    SingleHop,
    MultiHop,
}

/// Configuration of inbound ICMP allowed by API services.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
)]
pub struct ServiceIcmpConfig {
    /// When enabled, Nexus is able to receive ICMP Destination Unreachable
    /// type 3 (port unreachable) and type 4 (fragmentation needed),
    /// Redirect, and Time Exceeded messages. These enable Nexus to perform Path
    /// MTU discovery and better cope with fragmentation issues. Otherwise all
    /// inbound ICMP traffic will be dropped.
    pub enabled: bool,
}

/// A description of an uploaded TUF repository.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct TufRepoDescription {
    /// Information about the repository.
    pub repo: TufRepoMeta,

    /// Information about the artifacts present in the repository.
    pub artifacts: Vec<TufArtifactMeta>,
}

impl TufRepoDescription {
    /// Sorts the artifacts so that descriptions can be compared.
    pub fn sort_artifacts(&mut self) {
        self.artifacts.sort_by(|a, b| a.id.cmp(&b.id));
    }
}

/// Metadata about a TUF repository.
///
/// Found within a `TufRepoDescription`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct TufRepoMeta {
    /// The hash of the repository.
    ///
    /// This is a slight abuse of `ArtifactHash`, since that's the hash of
    /// individual artifacts within the repository. However, we use it here for
    /// convenience.
    pub hash: ArtifactHash,

    /// The version of the targets role.
    pub targets_role_version: u64,

    /// The time until which the repo is valid.
    pub valid_until: DateTime<Utc>,

    /// The system version in artifacts.json.
    pub system_version: Version,

    /// The file name of the repository.
    ///
    /// This is purely used for debugging and may not always be correct (e.g.
    /// with wicket, we read the file contents from stdin so we don't know the
    /// correct file name).
    pub file_name: String,
}

/// Metadata about an individual TUF artifact.
///
/// Found within a `TufRepoDescription`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct TufArtifactMeta {
    /// The artifact ID.
    pub id: ArtifactId,

    /// The hash of the artifact.
    pub hash: ArtifactHash,

    /// The size of the artifact in bytes.
    pub size: u64,
}

/// Data about a successful TUF repo import into Nexus.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct TufRepoInsertResponse {
    /// The repository as present in the database.
    pub recorded: TufRepoDescription,

    /// Whether this repository already existed or is new.
    pub status: TufRepoInsertStatus,
}

/// Status of a TUF repo import.
///
/// Part of `TufRepoInsertResponse`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TufRepoInsertStatus {
    /// The repository already existed in the database.
    AlreadyExists,

    /// The repository did not exist, and was inserted into the database.
    Inserted,
}

/// Data about a successful TUF repo get from Nexus.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct TufRepoGetResponse {
    /// The description of the repository.
    pub description: TufRepoDescription,
}

#[derive(
    Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq, ObjectIdentity,
)]
pub struct Probe {
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    pub sled: Uuid,
}

/// Define policy relating to the import and export of prefixes from a BGP
/// peer.
#[derive(
    Default,
    Debug,
    Serialize,
    Deserialize,
    Clone,
    JsonSchema,
    Eq,
    PartialEq,
    Hash,
)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ImportExportPolicy {
    /// Do not perform any filtering.
    #[default]
    NoFiltering,
    Allow(Vec<oxnet::IpNet>),
}

/// Use instead of Option in API request body structs to get a field that can
/// be null (parsed as `None`) but is not optional. Unlike Option, Nullable
/// will fail to parse if the key is not present. The JSON Schema in the
/// OpenAPI definition will also reflect that the field is required. See
/// <https://github.com/serde-rs/serde/issues/2753>.
#[derive(Clone, Debug, Serialize)]
pub struct Nullable<T>(pub Option<T>);

impl<T> From<Option<T>> for Nullable<T> {
    fn from(option: Option<T>) -> Self {
        Nullable(option)
    }
}

impl<T> Deref for Nullable<T> {
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// it looks like we're just using Option's impl here, so why not derive instead?
// For some reason, deriving JsonSchema + #[serde(transparent)] doesn't work --
// it almost does, but the field does not end up marked required in the schema.
// There must be some special handling of Option somewhere causing it to be
// marked optional rather than nullable + required.

impl<T: JsonSchema> JsonSchema for Nullable<T> {
    fn schema_name() -> String {
        T::schema_name()
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        Option::<T>::json_schema(generator)
    }

    fn is_referenceable() -> bool {
        Option::<T>::is_referenceable()
    }
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for Nullable<T> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        // This line is required to get a parse error on missing fields.
        // It seems that when the field is missing in the JSON, struct
        // deserialization produces an error before this function is even hit,
        // and that error is passed in here inside `deserializer`. If we don't
        // do this Value::deserialize to cause that error to be returned as a
        // missing field error, Option's deserialize will eat it by turning it
        // into a successful parse as None.
        let value = serde_json::Value::deserialize(deserializer)?;

        use serde::de::Error;
        Option::<T>::deserialize(value).map_err(D::Error::custom).map(Nullable)
    }
}

#[cfg(test)]
mod test {
    use serde::Deserialize;
    use serde::Serialize;

    use super::Generation;
    use super::RouteDestination;
    use super::RouteTarget;
    use super::VpcFirewallIcmpFilter;
    use super::VpcFirewallRuleHostFilter;
    use super::VpcFirewallRuleTarget;
    use super::{
        ByteCount, Digest, L4Port, L4PortRange, Name, VpcFirewallRuleAction,
        VpcFirewallRuleDirection, VpcFirewallRuleFilter,
        VpcFirewallRulePriority, VpcFirewallRuleProtocol,
        VpcFirewallRuleStatus, VpcFirewallRuleUpdate,
        VpcFirewallRuleUpdateParams,
    };
    use crate::api::external::Error;
    use crate::api::external::Hostname;
    use crate::api::external::ResourceType;
    use semver::Version;
    use std::convert::TryFrom;
    use std::str::FromStr;

    // This test originates from when we had a wrapper struct around
    // `semver::Version`, but it's probably worth carrying this test that
    // ensures the behavior we rely on is accurate.
    #[test]
    fn test_semver_serialize() {
        #[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
        struct MyStruct {
            version: Version,
        }

        let v = MyStruct { version: Version::new(1, 2, 3) };
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
            Err(Error::invalid_value(
                "the_name",
                "name requires at least one character"
            ))
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

        // The largest input value to the `from_*_u32` methods do not create
        // a value larger than i64::MAX.
        assert!(
            ByteCount::from_kibibytes_u32(u32::MAX).to_bytes()
                <= u64::try_from(i64::MAX).unwrap()
        );
        assert!(
            ByteCount::from_mebibytes_u32(u32::MAX).to_bytes()
                <= u64::try_from(i64::MAX).unwrap()
        );
        assert!(
            ByteCount::from_gibibytes_u32(u32::MAX).to_bytes()
                <= u64::try_from(i64::MAX).unwrap()
        );

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
    fn test_generation_display_parse() {
        assert_eq!(Generation::new().to_string(), "1");
        assert_eq!(Generation::from_str("1").unwrap(), Generation::new());
    }

    #[test]
    fn test_generation_serde() {
        assert_eq!(serde_json::to_string(&Generation::new()).unwrap(), "1");
        assert_eq!(
            serde_json::from_str::<Generation>("1").unwrap(),
            Generation::new()
        );
    }

    #[test]
    fn test_generation_from_int() {
        for good_value in [0, Generation::MAX.0] {
            Generation::try_from(good_value).unwrap();
            serde_json::from_str::<Generation>(&good_value.to_string())
                .unwrap();
        }
        for good_value in [0, i64::MAX] {
            Generation::try_from(good_value).unwrap();
            serde_json::from_str::<Generation>(&good_value.to_string())
                .unwrap();
        }
        for bad_value in [Generation::MAX.0 + 1, u64::MAX] {
            Generation::try_from(bad_value).unwrap_err();
            serde_json::from_str::<Generation>(&bad_value.to_string())
                .unwrap_err();
        }
        for bad_value in [-1, i64::MIN] {
            Generation::try_from(bad_value).unwrap_err();
            serde_json::from_str::<Generation>(&bad_value.to_string())
                .unwrap_err();
        }
    }

    #[test]
    #[should_panic(expected = "attempt to overflow generation number")]
    fn test_generation_overflow() {
        Generation::MAX.next();
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
            L4PortRange::try_from("".to_string()).map_err(Into::into),
            Err(Error::invalid_value(
                "l4_port_range",
                "\"\" unparsable for type: cannot parse integer from empty string"
            ))
        );
        assert_eq!(
            L4PortRange::try_from("65536".to_string()).map_err(Into::into),
            Err(Error::invalid_value(
                "l4_port_range",
                "\"65536\" unparsable for type: number too large to fit in target type"
            ))
        );
        assert_eq!(
            L4PortRange::try_from("65535-65536".to_string())
                .map_err(Into::into),
            Err(Error::invalid_value(
                "l4_port_range",
                "\"65536\" unparsable for type: number too large to fit in target type"
            ))
        );
        assert_eq!(
            L4PortRange::try_from("0x23".to_string()).map_err(Into::into),
            Err(Error::invalid_value(
                "l4_port_range",
                "\"0x23\" unparsable for type: invalid digit found in string"
            ))
        );
        assert_eq!(
            L4PortRange::try_from("0".to_string()).map_err(Into::into),
            Err(Error::invalid_value(
                "l4_port_range",
                "\"0\" unparsable for type: number would be zero for non-zero type"
            ))
        );
        assert_eq!(
            L4PortRange::try_from("0-20".to_string()).map_err(Into::into),
            Err(Error::invalid_value(
                "l4_port_range",
                "\"0\" unparsable for type: number would be zero for non-zero type"
            ))
        );
        assert_eq!(
            L4PortRange::try_from("-20".to_string()).map_err(Into::into),
            Err(Error::invalid_value(
                "l4_port_range",
                "range has no start value"
            ))
        );
        assert_eq!(
            L4PortRange::try_from("21-20".to_string()).map_err(Into::into),
            Err(Error::invalid_value(
                "l4_port_range",
                "range has larger start value than end value"
            ))
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
                "filters": {"ports": [ "22-25", "27" ], "protocols": [ { "type": "udp" } ]},
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
        use super::Ipv6NetExt;
        use oxnet::Ipv6Net;

        assert!("fd00::/8".parse::<Ipv6Net>().unwrap().is_unique_local());
        assert!(!"fe00::/8".parse::<Ipv6Net>().unwrap().is_unique_local());

        assert!("fd00::/48".parse::<Ipv6Net>().unwrap().is_vpc_prefix());
        assert!(!"fe00::/48".parse::<Ipv6Net>().unwrap().is_vpc_prefix());
        assert!(!"fd00::/40".parse::<Ipv6Net>().unwrap().is_vpc_prefix());

        let vpc_prefix = "fd00::/48".parse::<Ipv6Net>().unwrap();
        assert!(
            "fd00::/64".parse::<Ipv6Net>().unwrap().is_vpc_subnet(&vpc_prefix)
        );
        assert!(
            !"fd10::/64".parse::<Ipv6Net>().unwrap().is_vpc_subnet(&vpc_prefix)
        );
        assert!(
            !"fd00::/63".parse::<Ipv6Net>().unwrap().is_vpc_subnet(&vpc_prefix)
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
    fn test_firewall_rule_proto_filter_parse() {
        assert_eq!(VpcFirewallRuleProtocol::Tcp, "tcp".parse().unwrap());
        assert_eq!(VpcFirewallRuleProtocol::Udp, "udp".parse().unwrap());

        assert_eq!(
            VpcFirewallRuleProtocol::Icmp(None),
            "icmp".parse().unwrap()
        );
        assert_eq!(
            VpcFirewallRuleProtocol::Icmp(Some(VpcFirewallIcmpFilter {
                icmp_type: 4,
                code: None
            })),
            "icmp:4".parse().unwrap()
        );
        assert_eq!(
            VpcFirewallRuleProtocol::Icmp(Some(VpcFirewallIcmpFilter {
                icmp_type: 60,
                code: Some(0.into())
            })),
            "icmp:60,0".parse().unwrap()
        );
        assert_eq!(
            VpcFirewallRuleProtocol::Icmp(Some(VpcFirewallIcmpFilter {
                icmp_type: 60,
                code: Some((0..=10).try_into().unwrap())
            })),
            "icmp:60,0-10".parse().unwrap()
        );
        assert_eq!(
            "icmp:".parse::<VpcFirewallRuleProtocol>(),
            Err(Error::invalid_value(
                "icmp_type",
                "\"\" unparsable for type: cannot parse integer from empty string"
            ))
        );
        assert_eq!(
            "icmp:20-30".parse::<VpcFirewallRuleProtocol>(),
            Err(Error::invalid_value(
                "icmp_type",
                "\"20-30\" unparsable for type: invalid digit found in string"
            ))
        );
        assert_eq!(
            "icmp:10,".parse::<VpcFirewallRuleProtocol>(),
            Err(Error::invalid_value(
                "code",
                "\"\" unparsable for type: cannot parse integer from empty string"
            ))
        );
        assert_eq!(
            "icmp:257,".parse::<VpcFirewallRuleProtocol>(),
            Err(Error::invalid_value(
                "icmp_type",
                "\"257\" unparsable for type: number too large to fit in target type"
            ))
        );
        assert_eq!(
            "icmp:0,1000-1001".parse::<VpcFirewallRuleProtocol>(),
            Err(Error::invalid_value(
                "code",
                "\"1000\" unparsable for type: number too large to fit in target type"
            ))
        );
        assert_eq!(
            "icmp:0,30-".parse::<VpcFirewallRuleProtocol>(),
            Err(Error::invalid_value("code", "range has no end value"))
        );
        assert_eq!(
            "icmp:0,21-20".parse::<VpcFirewallRuleProtocol>(),
            Err(Error::invalid_value(
                "code",
                "range has larger start value than end value"
            ))
        );
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

    #[test]
    fn test_mac_system_iterator() {
        use super::MacAddr;

        let mut count = 0;
        for m in MacAddr::iter_system() {
            assert!(m.is_system());
            assert!(m.to_i64() > MacAddr::MAX_SYSTEM_RESV);
            count += 1;
        }
        assert_eq!(count, MacAddr::MAX_SYSTEM_ADDR - MacAddr::MAX_SYSTEM_RESV);
    }

    #[test]
    fn test_mac_to_int_conversions() {
        use super::MacAddr;
        let original: i64 = 0xa8_40_25_ff_00_01;
        let mac = MacAddr::from_i64(original);
        assert_eq!(mac.0.as_bytes(), &[0xa8, 0x40, 0x25, 0xff, 0x00, 0x01]);
        let conv = mac.to_i64();
        assert_eq!(original, conv);
    }

    #[test]
    fn test_hostname_from_str() {
        assert!(Hostname::from_str("name").is_ok());
        assert!(Hostname::from_str("a.good.name").is_ok());
        assert!(Hostname::from_str("another.very-good.name").is_ok());
        assert!(Hostname::from_str("0name").is_ok());
        assert!(Hostname::from_str("name0").is_ok());
        assert!(Hostname::from_str("0name0").is_ok());

        assert!(Hostname::from_str("").is_err());
        assert!(Hostname::from_str("no_no").is_err());
        assert!(Hostname::from_str("no.fqdns.").is_err());
        assert!(Hostname::from_str("empty..label").is_err());
        assert!(Hostname::from_str("-hypen.cannot.start").is_err());
        assert!(Hostname::from_str("hypen.-cannot.start").is_err());
        assert!(Hostname::from_str("hypen.cannot.end-").is_err());
        assert!(Hostname::from_str("hyphen-cannot-end-").is_err());
        assert!(Hostname::from_str(&"too-long".repeat(100)).is_err());
    }
}
