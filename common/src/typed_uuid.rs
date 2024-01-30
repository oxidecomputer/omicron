// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use derive_where::derive_where;
use parse_display::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use thiserror::Error;
use uuid::Uuid;

/// A UUID with type-level information about what it's used for.
///
/// In omicron, we use UUIDs to represent many different kinds of items. If
/// they're all the same type, it's easy to mix them up. This module provides a
/// level of type safety which should hopefully prevent most mistakes.
///
/// This uses a marker type rather than defining separate `SledId`, `ProjectId`
/// etc, to make it easier to write code that's generic over all possible typed
/// UUIDs.
#[derive_where(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct TypedUuid<T: TypedUuidKind> {
    uuid: Uuid,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: TypedUuidKind> TypedUuid<T> {
    /// Creates a new UUID of this type.
    #[inline]
    pub fn new_v4() -> Self {
        Self { uuid: Uuid::new_v4(), _phantom: std::marker::PhantomData }
    }

    /// Create a new `TypedUuid` from an untyped [`Uuid`].
    ///
    /// It is the caller's responsibility to ensure that the UUID is of the
    /// correct type.
    #[inline]
    pub fn from_untyped(uuid: Uuid) -> Self {
        Self { uuid, _phantom: std::marker::PhantomData }
    }

    /// Returns the inner [`Uuid`].
    ///
    /// Generally, [`to_untyped_uuid`](ToUntypedUuid::to_untyped_uuid) should
    /// be preferred. However, in some cases it may be necessary to use this
    /// method to satisfy lifetime constraints.
    #[inline]
    pub fn as_untyped_uuid(&self) -> &Uuid {
        &self.uuid
    }
}

impl<T: TypedUuidKind> fmt::Debug for TypedUuid<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.uuid.fmt(f)?;
        write!(f, " ({})", T::tag())
    }
}

impl<T: TypedUuidKind> fmt::Display for TypedUuid<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.uuid.fmt(f)
    }
}

impl<T: TypedUuidKind> FromStr for TypedUuid<T> {
    type Err = TypedUuidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uuid = Uuid::from_str(s)
            .map_err(|error| TypedUuidParseError { error, tag: T::tag() })?;
        Ok(Self { uuid, _phantom: std::marker::PhantomData })
    }
}

impl<T: TypedUuidKind> JsonSchema for TypedUuid<T> {
    #[inline]
    fn schema_name() -> String {
        format!("TypedUuidFor{}", T::schema_name())
    }

    #[inline]
    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(format!(
            "{}::TypedUuid<{}>",
            module_path!(),
            T::schema_id()
        ))
    }

    #[inline]
    fn json_schema(
        gen: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        Uuid::json_schema(gen)
    }
}

/// Represents marker types that can be used as a type parameter for [`TypedUuid`].
pub trait TypedUuidKind: JsonSchema {
    /// Returns the corresponding tag for this kind.
    ///
    /// The tag forms a runtime representation of this type-level value.
    fn tag() -> TypedUuidTag;
}

/// Describes what kind of [`TypedUuid`] something is.
///
/// This is the runtime equivalent of [`TypedUuidKind`].
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Display)]
#[display(style = "snake_case")]
pub enum TypedUuidTag {
    /// Corresponds to [`LoopbackAddressKind`].
    LoopbackAddress,
}

macro_rules! impl_typed_uuid_kind {
    ($($kind:ident => $tag:ident),* $(,)?) => {
        $(
            #[derive(JsonSchema)]
            pub enum $kind {}

            impl TypedUuidKind for $kind {
                #[inline]
                fn tag() -> TypedUuidTag {
                    TypedUuidTag::$tag
                }
            }
        )*
    };
}

impl_typed_uuid_kind! {
    LoopbackAddressKind => LoopbackAddress,
}

#[derive(Error, Debug)]
#[error("error parsing uuid for {tag}")]
pub struct TypedUuidParseError {
    #[source]
    pub(crate) error: uuid::Error,
    pub(crate) tag: TypedUuidTag,
}

/// A trait abstracting over typed and untyped UUIDs.
///
/// This trait is similar to `From`, but we don't want to implement
/// `From<TypedUuid<T>> for Uuid` because we want the conversion from typed to
/// untyped UUIDs to be explicit.
pub trait ToUntypedUuid {
    /// Convert `self` into an untyped [`Uuid`].
    fn to_untyped_uuid(self) -> Uuid;
}

impl ToUntypedUuid for Uuid {
    #[inline]
    fn to_untyped_uuid(self) -> Uuid {
        self
    }
}

impl<T: TypedUuidKind> ToUntypedUuid for TypedUuid<T> {
    #[inline]
    fn to_untyped_uuid(self) -> Uuid {
        self.uuid
    }
}
