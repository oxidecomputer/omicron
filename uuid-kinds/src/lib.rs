// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#![cfg_attr(not(feature = "std"), no_std)]

//! A registry for UUID kinds used in Omicron and related projects.
//!
//! See this crate's `README.adoc` for more information.

// Export these types so that other users don't have to pull in newtype-uuid.
#[doc(no_inline)]
pub use newtype_uuid::{
    GenericUuid, ParseError, TagError, TypedUuid, TypedUuidKind, TypedUuidTag,
};

macro_rules! impl_typed_uuid_kind {
    ($($kind:ident => $tag:literal),* $(,)?) => {
        $(
            paste::paste! {
                pub enum [< $kind Kind >] {}

                #[cfg(feature = "schemars08")]
                impl schemars::JsonSchema for [< $kind Kind >]{
                    fn schema_name() -> String {
                        stringify!([< $kind Kind >]).to_string()
                    }

                    fn is_referenceable() -> bool {
                        false
                    }

                    fn json_schema(_: &mut schemars::gen::SchemaGenerator)
                        -> schemars::schema::Schema
                    {
                        use schemars::schema::Metadata;
                        use schemars::schema::Schema;
                        use schemars::schema::SchemaObject;
                        use schemars::schema::SubschemaValidation;

                        SchemaObject {
                            metadata: Some(
                                Metadata{
                                    title: Some(Self::schema_name()),
                                    ..Default::default()
                                }.into(),
                            ),
                            subschemas: Some(
                                SubschemaValidation {
                                    not: Some(Schema::Bool(true).into()),
                                    ..Default::default()
                                }
                                .into(),
                            ),
                            extensions: [(
                                "x-rust-type".to_string(),
                                serde_json::json!({
                                    "crate": "omicron-uuid-kinds",
                                    "version": "0.1.0",
                                    "path":
                                        format!(
                                            "omicron_uuid_kinds::{}",
                                            Self::schema_name(),
                                        ),
                                }),
                            )]
                            .into_iter()
                            .collect(),
                            ..Default::default()
                        }
                        .into()
                    }
                }

                impl TypedUuidKind for [< $kind Kind >] {
                    #[inline]
                    fn tag() -> TypedUuidTag {
                        // `const` ensures that tags are validated at compile-time.
                        const TAG: TypedUuidTag = TypedUuidTag::new($tag);
                        TAG
                    }
                }

                pub type [< $kind Uuid>] = TypedUuid::<[< $kind Kind >]>;
            }
        )*
    };
}

// NOTE:
//
// This should generally be an append-only list. Removing items from this list
// will not break things for now (because newtype-uuid does not currently alter
// any serialization formats), but it may involve some degree of churn across
// repos.
//
// Please keep this list in alphabetical order.

impl_typed_uuid_kind! {
    Collection => "collection",
    Downstairs => "downstairs",
    DownstairsRegion => "downstairs_region",
    ExternalIp => "external_ip",
    Instance => "instance",
    LoopbackAddress => "loopback_address",
    OmicronZone => "service",
    PhysicalDisk => "physical_disk",
    Propolis => "propolis",
    Sled => "sled",
    TufRepo => "tuf_repo",
    Upstairs => "upstairs",
    UpstairsRepair => "upstairs_repair",
    UpstairsSession => "upstairs_session",
    Vnic => "vnic",
    Zpool => "zpool",
}
