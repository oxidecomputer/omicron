// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A registry for UUID kinds used in Omicron and related projects.
//!
//! See this crate's `README.adoc` for more information.

#![cfg_attr(not(feature = "std"), no_std)]

// Export these types so that other users don't have to pull in newtype-uuid.
#[doc(no_inline)]
pub use newtype_uuid::{
    GenericUuid, ParseError, TagError, TypedUuid, TypedUuidKind, TypedUuidTag,
};

use daft::Diffable;
use newtype_uuid_macros::impl_typed_uuid_kinds;

// NOTE:
//
// This should generally be an append-only list. Removing items from this list
// will not break things for now (because newtype-uuid does not currently alter
// any serialization formats), but it may involve some degree of churn across
// repos.
//
// Please keep this list in alphabetical order.
impl_typed_uuid_kinds! {
    settings = {
        attrs = [#[derive(Diffable)]],
        schemars08 = {
            attrs = [#[cfg(feature = "schemars08")]],
            rust_type = {
                crate = "omicron-uuid-kinds",
                version = "*",
                path = "omicron_uuid_kinds",
            },
        },
    },
    kinds = {
        AccessToken = {},
        AffinityGroup = {},
        Alert = {},
        AlertReceiver = {},
        AntiAffinityGroup = {},
        Blueprint = {},
        BuiltInUser = {},
        Collection = {},
        ConsoleSession = {},
        Dataset = {},
        DemoSaga = {},
        Downstairs = {},
        DownstairsRegion = {},
        EreporterRestart = {},
        ExternalIp = {},
        ExternalZpool = {},
        Instance = {},
        InternalZpool = {},
        LoopbackAddress = {},
        MulticastGroup = {},
        Mupdate = {},
        MupdateOverride = {},
        // `OmicronSledConfig`s do not themselves contain IDs, but we generate IDs
        // for them when they're serialized to the database during inventory
        // collection. This ID type is therefore only used by nexus-db-model and
        // nexus-db-queries.
        OmicronSledConfig = {},
        OmicronZone = {},
        PhysicalDisk = {},
        Probe = {},
        Propolis = {},
        Rack = {},
        RackInit = {},
        RackReset = {},
        ReconfiguratorSimState = {},
        Region = {},
        SiloGroup = {},
        SiloUser = {},
        Sitrep = {},
        Sled = {},
        SpUpdate = {},
        SupportBundle = {},
        TufArtifact = {},
        TufRepo = {},
        TufTrustRoot = {},
        Upstairs = {},
        UpstairsRepair = {},
        UpstairsSession = {},
        UserDataExport = {},
        Vnic = {},
        Volume = {},
        WebhookDelivery = {},
        WebhookDeliveryAttempt = {},
        WebhookSecret = {},
        Zpool = {},
    },
}

impl From<ExternalZpoolKind> for ZpoolKind {
    fn from(kind: ExternalZpoolKind) -> Self {
        match kind {}
    }
}

impl From<InternalZpoolKind> for ZpoolKind {
    fn from(kind: InternalZpoolKind) -> Self {
        match kind {}
    }
}
