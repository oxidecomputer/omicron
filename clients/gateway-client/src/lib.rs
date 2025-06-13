// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! Interface for API requests to a Management Gateway Service (MGS) instance

pub use gateway_messages::SpComponent;

// We specifically want to allow consumers, such as `wicketd`, to embed
// inventory datatypes into their own APIs, rather than recreate structs.
//
// We did not add this functionality to `omicron_common` because, in the common
// case, we want to prohibit users from accidentally exposing implementation
// specific, private types. As an example, we don't want to make it trivial
// to include propolis types that may change in the Oxide public API. For
// scenarios like this, forcing the user of a client API to generate a new type
// serves as a safety feature in the common case. The user of an underlying
// dropshot server can always choose to use this escape hatch.
//
// In this case, we choose to allow the escape hatch, because one of two
// primary consumers of MGS is wicketd, and wicketd wants to share inventory
// data, which is already in a useful format, directly with wicket for
// processing without having to perform unnecessary manipulation. In essence,
// wicketd is just proxying information for display. Furthermore, wicket
// itself is a TUI, and so it will not be forwarding these types to any public
// consumers. While MGS also has Nexus as a client, the use case is fairly
// constrained and it is unlikely we'd desire to expose MGS types directly. In
// this instance we'll want to be extra careful, but we shouldn't burden ourself
// unduly here to prevent an unlikely mistake.
//
// If the format of inventory data desired by wicket or nexus changes such that
// it is no longer useful to directly expose the JsonSchema types, we can go
// back to reusing `omicron_common`.
//
// As another alternative, since the `derives` and `patch` directives were
// introduced, these types have moved to living in gateway-types. This means
// that we can choose to use `replace` on them. That hasn't felt necessary so
// far, but it's an option if it becomes desirable in the future. (One reason
// to do that might be that currently, `nexus-types` depends on
// `gateway-client`. Generally we expect the `-client` layer to depend on the
// `-types` layer to avoid a circular dependency, and we've had to resolve a
// rather thorny one between Nexus and sled-agent. But Nexus and sled-agent
// call into each other. Since `gateway` is a lower-level service and never
// calls into Nexus, the current scheme is okay.)
progenitor::generate_api!(
    spec = "../../openapi/gateway.json",
    interface = Positional,
    inner_type = slog::Logger,
    pre_hook = (|log: &slog::Logger, request: &reqwest::Request| {
        slog::debug!(log, "client request";
            "method" => %request.method(),
            "uri" => %request.url(),
            "body" => ?&request.body(),
        );
    }),
    post_hook = (|log: &slog::Logger, result: &Result<_, _>| {
        slog::debug!(log, "client response"; "result" => ?result);
    }),
    derives = [schemars::JsonSchema],
    patch = {
        HostPhase2RecoveryImageId = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        ImageVersion = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        RotImageDetails = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        RotImageError = { derives = [ thiserror::Error, PartialEq, Eq, PartialOrd, Ord] },
        RotState = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        SpComponentCaboose = { derives = [PartialEq, Eq] },
        SpIdentifier = { derives = [Copy, PartialEq, Hash, Eq] },
        SpIgnition = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        SpIgnitionSystemType = { derives = [Copy, PartialEq, Eq, PartialOrd, Ord] },
        SpState = { derives = [PartialEq, Eq, PartialOrd, Ord] },
        SpType = { derives = [daft::Diffable] },
        SpUpdateStatus = { derives = [PartialEq, Hash, Eq] },
        UpdatePreparationProgress = { derives = [PartialEq, Hash, Eq] },
    },
    replace = {
        RotSlot = gateway_types::rot::RotSlot,
        TypedUuidForMupdateKind = omicron_uuid_kinds::MupdateUuid,
    },
);

// Override the impl of Ord for SpIdentifier because the default one orders the
// fields in a different order than people are likely to want.
impl Ord for crate::types::SpIdentifier {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.type_.cmp(&other.type_).then(self.slot.cmp(&other.slot))
    }
}

impl PartialOrd for crate::types::SpIdentifier {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
