// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Router-configuration utilities and fixed data.

use std::sync::LazyLock;

use omicron_common::api::external::Name;

/// The ID of the built-in "default" router configuration.
pub static DEFAULT_ROUTER_CONFIGURATION_ID: uuid::Uuid =
    uuid::Uuid::from_u128(0x001de000_defa_4000_8000_000000000000);

/// Whether `id` is the built-in default router configuration.
pub fn is_builtin_router_configuration_id(id: &uuid::Uuid) -> bool {
    *id == DEFAULT_ROUTER_CONFIGURATION_ID
}

/// Return the name of the built-in default router configuration.
pub fn default_router_configuration_name() -> &'static Name {
    static NAME: LazyLock<Name> = LazyLock::new(|| "default".parse().unwrap());

    &NAME
}

/// Whether `name` is reserved and cannot be minted by operators: the
/// built-in's name, so it can always be created by populate.
pub fn is_reserved_router_configuration_name(name: &Name) -> bool {
    name == default_router_configuration_name()
}

/// Defaults shared by RC API peers and commissioning input. Use the existing
/// rack-initialization values and materialize them during deserialization, so
/// serialized configuration and RSS handoff always contain concrete settings.
pub mod bgp_defaults {
    use sled_agent_types_versions::v47::early_networking::BgpPeerConfig;

    pub fn hold_time() -> u32 {
        BgpPeerConfig::DEFAULT_HOLD_TIME
            .try_into()
            .expect("BGP timer default fits in u32")
    }

    pub fn keepalive() -> u32 {
        BgpPeerConfig::DEFAULT_KEEPALIVE
            .try_into()
            .expect("BGP timer default fits in u32")
    }

    pub fn connect_retry() -> u32 {
        BgpPeerConfig::DEFAULT_CONNECT_RETRY
            .try_into()
            .expect("BGP timer default fits in u32")
    }

    pub fn delay_open() -> u32 {
        BgpPeerConfig::DEFAULT_DELAY_OPEN
            .try_into()
            .expect("BGP timer default fits in u32")
    }

    pub fn idle_hold_time() -> u32 {
        BgpPeerConfig::DEFAULT_IDLE_HOLD_TIME
            .try_into()
            .expect("BGP timer default fits in u32")
    }
}
