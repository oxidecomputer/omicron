// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_model as model;
use nexus_types::router_configuration::{
    DEFAULT_SWITCH0_ROUTER_CONFIGURATION_ID,
    DEFAULT_SWITCH1_ROUTER_CONFIGURATION_ID,
    default_switch0_router_configuration_name,
    default_switch1_router_configuration_name,
};
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_uuid_kinds::{GenericUuid, RouterConfigurationUuid};
use std::sync::LazyLock;

fn builtin(
    id: uuid::Uuid,
    name: &omicron_common::api::external::Name,
    switch: model::DbSwitchSlot,
) -> model::RouterConfiguration {
    model::RouterConfiguration {
        identity: model::RouterConfigurationIdentity::new(
            RouterConfigurationUuid::from_untyped_uuid(id),
            IdentityMetadataCreateParams {
                name: name.clone(),
                description: format!(
                    "Built-in default router configuration for {}.",
                    match switch {
                        model::DbSwitchSlot::Switch0 => "switch 0",
                        model::DbSwitchSlot::Switch1 => "switch 1",
                    }
                ),
            },
        ),
        switch,
        bgp_config: None,
    }
}

pub static DEFAULT_SWITCH0_ROUTER_CONFIGURATION: LazyLock<
    model::RouterConfiguration,
> = LazyLock::new(|| {
    builtin(
        DEFAULT_SWITCH0_ROUTER_CONFIGURATION_ID,
        default_switch0_router_configuration_name(),
        model::DbSwitchSlot::Switch0,
    )
});

pub static DEFAULT_SWITCH1_ROUTER_CONFIGURATION: LazyLock<
    model::RouterConfiguration,
> = LazyLock::new(|| {
    builtin(
        DEFAULT_SWITCH1_ROUTER_CONFIGURATION_ID,
        default_switch1_router_configuration_name(),
        model::DbSwitchSlot::Switch1,
    )
});

#[cfg(test)]
mod test {
    use super::{
        DEFAULT_SWITCH0_ROUTER_CONFIGURATION,
        DEFAULT_SWITCH1_ROUTER_CONFIGURATION,
    };
    use crate::assert_valid_uuid;
    use nexus_types::identity::Resource;
    use omicron_uuid_kinds::GenericUuid;

    #[test]
    fn test_default_router_configuration_ids_are_valid() {
        assert_valid_uuid(
            &DEFAULT_SWITCH0_ROUTER_CONFIGURATION.id().into_untyped_uuid(),
        );
        assert_valid_uuid(
            &DEFAULT_SWITCH1_ROUTER_CONFIGURATION.id().into_untyped_uuid(),
        );
    }
}
