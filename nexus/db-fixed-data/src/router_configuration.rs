// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_model as model;
use nexus_types::router_configuration::{
    DEFAULT_ROUTER_CONFIGURATION_ID, default_router_configuration_name,
};
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_uuid_kinds::{GenericUuid, RouterConfigurationUuid};
use std::sync::LazyLock;

pub static DEFAULT_ROUTER_CONFIGURATION: LazyLock<model::RouterConfiguration> =
    LazyLock::new(|| model::RouterConfiguration {
        identity: model::RouterConfigurationIdentity::new(
            RouterConfigurationUuid::from_untyped_uuid(
                DEFAULT_ROUTER_CONFIGURATION_ID,
            ),
            IdentityMetadataCreateParams {
                name: default_router_configuration_name().clone(),
                description: "Built-in configuration of each switch's default \
                router."
                    .to_string(),
            },
        ),
    });

#[cfg(test)]
mod test {
    use super::DEFAULT_ROUTER_CONFIGURATION;
    use crate::assert_valid_uuid;
    use nexus_types::identity::Resource;
    use omicron_uuid_kinds::GenericUuid;

    #[test]
    fn test_default_router_configuration_id_is_valid() {
        assert_valid_uuid(
            &DEFAULT_ROUTER_CONFIGURATION.id().into_untyped_uuid(),
        );
    }
}
