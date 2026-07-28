// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for user-provided multirack cluster join configuration options.

use crate::RssOrMultirackJoinConfigCommon;
use crate::bgp_auth_keys::BgpAuthKeys;
use crate::context::CommonConfigContainer;
use sled_hardware_types::Baseboard;
use std::collections::BTreeMap;
use std::net::Ipv6Addr;
use wicket_common::inventory::MgsV1Inventory;
use wicket_common::inventory::SledInventory;
use wicket_common::multirack_setup::CurrentMultirackJoinUserConfig;
use wicket_common::multirack_setup::MultirackJoinConfigBaseUserInput;
use wicket_common::rack_setup::GetBgpAuthKeyInfoResponse;
use wicketd_commission_types::rack_setup::UserSpecifiedRackNetworkConfig;

pub(crate) struct CurrentMultirackJoinConfig {
    pub common: RssOrMultirackJoinConfigCommon,
    rack_network_config: UserSpecifiedRackNetworkConfig,
}

impl CurrentMultirackJoinConfig {
    pub(crate) fn update(
        &mut self,
        config: MultirackJoinConfigBaseUserInput,
        our_baseboard: Option<&Baseboard>,
        inventory: &MgsV1Inventory,
        ddm_discovered_sleds: &BTreeMap<Baseboard, Ipv6Addr>,
        log: &slog::Logger,
    ) -> Result<(), String> {
        self.common.update(
            &config.bootstrap_slots,
            config.rack_network_config.get_bgp_auth_key_ids(),
            our_baseboard,
            inventory,
            ddm_discovered_sleds,
            log,
        )?;
        self.rack_network_config = config.rack_network_config;

        Ok(())
    }

    pub(crate) fn new_with_inventory_and_peers(
        our_baseboard: Option<&Baseboard>,
        config: MultirackJoinConfigBaseUserInput,
        inventory: &MgsV1Inventory,
        ddm_discovered_sleds: &BTreeMap<Baseboard, Ipv6Addr>,
        log: &slog::Logger,
    ) -> Result<Self, String> {
        let sled_inventory =
            SledInventory::new(inventory, &ddm_discovered_sleds, log);

        sled_inventory.verify_our_baseboard_is_in_inventory_slot(
            &config.bootstrap_slots,
            our_baseboard,
        )?;

        let bootstrap_sleds = sled_inventory
            .load_bootstrap_sleds_by_user_chosen_slots(
                &config.bootstrap_slots,
            )?;

        let mut bgp_auth_keys = BgpAuthKeys::default();
        let new_bgp_auth_key_ids =
            config.rack_network_config.get_bgp_auth_key_ids();
        bgp_auth_keys.sync_keys(new_bgp_auth_key_ids);

        let common = RssOrMultirackJoinConfigCommon {
            inventory: sled_inventory,
            bootstrap_sleds,
            bgp_auth_keys,
        };

        Ok(Self { common, rack_network_config: config.rack_network_config })
    }
}

impl CommonConfigContainer for CurrentMultirackJoinConfig {
    fn common_mut(&mut self) -> &mut RssOrMultirackJoinConfigCommon {
        &mut self.common
    }
}

impl From<&'_ CurrentMultirackJoinConfig> for CurrentMultirackJoinUserConfig {
    fn from(config: &'_ CurrentMultirackJoinConfig) -> Self {
        // If the user has selected bootstrap sleds, use those; otherwise,
        // default to the full inventory list.
        let bootstrap_sleds = if !config.common.bootstrap_sleds.is_empty() {
            config.common.bootstrap_sleds.clone()
        } else {
            config.common.inventory.sleds.clone()
        };

        CurrentMultirackJoinUserConfig {
            bootstrap_sleds,
            rack_network_config: config.rack_network_config.clone(),
            bgp_auth_keys: GetBgpAuthKeyInfoResponse {
                data: config.common.get_bgp_auth_key_data(),
            },
        }
    }
}
