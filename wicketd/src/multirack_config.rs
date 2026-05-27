// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for user-provided multirack cluster join configuration options.

use crate::bgp_auth_keys::BgpAuthKeyError;
use crate::bgp_auth_keys::BgpAuthKeys;
use crate::bootstrap_addrs::BootstrapPeers;
use omicron_common::api::external::AllowedSourceIps;
use sled_hardware_types::BaseboardId;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::mem;
use wicket_common::inventory::MgsV1Inventory;
use wicket_common::inventory::SledInventory;
use wicket_common::multirack_setup::CurrentMultirackJoinUserConfig;
use wicket_common::multirack_setup::MultirackJoinConfigBaseUserInput;
use wicket_common::rack_setup::BgpAuthKey;
use wicket_common::rack_setup::BgpAuthKeyId;
use wicket_common::rack_setup::BgpAuthKeyStatus;
use wicket_common::rack_setup::BootstrapSledDescription;
use wicket_common::rack_setup::GetBgpAuthKeyInfoResponse;
use wicket_common::rack_setup::UserSpecifiedRackNetworkConfig;
use wicketd_api::SetBgpAuthKeyStatus;

pub(crate) struct CurrentMultirackJoinConfig {
    inventory: SledInventory,
    bootstrap_sleds: BTreeSet<BootstrapSledDescription>,
    rack_network_config: UserSpecifiedRackNetworkConfig,
    bgp_auth_keys: BgpAuthKeys,
    allowed_source_ips: AllowedSourceIps,
}

impl CurrentMultirackJoinConfig {
    pub(crate) fn check_bgp_auth_keys_valid<'a>(
        &self,
        check_valid: impl IntoIterator<Item = &'a BgpAuthKeyId>,
    ) -> Result<(), BgpAuthKeyError> {
        self.bgp_auth_keys.check_valid(check_valid)
    }

    pub(crate) fn get_bgp_auth_key_data(
        &self,
    ) -> BTreeMap<BgpAuthKeyId, BgpAuthKeyStatus> {
        self.bgp_auth_keys.get_data()
    }

    pub(crate) fn set_bgp_auth_key(
        &mut self,
        key_id: BgpAuthKeyId,
        key: BgpAuthKey,
    ) -> Result<SetBgpAuthKeyStatus, BgpAuthKeyError> {
        self.bgp_auth_keys.set_key(key_id, key)
    }

    pub(crate) fn update_with_inventory_and_bootstrap_peers(
        &mut self,
        inventory: &MgsV1Inventory,
        bootstrap_peers: &BootstrapPeers,
        log: &slog::Logger,
    ) {
        let bootstrap_sleds = bootstrap_peers.sleds();
        self.inventory = SledInventory::new(inventory, &bootstrap_sleds, log);

        // If the user has already uploaded a config specifying bootstrap_sleds,
        // also update our knowledge of those sleds' bootstrap addresses.
        let our_bootstrap_sleds = mem::take(&mut self.bootstrap_sleds);
        self.bootstrap_sleds = our_bootstrap_sleds
            .into_iter()
            .map(|mut sled_desc| {
                sled_desc.bootstrap_ip =
                    bootstrap_sleds.get(&sled_desc.baseboard_id).copied();
                sled_desc
            })
            .collect();
    }

    pub(crate) fn update(
        &mut self,
        config: MultirackJoinConfigBaseUserInput,
        our_baseboard: &BaseboardId,
    ) -> Result<(), String> {
        self.inventory.verify_our_baseboard_is_in_inventory_slot(
            &config.bootstrap_slots,
            our_baseboard,
        )?;
        self.bootstrap_sleds =
            self.inventory.load_bootstrap_sleds(config.bootstrap_slots)?;

        let new_bgp_auth_key_ids =
            config.rack_network_config.get_bgp_auth_key_ids();
        self.bgp_auth_keys.sync_keys(new_bgp_auth_key_ids);
        self.rack_network_config = config.rack_network_config;
        self.allowed_source_ips = config.allowed_source_ips;

        Ok(())
    }

    pub(crate) fn new_with_inventory_and_bootstrap_peers(
        our_baseboard: &BaseboardId,
        config: MultirackJoinConfigBaseUserInput,
        inventory: &MgsV1Inventory,
        bootstrap_peers: &BootstrapPeers,
        log: &slog::Logger,
    ) -> Result<Self, String> {
        let bootstrap_sleds = bootstrap_peers.sleds();

        let sled_inventory =
            SledInventory::new(inventory, &bootstrap_sleds, log);

        sled_inventory.verify_our_baseboard_is_in_inventory_slot(
            &config.bootstrap_slots,
            our_baseboard,
        )?;

        let bootstrap_sleds =
            sled_inventory.load_bootstrap_sleds(config.bootstrap_slots)?;

        let mut bgp_auth_keys = BgpAuthKeys::default();
        let new_bgp_auth_key_ids =
            config.rack_network_config.get_bgp_auth_key_ids();
        bgp_auth_keys.sync_keys(new_bgp_auth_key_ids);

        Ok(Self {
            inventory: sled_inventory,
            bootstrap_sleds,
            rack_network_config: config.rack_network_config,
            bgp_auth_keys,
            allowed_source_ips: config.allowed_source_ips,
        })
    }
}

impl From<&'_ CurrentMultirackJoinConfig> for CurrentMultirackJoinUserConfig {
    fn from(config: &'_ CurrentMultirackJoinConfig) -> Self {
        // If the user has selected bootstrap sleds, use those; otherwise,
        // default to the full inventory list.
        let bootstrap_sleds = if !config.bootstrap_sleds.is_empty() {
            config.bootstrap_sleds.clone()
        } else {
            config.inventory.sleds.clone()
        };

        CurrentMultirackJoinUserConfig {
            bootstrap_sleds,
            rack_network_config: config.rack_network_config.clone(),
            allowed_source_ips: config.allowed_source_ips.clone(),
            bgp_auth_keys: GetBgpAuthKeyInfoResponse {
                data: config.get_bgp_auth_key_data(),
            },
        }
    }
}
