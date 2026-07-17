// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! User provided dropshot server context

use crate::MgsHandle;
use crate::bgp_auth_keys::BgpAuthKeyError;
use crate::bgp_auth_keys::BgpAuthKeys;
use crate::bootstrap_addrs::BootstrapPeersFromDdm;
use crate::multirack_config::CurrentMultirackJoinConfig;
use crate::preflight_check::PreflightCheckerHandler;
use crate::rss_config::CurrentRssConfig;
use crate::transceivers::Handle as TransceiverHandle;
use crate::update_tracker::UpdateTracker;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use dropshot::ClientErrorStatusCode;
use dropshot::HttpError;
use internal_dns_resolver::Resolver;
use sled_hardware_types::Baseboard;
use slog::info;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::mem;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use wicket_common::inventory::MgsV1Inventory;
use wicket_common::inventory::SledInventory;
use wicket_common::inventory::SpIdentifier;
use wicket_common::rack_setup::BgpAuthKey;
use wicket_common::rack_setup::BgpAuthKeyStatus;
use wicket_common::rack_setup::BootstrapSledDescription;
use wicketd_api::SetBgpAuthKeyStatus;
use wicketd_commission_types::rack_setup::BgpAuthKeyId;

#[derive(Default)]
pub(crate) struct RssOrMultirackJoinConfigCommon {
    pub inventory: SledInventory,
    pub bootstrap_sleds: BTreeSet<BootstrapSledDescription>,
    pub bgp_auth_keys: BgpAuthKeys,
}

impl RssOrMultirackJoinConfigCommon {
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

    pub(crate) fn update_sled_inventory(
        &mut self,
        inventory: &MgsV1Inventory,
        ddm_discovered_sleds: &BTreeMap<Baseboard, Ipv6Addr>,
        log: &slog::Logger,
    ) {
        self.inventory =
            SledInventory::new(inventory, &ddm_discovered_sleds, log);
    }

    pub(crate) fn update(
        &mut self,
        bootstrap_slots: &BTreeSet<u16>,
        new_bgp_auth_key_ids: impl IntoIterator<Item = BgpAuthKeyId>,
        our_baseboard: Option<&Baseboard>,
        inventory: &MgsV1Inventory,
        ddm_discovered_sleds: &BTreeMap<Baseboard, Ipv6Addr>,
        log: &slog::Logger,
    ) -> Result<(), String> {
        self.update_sled_inventory(inventory, ddm_discovered_sleds, log);

        // Updating can only fail in two ways:
        //
        // 1. If we have a real gimlet baseboard, that baseboard must be present
        //    in our inventory and in `value`'s list of sleds: we cannot exclude
        //    ourself from the rack.
        // 2. `value`'s bootstrap sleds includes sleds that aren't in our
        //    `inventory`.

        // First, confirm we have ourself in the inventory _and_ the user didn't
        // remove us from the list.
        self.inventory.verify_our_baseboard_is_in_inventory_slot(
            bootstrap_slots,
            our_baseboard,
        )?;
        // Next, confirm the user's list only consists of sleds in our
        // inventory and return those sleds.
        self.bootstrap_sleds = self
            .inventory
            .load_bootstrap_sleds_by_user_chosen_slots(bootstrap_slots)?;

        self.bgp_auth_keys.sync_keys(new_bgp_auth_key_ids);

        Ok(())
    }

    pub(crate) fn update_ip_addresses_for_existing_bootstrap_sleds(
        &mut self,
        ddm_discovered_sleds: &BTreeMap<Baseboard, Ipv6Addr>,
    ) {
        // If the user has already uploaded a config specifying bootstrap_sleds,
        // also update our knowledge of those sleds' bootstrap addresses as
        // learned via DDM.
        let our_bootstrap_sleds = mem::take(&mut self.bootstrap_sleds);
        self.bootstrap_sleds = our_bootstrap_sleds
            .into_iter()
            .map(|mut sled_desc| {
                sled_desc.bootstrap_ip =
                    ddm_discovered_sleds.get(&sled_desc.baseboard).copied();
                sled_desc
            })
            .collect();
    }

    pub(crate) fn get_latest<'a, T: CommonConfigContainer>(
        config: &'a mut T,
        inventory: &MgsV1Inventory,
        ddm_discovered_sleds: &BTreeMap<Baseboard, Ipv6Addr>,
        log: &slog::Logger,
    ) -> &'a T {
        config.common_mut().update_sled_inventory(
            inventory,
            ddm_discovered_sleds,
            log,
        );
        config.common_mut().update_ip_addresses_for_existing_bootstrap_sleds(
            ddm_discovered_sleds,
        );
        config
    }
}

pub(crate) trait CommonConfigContainer {
    fn common_mut(&mut self) -> &mut RssOrMultirackJoinConfigCommon;

    fn get_latest(
        &mut self,
        inventory: &MgsV1Inventory,
        ddm_discovered_sleds: &BTreeMap<Baseboard, Ipv6Addr>,
        log: &slog::Logger,
    ) -> &Self
    where
        Self: Sized,
    {
        RssOrMultirackJoinConfigCommon::get_latest(
            self,
            inventory,
            ddm_discovered_sleds,
            log,
        )
    }
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum RssOrMultirackJoinConfig {
    Rss(CurrentRssConfig),
    MultirackJoin(CurrentMultirackJoinConfig),
}

impl Default for RssOrMultirackJoinConfig {
    fn default() -> Self {
        // Backwards compatibility
        Self::default_rss_config()
    }
}

impl RssOrMultirackJoinConfig {
    pub fn default_rss_config() -> RssOrMultirackJoinConfig {
        Self::Rss(CurrentRssConfig::default())
    }

    /// Return the [`CurrentRssConfig`] from the `Rss` variant if set.
    pub fn rss_config_mut(&mut self) -> Option<&mut CurrentRssConfig> {
        match self {
            Self::Rss(c) => Some(c),
            _ => None,
        }
    }

    /// Return the [`CurrentRssConfig`] from the `Rss` variant if set or return
    /// a 409 CONFLICT HTTP error with the given message.
    pub fn rss_config_mut_or_conflict(
        &mut self,
        msg: impl Into<String>,
    ) -> Result<&mut CurrentRssConfig, HttpError> {
        self.rss_config_mut().ok_or_else(|| {
            HttpError::for_client_error(
                Some("Conflict".to_string()),
                ClientErrorStatusCode::CONFLICT,
                msg.into(),
            )
        })
    }

    /// Return the [`CurrentMultirackJoinConfig`] from the `MultirackJoin`
    /// variant if set.
    pub fn multirack_join_config_mut(
        &mut self,
    ) -> Option<&mut CurrentMultirackJoinConfig> {
        match self {
            Self::MultirackJoin(c) => Some(c),
            _ => None,
        }
    }

    /// Return the a mutable reference to the `Rss` variant if set or initialize
    /// `self` with `CurrentRssConfig::default()` and return a mutable reference
    /// to that.
    ///
    /// This is essentially an entry API that overwrites other variants as necessary.
    pub fn rss_config_mut_or_default(&mut self) -> &mut CurrentRssConfig {
        match self {
            Self::Rss(c) => c,
            _ => {
                *self = Self::default_rss_config();
                self.rss_config_mut().unwrap()
            }
        }
    }

    pub(crate) fn check_bgp_auth_keys_valid<'a>(
        &self,
        check_valid: impl IntoIterator<Item = &'a BgpAuthKeyId>,
    ) -> Result<(), BgpAuthKeyError> {
        match self {
            Self::Rss(c) => c.common.check_bgp_auth_keys_valid(check_valid),
            Self::MultirackJoin(c) => {
                c.common.check_bgp_auth_keys_valid(check_valid)
            }
        }
    }

    pub(crate) fn get_bgp_auth_key_data(
        &self,
    ) -> BTreeMap<BgpAuthKeyId, BgpAuthKeyStatus> {
        match self {
            Self::Rss(c) => c.common.get_bgp_auth_key_data(),
            Self::MultirackJoin(c) => c.common.get_bgp_auth_key_data(),
        }
    }

    pub(crate) fn set_bgp_auth_key(
        &mut self,
        key_id: BgpAuthKeyId,
        key: BgpAuthKey,
    ) -> Result<SetBgpAuthKeyStatus, BgpAuthKeyError> {
        match self {
            Self::Rss(c) => c.common.set_bgp_auth_key(key_id, key),
            Self::MultirackJoin(c) => c.common.set_bgp_auth_key(key_id, key),
        }
    }
}

/// Shared state used by API handlers
pub struct ServerContext {
    /// The main wicketd API address, stored as `config.address` in the SMF
    /// configuration.
    ///
    /// This address is used to reject SMF updates which attempt to change it.
    pub(crate) bind_address: SocketAddrV6,
    // TODO-RAINCLAUDE: parallel to bind_address; the commission API address
    // TODO-RAINCLAUDE: (config.commission-address), also rejected on SMF refresh.
    pub(crate) commission_bind_address: SocketAddrV6,
    pub mgs_handle: MgsHandle,
    pub mgs_client: gateway_client::Client,
    pub transceiver_handle: TransceiverHandle,
    pub(crate) log: slog::Logger,
    /// Our cached copy of what MGS's `/local/switch-id` endpoint returns; it
    /// identifies whether we're connected to switch 0 or 1 and cannot change
    /// (plugging us into a different switch would require powering off our sled
    /// and physically moving it).
    pub(crate) local_switch_id: OnceLock<SpIdentifier>,
    pub(crate) bootstrap_peers: BootstrapPeersFromDdm,
    pub(crate) update_tracker: Arc<UpdateTracker>,
    pub(crate) baseboard: Option<Baseboard>,
    pub(crate) rss_or_multirack_join_config: Mutex<RssOrMultirackJoinConfig>,
    pub(crate) preflight_checker: PreflightCheckerHandler,
    pub(crate) internal_dns_resolver: Arc<Mutex<Option<Resolver>>>,
}

impl ServerContext {
    pub(crate) fn bootstrap_agent_lockstep_addr(&self) -> Result<SocketAddrV6> {
        // Port on which the bootstrap agent lockstep API dropshot server within
        // sled-agent is listening.
        const BOOTSTRAP_AGENT_LOCKSTEP_PORT: u16 = 8080;

        let ip = self.bootstrap_agent_ip()?;
        Ok(SocketAddrV6::new(ip, BOOTSTRAP_AGENT_LOCKSTEP_PORT, 0, 0))
    }

    fn bootstrap_agent_ip(&self) -> Result<Ipv6Addr> {
        let mut any_bootstrap_peer = None;
        for (baseboard, ip) in self.bootstrap_peers.sleds() {
            if self.baseboard.as_ref() == Some(&baseboard) {
                return Ok(ip);
            }
            any_bootstrap_peer = Some((baseboard, ip));
        }

        // If we get past the loop above, we did not find a match for our
        // baseboard in our list of peers. If we know our own baseboard, this is
        // an error: we didn't find ourself. If we don't know our own
        // baseboard, we can pick any IP.
        if let Some(baseboard) = self.baseboard.as_ref() {
            bail!("IP address not known for our own sled ({baseboard:?})");
        } else {
            let (baseboard, ip) = any_bootstrap_peer
                .ok_or_else(|| anyhow!("no bootstrap agent peers found"))?;
            info!(
                self.log,
                "Baseboard unknown; choosing arbitrary bootstrap peer as 'our' sled-agent";
                "peer_baseboard" => ?baseboard,
                "peer_ip" => %ip,
            );
            Ok(ip)
        }
    }

    pub(crate) async fn local_switch_id(&self) -> Option<SpIdentifier> {
        // Do we already have it cached from a previous invocation?
        if let Some(&switch_id) = self.local_switch_id.get() {
            return Some(switch_id);
        }

        // We don't have a cached switch ID; try to fetch it from MGS. We
        // might be racing ourself (if this function is being called multiple
        // times concurrently), but that's fine: all invocations can query MGS,
        // and only one will succeed in setting the cache.
        match self.mgs_client.sp_local_switch_id().await {
            Ok(response) => {
                let switch_id = response.into_inner();

                // Ignore failures on set - that just means we lost the race and
                // another concurrent call to us already set it.
                //
                // However, we do need to notify the transceiver-fetching task
                // that we've learned our ID.
                if self.local_switch_id.set(switch_id).is_ok() {
                    self.transceiver_handle.set_local_switch_id(switch_id);
                }

                Some(switch_id)
            }
            Err(err) => {
                slog::warn!(
                    self.log,
                    "Failed to fetch local switch ID from MGS";
                    "err" => #%err,
                );
                None
            }
        }
    }
}
