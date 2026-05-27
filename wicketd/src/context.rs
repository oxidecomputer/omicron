// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! User provided dropshot server context

use crate::MgsHandle;
use crate::bgp_auth_keys::BgpAuthKeyError;
use crate::bootstrap_addrs::BootstrapPeers;
use crate::multirack_config::CurrentMultirackJoinConfig;
use crate::preflight_check::PreflightCheckerHandler;
use crate::rss_config::CurrentRssConfig;
use crate::transceivers::Handle as TransceiverHandle;
use crate::update_tracker::UpdateTracker;
use anyhow::Result;
use internal_dns_resolver::Resolver;
use sled_hardware_types::BaseboardId;
use std::collections::BTreeMap;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use wicket_common::inventory::SpIdentifier;
use wicket_common::rack_setup::BgpAuthKey;
use wicket_common::rack_setup::BgpAuthKeyId;
use wicket_common::rack_setup::BgpAuthKeyStatus;
use wicketd_api::SetBgpAuthKeyStatus;

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
            Self::Rss(c) => c.check_bgp_auth_keys_valid(check_valid),
            Self::MultirackJoin(c) => c.check_bgp_auth_keys_valid(check_valid),
        }
    }

    pub(crate) fn get_bgp_auth_key_data(
        &self,
    ) -> BTreeMap<BgpAuthKeyId, BgpAuthKeyStatus> {
        match self {
            Self::Rss(c) => c.get_bgp_auth_key_data(),
            Self::MultirackJoin(c) => c.get_bgp_auth_key_data(),
        }
    }

    pub(crate) fn set_bgp_auth_key(
        &mut self,
        key_id: BgpAuthKeyId,
        key: BgpAuthKey,
    ) -> Result<SetBgpAuthKeyStatus, BgpAuthKeyError> {
        match self {
            Self::Rss(c) => c.set_bgp_auth_key(key_id, key),
            Self::MultirackJoin(c) => c.set_bgp_auth_key(key_id, key),
        }
    }
}

/// Shared state used by API handlers
pub struct ServerContext {
    pub(crate) bind_address: SocketAddrV6,
    pub mgs_handle: MgsHandle,
    pub mgs_client: gateway_client::Client,
    pub transceiver_handle: TransceiverHandle,
    pub(crate) log: slog::Logger,
    /// Our cached copy of what MGS's `/local/switch-id` endpoint returns; it
    /// identifies whether we're connected to switch 0 or 1 and cannot change
    /// (plugging us into a different switch would require powering off our sled
    /// and physically moving it).
    pub(crate) local_switch_id: OnceLock<SpIdentifier>,
    pub(crate) bootstrap_agent_lockstep_address: SocketAddrV6,
    pub(crate) bootstrap_peers: BootstrapPeers,
    pub(crate) update_tracker: Arc<UpdateTracker>,
    pub(crate) baseboard_id: BaseboardId,
    pub(crate) rss_or_multirack_join_config: Mutex<RssOrMultirackJoinConfig>,
    pub(crate) preflight_checker: PreflightCheckerHandler,
    pub(crate) internal_dns_resolver: Arc<Mutex<Option<Resolver>>>,
}

impl ServerContext {
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
