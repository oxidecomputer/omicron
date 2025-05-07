// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for user-provided RSS configuration options.

use crate::bootstrap_addrs::BootstrapPeers;
use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use bootstrap_agent_client::types::BootstrapAddressDiscovery;
use bootstrap_agent_client::types::Certificate;
use bootstrap_agent_client::types::Name;
use bootstrap_agent_client::types::PortConfigV2 as BaPortConfigV2;
use bootstrap_agent_client::types::RackInitializeRequest;
use bootstrap_agent_client::types::RecoverySiloConfig;
use bootstrap_agent_client::types::UserId;
use display_error_chain::DisplayErrorChain;
use omicron_certificates::CertificateError;
use omicron_common::address;
use omicron_common::address::Ipv4Range;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::RACK_PREFIX;
use omicron_common::api::external::AllowedSourceIps;
use omicron_common::api::external::SwitchLocation;
use sled_hardware_types::Baseboard;
use slog::debug;
use slog::warn;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::btree_map;
use std::mem;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::LazyLock;
use thiserror::Error;
use wicket_common::inventory::MgsV1Inventory;
use wicket_common::inventory::SpType;
use wicket_common::rack_setup::BgpAuthKey;
use wicket_common::rack_setup::BgpAuthKeyId;
use wicket_common::rack_setup::BgpAuthKeyStatus;
use wicket_common::rack_setup::BootstrapSledDescription;
use wicket_common::rack_setup::CurrentRssUserConfigInsensitive;
use wicket_common::rack_setup::DisplaySlice;
use wicket_common::rack_setup::GetBgpAuthKeyInfoResponse;
use wicket_common::rack_setup::PutRssUserConfigInsensitive;
use wicket_common::rack_setup::UserSpecifiedPortConfig;
use wicket_common::rack_setup::UserSpecifiedRackNetworkConfig;
use wicketd_api::CertificateUploadResponse;
use wicketd_api::CurrentRssUserConfig;
use wicketd_api::CurrentRssUserConfigSensitive;
use wicketd_api::SetBgpAuthKeyStatus;

// TODO-correctness For now, we always use the same rack subnet when running
// RSS. When we get to multirack, this will be wrong, but there are many other
// RSS-related things that need to change then too.
static RACK_SUBNET: LazyLock<Ipv6Subnet<RACK_PREFIX>> = LazyLock::new(|| {
    let ip = Ipv6Addr::new(0xfd00, 0x1122, 0x3344, 0x0100, 0, 0, 0, 0);
    Ipv6Subnet::new(ip)
});

const RECOVERY_SILO_NAME: &str = "recovery";
const RECOVERY_SILO_USERNAME: &str = "recovery";

#[derive(Default)]
struct PartialCertificate {
    cert: Option<String>,
    key: Option<String>,
}

/// An analogue to `RackInitializeRequest`, but with optional fields to allow
/// the user to fill it in piecemeal.
#[derive(Default)]
pub(crate) struct CurrentRssConfig {
    inventory: BTreeSet<BootstrapSledDescription>,

    bootstrap_sleds: BTreeSet<BootstrapSledDescription>,
    ntp_servers: Vec<String>,
    dns_servers: Vec<IpAddr>,
    internal_services_ip_pool_ranges: Vec<address::IpRange>,
    external_dns_ips: Vec<IpAddr>,
    external_dns_zone_name: String,
    external_certificates: Vec<Certificate>,
    recovery_silo_password_hash: Option<omicron_passwords::NewPasswordHash>,
    rack_network_config: Option<UserSpecifiedRackNetworkConfig>,
    // BGP auth keys are identified by the key ID. It is an invariant that any
    // key IDs defined in `rack_network_config` exist here.
    //
    // Currently these are always TCP-MD5 keys,
    bgp_auth_keys: BTreeMap<BgpAuthKeyId, Option<BgpAuthKey>>,
    allowed_source_ips: Option<AllowedSourceIps>,
    // External certificates are uploaded in two separate actions (cert then
    // key, or vice versa). Here we store a partial certificate; once we have
    // both parts, we validate it and promote it to be a member of
    // external_certificates.
    partial_external_certificate: PartialCertificate,
}

impl CurrentRssConfig {
    pub(crate) fn dns_servers(&self) -> &[IpAddr] {
        &self.dns_servers
    }

    pub(crate) fn ntp_servers(&self) -> &[String] {
        &self.ntp_servers
    }

    pub(crate) fn user_specified_rack_network_config(
        &self,
    ) -> Option<&UserSpecifiedRackNetworkConfig> {
        self.rack_network_config.as_ref()
    }

    pub(crate) fn update_with_inventory_and_bootstrap_peers(
        &mut self,
        inventory: &MgsV1Inventory,
        bootstrap_peers: &BootstrapPeers,
        log: &slog::Logger,
    ) {
        let bootstrap_sleds = bootstrap_peers.sleds();

        self.inventory = inventory
            .sps
            .iter()
            .filter_map(|sp| {
                if sp.id.type_ != SpType::Sled {
                    return None;
                }

                let Some(state) = sp.state.as_ref() else {
                    debug!(
                        log,
                        "in update_with_inventory_and_bootstrap_peers, \
                         filtering out SP with no state";
                        "sp" => ?sp,
                    );
                    return None;
                };
                let baseboard = Baseboard::new_gimlet(
                    state.serial_number.clone(),
                    state.model.clone(),
                    state.revision,
                );
                let bootstrap_ip = bootstrap_sleds.get(&baseboard).copied();
                Some(BootstrapSledDescription {
                    id: sp.id,
                    baseboard,
                    bootstrap_ip,
                })
            })
            .collect();

        // If the user has already uploaded a config specifying bootstrap_sleds,
        // also update our knowledge of those sleds' bootstrap addresses.
        let our_bootstrap_sleds = mem::take(&mut self.bootstrap_sleds);
        self.bootstrap_sleds = our_bootstrap_sleds
            .into_iter()
            .map(|mut sled_desc| {
                sled_desc.bootstrap_ip =
                    bootstrap_sleds.get(&sled_desc.baseboard).copied();
                sled_desc
            })
            .collect();
    }

    pub(crate) fn start_rss_request(
        &mut self,
        bootstrap_peers: &BootstrapPeers,
        log: &slog::Logger,
    ) -> Result<RackInitializeRequest> {
        // Basic "client-side" checks.
        //
        // TODO: Instead, we should collect a list of failed checks that we can
        // send down to the client, and that the client can then display as
        // action items.
        if self.bootstrap_sleds.is_empty() {
            bail!("bootstrap_sleds is empty (have you uploaded a config?)");
        }
        if self.ntp_servers.is_empty() {
            bail!("at least one NTP server is required");
        }
        if self.dns_servers.is_empty() {
            bail!("at least one DNS server is required");
        }
        if self.internal_services_ip_pool_ranges.is_empty() {
            bail!("at least one internal services IP pool range is required");
        }
        if self.external_dns_ips.is_empty() {
            bail!("at least one external DNS IP address is required");
        }
        if self.external_dns_zone_name.is_empty() {
            bail!("external dns zone name is required");
        }
        if self.external_certificates.is_empty() {
            bail!("at least one certificate/key pair is required");
        }

        // We validated all the external certs as they were uploaded, but if we
        // didn't yet have our `external_dns_zone_name` that validation would've
        // skipped checking the hostname. Repeat validation on all certs now
        // that we definitely have it.
        let cert_validator =
            CertificateValidator::new(Some(&self.external_dns_zone_name));
        for (i, pair) in self.external_certificates.iter().enumerate() {
            if let Err(err) = cert_validator
                .validate(&pair.cert, &pair.key)
                .with_context(|| {
                    let i = i + 1;
                    let tot = self.external_certificates.len();
                    format!("certificate {i} of {tot} is invalid")
                })
            {
                // Remove the invalid cert prior to returning.
                self.external_certificates.remove(i);
                return Err(err);
            }
        }

        let Some(recovery_silo_password_hash) =
            self.recovery_silo_password_hash.as_ref()
        else {
            bail!("recovery password not yet set");
        };
        let Some(rack_network_config) = self.rack_network_config.as_ref()
        else {
            bail!("rack network config not set (have you uploaded a config?)");
        };
        let rack_network_config = validate_rack_network_config(
            rack_network_config,
            &self.bgp_auth_keys,
        )?;

        let known_bootstrap_sleds = bootstrap_peers.sleds();
        let mut bootstrap_ips = Vec::new();
        for sled in &self.bootstrap_sleds {
            let Some(ip) = known_bootstrap_sleds.get(&sled.baseboard).copied()
            else {
                bail!(
                    "IP address not (yet?) known for sled {} ({:?})",
                    sled.id.slot,
                    sled.baseboard,
                );
            };
            bootstrap_ips.push(ip);
        }

        // LRTQ requires at least 3 sleds
        //
        // TODO: Warn users in the wicket UI if they are configuring
        // a small rack cluster that does not support trust quorum.
        // https://github.com/oxidecomputer/omicron/issues/3690
        const TRUST_QUORUM_MIN_SIZE: usize = 3;
        let trust_quorum_peers: Option<
            Vec<bootstrap_agent_client::types::Baseboard>,
        > = if self.bootstrap_sleds.len() >= TRUST_QUORUM_MIN_SIZE {
            Some(
                self.bootstrap_sleds
                    .iter()
                    .map(|sled| sled.baseboard.clone().into())
                    .collect(),
            )
        } else {
            warn!(
                log,
                "Trust quorum disabled: requires at least {} sleds",
                TRUST_QUORUM_MIN_SIZE
            );
            None
        };

        // Convert between internal and progenitor types.
        let user_password_hash = bootstrap_agent_client::types::NewPasswordHash(
            recovery_silo_password_hash.to_string(),
        );
        let internal_services_ip_pool_ranges = self
            .internal_services_ip_pool_ranges
            .iter()
            .map(|pool| {
                use bootstrap_agent_client::types::IpRange;
                use bootstrap_agent_client::types::Ipv4Range;
                use bootstrap_agent_client::types::Ipv6Range;
                match pool {
                    address::IpRange::V4(range) => IpRange::V4(Ipv4Range {
                        first: range.first,
                        last: range.last,
                    }),
                    address::IpRange::V6(range) => IpRange::V6(Ipv6Range {
                        first: range.first,
                        last: range.last,
                    }),
                }
            })
            .collect();

        let request = RackInitializeRequest {
            trust_quorum_peers,
            bootstrap_discovery: BootstrapAddressDiscovery::OnlyThese(
                bootstrap_ips,
            ),
            ntp_servers: self.ntp_servers.clone(),
            dns_servers: self.dns_servers.clone(),
            internal_services_ip_pool_ranges,
            external_dns_ips: self.external_dns_ips.clone(),
            external_dns_zone_name: self.external_dns_zone_name.clone(),
            external_certificates: self.external_certificates.clone(),
            recovery_silo: RecoverySiloConfig {
                silo_name: Name::try_from(RECOVERY_SILO_NAME).unwrap(),
                user_name: UserId::try_from(RECOVERY_SILO_USERNAME).unwrap(),
                user_password_hash,
            },
            rack_network_config,
            allowed_source_ips: self
                .allowed_source_ips
                .clone()
                .unwrap_or(AllowedSourceIps::Any),
            // Reserve a set amount of space for non-Crucible (i.e. control
            // plane) storage. See oxidecomputer/omicron#7875 for the size
            // determination.
            control_plane_storage_buffer_gib: 250,
        };

        Ok(request)
    }

    pub(crate) fn set_recovery_user_password_hash(
        &mut self,
        hash: omicron_passwords::NewPasswordHash,
    ) {
        self.recovery_silo_password_hash = Some(hash);
    }

    pub(crate) fn push_cert(
        &mut self,
        cert: String,
    ) -> Result<CertificateUploadResponse, String> {
        self.partial_external_certificate.cert = Some(cert);
        self.maybe_promote_external_certificate()
    }

    pub(crate) fn push_key(
        &mut self,
        key: String,
    ) -> Result<CertificateUploadResponse, String> {
        self.partial_external_certificate.key = Some(key);
        self.maybe_promote_external_certificate()
    }

    fn maybe_promote_external_certificate(
        &mut self,
    ) -> Result<CertificateUploadResponse, String> {
        // If we're still waiting on either the cert or the key, we have nothing
        // to do (but this isn't an error).
        let (cert, key) = match (
            self.partial_external_certificate.cert.as_ref(),
            self.partial_external_certificate.key.as_ref(),
        ) {
            (Some(cert), Some(key)) => (cert, key),
            (None, Some(_)) => {
                return Ok(CertificateUploadResponse::WaitingOnCert);
            }
            (Some(_), None) => {
                return Ok(CertificateUploadResponse::WaitingOnKey);
            }
            // We are only called by `push_key` or `push_cert`; one or the other
            // must be `Some(_)`.
            (None, None) => unreachable!(),
        };

        // We store `external_dns_zone_name` as a `String` for simpler TOML
        // parsing, but we want to convert an empty string to an option here so
        // we don't reject certs if the external DNS zone name hasn't been set
        // yet.
        let external_dns_zone_name = if self.external_dns_zone_name.is_empty() {
            None
        } else {
            Some(self.external_dns_zone_name.as_str())
        };

        // If the certificate is invalid, clear out the cert and key before
        // returning an error.
        if let Err(err) = CertificateValidator::new(external_dns_zone_name)
            .validate(cert, key)
        {
            self.partial_external_certificate.cert = None;
            self.partial_external_certificate.key = None;
            return Err(DisplayErrorChain::new(&err).to_string());
        }

        // Cert and key appear to be valid; steal them out of
        // `partial_external_certificate` and promote them to
        // `external_certificates`.
        self.external_certificates.push(Certificate {
            cert: self.partial_external_certificate.cert.take().unwrap(),
            key: self.partial_external_certificate.key.take().unwrap(),
        });

        Ok(CertificateUploadResponse::CertKeyAccepted)
    }

    pub(crate) fn check_bgp_auth_keys_valid<'a>(
        &self,
        check_valid: impl IntoIterator<Item = &'a BgpAuthKeyId>,
    ) -> Result<(), BgpAuthKeyError> {
        if self.rack_network_config.is_none() {
            return Err(BgpAuthKeyError::RackNetworkConfigNotSet);
        }

        let not_found: Vec<_> = check_valid
            .into_iter()
            .filter(|key_id| !self.bgp_auth_keys.contains_key(key_id))
            .cloned()
            .collect();
        if !not_found.is_empty() {
            return Err(self.make_bgp_key_ids_not_found_error(not_found));
        }

        Ok(())
    }

    pub(crate) fn get_bgp_auth_key_data(
        &self,
    ) -> BTreeMap<BgpAuthKeyId, BgpAuthKeyStatus> {
        self.bgp_auth_keys
            .iter()
            .map(|(key_id, key)| {
                let status = key
                    .as_ref()
                    .map(|key| BgpAuthKeyStatus::Set { info: key.info() })
                    .unwrap_or(BgpAuthKeyStatus::Unset);
                (key_id.clone(), status)
            })
            .collect()
    }

    pub(crate) fn set_bgp_auth_key(
        &mut self,
        key_id: BgpAuthKeyId,
        key: BgpAuthKey,
    ) -> Result<SetBgpAuthKeyStatus, BgpAuthKeyError> {
        if self.rack_network_config.is_none() {
            return Err(BgpAuthKeyError::RackNetworkConfigNotSet);
        }

        match self.bgp_auth_keys.entry(key_id.clone()) {
            btree_map::Entry::Occupied(mut entry) => {
                match entry.get() {
                    Some(old_key) if old_key == &key => {
                        Ok(SetBgpAuthKeyStatus::Unchanged)
                    }
                    Some(_) => {
                        entry.insert(Some(key));
                        Ok(SetBgpAuthKeyStatus::Replaced)
                    }
                    None => {
                        // This is a new key; we don't have it yet.
                        entry.insert(Some(key));
                        Ok(SetBgpAuthKeyStatus::Added)
                    }
                }
            }
            btree_map::Entry::Vacant(_) => {
                Err(self.make_bgp_key_ids_not_found_error(vec![key_id]))
            }
        }
    }

    #[must_use]
    fn make_bgp_key_ids_not_found_error(
        &self,
        key_ids: Vec<BgpAuthKeyId>,
    ) -> BgpAuthKeyError {
        let valid_key_ids = self.bgp_auth_keys.keys().cloned().collect();
        BgpAuthKeyError::KeyIdsNotFound {
            not_found: key_ids,
            valid_keys: valid_key_ids,
        }
    }

    pub(crate) fn update(
        &mut self,
        value: PutRssUserConfigInsensitive,
        our_baseboard: Option<&Baseboard>,
    ) -> Result<(), String> {
        // Updating can only fail in two ways:
        //
        // 1. If we have a real gimlet baseboard, that baseboard must be present
        //    in our inventory and in `value`'s list of sleds: we cannot exclude
        //    ourself from the rack.
        // 2. `value`'s bootstrap sleds includes sleds that aren't in our
        //    `inventory`.

        // First, confirm we have ourself in the inventory _and_ the user didn't
        // remove us from the list.
        if let Some(our_baseboard @ Baseboard::Gimlet { .. }) = our_baseboard {
            let our_slot = self
                .inventory
                .iter()
                .find_map(|sled| {
                    if sled.baseboard == *our_baseboard {
                        Some(sled.id.slot)
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    format!(
                        "Inventory is missing the scrimlet where wicketd is \
                         running ({our_baseboard:?})",
                    )
                })?;
            if !value.bootstrap_sleds.contains(&our_slot) {
                return Err(format!(
                    "Cannot remove the scrimlet where wicketd is running \
                     (sled {our_slot}: {our_baseboard:?}) \
                     from bootstrap_sleds"
                ));
            }
        }

        // Next, confirm the user's list only consists of sleds in our
        // inventory.
        let mut bootstrap_sleds = BTreeSet::new();
        for slot in value.bootstrap_sleds {
            let sled =
                self.inventory
                    .iter()
                    .find(|sled| sled.id.slot == slot)
                    .ok_or_else(|| {
                        format!(
                            "cannot add unknown sled {slot} to bootstrap_sleds",
                        )
                    })?;
            bootstrap_sleds.insert(sled.clone());
        }

        self.bootstrap_sleds = bootstrap_sleds;
        self.ntp_servers = value.ntp_servers;
        self.dns_servers = value.dns_servers;
        self.internal_services_ip_pool_ranges =
            value.internal_services_ip_pool_ranges;
        self.external_dns_ips = value.external_dns_ips;
        self.external_dns_zone_name = value.external_dns_zone_name;
        self.allowed_source_ips = Some(value.allowed_source_ips);

        // Build a new auth key map, dropping all old keys from the map.
        let new_bgp_auth_key_ids =
            value.rack_network_config.get_bgp_auth_key_ids();
        let mut old_bgp_auth_keys = std::mem::take(&mut self.bgp_auth_keys);
        let new_bgp_auth_keys = new_bgp_auth_key_ids
            .into_iter()
            .map(|key_id| {
                (
                    key_id.clone(),
                    // For each new key, either grab the corresponding old key,
                    // or initialize to None.
                    old_bgp_auth_keys.remove(&key_id).unwrap_or_else(|| None),
                )
            })
            .collect();
        self.bgp_auth_keys = new_bgp_auth_keys;

        self.rack_network_config = Some(value.rack_network_config);

        Ok(())
    }
}

impl From<&'_ CurrentRssConfig> for CurrentRssUserConfig {
    fn from(rss: &CurrentRssConfig) -> Self {
        // If the user has selected bootstrap sleds, use those; otherwise,
        // default to the full inventory list.
        let bootstrap_sleds = if !rss.bootstrap_sleds.is_empty() {
            rss.bootstrap_sleds.clone()
        } else {
            rss.inventory.clone()
        };

        Self {
            sensitive: CurrentRssUserConfigSensitive {
                num_external_certificates: rss.external_certificates.len(),
                recovery_silo_password_set: rss
                    .recovery_silo_password_hash
                    .is_some(),
                bgp_auth_keys: GetBgpAuthKeyInfoResponse {
                    data: rss.get_bgp_auth_key_data(),
                },
            },
            insensitive: CurrentRssUserConfigInsensitive {
                bootstrap_sleds,
                ntp_servers: rss.ntp_servers.clone(),
                dns_servers: rss.dns_servers.clone(),
                internal_services_ip_pool_ranges: rss
                    .internal_services_ip_pool_ranges
                    .clone(),
                external_dns_ips: rss.external_dns_ips.clone(),
                external_dns_zone_name: rss.external_dns_zone_name.clone(),
                rack_network_config: rss.rack_network_config.clone(),
                allowed_source_ips: rss.allowed_source_ips.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(crate) enum BgpAuthKeyError {
    #[error(
        "rack network config not set (upload the config before setting a key)"
    )]
    RackNetworkConfigNotSet,

    #[error(
        "key IDs not found: {} (valid key IDs: {})",
        DisplaySlice(.not_found),
        DisplaySlice(.valid_keys),
    )]
    KeyIdsNotFound {
        not_found: Vec<BgpAuthKeyId>,
        valid_keys: Vec<BgpAuthKeyId>,
    },
}

fn validate_rack_network_config(
    config: &UserSpecifiedRackNetworkConfig,
    bgp_auth_keys: &BTreeMap<BgpAuthKeyId, Option<BgpAuthKey>>,
) -> Result<bootstrap_agent_client::types::RackNetworkConfigV2> {
    use bootstrap_agent_client::types::BgpConfig as BaBgpConfig;

    // Ensure that there is at least one uplink
    if !config.has_any_uplinks() {
        return Err(anyhow!("Must have at least one port configured"));
    }

    // Make sure `infra_ip_first`..`infra_ip_last` is a well-defined range...
    let infra_ip_range =
        Ipv4Range::new(config.infra_ip_first, config.infra_ip_last).map_err(
            |s: String| {
                anyhow!("invalid `infra_ip_first`, `infra_ip_last` range: {s}")
            },
        )?;

    // TODO this implies a single contiguous range for port IPs which is over
    // constraining
    // iterate through each port config
    for (_, _, port_config) in config.iter_uplinks() {
        for addr in &port_config.addresses {
            // ... and check that it contains `uplink_ip`.
            if addr.addr() < infra_ip_range.first
                || addr.addr() > infra_ip_range.last
            {
                bail!(
                    "`uplink_cidr`'s IP address must be in the range defined by \
                `infra_ip_first` and `infra_ip_last`"
                );
            }
        }
    }

    // Check that all auth keys are present.
    for (key_id, key) in bgp_auth_keys {
        if key.is_none() {
            bail!("No BGP MD5 auth key provided for key ID {key_id}");
        }
    }

    // TODO Add more client side checks on `rack_network_config` contents?

    Ok(bootstrap_agent_client::types::RackNetworkConfigV2 {
        rack_subnet: RACK_SUBNET.net(),
        infra_ip_first: config.infra_ip_first,
        infra_ip_last: config.infra_ip_last,
        ports: config
            .iter_uplinks()
            .map(|(switch, port, config)| {
                build_port_config(switch, port, config, bgp_auth_keys)
            })
            .collect(),
        bgp: config
            .bgp
            .iter()
            .map(|config| BaBgpConfig {
                asn: config.asn,
                originate: config.originate.clone(),
                checker: config.checker.clone(),
                shaper: config.shaper.clone(),
            })
            .collect(),
        //TODO bfd config in wicket
        bfd: vec![],
    })
}

/// Builds a `BaPortConfigV2` from a `UserSpecifiedPortConfig`.
///
/// Assumes that all auth keys are present in `bgp_auth_keys`.
fn build_port_config(
    switch: SwitchLocation,
    port: &str,
    config: &UserSpecifiedPortConfig,
    bgp_auth_keys: &BTreeMap<BgpAuthKeyId, Option<BgpAuthKey>>,
) -> BaPortConfigV2 {
    use bootstrap_agent_client::types::BgpPeerConfig as BaBgpPeerConfig;
    use bootstrap_agent_client::types::LldpAdminStatus as BaLldpAdminStatus;
    use bootstrap_agent_client::types::LldpPortConfig as BaLldpPortConfig;
    use bootstrap_agent_client::types::PortFec as BaPortFec;
    use bootstrap_agent_client::types::PortSpeed as BaPortSpeed;
    use bootstrap_agent_client::types::RouteConfig as BaRouteConfig;
    use bootstrap_agent_client::types::SwitchLocation as BaSwitchLocation;
    use bootstrap_agent_client::types::TxEqConfig as BaTxEqConfig;
    use bootstrap_agent_client::types::UplinkAddressConfig as BaUplinkAddressConfig;
    use omicron_common::api::internal::shared::LldpAdminStatus;
    use omicron_common::api::internal::shared::PortFec;
    use omicron_common::api::internal::shared::PortSpeed;

    BaPortConfigV2 {
        port: port.to_owned(),
        routes: config
            .routes
            .iter()
            .map(|r| BaRouteConfig {
                destination: r.destination,
                nexthop: r.nexthop,
                vlan_id: r.vlan_id,
                rib_priority: r.rib_priority,
            })
            .collect(),
        addresses: config
            .addresses
            .iter()
            .map(|a| BaUplinkAddressConfig {
                address: a.address,
                vlan_id: a.vlan_id,
            })
            .collect(),
        bgp_peers: config
            .bgp_peers
            .iter()
            .map(|p| {
                let md5_auth_key = p.auth_key_id.as_ref().map(|key_id| {
                    let BgpAuthKey::TcpMd5 { key } = bgp_auth_keys
                        .get(key_id)
                        .unwrap_or_else(|| {
                            panic!(
                                "invariant violation: auth key ID {} exists",
                                key_id
                            )
                        })
                        .clone()
                        .unwrap_or_else(|| {
                            panic!(
                                "invariant violation: auth key ID {} has a key",
                                key_id
                            )
                        });
                    key
                });

                BaBgpPeerConfig {
                    addr: p.addr,
                    asn: p.asn,
                    port: p.port.clone(),
                    hold_time: p.hold_time,
                    connect_retry: p.connect_retry,
                    delay_open: p.delay_open,
                    idle_hold_time: p.idle_hold_time,
                    keepalive: p.keepalive,
                    communities: Vec::new(),
                    enforce_first_as: p.enforce_first_as,
                    local_pref: p.local_pref,
                    md5_auth_key,
                    min_ttl: p.min_ttl,
                    multi_exit_discriminator: p.multi_exit_discriminator,
                    remote_asn: p.remote_asn,
                    allowed_export: p.allowed_export.clone().into(),
                    allowed_import: p.allowed_import.clone().into(),
                    vlan_id: p.vlan_id,
                }
            })
            .collect(),
        switch: match switch {
            SwitchLocation::Switch0 => BaSwitchLocation::Switch0,
            SwitchLocation::Switch1 => BaSwitchLocation::Switch1,
        },
        uplink_port_speed: match config.uplink_port_speed {
            PortSpeed::Speed0G => BaPortSpeed::Speed0G,
            PortSpeed::Speed1G => BaPortSpeed::Speed1G,
            PortSpeed::Speed10G => BaPortSpeed::Speed10G,
            PortSpeed::Speed25G => BaPortSpeed::Speed25G,
            PortSpeed::Speed40G => BaPortSpeed::Speed40G,
            PortSpeed::Speed50G => BaPortSpeed::Speed50G,
            PortSpeed::Speed100G => BaPortSpeed::Speed100G,
            PortSpeed::Speed200G => BaPortSpeed::Speed200G,
            PortSpeed::Speed400G => BaPortSpeed::Speed400G,
        },
        uplink_port_fec: config.uplink_port_fec.map(|fec| match fec {
            PortFec::Firecode => BaPortFec::Firecode,
            PortFec::None => BaPortFec::None,
            PortFec::Rs => BaPortFec::Rs,
        }),
        autoneg: config.autoneg,
        lldp: config.lldp.as_ref().map(|c| BaLldpPortConfig {
            status: match c.status {
                LldpAdminStatus::Enabled => BaLldpAdminStatus::Enabled,
                LldpAdminStatus::Disabled => BaLldpAdminStatus::Disabled,
                LldpAdminStatus::TxOnly => BaLldpAdminStatus::TxOnly,
                LldpAdminStatus::RxOnly => BaLldpAdminStatus::RxOnly,
            },
            chassis_id: c.chassis_id.clone(),
            port_id: c.port_id.clone(),
            system_name: c.system_name.clone(),
            system_description: c.system_description.clone(),
            port_description: c.port_description.clone(),
            management_addrs: c.management_addrs.clone(),
        }),
        tx_eq: config.tx_eq.as_ref().map(|c| BaTxEqConfig {
            pre1: c.pre1,
            pre2: c.pre2,
            main: c.main,
            post2: c.post2,
            post1: c.post1,
        }),
    }
}

// Thin wrapper around an `omicron_certificates::CertificateValidator` that we
// use both when certs are uploaded and when we start RSS.
struct CertificateValidator {
    inner: omicron_certificates::CertificateValidator,
    silo_dns_name: Option<String>,
}

impl CertificateValidator {
    fn new(external_dns_zone_name: Option<&str>) -> Self {
        let mut inner = omicron_certificates::CertificateValidator::default();

        // We are running in 1986! We're in the code path where the operator is
        // giving us NTP servers so we can find out the actual time, but any
        // validation we attempt now must ignore certificate expiration (and in
        // particular, we don't want to fail a "not before" check because we
        // think the cert is from the next century).
        inner.danger_disable_expiration_validation();

        // We validate certificates both when they are uploaded and just before
        // beginning RSS. In the former case we may not yet know our external
        // DNS name (e.g., if certs are uploaded before the config TOML that
        // provides the DNS name), but in the latter case we do.
        let silo_dns_name =
            external_dns_zone_name.map(|external_dns_zone_name| {
                format!("{RECOVERY_SILO_NAME}.sys.{external_dns_zone_name}",)
            });

        Self { inner, silo_dns_name }
    }

    fn validate(&self, cert: &str, key: &str) -> Result<(), CertificateError> {
        // Cert validation accepts multiple possible silo DNS names, but at rack
        // setup time we only have one. Stuff it into a Vec.
        let silo_dns_names =
            if let Some(silo_dns_name) = self.silo_dns_name.as_deref() {
                vec![silo_dns_name]
            } else {
                vec![]
            };
        self.inner.validate(cert.as_bytes(), key.as_bytes(), &silo_dns_names)
    }
}

#[cfg(test)]
mod tests {
    use wicket_common::example::ExampleRackSetupData;

    use super::*;

    #[test]
    fn test_bgp_auth_key_states() {
        let example = ExampleRackSetupData::non_empty();

        let mut current_config = CurrentRssConfig::default();

        // XXX: This is a hack -- ideally we'd go through the front door of
        // update_with_inventory_and_bootstrap_peers. But it works for now.
        current_config.inventory = example.inventory;

        current_config
            .update(
                example.put_insensitive.clone(),
                example.our_baseboard.as_ref(),
            )
            .expect("update of example data should succeed");

        // At this point, both BGP keys should be unset.
        let key_data = current_config.get_bgp_auth_key_data();
        assert_eq!(key_data.len(), 2);

        let key1 = example.bgp_auth_keys[0].clone();
        let key2 = example.bgp_auth_keys[1].clone();

        for key_id in &example.bgp_auth_keys {
            assert_eq!(key_data.get(key_id), Some(&BgpAuthKeyStatus::Unset));
        }

        let shared_key = BgpAuthKey::TcpMd5 { key: "shared-key".to_owned() };
        let new_key = BgpAuthKey::TcpMd5 { key: "new-key".to_owned() };

        // Add a key for the first key ID.
        {
            let status = current_config
                .set_bgp_auth_key(key1.clone(), shared_key.clone())
                .expect("setting key1 succeeded");
            assert_eq!(status, SetBgpAuthKeyStatus::Added);

            // Check that the key is now set.
            let key_data = current_config.get_bgp_auth_key_data();
            assert_eq!(
                key_data.get(&key1),
                Some(&BgpAuthKeyStatus::Set { info: shared_key.info() })
            );
        }

        // Try replacing the key with the same one.
        {
            let status = current_config
                .set_bgp_auth_key(key1.clone(), shared_key.clone())
                .expect("replacing key1 succeeded");
            assert_eq!(status, SetBgpAuthKeyStatus::Unchanged);
            let key_data = current_config.get_bgp_auth_key_data();
            assert_eq!(
                key_data.get(&key1),
                Some(&BgpAuthKeyStatus::Set { info: shared_key.info() })
            );
        }

        // Replace the key with a different one.
        {
            let status = current_config
                .set_bgp_auth_key(key1.clone(), new_key.clone())
                .expect("replacing key1 succeeded");
            assert_eq!(status, SetBgpAuthKeyStatus::Replaced);
            let key_data = current_config.get_bgp_auth_key_data();
            assert_eq!(
                key_data.get(&key1),
                Some(&BgpAuthKeyStatus::Set { info: new_key.info() })
            );
        }

        // Query and set a key that doesn't exist.
        {
            let does_not_exist: BgpAuthKeyId =
                "does-not-exist".parse().unwrap();
            let err =
                current_config.check_bgp_auth_keys_valid([&does_not_exist]);
            assert_eq!(
                err,
                Err(BgpAuthKeyError::KeyIdsNotFound {
                    not_found: vec![does_not_exist.clone()],
                    valid_keys: example.bgp_auth_keys.clone(),
                })
            );
            let err = current_config
                .set_bgp_auth_key(does_not_exist.clone(), shared_key.clone())
                .expect_err("setting a non-existent key should fail");
            assert_key_ids_not_found(
                err,
                vec![does_not_exist.clone()],
                example.bgp_auth_keys.clone(),
            );
        }

        // Set key2 as well.
        {
            let status = current_config
                .set_bgp_auth_key(key2.clone(), shared_key.clone())
                .expect("setting key2 succeeded");
            assert_eq!(status, SetBgpAuthKeyStatus::Added);
            let key_data = current_config.get_bgp_auth_key_data();
            assert_eq!(
                key_data.get(&key2),
                Some(&BgpAuthKeyStatus::Set { info: shared_key.info() })
            );
        }

        // Now generate a new example data without key2 and upload it.
        let example_data_2 = ExampleRackSetupData::one_bgp_peer();
        current_config
            .update(
                example_data_2.put_insensitive,
                example_data_2.our_baseboard.as_ref(),
            )
            .expect("update of example data 2 should succeed");

        // key1 should have been retained, but key2 should have been dropped.
        let key_data = current_config.get_bgp_auth_key_data();
        assert_eq!(key_data.len(), 1);
        assert_eq!(
            key_data.get(&key1),
            Some(&BgpAuthKeyStatus::Set { info: new_key.info() })
        );
        assert_eq!(key_data.get(&key2), None, "key2 should have been dropped",);

        // Update the old data again.
        current_config
            .update(example.put_insensitive, example.our_baseboard.as_ref())
            .expect("update of example data should succeed");

        // key1 should stay set, but not key2.
        let key_data = current_config.get_bgp_auth_key_data();
        assert_eq!(key_data.len(), 2);
        assert_eq!(
            key_data.get(&key1),
            Some(&BgpAuthKeyStatus::Set { info: new_key.info() })
        );
        assert_eq!(key_data.get(&key2), Some(&BgpAuthKeyStatus::Unset));
    }

    fn assert_key_ids_not_found(
        err: BgpAuthKeyError,
        expected_not_found: Vec<BgpAuthKeyId>,
        expected_valid_keys: Vec<BgpAuthKeyId>,
    ) {
        match err {
            BgpAuthKeyError::KeyIdsNotFound { not_found, valid_keys } => {
                assert_eq!(not_found, expected_not_found);
                assert_eq!(valid_keys, expected_valid_keys);
            }
            _ => panic!("expected KeyIdsNotFound, got {:?}", err),
        }
    }
}
