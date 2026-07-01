// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for user-provided RSS configuration options.

use crate::RssOrMultirackJoinConfigCommon;
use crate::bgp_auth_keys::BgpAuthKeys;
use crate::bootstrap_addrs::BootstrapPeersFromDdm;
use crate::context::CommonConfigContainer;
use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use bootstrap_agent_lockstep_client::types::BootstrapAddressDiscovery;
use bootstrap_agent_lockstep_client::types::Certificate;
use bootstrap_agent_lockstep_client::types::Name;
use bootstrap_agent_lockstep_client::types::RackInitializeRequest;
use bootstrap_agent_lockstep_client::types::RecoverySiloConfig;
use bootstrap_agent_lockstep_client::types::UserId;
use display_error_chain::DisplayErrorChain;
use omicron_certificates::CertificateError;
use omicron_common::address;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv4Range;
use omicron_common::address::Ipv6Range;
use omicron_common::api::external::AllowedSourceIps;
use oxnet::Ipv6Net;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::RouterLifetimeConfig;
use sled_agent_types::early_networking::RouterPeerType;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::UplinkAddress;
use sled_agent_types::early_networking::UplinkPorts;
use sled_hardware_types::BaseboardId;
use slog::warn;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use wicket_common::inventory::MgsV1Inventory;
use wicket_common::rack_setup::BgpAuthKey;
use wicket_common::rack_setup::CurrentRssUserConfigInsensitive;
use wicket_common::rack_setup::GetBgpAuthKeyInfoResponse;
use wicket_common::rack_setup::ManualPortConfig;
use wicket_common::rack_setup::PutRssUserConfigInsensitive;
use wicket_common::rack_setup::UserSpecifiedRackNetworkConfig;
use wicket_common::rack_setup::UserSpecifiedRouterPeerAddr;
use wicketd_api::CertificateUploadResponse;
use wicketd_api::CurrentRssUserConfig;
use wicketd_api::CurrentRssUserConfigSensitive;

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
    pub common: RssOrMultirackJoinConfigCommon,
    ntp_servers: Vec<String>,
    dns_servers: Vec<IpAddr>,
    internal_services_ip_pool_ranges: Vec<address::IpRange>,
    external_dns_ips: Vec<IpAddr>,
    external_dns_zone_name: String,
    external_certificates: Vec<Certificate>,
    recovery_silo_password_hash: Option<omicron_passwords::NewPasswordHash>,
    rack_network_config: Option<UserSpecifiedRackNetworkConfig>,
    allowed_source_ips: Option<AllowedSourceIps>,
    external_jumbo_frames_opt_in_enabled: bool,
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

    pub(crate) fn start_rss_request(
        &mut self,
        bootstrap_peers: &BootstrapPeersFromDdm,
        log: &slog::Logger,
    ) -> Result<RackInitializeRequest> {
        // Basic "client-side" checks.
        //
        // TODO: Instead, we should collect a list of failed checks that we can
        // send down to the client, and that the client can then display as
        // action items.
        if self.common.bootstrap_sleds.is_empty() {
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
            &self.common.bgp_auth_keys,
        )?;

        let known_bootstrap_sleds = bootstrap_peers.sleds();
        let mut bootstrap_ips = Vec::new();
        for sled in &self.common.bootstrap_sleds {
            let Some(ip) =
                known_bootstrap_sleds.get(&sled.baseboard_id).copied()
            else {
                bail!(
                    "IP address not (yet?) known for sled {} ({:?})",
                    sled.id.slot,
                    sled.baseboard_id,
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
        let trust_quorum_peers: Option<Vec<BaseboardId>> =
            if self.common.bootstrap_sleds.len() >= TRUST_QUORUM_MIN_SIZE {
                Some(
                    self.common
                        .bootstrap_sleds
                        .iter()
                        .map(|sled| sled.baseboard_id.clone())
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
        let user_password_hash =
            bootstrap_agent_lockstep_client::types::NewPasswordHash(
                recovery_silo_password_hash.to_string(),
            );
        let internal_services_ip_pool_ranges = self
            .internal_services_ip_pool_ranges
            .iter()
            .map(|pool| {
                use bootstrap_agent_lockstep_client::types::IpRange;
                use bootstrap_agent_lockstep_client::types::Ipv4Range;
                use bootstrap_agent_lockstep_client::types::Ipv6Range;
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
            external_jumbo_frames_opt_in_enabled: self
                .external_jumbo_frames_opt_in_enabled,
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

    pub(crate) fn update(
        &mut self,
        config: PutRssUserConfigInsensitive,
        our_baseboard: &BaseboardId,
        inventory: &MgsV1Inventory,
        ddm_discovered_sleds: &BTreeMap<BaseboardId, Ipv6Addr>,
        log: &slog::Logger,
    ) -> Result<(), String> {
        self.common.update(
            &config.bootstrap_sleds,
            config.rack_network_config.get_bgp_auth_key_ids(),
            our_baseboard,
            inventory,
            ddm_discovered_sleds,
            log,
        )?;
        self.ntp_servers = config.ntp_servers;
        self.dns_servers = config.dns_servers;
        self.internal_services_ip_pool_ranges =
            config.internal_services_ip_pool_ranges;
        self.external_dns_ips = config.external_dns_ips;
        self.external_dns_zone_name = config.external_dns_zone_name;
        self.allowed_source_ips = Some(config.allowed_source_ips);
        self.rack_network_config = Some(config.rack_network_config);
        self.external_jumbo_frames_opt_in_enabled =
            config.external_jumbo_frames_opt_in_enabled;

        Ok(())
    }
}

impl CommonConfigContainer for CurrentRssConfig {
    fn common_mut(&mut self) -> &mut RssOrMultirackJoinConfigCommon {
        &mut self.common
    }
}

impl From<&'_ CurrentRssConfig> for CurrentRssUserConfig {
    fn from(rss: &CurrentRssConfig) -> Self {
        // If the user has selected bootstrap sleds, use those; otherwise,
        // default to the full inventory list.
        let bootstrap_sleds = if !rss.common.bootstrap_sleds.is_empty() {
            rss.common.bootstrap_sleds.clone()
        } else {
            rss.common.inventory.sleds.clone()
        };

        Self {
            sensitive: CurrentRssUserConfigSensitive {
                num_external_certificates: rss.external_certificates.len(),
                recovery_silo_password_set: rss
                    .recovery_silo_password_hash
                    .is_some(),
                bgp_auth_keys: GetBgpAuthKeyInfoResponse {
                    data: rss.common.get_bgp_auth_key_data(),
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
                external_jumbo_frames_opt_in_enabled: rss
                    .external_jumbo_frames_opt_in_enabled,
            },
        }
    }
}

fn validate_rack_network_config(
    config: &UserSpecifiedRackNetworkConfig,
    bgp_auth_keys: &BgpAuthKeys,
) -> Result<bootstrap_agent_lockstep_client::types::RackNetworkConfig> {
    use bootstrap_agent_lockstep_client::types::BgpConfig as BaBgpConfig;
    use bootstrap_agent_lockstep_client::types::MaxPathConfig as BaMaxPathConfig;

    // Ensure that there is at least one uplink
    if !config.has_any_uplinks() {
        return Err(anyhow!("Must have at least one port configured"));
    }

    // Make sure `infra_ip_first`..`infra_ip_last` is a well-defined range.
    let infra_ip_range = match (config.infra_ip_first, config.infra_ip_last) {
        (IpAddr::V4(first), IpAddr::V4(last)) => Ipv4Range::new(first, last)
            .map_err(|s: String| {
                anyhow!("invalid `infra_ip_first`, `infra_ip_last` range: {s}")
            })
            .map(|v| IpRange::V4(v)),
        (IpAddr::V6(first), IpAddr::V6(last)) => Ipv6Range::new(first, last)
            .map_err(|s: String| {
                anyhow!("invalid `infra_ip_first`, `infra_ip_last` range: {s}")
            })
            .map(|v| IpRange::V6(v)),
        _ => Err(anyhow!(
            "`infra_ip_first` and `infra_ip_last` must be of the same type"
        )),
    }?;

    for (_, _, port_config) in config.iter_uplinks() {
        // Check that `infra_ip_{first...last}` contains every `uplink_ip`.
        //
        // TODO this implies a single contiguous range for port IPs which is
        // over constraining
        for addr in &port_config.addresses {
            let addr: IpAddr = match addr.address {
                UplinkAddress::AddrConf => continue,
                UplinkAddress::Static { ip_net } => ip_net.addr(),
            };
            if addr < infra_ip_range.first_address()
                || addr > infra_ip_range.last_address()
            {
                bail!(
                    "`uplink_cidr` IP address {addr} is not covered by the \
                     range defined by `infra_ip_first` ({}) and \
                     `infra_ip_last` ({})",
                    infra_ip_range.first_address(),
                    infra_ip_range.last_address(),
                );
            }
        }

        // Check that router_lifetime is only specified for unnumbered peers
        for peer in &port_config.bgp_peers {
            match peer.addr {
                UserSpecifiedRouterPeerAddr::Unnumbered => (),
                UserSpecifiedRouterPeerAddr::Numbered(ip) => {
                    if peer.router_lifetime != RouterLifetimeConfig::default() {
                        bail!(
                            "numbered BGP peer {ip} specifies a \
                             router_lifetime, but router_lifetime is only \
                             supported for unnumbered BGP peers"
                        );
                    }
                }
            }
        }
    }

    // Check that all auth keys are present.
    for (key_id, key) in bgp_auth_keys.iter() {
        if key.is_none() {
            bail!("No BGP MD5 auth key provided for key ID {key_id}");
        }
    }

    let rack_subnet = match validate_rack_subnet(config.rack_subnet_address) {
        Ok(v) => v,
        Err(e) => bail!(e),
    };

    // TODO Add more client side checks on `rack_network_config` contents?

    let ports = config
        .iter_uplinks()
        .map(|(switch, port, config)| {
            build_port_config(switch, port, config, bgp_auth_keys)
        })
        .collect::<Vec<_>>();
    let ports = UplinkPorts::new(ports)
        .context("rack network config must specify at least one uplink port")?;

    Ok(bootstrap_agent_lockstep_client::types::RackNetworkConfig {
        rack_subnet,
        infra_ip_first: config.infra_ip_first,
        infra_ip_last: config.infra_ip_last,
        ports,
        bgp: config
            .bgp
            .iter()
            .map(|config| BaBgpConfig {
                asn: config.asn,
                originate: config.originate.clone(),
                checker: config.checker.clone(),
                shaper: config.shaper.clone(),
                max_paths: BaMaxPathConfig(config.max_paths.as_nonzero_u8()),
            })
            .collect(),
        //TODO bfd config in wicket
        bfd: vec![],
    })
}

pub fn validate_rack_subnet(
    subnet_address: Option<Ipv6Addr>,
) -> Result<Ipv6Net, String> {
    use rand::prelude::*;

    let rack_subnet_address = match subnet_address {
        Some(addr) => addr,
        None => {
            let mut rng = rand::rng();
            let a: u16 = 0xfd00 + Into::<u16>::into(rng.random::<u8>());
            Ipv6Addr::new(
                a,
                rng.random::<u16>(),
                rng.random::<u16>(),
                0x0100,
                0,
                0,
                0,
                0,
            )
        }
    };

    // first octet must be fd
    if rack_subnet_address.octets()[0] != 0xfd {
        return Err("rack subnet address must begin with 0xfd".into());
    };

    // Do not allow rack0
    if rack_subnet_address.octets()[6] == 0x00 {
        return Err("rack number (seventh octet) cannot be 0".into());
    };

    // Do not allow addresses more specific than /56
    if rack_subnet_address.octets()[7..].iter().any(|x| *x != 0x00) {
        return Err("rack subnet address is /56, \
                   but a more specific prefix was provided"
            .into());
    };

    Ipv6Net::new(rack_subnet_address, 56).map_err(|e| e.to_string())
}

/// Builds a [`PortConfig`] from a
/// [`wicket_common::rack_setup::UserSpecifiedPortConfig`].
///
/// Assumes that all auth keys are present in `bgp_auth_keys`.
fn build_port_config(
    switch: SwitchSlot,
    port: &str,
    config: &ManualPortConfig,
    bgp_auth_keys: &BgpAuthKeys,
) -> PortConfig {
    use sled_agent_types::early_networking::BgpPeerConfig;

    PortConfig {
        port: port.to_owned(),
        routes: config.routes.clone(),
        addresses: config.addresses.iter().copied().map(From::from).collect(),
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

                let addr = match p.addr {
                    UserSpecifiedRouterPeerAddr::Unnumbered => {
                        RouterPeerType::Unnumbered {
                            router_lifetime: p.router_lifetime,
                        }
                    }
                    UserSpecifiedRouterPeerAddr::Numbered(ip) => {
                        RouterPeerType::Numbered { ip }
                    }
                };

                BgpPeerConfig {
                    addr,
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
        switch,
        uplink_port_speed: config.uplink_port_speed,
        uplink_port_fec: config.uplink_port_fec,
        autoneg: config.autoneg,
        lldp: config.lldp.clone(),
        tx_eq: config.tx_eq,
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
    use crate::bgp_auth_keys::BgpAuthKeyError;
    use omicron_test_utils::dev;
    use wicket_common::example::ExampleRackSetupData;
    use wicket_common::rack_setup::BgpAuthKeyId;
    use wicket_common::rack_setup::BgpAuthKeyStatus;
    use wicketd_api::SetBgpAuthKeyStatus;

    use super::*;

    #[test]
    fn test_router_lifetime_unnumbered_only() {
        // Default should be okay and have at least one BGP peer.
        let example = ExampleRackSetupData::non_empty();
        let bgp_auth_keys = {
            let mut keys = BgpAuthKeys::default();
            for id in example.bgp_auth_keys {
                keys.insert(
                    id,
                    Some(BgpAuthKey::TcpMd5 { key: "dummy".to_owned() }),
                );
            }
            keys
        };
        let rack_network_config = example.put_insensitive.rack_network_config;
        validate_rack_network_config(&rack_network_config, &bgp_auth_keys)
            .expect("base config is valid");
        assert!(
            !rack_network_config
                .switch0
                .first_key_value()
                .expect("at least one switch0 port")
                .1
                .manual()
                .unwrap()
                .bgp_peers
                .is_empty()
        );

        // Combine unnumbered with a non-default router_lifetime - fine.
        let mut valid_router_lifetime = rack_network_config.clone();
        {
            let peer = valid_router_lifetime
                .switch0
                .first_entry()
                .unwrap()
                .into_mut()
                .manual_mut()
                .unwrap()
                .bgp_peers
                .get_mut(0)
                .unwrap();
            peer.addr = UserSpecifiedRouterPeerAddr::Unnumbered;
            peer.router_lifetime = RouterLifetimeConfig::new(1234).unwrap();
        }
        validate_rack_network_config(&valid_router_lifetime, &bgp_auth_keys)
            .expect("unnumbered with non-zero router_lifetime is ok");

        // Keep non-default router_lifetime but change to a numbered peer -
        // should fail with a reasonable error.
        let mut invalid_router_lifetime = valid_router_lifetime.clone();
        {
            let peer = invalid_router_lifetime
                .switch0
                .first_entry()
                .unwrap()
                .into_mut()
                .manual_mut()
                .unwrap()
                .bgp_peers
                .get_mut(0)
                .unwrap();
            peer.addr = UserSpecifiedRouterPeerAddr::Numbered(
                "1.2.3.4".parse().unwrap(),
            );
        }
        let err = validate_rack_network_config(
            &invalid_router_lifetime,
            &bgp_auth_keys,
        )
        .expect_err("numbered with non-zero router_lifetime is not ok");
        assert_eq!(
            format!("{err:#}"),
            "numbered BGP peer 1.2.3.4 specifies a router_lifetime, but \
             router_lifetime is only supported for unnumbered BGP peers"
        );

        // Keep numbered peer but switch router_lifetime back to default - fine.
        let mut valid_router_lifetime = invalid_router_lifetime.clone();
        {
            let peer = valid_router_lifetime
                .switch0
                .first_entry()
                .unwrap()
                .into_mut()
                .manual_mut()
                .unwrap()
                .bgp_peers
                .get_mut(0)
                .unwrap();
            peer.router_lifetime = RouterLifetimeConfig::default()
        }
        validate_rack_network_config(&valid_router_lifetime, &bgp_auth_keys)
            .expect("numbered with zero router_lifetime is ok");
    }

    #[test]
    fn test_bgp_auth_key_states() {
        let logctx = dev::test_setup_log("test_bgp_auth_key_states");
        let example = ExampleRackSetupData::non_empty();

        let mut current_config = CurrentRssConfig::default();

        current_config
            .update(
                example.put_insensitive.clone(),
                &example.our_baseboard_id,
                &example.inventory,
                &example.ddm_discovered_sleds,
                &logctx.log,
            )
            .expect("update of example data should succeed");

        // At this point, both BGP keys should be unset.
        let key_data = current_config.common.get_bgp_auth_key_data();
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
                .common
                .set_bgp_auth_key(key1.clone(), shared_key.clone())
                .expect("setting key1 succeeded");
            assert_eq!(status, SetBgpAuthKeyStatus::Added);

            // Check that the key is now set.
            let key_data = current_config.common.get_bgp_auth_key_data();
            assert_eq!(
                key_data.get(&key1),
                Some(&BgpAuthKeyStatus::Set { info: shared_key.info() })
            );
        }

        // Try replacing the key with the same one.
        {
            let status = current_config
                .common
                .set_bgp_auth_key(key1.clone(), shared_key.clone())
                .expect("replacing key1 succeeded");
            assert_eq!(status, SetBgpAuthKeyStatus::Unchanged);
            let key_data = current_config.common.get_bgp_auth_key_data();
            assert_eq!(
                key_data.get(&key1),
                Some(&BgpAuthKeyStatus::Set { info: shared_key.info() })
            );
        }

        // Replace the key with a different one.
        {
            let status = current_config
                .common
                .set_bgp_auth_key(key1.clone(), new_key.clone())
                .expect("replacing key1 succeeded");
            assert_eq!(status, SetBgpAuthKeyStatus::Replaced);
            let key_data = current_config.common.get_bgp_auth_key_data();
            assert_eq!(
                key_data.get(&key1),
                Some(&BgpAuthKeyStatus::Set { info: new_key.info() })
            );
        }

        // Query and set a key that doesn't exist.
        {
            let does_not_exist: BgpAuthKeyId =
                "does-not-exist".parse().unwrap();
            let err = current_config
                .common
                .check_bgp_auth_keys_valid([&does_not_exist]);
            assert_eq!(
                err,
                Err(BgpAuthKeyError::KeyIdsNotFound {
                    not_found: vec![does_not_exist.clone()],
                    valid_keys: example.bgp_auth_keys.clone(),
                })
            );
            let err = current_config
                .common
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
                .common
                .set_bgp_auth_key(key2.clone(), shared_key.clone())
                .expect("setting key2 succeeded");
            assert_eq!(status, SetBgpAuthKeyStatus::Added);
            let key_data = current_config.common.get_bgp_auth_key_data();

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
                &example_data_2.our_baseboard_id,
                &example_data_2.inventory,
                &example_data_2.ddm_discovered_sleds,
                &logctx.log,
            )
            .expect("update of example data 2 should succeed");

        // key1 should have been retained, but key2 should have been dropped.
        let key_data = current_config.common.get_bgp_auth_key_data();
        assert_eq!(key_data.len(), 1);
        assert_eq!(
            key_data.get(&key1),
            Some(&BgpAuthKeyStatus::Set { info: new_key.info() })
        );
        assert_eq!(key_data.get(&key2), None, "key2 should have been dropped",);

        // Update the old data again.
        current_config
            .update(
                example.put_insensitive,
                &example.our_baseboard_id,
                &example.inventory,
                &example.ddm_discovered_sleds,
                &logctx.log,
            )
            .expect("update of example data should succeed");

        // key1 should stay set, but not key2.
        let key_data = current_config.common.get_bgp_auth_key_data();
        assert_eq!(key_data.len(), 2);
        assert_eq!(
            key_data.get(&key1),
            Some(&BgpAuthKeyStatus::Set { info: new_key.info() })
        );
        assert_eq!(key_data.get(&key2), Some(&BgpAuthKeyStatus::Unset));

        logctx.cleanup_successful();
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
        }
    }
}
