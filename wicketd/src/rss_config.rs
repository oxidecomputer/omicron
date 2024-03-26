// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for user-provided RSS configuration options.

use crate::bootstrap_addrs::BootstrapPeers;
use crate::http_entrypoints::BootstrapSledDescription;
use crate::http_entrypoints::CertificateUploadResponse;
use crate::http_entrypoints::CurrentRssUserConfig;
use crate::http_entrypoints::CurrentRssUserConfigInsensitive;
use crate::http_entrypoints::CurrentRssUserConfigSensitive;
use crate::RackV1Inventory;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use bootstrap_agent_client::types::BootstrapAddressDiscovery;
use bootstrap_agent_client::types::Certificate;
use bootstrap_agent_client::types::Name;
use bootstrap_agent_client::types::RackInitializeRequest;
use bootstrap_agent_client::types::RecoverySiloConfig;
use bootstrap_agent_client::types::UserId;
use display_error_chain::DisplayErrorChain;
use gateway_client::types::SpType;
use omicron_certificates::CertificateError;
use omicron_common::address;
use omicron_common::address::Ipv4Range;
use sled_hardware_types::Baseboard;
use slog::warn;
use std::collections::BTreeSet;
use std::mem;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use wicket_common::rack_setup::PutRssUserConfigInsensitive;
use wicket_common::rack_setup::UserSpecifiedRackNetworkConfig;

// TODO-correctness For now, we always use the same rack subnet when running
// RSS. When we get to multirack, this will be wrong, but there are many other
// RSS-related things that need to change then too.
const RACK_SUBNET: Ipv6Addr =
    Ipv6Addr::new(0xfd00, 0x1122, 0x3344, 0x0100, 0, 0, 0, 0);

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
        inventory: &RackV1Inventory,
        bootstrap_peers: &BootstrapPeers,
    ) {
        let bootstrap_sleds = bootstrap_peers.sleds();

        self.inventory = inventory
            .sps
            .iter()
            .filter_map(|sp| {
                if sp.id.type_ != SpType::Sled {
                    return None;
                }
                let state = sp.state.as_ref()?;
                let baseboard = Baseboard::new_gimlet(
                    state.serial_number.clone(),
                    state.model.clone(),
                    state.revision.into(),
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
        let rack_network_config =
            validate_rack_network_config(rack_network_config)?;

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
                user_name: UserId(RECOVERY_SILO_USERNAME.into()),
                user_password_hash,
            },
            rack_network_config,
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
            },
        }
    }
}

fn validate_rack_network_config(
    config: &UserSpecifiedRackNetworkConfig,
) -> Result<bootstrap_agent_client::types::RackNetworkConfigV1> {
    use bootstrap_agent_client::types::BgpConfig as BaBgpConfig;
    use bootstrap_agent_client::types::BgpPeerConfig as BaBgpPeerConfig;
    use bootstrap_agent_client::types::PortConfigV1 as BaPortConfigV1;
    use bootstrap_agent_client::types::PortFec as BaPortFec;
    use bootstrap_agent_client::types::PortSpeed as BaPortSpeed;
    use bootstrap_agent_client::types::RouteConfig as BaRouteConfig;
    use bootstrap_agent_client::types::SwitchLocation as BaSwitchLocation;
    use omicron_common::api::internal::shared::PortFec;
    use omicron_common::api::internal::shared::PortSpeed;
    use omicron_common::api::internal::shared::SwitchLocation;

    // Ensure that there is at least one uplink
    if config.ports.is_empty() {
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
    for port_config in &config.ports {
        for addr in &port_config.addresses {
            // ... and check that it contains `uplink_ip`.
            if addr.ip() < infra_ip_range.first
                || addr.ip() > infra_ip_range.last
            {
                bail!(
                "`uplink_cidr`'s IP address must be in the range defined by \
                `infra_ip_first` and `infra_ip_last`"
            );
            }
        }
    }
    // TODO Add more client side checks on `rack_network_config` contents?

    Ok(bootstrap_agent_client::types::RackNetworkConfigV1 {
        rack_subnet: RACK_SUBNET.into(),
        infra_ip_first: config.infra_ip_first,
        infra_ip_last: config.infra_ip_last,
        ports: config
            .ports
            .iter()
            .map(|config| BaPortConfigV1 {
                port: config.port.clone(),
                routes: config
                    .routes
                    .iter()
                    .map(|r| BaRouteConfig {
                        destination: r.destination,
                        nexthop: r.nexthop,
                    })
                    .collect(),
                addresses: config.addresses.clone(),
                bgp_peers: config
                    .bgp_peers
                    .iter()
                    .map(|p| BaBgpPeerConfig {
                        addr: p.addr,
                        asn: p.asn,
                        port: p.port.clone(),
                        hold_time: p.hold_time,
                        connect_retry: p.connect_retry,
                        delay_open: p.delay_open,
                        idle_hold_time: p.idle_hold_time,
                        keepalive: p.keepalive,
                    })
                    .collect(),
                switch: match config.switch {
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
                uplink_port_fec: match config.uplink_port_fec {
                    PortFec::Firecode => BaPortFec::Firecode,
                    PortFec::None => BaPortFec::None,
                    PortFec::Rs => BaPortFec::Rs,
                },
                autoneg: config.autoneg,
            })
            .collect(),
        bgp: config
            .bgp
            .iter()
            .map(|config| BaBgpConfig {
                asn: config.asn,
                originate: config.originate.clone(),
            })
            .collect(),
        //TODO(ry) bfd config in wicket
        bfd: vec![],
    })
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
