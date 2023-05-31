// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for user-provided RSS configuration options.

use crate::http_entrypoints::BootstrapSledDescription;
use crate::http_entrypoints::CertificateUploadResponse;
use crate::http_entrypoints::CurrentRssUserConfig;
use crate::http_entrypoints::PutRssUserConfig;
use crate::RackV1Inventory;
use bootstrap_agent_client::types::Certificate;
use gateway_client::types::SpType;
use omicron_certificates::CertificateValidator;
use omicron_common::address;
use omicron_common::api::internal::shared::RackNetworkConfig;
use sled_hardware::Baseboard;
use std::collections::BTreeSet;

#[derive(Default)]
struct PartialCertificate {
    cert: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
}

/// An analogue to `RackInitializeRequest`, but with optional fields to allow
/// the user to fill it in piecemeal.
#[derive(Default)]
pub(crate) struct CurrentRssConfig {
    inventory: BTreeSet<BootstrapSledDescription>,

    bootstrap_sleds: BTreeSet<BootstrapSledDescription>,
    ntp_servers: Vec<String>,
    dns_servers: Vec<String>,
    internal_services_ip_pool_ranges: Vec<address::IpRange>,
    external_dns_zone_name: Option<String>,
    external_certificates: Vec<Certificate>,
    recovery_silo_password_hash: Option<omicron_passwords::NewPasswordHash>,
    rack_network_config: Option<RackNetworkConfig>,

    // External certificates are uploaded in two separate actions (cert then
    // key, or vice versa). Here we store a partial certificate; once we have
    // both parts, we validate it and promote it to be a member of
    // external_certificates.
    partial_external_certificate: PartialCertificate,
}

impl CurrentRssConfig {
    pub(crate) fn populate_available_bootstrap_sleds_from_inventory(
        &mut self,
        inventory: &RackV1Inventory,
    ) {
        self.inventory = inventory
            .sps
            .iter()
            .filter_map(|sp| {
                if sp.id.type_ != SpType::Sled {
                    return None;
                }
                let state = sp.state.as_ref()?;
                Some(BootstrapSledDescription {
                    id: sp.id,
                    serial_number: state.serial_number.clone(),
                    model: state.model.clone(),
                    revision: state.revision,
                })
            })
            .collect();
    }

    pub(crate) fn set_recovery_user_password_hash(
        &mut self,
        hash: omicron_passwords::NewPasswordHash,
    ) {
        self.recovery_silo_password_hash = Some(hash);
    }

    pub(crate) fn push_cert(
        &mut self,
        cert: Vec<u8>,
    ) -> Result<CertificateUploadResponse, String> {
        self.partial_external_certificate.cert = Some(cert);
        self.maybe_promote_external_certificate()
    }

    pub(crate) fn push_key(
        &mut self,
        key: Vec<u8>,
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

        let mut validator = CertificateValidator::default();

        // We are running pre-NTP, so we can't check cert expirations; nexus
        // will have to do that.
        validator.danger_disable_expiration_validation();

        validator.validate(cert, key).map_err(|err| err.to_string())?;

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
        value: PutRssUserConfig,
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
        if let Some(Baseboard::Gimlet { identifier, model, revision }) =
            our_baseboard
        {
            let our_id = self
                .inventory
                .iter()
                .find_map(|sled| {
                    if &sled.serial_number == identifier
                        && &sled.model == model
                        && i64::from(sled.revision) == *revision
                    {
                        Some(sled.id)
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    format!(
                        "Inventory is missing the scrimlet where wicketd is \
                         running ({identifier}, model {model} rev {revision})",
                    )
                })?;
            if !value.bootstrap_sleds.contains(&our_id) {
                return Err(format!(
                    "Cannot remove the scrimlet where wicketd is running \
                     ({identifier}, model {model} rev {revision}) \
                     from bootstrap_sleds"
                ));
            }
        }

        // Next, confirm the user's list only consists of sleds in our
        // inventory.
        let mut bootstrap_sleds = BTreeSet::new();
        for id in value.bootstrap_sleds {
            let sled =
                self.inventory.iter().find(|sled| sled.id == id).ok_or_else(
                    || {
                        format!(
                            "cannot add unknown sled {} to bootstrap_sleds",
                            id.slot
                        )
                    },
                )?;
            bootstrap_sleds.insert(sled.clone());
        }

        self.bootstrap_sleds = bootstrap_sleds;
        self.ntp_servers = value.ntp_servers;
        self.dns_servers = value.dns_servers;
        self.internal_services_ip_pool_ranges =
            value.internal_services_ip_pool_ranges;
        self.external_dns_zone_name = Some(value.external_dns_zone_name);
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
            bootstrap_sleds,
            ntp_servers: rss.ntp_servers.clone(),
            dns_servers: rss.dns_servers.clone(),
            internal_services_ip_pool_ranges: rss
                .internal_services_ip_pool_ranges
                .clone(),
            external_dns_zone_name: rss.external_dns_zone_name.clone(),
            rack_network_config: rss.rack_network_config.clone(),
            num_external_certificates: rss.external_certificates.len(),
            recovery_silo_password_set: rss
                .recovery_silo_password_hash
                .is_some(),
        }
    }
}
