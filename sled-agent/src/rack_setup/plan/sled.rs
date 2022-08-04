// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Plan generation for "how should sleds be initialized".

use crate::bootstrap::{
    config::BOOTSTRAP_AGENT_PORT,
    params::SledAgentRequest,
    trust_quorum::{RackSecret, ShareDistribution},
};
use crate::rack_setup::config::SetupServiceConfig as Config;
use serde::{Deserialize, Serialize};
use slog::Logger;
use sprockets_host::Ed25519Certificate;
use std::collections::HashMap;
use std::net::{Ipv6Addr, SocketAddrV6};
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

fn rss_sled_plan_path() -> PathBuf {
    Path::new(omicron_common::OMICRON_CONFIG_PATH).join("rss-sled-plan.toml")
}

pub fn generate_rack_secret<'a>(
    rack_secret_threshold: usize,
    member_device_id_certs: &'a [Ed25519Certificate],
    log: &Logger,
) -> Result<
    Option<impl ExactSizeIterator<Item = ShareDistribution> + 'a>,
    PlanError,
> {
    // We do not generate a rack secret if we only have a single sled or if our
    // config specifies that the threshold for unlock is only a single sled.
    let total_shares = member_device_id_certs.len();
    if total_shares <= 1 {
        info!(log, "Skipping rack secret creation (only one sled present)");
        return Ok(None);
    }

    if rack_secret_threshold <= 1 {
        warn!(
            log,
            concat!(
                "Skipping rack secret creation due to config",
                " (despite discovery of {} bootstrap agents)"
            ),
            total_shares,
        );
        return Ok(None);
    }

    let secret = RackSecret::new();
    let (shares, verifier) = secret
        .split(rack_secret_threshold, total_shares)
        .map_err(PlanError::SplitRackSecret)?;

    Ok(Some(shares.into_iter().map(move |share| ShareDistribution {
        threshold: rack_secret_threshold,
        verifier: verifier.clone(),
        share,
        member_device_id_certs: member_device_id_certs.to_vec(),
    })))
}

/// Describes errors which may occur while generating a plan for sleds.
#[derive(Error, Debug)]
pub enum PlanError {
    #[error("I/O error while {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Cannot deserialize TOML file at {path}: {err}")]
    Toml { path: PathBuf, err: toml::de::Error },

    #[error("Failed to split rack secret: {0:?}")]
    SplitRackSecret(vsss_rs::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Plan {
    pub rack_id: Uuid,
    pub sleds: HashMap<SocketAddrV6, SledAgentRequest>,
    // TODO: Consider putting the rack subnet here? This may be operator-driven
    // in the future, so it should exist in the "plan".
    //
    // TL;DR: The more we decouple rom "rss-config.toml", the easier it'll be to
    // switch to an operator-driven interface.
}

impl Plan {
    pub async fn load(log: &Logger) -> Result<Option<Self>, PlanError> {
        // If we already created a plan for this RSS to allocate
        // subnets/requests to sleds, re-use that existing plan.
        let rss_sled_plan_path = rss_sled_plan_path();
        if rss_sled_plan_path.exists() {
            info!(log, "RSS plan already created, loading from file");

            let plan: Self = toml::from_str(
                &tokio::fs::read_to_string(&rss_sled_plan_path).await.map_err(
                    |err| PlanError::Io {
                        message: format!(
                            "Loading RSS plan {rss_sled_plan_path:?}"
                        ),
                        err,
                    },
                )?,
            )
            .map_err(|err| PlanError::Toml { path: rss_sled_plan_path, err })?;
            Ok(Some(plan))
        } else {
            Ok(None)
        }
    }

    pub async fn create(
        log: &Logger,
        config: &Config,
        bootstrap_addrs: Vec<Ipv6Addr>,
    ) -> Result<Self, PlanError> {
        let rack_id = Uuid::new_v4();

        let bootstrap_addrs = bootstrap_addrs.into_iter().enumerate();
        let allocations = bootstrap_addrs.map(|(idx, bootstrap_addr)| {
            info!(log, "Creating plan for the sled at {:?}", bootstrap_addr);
            let bootstrap_addr =
                SocketAddrV6::new(bootstrap_addr, BOOTSTRAP_AGENT_PORT, 0, 0);
            let sled_subnet_index =
                u8::try_from(idx + 1).expect("Too many peers!");
            let subnet = config.sled_subnet(sled_subnet_index);

            (
                bootstrap_addr,
                SledAgentRequest {
                    id: Uuid::new_v4(),
                    subnet,
                    gateway: config.gateway.clone(),
                    rack_id
                },
            )
        });

        info!(log, "Serializing plan");

        let mut sleds = std::collections::HashMap::new();
        for (addr, allocation) in allocations {
            sleds.insert(addr, allocation);
        }

        let plan = Self { rack_id, sleds };

        // Once we've constructed a plan, write it down to durable storage.
        let serialized_plan =
            toml::Value::try_from(&plan).unwrap_or_else(|e| {
                panic!("Cannot serialize configuration: {:#?}: {}", plan, e)
            });
        let plan_str = toml::to_string(&serialized_plan)
            .expect("Cannot turn config to string");

        info!(log, "Plan serialized as: {}", plan_str);
        let path = rss_sled_plan_path();
        tokio::fs::write(&path, plan_str).await.map_err(|err| {
            PlanError::Io {
                message: format!("Storing RSS sled plan to {path:?}"),
                err,
            }
        })?;
        info!(log, "Sled plan written to storage");

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_test_utils::dev::test_setup_log;
    use sprockets_common::certificates::Ed25519Signature;
    use sprockets_common::certificates::KeyType;
    use std::collections::HashSet;

    fn dummy_certs(n: usize) -> Vec<Ed25519Certificate> {
        vec![
            Ed25519Certificate {
                subject_key_type: KeyType::DeviceId,
                subject_public_key: sprockets_host::Ed25519PublicKey([0; 32]),
                signer_key_type: KeyType::Manufacturing,
                signature: Ed25519Signature([0; 64]),
            };
            n
        ]
    }

    #[test]
    fn test_generate_rack_secret() {
        let logctx = test_setup_log("test_generate_rack_secret");

        // No secret generated if we have <= 1 sled
        assert!(generate_rack_secret(10, &dummy_certs(1), &logctx.log)
            .unwrap()
            .is_none());

        // No secret generated if threshold <= 1
        assert!(generate_rack_secret(1, &dummy_certs(10), &logctx.log)
            .unwrap()
            .is_none());

        // Secret generation fails if threshold > total sleds
        assert!(matches!(
            generate_rack_secret(10, &dummy_certs(5), &logctx.log),
            Err(PlanError::SplitRackSecret(_))
        ));

        // Secret generation succeeds if threshold <= total shares and both are
        // > 1, and the returned iterator satifies:
        //
        // * total length == total shares
        // * each share is distinct
        for total_shares in 2..=32 {
            for threshold in 2..=total_shares {
                let certs = dummy_certs(total_shares);
                let shares =
                    generate_rack_secret(threshold, &certs, &logctx.log)
                        .unwrap()
                        .unwrap();

                assert_eq!(shares.len(), total_shares);

                // `Share` doesn't implement `Hash`, but it's a newtype around
                // `Vec<u8>` (which does). Unwrap the newtype to check that all
                // shares are distinct.
                let shares_set = shares
                    .map(|share_dist| share_dist.share.0)
                    .collect::<HashSet<_>>();
                assert_eq!(shares_set.len(), total_shares);
            }
        }

        logctx.cleanup_successful();
    }
}
