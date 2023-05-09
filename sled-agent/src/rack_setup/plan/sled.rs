// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Plan generation for "how should sleds be initialized".

use crate::bootstrap::{
    config::BOOTSTRAP_AGENT_SPROCKETS_PORT,
    params::SledAgentRequest,
    trust_quorum::{RackSecret, ShareDistribution},
};
use crate::ledger::{Ledger, Ledgerable};
use crate::rack_setup::config::SetupServiceConfig as Config;
use crate::storage_manager::StorageResources;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use slog::Logger;
use sprockets_host::Ed25519Certificate;
use std::collections::{HashMap, HashSet};
use std::net::{Ipv6Addr, SocketAddrV6};
use thiserror::Error;
use uuid::Uuid;

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

    #[error("Failed to access ledger: {0}")]
    Ledger(#[from] crate::ledger::Error),

    #[error("Failed to split rack secret: {0:?}")]
    SplitRackSecret(vsss_rs::Error),
}

impl Ledgerable for Plan {
    fn is_newer_than(&self, _other: &Self) -> bool {
        true
    }
    fn generation_bump(&mut self) {}
}
const RSS_SLED_PLAN_FILENAME: &str = "rss-sled-plan.toml";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Plan {
    pub rack_id: Uuid,
    pub sleds: HashMap<SocketAddrV6, SledAgentRequest>,

    // Store the provided RSS configuration as part of the sled plan; if it
    // changes after reboot, we need to know.
    pub config: Config,
}

impl Plan {
    pub async fn load(
        log: &Logger,
        storage: &StorageResources,
    ) -> Result<Option<Self>, PlanError> {
        let paths: Vec<Utf8PathBuf> = storage
            .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
            .await
            .into_iter()
            .map(|p| p.join(RSS_SLED_PLAN_FILENAME))
            .collect();

        // If we already created a plan for this RSS to allocate
        // subnets/requests to sleds, re-use that existing plan.
        let ledger = Ledger::<Self>::new(log, paths.clone()).await;
        if let Some(ledger) = ledger {
            info!(log, "RSS plan already created, loading from file");
            Ok(Some(ledger.data().clone()))
        } else {
            Ok(None)
        }
    }

    pub async fn create(
        log: &Logger,
        config: &Config,
        storage: &StorageResources,
        bootstrap_addrs: HashSet<Ipv6Addr>,
    ) -> Result<Self, PlanError> {
        let rack_id = Uuid::new_v4();

        let bootstrap_addrs = bootstrap_addrs.into_iter().enumerate();
        let allocations = bootstrap_addrs.map(|(idx, bootstrap_addr)| {
            info!(log, "Creating plan for the sled at {:?}", bootstrap_addr);
            let bootstrap_addr = SocketAddrV6::new(
                bootstrap_addr,
                BOOTSTRAP_AGENT_SPROCKETS_PORT,
                0,
                0,
            );
            let sled_subnet_index =
                u8::try_from(idx + 1).expect("Too many peers!");
            let subnet = config.sled_subnet(sled_subnet_index);

            (
                bootstrap_addr,
                SledAgentRequest {
                    id: Uuid::new_v4(),
                    subnet,
                    ntp_servers: config.ntp_servers.clone(),
                    dns_servers: config.dns_servers.clone(),
                    rack_id,
                },
            )
        });

        info!(log, "Serializing plan");

        let mut sleds = std::collections::HashMap::new();
        for (addr, allocation) in allocations {
            sleds.insert(addr, allocation);
        }

        let plan = Self { rack_id, sleds, config: config.clone() };

        // Once we've constructed a plan, write it down to durable storage.
        let paths: Vec<Utf8PathBuf> = storage
            .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
            .await
            .into_iter()
            .map(|p| p.join(RSS_SLED_PLAN_FILENAME))
            .collect();

        let mut ledger = Ledger::<Self>::new_with(log, paths, plan.clone());
        ledger.commit().await?;
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
