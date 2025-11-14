// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Plan generation for "how should sleds be initialized".

use crate::bootstrap::config::BOOTSTRAP_AGENT_RACK_INIT_PORT;
use omicron_uuid_kinds::SledUuid;
use sled_agent_types::rack_init::RackInitializeRequest as Config;
use sled_agent_types::sled::StartSledAgentRequest;
use sled_agent_types::sled::StartSledAgentRequestBody;
use slog::Logger;
use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv6Addr, SocketAddrV6};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Plan {
    pub rack_id: Uuid,
    pub sleds: BTreeMap<SocketAddrV6, StartSledAgentRequest>,

    // Store the provided RSS configuration as part of the sled plan.
    pub config: Config,
}

impl Plan {
    pub fn create(
        log: &Logger,
        config: &Config,
        bootstrap_addrs: BTreeSet<Ipv6Addr>,
        use_trust_quorum: bool,
    ) -> Self {
        let rack_id = Uuid::new_v4();

        let bootstrap_addrs = bootstrap_addrs.into_iter().enumerate();
        let allocations = bootstrap_addrs.map(|(idx, bootstrap_addr)| {
            info!(log, "Creating plan for the sled at {:?}", bootstrap_addr);
            let bootstrap_addr = SocketAddrV6::new(
                bootstrap_addr,
                BOOTSTRAP_AGENT_RACK_INIT_PORT,
                0,
                0,
            );
            let sled_subnet_index =
                u8::try_from(idx + 1).expect("Too many peers!");
            let subnet = config.sled_subnet(sled_subnet_index);

            (
                bootstrap_addr,
                StartSledAgentRequest {
                    generation: 0,
                    schema_version: 1,
                    body: StartSledAgentRequestBody {
                        id: SledUuid::new_v4(),
                        subnet,
                        use_trust_quorum,
                        is_lrtq_learner: false,
                        rack_id,
                    },
                },
            )
        });

        let mut sleds = BTreeMap::new();
        for (addr, allocation) in allocations {
            sleds.insert(addr, allocation);
        }

        Self { rack_id, sleds, config: config.clone() }
    }
}
