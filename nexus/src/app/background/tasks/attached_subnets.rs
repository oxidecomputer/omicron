// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task to push configuration for attached subnets to the switch and
//! OPTE instances.

use crate::app::background::BackgroundTask;
use crate::app::dpd_clients;
use dpd_client::types::AttachedSubnetEntry;
use dpd_client::types::InstanceTarget;
use futures::FutureExt as _;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use internal_dns_resolver::Resolver;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::AttachedSubnetManagerStatus;
use nexus_types::internal_api::background::DendriteSubnetDetails;
use nexus_types::internal_api::background::SledSubnetDetails;
use omicron_common::api::external::SwitchLocation;
use omicron_common::api::internal::shared;
use omicron_common::api::internal::shared::AttachedSubnetId;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledUuid;
use oxnet::IpNet;
use serde_json::Value;
use serde_json::json;
use sled_agent_client::types::AttachedSubnet;
use sled_agent_client::types::AttachedSubnets;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::Ipv6Addr;
use std::sync::Arc;

/// Background task that pushes attached subnets.
pub struct Manager {
    resolver: Resolver,
    datastore: Arc<DataStore>,
}

impl Manager {
    pub fn new(resolver: Resolver, datastore: Arc<DataStore>) -> Self {
        Self { resolver, datastore }
    }

    async fn send_attachments_to_dendrite(
        &self,
        log: &Logger,
        clients: &HashMap<SwitchLocation, dpd_client::Client>,
        attachments: &[shared::AttachedSubnet],
    ) -> HashMap<SwitchLocation, DendriteSubnetDetails> {
        // Dendrite provides an API to list all attached subnets, and to delete /
        // put one at a time, rather than putting an entire _set_ of mappings.
        // That means we have to do the diff on the client side, to compute teh
        // set of subnets to remove and add, separately for each Dendrite
        // instance.
        //
        // First, build the full set of expected mappings for all switches. This
        // is only derived from the database.
        let desired_attachments = attachments
            .iter()
            .map(|at| {
                let shared::AttachedSubnet {
                    sled_ip, subnet, mac, vni, ..
                } = at;
                let tgt = InstanceTarget {
                    internal_ip: *sled_ip,
                    inner_mac: dpd_client::types::MacAddr {
                        a: mac.into_array(),
                    },
                    // Safety: We've collected this from the DB, so it has to
                    // have been a valid 24-bit VNI.
                    vni: u32::from(*vni).try_into().unwrap(),
                };
                (*subnet, tgt)
            })
            .collect::<HashMap<_, _>>();

        // Loop over each Dendrite instance, find the subnets it has and the
        // diff we need to apply.
        let mut res = HashMap::<_, DendriteSubnetDetails>::new();
        for (loc, client) in clients.iter() {
            let details = res.entry(*loc).or_default();
            let existing_attachments = match client
                .attached_subnet_list_stream(None)
                .map(|entry| {
                    entry.map(|e| {
                        let AttachedSubnetEntry { subnet, tgt } = e;
                        (subnet, tgt)
                    })
                })
                .try_collect::<HashMap<_, _>>()
                .await
            {
                Ok(m) => {
                    details.n_total_subnets = m.len();
                    m
                }
                Err(e) => {
                    let err = InlineErrorChain::new(&e);
                    details.errors.push(err.to_string());
                    error!(
                        log,
                        "failed to list existing attached subnets \
                        from switch, it will be skipped this time";
                        "switch_location" => %loc,
                        "error" => err,
                    );
                    continue;
                }
            };

            // Set-diff them to figure out the changes we need to apply.
            let AttachedSubnetDiff { to_add, to_remove } =
                AttachedSubnetDiff::new(
                    &existing_attachments,
                    &desired_attachments,
                );

            // Remove any attachments Dendrite has that we no longer want.
            for subnet in to_remove.into_iter() {
                match client.attached_subnet_delete(subnet).await {
                    Ok(_) => {
                        details.n_subnets_removed += 1;
                        details.n_total_subnets -= 1;
                        debug!(
                            log,
                            "deleted subnet from dendrite";
                            "subnet" => %subnet,
                            "switch" => %loc,
                        );
                    }
                    Err(e) => {
                        let err = InlineErrorChain::new(&e);
                        details.errors.push(err.to_string());
                        error!(
                            log,
                            "failed to delete subnet from dendrite";
                            "subnet" => %subnet,
                            "switch" => %loc,
                            "error" => err,
                        );
                    }
                }
            }

            // Add attachments we do want.
            for (subnet, target) in to_add.into_iter() {
                match client.attached_subnet_create(subnet, target).await {
                    Ok(_) => {
                        details.n_subnets_added += 1;
                        details.n_total_subnets += 1;
                        debug!(
                            log,
                            "created attached subnet on dendrite";
                            "subnet" => %subnet,
                            "target" => ?target,
                            "switch" => %loc,
                        );
                    }
                    Err(e) => {
                        let err = InlineErrorChain::new(&e);
                        details.errors.push(err.to_string());
                        error!(
                            log,
                            "failed to create subnet on dendrite";
                            "subnet" => %subnet,
                            "target" => ?target,
                            "switch" => %loc,
                            "error" => err,
                        );
                    }
                }
            }
        }
        res
    }

    async fn send_attachments_to_sled_agents(
        &self,
        log: &Logger,
        clients: &mut HashMap<Ipv6Addr, sled_agent_client::Client>,
        attachments: &[shared::AttachedSubnet],
    ) -> HashMap<SledUuid, SledSubnetDetails> {
        // Send to one sled at a time, all the attachments for its instances.
        //
        // We might want to reorder the mapping above to be only by VMM ID. That
        // would spread the work out across the different sleds, rather than
        // sending each sled all its subnets in a short time. But it's not clear
        // there's any meaningful difference at this point.
        let attachments_by_sled =
            group_attached_subnets_by_sled_and_vmm(attachments);
        let mut res = HashMap::<_, SledSubnetDetails>::new();
        for ((sled_id, sled_ip), attachments_by_sled) in
            attachments_by_sled.iter()
        {
            let details = res.entry(*sled_id).or_default();
            let Some(client) = clients.get(&sled_ip) else {
                error!(
                    log,
                    "no client for sled with attached subnets, skipping";
                    "sled_ip" => %sled_ip,
                );
                continue;
            };
            for (vmm_id, attachments) in attachments_by_sled.iter() {
                match client.vmm_put_attached_subnets(vmm_id, attachments).await
                {
                    Ok(_) => {
                        details.n_subnets += attachments.subnets.len();
                        debug!(
                            log,
                            "sent attached subnets to sled";
                            "n_subnets" => attachments.subnets.len(),
                            "sled_ip" => %sled_ip,
                            "vmm_id" => %vmm_id,
                        );
                    }
                    Err(e) => {
                        let err = InlineErrorChain::new(&e);
                        details.errors.push(err.to_string());
                        error!(
                            log,
                            "failed to send attached subnets to sled";
                            "sled_ip" => %sled_ip,
                            "vmm_id" => %vmm_id,
                            "error" => err,
                        );
                    }
                }
            }
        }
        res
    }
}

type VmmAttachedSubnetMap = HashMap<PropolisUuid, AttachedSubnets>;
type SledAttachedSubnetMap =
    HashMap<(SledUuid, Ipv6Addr), VmmAttachedSubnetMap>;

// Organize attached subnets first by sled, then by VMM on the sled.
//
// We really care about the instance ID, but the sled-agent API exposes a
// per-VMM endpoint for replacing attached subnets.
fn group_attached_subnets_by_sled_and_vmm(
    attachments: &[shared::AttachedSubnet],
) -> SledAttachedSubnetMap {
    let mut attachments_by_sled = SledAttachedSubnetMap::new();
    for attachment in attachments.iter() {
        attachments_by_sled
            .entry((attachment.sled_id, attachment.sled_ip))
            .or_default()
            .entry(attachment.vmm_id)
            .or_insert_with(|| AttachedSubnets { subnets: Vec::new() })
            .subnets
            .push(AttachedSubnet {
                subnet: attachment.subnet,
                is_external: matches!(
                    attachment.subnet_id,
                    AttachedSubnetId::External(_)
                ),
            });
    }
    attachments_by_sled
}

// Diff between existing and desired attached subnets on a Dendrite instance.
struct AttachedSubnetDiff<'a> {
    to_add: HashMap<&'a IpNet, &'a InstanceTarget>,
    to_remove: HashSet<&'a IpNet>,
}

impl<'a> AttachedSubnetDiff<'a> {
    fn new(
        existing: &'a HashMap<IpNet, InstanceTarget>,
        desired: &'a HashMap<IpNet, InstanceTarget>,
    ) -> AttachedSubnetDiff<'a> {
        let mut to_remove = HashSet::new();
        let mut to_add = HashMap::new();

        // Add all those in existing, but not desired, to `to_remove`
        for (subnet, target) in existing.iter() {
            match desired.get(subnet) {
                Some(desired_target) if desired_target == target => {}
                None | Some(_) => {
                    let _ = to_remove.insert(subnet);
                }
            }
        }

        // Add all those in desired, but not existing, to `to_add`.
        for (subnet, desired_target) in desired.iter() {
            match existing.get(subnet) {
                Some(target) if desired_target == target => {}
                None | Some(_) => {
                    let _ = to_add.insert(subnet, desired_target);
                }
            }
        }
        AttachedSubnetDiff { to_add, to_remove }
    }
}

impl BackgroundTask for Manager {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, Value> {
        // Do a page at a time of:
        //
        // - Fetch a page of attached subnets
        // - Construct Dendrite requests and push
        // - Construct sled-agent requests and push, need an API for this first
        async {
            let log = &opctx.log;
            info!(log, "starting attached subnet manager work");

            // Fetch Dendrite clients. We will need to move this into the loop
            // when we resolve #5201, since we'll look up Dendrite instances per
            // rack in that case.
            let maybe_dpd_clients = dpd_clients(&self.resolver, &log)
                .await
                .inspect_err(|e| {
                    error!(
                        log,
                        "failed to lookup Dendrite clients, will \
                        not be able to forward attachments to Dendrite";
                        "error" => %e
                    )
                })
                .ok();
            let mut sled_agent_clients = HashMap::new();

            let attachments = match self
                .datastore
                .list_all_attached_subnets_batched(opctx)
                .await
            {
                Ok(attachments) => {
                    debug!(
                        log,
                        "listed attached subnets";
                        "n_subnets" => attachments.len(),
                    );
                    attachments
                }
                Err(e) => {
                    let err = InlineErrorChain::new(&e);
                    error!(
                        log,
                        "failed to list attached subnets";
                        "error" => &err,
                    );
                    return json!({"err" : err.to_string()});
                }
            };

            let dendrite = match &maybe_dpd_clients {
                None => HashMap::new(),
                Some(clients) => {
                    self.send_attachments_to_dendrite(
                        &log,
                        &clients,
                        &attachments,
                    )
                    .await
                }
            };
            let sled = self
                .send_attachments_to_sled_agents(
                    &log,
                    &mut sled_agent_clients,
                    &attachments,
                )
                .await;
            let out = Result::<_, String>::Ok(AttachedSubnetManagerStatus {
                dendrite,
                sled,
            });
            json!(out)
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use dpd_client::types::{MacAddr, Vni};

    use super::*;

    #[test]
    fn attached_subnet_diff_works_with_empty_sets() {
        let desired = HashMap::new();
        let existing = HashMap::new();
        let AttachedSubnetDiff { to_add, to_remove } =
            AttachedSubnetDiff::new(&existing, &desired);
        assert!(to_add.is_empty());
        assert!(to_remove.is_empty());
    }

    #[test]
    fn attached_subnet_diff_removes_entries() {
        let existing = HashMap::from([(
            "10.0.0.0/16".parse().unwrap(),
            InstanceTarget {
                inner_mac: MacAddr { a: [0xa8, 0x40, 0x25, 0x00, 0x00, 0x00] },
                internal_ip: "fd00::1".parse().unwrap(),
                vni: Vni(100),
            },
        )]);
        let desired = HashMap::new();
        let AttachedSubnetDiff { to_add, to_remove } =
            AttachedSubnetDiff::new(&existing, &desired);
        assert!(to_add.is_empty());
        assert_eq!(to_remove, existing.keys().collect());
    }

    #[test]
    fn attached_subnet_diff_adds_entries() {
        let desired = HashMap::from([(
            "10.0.0.0/16".parse().unwrap(),
            InstanceTarget {
                inner_mac: MacAddr { a: [0xa8, 0x40, 0x25, 0x00, 0x00, 0x00] },
                internal_ip: "fd00::1".parse().unwrap(),
                vni: Vni(100),
            },
        )]);
        let existing = HashMap::new();
        let AttachedSubnetDiff { to_add, to_remove } =
            AttachedSubnetDiff::new(&existing, &desired);
        assert!(to_remove.is_empty());
        assert_eq!(to_add, desired.iter().collect());
    }

    #[test]
    fn attached_subnet_leaves_valid_entries() {
        let desired = HashMap::from([(
            "10.0.0.0/16".parse().unwrap(),
            InstanceTarget {
                inner_mac: MacAddr { a: [0xa8, 0x40, 0x25, 0x00, 0x00, 0x00] },
                internal_ip: "fd00::1".parse().unwrap(),
                vni: Vni(100),
            },
        )]);
        let existing = desired.clone();
        let AttachedSubnetDiff { to_add, to_remove } =
            AttachedSubnetDiff::new(&existing, &desired);
        assert!(to_remove.is_empty());
        assert!(to_add.is_empty());
    }

    #[test]
    fn attached_subnet_modifies_changed_entries() {
        let existing = HashMap::from([(
            "10.0.0.0/16".parse().unwrap(),
            InstanceTarget {
                inner_mac: MacAddr { a: [0xa8, 0x40, 0x25, 0xff, 0xff, 0xff] },
                internal_ip: "fd00::1".parse().unwrap(),
                vni: Vni(100),
            },
        )]);
        let desired = HashMap::from([(
            "10.0.0.0/16".parse().unwrap(),
            InstanceTarget {
                inner_mac: MacAddr { a: [0xa8, 0x40, 0x25, 0x00, 0x00, 0x00] },
                internal_ip: "fd00::1".parse().unwrap(),
                vni: Vni(100),
            },
        )]);
        let AttachedSubnetDiff { to_add, to_remove } =
            AttachedSubnetDiff::new(&existing, &desired);

        // We should remove everything, because the mapping we have is wrong.
        assert_eq!(to_remove, existing.keys().collect());

        // And add the entire new mapping.
        assert_eq!(to_add, desired.iter().collect());
    }
}
