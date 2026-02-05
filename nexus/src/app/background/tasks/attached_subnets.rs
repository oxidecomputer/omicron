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
use sled_agent_client::types::AttachedSubnetKind;
use sled_agent_client::types::AttachedSubnets;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
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
        // That means we have to do the diff on the client side, to compute the
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
        opctx: &OpContext,
        attachments: &[shared::AttachedSubnet],
    ) -> HashMap<SledUuid, SledSubnetDetails> {
        // Map of all the clients needed to send data to sled agents. Update
        // this lazily while we iterate below.
        let mut clients = HashMap::new();

        // Send to one sled at a time, all the attachments for its instances.
        //
        // We might want to reorder the mapping above to be only by VMM ID. That
        // would spread the work out across the different sleds, rather than
        // sending each sled all its subnets in a short time. But it's not clear
        // there's any meaningful difference at this point.
        let attachments_by_sled =
            group_attached_subnets_by_sled_and_vmm(attachments);
        let mut res = HashMap::<_, SledSubnetDetails>::new();
        'sleds: for ((sled_id, sled_ip), attachments_by_sled) in
            attachments_by_sled.iter()
        {
            let details = res.entry(*sled_id).or_default();

            // Look up the client or get it from the datastore if needed.
            let client = match clients.entry(*sled_id) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    match nexus_networking::sled_client(
                        &self.datastore,
                        opctx,
                        *sled_id,
                        log,
                    )
                    .await
                    {
                        Ok(client) => {
                            debug!(
                                log,
                                "fetched new sled client";
                                "sled_id" => %sled_id,
                                "sled_ip" => %sled_ip,
                            );
                            entry.insert(client)
                        }
                        Err(e) => {
                            let e = InlineErrorChain::new(&e);
                            let n_vmms = attachments_by_sled.len();
                            let n_subnets: usize = attachments_by_sled
                                .values()
                                .map(|att| att.subnets.len())
                                .sum();
                            let message = format!(
                                "failed to lookup client for sled with attached \
                                subnets, sled_id={sled_id}, sled_ip={sled_ip}, \
                                n_vmms={n_vmms}, n_subnets={n_subnets}, error={e}"
                            );
                            details.errors.push(message);
                            error!(
                                log,
                                "no client for sled with attached subnets";
                                "sled_id" => %sled_id,
                                "sled_ip" => %sled_ip,
                                "n_vmms" => n_vmms,
                                "n_subnets" => n_subnets,
                                "error" => &e,
                            );
                            continue 'sleds;
                        }
                    }
                }
            };

            // Send all the attached subnets, per-VMM, and update our counters.
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
                kind: match attachment.subnet_id {
                    AttachedSubnetId::External(_) => {
                        AttachedSubnetKind::External
                    }
                    AttachedSubnetId::Vpc(_) => AttachedSubnetKind::Vpc,
                },
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
            let mut out = AttachedSubnetManagerStatus::default();

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
                    out.db_error = Some(err.to_string());
                    return json!(out);
                }
            };

            out.dendrite = match &maybe_dpd_clients {
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
            out.sled = self
                .send_attachments_to_sled_agents(&log, opctx, &attachments)
                .await;
            json!(out)
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use async_bb8_diesel::AsyncRunQueryDsl as _;
    use chrono::Utc;
    use diesel::ExpressionMethods as _;
    use diesel::QueryDsl as _;
    use dpd_client::types::MacAddr;
    use dpd_client::types::Vni;
    use nexus_db_model::IpAttachState;
    use nexus_db_schema::schema::external_subnet::dsl;
    use nexus_test_utils::resource_helpers::create_default_ip_pools;
    use nexus_test_utils::resource_helpers::create_external_subnet_in_pool;
    use nexus_test_utils::resource_helpers::create_instance;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::create_subnet_pool;
    use nexus_test_utils::resource_helpers::create_subnet_pool_member;
    use nexus_test_utils_macros::nexus_test;
    use omicron_common::address::IpVersion;
    use omicron_common::api::internal::shared;
    use omicron_uuid_kinds::GenericUuid;
    use sled_agent_types::attached_subnet::AttachedSubnetKind;
    use std::collections::BTreeSet;

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

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    // NOTE: This is also a test of the datastore method
    // `list_all_attached_subnets_batched`, but it relies on enough related
    // records that it's easier to write here. This task is also the only
    // consume right now.
    #[nexus_test(server = crate::Server)]
    async fn test_attached_subnet_manager(cptestctx: &ControlPlaneTestContext) {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );
        let mut task =
            Manager::new(nexus.resolver().clone(), datastore.clone());

        // Create a resource hierarchy.
        let _subnet_pool =
            create_subnet_pool(client, "apple", IpVersion::V6).await;
        let _member = create_subnet_pool_member(
            client,
            "apple",
            "2001:db8::/48".parse().unwrap(),
        )
        .await;
        let (_v4_pool, _v6_pool) = create_default_ip_pools(client).await;
        let _project = create_project(client, "banana").await;

        // Now let's create some instances
        let n_instances = 6;
        let mut instances = Vec::with_capacity(n_instances);
        for i in 0..n_instances {
            let instance =
                create_instance(client, "banana", &format!("mango-{i}")).await;
            instances.push(instance);
        }

        // And some eternal subnets.
        let n_subnets = 4;
        let n_to_attach = 2;
        let mut subnets = Vec::with_capacity(n_subnets);
        for i in 0..n_subnets {
            let subnet = create_external_subnet_in_pool(
                client,
                "apple",
                "banana",
                &format!("plum-{i}"),
                64,
            )
            .await;
            subnets.push(subnet);
        }

        // To start, nothing should be marked as attached.
        let attached =
            datastore.list_all_attached_subnets_batched(&opctx).await.unwrap();
        assert!(
            attached.is_empty(),
            "No subnets should be attached at this point"
        );

        // And the task should report the same thing.
        let result = task.activate(&opctx).await;
        let result = result.as_object().expect("should be a JSON object");
        assert_task_result_is_empty(&result);

        // Mark a few of them in a transitional state, as if the saga for
        // attaching them has partially run. These should still now show up.
        for (inst, sub) in
            instances.iter().take(n_to_attach).zip(subnets.iter())
        {
            let n_rows =
                diesel::update(dsl::external_subnet.find(sub.identity.id))
                    .set((
                        dsl::instance_id.eq(inst.identity.id),
                        dsl::attach_state.eq(IpAttachState::Attaching),
                        dsl::time_modified.eq(Utc::now()),
                    ))
                    .execute_async(
                        &*datastore.pool_connection_for_tests().await.unwrap(),
                    )
                    .await
                    .unwrap();
            assert_eq!(n_rows, 1);
        }
        let attached =
            datastore.list_all_attached_subnets_batched(&opctx).await.unwrap();
        assert!(
            attached.is_empty(),
            "No subnets should be attached at this point"
        );

        // And the task should still report the same thing.
        let result = task.activate(&opctx).await;
        let result = result.as_object().expect("should be a JSON object");
        assert_task_result_is_empty(result);

        // "Complete" the attachment above, and we should see them
        for (_inst, sub) in
            instances.iter().take(n_to_attach).zip(subnets.iter())
        {
            let n_rows =
                diesel::update(dsl::external_subnet.find(sub.identity.id))
                    .set((
                        dsl::attach_state.eq(IpAttachState::Attached),
                        dsl::time_modified.eq(Utc::now()),
                    ))
                    .execute_async(
                        &*datastore.pool_connection_for_tests().await.unwrap(),
                    )
                    .await
                    .unwrap();
            assert_eq!(n_rows, 1);
        }
        let attached =
            datastore.list_all_attached_subnets_batched(&opctx).await.unwrap();
        assert_eq!(attached.len(), n_to_attach);
        assert_eq!(
            attached
                .iter()
                .map(|att| att.instance_id.into_untyped_uuid())
                .collect::<BTreeSet<_>>(),
            instances
                .iter()
                .take(n_to_attach)
                .map(|inst| inst.identity.id)
                .collect::<BTreeSet<_>>(),
            "Attached subnets aren't attached to the expected instances"
        );

        // The task should also report sending the same items.
        let result = task.activate(&opctx).await;
        let result = result.as_object().expect("should be a JSON object");
        assert_task_result_has(result, &attached, &attached, &[]);

        // And the sled agent itself should have records for these subnets.
        {
            let sa = &cptestctx.sled_agents[0];
            assert!(
                attached.iter().all(|att| att.sled_id == sa.sled_agent_id())
            );
            let sa_subnets = sa.sled_agent().attached_subnets.lock().unwrap();
            assert_eq!(sa_subnets.len(), attached.len());
            for att in attached.iter() {
                let on_sled = sa_subnets.get(&att.vmm_id).unwrap();
                assert_eq!(on_sled.len(), 1);
                let attached_on_sled = on_sled.iter().next().unwrap();
                assert!(matches!(
                    attached_on_sled.kind,
                    AttachedSubnetKind::External
                ));
                assert_eq!(attached_on_sled.subnet, att.subnet);
            }
        }

        // Now detach the one on the first instance, and check that the
        // sled-agent no longer has it.
        let removed = attached
            .into_iter()
            .find(|att| {
                att.instance_id.into_untyped_uuid() == instances[0].identity.id
            })
            .unwrap();
        let AttachedSubnetId::External(removed_id) = &removed.subnet_id else {
            panic!("All subnets are external right now");
        };
        let n_rows = diesel::update(
            dsl::external_subnet.find(removed_id.into_untyped_uuid()),
        )
        .set((
            dsl::attach_state.eq(IpAttachState::Detached),
            dsl::instance_id.eq(Option::<uuid::Uuid>::None),
            dsl::time_modified.eq(Utc::now()),
        ))
        .execute_async(&*datastore.pool_connection_for_tests().await.unwrap())
        .await
        .unwrap();
        assert_eq!(n_rows, 1);
        let attached =
            datastore.list_all_attached_subnets_batched(&opctx).await.unwrap();
        assert_eq!(attached.len(), 1);
        assert_eq!(
            attached[0].instance_id.into_untyped_uuid(),
            instances[1].identity.id,
            "Attached subnets aren't attached to the expected instances"
        );

        // We'd like to test that the sled-agent has exactly what we send it.
        // That's not really possible, nor necessary. Attached subnets get
        // removed from the sled-agent when they're detached in two scenarios:
        //
        // - The subnet detach saga runs
        // - An instance delete saga runs
        //
        // In both of those cases, the attachment will go away. It's also not
        // possible for the saga to succeed and the sled-agent to retain the
        // attachment. (Absent a bug!) If we fail to run the part of the saga
        // that deletes the attachment, the whole saga would fail and unwind.
        //
        // So we can simulate the subnet-detach operation above we just ran by
        // deleting the relevant attachment from the simulated sled agent.
        {
            cptestctx.sled_agents[0]
                .sled_agent()
                .attached_subnets
                .lock()
                .unwrap()
                .get_mut(&removed.vmm_id)
                .expect("Should still have mapping for this VMM")
                .remove(&removed.subnet)
                .expect("Should have removed an actual mapping");
        }

        // The task should also report sending the same items.
        let result = task.activate(&opctx).await;
        let result = result.as_object().expect("should be a JSON object");
        assert_task_result_has(result, &attached, &[], &[removed]);

        // And the sled agent itself should have a record for this one attached
        // subnet.
        {
            let sa = &cptestctx.sled_agents[0];
            assert!(
                attached.iter().all(|att| att.sled_id == sa.sled_agent_id())
            );
            let sa_subnets = sa.sled_agent().attached_subnets.lock().unwrap();

            // It should still have a mapping for the _instance_ we detached the
            // subnet from, but that should be empty.
            assert_eq!(sa_subnets.len(), attached.len() + 1);
            assert!(sa_subnets.get(&removed.vmm_id).unwrap().is_empty());

            // All the attached subnets should still be there.
            for att in attached.iter() {
                let on_sled = sa_subnets.get(&att.vmm_id).unwrap();
                assert_eq!(on_sled.len(), 1);
                let attached_on_sled = on_sled.iter().next().unwrap();
                assert!(matches!(
                    attached_on_sled.kind,
                    AttachedSubnetKind::External
                ));
                assert_eq!(attached_on_sled.subnet, att.subnet);
            }
        }
    }

    fn assert_task_result_has(
        result: &serde_json::Map<String, serde_json::Value>,
        attached: &[shared::AttachedSubnet],
        added: &[shared::AttachedSubnet],
        removed: &[shared::AttachedSubnet],
    ) {
        // No errors
        assert!(result.contains_key("db_error"));
        assert!(result["db_error"].is_null());

        // Dendrite should have added all the mappings.
        assert!(result.contains_key("dendrite"));
        let dendrite = result["dendrite"].as_object().unwrap();
        let switch0 = dendrite["switch0"].as_object().unwrap();
        assert!(switch0["errors"].as_array().unwrap().is_empty());
        assert_eq!(
            switch0["n_subnets_added"].as_number().unwrap().as_u64().unwrap(),
            u64::try_from(added.len()).unwrap()
        );
        assert_eq!(
            switch0["n_subnets_removed"].as_number().unwrap().as_u64().unwrap(),
            u64::try_from(removed.len()).unwrap(),
        );
        assert_eq!(
            switch0["n_total_subnets"].as_number().unwrap().as_u64().unwrap(),
            u64::try_from(attached.len()).unwrap(),
        );

        // And we should have one sled, with `n_to_attach` attached subnets.
        assert!(result.contains_key("sled"));
        let sled = result["sled"].as_object().unwrap();
        let sled_id = attached[0].sled_id.to_string();
        assert!(sled.contains_key(&sled_id));
        let this_sled = sled[&sled_id].as_object().unwrap();
        assert!(this_sled["errors"].as_array().unwrap().is_empty());
        assert_eq!(
            this_sled["n_subnets"].as_number().unwrap().as_u64().unwrap(),
            u64::try_from(attached.len()).unwrap()
        );
    }

    fn assert_task_result_is_empty(
        result: &serde_json::Map<String, serde_json::Value>,
    ) {
        // No errors.
        assert!(result.contains_key("db_error"));
        assert!(result["db_error"].is_null());

        // We have a dendrite key with a switch, but there are no mappings.
        assert!(result.contains_key("dendrite"));
        let dendrite = result["dendrite"].as_object().unwrap();
        let switch0 = dendrite["switch0"].as_object().unwrap();
        assert!(switch0["errors"].as_array().unwrap().is_empty());
        assert_eq!(
            switch0["n_subnets_added"].as_number().unwrap().as_u64().unwrap(),
            0
        );
        assert_eq!(
            switch0["n_subnets_removed"].as_number().unwrap().as_u64().unwrap(),
            0
        );
        assert_eq!(
            switch0["n_total_subnets"].as_number().unwrap().as_u64().unwrap(),
            0
        );

        // We have no sleds, because there are no active VMMs.
        assert!(result.contains_key("sled"));
        assert!(result["sled"].as_object().unwrap().is_empty());
    }
}
