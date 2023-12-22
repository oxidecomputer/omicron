// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Routines that manage instance-related networking state.

use crate::app::sagas::retry_until_known_result;
use ipnetwork::IpNetwork;
use ipnetwork::Ipv6Network;
use nexus_db_model::IpAttachState;
use nexus_db_model::Ipv4NatValues;
use nexus_db_model::Vni as DbVni;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::Ipv4Net;
use omicron_common::api::external::Ipv6Net;
use omicron_common::api::internal::nexus;
use omicron_common::api::internal::shared::SwitchLocation;
use sled_agent_client::types::DeleteVirtualNetworkInterfaceHost;
use sled_agent_client::types::SetVirtualNetworkInterfaceHost;
use std::collections::HashSet;
use std::str::FromStr;

use uuid::Uuid;

impl super::Nexus {
    /// Returns the set of switches with uplinks configured and boundary
    /// services enabled.
    pub(crate) async fn boundary_switches(
        &self,
        opctx: &OpContext,
    ) -> Result<HashSet<SwitchLocation>, Error> {
        let mut boundary_switches: HashSet<SwitchLocation> = HashSet::new();
        let uplinks = self.list_switch_ports_with_uplinks(opctx).await?;
        for uplink in &uplinks {
            let location: SwitchLocation =
                uplink.switch_location.parse().map_err(|_| {
                    Error::internal_error(&format!(
                        "invalid switch location in uplink config: {}",
                        uplink.switch_location
                    ))
                })?;
            boundary_switches.insert(location);
        }
        Ok(boundary_switches)
    }

    /// Ensures that V2P mappings exist that indicate that the instance with ID
    /// `instance_id` is resident on the sled with ID `sled_id`.
    pub(crate) async fn create_instance_v2p_mappings(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
        sled_id: Uuid,
    ) -> Result<(), Error> {
        info!(&self.log, "creating V2P mappings for instance";
              "instance_id" => %instance_id,
              "sled_id" => %sled_id);

        // For every sled that isn't the sled this instance was allocated to, create
        // a virtual to physical mapping for each of this instance's NICs.
        //
        // For the mappings to be correct, a few invariants must hold:
        //
        // - mappings must be set whenever an instance's sled changes (eg.
        //   during instance creation, migration, stop + start)
        //
        // - an instances' sled must not change while its corresponding mappings
        //   are being created
        //
        // - the same mapping creation must be broadcast to all sleds
        //
        // A more targeted approach would be to see what other instances share
        // the VPC this instance is in (or more generally, what instances should
        // have connectivity to this one), see what sleds those are allocated
        // to, and only create V2P mappings for those sleds.
        //
        // There's additional work with this approach:
        //
        // - it means that delete calls are required as well as set calls,
        //   meaning that now the ordering of those matters (this may also
        //   necessitate a generation number for V2P mappings)
        //
        // - V2P mappings have to be bidirectional in order for both instances's
        //   packets to make a round trip. This isn't a problem with the
        //   broadcast approach because one of the sides will exist already, but
        //   it is something to orchestrate with a more targeted approach.
        //
        // TODO-correctness Default firewall rules currently will block
        // instances in different VPCs from connecting to each other. If it ever
        // stops doing this, the broadcast approach will create V2P mappings
        // that shouldn't exist.
        let (.., authz_instance) = LookupPath::new(&opctx, &self.db_datastore)
            .instance_id(instance_id)
            .lookup_for(authz::Action::Read)
            .await?;

        let instance_nics = self
            .db_datastore
            .derive_guest_network_interface_info(&opctx, &authz_instance)
            .await?;

        // Look up the supplied sled's physical host IP.
        let physical_host_ip =
            *self.sled_lookup(&self.opctx_alloc, &sled_id)?.fetch().await?.1.ip;

        let mut last_sled_id: Option<Uuid> = None;
        loop {
            let pagparams = DataPageParams {
                marker: last_sled_id.as_ref(),
                direction: dropshot::PaginationOrder::Ascending,
                limit: std::num::NonZeroU32::new(10).unwrap(),
            };

            let sleds_page =
                self.sled_list(&self.opctx_alloc, &pagparams).await?;
            let mut join_handles =
                Vec::with_capacity(sleds_page.len() * instance_nics.len());

            for sled in &sleds_page {
                // set_v2p not required for sled instance was allocated to, OPTE
                // currently does that automatically
                //
                // TODO(#3107): Remove this when XDE stops creating mappings
                // implicitly.
                if sled.id() == sled_id {
                    continue;
                }

                for nic in &instance_nics {
                    let client = self.sled_client(&sled.id()).await?;
                    let nic_id = nic.id;
                    let mapping = SetVirtualNetworkInterfaceHost {
                        virtual_ip: nic.ip,
                        virtual_mac: nic.mac.clone(),
                        physical_host_ip,
                        vni: nic.vni.clone(),
                    };

                    let log = self.log.clone();

                    // This function is idempotent: calling the set_v2p ioctl with
                    // the same information is a no-op.
                    join_handles.push(tokio::spawn(futures::future::lazy(
                        move |_ctx| async move {
                            retry_until_known_result(&log, || async {
                                client.set_v2p(&nic_id, &mapping).await
                            })
                            .await
                        },
                    )));
                }
            }

            // Concurrently run each future to completion, but return the last
            // error seen.
            let mut error = None;
            for join_handle in join_handles {
                let result = join_handle
                    .await
                    .map_err(|e| Error::internal_error(&e.to_string()))?
                    .await;

                if result.is_err() {
                    error!(self.log, "{:?}", result);
                    error = Some(result);
                }
            }
            if let Some(e) = error {
                return e.map(|_| ()).map_err(|e| e.into());
            }

            if sleds_page.len() < 10 {
                break;
            }

            if let Some(last) = sleds_page.last() {
                last_sled_id = Some(last.id());
            }
        }

        Ok(())
    }

    /// Ensure that the necessary v2p mappings for an instance are deleted
    pub(crate) async fn delete_instance_v2p_mappings(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<(), Error> {
        // For every sled that isn't the sled this instance was allocated to, delete
        // the virtual to physical mapping for each of this instance's NICs. If
        // there isn't a V2P mapping, del_v2p should be a no-op.
        let (.., authz_instance) = LookupPath::new(&opctx, &self.db_datastore)
            .instance_id(instance_id)
            .lookup_for(authz::Action::Read)
            .await?;

        let instance_nics = self
            .db_datastore
            .derive_guest_network_interface_info(&opctx, &authz_instance)
            .await?;

        let mut last_sled_id: Option<Uuid> = None;

        loop {
            let pagparams = DataPageParams {
                marker: last_sled_id.as_ref(),
                direction: dropshot::PaginationOrder::Ascending,
                limit: std::num::NonZeroU32::new(10).unwrap(),
            };

            let sleds_page =
                self.sled_list(&self.opctx_alloc, &pagparams).await?;
            let mut join_handles =
                Vec::with_capacity(sleds_page.len() * instance_nics.len());

            for sled in &sleds_page {
                for nic in &instance_nics {
                    let client = self.sled_client(&sled.id()).await?;
                    let nic_id = nic.id;
                    let mapping = DeleteVirtualNetworkInterfaceHost {
                        virtual_ip: nic.ip,
                        vni: nic.vni.clone(),
                    };

                    let log = self.log.clone();

                    // This function is idempotent: calling the set_v2p ioctl with
                    // the same information is a no-op.
                    join_handles.push(tokio::spawn(futures::future::lazy(
                        move |_ctx| async move {
                            retry_until_known_result(&log, || async {
                                client.del_v2p(&nic_id, &mapping).await
                            })
                            .await
                        },
                    )));
                }
            }

            // Concurrently run each future to completion, but return the last
            // error seen.
            let mut error = None;
            for join_handle in join_handles {
                let result = join_handle
                    .await
                    .map_err(|e| Error::internal_error(&e.to_string()))?
                    .await;

                if result.is_err() {
                    error!(self.log, "{:?}", result);
                    error = Some(result);
                }
            }
            if let Some(e) = error {
                return e.map(|_| ()).map_err(|e| e.into());
            }

            if sleds_page.len() < 10 {
                break;
            }

            if let Some(last) = sleds_page.last() {
                last_sled_id = Some(last.id());
            }
        }

        Ok(())
    }

    /// Ensures that the Dendrite configuration for the supplied instance is
    /// up-to-date.
    ///
    /// # Parameters
    ///
    /// - `opctx`: An operation context that grants read and list-children
    ///   permissions on the identified instance.
    /// - `instance_id`: The ID of the instance to act on.
    /// - `sled_ip_address`: The internal IP address assigned to the sled's
    ///   sled agent.
    /// - `ip_filter`: An optional filter on the index into the instance's
    ///   external IP array.
    ///   - If this is `Some(id)`, this routine configures DPD state for only the
    ///     external IP with `id` in the collection returned from CRDB.
    ///   - If this is `None`, this routine configures DPD for all external
    ///     IPs.
    pub(crate) async fn instance_ensure_dpd_config(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
        sled_ip_address: &std::net::SocketAddrV6,
        ip_filter: Option<Uuid>,
    ) -> Result<(), Error> {
        let log = &self.log;

        info!(log, "looking up instance's primary network interface";
              "instance_id" => %instance_id);

        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .instance_id(instance_id)
            .lookup_for(authz::Action::ListChildren)
            .await?;

        // All external IPs map to the primary network interface, so find that
        // interface. If there is no such interface, there's no way to route
        // traffic destined to those IPs, so there's nothing to configure and
        // it's safe to return early.
        let network_interface = match self
            .db_datastore
            .derive_guest_network_interface_info(&opctx, &authz_instance)
            .await?
            .into_iter()
            .find(|interface| interface.primary)
        {
            Some(interface) => interface,
            None => {
                info!(log, "Instance has no primary network interface";
                      "instance_id" => %instance_id);
                return Ok(());
            }
        };

        let mac_address =
            macaddr::MacAddr6::from_str(&network_interface.mac.to_string())
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "failed to convert mac address: {e}"
                    ))
                })?;

        info!(log, "looking up instance's external IPs";
              "instance_id" => %instance_id);

        let ips = self
            .db_datastore
            .instance_lookup_external_ips(&opctx, instance_id)
            .await?;

        let (ips_of_interest, must_all_be_attached) =
            if let Some(wanted_id) = ip_filter {
                if let Some(ip) = ips.iter().find(|v| v.id == wanted_id) {
                    (std::slice::from_ref(ip), false)
                } else {
                    return Err(Error::internal_error(&format!(
                    "failed to find external ip address with id: {wanted_id}",
                )));
                }
            } else {
                (&ips[..], true)
            };

        // This is performed so that an IP attach/detach will block the
        // instance_start saga. Return service unavailable to indicate
        // the request is retryable.
        if ips_of_interest
            .iter()
            .any(|ip| {
                must_all_be_attached && ip.state != IpAttachState::Attached
            })
        {
            return Err(Error::unavail(
                "cannot push all DPD state: IP attach/detach in progress",
            ));
        }

        let sled_address =
            Ipv6Net(Ipv6Network::new(*sled_ip_address.ip(), 128).unwrap());

        // Querying boundary switches also requires fleet access and the use of the
        // instance allocator context.
        let boundary_switches =
            self.boundary_switches(&self.opctx_alloc).await?;

        for switch in &boundary_switches {
            debug!(&self.log, "notifying dendrite of updates";
                       "instance_id" => %authz_instance.id(),
                       "switch" => switch.to_string());

            let dpd_client = self.dpd_clients.get(switch).ok_or_else(|| {
                Error::internal_error(&format!(
                    "unable to find dendrite client for {switch}"
                ))
            })?;

            for external_ip in ips_of_interest {
                // For each external ip, add a nat entry to the database
                self.ensure_nat_entry(
                    external_ip,
                    sled_address,
                    &network_interface,
                    mac_address,
                    opctx,
                )
                .await?;
            }

            // Notify dendrite that there are changes for it to reconcile.
            // In the event of a failure to notify dendrite, we'll log an error
            // and rely on dendrite's RPW timer to catch it up.
            if let Err(e) = dpd_client.ipv4_nat_trigger_update().await {
                error!(self.log, "failed to notify dendrite of nat updates"; "error" => ?e);
            };
        }

        Ok(())
    }

    async fn ensure_nat_entry(
        &self,
        target_ip: &nexus_db_model::ExternalIp,
        sled_address: Ipv6Net,
        network_interface: &sled_agent_client::types::NetworkInterface,
        mac_address: macaddr::MacAddr6,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        match target_ip.ip {
            IpNetwork::V4(v4net) => {
                let nat_entry = Ipv4NatValues {
                    external_address: Ipv4Net(v4net).into(),
                    first_port: target_ip.first_port,
                    last_port: target_ip.last_port,
                    sled_address: sled_address.into(),
                    vni: DbVni(network_interface.vni.clone().into()),
                    mac: nexus_db_model::MacAddr(
                        omicron_common::api::external::MacAddr(mac_address),
                    ),
                };
                self.db_datastore
                    .ensure_ipv4_nat_entry(opctx, nat_entry)
                    .await?;
            }
            IpNetwork::V6(_v6net) => {
                // TODO: implement handling of v6 nat.
                return Err(Error::InternalError {
                    internal_message: "ipv6 nat is not yet implemented".into(),
                });
            }
        };
        Ok(())
    }

    /// Attempts to delete all of the Dendrite NAT configuration for the
    /// instance identified by `authz_instance`.
    ///
    /// # Return value
    ///
    /// - `Ok(())` if all NAT entries were successfully deleted.
    /// - If an operation fails before this routine begins to walk and delete
    ///   individual NAT entries, this routine returns `Err` and reports that
    ///   error.
    /// - If an operation fails while this routine is walking NAT entries, it
    ///   will continue trying to delete subsequent entries but will return the
    ///   first error it encountered.
    /// - `ip_filter`: An optional filter on the index into the instance's
    ///   external IP array.
    ///   - If this is `Some(id)`, this routine configures DPD state for only the
    ///     external IP with `id` in the collection returned from CRDB.
    ///   - If this is `None`, this routine configures DPD for all external
    ///     IPs.
    pub(crate) async fn instance_delete_dpd_config(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        ip_filter: Option<Uuid>,
    ) -> Result<(), Error> {
        let log = &self.log;
        let instance_id = authz_instance.id();

        info!(log, "deleting instance dpd configuration";
              "instance_id" => %instance_id);

        let external_ips = self
            .db_datastore
            .instance_lookup_external_ips(opctx, instance_id)
            .await?;

        let ips_of_interest = if let Some(wanted_id) = ip_filter {
            if let Some(ip) = external_ips.iter().find(|v| v.id == wanted_id) {
                std::slice::from_ref(ip)
            } else {
                return Err(Error::internal_error(&format!(
                    "failed to find external ip address with id: {wanted_id}",
                )));
            }
        } else {
            &external_ips[..]
        };

        let mut errors = vec![];
        for entry in ips_of_interest {
            // Soft delete the NAT entry
            match self
                .db_datastore
                .ipv4_nat_delete_by_external_ip(&opctx, &entry)
                .await
            {
                Ok(_) => Ok(()),
                Err(err) => match err {
                    Error::ObjectNotFound { .. } => {
                        warn!(log, "no matching nat entries to soft delete");
                        Ok(())
                    }
                    _ => {
                        let message = format!(
                            "failed to delete nat entry due to error: {err:?}"
                        );
                        error!(log, "{}", message);
                        Err(Error::internal_error(&message))
                    }
                },
            }?;
        }

        let boundary_switches =
            self.boundary_switches(&self.opctx_alloc).await?;

        for switch in &boundary_switches {
            debug!(&self.log, "notifying dendrite of updates";
                       "instance_id" => %authz_instance.id(),
                       "switch" => switch.to_string());

            let client_result = self.dpd_clients.get(switch).ok_or_else(|| {
                Error::internal_error(&format!(
                    "unable to find dendrite client for {switch}"
                ))
            });

            let dpd_client = match client_result {
                Ok(client) => client,
                Err(new_error) => {
                    errors.push(new_error);
                    continue;
                }
            };

            // Notify dendrite that there are changes for it to reconcile.
            // In the event of a failure to notify dendrite, we'll log an error
            // and rely on dendrite's RPW timer to catch it up.
            if let Err(e) = dpd_client.ipv4_nat_trigger_update().await {
                error!(self.log, "failed to notify dendrite of nat updates"; "error" => ?e);
            };
        }

        if let Some(e) = errors.into_iter().next() {
            return Err(e);
        }

        Ok(())
    }

    /// Deletes an instance's OPTE V2P mappings and the boundary switch NAT
    /// entries for its external IPs.
    ///
    /// This routine returns immediately upon encountering any errors (and will
    /// not try to destroy any more objects after the point of failure).
    async fn clear_instance_networking_state(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> Result<(), Error> {
        self.delete_instance_v2p_mappings(opctx, authz_instance.id()).await?;

        let external_ips = self
            .datastore()
            .instance_lookup_external_ips(opctx, authz_instance.id())
            .await?;

        let boundary_switches = self.boundary_switches(opctx).await?;
        for external_ip in external_ips {
            match self
                .db_datastore
                .ipv4_nat_delete_by_external_ip(&opctx, &external_ip)
                .await
            {
                Ok(_) => Ok(()),
                Err(err) => match err {
                    Error::ObjectNotFound { .. } => {
                        warn!(
                            self.log,
                            "no matching nat entries to soft delete"
                        );
                        Ok(())
                    }
                    _ => {
                        let message = format!(
                            "failed to delete nat entry due to error: {err:?}"
                        );
                        error!(self.log, "{}", message);
                        Err(Error::internal_error(&message))
                    }
                },
            }?;
        }

        for switch in &boundary_switches {
            debug!(&self.log, "notifying dendrite of updates";
                       "instance_id" => %authz_instance.id(),
                       "switch" => switch.to_string());

            let dpd_client = self.dpd_clients.get(switch).ok_or_else(|| {
                Error::internal_error(&format!(
                    "unable to find dendrite client for {switch}"
                ))
            })?;

            // Notify dendrite that there are changes for it to reconcile.
            // In the event of a failure to notify dendrite, we'll log an error
            // and rely on dendrite's RPW timer to catch it up.
            if let Err(e) = dpd_client.ipv4_nat_trigger_update().await {
                error!(self.log, "failed to notify dendrite of nat updates"; "error" => ?e);
            };
        }

        Ok(())
    }

    /// Given old and new instance runtime states, determines the desired
    /// networking configuration for a given instance and ensures it has been
    /// propagated to all relevant sleds.
    ///
    /// # Arguments
    ///
    /// - opctx: An operation context for this operation.
    /// - authz_instance: A resolved authorization context for the instance of
    ///   interest.
    /// - prev_instance_state: The most-recently-recorded instance runtime
    ///   state for this instance.
    /// - new_instance_state: The instance state that the caller of this routine
    ///   has observed and that should be used to set up this instance's
    ///   networking state.
    ///
    /// # Return value
    ///
    /// `Ok(())` if this routine completed all the operations it wanted to
    /// complete, or an appropriate `Err` otherwise.
    pub(crate) async fn ensure_updated_instance_network_config(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        prev_instance_state: &db::model::InstanceRuntimeState,
        new_instance_state: &nexus::InstanceRuntimeState,
    ) -> Result<(), Error> {
        let log = &self.log;
        let instance_id = authz_instance.id();

        // If this instance update is stale, do nothing, since the superseding
        // update may have allowed the instance's location to change further.
        if prev_instance_state.gen >= new_instance_state.gen.into() {
            debug!(log,
                   "instance state generation already advanced, \
                   won't touch network config";
                   "instance_id" => %instance_id);

            return Ok(());
        }

        // If this update will retire the instance's active VMM, delete its
        // networking state. It will be re-established the next time the
        // instance starts.
        if new_instance_state.propolis_id.is_none() {
            info!(log,
                  "instance cleared its Propolis ID, cleaning network config";
                  "instance_id" => %instance_id,
                  "propolis_id" => ?prev_instance_state.propolis_id);

            self.clear_instance_networking_state(opctx, authz_instance).await?;
            return Ok(());
        }

        // If the instance still has a migration in progress, don't change
        // any networking state until an update arrives that retires that
        // migration.
        //
        // This is needed to avoid the following race:
        //
        // 1. Migration from S to T completes.
        // 2. Migration source sends an update that changes the instance's
        //    active VMM but leaves the migration ID in place.
        // 3. Meanwhile, migration target sends an update that changes the
        //    instance's active VMM and clears the migration ID.
        // 4. The migration target's call updates networking state and commits
        //    the new instance record.
        // 5. The instance migrates from T to T' and Nexus applies networking
        //    configuration reflecting that the instance is on T'.
        // 6. The update in step 2 applies configuration saying the instance
        //    is on sled T.
        if new_instance_state.migration_id.is_some() {
            debug!(log,
                   "instance still has a migration in progress, won't touch \
                   network config";
                   "instance_id" => %instance_id,
                   "migration_id" => ?new_instance_state.migration_id);

            return Ok(());
        }

        let new_propolis_id = new_instance_state.propolis_id.unwrap();

        // Updates that end live migration need to push OPTE V2P state even if
        // the instance's active sled did not change (see below).
        let migration_retired = prev_instance_state.migration_id.is_some()
            && new_instance_state.migration_id.is_none();

        if (prev_instance_state.propolis_id == new_instance_state.propolis_id)
            && !migration_retired
        {
            debug!(log, "instance didn't move, won't touch network config";
                   "instance_id" => %instance_id);

            return Ok(());
        }

        // Either the instance moved from one sled to another, or it attempted
        // to migrate and failed. Ensure the correct networking configuration
        // exists for its current home.
        //
        // TODO(#3107) This is necessary even if the instance didn't move,
        // because registering a migration target on a sled creates OPTE ports
        // for its VNICs, and that creates new V2P mappings on that sled that
        // place the relevant virtual IPs on the local sled. Once OPTE stops
        // creating these mappings, this path only needs to be taken if an
        // instance has changed sleds.
        let new_sled_id = match self
            .db_datastore
            .vmm_fetch(&opctx, authz_instance, &new_propolis_id)
            .await
        {
            Ok(vmm) => vmm.sled_id,

            // A VMM in the active position should never be destroyed. If the
            // sled sending this message is the owner of the instance's last
            // active VMM and is destroying it, it should also have retired that
            // VMM.
            Err(Error::ObjectNotFound { .. }) => {
                error!(log, "instance's active vmm unexpectedly not found";
                       "instance_id" => %instance_id,
                       "propolis_id" => %new_propolis_id);

                return Ok(());
            }

            Err(e) => return Err(e),
        };

        self.create_instance_v2p_mappings(opctx, instance_id, new_sled_id)
            .await?;

        let (.., sled) = LookupPath::new(opctx, &self.db_datastore)
            .sled_id(new_sled_id)
            .fetch()
            .await?;

        self.instance_ensure_dpd_config(
            opctx,
            instance_id,
            &sled.address(),
            None,
        )
        .await?;

        Ok(())
    }
}
