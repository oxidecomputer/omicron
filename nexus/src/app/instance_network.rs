// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Routines that manage instance-related networking state.

use crate::app::sagas::retry_until_known_result;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::internal::shared::SwitchLocation;
use sled_agent_client::types::SetVirtualNetworkInterfaceHost;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
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
        let (.., authz_instance, db_instance) =
            LookupPath::new(&opctx, &self.db_datastore)
                .instance_id(instance_id)
                .fetch_for(authz::Action::Read)
                .await?;

        let instance_nics = self
            .db_datastore
            .derive_guest_network_interface_info(&opctx, &authz_instance)
            .await?;

        // Lookup the physical host IP of the sled hosting this instance
        let instance_sled_id = db_instance.runtime().sled_id;
        let physical_host_ip = *self
            .sled_lookup(&self.opctx_alloc, &instance_sled_id)?
            .fetch()
            .await?
            .1
            .ip;

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
                // del_v2p not required for sled instance was allocated to, OPTE
                // currently does that automatically
                //
                // TODO(#3107): Remove this when XDE stops deleting mappings
                // implicitly.
                if sled.id() == instance_sled_id {
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
    /// - `ip_index_filter`: An optional filter on the index into the instance's
    ///   external IP array.
    ///   - If this is `Some(n)`, this routine configures DPD state for only the
    ///     Nth external IP in the collection returned from CRDB. The caller is
    ///     responsible for ensuring that the IP collection has stable indices
    ///     when making this call.
    ///   - If this is `None`, this routine configures DPD for all external
    ///     IPs.
    pub(crate) async fn instance_ensure_dpd_config(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
        sled_ip_address: &std::net::SocketAddrV6,
        ip_index_filter: Option<usize>,
        dpd_client: &Arc<dpd_client::Client>,
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

        let vni: u32 = network_interface.vni.into();

        info!(log, "looking up instance's external IPs";
              "instance_id" => %instance_id);

        let ips = self
            .db_datastore
            .instance_lookup_external_ips(&opctx, instance_id)
            .await?;

        if let Some(wanted_index) = ip_index_filter {
            if let None = ips.get(wanted_index) {
                return Err(Error::internal_error(&format!(
                    "failed to find external ip address at index: {}",
                    wanted_index
                )));
            }
        }

        for target_ip in ips
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                if let Some(wanted_index) = ip_index_filter {
                    *index == wanted_index
                } else {
                    true
                }
            })
            .map(|(_, ip)| ip)
        {
            retry_until_known_result(log, || async {
                dpd_client
                    .ensure_nat_entry(
                        &log,
                        target_ip.ip,
                        dpd_client::types::MacAddr {
                            a: mac_address.into_array(),
                        },
                        *target_ip.first_port,
                        *target_ip.last_port,
                        vni,
                        sled_ip_address.ip(),
                    )
                    .await
            })
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "failed to ensure dpd entry: {e}"
                ))
            })?;
        }

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
    pub(crate) async fn instance_delete_dpd_config(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> Result<(), Error> {
        let log = &self.log;
        let instance_id = authz_instance.id();

        info!(log, "deleting instance dpd configuration";
              "instance_id" => %instance_id);

        let external_ips = self
            .db_datastore
            .instance_lookup_external_ips(opctx, instance_id)
            .await?;

        let boundary_switches = self.boundary_switches(opctx).await?;

        let mut errors = vec![];
        for entry in external_ips {
            for switch in &boundary_switches {
                debug!(log, "deleting instance nat mapping";
                       "instance_id" => %instance_id,
                       "switch" => switch.to_string(),
                       "entry" => #?entry);

                let client_result =
                    self.dpd_clients.get(switch).ok_or_else(|| {
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

                let result = retry_until_known_result(log, || async {
                    dpd_client
                        .ensure_nat_entry_deleted(
                            log,
                            entry.ip,
                            *entry.first_port,
                        )
                        .await
                })
                .await;

                if let Err(e) = result {
                    let e = Error::internal_error(&format!(
                        "failed to delete nat entry via dpd: {e}"
                    ));

                    error!(log, "error deleting nat mapping: {e:#?}");
                    errors.push(e);
                } else {
                    debug!(log, "deleting nat mapping successful";
                           "instance_id" => %instance_id,
                           "switch" => switch.to_string(),
                           "entry" => #?entry);
                }
            }
        }

        if let Some(error) = errors.first() {
            return Err(error.clone());
        }

        Ok(())
    }
}
