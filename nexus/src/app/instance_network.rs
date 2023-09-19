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
use sled_agent_client::types::SetVirtualNetworkInterfaceHost;
use uuid::Uuid;

impl super::Nexus {
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
}
