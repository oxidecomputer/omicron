// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Routines that manage instance-related networking state.

use crate::app::switch_port;
use ipnetwork::IpNetwork;
use ipnetwork::Ipv6Network;
use nexus_db_model::ExternalIp;
use nexus_db_model::IpAttachState;
use nexus_db_model::Ipv4NatEntry;
use nexus_db_model::Ipv4NatValues;
use nexus_db_model::Vni as DbVni;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::SledFilter;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::Ipv4Net;
use omicron_common::api::external::Ipv6Net;
use omicron_common::api::internal::nexus;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_common::retry_until_known_result;
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
        boundary_switches(&self.db_datastore, opctx).await
    }

    /// Ensures that V2P mappings exist that indicate that the instance with ID
    /// `instance_id` is resident on the sled with ID `sled_id`.
    pub(crate) async fn create_instance_v2p_mappings(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
        sled_id: Uuid,
    ) -> Result<(), Error> {
        create_instance_v2p_mappings(
            &self.db_datastore,
            &self.log,
            opctx,
            &self.opctx_alloc,
            instance_id,
            sled_id,
        )
        .await
    }

    /// Ensure that the necessary v2p mappings for an instance are deleted
    pub(crate) async fn delete_instance_v2p_mappings(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<(), Error> {
        delete_instance_v2p_mappings(
            &self.db_datastore,
            &self.log,
            opctx,
            &self.opctx_alloc,
            instance_id,
        )
        .await
    }

    /// Ensures that the Dendrite configuration for the supplied instance is
    /// up-to-date.
    ///
    /// Returns a list of live NAT RPW table entries from this call. Generally
    /// these should only be needed for specific unwind operations, like in
    /// the IP attach saga.
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
    ///     external IP with `id` in the collection returned from CRDB. This will
    ///     proceed even when the target IP is 'attaching'.
    ///   - If this is `None`, this routine configures DPD for all external
    ///     IPs and *will back out* if any IPs are not yet fully attached to
    ///     the instance.
    pub(crate) async fn instance_ensure_dpd_config(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
        sled_ip_address: &std::net::SocketAddrV6,
        ip_filter: Option<Uuid>,
    ) -> Result<Vec<Ipv4NatEntry>, Error> {
        instance_ensure_dpd_config(
            &self.db_datastore,
            &self.log,
            &self.resolver().await,
            opctx,
            &self.opctx_alloc,
            instance_id,
            sled_ip_address,
            ip_filter,
        )
        .await
    }

    // The logic of this function should follow very closely what
    // `instance_ensure_dpd_config` does. However, there are enough differences
    // in the mechanics of how the logic is being carried out to justify having
    // this separate function, it seems.
    pub(crate) async fn probe_ensure_dpd_config(
        &self,
        opctx: &OpContext,
        probe_id: Uuid,
        sled_ip_address: std::net::Ipv6Addr,
        ip_index_filter: Option<usize>,
        dpd_client: &dpd_client::Client,
    ) -> Result<(), Error> {
        probe_ensure_dpd_config(
            &self.db_datastore,
            &self.log,
            opctx,
            probe_id,
            sled_ip_address,
            ip_index_filter,
            dpd_client,
        )
        .await
    }

    /// Attempts to delete all of the Dendrite NAT configuration for the
    /// instance identified by `authz_instance`.
    ///
    /// Unlike `instance_ensure_dpd_config`, this function will disregard the
    /// attachment states of any external IPs because likely callers (instance
    /// delete) cannot be piecewise undone.
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
    ) -> Result<(), Error> {
        let resolver = self.resolver().await;
        instance_delete_dpd_config(
            &self.db_datastore,
            &self.log,
            &resolver,
            opctx,
            &self.opctx_alloc,
            authz_instance,
        )
        .await
    }

    /// Attempts to delete Dendrite NAT configuration for a single external IP.
    ///
    /// This function is primarily used to detach an IP which currently belongs
    /// to a known instance.
    pub(crate) async fn external_ip_delete_dpd_config(
        &self,
        opctx: &OpContext,
        external_ip: &ExternalIp,
    ) -> Result<(), Error> {
        external_ip_delete_dpd_config_inner(
            &self.db_datastore,
            &self.log,
            opctx,
            external_ip,
        )
        .await
    }

    /// Attempts to soft-delete Dendrite NAT configuration for a specific entry
    /// via ID.
    ///
    /// This function is needed to safely cleanup in at least one unwind scenario
    /// where a potential second user could need to use the same (IP, portset) pair,
    /// e.g. a rapid reattach or a reallocated ephemeral IP.
    pub(crate) async fn delete_dpd_config_by_entry(
        &self,
        opctx: &OpContext,
        nat_entry: &Ipv4NatEntry,
    ) -> Result<(), Error> {
        delete_dpd_config_by_entry(
            &self.db_datastore,
            &self.resolver().await,
            &self.log,
            opctx,
            &self.opctx_alloc,
            nat_entry,
        )
        .await
    }

    // The logic of this function should follow very closely what
    // `instance_delete_dpd_config` does. However, there are enough differences
    // in the mechanics of how the logic is being carried out to justify having
    // this separate function, it seems.
    pub(crate) async fn probe_delete_dpd_config(
        &self,
        opctx: &OpContext,
        probe_id: Uuid,
    ) -> Result<(), Error> {
        probe_delete_dpd_config(
            &self.db_datastore,
            &self.log,
            &self.resolver().await,
            opctx,
            &self.opctx_alloc,
            probe_id,
        )
        .await
    }
}

/// Returns the set of switches with uplinks configured and boundary
/// services enabled.
pub(crate) async fn boundary_switches(
    datastore: &DataStore,
    opctx: &OpContext,
) -> Result<HashSet<SwitchLocation>, Error> {
    let mut boundary_switches: HashSet<SwitchLocation> = HashSet::new();
    let uplinks =
        switch_port::list_switch_ports_with_uplinks(datastore, opctx).await?;
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

/// Given old and new instance runtime states, determines the desired
/// networking configuration for a given instance and ensures it has been
/// propagated to all relevant sleds.
///
/// # Arguments
///
/// - `datastore`: the datastore to use for lookups and updates.
/// - `log`: the [`slog::Logger`] to log to.
/// - `resolver`: an internal DNS resolver to look up DPD service addresses.
/// - `opctx`: An operation context for this operation.
/// - `opctx_alloc`: An operational context list permissions for all sleds. When
///   called by methods on the [`Nexus`] type, this is the `OpContext` used for
///   instance allocation. In a background task, this may be the background
///   task's operational context; nothing stops you from passing the same
///   `OpContext` as both `opctx` and `opctx_alloc`.
/// - `authz_instance``: A resolved authorization context for the instance of
///   interest.
/// - `prev_instance_state``: The most-recently-recorded instance runtime
///   state for this instance.
/// - `new_instance_state`: The instance state that the caller of this routine
///   has observed and that should be used to set up this instance's
///   networking state.
///
/// # Return value
///
/// `Ok(())` if this routine completed all the operations it wanted to
/// complete, or an appropriate `Err` otherwise.
#[allow(clippy::too_many_arguments)] // Yeah, I know, I know, Clippy...
pub(crate) async fn ensure_updated_instance_network_config(
    datastore: &DataStore,
    log: &slog::Logger,
    resolver: &internal_dns::resolver::Resolver,
    opctx: &OpContext,
    opctx_alloc: &OpContext,
    authz_instance: &authz::Instance,
    prev_instance_state: &db::model::InstanceRuntimeState,
    new_instance_state: &nexus::InstanceRuntimeState,
) -> Result<(), Error> {
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

        clear_instance_networking_state(
            datastore,
            log,
            resolver,
            opctx,
            opctx_alloc,
            authz_instance,
        )
        .await?;
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
    let new_sled_id = match datastore
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

    create_instance_v2p_mappings(
        datastore,
        log,
        opctx,
        opctx_alloc,
        instance_id,
        new_sled_id,
    )
    .await?;

    let (.., sled) =
        LookupPath::new(opctx, datastore).sled_id(new_sled_id).fetch().await?;

    instance_ensure_dpd_config(
        datastore,
        log,
        resolver,
        opctx,
        opctx_alloc,
        instance_id,
        &sled.address(),
        None,
    )
    .await?;

    Ok(())
}

/// Ensures that the Dendrite configuration for the supplied instance is
/// up-to-date.
///
/// Returns a list of live NAT RPW table entries from this call. Generally
/// these should only be needed for specific unwind operations, like in
/// the IP attach saga.
///
/// # Parameters
///
/// - `datastore`: the datastore to use for lookups and updates.
/// - `log`: the [`slog::Logger`] to log to.
/// - `resolver`: an internal DNS resolver to look up DPD service addresses.
/// - `opctx`: An operation context that grants read and list-children
///   permissions on the identified instance.
/// - `opctx_alloc`: An operational context list permissions for all sleds. When
///   called by methods on the [`Nexus`] type, this is the `OpContext` used for
///   instance allocation. In a background task, this may be the background
///   task's operational context; nothing stops you from passing the same
///   `OpContext` as both `opctx` and `opctx_alloc`.
/// - `instance_id`: The ID of the instance to act on.
/// - `sled_ip_address`: The internal IP address assigned to the sled's
///   sled agent.
/// - `ip_filter`: An optional filter on the index into the instance's
///   external IP array.
///   - If this is `Some(id)`, this routine configures DPD state for only the
///     external IP with `id` in the collection returned from CRDB. This will
///     proceed even when the target IP is 'attaching'.
///   - If this is `None`, this routine configures DPD for all external
///     IPs and *will back out* if any IPs are not yet fully attached to
///     the instance.
#[allow(clippy::too_many_arguments)] // I don't like it either, clippy...
pub(crate) async fn instance_ensure_dpd_config(
    datastore: &DataStore,
    log: &slog::Logger,
    resolver: &internal_dns::resolver::Resolver,
    opctx: &OpContext,
    opctx_alloc: &OpContext,
    instance_id: Uuid,
    sled_ip_address: &std::net::SocketAddrV6,
    ip_filter: Option<Uuid>,
) -> Result<Vec<Ipv4NatEntry>, Error> {
    info!(log, "looking up instance's primary network interface";
            "instance_id" => %instance_id);

    let (.., authz_instance) = LookupPath::new(opctx, datastore)
        .instance_id(instance_id)
        .lookup_for(authz::Action::ListChildren)
        .await?;

    // XXX: Need to abstract over v6 and v4 entries here.
    let mut nat_entries = vec![];

    // All external IPs map to the primary network interface, so find that
    // interface. If there is no such interface, there's no way to route
    // traffic destined to those IPs, so there's nothing to configure and
    // it's safe to return early.
    let network_interface = match datastore
        .derive_guest_network_interface_info(&opctx, &authz_instance)
        .await?
        .into_iter()
        .find(|interface| interface.primary)
    {
        Some(interface) => interface,
        None => {
            info!(log, "Instance has no primary network interface";
                    "instance_id" => %instance_id);
            return Ok(nat_entries);
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

    let ips =
        datastore.instance_lookup_external_ips(&opctx, instance_id).await?;

    let (ips_of_interest, must_all_be_attached) = if let Some(wanted_id) =
        ip_filter
    {
        if let Some(ip) = ips.iter().find(|v| v.id == wanted_id) {
            (std::slice::from_ref(ip), false)
        } else {
            return Err(Error::internal_error(&format!(
                "failed to find external ip address with id: {wanted_id}, saw {ips:?}",
            )));
        }
    } else {
        (&ips[..], true)
    };

    // This is performed so that an IP attach/detach will block the
    // instance_start saga. Return service unavailable to indicate
    // the request is retryable.
    if must_all_be_attached
        && ips_of_interest.iter().any(|ip| ip.state != IpAttachState::Attached)
    {
        return Err(Error::unavail(
            "cannot push all DPD state: IP attach/detach in progress",
        ));
    }

    let sled_address =
        Ipv6Net(Ipv6Network::new(*sled_ip_address.ip(), 128).unwrap());

    // If all of our IPs are attached or are guaranteed to be owned
    // by the saga calling this fn, then we need to disregard and
    // remove conflicting rows. No other instance/service should be
    // using these as its own, and we are dealing with detritus, e.g.,
    // the case where we have a concurrent stop -> detach followed
    // by an attach to another instance, or other ongoing attach saga
    // cleanup.
    let mut err_and_limit = None;
    for (i, external_ip) in ips_of_interest.iter().enumerate() {
        // For each external ip, add a nat entry to the database
        if let Ok(id) = ensure_nat_entry(
            datastore,
            external_ip,
            sled_address,
            &network_interface,
            mac_address,
            opctx,
        )
        .await
        {
            nat_entries.push(id);
            continue;
        }

        // We seem to be blocked by a bad row -- take it out and retry.
        // This will return Ok() for a non-existent row.
        if let Err(e) = external_ip_delete_dpd_config_inner(
            datastore,
            log,
            opctx,
            external_ip,
        )
        .await
        {
            err_and_limit = Some((e, i));
            break;
        };

        match ensure_nat_entry(
            datastore,
            external_ip,
            sled_address,
            &network_interface,
            mac_address,
            opctx,
        )
        .await
        {
            Ok(id) => nat_entries.push(id),
            Err(e) => {
                err_and_limit = Some((e, i));
                break;
            }
        }
    }

    // In the event of an unresolvable failure, we need to remove
    // the entries we just added because the undo won't call into
    // `instance_delete_dpd_config`. These entries won't stop a
    // future caller, but it's better not to pollute switch state.
    if let Some((e, max)) = err_and_limit {
        for external_ip in &ips_of_interest[..max] {
            let _ = external_ip_delete_dpd_config_inner(
                datastore,
                log,
                opctx,
                external_ip,
            )
            .await;
        }
        return Err(e);
    }

    notify_dendrite_nat_state(
        datastore,
        log,
        resolver,
        opctx_alloc,
        Some(instance_id),
        true,
    )
    .await?;

    Ok(nat_entries)
}

// The logic of this function should follow very closely what
// `instance_ensure_dpd_config` does. However, there are enough differences
// in the mechanics of how the logic is being carried out to justify having
// this separate function, it seems.
pub(crate) async fn probe_ensure_dpd_config(
    datastore: &DataStore,
    log: &slog::Logger,
    opctx: &OpContext,
    probe_id: Uuid,
    sled_ip_address: std::net::Ipv6Addr,
    ip_index_filter: Option<usize>,
    dpd_client: &dpd_client::Client,
) -> Result<(), Error> {
    // All external IPs map to the primary network interface, so find that
    // interface. If there is no such interface, there's no way to route
    // traffic destined to those IPs, so there's nothing to configure and
    // it's safe to return early.
    let network_interface = match datastore
        .derive_probe_network_interface_info(&opctx, probe_id)
        .await?
        .into_iter()
        .find(|interface| interface.primary)
    {
        Some(interface) => interface,
        None => {
            info!(log, "probe has no primary network interface";
                    "probe_id" => %probe_id);
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

    info!(log, "looking up probe's external IPs";
            "probe_id" => %probe_id);

    let ips = datastore.probe_lookup_external_ips(&opctx, probe_id).await?;

    if let Some(wanted_index) = ip_index_filter {
        if let None = ips.get(wanted_index) {
            return Err(Error::internal_error(&format!(
                "failed to find external ip address at index: {}",
                wanted_index
            )));
        }
    }

    let sled_address = Ipv6Net(Ipv6Network::new(sled_ip_address, 128).unwrap());

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
        // For each external ip, add a nat entry to the database
        ensure_nat_entry(
            datastore,
            target_ip,
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
        error!(log, "failed to notify dendrite of nat updates"; "error" => ?e);
    };

    Ok(())
}

/// Deletes an instance's OPTE V2P mappings and the boundary switch NAT
/// entries for its external IPs.
///
/// This routine returns immediately upon encountering any errors (and will
/// not try to destroy any more objects after the point of failure).
async fn clear_instance_networking_state(
    datastore: &DataStore,
    log: &slog::Logger,

    resolver: &internal_dns::resolver::Resolver,
    opctx: &OpContext,
    opctx_alloc: &OpContext,
    authz_instance: &authz::Instance,
) -> Result<(), Error> {
    delete_instance_v2p_mappings(
        datastore,
        log,
        opctx,
        opctx_alloc,
        authz_instance.id(),
    )
    .await?;

    instance_delete_dpd_config(
        datastore,
        log,
        resolver,
        opctx,
        opctx_alloc,
        authz_instance,
    )
    .await?;

    notify_dendrite_nat_state(
        datastore,
        log,
        resolver,
        opctx_alloc,
        Some(authz_instance.id()),
        true,
    )
    .await
}

/// Ensures that V2P mappings exist that indicate that the instance with ID
/// `instance_id` is resident on the sled with ID `sled_id`.
pub(crate) async fn create_instance_v2p_mappings(
    datastore: &DataStore,
    log: &slog::Logger,
    opctx: &OpContext,
    opctx_alloc: &OpContext,
    instance_id: Uuid,
    sled_id: Uuid,
) -> Result<(), Error> {
    info!(log, "creating V2P mappings for instance";
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
    let (.., authz_instance) = LookupPath::new(&opctx, &datastore)
        .instance_id(instance_id)
        .lookup_for(authz::Action::Read)
        .await?;

    let instance_nics = datastore
        .derive_guest_network_interface_info(&opctx, &authz_instance)
        .await?;

    // Look up the supplied sled's physical host IP.
    let physical_host_ip =
        nexus_networking::sled_lookup(&datastore, &opctx_alloc, sled_id)?
            .fetch()
            .await?
            .1
            .ip
            .into();

    let mut last_sled_id: Option<Uuid> = None;
    loop {
        let pagparams = DataPageParams {
            marker: last_sled_id.as_ref(),
            direction: dropshot::PaginationOrder::Ascending,
            limit: std::num::NonZeroU32::new(10).unwrap(),
        };

        let sleds_page = datastore
            // XXX: InService might not be exactly correct
            .sled_list(&opctx_alloc, &pagparams, SledFilter::InService)
            .await?;
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
                let client = nexus_networking::sled_client(
                    datastore,
                    opctx_alloc,
                    sled.id(),
                    log,
                )
                .await?;
                let nic_id = nic.id;
                let mapping = SetVirtualNetworkInterfaceHost {
                    virtual_ip: nic.ip,
                    virtual_mac: nic.mac,
                    physical_host_ip,
                    vni: nic.vni,
                };

                let log = log.clone();

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
                error!(log, "{:?}", result);
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
    datastore: &DataStore,
    log: &slog::Logger,
    opctx: &OpContext,
    opctx_alloc: &OpContext,
    instance_id: Uuid,
) -> Result<(), Error> {
    // For every sled that isn't the sled this instance was allocated to, delete
    // the virtual to physical mapping for each of this instance's NICs. If
    // there isn't a V2P mapping, del_v2p should be a no-op.
    let (.., authz_instance) = LookupPath::new(&opctx, datastore)
        .instance_id(instance_id)
        .lookup_for(authz::Action::Read)
        .await?;

    let instance_nics = datastore
        .derive_guest_network_interface_info(&opctx, &authz_instance)
        .await?;

    let mut last_sled_id: Option<Uuid> = None;

    loop {
        let pagparams = DataPageParams {
            marker: last_sled_id.as_ref(),
            direction: dropshot::PaginationOrder::Ascending,
            limit: std::num::NonZeroU32::new(10).unwrap(),
        };

        let sleds_page = datastore
            // XXX: InService might not be exactly correct
            .sled_list(&opctx_alloc, &pagparams, SledFilter::InService)
            .await?;
        let mut join_handles =
            Vec::with_capacity(sleds_page.len() * instance_nics.len());

        for sled in &sleds_page {
            for nic in &instance_nics {
                let client = nexus_networking::sled_client(
                    &datastore,
                    &opctx_alloc,
                    sled.id(),
                    &log,
                )
                .await?;
                let nic_id = nic.id;
                let mapping = DeleteVirtualNetworkInterfaceHost {
                    virtual_ip: nic.ip,
                    vni: nic.vni,
                };

                let log = log.clone();

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
                error!(log, "{:?}", result);
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

/// Attempts to delete all of the Dendrite NAT configuration for the
/// instance identified by `authz_instance`.
///
/// Unlike `instance_ensure_dpd_config`, this function will disregard the
/// attachment states of any external IPs because likely callers (instance
/// delete) cannot be piecewise undone.
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
    datastore: &DataStore,
    log: &slog::Logger,
    resolver: &internal_dns::resolver::Resolver,
    opctx: &OpContext,
    opctx_alloc: &OpContext,
    authz_instance: &authz::Instance,
) -> Result<(), Error> {
    let instance_id = authz_instance.id();

    info!(log, "deleting instance dpd configuration";
            "instance_id" => %instance_id);

    let external_ips =
        datastore.instance_lookup_external_ips(opctx, instance_id).await?;

    for entry in external_ips {
        external_ip_delete_dpd_config_inner(&datastore, &log, opctx, &entry)
            .await?;
    }

    notify_dendrite_nat_state(
        datastore,
        log,
        resolver,
        opctx_alloc,
        Some(instance_id),
        false,
    )
    .await
}

// The logic of this function should follow very closely what
// `instance_delete_dpd_config` does. However, there are enough differences
// in the mechanics of how the logic is being carried out to justify having
// this separate function, it seems.
pub(crate) async fn probe_delete_dpd_config(
    datastore: &DataStore,
    log: &slog::Logger,
    resolver: &internal_dns::resolver::Resolver,
    opctx: &OpContext,
    opctx_alloc: &OpContext,
    probe_id: Uuid,
) -> Result<(), Error> {
    info!(log, "deleting probe dpd configuration";
            "probe_id" => %probe_id);

    let external_ips =
        datastore.probe_lookup_external_ips(opctx, probe_id).await?;

    let mut errors = vec![];
    for entry in external_ips {
        // Soft delete the NAT entry
        match datastore.ipv4_nat_delete_by_external_ip(&opctx, &entry).await {
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

    let boundary_switches = boundary_switches(datastore, opctx_alloc).await?;

    for switch in &boundary_switches {
        debug!(log, "notifying dendrite of updates";
                "probe_id" => %probe_id,
                "switch" => switch.to_string());

        let dpd_clients =
            super::dpd_clients(resolver, log).await.map_err(|e| {
                Error::internal_error(&format!(
                    "unable to get dpd_clients: {e}"
                ))
            })?;

        let client_result = dpd_clients.get(switch).ok_or_else(|| {
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
            error!(log, "failed to notify dendrite of nat updates"; "error" => ?e);
        };
    }

    if let Some(e) = errors.into_iter().next() {
        return Err(e);
    }

    Ok(())
}

/// Attempts to soft-delete Dendrite NAT configuration for a specific entry
/// via ID.
///
/// This function is needed to safely cleanup in at least one unwind scenario
/// where a potential second user could need to use the same (IP, portset) pair,
/// e.g. a rapid reattach or a reallocated ephemeral IP.
pub(crate) async fn delete_dpd_config_by_entry(
    datastore: &DataStore,
    resolver: &internal_dns::resolver::Resolver,
    log: &slog::Logger,
    opctx: &OpContext,
    opctx_alloc: &OpContext,
    nat_entry: &Ipv4NatEntry,
) -> Result<(), Error> {
    info!(log, "deleting individual NAT entry from dpd configuration";
            "id" => ?nat_entry.id,
            "version_added" => %nat_entry.external_address.0);

    match datastore.ipv4_nat_delete(&opctx, nat_entry).await {
        Ok(_) => {}
        Err(err) => match err {
            Error::ObjectNotFound { .. } => {
                warn!(log, "no matching nat entries to soft delete");
            }
            _ => {
                let message =
                    format!("failed to delete nat entry due to error: {err:?}");
                error!(log, "{}", message);
                return Err(Error::internal_error(&message));
            }
        },
    }

    notify_dendrite_nat_state(
        datastore,
        log,
        resolver,
        opctx_alloc,
        None,
        false,
    )
    .await
}

/// Soft-delete an individual external IP from the NAT RPW, without
/// triggering a Dendrite notification.
async fn external_ip_delete_dpd_config_inner(
    datastore: &DataStore,
    log: &slog::Logger,
    opctx: &OpContext,
    external_ip: &ExternalIp,
) -> Result<(), Error> {
    // Soft delete the NAT entry
    match datastore.ipv4_nat_delete_by_external_ip(&opctx, external_ip).await {
        Ok(_) => Ok(()),
        Err(err) => match err {
            Error::ObjectNotFound { .. } => {
                warn!(log, "no matching nat entries to soft delete");
                Ok(())
            }
            _ => {
                let message =
                    format!("failed to delete nat entry due to error: {err:?}");
                error!(log, "{}", message);
                Err(Error::internal_error(&message))
            }
        },
    }
}

/// Informs all available boundary switches that the set of NAT entries
/// has changed.
///
/// When `fail_fast` is set, this function will return on any error when
/// acquiring a handle to a DPD client. Otherwise, it will attempt to notify
/// all clients and then finally return the first error.
async fn notify_dendrite_nat_state(
    datastore: &DataStore,
    log: &slog::Logger,
    resolver: &internal_dns::resolver::Resolver,
    opctx_alloc: &OpContext,
    instance_id: Option<Uuid>,
    fail_fast: bool,
) -> Result<(), Error> {
    // Querying boundary switches also requires fleet access and the use of the
    // instance allocator context.
    let boundary_switches = boundary_switches(datastore, opctx_alloc).await?;

    let mut errors = vec![];
    for switch in &boundary_switches {
        debug!(log, "notifying dendrite of updates";
                    "instance_id" => ?instance_id,
                    "switch" => switch.to_string());

        let clients = super::dpd_clients(resolver, log).await.map_err(|e| {
            Error::internal_error(&format!("failed to get dpd clients: {e}"))
        })?;
        let client_result = clients.get(switch).ok_or_else(|| {
            Error::internal_error(&format!(
                "unable to find dendrite client for {switch}"
            ))
        });

        let dpd_client = match client_result {
            Ok(client) => client,
            Err(new_error) => {
                errors.push(new_error);
                if fail_fast {
                    break;
                } else {
                    continue;
                }
            }
        };

        // Notify dendrite that there are changes for it to reconcile.
        // In the event of a failure to notify dendrite, we'll log an error
        // and rely on dendrite's RPW timer to catch it up.
        if let Err(e) = dpd_client.ipv4_nat_trigger_update().await {
            error!(log, "failed to notify dendrite of nat updates"; "error" => ?e);
        };
    }

    if let Some(e) = errors.into_iter().next() {
        return Err(e);
    }

    Ok(())
}

async fn ensure_nat_entry(
    datastore: &DataStore,
    target_ip: &nexus_db_model::ExternalIp,
    sled_address: Ipv6Net,
    network_interface: &NetworkInterface,
    mac_address: macaddr::MacAddr6,
    opctx: &OpContext,
) -> Result<Ipv4NatEntry, Error> {
    match target_ip.ip {
        IpNetwork::V4(v4net) => {
            let nat_entry = Ipv4NatValues {
                external_address: Ipv4Net(v4net).into(),
                first_port: target_ip.first_port,
                last_port: target_ip.last_port,
                sled_address: sled_address.into(),
                vni: DbVni(network_interface.vni),
                mac: nexus_db_model::MacAddr(
                    omicron_common::api::external::MacAddr(mac_address),
                ),
            };
            Ok(datastore.ensure_ipv4_nat_entry(opctx, nat_entry).await?)
        }
        IpNetwork::V6(_v6net) => {
            // TODO: implement handling of v6 nat.
            return Err(Error::InternalError {
                internal_message: "ipv6 nat is not yet implemented".into(),
            });
        }
    }
}
