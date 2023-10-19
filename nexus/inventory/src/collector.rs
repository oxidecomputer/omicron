// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collection of inventory from Omicron components

use crate::builder::{CollectionBuilder, SpSlot};
use anyhow::Context;
use gateway_client::types::RotSlot;
use nexus_types::inventory::Collection;
use slog::{debug, error};
use std::sync::Arc;
use strum::IntoEnumIterator;

pub struct Collector {
    log: slog::Logger,
    mgs_clients: Vec<Arc<gateway_client::Client>>,
    in_progress: CollectionBuilder,
}

impl Collector {
    pub fn new(
        creator: &str,
        mgs_clients: &[Arc<gateway_client::Client>],
        log: slog::Logger,
    ) -> Self {
        Collector {
            log,
            mgs_clients: mgs_clients.to_vec(),
            in_progress: CollectionBuilder::new(creator),
        }
    }

    /// Begin the process of collecting a complete hardware/software inventory
    /// of the rack
    ///
    /// The collection process makes a bunch of requests to a bunch of
    /// components.  This can take a while and produce any number of errors.
    /// Such errors generally don't cause this function to fail.  Rather, the
    /// returned `Collection` keeps track of these errors.
    pub async fn collect_all(mut self) -> Result<Collection, anyhow::Error> {
        // We're about to do a bunch of asynchronous operations.  With a
        // combination of async, futures, and some cleverness, we could do much
        // of this in parallel.  But this code path is not remotely
        // latency-sensitive.  And there's real risk of overloading our
        // downstream services.  So we just do one step at a time.  This also
        // keeps the code simpler.

        debug!(&self.log, "begin collection");

        // When we add stages to collect from other components (e.g., sled
        // agents), those will go here.
        self.collect_all_mgs().await;

        debug!(&self.log, "finished collection");

        Ok(self.in_progress.build())
    }

    /// Collect inventory from all MGS instances
    async fn collect_all_mgs(&mut self) {
        let clients = self.mgs_clients.clone();
        for client in &clients {
            self.collect_one_mgs(&client).await;
        }
    }

    async fn collect_one_mgs(&mut self, client: &gateway_client::Client) {
        debug!(&self.log, "begin collection from MGS";
            "mgs_url" => client.baseurl()
        );

        // First, see which SPs MGS can see via Ignition.
        let ignition_result = client.ignition_list().await.with_context(|| {
            format!("MGS {:?}: listing ignition targets", client.baseurl())
        });

        // Select only the SPs that appear powered on.
        //
        // This choice is debatable.  It's conceivable that an SP could be
        // functioning but not visible to ignition.  In that case, we'd be
        // better off trying to ask MGS about it even though ignition reports it
        // powered off.  But in practice, if ignition can't see it, it's much
        // more likely that there's just nothing plugged in.  And in that case,
        // if we try to ask MGS about it, we have to wait for MGS to time out
        // its attempt to reach it (currently several seconds).  This choice
        // enables inventory to complete much faster, at the expense of not
        // being able to identify this particular condition.
        let sps = match ignition_result {
            Err(error) => {
                self.in_progress.found_error(error);
                return;
            }

            Ok(targets) => {
                targets.into_inner().into_iter().filter_map(|sp_ignition| {
                    match sp_ignition.details {
                        gateway_client::types::SpIgnition::No => None,
                        gateway_client::types::SpIgnition::Yes {
                            power: false,
                            ..
                        } => None,
                        gateway_client::types::SpIgnition::Yes {
                            power: true,
                            ..
                        } => Some(sp_ignition.id),
                    }
                })
            }
        };

        // For each SP that ignition reports up, fetch the state and caboose
        // information.
        for sp in sps {
            // First, fetch the state of the SP.  If that fails, report the
            // error but continue.
            let result =
                client.sp_get(sp.type_, sp.slot).await.with_context(|| {
                    format!(
                        "MGS {:?}: fetching state of SP {:?}",
                        client.baseurl(),
                        sp
                    )
                });
            let sp_state = match result {
                Err(error) => {
                    self.in_progress.found_error(error);
                    continue;
                }
                Ok(response) => response.into_inner(),
            };

            // Record the state that we found.
            let Some(baseboard_id) = self.in_progress.found_sp_state(
                client.baseurl(),
                sp.type_,
                sp.slot,
                sp_state,
            ) else {
                // We failed to parse this SP for some reason.  The error was
                // reported already.  Move on.
                continue;
            };

            // For each kind of caboose that we care about, if it hasn't been
            // fetched already, fetch it and record it.  Generally, we'd only
            // get here for the first MGS client.  Assuming that one succeeds,
            // the other(s) will skip this loop.
            for sp_slot in SpSlot::iter() {
                if self
                    .in_progress
                    .found_sp_caboose_already(&baseboard_id, sp_slot)
                {
                    continue;
                }

                let slot_num = match sp_slot {
                    SpSlot::Slot0 => 0,
                    SpSlot::Slot1 => 1,
                };

                let result = client
                    .sp_component_caboose_get(sp.type_, sp.slot, "sp", slot_num)
                    .await
                    .with_context(|| {
                        format!(
                            "MGS {:?}: SP {:?}: SP caboose {:?}",
                            client.baseurl(),
                            sp,
                            sp_slot
                        )
                    });
                let caboose = match result {
                    Err(error) => {
                        self.in_progress.found_error(error);
                        continue;
                    }
                    Ok(response) => response.into_inner(),
                };
                if let Err(error) = self.in_progress.found_sp_caboose(
                    &baseboard_id,
                    sp_slot,
                    client.baseurl(),
                    caboose,
                ) {
                    error!(
                        &self.log,
                        "error reporting caboose: {:?} SP {:?} {:?}: {:#}",
                        baseboard_id,
                        sp_slot,
                        client.baseurl(),
                        error
                    );
                }
            }

            for rot_slot in RotSlot::iter() {
                if self
                    .in_progress
                    .found_rot_caboose_already(&baseboard_id, rot_slot)
                {
                    continue;
                }

                let slot_num = match rot_slot {
                    RotSlot::A => 0,
                    RotSlot::B => 1,
                };

                let result = client
                    .sp_component_caboose_get(
                        sp.type_, sp.slot, "rot", slot_num,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "MGS {:?}: SP {:?}: RoT caboose {:?}",
                            client.baseurl(),
                            sp,
                            rot_slot
                        )
                    });
                let caboose = match result {
                    Err(error) => {
                        self.in_progress.found_error(error);
                        continue;
                    }
                    Ok(response) => response.into_inner(),
                };
                if let Err(error) = self.in_progress.found_rot_caboose(
                    &baseboard_id,
                    rot_slot,
                    client.baseurl(),
                    caboose,
                ) {
                    error!(
                        &self.log,
                        "error reporting caboose: {:?} RoT {:?} {:?}: {:#}",
                        baseboard_id,
                        rot_slot,
                        client.baseurl(),
                        error
                    );
                }
            }
        }
    }
}
