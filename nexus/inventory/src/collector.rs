// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collection of inventory from Omicron components

use crate::builder::CollectionBuilder;
use crate::Collection;
use anyhow::Context;
use nexus_types::inventory::CabooseWhich;
use std::sync::Arc;
use strum::IntoEnumIterator;

pub struct Collector {
    mgs_clients: Vec<Arc<gateway_client::Client>>,
    in_progress: CollectionBuilder,
}

impl Collector {
    pub fn new(
        creator: &str,
        mgs_clients: &[Arc<gateway_client::Client>],
    ) -> Self {
        Collector {
            mgs_clients: mgs_clients.to_vec(),
            in_progress: CollectionBuilder::new(creator),
        }
    }

    // XXX-dap TODO-doc, especially errors
    pub async fn enumerate(mut self) -> Result<Collection, anyhow::Error> {
        // We're about to do a bunch of asynchronous operations.  With a
        // combination of async, futures, and some cleverness, we could do much
        // of this in parallel.  But this code path is not remotely
        // latency-sensitive.  And there's real risk of overloading our
        // downstream services.  So we just do one step at a time.  This also
        // keeps the code pretty simple.

        let clients = self.mgs_clients.clone();
        for client in clients {
            self.enumerate_client(&client).await?;
        }

        Ok(self.in_progress.build())
    }

    async fn enumerate_client(
        &mut self,
        client: &gateway_client::Client,
    ) -> Result<(), anyhow::Error> {
        // First, see which SPs MGS can see via Ignition.
        let ignition_result = client.ignition_list().await.with_context(|| {
            format!("MGS {:?}: listing ignition targets", client.baseurl())
        });

        // Select only the SPs that appear powered on.
        let sps = match ignition_result {
            Err(error) => {
                self.in_progress.found_error(error);
                return Ok(());
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

        // Fetch the state and caboose information for each SP.
        for sp in sps {
            // First, fetch the state of the SP.
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

            let Some(baseboard_id) = self.in_progress.found_sp_state(
                client.baseurl(),
                sp.type_,
                sp.slot,
                sp_state,
            ) else {
                // We failed to parse this SP for some reason.  Move on.
                continue;
            };

            // For each caboose that we care about, if it hasn't been fetched
            // already, fetch it.  Generally, we'd only get here for the first
            // MGS client.  Assuming that one succeeds, the others will skip
            // all these iterations.
            for which in CabooseWhich::iter() {
                if self
                    .in_progress
                    .sp_found_caboose_already(&baseboard_id, which)
                {
                    continue;
                }

                let (component, slot) = match which {
                    CabooseWhich::SpSlot0 => ("sp", 0),
                    CabooseWhich::SpSlot1 => ("sp", 1),
                    CabooseWhich::RotSlotA => ("rot", 0),
                    CabooseWhich::RotSlotB => ("rot", 1),
                };

                let result = client
                    .sp_component_caboose_get(
                        sp.type_, sp.slot, component, slot,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "MGS {:?}: SP {:?}: caboose {:?}",
                            client.baseurl(),
                            sp,
                            which
                        )
                    });
                let caboose = match result {
                    Err(error) => {
                        self.in_progress.found_error(error);
                        continue;
                    }
                    Ok(response) => response.into_inner(),
                };
                self.in_progress.found_sp_caboose(
                    &baseboard_id,
                    which,
                    client.baseurl(),
                    caboose,
                )?;
            }
        }

        Ok(())
    }
}
