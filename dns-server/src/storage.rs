// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages DNS data (configured zone(s), records, etc.)

// This module provides persistent storage for DNS data: the set of DNS zones
// that we're operating and the set of DNS records associated with each name.
// See the crate-level documentation for background.
//
// Most importantly here: the DNS data is versioned with a generation number.
// The data for a given generation will never change.  The only way data can
// change is for us to receive an update that provides the new data with a newer
// generation number.  We will atomically move from one generation's data to the
// next.
//
//
// PERSISTENT STORAGE
//
// So how can we store this data?  We use the `sled` crate, which essentially
// provides a key-value interface with arbitrary byte sequences for both keys
// and values.  This data is stored on disk and cached in memory.  Each sled
// _database_ provides one or more _trees_, each of which is its own namespace
// of keys (and associated values).  Trees are also named with arbitrary byte
// sequences.  There's also a default tree that cannot be removed.
//
// In the default tree, we use the following keys:
//
// - "config": describes the current generation and the list of DNS zones
//   associated with that generation
//
// Then we have one tree for each generation for each zone.  This tree describes
// all the DNS names that appear in that zone and what records are associated
// with them.  Thus, we use the following trees in addition to the default one:
//
// - "generation_$generation_zone_$zoneid": describes the DNS data for this
//   zone.  Keys in this tree represent DNS names (excluding the zone's DNS name
//   itself, which needs to be appended to each key to get the fully-qualified
//   domain name).  Each value is a Vec of DNS records.
//
// For all values in the sled database, we store JSON-serialized Rust
// structures.  We don't have to worry about versioning or compatibility of any
// kind.  Each database will only be read or written by one version of this
// program.  When we want to upgrade this component, we'll deploy a new one,
// which will receive its own copy of the data and store it in its own database.
//
//
// UPDATING DNS DATA
//
// For simplicity, it's not supported to attempt concurrent updates to the DNS
// data.  If we receive a request to update the DNS data while another update is
// still in progress, the new request will fail.  (We could instead attempt to
// process these concurrently, but that's tricky.  We could queue them up, but
// how long would we allow this queue to grow?  Why bother allowing it to grow
// at all?)
//
// When we receive a request to update to a new generation, we create a tree for
// each zone that we find in the data.  Then we fill it out using the set of
// data in the update request.  Upon successful completion, we'll update
// "config" in the default tree to reflect the new generation.  At that point,
// we'll have atomically switched to the new generation's data.
//
// On startup and after finishing any update, we prune any trees associated with
// older generations.  We'll keep the last few for debugging or for some kind of
// emergency recovery.  We'll also remove trees associated with _newer_
// generations.  This should only be possible on startup.  That would reflect
// that a previous update was interrupted.  We'll just remove whatever data was
// written and assume that the surrounding system will re-attempt the update if
// desired.
//
//
// INTERFACE
//
// This module exposes just one noteworthy type: the `Store`.  You can think of
// this as both a "server" (storing the persistent data) and a "client" (that
// provides access to the data).  With a more sophisticated system, we might
// separate these, having one server task communicating over a channel with
// multiple clients (the clients being DNS queries reading data or HTTP requests
// reading or writing data).  That's not really necessary here because `sled`
// does its own synchronization.  Instead, we create one `Store` during program
// startup and clone it as needed.  Each clone has a handle to the same
// underlying database and can read and write to the database.
//
// TODO-scalability There are several places here where we take an approach that
// won't scale well with the number of DNS names.  Mostly: we expect the DNS
// data to come in and go out over HTTP as one big JSON blob, including all
// zones and DNS names.  Since each generation's data is immutable, it wouldn't
// be hard to turn this into a paginated API for reading the DNS data.  It's a
// bit more work (but definitely doable) to do something similar on the way in,
// uploading in chunks.  We could also accept and emit streaming,
// newline-delimited JSON instead.  Both of these could be done in a
// backwards-compatible way (but obviously one wouldn't get the scaling benefits
// while continuing to use the old API).
//
// XXX-dap would it be useful / possible to have a tool for looking at the
// database and rolling back?

use anyhow::{anyhow, bail, Context};
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sled::transaction::ConflictableTransactionError;
use slog::{debug, error, info, o, warn};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use trust_dns_client::rr::LowerName;
use trust_dns_client::rr::Name;

// XXX-dap
use crate::dns_types::*;

/// Configuration for persistent storage of DNS data
#[derive(Deserialize, Debug)]
pub struct Config {
    /// The path for the embedded "sled" kv store
    pub storage_path: Utf8PathBuf,
}

/// Encapsulates persistent storage of DNS data
#[derive(Clone)]
pub struct Store {
    log: slog::Logger,
    db: Arc<sled::Db>,
    keep: usize, // XXX-dap from configuration file
}

const KEY_CONFIG: &'static str = "config";
const KEEP_OLD_TREES: usize = 3;

#[derive(Deserialize, Serialize)]
struct CurrentConfig {
    generation: u64,
    zones: Vec<String>,
}

impl Store {
    pub fn new(
        log: slog::Logger,
        config: &Config,
    ) -> Result<Self, anyhow::Error> {
        let db = sled::open(&config.storage_path).with_context(|| {
            format!("open DNS database {:?}", &config.storage_path)
        })?;

        Ok(Self::new_with_db(log, Arc::new(db)))
    }

    pub fn new_with_db(log: slog::Logger, db: Arc<sled::Db>) -> Store {
        Store { log, db, keep: KEEP_OLD_TREES }
    }

    fn read_config(&self) -> anyhow::Result<CurrentConfig> {
        self.read_config_optional()?.ok_or_else(|| anyhow!("no config found"))
    }

    fn read_config_optional(&self) -> anyhow::Result<Option<CurrentConfig>> {
        self.db
            .get(KEY_CONFIG)
            .context("fetching current config")?
            .map(|config_bytes| {
                serde_json::from_slice(&config_bytes)
                    .context("parsing current config")
            })
            .transpose()
    }

    fn tree_name_for_zone(zone_name: &str, generation: u64) -> String {
        format!("generation_{}_zone_{}", generation, zone_name)
    }

    /// Fetches the full configuration for the current generation (including all
    /// zones and their associated DNS names)
    pub(crate) async fn dns_config(&self) -> Result<DnsConfig, anyhow::Error> {
        let config = self.read_config()?;
        let zones = config
            .zones
            .iter()
            .map(|zone_name| {
                let tree_name =
                    Self::tree_name_for_zone(zone_name, config.generation);
                let tree = self
                    .db
                    .open_tree(&tree_name)
                    .with_context(|| format!("opening tree {:?}", tree_name))?;

                let records = tree
                    .iter()
                    .map(|entry| {
                        let (name_bytes, records_bytes) =
                            entry.context("loading entry")?;
                        let name = std::str::from_utf8(&name_bytes)
                            .with_context(|| {
                                format!("parsing {:?} key name", tree_name)
                            })?;
                        let records: Vec<DnsRecord> =
                            serde_json::from_slice(&records_bytes)
                                .with_context(|| {
                                    format!(
                                        "parsing {:?} key {:?}",
                                        tree_name, name
                                    )
                                })?;
                        Ok(DnsKV {
                            key: DnsRecordKey { name: name.to_owned() },
                            records,
                        })
                    })
                    .collect::<anyhow::Result<_>>()
                    .context("assembling records")?;

                Ok(DnsConfigZone { zone_name: zone_name.to_owned(), records })
            })
            .collect::<anyhow::Result<_>>()?;

        // XXX-dap what happens if any of these trees are removed while this is
        // going on?
        Ok(DnsConfig { generation: config.generation, zones })
    }

    /// Updates to a new generation of DNS data
    ///
    /// See module-level documentation for constraints and design.
    pub(crate) async fn dns_config_update(
        &self,
        config: &DnsConfig,
    ) -> Result<(), anyhow::Error> {
        let log = &self.log.new(o!("new_generation" => config.generation));

        // This boolean acts as a lock to prevent concurrent updates.
        // We must not return early without setting it back!  (We make this
        // harder to do by accident by putting the bulk of the update into a
        // separate function.)
        // XXX-dap doesn't work at the Store level unless we separate it into
        // "server" and "clients".  We could do this at the dropshot level but
        // it'd be a little riskier.
        // if let Err(error) = self.updating.compare_exchange(
        //     false,
        //     true,
        //     Ordering::SeqCst,
        //     Ordering::SeqCst,
        // ) {
        //     error!(log, "skipping update (update already in progress)");
        //     bail!("concurrent update in progress");
        // }

        info!(log, "attempting generation update");
        let result = self.do_update(config).await;
        match &result {
            Ok(_) => info!(log, "updated generation"),
            Err(error) => {
                error!(log, "failed update"; "error_message" => #%error);
            }
        };

        // Nobody else should have modified this value since we locked it above.
        // XXX-dap
        //self.updating
        //    .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
        //    .expect("self.updating changed during update");

        result.with_context(|| {
            format!("update to generation {}", config.generation)
        })
    }

    async fn do_update(&self, config: &DnsConfig) -> Result<(), anyhow::Error> {
        let log = &self.log;
        let generation = config.generation;

        // First, check if we're already at least as new as what's being
        // requested.  Because we should have exclusive access to updates right
        // now, it shouldn't be possible for this to change after we've checked
        // it.
        if let Some(old_config) = self.read_config_optional()? {
            if old_config.generation >= generation {
                bail!("already at generation {}", generation);
            }

            // Prune any trees in the db that are newer than the current
            // generation.
            self.prune_newer(&old_config).await;
        } else {
            self.prune_all().await;
        }

        // For each zone in the config, create the corresponding tree.  Populate
        // it with the data from the config.
        // XXX-dap this is probably a lot faster with a batch?
        for zone_config in &config.zones {
            let zone_name = zone_config.zone_name.to_lowercase();
            let tree_name = Self::tree_name_for_zone(&zone_name, generation);
            debug!(&log, "creating tree"; "tree_name" => &tree_name);
            let tree = self
                .db
                .open_tree(&tree_name)
                .with_context(|| format!("creating tree {:?}", &tree_name))?;

            for record in &zone_config.records {
                let DnsRecordKey { name } = &record.key;
                let name = name.to_lowercase();
                let records_json = serde_json::to_vec(&record.records)
                    .with_context(|| {
                        format!(
                            "serializing records for zone {:?} key {:?}",
                            zone_name, name
                        )
                    })?;
                tree.insert(&name, records_json).with_context(|| {
                    format!(
                        "inserting records for zone {:?} key {:?}",
                        zone_name, name
                    )
                })?;
            }
        }

        let new_config = CurrentConfig {
            generation,
            zones: config.zones.iter().map(|z| z.zone_name.clone()).collect(),
        };
        let new_config_bytes = sled::IVec::from(
            serde_json::to_vec(&new_config)
                .context("serializing current config")?,
        );

        // XXX-dap does Sled guarantee that keys will be persisted in order
        // (i.e., this won't show up before some of the changes to the other
        // tree, will it?)
        debug!(&log, "updating current config");
        let result = self.db.transaction(move |t| {
            // Double-check that the generation we're replacing is older.
            if let Some(old_config_bytes) = t.get(&KEY_CONFIG)? {
                let old_config: CurrentConfig = serde_json::from_slice(
                    &old_config_bytes,
                )
                .map_err(|error| {
                    ConflictableTransactionError::Abort(anyhow!(
                        "parsing config: {:#}",
                        error
                    ))
                })?;
                if old_config.generation >= generation {
                    return Err(ConflictableTransactionError::Abort(anyhow!(
                        "unexpectedly found newer generation {}",
                        old_config.generation
                    )));
                }
            }

            t.insert(KEY_CONFIG, new_config_bytes.clone())?;
            Ok(())
        });

        if let Err(error) = result {
            bail!("final update: {:#}", error);
        }

        self.db.flush_async().await.context("flush")?;

        self.prune_older(&new_config).await;
        Ok(())
    }

    async fn prune_newer(&self, config: &CurrentConfig) {
        let log = &self.log;
        let current_generation = config.generation;

        info!(
            log,
            "pruning trees for generations newer than {}", current_generation
        );

        let trees_to_prune =
            self.all_name_trees().filter_map(|(gen_num, tree_name)| {
                if gen_num > current_generation {
                    Some(tree_name)
                } else {
                    None
                }
            });

        self.prune_trees(trees_to_prune, "too new");
    }

    async fn prune_all(&self) {
        let log = &self.log;
        info!(log, "pruning all name trees");
        self.prune_trees(
            self.all_name_trees().map(|(_, n)| n),
            "too new (no config yet)",
        );
    }

    fn all_name_trees(&self) -> impl Iterator<Item = (u64, String)> {
        self.db.tree_names().into_iter().filter_map(|tree_name_bytes| {
            let tree_name = std::str::from_utf8(&tree_name_bytes).ok()?;
            let parts = tree_name.splitn(4, '_').collect::<Vec<_>>();
            if parts.len() != 4
                || parts[0] != "generation"
                || parts[2] != "zone"
            {
                return None;
            }

            let gen_num = parts[1].parse::<u64>().ok()?;
            Some((gen_num, tree_name.to_owned()))
        })
    }

    fn prune_trees<I>(&self, trees_to_prune: I, reason: &'static str)
    where
        I: Iterator<Item = String>,
    {
        let log = &self.log;

        for tree_name in trees_to_prune {
            info!(
                log,
                "pruning tree";
                "tree_name" => &tree_name,
                "reason" => reason
            );

            if let Err(error) = self.db.drop_tree(&tree_name) {
                warn!(
                    log,
                    "failed to remove tree";
                    "tree_name" => &tree_name,
                    "error_message" => #%error,
                );
            }
        }
    }

    async fn prune_older(&self, config: &CurrentConfig) {
        let log = &self.log;
        let keep = self.keep;
        let current_generation = config.generation;

        info!(
            log,
            "pruning trees for generations older than {}", current_generation;
            "keep" => keep,
        );

        let mut trees_older = self
            .all_name_trees()
            .filter(|(gen_num, _)| *gen_num < current_generation)
            .collect::<Vec<_>>();

        // Now remove all but the last "keep" items.
        if trees_older.len() < keep {
            return;
        }

        // Sort by each tree's generation number and take the first "keep".
        trees_older.sort_by_key(|(k, _)| *k);
        let ntake = trees_older.len() - keep;
        let trees_to_prune =
            trees_older.into_iter().take(ntake).map(|(_, n)| n);
        self.prune_trees(trees_to_prune, "too old");
    }

    pub(crate) async fn query(
        &self,
        mr: &trust_dns_server::authority::MessageRequest,
    ) -> Result<Vec<DnsRecord>, QueryError> {
        let config = self.read_config().map_err(QueryError::QueryFail)?;
        let name = mr.query().name();

        let zone_name = config
            .zones
            .iter()
            .find(|z| {
                let zone_name = LowerName::from(Name::from_str(&z).unwrap());
                zone_name.zone_of(name)
            })
            .ok_or_else(|| QueryError::NoZone(name.to_string()))?;

        let tree_name = Self::tree_name_for_zone(zone_name, config.generation);
        let tree = self
            .db
            .open_tree(&tree_name)
            .with_context(|| format!("open tree {:?}", tree_name))
            .map_err(QueryError::QueryFail)?;

        // The name tree stores just the part of each name that doesn't include
        // the zone.  So we need to trim the zone part from the name provided in
        // the request.  (This basically duplicates work in `zone_of` above.)
        let name_str = name.to_string();
        let key = {
            let orig_name = mr.query().original().name();
            let zone_name = Name::from_str(zone_name).unwrap();
            // This is implied by passing the `zone_of()` check above.
            assert!(zone_name.num_labels() >= orig_name.num_labels());
            let name_only_labels =
                usize::from(zone_name.num_labels() - orig_name.num_labels());
            let name_only =
                Name::from_labels(orig_name.iter().take(name_only_labels))
                    .unwrap();
            let key = name_only.to_string().to_lowercase();
            assert!(!key.ends_with('.'));
            key
        };

        let bits = tree
            .get(key.as_bytes())
            .with_context(|| format!("query tree {:?}", tree_name))
            .map_err(QueryError::QueryFail)?
            .ok_or_else(|| QueryError::NoName(name_str.clone()))?;

        let records: Vec<DnsRecord> = serde_json::from_slice(&bits)
            .with_context(|| {
                format!("deserialize record for key {:?}", name_str.clone())
            })
            .map_err(QueryError::ParseFail)?;

        if records.is_empty() {
            // XXX-dap we should make this illegal (by not inserting these) and
            // then warn here.
            return Err(QueryError::NoName(name_str.clone()));
        }

        Ok(records)
    }
}

#[derive(Debug, Error)]
pub(crate) enum QueryError {
    #[error("server is not authoritative for name: {0:?}")]
    NoZone(String),

    #[error("no records found for name: {0:?}")]
    NoName(String),

    #[error("failed to query database")]
    QueryFail(#[source] anyhow::Error),

    #[error("failed to parse database result")]
    ParseFail(#[source] anyhow::Error),
}
