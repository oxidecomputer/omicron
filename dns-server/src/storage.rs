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

use crate::dns_types::{
    DnsConfig, DnsConfigParams, DnsConfigZone, DnsKV, DnsRecord, DnsRecordKey,
};
use anyhow::{anyhow, Context};
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sled::transaction::ConflictableTransactionError;
use slog::{debug, error, info, o, warn};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use trust_dns_client::rr::LowerName;
use trust_dns_client::rr::Name;

const KEY_CONFIG: &'static str = "config";

/// Configuration for persistent storage of DNS data
#[derive(Deserialize, Debug)]
pub struct Config {
    /// The path for the embedded "sled" kv store
    pub storage_path: Utf8PathBuf,
    /// How many previous generations' DNS data to keep
    pub keep_old_generations: usize,
}

/// Encapsulates persistent storage of DNS data
#[derive(Clone)]
pub struct Store {
    log: slog::Logger,
    db: Arc<sled::Db>,
    keep: usize,
    updating: Arc<Mutex<Option<UpdateInfo>>>,
}

#[derive(Deserialize, Serialize)]
struct CurrentConfig {
    generation: u64,
    zones: Vec<String>,
    time_created: chrono::DateTime<chrono::Utc>,
    time_applied: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error(
        "unsupported attempt to update to generation \
        {attempted_generation} while at generation {current_generation}"
    )]
    BadUpdateGeneration { current_generation: u64, attempted_generation: u64 },

    #[error(
        "update already in progress (from req_id {req_id:?}, \
        to generation {generation}, started at {start_time} ({elapsed} ago))"
    )]
    UpdateInProgress {
        start_time: chrono::DateTime<chrono::Utc>,
        elapsed: chrono::Duration,
        generation: u64,
        req_id: String,
    },

    #[error("internal error")]
    InternalError(#[from] anyhow::Error),
}

impl Store {
    pub fn new(
        log: slog::Logger,
        config: &Config,
    ) -> Result<Self, anyhow::Error> {
        info!(&log,
            "opening sled database";
            "path" => &config.storage_path.to_string()
        );

        let db = sled::open(&config.storage_path).with_context(|| {
            format!("open DNS database {:?}", &config.storage_path)
        })?;

        Self::new_with_db(log, Arc::new(db), config)
    }

    pub fn new_with_db(
        log: slog::Logger,
        db: Arc<sled::Db>,
        config: &Config,
    ) -> Result<Self, anyhow::Error> {
        let store = Store {
            log,
            db,
            keep: config.keep_old_generations,
            updating: Arc::new(Mutex::new(None)),
        };
        if store.read_config_optional()?.is_none() {
            let now = chrono::Utc::now();
            let initial_config_bytes = serde_json::to_vec(&CurrentConfig {
                generation: 0,
                zones: vec![],
                time_created: now,
                time_applied: now,
            })
            .context("serializing initial config")?;
            store
                .db
                .insert(&KEY_CONFIG, initial_config_bytes)
                .context("inserting initial config")?;
        }

        let config = store.read_config()?;
        store.prune_newer(&config);
        store.prune_older(&config);
        Ok(store)
    }

    /// Returns true if this Store's database was newly created when this Store
    /// was created (i.e., we did not restore data from an old database)
    ///
    /// This is only intended for testing.
    pub fn is_new(&self) -> bool {
        !self.db.was_recovered()
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
                // TODO-correctness What happens if any of these trees are
                // removed while we're doing this (as might happen if somebody
                // does an update)?  In practice this seems unlikely because we
                // keep the last few generations' trees.  If it does happen, it
                // seems like we'll wind up bailing with a SERVFAIL.  That's not
                // great, but it's not the worst.  A retry should work as long
                // as updates aren't constantly streaming in.  If this becomes a
                // problem, we could centrally track the generations being read
                // and avoid deleting trees that we would otherwise prune until
                // those reads finish.  (That creates a new problem: what if the
                // read gets stuck for some reason?  We don't want to leave
                // these trees hanging around forever.)
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

        Ok(DnsConfig {
            generation: config.generation,
            time_created: config.time_created,
            time_applied: config.time_applied,
            zones,
        })
    }

    async fn begin_update<'a, 'b>(
        &'a self,
        req_id: &'b str,
        generation: u64,
    ) -> Result<UpdateGuard<'a, 'b>, UpdateError> {
        let mut update = self.updating.lock().await;
        if let Some(ref update) = *update {
            let elapsed =
                chrono::Duration::from_std(update.start_instant.elapsed())
                    .context("elapsed duration out of range")?;
            return Err(UpdateError::UpdateInProgress {
                start_time: update.start_time,
                elapsed,
                generation: update.generation,
                req_id: update.req_id.clone(),
            });
        }

        *update = Some(UpdateInfo {
            start_time: chrono::Utc::now(),
            start_instant: std::time::Instant::now(),
            generation,
            req_id: req_id.to_string(),
        });

        Ok(UpdateGuard { store: self, req_id, finished: false })
    }

    /// Updates to a new generation of DNS data
    ///
    /// See module-level documentation for constraints and design.
    pub(crate) async fn dns_config_update(
        &self,
        config: &DnsConfigParams,
        req_id: &str,
    ) -> Result<(), UpdateError> {
        let log = &self.log.new(o!(
            "req_id" => req_id.to_owned(),
            "new_generation" => config.generation
        ));

        // Lock out concurrent updates.  We must not return until we've released
        // the "updating" lock!  (See UpdateGuard's `drop` impl.)
        let update = self.begin_update(req_id, config.generation).await?;

        info!(log, "attempting generation update");
        let result = self.do_update(config).await;
        match &result {
            Ok(_) => info!(log, "updated generation"),
            Err(error) => {
                error!(log, "failed update"; "error_message" => #%error);
            }
        };

        // Release our lock on concurrent update.
        update.finish().await;

        result
    }

    async fn do_update(
        &self,
        config: &DnsConfigParams,
    ) -> Result<(), UpdateError> {
        let log = &self.log;
        let generation = config.generation;

        // First, check if we're already at least as new as what's being
        // requested.  Because we should have exclusive access to updates right
        // now, it shouldn't be possible for this to change after we've checked
        // it.
        let old_config = self.read_config()?;
        if old_config.generation > generation {
            return Err(UpdateError::BadUpdateGeneration {
                current_generation: old_config.generation,
                attempted_generation: config.generation,
            });
        }
        if old_config.generation == generation {
            return Ok(());
        }

        // Prune any trees in the db that are newer than the current generation.
        // These could exist if we were previously crashed while trying to move
        // to this generation.
        self.prune_newer(&old_config);

        // For each zone in the config, create the corresponding tree.  Populate
        // it with the data from the config.
        // TODO-performance This would probably be a lot faster with a batch
        // operation.
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
                if record.records.is_empty() {
                    // There's no distinction between in DNS between a name that
                    // doesn't exist at all and one with no records associated
                    // with it.  If there are no records, don't bother inserting
                    // the name.
                    continue;
                }
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

            // Flush this tree.  We do this here to make sure the tree is fully
            // written before we update the config in the main tree below.
            // Otherwise, if Sled reorders writes between flush points, it's
            // possible that if we crash between here and the final flush below,
            // then we could come back up having updated config that refers to a
            // tree that was never flushed.  It's not clear if sled _does_ allow
            // this, but it's not clear that it doesn't.  It's safer to just
            // flush here.  This code path is assumed not to be particularly
            // latency-sensitive.
            tree.flush_async()
                .await
                .with_context(|| format!("flush tree {:?}", tree_name))?;
        }

        let new_config = CurrentConfig {
            generation,
            zones: config.zones.iter().map(|z| z.zone_name.clone()).collect(),
            time_created: config.time_created,
            time_applied: chrono::Utc::now(),
        };
        let new_config_bytes = sled::IVec::from(
            serde_json::to_vec(&new_config)
                .context("serializing current config")?,
        );

        debug!(&log, "updating current config");
        let result = self.db.transaction(move |t| {
            // Double-check that the generation we're replacing is older.
            let old_config_bytes = t.get(&KEY_CONFIG)?.ok_or_else(|| {
                ConflictableTransactionError::Abort(anyhow!(
                    "found no config during update",
                ))
            })?;

            let old_config: CurrentConfig =
                serde_json::from_slice(&old_config_bytes).map_err(|error| {
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

            t.insert(KEY_CONFIG, new_config_bytes.clone())?;
            Ok(())
        });

        result.map_err(|error| anyhow!("final update: {:#}", error))?;

        debug!(&log, "flushing default tree");
        self.db.flush_async().await.context("flush")?;

        self.prune_older(&new_config);
        Ok(())
    }

    fn prune_newer(&self, config: &CurrentConfig) {
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

    fn prune_older(&self, config: &CurrentConfig) {
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

    /// Returns a non-empty list of DNS records associated with the name in the
    /// given DNS request.
    ///
    /// If the returned set would have been empty, returns `QueryError::NoName`.
    pub(crate) fn query(
        &self,
        mr: &trust_dns_server::authority::MessageRequest,
    ) -> Result<Vec<DnsRecord>, QueryError> {
        let name = mr.query().name();
        let orig_name = mr.query().original().name();
        self.query_name(name, orig_name)
    }

    fn query_name(
        &self,
        name: &LowerName,
        orig_name: &Name,
    ) -> Result<Vec<DnsRecord>, QueryError> {
        let config = self.read_config().map_err(QueryError::QueryFail)?;

        let zone_name = config
            .zones
            .iter()
            .find(|z| {
                let zone_name = LowerName::from(Name::from_str(&z).unwrap());
                zone_name.zone_of(name)
            })
            .ok_or_else(|| QueryError::NoZone(orig_name.to_string()))?;

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
            let zone_name = Name::from_str(zone_name).unwrap();
            // This is implied by passing the `zone_of()` check above.
            assert!(zone_name.num_labels() <= orig_name.num_labels());
            let name_only_labels =
                usize::from(orig_name.num_labels() - zone_name.num_labels());
            let mut name_only =
                Name::from_labels(orig_name.iter().take(name_only_labels))
                    .unwrap();
            name_only.set_fqdn(false);
            let key = name_only.to_string().to_lowercase();
            assert!(!key.ends_with('.'));
            key
        };

        debug!(&self.log, "query key"; "key" => &key);

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
            // This shouldn't be possible because we don't insert names with no
            // records.
            warn!(
                &self.log,
                "found name with no records";
                "dns_name" => &name_str
            );

            return Err(QueryError::NoName(name_str));
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

/// Describes an ongoing update, if any
struct UpdateInfo {
    start_time: chrono::DateTime<chrono::Utc>,
    start_instant: std::time::Instant,
    generation: u64,
    req_id: String,
}

/// Used to help ensure that code paths that begin an exclusive update also
/// release their exclusive lock.
struct UpdateGuard<'store, 'req_id> {
    store: &'store Store,
    req_id: &'req_id str,
    finished: bool,
}

impl<'a, 'b> UpdateGuard<'a, 'b> {
    async fn finish(mut self) {
        let store = self.store;
        let mut update = store.updating.lock().await;
        match update.take() {
            None => panic!(
                "expected to end update from req_id {:?}, but \
                there is no update in progress",
                self.req_id,
            ),
            Some(UpdateInfo { req_id, .. }) if req_id != self.req_id => panic!(
                "expected to end update from req_id {:?}, but \
                    the current update is from req_id {:?}",
                self.req_id, req_id
            ),
            _ => (),
        };
        self.finished = true;
    }
}

impl<'a, 'b> Drop for UpdateGuard<'a, 'b> {
    fn drop(&mut self) {
        // TODO-cleanup It would be far better if we could enforce this at
        // compile-time, similar to a MutexGuard.  The obvious approach of doing
        // it on drop does not work because we cannot take the async lock from
        // the synchronous Drop function.  And we don't want to use a std Mutex
        // and risk blocking the executor.  And it doesn't seem like we can use
        // blocking_lock() because we _are_ in an asynchronous context.  We
        // could use a semaphore like tokio's MutexGuard does, but that would
        // involve unsafe code.)
        if !self.finished {
            panic!("attempted to return early without finishing update!");
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Config, Store};
    use crate::dns_types::DnsConfigParams;
    use crate::dns_types::DnsConfigZone;
    use crate::dns_types::DnsKV;
    use crate::dns_types::DnsRecord;
    use crate::dns_types::DnsRecordKey;
    use crate::storage::QueryError;
    use omicron_test_utils::dev::test_setup_log;
    use std::net::Ipv6Addr;
    use std::str::FromStr;
    use trust_dns_client::rr::LowerName;
    use trust_dns_client::rr::Name;

    /// As usual, `TestContext` groups the various pieces we need in a bunch of
    /// our tests and helps make sure they get cleaned up properly.
    struct TestContext {
        logctx: dropshot::test_util::LogContext,
        tmpdir: tempdir::TempDir,
        store: Store,
    }

    impl TestContext {
        fn new(test_name: &str) -> TestContext {
            let logctx = test_setup_log(test_name);
            let tmpdir = tempdir::TempDir::new("dns-server-storage-test")
                .expect("failed to create tmp directory for test");
            let storage_path = camino::Utf8PathBuf::from_path_buf(
                tmpdir.path().to_path_buf(),
            )
            .expect(
                "failed to create Utf8PathBuf for test temporary directory",
            );
            let store = Store::new(
                logctx.log.clone(),
                &Config { storage_path, keep_old_generations: 3 },
            )
            .expect("failed to create test Store");
            assert!(store.is_new());
            TestContext { logctx, tmpdir, store }
        }

        /// Invoke upon successful completion of a test to clean up the
        /// temporary files that were made.  These files are deliberately
        /// preserved for debugging on failure.
        fn cleanup_successful(self) {
            self.logctx.cleanup_successful();

            // These are redundant given the current implementation (that this
            // function consumes `self`).  But they're here for clarity: first
            // we drop the Store to close the database.  Then we drop the
            // temporary directory so that it gets removed.
            drop(self.store);
            drop(self.tmpdir);
        }
    }

    /// Describes what one of the tests expects to get back for a particular DNS
    /// query
    #[derive(Debug)]
    enum Expect<'a> {
        NoZone,
        NoName,
        Record(&'a DnsRecord),
    }

    /// Looks up the given name and verifies that the store layer returns the
    /// correct error: that the name is not in a zone that we know about
    fn expect(store: &Store, name: &str, expect: Expect<'_>) {
        let dns_name_orig = Name::from_str(name).expect("bad DNS name");
        let dns_name_lower = LowerName::from(dns_name_orig.clone());
        let result = store.query_name(&dns_name_lower, &dns_name_orig);
        println!(
            "expecting {:?} for query of {:?}: {:?}",
            expect, name, result
        );

        match (expect, result) {
            (Expect::NoZone, Err(QueryError::NoZone(n))) if n == name => (),
            (Expect::NoName, Err(QueryError::NoName(n))) if n == name => (),
            (Expect::Record(r), Ok(records))
                if records.len() == 1 && records[0] == *r =>
            {
                ()
            }
            _ => panic!("did not get what we expected from DNS query"),
        }
    }

    #[tokio::test]
    async fn test_update_success() {
        let tc = TestContext::new("test_update_success");

        // XXX-dap figure out how best to generalize this in terms of
        // declarative generations.

        // Verify the initial configuration.
        let config = tc.store.dns_config().await.unwrap();
        assert_eq!(config.generation, 0);
        assert!(config.zones.is_empty());
        expect(&tc.store, "gen1_name.zone1.internal", Expect::NoZone);
        expect(&tc.store, "Gen1_name.zone1.internal", Expect::NoZone);
        expect(&tc.store, "shared_name.zone1.internal", Expect::NoZone);
        expect(&tc.store, "gen2_name.zone2.internal", Expect::NoZone);
        expect(&tc.store, "gen3_name.zone3.internal", Expect::NoZone);

        // Update to generation 1, which contains one zone with one name.
        let dummy_record = DnsRecord::AAAA(Ipv6Addr::LOCALHOST);
        let update1 = DnsConfigParams {
            time_created: chrono::Utc::now(),
            generation: 1,
            zones: vec![DnsConfigZone {
                zone_name: "zone1.internal".to_string(),
                records: vec![
                    DnsKV {
                        key: DnsRecordKey { name: "gen1_name".to_string() },
                        records: vec![dummy_record.clone()],
                    },
                    DnsKV {
                        key: DnsRecordKey { name: "shared_name".to_string() },
                        records: vec![dummy_record.clone()],
                    },
                ],
            }],
        };

        tc.store.dns_config_update(&update1, "my request id").await.unwrap();
        expect(
            &tc.store,
            "gen1_name.zone1.internal",
            Expect::Record(&dummy_record),
        );
        expect(
            &tc.store,
            "gen1_name.ZONE1.internal",
            Expect::Record(&dummy_record),
        );
        expect(
            &tc.store,
            "Gen1_name.zone1.internal",
            Expect::Record(&dummy_record),
        );
        expect(
            &tc.store,
            "shared_name.zone1.internal",
            Expect::Record(&dummy_record),
        );
        expect(&tc.store, "enoent.zone1.internal", Expect::NoName);
        expect(&tc.store, "gen2_name.zone2.internal", Expect::NoZone);
        expect(&tc.store, "gen3_name.zone3.internal", Expect::NoZone);

        // XXX-dap when it comes time to test the update process, maybe the
        // thing to is separate the existing do_update() into two phases that
        // the test suite can call separately, with some checks in between.  Or
        // maybe we can drop the store, keep the Db, make a new Store, and see
        // what it does
        tc.cleanup_successful();
    }

    // XXX-dap TODO-coverage
    // A thought on testing the storage stuff: maybe the thing to do is to set
    // up the Db in a particular way, then apply an update, then inspect the
    // contents of the Db?  That is, the primitive operation that we're testing
    // is: given this Db, apply this update?  That will let us verify a bunch of
    // cases:
    // - update to a generation that had an aborted update (i.e., left turds
    //   around)
    // - update to a generation older than current
    // - cleanup of older generations' data (including cases where we keep say 3
    //   generations' worth that might span 5 generation numbers (if we never
    //   updated to those intermediate ones))
}
