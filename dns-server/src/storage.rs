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

use anyhow::{Context, anyhow};
use camino::Utf8PathBuf;
use hickory_proto::{op::LowerQuery, rr::LowerName};
use hickory_resolver::Name;
use hickory_server::authority::Catalog;
use internal_dns_types::{
    config::{DnsConfig, DnsConfigParams, DnsConfigZone, DnsRecord},
    names::ZONE_APEX_NAME,
};
use omicron_common::api::external::Generation;
use serde::{Deserialize, Serialize};
use sled::transaction::ConflictableTransactionError;
use slog::{debug, error, info, o, warn};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use thiserror::Error;
use tokio::sync::Mutex;

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
    poisoned: Arc<AtomicBool>,
    catalog_tx: tokio::sync::watch::Sender<Arc<Catalog>>,
}

/// A temporary schema for DNS configurations from before the presence of the
/// `serial` field.
///
/// This form of `CurrentConfig` can be removed once we are certain all DNS
/// configurations have been updated to include a `serial` field.
///
/// This is necessary for an unfortunate chicken-and-egg problem: each DNS zone
/// (both internal and external) has a copy of its current configuration as a
/// JSON file in its zone. When upgraded, Nexus will be able to provide a new
/// configuration consistent with `CurrentConfig`. But to get Nexus running,
/// internal DNS must be able to function sufficiently for internal services to
/// start - Nexus, CockroachDB, etc. And herein lies the problem; immediately
/// after upgrading, internal DNS will have a configuration without `serial` on
/// disk, so it won't be able to load the configuration, won't serve any
/// records, and sled-agent will get stuck very early on in bringing up the
/// system post-upgrade.
///
/// So, we maintain support for reading the previous configuration schema which
/// is trivially and (almost) infallibly convertable to the new schema with
/// `serial`.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ConfigWithoutSerial {
    generation: Generation,
    zones: Vec<String>,
    time_created: chrono::DateTime<chrono::Utc>,
    time_applied: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CurrentConfig {
    generation: Generation,
    serial: u32,
    zones: Vec<String>,
    time_created: chrono::DateTime<chrono::Utc>,
    time_applied: chrono::DateTime<chrono::Utc>,
}

impl TryFrom<ConfigWithoutSerial> for CurrentConfig {
    type Error = anyhow::Error;

    fn try_from(value: ConfigWithoutSerial) -> Result<Self, Self::Error> {
        let ConfigWithoutSerial {
            generation,
            zones,
            time_created,
            time_applied,
        } = value;

        // This is.. unlikely to say the least, but it would be impolite to
        // panic here. To overflow a u32, the generation number would have had
        // to be bumped on average 68 times a second, every second, for two
        // years. If the generation number were this high, it would certainly
        // imply other issues (and we would imminently have issues when Nexus
        // tries this same conversion).
        let serial = generation
            .as_u64()
            .try_into()
            .context("generation overflows u32?")?;

        Ok(CurrentConfig {
            generation,
            serial,
            zones,
            time_created,
            time_applied,
        })
    }
}

impl CurrentConfig {
    /// Try parsing the provided bytes as JSON representing a `CurrentConfig`.
    /// If not a `CurrentConfig`, try parsing as a `ConfigWithoutSerial` and
    /// converting it forward.
    fn parse_with_fallback(bytes: &[u8]) -> anyhow::Result<Self> {
        let current_result = serde_json::from_slice::<Self>(&bytes)
            .context("parsing current config");

        // If we can't parse the current on-disk configuration format,
        // it may just be in the old format. Try parsing it that way
        // instead; if we can read it as an old configuration, we can
        // translate it forward and move on. If we still can't read it,
        // the initial result is more representative of whatever went
        // wrong, so if there's an error here we ignore it.
        if current_result.is_err() {
            let without_serial =
                serde_json::from_slice::<ConfigWithoutSerial>(&bytes);

            if let Ok(without_serial) = without_serial {
                return CurrentConfig::try_from(without_serial);
            }
        }

        current_result
    }
}

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error(
        "unsupported attempt to update to generation \
        {attempted_generation} while at generation {current_generation}"
    )]
    BadUpdateGeneration {
        current_generation: Generation,
        attempted_generation: Generation,
    },

    #[error(
        "update already in progress (from req_id {req_id:?}, \
        to generation {generation}, started at {start_time} ({elapsed} ago))"
    )]
    UpdateInProgress {
        start_time: chrono::DateTime<chrono::Utc>,
        elapsed: chrono::Duration,
        generation: Generation,
        req_id: String,
    },

    #[error("internal error")]
    InternalError(#[from] anyhow::Error),
}

impl Store {
    /// Build a Catalog from the current DNS configuration
    ///
    /// This creates a hickory-server Catalog with one Authority for each zone
    /// we're authoritative for.
    fn build_catalog(&self) -> Result<Catalog, anyhow::Error> {
        use crate::authority::OmicronAuthority;
        use hickory_server::authority::ZoneType;

        let config = self.read_config()?;
        let mut catalog = Catalog::new();

        info!(&self.log, "building catalog"; "num_zones" => config.zones.len());

        for zone_name in &config.zones {
            info!(&self.log, "adding zone to catalog"; "zone" => zone_name);
            // Parse the zone name and ensure it's absolute (ends with .)
            let mut origin = Name::from_str(zone_name)
                .with_context(|| format!("parsing zone name {:?}", zone_name))?;

            // Make sure the name is absolute (FQDN)
            if !origin.is_fqdn() {
                // Append the root label to make it absolute
                origin = origin.append_domain(&Name::root())
                    .with_context(|| format!("making zone name absolute: {:?}", zone_name))?;
            }

            info!(&self.log, "parsed zone name";
                "zone" => zone_name,
                "origin" => ?origin,
                "is_fqdn" => origin.is_fqdn());

            let authority = Arc::new(OmicronAuthority::new(
                self.clone(),
                origin.clone(),
                ZoneType::Primary,
                self.log.new(o!("zone" => zone_name.clone())),
            ));

            let origin_lower = origin.clone().into();
            catalog.upsert(origin_lower, vec![authority as Arc<_>]);
            info!(&self.log, "added zone to catalog"; "zone" => zone_name, "origin" => ?origin);
        }

        info!(&self.log, "catalog built successfully"; "total_zones" => config.zones.len());
        Ok(catalog)
    }

    /// Get a receiver for catalog updates
    ///
    /// This returns a watch channel receiver that will be notified whenever
    /// the DNS configuration changes and the catalog is rebuilt.
    pub fn catalog_receiver(&self) -> tokio::sync::watch::Receiver<Arc<Catalog>> {
        self.catalog_tx.subscribe()
    }

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
        // Create initial empty catalog and watch channel
        let initial_catalog = Catalog::new();
        let (catalog_tx, _catalog_rx) = tokio::sync::watch::channel(Arc::new(initial_catalog));

        let store = Store {
            log,
            db,
            keep: config.keep_old_generations,
            updating: Arc::new(Mutex::new(None)),
            poisoned: Arc::new(AtomicBool::new(false)),
            catalog_tx,
        };

        if store.read_config_optional()?.is_none() {
            let now = chrono::Utc::now();
            let initial_config_bytes = serde_json::to_vec(&CurrentConfig {
                generation: Generation::from_u32(0),
                serial: 0,
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

        let current_config = store.read_config()?;
        store.prune_newer(&current_config);
        store.prune_older(&current_config);

        // Build and broadcast initial catalog
        match store.build_catalog() {
            Ok(catalog) => {
                info!(&store.log, "sending initial catalog on watch channel");
                let _ = store.catalog_tx.send(Arc::new(catalog));
            }
            Err(e) => {
                warn!(&store.log, "failed to build initial catalog"; "error" => ?e);
            }
        }

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
                CurrentConfig::parse_with_fallback(config_bytes.as_ref())
            })
            .transpose()
    }

    fn tree_name_for_zone(zone_name: &str, generation: Generation) -> String {
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
                        Ok((name.to_owned(), records))
                    })
                    .collect::<anyhow::Result<_>>()
                    .context("assembling records")?;

                Ok(DnsConfigZone { zone_name: zone_name.to_owned(), records })
            })
            .collect::<anyhow::Result<_>>()?;

        Ok(DnsConfig {
            generation: config.generation,
            serial: config.serial,
            time_created: config.time_created,
            time_applied: config.time_applied,
            zones,
        })
    }

    pub(crate) fn soa_for(
        &self,
        answer: &Answer,
    ) -> Result<hickory_proto::rr::Record, QueryError> {
        fn name_from_str(s: impl AsRef<str>) -> Result<Name, QueryError> {
            let name_str = s.as_ref();
            Name::from_str(name_str).map_err(|error| {
                QueryError::ParseFail(anyhow!(
                    "unable to create a Name from {:?}: {:#}",
                    name_str,
                    error
                ))
            })
        }

        let apex_answer =
            self.query_name(&name_from_str(answer.zone.as_str())?)?;

        let mut nameservers = apex_answer
            .records
            .as_ref()
            .map(|records| {
                records
                    .iter()
                    .filter_map(|record| {
                        if let DnsRecord::Ns(nsdname) = record {
                            Some(nsdname)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or(Vec::new());

        nameservers.sort();
        let preferred_nameserver = nameservers
            .first()
            .ok_or_else(|| {
                QueryError::QueryFail(anyhow!(
                    "tried to produce an SOA record but \
                    the zone has no nameservers"
                ))
            })
            .map(name_from_str)??;

        let soa_name = name_from_str(&answer.queried_fqdn())?;
        let rname = name_from_str(format!("admin.{}", answer.zone.as_str()))?;

        let record = hickory_proto::rr::Record::from_rdata(
            soa_name,
            0,
            hickory_proto::rr::RData::SOA(hickory_proto::rr::rdata::SOA::new(
                preferred_nameserver,
                rname,
                answer.serial,
                3600,
                600,
                1800,
                600,
            )),
        );

        Ok(record)
    }

    async fn begin_update<'a, 'b>(
        &'a self,
        req_id: &'b str,
        generation: Generation,
    ) -> Result<UpdateGuard<'a, 'b>, UpdateError> {
        if self.poisoned.load(Ordering::SeqCst) {
            panic!(
                "store is poisoned (attempted update after previous \
                UpdateGuard was dropped)"
            );
        }

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
            "new_generation" => u64::from(config.generation),
        ));

        // Lock out concurrent updates.  We must not return until we've released
        // the "updating" lock.
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
        //
        // We are authoritative for zones whose records we serve, so we also
        // create an SOA record at this point.  This record is not provided by
        // the control plane for simplicity; we can determine the serial from
        // the generation we are updating to.
        //
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

            for (name, records) in &zone_config.records {
                if records.is_empty() {
                    // There's no distinction between in DNS between a name that
                    // doesn't exist at all and one with no records associated
                    // with it.  If there are no records, don't bother inserting
                    // the name.
                    continue;
                }

                let records_json =
                    serde_json::to_vec(&records).with_context(|| {
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
            serial: config.serial,
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
                CurrentConfig::parse_with_fallback(old_config_bytes.as_ref())
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

            t.insert(KEY_CONFIG, new_config_bytes.clone())?;
            Ok(())
        });

        result.map_err(|error| anyhow!("final update: {:#}", error))?;

        debug!(&log, "flushing default tree");
        self.db.flush_async().await.context("flush")?;

        self.prune_older(&new_config);

        // Rebuild and broadcast the catalog with the new zone configuration
        match self.build_catalog() {
            Ok(catalog) => {
                debug!(&log, "rebuilt catalog after update");
                info!(&log, "sending updated catalog on watch channel"; "has_receivers" => self.catalog_tx.receiver_count());
                let _ = self.catalog_tx.send(Arc::new(catalog));
            }
            Err(e) => {
                warn!(&log, "failed to rebuild catalog after update"; "error" => ?e);
                // Don't fail the update if catalog rebuild fails
            }
        }

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

    fn all_name_trees(&self) -> impl Iterator<Item = (Generation, String)> {
        self.db.tree_names().into_iter().filter_map(|tree_name_bytes| {
            let tree_name = std::str::from_utf8(&tree_name_bytes).ok()?;
            let parts = tree_name.splitn(4, '_').collect::<Vec<_>>();
            if parts.len() != 4
                || parts[0] != "generation"
                || parts[2] != "zone"
            {
                return None;
            }

            let gen_num =
                Generation::try_from(parts[1].parse::<u64>().ok()?).ok()?;
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

    /// Returns an [`Answer`] describing the records associated with the name in
    /// the given DNS request, as well as the zone containing the name and the
    /// name prefix in that zone that the query is for.
    ///
    /// If the name does not match any zone, returns `QueryError::NoZone`.
    pub(crate) fn query(
        &self,
        query: &LowerQuery,
    ) -> Result<Answer, QueryError> {
        let name = query.name();
        let orig_name = query.original().name();
        self.query_raw(name, orig_name)
    }

    /// Returns an [`Answer`] describing the records associated with the given
    /// name, as well as the zone containing the name and the name prefix in
    /// that zone that the query is for.
    ///
    /// If the name does not match any zone, returns `QueryError::NoZone`.
    pub(crate) fn query_name(&self, name: &Name) -> Result<Answer, QueryError> {
        self.query_raw(&LowerName::new(name), name)
    }

    fn query_raw(
        &self,
        name: &LowerName,
        orig_name: &Name,
    ) -> Result<Answer, QueryError> {
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
            if key.is_empty() { ZONE_APEX_NAME.to_string() } else { key }
        };

        debug!(&self.log, "query key"; "key" => &key);

        let mut answer = Answer {
            zone: zone_name.clone(),
            name: if key == ZONE_APEX_NAME { None } else { Some(key.clone()) },
            serial: config.serial,
            records: None,
        };

        let record_json = tree
            .get(key.as_bytes())
            .with_context(|| format!("query tree {:?}", tree_name))
            .map_err(QueryError::QueryFail)?;

        if let Some(record_json) = record_json {
            let records: Vec<DnsRecord> = serde_json::from_slice(&record_json)
                .with_context(|| {
                    format!("deserialize record for key {:?}", key)
                })
                .map_err(QueryError::ParseFail)?;

            if records.is_empty() {
                // This shouldn't be possible because we don't insert names with no
                // records.
                warn!(
                    &self.log,
                    "found name with no records";
                    "key" => &key
                );

                return Ok(answer);
            }

            answer.records = Some(records);
        }

        Ok(answer)
    }
}

#[derive(Debug, Error)]
pub(crate) enum QueryError {
    #[error("server is not authoritative for name: {0:?}")]
    NoZone(String),

    #[error("failed to query database")]
    QueryFail(#[source] anyhow::Error),

    #[error("failed to parse database result")]
    ParseFail(#[source] anyhow::Error),
}

/// The records to answer a query, along with the name and zone that matched the
/// query.
#[derive(Debug)]
pub(crate) struct Answer {
    zone: String,
    /// The name in `zone` that this answer describes. `None` if the query is
    /// for the zone apex.
    pub name: Option<String>,
    /// The serial number for the zone which provided this answer. While this
    /// currently matches the DNS config generation that provided this answer,
    /// they may differ in the future.
    serial: u32,
    pub records: Option<Vec<DnsRecord>>,
}

impl Answer {
    pub fn queried_fqdn(&self) -> String {
        if let Some(name) = self.name.as_ref() {
            format!("{}.{}", name, self.zone)
        } else {
            self.zone.clone()
        }
    }
}

/// Describes an ongoing update, if any
struct UpdateInfo {
    start_time: chrono::DateTime<chrono::Utc>,
    start_instant: std::time::Instant,
    generation: Generation,
    req_id: String,
}

/// Used to help ensure that code paths that begin an exclusive update also
/// release their exclusive lock.
struct UpdateGuard<'store, 'req_id> {
    store: &'store Store,
    req_id: &'req_id str,
    finished: bool,
}

impl UpdateGuard<'_, '_> {
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

impl Drop for UpdateGuard<'_, '_> {
    fn drop(&mut self) {
        // UpdateGuard exists because we must enforce at most one Update is
        // happening at a time, but we also want to catch the case where an
        // errant code path begins an update and forgets to call
        // UpdateGuard::finish().  Why?  If this happens, the DNS server will
        // forever report "update in progress" errors, and nothing will ever fix
        // it.  This is essentially a refcount leak, and it's very difficult to
        // debug since we wouldn't know what code path caused the problem.  But
        // we *do* know what code path caused the problem: it's whoever dropped
        // the UpdateGuard without finishing it.  That's why this is the place
        // to identify and report the problem.
        //
        // The first thing we'll do is poison the store so that any subsequent
        // attempt to update it will fail explicitly.
        if !self.finished {
            self.store.poisoned.store(true, Ordering::SeqCst);

            // Now, in the case above where a code path just forgot to finish
            // the UpdateGuard, we want to panic right here.  That makes this
            // problem maximally debuggable: it points precisely to where the
            // missed call was.  But it's also possible that we got here because
            // the current thread is panicking, causing the UpdateGuard to be
            // dropped.  There's no point in panicking again because there's no
            // bug here.  Plus, it's quite disruptive to panic while panicking.
            // So we don't want to panic if we're already panicking.
            //
            // TODO-cleanup Better than all this would be to enforce at
            // compile-time that the UpdateGuard gets finished before it gets
            // dropped.  Maybe better than the above would be to have `drop` of
            // the UpdateGuard do the same thing as `finish()`, similar to
            // `MutexGuard`.  This is tricky because:
            // - we cannot take the async lock from the synchronous Drop
            //   function
            // - it's risky to use a std Mutex since taking the lock could block
            //   the executor; plus, if the lock were poisoned due to a panic,
            //   we'd panic while panicking again
            // - it doesn't seem like we can use `blocking_lock()` because we
            //   _are_ in an async context (i.e., running as part of a task in
            //   an async runtime), even if we're not in an async block
            // - we could use a semaphore like tokio's MutexGuard does, but that
            //   involves unsafe code
            if !std::thread::panicking() {
                panic!("dropped UpdateGuard without finishing update");
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Config, Store, UpdateError};
    use crate::storage::QueryError;
    use anyhow::Context;
    use camino::Utf8PathBuf;
    use camino_tempfile::Utf8TempDir;
    use hickory_proto::rr::LowerName;
    use hickory_resolver::Name;
    use internal_dns_types::config::DnsConfigParams;
    use internal_dns_types::config::DnsConfigZone;
    use internal_dns_types::config::DnsRecord;
    use internal_dns_types::names::ZONE_APEX_NAME;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev::test_setup_log;
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::net::Ipv6Addr;
    use std::str::FromStr;
    use std::sync::Arc;

    /// As usual, `TestContext` groups the various pieces we need in a bunch of
    /// our tests and helps make sure they get cleaned up properly.
    struct TestContext {
        logctx: dropshot::test_util::LogContext,
        tmpdir: Utf8TempDir,
        store: Store,
        db: Arc<sled::Db>,
    }

    impl TestContext {
        fn new(test_name: &str) -> TestContext {
            let logctx = test_setup_log(test_name);
            let tmpdir = Utf8TempDir::with_prefix("dns-server-storage-test")
                .expect("failed to create tmp directory for test");
            let storage_path = tmpdir.path().to_path_buf();

            let db = Arc::new(
                sled::open(&storage_path).context("creating db").unwrap(),
            );
            let store = Store::new_with_db(
                logctx.log.clone(),
                Arc::clone(&db),
                &Config { storage_path, keep_old_generations: 3 },
            )
            .expect("failed to create test Store");
            assert!(store.is_new());
            TestContext { logctx, tmpdir, store, db }
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
        Only(&'a DnsRecord),
        Record(&'a DnsRecord),
    }

    /// Looks up the given name and verifies that the store layer returns the
    /// correct error: that the name is not in a zone that we know about
    fn expect(store: &Store, name: &str, expect: Expect<'_>) {
        let dns_name_orig = Name::from_str(name).expect("bad DNS name");
        let dns_name_lower = LowerName::from(dns_name_orig.clone());
        let result = store.query_raw(&dns_name_lower, &dns_name_orig);

        let records = result.map(|answer| {
            // Regardless of if the answer's records are as we expect, the name
            // and zone in the answer must describe the queried name.
            assert_eq!(name.to_lowercase(), answer.queried_fqdn());

            // And if there are no records, it is represented as `None` here,
            // rather than recording a key with an empty list of records into
            // the database:
            assert!(answer.records != Some(Vec::new()));

            answer.records
        });

        match (expect, records) {
            (Expect::NoZone, Err(QueryError::NoZone(n))) if n == name => (),
            (Expect::NoName, Ok(None)) => {
                // No records, as expected.
            }
            (Expect::Only(r), Ok(Some(records)))
                if records.len() == 1 && records[0] == *r =>
            {
                ()
            }
            (Expect::Record(r), Ok(Some(records))) if records.contains(r) => (),
            (expected, answer) => panic!(
                "did not get what we expected from DNS query, expected {:?} but got {:?}",
                expected, answer
            ),
        }
    }

    /// Returns an ordered list of the generation numbers that have trees in
    /// the underlying Store's database.  This is used to verify the
    /// behavior around pruning trees.
    fn generations_with_trees(store: &Store) -> Vec<Generation> {
        store
            .all_name_trees()
            .map(|(gen, _)| gen)
            .collect::<BTreeSet<Generation>>()
            .into_iter()
            .collect()
    }

    #[tokio::test]
    async fn test_update_basic() {
        let tc = TestContext::new("test_update_basic");

        // Verify the initial configuration.
        assert!(generations_with_trees(&tc.store).is_empty());
        let config = tc.store.dns_config().await.unwrap();
        assert_eq!(config.generation, Generation::from_u32(0));
        assert!(config.zones.is_empty());
        expect(&tc.store, "gen1_name.zone1.internal", Expect::NoZone);
        expect(&tc.store, "Gen1_name.zone1.internal", Expect::NoZone);
        expect(&tc.store, "shared_name.zone1.internal", Expect::NoZone);
        expect(&tc.store, "gen2_name.zone2.internal", Expect::NoZone);
        expect(&tc.store, "gen8_name.zone8.internal", Expect::NoZone);

        // Update to generation 1, which contains one zone with one name.
        let dummy_record = DnsRecord::Aaaa(Ipv6Addr::LOCALHOST);
        let update1 = DnsConfigParams {
            time_created: chrono::Utc::now(),
            generation: Generation::from_u32(1),
            serial: 1,
            zones: vec![DnsConfigZone {
                zone_name: "zone1.internal".to_string(),
                records: HashMap::from([
                    ("gen1_name".to_string(), vec![dummy_record.clone()]),
                    ("shared_name".to_string(), vec![dummy_record.clone()]),
                ]),
            }],
        };

        tc.store.dns_config_update(&update1, "my request id").await.unwrap();
        assert_eq!(
            vec![Generation::from_u32(1)],
            generations_with_trees(&tc.store)
        );
        expect(
            &tc.store,
            "gen1_name.zone1.internal",
            Expect::Only(&dummy_record),
        );
        expect(
            &tc.store,
            "gen1_name.ZONE1.internal",
            Expect::Only(&dummy_record),
        );
        expect(
            &tc.store,
            "Gen1_name.zone1.internal",
            Expect::Only(&dummy_record),
        );
        expect(
            &tc.store,
            "shared_name.zone1.internal",
            Expect::Only(&dummy_record),
        );
        expect(&tc.store, "enoent.zone1.internal", Expect::NoName);
        expect(&tc.store, "gen2_name.zone2.internal", Expect::NoZone);
        expect(&tc.store, "gen8_name.zone8.internal", Expect::NoZone);

        // Update to generation 2, which contains an additional zone and removes
        // one of the names from the existing zone.
        let update2 = DnsConfigParams {
            time_created: chrono::Utc::now(),
            generation: Generation::from_u32(2),
            serial: 2,
            zones: vec![
                DnsConfigZone {
                    zone_name: "zone1.internal".to_string(),
                    records: HashMap::from([(
                        "shared_name".to_string(),
                        vec![dummy_record.clone()],
                    )]),
                },
                DnsConfigZone {
                    zone_name: "zone2.internal".to_string(),
                    records: HashMap::from([(
                        "gen2_name".to_string(),
                        vec![dummy_record.clone()],
                    )]),
                },
            ],
        };
        tc.store.dns_config_update(&update2, "my request id").await.unwrap();
        assert_eq!(
            vec![Generation::from_u32(1), Generation::from_u32(2)],
            generations_with_trees(&tc.store)
        );
        expect(&tc.store, "gen1_name.zone1.internal", Expect::NoName);
        expect(&tc.store, "gen1_name.ZONE1.internal", Expect::NoName);
        expect(&tc.store, "Gen1_name.zone1.internal", Expect::NoName);
        expect(
            &tc.store,
            "shared_name.zone1.internal",
            Expect::Only(&dummy_record),
        );
        expect(
            &tc.store,
            "gen2_name.zone2.internal",
            Expect::Only(&dummy_record),
        );
        expect(&tc.store, "gen8_name.zone8.internal", Expect::NoZone);

        // Do another update, but this time, skip several generation numbers.
        let update8 = DnsConfigParams {
            time_created: chrono::Utc::now(),
            generation: Generation::from_u32(8),
            serial: 8,
            zones: vec![DnsConfigZone {
                zone_name: "zone8.internal".to_string(),
                records: HashMap::from([(
                    "gen8_name".to_string(),
                    vec![dummy_record.clone()],
                )]),
            }],
        };
        tc.store.dns_config_update(&update8, "my request id").await.unwrap();
        assert_eq!(
            vec![
                Generation::from_u32(1),
                Generation::from_u32(2),
                Generation::from_u32(8)
            ],
            generations_with_trees(&tc.store)
        );
        expect(&tc.store, "gen1_name.zone1.internal", Expect::NoZone);
        expect(&tc.store, "shared_name.zone1.internal", Expect::NoZone);
        expect(&tc.store, "gen2_name.zone2.internal", Expect::NoZone);
        expect(
            &tc.store,
            "gen8_name.zone8.internal",
            Expect::Only(&dummy_record),
        );

        // Updating to generation 8 again should be a no-op.  It should succeed
        // and show the same behavior.
        tc.store.dns_config_update(&update8, "my request id").await.unwrap();
        assert_eq!(
            vec![
                Generation::from_u32(1),
                Generation::from_u32(2),
                Generation::from_u32(8)
            ],
            generations_with_trees(&tc.store)
        );
        expect(&tc.store, "gen1_name.zone1.internal", Expect::NoZone);
        expect(&tc.store, "shared_name.zone1.internal", Expect::NoZone);
        expect(&tc.store, "gen2_name.zone2.internal", Expect::NoZone);
        expect(
            &tc.store,
            "gen8_name.zone8.internal",
            Expect::Only(&dummy_record),
        );

        // Failure: try a backwards update.
        println!("attempting invalid update to generation 2");
        let error = tc
            .store
            .dns_config_update(&update2, "my request id")
            .await
            .expect_err("update unexpectedly succeeded");
        println!("found error: {:#}", error);
        println!("{:?}", error);
        match &error {
            UpdateError::BadUpdateGeneration {
                current_generation,
                attempted_generation,
            } if *current_generation == Generation::from_u32(8)
                && *attempted_generation == Generation::from_u32(2) =>
            {
                ()
            }
            e => panic!("unexpected failure to update: {:#}", e),
        };
        assert_eq!(
            error.to_string(),
            "unsupported attempt to update to generation 2 \
                     while at generation 8",
        );

        // Now make one more update and make sure we've pruned the oldest
        // generation's trees.  (This assumes that we've configured the Store to
        // keep three generations' worth of trees, which is what we did above.)
        // We should have kept the last three trees that we saw (which includes
        // generation 2), not the last three integers.
        let update9 = DnsConfigParams {
            time_created: chrono::Utc::now(),
            generation: Generation::from_u32(9),
            serial: 9,
            zones: vec![DnsConfigZone {
                zone_name: "zone8.internal".to_string(),
                records: HashMap::from([(
                    "gen8_name".to_string(),
                    vec![dummy_record.clone()],
                )]),
            }],
        };
        tc.store.dns_config_update(&update9, "my request id").await.unwrap();
        assert_eq!(
            vec![
                Generation::from_u32(2),
                Generation::from_u32(8),
                Generation::from_u32(9)
            ],
            generations_with_trees(&tc.store)
        );

        tc.cleanup_successful();
    }

    #[tokio::test]
    async fn test_update_interrupted() {
        let tc = TestContext::new("test_update_interrupted");

        // Initial configuration.
        assert!(generations_with_trees(&tc.store).is_empty());
        let config = tc.store.dns_config().await.unwrap();
        assert_eq!(config.generation, Generation::from_u32(0));
        assert!(config.zones.is_empty());

        // Make one normal update.
        let dummy_record = DnsRecord::Aaaa(Ipv6Addr::LOCALHOST);
        let update1 = DnsConfigParams {
            time_created: chrono::Utc::now(),
            generation: Generation::from_u32(1),
            serial: 1,
            zones: vec![DnsConfigZone {
                zone_name: "zone1.internal".to_string(),
                records: HashMap::from([(
                    "gen1_name".to_string(),
                    vec![dummy_record.clone()],
                )]),
            }],
        };

        tc.store.dns_config_update(&update1, "my request id").await.unwrap();
        assert_eq!(
            vec![Generation::from_u32(1)],
            generations_with_trees(&tc.store)
        );

        // Now make an update to generation 2.  We're going to do this like
        // normal, examine the state, and then we're going to unwind the very
        // last step.  This should _look_ like an interrupted update.  We'll
        // create a new Store atop that, verify that it reports being on
        // generation 1, and that we can then successfully update to generation
        // 2 again.
        //
        // This isn't a perfect test.  And it's unfortunate that we have to dig
        // into the guts of the database.  But it's not a bad simulation, and
        // it's better to test some of this behavior than none.
        let update2 = DnsConfigParams {
            time_created: chrono::Utc::now(),
            generation: Generation::from_u32(2),
            serial: 2,
            zones: vec![DnsConfigZone {
                zone_name: "zone2.internal".to_string(),
                records: HashMap::from([(
                    "gen2_name".to_string(),
                    vec![dummy_record.clone()],
                )]),
            }],
        };

        let gen1_config = tc.store.read_config().unwrap();
        assert_eq!(Generation::from_u32(1), gen1_config.generation);
        expect(
            &tc.store,
            "gen1_name.zone1.internal",
            Expect::Only(&dummy_record),
        );
        expect(&tc.store, "gen2_name.zone2.internal", Expect::NoZone);

        tc.store.dns_config_update(&update2, "my request id").await.unwrap();
        assert_eq!(
            vec![Generation::from_u32(1), Generation::from_u32(2)],
            generations_with_trees(&tc.store)
        );
        let gen2_config = tc.store.read_config().unwrap();
        assert_eq!(Generation::from_u32(2), gen2_config.generation);
        expect(&tc.store, "gen1_name.zone1.internal", Expect::NoZone);
        expect(
            &tc.store,
            "gen2_name.zone2.internal",
            Expect::Only(&dummy_record),
        );

        // At this point, we want to drop the Store, but we need to keep around
        // the temporary directory.  The easiest thing is to grab the pieces we
        // want out of the TestContext, drop it (without cleaning it up), then
        // assemble a new one out of these pieces and the new Store.
        let (tmpdir, db, logctx) = (tc.tmpdir, tc.db, tc.logctx);
        drop(tc.store);

        // Undo the last step of the update to make this look like an
        // interrupted update.
        db.insert(
            &super::KEY_CONFIG,
            serde_json::to_vec(&gen1_config).unwrap(),
        )
        .unwrap();

        let store = Store::new_with_db(
            logctx.log.clone(),
            Arc::clone(&db),
            &Config {
                storage_path: Utf8PathBuf::from_str("/nonexistent_unused")
                    .unwrap(),
                keep_old_generations: 3,
            },
        )
        .unwrap();

        let config = store.read_config().unwrap();
        assert_eq!(gen1_config, config);
        // We ought to have pruned the tree associated with generation 2.
        assert_eq!(
            vec![Generation::from_u32(1)],
            generations_with_trees(&store)
        );
        // The rest of the behavior ought to be like generation 1.
        expect(&store, "gen1_name.zone1.internal", Expect::Only(&dummy_record));
        expect(&store, "gen2_name.zone2.internal", Expect::NoZone);

        // Now we can do another update to generation 2.
        store.dns_config_update(&update2, "my request id").await.unwrap();
        assert_eq!(
            vec![Generation::from_u32(1), Generation::from_u32(2)],
            generations_with_trees(&store)
        );
        let gen2_config = store.read_config().unwrap();
        assert_eq!(Generation::from_u32(2), gen2_config.generation);
        expect(&store, "gen1_name.zone1.internal", Expect::NoZone);
        expect(&store, "gen2_name.zone2.internal", Expect::Only(&dummy_record));

        let tc = TestContext { logctx, tmpdir, store, db };
        tc.cleanup_successful();
    }

    #[tokio::test]
    async fn test_update_in_progress() {
        let tc = TestContext::new("test_update_in_progress");

        // Begin an update.
        let before = chrono::Utc::now();
        let update1 = tc
            .store
            .begin_update("my req id", Generation::from_u32(3))
            .await
            .unwrap();
        let after = chrono::Utc::now();

        // Concurrently attempt another update.
        let dummy_record = DnsRecord::Aaaa(Ipv6Addr::LOCALHOST);
        let update2 = DnsConfigParams {
            time_created: chrono::Utc::now(),
            generation: Generation::from_u32(1),
            serial: 1,
            zones: vec![DnsConfigZone {
                zone_name: "zone1.internal".to_string(),
                records: HashMap::from([(
                    "gen1_name".to_string(),
                    vec![dummy_record.clone()],
                )]),
            }],
        };

        let result =
            tc.store.dns_config_update(&update2, "my request id").await;

        // "Finish" the first update now.  This just marks that we're no longer
        // updating.  The database will not actually be updated to the
        // generation in question
        //
        // We do this before checking the error we just got back to avoid a
        // double-panic if we catch a problem here.
        update1.finish().await;

        let error = result
            .expect_err("unexpected success from concurrent update attempt");
        println!("found error: {:#}", error);
        match &error {
            UpdateError::UpdateInProgress {
                start_time,
                elapsed: _,
                generation,
                req_id,
            } if *start_time >= before
                && *start_time <= after
                && *generation == Generation::from_u32(3)
                && *req_id == "my req id" =>
            {
                ()
            }
            e => panic!(
                "unexpected error from concurrent update attempt: {:#}\n{:?}",
                e, e
            ),
        };

        // Now we should be able to apply that update.
        tc.store
            .dns_config_update(&update2, "my request id")
            .await
            .expect("unexpected failure");

        tc.cleanup_successful();
    }

    #[tokio::test]
    async fn test_zone_gets_soa_record() {
        let tc = TestContext::new("test_zone_gets_soa_record");

        let ns1_a = DnsRecord::Aaaa(Ipv6Addr::LOCALHOST);
        let ns1_ns = DnsRecord::Ns("ns1.zone1.internal".to_string());
        let update = DnsConfigParams {
            time_created: chrono::Utc::now(),
            generation: Generation::from_u32(1),
            serial: 1,
            zones: vec![DnsConfigZone {
                zone_name: "zone1.internal".to_string(),
                records: HashMap::from([
                    ("ns1".to_string(), vec![ns1_a.clone()]),
                    (ZONE_APEX_NAME.to_string(), vec![ns1_ns.clone()]),
                ]),
            }],
        };

        tc.store
            .dns_config_update(&update, "my request id")
            .await
            .expect("can apply update");

        // These two records are ones we provided, they ought to be there.
        expect(&tc.store, "ns1.zone1.internal", Expect::Only(&ns1_a));

        expect(
            &tc.store,
            "zone1.internal",
            Expect::Record(&DnsRecord::Ns("ns1.zone1.internal".to_string())),
        );

        // We can update DNS to a configuration without NS records and the
        // server will survive the encounter.  We won't have an SOA record
        // without a nameserver to indicate as the primary source for this zone,
        // though.

        let update2 = DnsConfigParams {
            time_created: chrono::Utc::now(),
            generation: Generation::from_u32(2),
            serial: 2,
            zones: vec![DnsConfigZone {
                zone_name: "zone1.internal".to_string(),
                records: HashMap::from([(
                    "ns1".to_string(),
                    vec![ns1_a.clone()],
                )]),
            }],
        };

        tc.store
            .dns_config_update(&update2, "my request id")
            .await
            .expect("can apply update");

        // At this point we have a zone `zone1.internal`, but no records on the
        // zone itself.
        expect(&tc.store, "zone1.internal", Expect::NoName);

        let ns2_a = DnsRecord::Aaaa(Ipv6Addr::LOCALHOST);
        let ns2_ns = DnsRecord::Ns("ns2.zone1.internal".to_string());

        // Finally, even if the NS records are ordered in a strange way, we'll
        // consistently reorder records in the update so that the
        // lowest-numbered NS record is first and used as the SOA mname.
        let update3 = DnsConfigParams {
            time_created: chrono::Utc::now(),
            generation: Generation::from_u32(3),
            serial: 3,
            zones: vec![DnsConfigZone {
                zone_name: "zone1.internal".to_string(),
                records: HashMap::from([
                    ("ns2".to_string(), vec![ns2_a.clone()]),
                    ("ns1".to_string(), vec![ns1_a.clone()]),
                    (
                        ZONE_APEX_NAME.to_string(),
                        vec![ns1_ns.clone(), ns2_ns.clone()],
                    ),
                ]),
            }],
        };

        tc.store
            .dns_config_update(&update3, "my request id")
            .await
            .expect("can apply update");

        // Both NS records *are* present at the zone apex.
        expect(&tc.store, "zone1.internal", Expect::Record(&ns1_ns));

        expect(&tc.store, "zone1.internal", Expect::Record(&ns2_ns));

        tc.cleanup_successful();
    }
}
