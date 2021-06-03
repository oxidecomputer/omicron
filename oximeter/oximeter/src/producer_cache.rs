//! A local, durable cache of producers assigned to a collector.
// Copyright 2021 Oxide Computer Company

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use omicron_common::model::{ProducerEndpoint, ProducerId};
use rusqlite::{params, Connection};
use uuid::Uuid;

use crate::Error;

/// The `ProducerCache` is a durable set of producers assigned to a collector.
///
/// As metric producers register, they are assigned to a collector, an `Oximeter` server instance
/// that collects data from it periodically. This assignment is persisted via the `ProducerCache`,
/// which allows the assignment of producers to a collector to survive restart of the collector
/// itself.
#[derive(Debug, Clone)]
pub struct ProducerCache {
    path: PathBuf,
    conn: Arc<Mutex<Connection>>,
}

impl ProducerCache {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        let conn = Connection::open(&path).map_err(map_sqlite_err)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS producers (
                id TEXT PRIMARY KEY, -- Producer ID as UUID text
                address TEXT NOT NULL, -- IP address of the producer as text
                collection_route TEXT NOT NULL,
                interval REAL NOT NULL -- Collection interval in seconds
            );",
            [],
        )
        .map_err(map_sqlite_err)?;
        Ok(Self { path, conn: Arc::new(Mutex::new(conn)) })
    }

    /// Return the path of the durable storage for this cache.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Insert a record for a producer.
    ///
    /// Note that this is not idempotent, and fails if the producer already exists in the cache.
    pub fn insert(&self, producer: &ProducerEndpoint) -> Result<(), Error> {
        let id = producer.producer_id().producer_id.to_string();
        let address = producer.address().to_string();
        let interval = producer.interval().as_secs_f64();

        // Route looks like `/some/path/{producer_id}`. Remove the `/{producer_id}` portion
        // in the database, as it's combined when constructing again via `ProducerEndpoint::with_id`.
        let route = producer.collection_route();
        let len_to_remove = id.len() + 1; // +1 for `/`
        let split_point = route.len() - len_to_remove;
        let route = route.split_at(split_point).0.to_string();

        self.conn
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO producers VALUES (?, ?, ?, ?)",
                params![id, address, route, interval],
            )
            .map_err(map_sqlite_err)?;
        Ok(())
    }

    /// Idempotently remove a record from a producer
    #[allow(dead_code)]
    pub fn remove(&self, producer_id: &ProducerId) -> Result<(), Error> {
        let id = producer_id.producer_id.to_string();
        let n_updates = self
            .conn
            .lock()
            .unwrap()
            .execute("DELETE FROM producers WHERE id = ?", [id])
            .map_err(map_sqlite_err)?;
        assert!(n_updates == 0 || n_updates == 1);
        Ok(())
    }

    /// Return the list of producers in the cache
    pub fn producers(
        &self,
    ) -> Result<BTreeMap<ProducerId, ProducerEndpoint>, Error> {
        get_producers(&self.conn.lock().unwrap())
    }
}

fn map_sqlite_err(e: rusqlite::Error) -> Error {
    Error::OximeterServer(format!("Producer cache error: {}", e))
}

// Read the list of producers from the database file.
//
// This is done on startup to initially populate the in-memory cache.
fn get_producers(
    conn: &Connection,
) -> Result<BTreeMap<ProducerId, ProducerEndpoint>, Error> {
    let mut stmt = conn
        .prepare(
            "SELECT id, address, collection_route, interval FROM producers",
        )
        .map_err(map_sqlite_err)?;
    let rows = stmt
        .query_map([], |row| {
            let id: String = row.get(0)?;
            let producer_id = ProducerId::from(Uuid::from_str(&id).unwrap());
            let address: String = row.get(1)?;
            let address = SocketAddr::from_str(&address).unwrap();
            let collection_route: String = row.get(2)?;
            let interval = Duration::from_secs_f64(row.get(3)?);
            Ok(ProducerEndpoint::with_id(
                producer_id,
                address,
                &collection_route,
                interval,
            ))
        })
        .map_err(map_sqlite_err)?;
    let mut producers = BTreeMap::new();
    for row in rows {
        let row = row.map_err(map_sqlite_err)?;
        producers.insert(row.producer_id(), row);
    }
    Ok(producers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::Builder;

    #[test]
    fn test_producer_cache() {
        let file = Builder::new().suffix(".db").tempfile().unwrap();
        let cache = ProducerCache::new(file.path()).unwrap();
        assert_eq!(
            cache.producers().unwrap().len(),
            0,
            "Expected zero producers when building cache from new file"
        );
        let producer = ProducerEndpoint::new(
            "[::1]:10001",
            "/some/route",
            Duration::from_secs(2),
        );
        cache.insert(&producer).unwrap();
        assert_eq!(
            cache.producers().unwrap().len(),
            1,
            "There should be one producer cached"
        );

        let from_file = get_producers(&cache.conn.lock().unwrap()).unwrap();
        assert_eq!(
            from_file,
            cache.producers().unwrap(),
            "Reading directly from the database should return the same set of producers"
        );

        cache.remove(&producer.producer_id()).unwrap();
        assert_eq!(
            cache.producers().unwrap().len(),
            0,
            "There should be zero producers"
        );
        assert_eq!(
            get_producers(&cache.conn.lock().unwrap()).unwrap().len(),
            0,
            "Reading directly from the database should return the same set of producers"
        );
    }

    #[test]
    fn test_producer_cache_duplicates() {
        let file = Builder::new().suffix(".db").tempfile().unwrap();
        let cache = ProducerCache::new(file.path()).unwrap();
        let producer = ProducerEndpoint::new(
            "[::1]:10001",
            "/some/route",
            Duration::from_secs(2),
        );
        cache.insert(&producer).unwrap();
        assert!(
            cache.insert(&producer).is_err(),
            "Inserting duplicate producers should fail"
        );
    }
}
