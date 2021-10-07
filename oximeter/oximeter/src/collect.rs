//! Types for collecting metric data within client code.

// Copyright 2021 Oxide Computer Company

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use crate::types;
use crate::{Error, Producer};

type ProducerList = Vec<Box<dyn Producer>>;
pub type ProducerResults = Vec<Result<BTreeSet<types::Sample>, Error>>;

/// A central collection point for metrics within an application.
#[derive(Debug, Clone)]
pub struct Collector {
    producers: Arc<Mutex<ProducerList>>,
    producer_id: Uuid,
}

impl Default for Collector {
    fn default() -> Self {
        Self::new()
    }
}

impl Collector {
    /// Construct a new `Collector`.
    pub fn new() -> Self {
        Self::with_id(Uuid::new_v4())
    }

    /// Construct a new `Collector` with the given producer ID.
    pub fn with_id(producer_id: Uuid) -> Self {
        Self { producers: Arc::new(Mutex::new(vec![])), producer_id }
    }

    /// Register a new [`Producer`] object with the collector.
    pub fn register_producer<P>(&self, producer: P) -> Result<(), Error>
    where
        P: Producer,
    {
        self.producers.lock().unwrap().push(Box::new(producer));
        Ok(())
    }

    /// Collect available samples from all registered producers.
    ///
    /// This method returns a vector of results, one from each producer. If the producer generates
    /// an error, that's propagated here. Successfully produced samples are returned in a set,
    /// ordered by the [`types::Sample::cmp`] method.
    pub fn collect(&self) -> ProducerResults {
        let mut producers = self.producers.lock().unwrap();
        let mut results = Vec::with_capacity(producers.len());
        for producer in producers.iter_mut() {
            results.push(producer.produce().map(|samples| samples.collect()));
        }
        results
    }

    /// Return the producer ID associated with this collector.
    pub fn producer_id(&self) -> Uuid {
        self.producer_id
    }
}
