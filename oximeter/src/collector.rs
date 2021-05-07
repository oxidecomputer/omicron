use std::boxed::Box;
use std::collections::{
    hash_map::{DefaultHasher, Entry},
    HashMap,
};
use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};

use crate::{producer, Counter, Error, Metric, Producer, Sample, Target};

#[derive(Clone, Copy, Debug, Hash, PartialOrd, Ord, PartialEq, Eq)]
pub struct CollectionToken(u64);

/// The `Collector` provides a way to register and export metrics from an application
pub struct Collector {
    // server in here somewhere
    collections: HashMap<CollectionToken, Collection>,
    n_items: usize,
}

impl Collector {
    pub fn new() -> Self {
        Self {
            collections: HashMap::new(),
            n_items: 0,
        }
    }

    pub fn register<T, M, P>(
        &mut self,
        target: &T,
        metric: &M,
        producer: &P,
    ) -> Result<CollectionToken, Error>
    where
        T: Target + Clone + 'static,
        M: Metric + Clone + 'static,
        P: Producer + Clone + 'static,
    {
        let mut hasher = DefaultHasher::new();
        target.key().hash(&mut hasher);
        metric.key().hash(&mut hasher);
        let tok = CollectionToken(hasher.finish());
        self.n_items += 1;
        match self.collections.entry(tok) {
            Entry::Occupied(_) => Err(Error::CollectionAlreadyRegistered),
            Entry::Vacant(entry) => {
                entry.insert(Collection {
                    targets: vec![Box::new(target.clone())],
                    metrics: vec![Box::new(metric.clone())],
                    producer: Box::new(producer.clone()),
                    created: Utc::now(),
                });
                Ok(tok)
            }
        }
    }

    /// A convenience function to register and return an atomic counter.
    pub fn register_counter<T, M>(
        &mut self,
        target: &T,
        metric: &M,
    ) -> Result<(CollectionToken, Counter), Error>
    where
        T: Target + Clone + 'static,
        M: Metric + Clone + 'static,
    {
        let counter = Counter::new();
        let token = self.register(target, metric, &counter)?;
        Ok((token, counter))
    }

    /// A convenience function to register and return a distribution over 64-bit integers.
    pub fn register_distribution_i64<T, M>(
        &mut self,
        target: &T,
        metric: &M,
        bins: &[i64],
    ) -> Result<(CollectionToken, producer::Distribution<i64>), Error>
    where
        T: Target + Clone + 'static,
        M: Metric + Clone + 'static,
    {
        let distribution = producer::Distribution::new(bins)?;
        let token = self.register(target, metric, &distribution)?;
        Ok((token, distribution))
    }

    /// A convenience function to register and return a distribution over 64-bit floats.
    pub fn register_distribution_f64<T, M>(
        &mut self,
        target: &T,
        metric: &M,
        bins: &[f64],
    ) -> Result<(CollectionToken, producer::Distribution<f64>), Error>
    where
        T: Target + Clone + 'static,
        M: Metric + Clone + 'static,
    {
        let distribution = producer::Distribution::new(bins)?;
        let token = self.register(target, metric, &distribution)?;
        Ok((token, distribution))
    }

    pub fn register_collection<T, M, P>(
        &mut self,
        targets: &[T],
        metrics: &[M],
        producer: &P,
    ) -> Result<CollectionToken, Error>
    where
        T: Target + Clone + 'static,
        M: Metric + Clone + 'static,
        P: Producer + Clone + 'static,
    {
        if targets.is_empty() || targets.len() != metrics.len() {
            return Err(Error::InvalidCollection(String::from(
                "Targets and metrics must have the same size and be nonempty",
            )));
        }
        let mut targets_ = Vec::<Box<dyn Target>>::with_capacity(targets.len());
        let mut metrics_ = Vec::<Box<dyn Metric>>::with_capacity(metrics.len());
        let mut hasher = DefaultHasher::new();
        for (target, metric) in targets.iter().zip(metrics.iter()) {
            target.key().hash(&mut hasher);
            metric.key().hash(&mut hasher);
            targets_.push(Box::new(target.clone()));
            metrics_.push(Box::new(metric.clone()));
        }
        let tok = CollectionToken(hasher.finish());
        self.n_items += targets_.len();
        match self.collections.entry(tok) {
            Entry::Occupied(_) => Err(Error::CollectionAlreadyRegistered),
            Entry::Vacant(entry) => {
                entry.insert(Collection {
                    targets: targets_,
                    metrics: metrics_,
                    producer: Box::new(producer.clone()),
                    created: Utc::now(),
                });
                Ok(tok)
            }
        }
    }

    pub fn deregister(&mut self, token: CollectionToken) -> Result<(), Error> {
        self.n_items -= self
            .collections
            .remove(&token)
            .ok_or_else(|| Error::CollectionNotRegistered)?
            .len();
        Ok(())
    }

    pub fn collect(&mut self) -> Result<Vec<Sample>, Error> {
        let mut samples = Vec::new();
        for collection in self.collections.values_mut() {
            let producer = &mut collection.producer;
            producer.setup_collection()?;
            let measurements = producer.collect()?;
            for i in 0..measurements.len() {
                let sample = Sample::new(
                    &collection.targets[i],
                    &collection.metrics[i],
                    &measurements[i],
                );
                samples.push(sample);
            }
        }
        Ok(samples)
    }

    pub fn n_collections(&self) -> usize {
        self.collections.len()
    }

    pub fn n_items(&self) -> usize {
        self.n_items
    }

    pub fn collection_info(&self, token: CollectionToken) -> Result<(usize, DateTime<Utc>), Error> {
        self.collections
            .get(&token)
            .ok_or_else(|| Error::CollectionNotRegistered)
            .map(|collection| (collection.len(), collection.created))
    }
}

pub(crate) struct Collection {
    pub(crate) targets: Vec<Box<dyn Target>>,
    pub(crate) metrics: Vec<Box<dyn Metric>>,
    pub(crate) producer: Box<dyn Producer>,
    pub(crate) created: DateTime<Utc>,
}

impl Collection {
    pub(crate) fn len(&self) -> usize {
        self.targets.len()
    }
}
