//! Tools for producing metrics that may be collected by `oximeter`.
// Copyright 2021 Oxide Computer Company

use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc, RwLock,
};

use crate::distribution;
use crate::{Error, Measurement};

/// A trait used to generate measurements for one or metrics on demand.
///
/// The `Producer` trait provides a callback-like mechanism for client code to control how metrics
/// are generated when a [`Collector`](crate::Collector) attempts to scrape them. Metrics are
/// registered as a "collection", which is one or more target and metric pairs. When the
/// [`Collector::collect`](crate::Collector::collect) method is called, the
/// [`Producer::setup_collection`] method is called _once_ for the entire collection. Then the
/// [`Producer::collect`] method is called, which is expected to produce the measurements for each
/// metric in the collection (in order).
///
/// Users may implement this trait on custom structs, or use some of the convenience types such as
/// the [`Counter`]. This affords some flexibility, allowing the user to decide how to actually
/// generate the metric data, which may be at a different time than the `Collector` requests it.
///
/// For example, if a collection requires an expensive system or network call, but a single such
/// call returns a measurement for each metric in the collection, performing that operation for
/// each measurement is inefficient (and possibly incorrect). Instead, an implementor of this trait
/// can perform that expensive operation in the `setup_collection` method, and then report all
/// generated measurements as a batch via the `collect` method.
pub trait Producer {
    /// Perform any initialization required for a single metric collection
    fn setup_collection(&mut self) -> Result<(), Error>;

    /// Return all metrics for a collection, in order.
    ///
    /// Note that this method currently expects that the metrics themselves are returned in the
    /// order in which they are registered with the `Collector`. This means clients must maintain
    /// that order themselves.
    ///
    /// This method also verifies that the type of each measurement matches the expected type of
    /// the metric it corresponds to. An [`Error`] is returned if the types don't match.
    fn collect(&mut self) -> Result<Vec<Measurement>, Error>;
}

/// A `Producer` that lets users keep track of a simple counter.
///
/// The `collect()` method returns the current value of the internal counter. Users can bump this
/// counter whenever they want using the `increment()` method. All accesses are atomic, and this
/// can be cheaply cloned, while referring to the same underlying counter.
#[derive(Clone)]
pub struct Counter {
    value: Arc<AtomicI64>,
}

impl Counter {
    /// Construct a new counter
    pub fn new() -> Self {
        Self {
            value: Default::default(),
        }
    }

    /// Increment the counter by 1
    pub fn increment(&self) {
        self.add(1);
    }

    /// Atomically add the given value to the counter.
    pub fn add(&self, value: i64) {
        self.value.fetch_add(value, Ordering::AcqRel);
    }

    /// Return the current value of the counter..
    pub fn value(&self) -> i64 {
        self.value.load(Ordering::Acquire)
    }
}

impl Producer for Counter {
    fn setup_collection(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn collect(&mut self) -> Result<Vec<Measurement>, Error> {
        Ok(vec![self.value().into()])
    }
}

/// A `Producer` which maintains a distribution for the user.
///
/// Users construct this type with a set of bins, and may then add samples into it with the
/// `sample()` method. The current value of the distribution is returned when the `collect()`
/// method is called.
///
/// Note that this type is thread-safe, and cheaply cloned, but acquires a lock internally to do
/// so.
#[derive(Debug, Clone)]
pub struct Distribution<T> {
    dist: Arc<RwLock<distribution::Distribution<T>>>,
}

impl<T> Distribution<T>
where
    T: distribution::DistributionSupport,
{
    pub fn new(bins: &[T]) -> Result<Self, distribution::DistributionError> {
        Ok(Self {
            dist: Arc::new(RwLock::new(distribution::Distribution::new(bins)?)),
        })
    }

    /// Add a sample into the distribution
    pub fn sample(&mut self, value: T) -> Result<(), distribution::DistributionError> {
        self.dist.write().unwrap().sample(value)
    }
}

impl Producer for Distribution<i64> {
    fn setup_collection(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn collect(&mut self) -> Result<Vec<Measurement>, Error> {
        Ok(vec![self.dist.read().unwrap().clone().into()])
    }
}

impl Producer for Distribution<f64> {
    fn setup_collection(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn collect(&mut self) -> Result<Vec<Measurement>, Error> {
        Ok(vec![self.dist.read().unwrap().clone().into()])
    }
}

#[cfg(test)]
mod tests {
    use super::Counter;

    #[test]
    fn test_counter() {
        let counter = Counter::new();
        assert_eq!(counter.value(), 0, "Counter should start at 0");
        counter.add(4);
        assert_eq!(counter.value(), 4, "Counter incrementing failed");
        let c2 = counter.clone();
        c2.increment();
        assert_eq!(
            counter.value(),
            5,
            "Incrementing a clone of the counter should increment the same data"
        );
    }
}
