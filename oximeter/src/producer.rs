use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc, RwLock,
};

use crate::distribution;
use crate::{Error, Measurement};

pub trait Producer {
    fn setup_collection(&mut self) -> Result<(), Error>;
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
