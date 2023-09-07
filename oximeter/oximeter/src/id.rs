// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code for generating stable hashes, used for IDs and versions.

// Copyright 2023 Oxide Computer Company

use twox_hash::XxHash64;
use std::hash::Hash as _;
use std::hash::Hasher as _;
use crate::Metric;
use crate::Target;
use serde::Serialize;
use serde::Deserialize;

/// Hasher used for creating stable IDs and versions based on hashes.
#[derive(Debug)]
pub struct Hasher(XxHash64);

impl std::hash::Hasher for Hasher {
    fn finish(&self) -> u64 {
        self.0.finish()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes)
    }
}

impl Hasher {
    pub const SEED: u64 = 0x0;

    /// Create a new hasher for generating stable hashes.
    pub fn new() -> Self {
        Self(XxHash64::with_seed(Self::SEED))
    }
}

/// An identifier for the schema for a target, metric, or timeseries.
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SchemaId(u64);

// Helper enum, to ensure that two objects of different _types_, but otherwise
// the same, hash to different values.
#[derive(Copy, Clone, Debug, Hash)]
enum ObjectKind {
    Target,
    Metric,
}

impl SchemaId {
    /// Build a schema ID for a target.
    pub fn from_target(target: &impl Target) -> Self {
        let mut hasher = Hasher::new();
        ObjectKind::Target.hash(&mut hasher);
        target.name().hash(&mut hasher);
        for (name, typ) in target.field_names().iter().zip(target.field_types()) {
            name.hash(&mut hasher);
            typ.hash(&mut hasher);
        }
        Self(hasher.finish())
    }

    /// Build a schema ID for a metric.
    pub fn from_metric(metric: &impl Metric) -> Self {
        let mut hasher = Hasher::new();
        ObjectKind::Metric.hash(&mut hasher);
        metric.name().hash(&mut hasher);
        for (name, typ) in metric.field_names().iter().zip(metric.field_types()) {
            name.hash(&mut hasher);
            typ.hash(&mut hasher);
        }
        metric.datum_type().hash(&mut hasher);
        Self(hasher.finish())
    }
}

/// An identifier for the individual instance of a target, metric or timeseries.
///
/// While the `SchemaId` is the same for any two instances of the same type
/// which impls `Target`, the `Id` is the same only if all the fields are also
/// the same.
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Id(u64);

impl Id {
    /// Build an ID for a target.
    pub fn from_target(target: &impl Target) -> Self {
        let mut hasher = Hasher::new();
        ObjectKind::Target.hash(&mut hasher);
        target.name().hash(&mut hasher);
        for field in target.fields().into_iter() {
            field.name.hash(&mut hasher);
            field.value.field_type().hash(&mut hasher);
            field.value.hash(&mut hasher);
        }
        Self(hasher.finish())
    }

    /// Build an ID for a metric.
    pub fn from_metric(metric: &impl Metric) -> Self {
        let mut hasher = Hasher::new();
        ObjectKind::Metric.hash(&mut hasher);
        metric.name().hash(&mut hasher);
        for field in metric.fields().into_iter() {
            field.name.hash(&mut hasher);
            field.value.field_type().hash(&mut hasher);
            field.value.hash(&mut hasher);
        }
        metric.datum_type().hash(&mut hasher);
        Self(hasher.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oximeter_ids() {
        #[derive(Target)]
        struct Targ {
            name: String,
            id: uuid::Uuid,
            v: i64,
        }
        let t = Targ { name: "foo".into(), id: uuid::Uuid::new_v4(), v: 1 };
        let t2 = Targ { name: "foo".into(), id: uuid::Uuid::new_v4(), v: 1 };
        assert_eq!(
            SchemaId::from_target(&t),
            SchemaId::from_target(&t2),
            "Expected two instances of the same type to have the same schema ID",
        );
        assert!(
            Id::from_target(&t) != Id::from_target(&t2),
            "Expected two instances of the same type to have different IDs",
        );
    }

    mod target {
        use oximeter::Target;
        #[derive(Target)]
        pub struct Targ {
            pub datum: i64,
        }
    }
    mod metric {
        use oximeter::Metric;
        #[derive(Metric)]
        pub struct Targ {
            pub datum: i64,
        }
    }
    #[test]
    fn test_oximeter_ids_with_same_fields() {
        // Sanity check that a target and metric that happen to have all the
        // exact same fields, still hash differently.
        let target = target::Targ { datum: 1 };
        let metric = metric::Targ { datum: 1 };
        assert!(
            SchemaId::from_target(&target) != SchemaId::from_metric(&metric),
            "Target and metric with the same schema should hash to \
            different values"
        );
        assert!(
            Id::from_target(&target) != Id::from_metric(&metric),
            "Target and metric with the same schema should hash to \
            different values"
        );
    }
}
