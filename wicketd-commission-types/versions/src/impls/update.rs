// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeSet;

use gateway_types_versions::v1::component::SpIdentifier;

use crate::latest::update::UpdateTargets;

impl UpdateTargets {
    /// Create a new `UpdateTargets` set containing a single target.
    pub fn single(target: SpIdentifier) -> Self {
        Self(std::iter::once(target).collect())
    }

    /// Return an iterator over the targets in this set.
    pub fn iter(&self) -> std::collections::btree_set::Iter<'_, SpIdentifier> {
        self.0.iter()
    }

    /// Return true if the set contains the given target.
    pub fn contains(&self, target: &SpIdentifier) -> bool {
        self.0.contains(target)
    }

    /// Convert this set into the inner `BTreeSet<SpIdentifier>`, losing the
    /// non-empty guarantee.
    pub fn into_inner(self) -> BTreeSet<SpIdentifier> {
        self.0
    }
}

impl IntoIterator for UpdateTargets {
    type Item = SpIdentifier;
    type IntoIter = std::collections::btree_set::IntoIter<SpIdentifier>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a UpdateTargets {
    type Item = &'a SpIdentifier;
    type IntoIter = std::collections::btree_set::Iter<'a, SpIdentifier>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::latest::update::EmptyUpdateTargets;
    use gateway_types_versions::v1::component::SpType;
    use proptest::prelude::*;
    use test_strategy::proptest;

    fn target(slot: u16) -> SpIdentifier {
        SpIdentifier { typ: SpType::Sled, slot }
    }

    #[test]
    fn new_rejects_empty() {
        assert_eq!(
            UpdateTargets::new(BTreeSet::new()),
            Err(EmptyUpdateTargets)
        );
    }

    #[test]
    fn new_accepts_single() {
        let set: BTreeSet<_> = std::iter::once(target(0)).collect();
        let targets =
            UpdateTargets::new(set.clone()).expect("a singleton is non-empty");
        assert_eq!(targets.into_inner(), set);
    }

    #[test]
    fn deserialize_rejects_empty() {
        let err = serde_json::from_str::<UpdateTargets>("[]")
            .expect_err("an empty array is not a valid UpdateTargets");
        assert!(
            err.to_string().contains("at least one target"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn deserialize_accepts_single() {
        let targets: UpdateTargets =
            serde_json::from_value(serde_json::json!([target(1)]))
                .expect("a singleton array deserializes");
        let expected: BTreeSet<_> = std::iter::once(target(1)).collect();
        assert_eq!(targets.into_inner(), expected);
    }

    #[proptest]
    fn serde_roundtrip(
        #[strategy(prop::collection::btree_set(any::<SpIdentifier>(), 1..=8))]
        set: BTreeSet<SpIdentifier>,
    ) {
        let targets =
            UpdateTargets::new(set).expect("generated set is non-empty");
        let json = serde_json::to_value(&targets).expect("serializes");
        let round_tripped: UpdateTargets =
            serde_json::from_value(json).expect("deserializes");
        prop_assert_eq!(round_tripped, targets);
    }

    // Test that duplicate items are accepted, even though the schema specifies
    // uniqueItems.
    //
    // This is because we delegate to serde's `BTreeSet` deserializer which
    // accepts duplicates. There's an argument to be made that our deserializer
    // should not accept duplicates, but it doesn't matter too much and is
    // easier this way.
    #[proptest]
    fn deserialize_dedups_duplicates(
        #[strategy(prop::collection::vec(any::<SpIdentifier>(), 1..=8))]
        entries: Vec<SpIdentifier>,
    ) {
        let mut doubled = entries.clone();
        doubled.extend(entries.iter().copied());
        let json = serde_json::to_value(&doubled).expect("serializes");
        let targets: UpdateTargets =
            serde_json::from_value(json).expect("deserializes");
        let expected: BTreeSet<_> = entries.into_iter().collect();
        prop_assert_eq!(targets.into_inner(), expected);
    }
}
