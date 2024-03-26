// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::types::DnsConfigParams;
use crate::types::DnsRecord;
use crate::DnsRecords;
use anyhow::ensure;
use anyhow::Context;

/// Compare the DNS records contained in two sets of DNS configuration
#[derive(Debug)]
pub struct DnsDiff<'a> {
    left: &'a DnsRecords,
    right: &'a DnsRecords,
}

impl<'a> DnsDiff<'a> {
    /// Compare the DNS records contained in two sets of DNS configuration
    ///
    /// Both configurations are expected to contain exactly one zone and they
    /// should have the same name.
    pub fn new(
        left: &'a DnsConfigParams,
        right: &'a DnsConfigParams,
    ) -> Result<DnsDiff<'a>, anyhow::Error> {
        let left_zone = left.sole_zone().context("left side of diff")?;
        let right_zone = right.sole_zone().context("right side of diff")?;

        ensure!(
            left_zone.zone_name == right_zone.zone_name,
            "cannot compare DNS configuration from zones with different names: \
            {:?} vs. {:?}", left_zone.zone_name, right_zone.zone_name,
        );

        Ok(DnsDiff { left: &left_zone.records, right: &right_zone.records })
    }

    /// Iterate over the names that are present in the `right` config but
    /// absent in the `left` one (i.e., added between `left` and `right`)
    pub fn names_added(&self) -> impl Iterator<Item = (&str, &[DnsRecord])> {
        self.right
            .iter()
            .filter(|(k, _)| !self.left.contains_key(*k))
            .map(|(k, v)| (k.as_ref(), v.as_ref()))
    }

    /// Iterate over the names that are present in the `left` config but
    /// absent in the `right` one (i.e., removed between `left` and `right`)
    pub fn names_removed(&self) -> impl Iterator<Item = (&str, &[DnsRecord])> {
        self.left
            .iter()
            .filter(|(k, _)| !self.right.contains_key(*k))
            .map(|(k, v)| (k.as_ref(), v.as_ref()))
    }

    /// Iterate over the names whose records changed between `left` and `right`.
    pub fn names_changed(
        &self,
    ) -> impl Iterator<Item = (&str, &[DnsRecord], &[DnsRecord])> {
        self.left.iter().filter_map(|(k, v1)| match self.right.get(k) {
            Some(v2) => {
                let mut v1_sorted = v1.clone();
                let mut v2_sorted = v2.clone();
                v1_sorted.sort();
                v2_sorted.sort();
                (v1_sorted != v2_sorted)
                    .then(|| (k.as_ref(), v1.as_ref(), v2.as_ref()))
            }
            _ => None,
        })
    }

    /// Returns true iff there are no differences in the DNS names and records
    /// described by the given configurations
    pub fn is_empty(&self) -> bool {
        self.names_added().next().is_none()
            && self.names_removed().next().is_none()
            && self.names_changed().next().is_none()
    }
}

#[cfg(test)]
mod test {
    use super::DnsDiff;
    use crate::types::DnsConfigParams;
    use crate::types::DnsConfigZone;
    use crate::types::DnsRecord;
    use chrono::Utc;
    use std::collections::HashMap;
    use std::net::Ipv4Addr;

    const ZONE_NAME: &str = "dummy";

    fn example() -> DnsConfigParams {
        DnsConfigParams {
            generation: 4,
            time_created: Utc::now(),
            zones: vec![DnsConfigZone {
                zone_name: ZONE_NAME.to_string(),
                records: HashMap::from([
                    (
                        "ex1".to_string(),
                        vec![DnsRecord::A(Ipv4Addr::LOCALHOST)],
                    ),
                    (
                        "ex2".to_string(),
                        vec![DnsRecord::A("192.168.1.3".parse().unwrap())],
                    ),
                ]),
            }],
        }
    }

    #[test]
    fn diff_invalid() {
        let example_empty = DnsConfigParams {
            generation: 3,
            time_created: Utc::now(),
            zones: vec![],
        };

        // Configs must have at least one zone.
        let error = DnsDiff::new(&example_empty, &example_empty)
            .expect_err("unexpectedly succeeded comparing two empty configs");
        assert!(
            format!("{:#}", error).contains("expected exactly one DNS zone")
        );

        let example = example();
        let error = DnsDiff::new(&example_empty, &example)
            .expect_err("unexpectedly succeeded comparing an empty config");
        assert!(
            format!("{:#}", error).contains("expected exactly one DNS zone")
        );

        // Configs must not have more than one zone.
        let example_multiple = DnsConfigParams {
            generation: 3,
            time_created: Utc::now(),
            zones: vec![
                DnsConfigZone {
                    zone_name: ZONE_NAME.to_string(),
                    records: HashMap::new(),
                },
                DnsConfigZone {
                    zone_name: "two".to_string(),
                    records: HashMap::new(),
                },
            ],
        };
        let error = DnsDiff::new(&example_multiple, &example).expect_err(
            "unexpectedly succeeded comparing config with multiple zones",
        );
        assert!(
            format!("{:#}", error).contains("expected exactly one DNS zone")
        );

        // Cannot compare different zone names
        let example_different_zone = DnsConfigParams {
            generation: 3,
            time_created: Utc::now(),
            zones: vec![DnsConfigZone {
                zone_name: format!("{}-other", ZONE_NAME),
                records: HashMap::new(),
            }],
        };
        let error = DnsDiff::new(&example_different_zone, &example).expect_err(
            "unexpectedly succeeded comparing configs with \
            different zone names",
        );
        assert_eq!(
            format!("{:#}", error),
            "cannot compare DNS configuration from zones with different \
            names: \"dummy-other\" vs. \"dummy\""
        );
    }

    #[test]
    fn diff_equivalent() {
        let example = example();
        let diff = DnsDiff::new(&example, &example).unwrap();
        assert!(diff.is_empty());
        assert_eq!(diff.names_removed().count(), 0);
        assert_eq!(diff.names_added().count(), 0);
        assert_eq!(diff.names_changed().count(), 0);
    }

    #[test]
    fn diff_different() {
        let example = example();
        let example2 = DnsConfigParams {
            generation: 4,
            time_created: Utc::now(),
            zones: vec![DnsConfigZone {
                zone_name: ZONE_NAME.to_string(),
                records: HashMap::from([
                    (
                        "ex2".to_string(),
                        vec![DnsRecord::A("192.168.1.4".parse().unwrap())],
                    ),
                    (
                        "ex3".to_string(),
                        vec![DnsRecord::A(std::net::Ipv4Addr::LOCALHOST)],
                    ),
                ]),
            }],
        };

        let diff = DnsDiff::new(&example, &example2).unwrap();
        assert!(!diff.is_empty());

        let removed = diff.names_removed().collect::<Vec<_>>();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].0, "ex1");
        assert_eq!(removed[0].1, vec![DnsRecord::A(Ipv4Addr::LOCALHOST)]);

        let added = diff.names_added().collect::<Vec<_>>();
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].0, "ex3");
        assert_eq!(added[0].1, vec![DnsRecord::A(Ipv4Addr::LOCALHOST)]);

        let changed = diff.names_changed().collect::<Vec<_>>();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, "ex2");
        assert_eq!(
            changed[0].1,
            vec![DnsRecord::A("192.168.1.3".parse().unwrap())]
        );
        assert_eq!(
            changed[0].2,
            vec![DnsRecord::A("192.168.1.4".parse().unwrap())]
        );
    }
}
