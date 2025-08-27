// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::ensure;
use std::collections::BTreeSet;
use std::collections::HashMap;

use crate::config::DnsConfigZone;
use crate::config::DnsRecord;
use crate::config::Srv;

#[derive(Debug)]
enum NameDiff<'a> {
    Added(&'a str, &'a [DnsRecord]),
    Removed(&'a str, &'a [DnsRecord]),
    Changed(&'a str, &'a [DnsRecord], &'a [DnsRecord]),
    Unchanged(&'a str, &'a [DnsRecord]),
}

type DnsRecords = HashMap<String, Vec<DnsRecord>>;

/// Compare the DNS records contained in two sets of DNS configuration
#[derive(Debug)]
pub struct DnsDiff<'a> {
    left: &'a DnsRecords,
    right: &'a DnsRecords,
    zone_name: &'a str,
    all_names: BTreeSet<&'a String>,
}

impl<'a> DnsDiff<'a> {
    /// Compare the DNS records contained in two DNS zones' configs
    ///
    /// Both zones are expected to have the same name.
    pub fn new(
        left_zone: &'a DnsConfigZone,
        right_zone: &'a DnsConfigZone,
    ) -> Result<DnsDiff<'a>, anyhow::Error> {
        ensure!(
            left_zone.zone_name == right_zone.zone_name,
            "cannot compare DNS configuration from zones with different names: \
            {:?} vs. {:?}",
            left_zone.zone_name,
            right_zone.zone_name,
        );

        let all_names =
            left_zone.records.keys().chain(right_zone.records.keys()).collect();

        Ok(DnsDiff {
            left: &left_zone.records,
            right: &right_zone.records,
            zone_name: &left_zone.zone_name,
            all_names,
        })
    }

    fn iter_names(&self) -> impl Iterator<Item = NameDiff<'_>> {
        self.all_names.iter().map(|k| {
            let name = k.as_str();
            let v1 = self.left.get(*k);
            let v2 = self.right.get(*k);
            match (v1, v2) {
                (None, Some(v2)) => NameDiff::Added(name, v2.as_ref()),
                (Some(v1), None) => NameDiff::Removed(name, v1.as_ref()),
                (Some(v1), Some(v2)) => {
                    let mut v1_sorted = v1.clone();
                    let mut v2_sorted = v2.clone();
                    v1_sorted.sort();
                    v2_sorted.sort();
                    if v1_sorted == v2_sorted {
                        NameDiff::Unchanged(name, v1.as_ref())
                    } else {
                        NameDiff::Changed(name, v1.as_ref(), v2.as_ref())
                    }
                }
                (None, None) => unreachable!(),
            }
        })
    }

    /// Iterate over the names that are present in the `right` config but
    /// absent in the `left` one (i.e., added between `left` and `right`)
    pub fn names_added(&self) -> impl Iterator<Item = (&str, &[DnsRecord])> {
        self.iter_names().filter_map(|nd| {
            if let NameDiff::Added(k, v) = nd { Some((k, v)) } else { None }
        })
    }

    /// Iterate over the names that are present in the `left` config but
    /// absent in the `right` one (i.e., removed between `left` and `right`)
    pub fn names_removed(&self) -> impl Iterator<Item = (&str, &[DnsRecord])> {
        self.iter_names().filter_map(|nd| {
            if let NameDiff::Removed(k, v) = nd { Some((k, v)) } else { None }
        })
    }

    /// Iterate over the names whose records changed between `left` and `right`.
    pub fn names_changed(
        &self,
    ) -> impl Iterator<Item = (&str, &[DnsRecord], &[DnsRecord])> {
        self.iter_names().filter_map(|nd| {
            if let NameDiff::Changed(k, v1, v2) = nd {
                Some((k, v1, v2))
            } else {
                None
            }
        })
    }

    /// Iterate over the names whose records were unchanged between `left` and
    /// `right`
    pub fn names_unchanged(
        &self,
    ) -> impl Iterator<Item = (&str, &[DnsRecord])> {
        self.iter_names().filter_map(|nd| {
            if let NameDiff::Unchanged(k, v) = nd { Some((k, v)) } else { None }
        })
    }

    /// Returns true iff there are no differences in the DNS names and records
    /// described by the given configurations
    pub fn is_empty(&self) -> bool {
        self.iter_names().all(|nd| matches!(nd, NameDiff::Unchanged(_, _)))
    }
}

impl std::fmt::Display for DnsDiff<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names_changed = !self.is_empty();
        let zone_name = &self.zone_name;

        if !names_changed {
            writeln!(f, "  DNS zone: {:?} (unchanged)", zone_name,)?;
        } else {
            writeln!(f, "* DNS zone: {:?}: ", zone_name)?;
        }

        let print_records = |f: &mut std::fmt::Formatter<'_>,
                             prefix,
                             records: &[DnsRecord]|
         -> std::fmt::Result {
            for r in records.iter() {
                writeln!(
                    f,
                    "{}       {}",
                    prefix,
                    match r {
                        DnsRecord::A(addr) => format!("A    {}", addr),
                        DnsRecord::Aaaa(addr) => format!("AAAA {}", addr),
                        DnsRecord::Srv(Srv { port, target, .. }) => {
                            format!("SRV  port {:5} {}", port, target)
                        }
                        DnsRecord::Ns(name) => format!("NS   {}", name),
                    }
                )?;
            }

            Ok(())
        };

        let mut num_names_unchanged = 0;
        let mut num_records_unchanged = 0;
        for name_diff in self.iter_names() {
            match name_diff {
                NameDiff::Added(name, records) => {
                    writeln!(
                        f,
                        "+   name: {:50} (records: {})",
                        name,
                        records.len()
                    )?;
                    print_records(f, "+", records)?;
                }
                NameDiff::Removed(name, records) => {
                    writeln!(
                        f,
                        "-   name: {:50} (records: {})",
                        name,
                        records.len()
                    )?;
                    print_records(f, "-", records)?;
                }
                NameDiff::Unchanged(_name, records) => {
                    num_names_unchanged += 1;
                    num_records_unchanged += records.len();
                }
                NameDiff::Changed(name, records1, records2) => {
                    writeln!(
                        f,
                        "*   name: {:50} (records: {} -> {})",
                        name,
                        records1.len(),
                        records2.len(),
                    )?;
                    print_records(f, "-", records1)?;
                    print_records(f, "+", records2)?;
                }
            }
        }
        writeln!(
            f,
            "    unchanged names: {num_names_unchanged} \
                  (records: {num_records_unchanged})"
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::DnsDiff;
    use crate::config::DnsConfigZone;
    use crate::config::DnsRecord;
    use std::collections::HashMap;
    use std::net::Ipv4Addr;

    const ZONE_NAME: &str = "dummy";

    fn example() -> DnsConfigZone {
        DnsConfigZone {
            zone_name: ZONE_NAME.to_string(),
            records: HashMap::from([
                ("ex1".to_string(), vec![DnsRecord::A(Ipv4Addr::LOCALHOST)]),
                (
                    "ex2".to_string(),
                    vec![DnsRecord::A("192.168.1.3".parse().unwrap())],
                ),
            ]),
        }
    }

    #[test]
    fn diff_invalid() {
        // Cannot compare different zone names
        let example_different_zone = DnsConfigZone {
            zone_name: format!("{}-other", ZONE_NAME),
            records: HashMap::new(),
        };
        let error = DnsDiff::new(&example_different_zone, &example())
            .expect_err(
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
        expectorate::assert_contents(
            "tests/output/diff_example_empty.out",
            &diff.to_string(),
        );
    }

    #[test]
    fn diff_different() {
        let example = example();
        let example2 = DnsConfigZone {
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

        expectorate::assert_contents(
            "tests/output/diff_example_different.out",
            &diff.to_string(),
        );

        // Diff'ing the reverse direction exercises different cases (e.g., what
        // was added now appears as removed).  Also, the generation number
        // should really be different.
        let diff = DnsDiff::new(&example2, &example).unwrap();
        expectorate::assert_contents(
            "tests/output/diff_example_different_reversed.out",
            &diff.to_string(),
        );
    }
}
