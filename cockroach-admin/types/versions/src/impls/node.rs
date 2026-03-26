// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for node-related types.

use crate::latest::node::{NodeDecommission, NodeMembership, NodeStatus};
use chrono::{DateTime, NaiveDateTime, Utc};
use csv::StringRecord;
use serde::de;
use std::io;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("failed to parse `cockroach node status` output")]
    NodeStatus(#[from] NodeStatusError),
    #[error("failed to parse `cockroach decommission` output")]
    Decommission(#[from] DecommissionError),
}

#[derive(Debug, thiserror::Error)]
pub enum NodeStatusError {
    #[error("missing `membership` header (found: {0:?})")]
    MissingMembershipHeader(StringRecord),
    #[error("failed to parse header row")]
    ParseHeaderRow(#[source] csv::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum NodeStatusRowError {
    #[error("failed to parse record row")]
    ParseRecordRow(#[source] csv::Error),
    #[error("fewer fields than expected in status row: {0:?}")]
    StatusRowMissingFields(StringRecord),
    #[error("failed to parse node status row {row:?}")]
    ParseStatusRow {
        row: StringRecord,
        #[source]
        err: csv::Error,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum DecommissionError {
    #[error("missing output row after headers")]
    MissingOutputRow,
    #[error("failed to parse row")]
    ParseRow(#[from] csv::Error),
}

impl NodeStatus {
    /// Parse output of `cockroach node status --all`
    ///
    /// Fails if we cannot parse the first (header) row. If parsing the header
    /// row succeeds, returns two vectors containing all successfully-parsed
    /// rows and errors for any row that failed to parse.
    pub fn parse_from_csv(
        data: &[u8],
    ) -> Result<(Vec<Self>, Vec<NodeStatusRowError>), ParseError> {
        let mut statuses = Vec::new();
        let mut row_errs = Vec::new();
        let mut reader = csv::Reader::from_reader(io::Cursor::new(data));

        // We want to intentionally omit any decommissioned nodes: they
        // shouldn't be returned as either `NodeStatus` rows _or_ parse errors.
        // We expect them to produce parse errors (they report a socket address
        // of `NULL`), so we'll check for decommissioned before attempting to
        // parse.
        let headers =
            reader.headers().map_err(NodeStatusError::ParseHeaderRow)?.clone();
        let Some(membership_idx) =
            headers.iter().position(|h| h == "membership")
        else {
            return Err(
                NodeStatusError::MissingMembershipHeader(headers).into()
            );
        };

        for row in reader.into_records() {
            let row = match row {
                Ok(row) => row,
                Err(err) => {
                    row_errs.push(NodeStatusRowError::ParseRecordRow(err));
                    continue;
                }
            };

            // Skip decommissioned nodes without attempting to parse them
            // further, as noted above.
            let Some(membership) = row.get(membership_idx) else {
                row_errs.push(NodeStatusRowError::StatusRowMissingFields(row));
                continue;
            };
            if membership == "decommissioned" {
                continue;
            }

            let record = match row.deserialize::<CliNodeStatus>(Some(&headers))
            {
                Ok(record) => record,
                Err(err) => {
                    row_errs
                        .push(NodeStatusRowError::ParseStatusRow { row, err });
                    continue;
                }
            };

            statuses.push(record.into());
        }

        Ok((statuses, row_errs))
    }
}

// Slightly different `NodeStatus` that matches what we get from `cockroach`:
// timestamps are a fixed format with no timezone (but are actually UTC), so we
// have a custom deserializer, and the ID column is `id` instead of `node_id`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
struct CliNodeStatus {
    id: String,
    address: std::net::SocketAddr,
    sql_address: std::net::SocketAddr,
    build: String,
    #[serde(deserialize_with = "parse_cockroach_cli_timestamp")]
    started_at: DateTime<Utc>,
    #[serde(deserialize_with = "parse_cockroach_cli_timestamp")]
    updated_at: DateTime<Utc>,
    locality: String,
    is_available: bool,
    is_live: bool,
    replicas_leaders: i64,
    replicas_leaseholders: i64,
    ranges: i64,
    ranges_unavailable: i64,
    ranges_underreplicated: i64,
    live_bytes: i64,
    key_bytes: i64,
    value_bytes: i64,
    intent_bytes: i64,
    system_bytes: i64,
    gossiped_replicas: i64,
    is_decommissioning: bool,
    membership: String,
    is_draining: bool,
}

impl From<CliNodeStatus> for NodeStatus {
    fn from(cli: CliNodeStatus) -> Self {
        Self {
            node_id: cli.id,
            address: cli.address,
            sql_address: cli.sql_address,
            build: cli.build,
            started_at: cli.started_at,
            updated_at: cli.updated_at,
            locality: cli.locality,
            is_available: cli.is_available,
            is_live: cli.is_live,
            replicas_leaders: cli.replicas_leaders,
            replicas_leaseholders: cli.replicas_leaseholders,
            ranges: cli.ranges,
            ranges_unavailable: cli.ranges_unavailable,
            ranges_underreplicated: cli.ranges_underreplicated,
            live_bytes: cli.live_bytes,
            key_bytes: cli.key_bytes,
            value_bytes: cli.value_bytes,
            intent_bytes: cli.intent_bytes,
            system_bytes: cli.system_bytes,
            gossiped_replicas: cli.gossiped_replicas,
            is_decommissioning: cli.is_decommissioning,
            membership: cli.membership,
            is_draining: cli.is_draining,
        }
    }
}

fn parse_cockroach_cli_timestamp<'de, D>(
    d: D,
) -> Result<DateTime<Utc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct CockroachTimestampVisitor;
    impl de::Visitor<'_> for CockroachTimestampVisitor {
        type Value = DateTime<Utc>;

        fn expecting(
            &self,
            formatter: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            formatter.write_str("a Cockroach CLI timestamp")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let dt = NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S%.f")
                .map_err(E::custom)?;
            Ok(DateTime::from_naive_utc_and_offset(dt, Utc))
        }
    }

    d.deserialize_str(CockroachTimestampVisitor)
}

impl NodeDecommission {
    pub fn parse_from_csv(data: &[u8]) -> Result<Self, ParseError> {
        // Reading the node decommission output is awkward because it isn't
        // fully CSV. We expect a CSV header, then a row for each node being
        // decommissioned, then (maybe) a blank line followed by a note that is
        // just a string, not related to the initial CSV data. Even though the
        // CLI supports decommissioning more than one node in one invocation, we
        // only provide an API to decommission a single node, so we expect:
        //
        // 1. The CSV header line
        // 2. The one row of CSV data
        // 3. Trailing notes
        //
        // We'll collect the notes as a separate field and return them to our
        // caller.

        // First we'll run the data through a csv::Reader; this will pull out
        // the header row and the one row of data.
        let mut reader = csv::Reader::from_reader(io::Cursor::new(data));
        let record: CliNodeDecommission = reader
            .deserialize()
            .next()
            .ok_or_else(|| DecommissionError::MissingOutputRow)?
            .map_err(DecommissionError::ParseRow)?;

        // Get the position where the reader ended after that one row; we'll
        // collect any remaining nonempty lines as `notes`.
        let extra_data = &data[reader.position().byte() as usize..];
        let mut notes = Vec::new();
        for line in String::from_utf8_lossy(extra_data).lines() {
            let line = line.trim();
            if !line.is_empty() {
                notes.push(line.to_string());
            }
        }

        Ok(Self::from((record, notes)))
    }
}

// Slightly different `NodeDecommission` that matches what we get from
// `cockroach`: this omites `notes`, which isn't really a CSV field at all, but
// is instead where we collect the non-CSV string output from the CLI, uses
// a custom deserializer for `membership` to handle unknown variants, and the ID
// column is `id` instead of `node_id`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
struct CliNodeDecommission {
    pub id: String,
    pub is_live: bool,
    pub replicas: i64,
    pub is_decommissioning: bool,
    #[serde(deserialize_with = "parse_node_membership")]
    pub membership: NodeMembership,
    pub is_draining: bool,
}

impl From<(CliNodeDecommission, Vec<String>)> for NodeDecommission {
    fn from((cli, notes): (CliNodeDecommission, Vec<String>)) -> Self {
        Self {
            node_id: cli.id,
            is_live: cli.is_live,
            replicas: cli.replicas,
            is_decommissioning: cli.is_decommissioning,
            membership: cli.membership,
            is_draining: cli.is_draining,
            notes,
        }
    }
}

fn parse_node_membership<'de, D>(d: D) -> Result<NodeMembership, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct CockroachNodeMembershipVisitor;

    impl de::Visitor<'_> for CockroachNodeMembershipVisitor {
        type Value = NodeMembership;

        fn expecting(
            &self,
            formatter: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            formatter.write_str("a Cockroach node membership string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let membership = match v {
                "active" => NodeMembership::Active,
                "decommissioning" => NodeMembership::Decommissioning,
                "decommissioned" => NodeMembership::Decommissioned,
                _ => NodeMembership::Unknown { value: v.to_string() },
            };
            Ok(membership)
        }
    }

    d.deserialize_str(CockroachNodeMembershipVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use slog_error_chain::InlineErrorChain;
    use test_strategy::proptest;

    #[test]
    fn test_node_status_parse_single_line_from_csv() {
        let input = br#"id,address,sql_address,build,started_at,updated_at,locality,is_available,is_live,replicas_leaders,replicas_leaseholders,ranges,ranges_unavailable,ranges_underreplicated,live_bytes,key_bytes,value_bytes,intent_bytes,system_bytes,gossiped_replicas,is_decommissioning,membership,is_draining
5,[fd00:1122:3344:103::3]:32221,[fd00:1122:3344:103::3]:32221,v22.1.22-29-g865aff1595,2025-05-30 21:10:30.527658,2025-06-02 14:00:36.749872,,true,true,38,38,210,0,0,3958791538,846009128,5249950302,0,108083397,210,false,active,false"#;
        let expected = NodeStatus {
            node_id: "5".to_string(),
            address: "[fd00:1122:3344:103::3]:32221".parse().unwrap(),
            sql_address: "[fd00:1122:3344:103::3]:32221".parse().unwrap(),
            build: "v22.1.22-29-g865aff1595".to_string(),
            started_at: DateTime::from_naive_utc_and_offset(
                NaiveDate::from_ymd_opt(2025, 5, 30)
                    .unwrap()
                    .and_hms_micro_opt(21, 10, 30, 527658)
                    .unwrap(),
                Utc,
            ),
            updated_at: DateTime::from_naive_utc_and_offset(
                NaiveDate::from_ymd_opt(2025, 6, 2)
                    .unwrap()
                    .and_hms_micro_opt(14, 0, 36, 749872)
                    .unwrap(),
                Utc,
            ),
            locality: String::new(),
            is_available: true,
            is_live: true,
            replicas_leaders: 38,
            replicas_leaseholders: 38,
            ranges: 210,
            ranges_unavailable: 0,
            ranges_underreplicated: 0,
            live_bytes: 3958791538,
            key_bytes: 846009128,
            value_bytes: 5249950302,
            intent_bytes: 0,
            system_bytes: 108083397,
            gossiped_replicas: 210,
            is_decommissioning: false,
            membership: "active".to_string(),
            is_draining: false,
        };

        let (statuses, errs) =
            NodeStatus::parse_from_csv(input).expect("parsed input");
        assert_eq!(statuses, vec![expected]);
        assert!(errs.is_empty());
    }

    #[test]
    fn test_node_status_parse_multiple_lines_from_csv() {
        let input = br#"id,address,sql_address,build,started_at,updated_at,locality,is_available,is_live,replicas_leaders,replicas_leaseholders,ranges,ranges_unavailable,ranges_underreplicated,live_bytes,key_bytes,value_bytes,intent_bytes,system_bytes,gossiped_replicas,is_decommissioning,membership,is_draining
1,[fd00:1122:3344:101::3]:32221,[fd00:1122:3344:101::3]:32221,v22.1.22-29-g865aff1595,2025-05-30 21:10:26.237011,2025-06-02 14:05:05.508688,,true,true,41,41,210,0,0,3967748150,846544773,5119261316,34,108060755,210,false,active,false
2,[fd00:1122:3344:102::3]:32221,[fd00:1122:3344:102::3]:32221,v22.1.22-29-g865aff1595,2025-05-30 21:10:30.00501,2025-06-02 14:05:05.090293,,true,true,44,44,210,0,0,3967896249,846559229,5119394994,861,108017090,210,false,active,false
3,NULL,NULL,NULL,NULL,2025-05-30 21:11:45.350419,NULL,false,false,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,true,decommissioned,false
4,NULL,NULL,NULL,NULL,2025-05-30 21:11:45.668157,NULL,false,false,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,true,decommissioned,false
6,[fd00:1122:3344:102::21]:32221,[fd00:1122:3344:102::21]:32221,v22.1.22-29-g865aff1595,2025-05-30 21:10:26.26209,2025-06-02 14:05:05.906022,,true,true,41,41,210,0,0,3967896044,846559229,5119394789,0,108016856,210,false,active,false"#;
        let expected = vec![
            NodeStatus {
                node_id: "1".to_string(),
                address: "[fd00:1122:3344:101::3]:32221".parse().unwrap(),
                sql_address: "[fd00:1122:3344:101::3]:32221".parse().unwrap(),
                build: "v22.1.22-29-g865aff1595".to_string(),
                started_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2025, 5, 30)
                        .unwrap()
                        .and_hms_micro_opt(21, 10, 26, 237011)
                        .unwrap(),
                    Utc,
                ),
                updated_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2025, 6, 2)
                        .unwrap()
                        .and_hms_micro_opt(14, 5, 5, 508688)
                        .unwrap(),
                    Utc,
                ),
                locality: String::new(),
                is_available: true,
                is_live: true,
                replicas_leaders: 41,
                replicas_leaseholders: 41,
                ranges: 210,
                ranges_unavailable: 0,
                ranges_underreplicated: 0,
                live_bytes: 3967748150,
                key_bytes: 846544773,
                value_bytes: 5119261316,
                intent_bytes: 34,
                system_bytes: 108060755,
                gossiped_replicas: 210,
                is_decommissioning: false,
                membership: "active".to_string(),
                is_draining: false,
            },
            NodeStatus {
                node_id: "2".to_string(),
                address: "[fd00:1122:3344:102::3]:32221".parse().unwrap(),
                sql_address: "[fd00:1122:3344:102::3]:32221".parse().unwrap(),
                build: "v22.1.22-29-g865aff1595".to_string(),
                started_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2025, 5, 30)
                        .unwrap()
                        .and_hms_micro_opt(21, 10, 30, 5010)
                        .unwrap(),
                    Utc,
                ),
                updated_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2025, 6, 2)
                        .unwrap()
                        .and_hms_micro_opt(14, 5, 5, 90293)
                        .unwrap(),
                    Utc,
                ),
                locality: String::new(),
                is_available: true,
                is_live: true,
                replicas_leaders: 44,
                replicas_leaseholders: 44,
                ranges: 210,
                ranges_unavailable: 0,
                ranges_underreplicated: 0,
                live_bytes: 3967896249,
                key_bytes: 846559229,
                value_bytes: 5119394994,
                intent_bytes: 861,
                system_bytes: 108017090,
                gossiped_replicas: 210,
                is_decommissioning: false,
                membership: "active".to_string(),
                is_draining: false,
            },
            NodeStatus {
                node_id: "6".to_string(),
                address: "[fd00:1122:3344:102::21]:32221".parse().unwrap(),
                sql_address: "[fd00:1122:3344:102::21]:32221".parse().unwrap(),
                build: "v22.1.22-29-g865aff1595".to_string(),
                started_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2025, 5, 30)
                        .unwrap()
                        .and_hms_micro_opt(21, 10, 26, 262090)
                        .unwrap(),
                    Utc,
                ),
                updated_at: DateTime::from_naive_utc_and_offset(
                    NaiveDate::from_ymd_opt(2025, 6, 2)
                        .unwrap()
                        .and_hms_micro_opt(14, 5, 5, 906022)
                        .unwrap(),
                    Utc,
                ),
                locality: String::new(),
                is_available: true,
                is_live: true,
                replicas_leaders: 41,
                replicas_leaseholders: 41,
                ranges: 210,
                ranges_unavailable: 0,
                ranges_underreplicated: 0,
                live_bytes: 3967896044,
                key_bytes: 846559229,
                value_bytes: 5119394789,
                intent_bytes: 0,
                system_bytes: 108016856,
                gossiped_replicas: 210,
                is_decommissioning: false,
                membership: "active".to_string(),
                is_draining: false,
            },
        ];

        let (statuses, errs) =
            NodeStatus::parse_from_csv(input).expect("parsed input");
        assert_eq!(statuses.len(), expected.len());
        for (status, expected) in statuses.iter().zip(&expected) {
            assert_eq!(status, expected);
        }
        // We have 2 rows we can't parse, but they both have a `membership` of
        // `decommissioned`, so we should skip them instead of reporting them as
        // parse errors.
        assert_eq!(errs.len(), 0);
    }

    // Test actual status from london racklette; omicron#10068
    #[test]
    fn test_node_status_invalid_rows_not_decommissioned() {
        let input = br#"id,address,sql_address,build,started_at,updated_at,locality,is_available,is_live,replicas_leaders,replicas_leaseholders,ranges,ranges_unavailable,ranges_underreplicated,live_bytes,key_bytes,value_bytes,intent_bytes,system_bytes,gossiped_replicas,is_decommissioning,membership,is_draining
1,NULL,NULL,NULL,NULL,2026-03-17 00:28:07.845312,NULL,false,false,50,50,244,0,0,232400764,76374197,322723215,0,166651,NULL,false,active,false
2,[fd3e:7225:64f1:102::3]:32221,[fd3e:7225:64f1:102::3]:32221,v22.1.22-46-g367bca413b,2026-03-16 19:20:59.488975,2026-03-17 00:49:12.573435,,true,true,48,48,244,0,0,505851138,114074028,651318542,0,707599,244,false,active,false
3,[fd3e:7225:64f1:104::3]:32221,[fd3e:7225:64f1:104::3]:32221,v22.1.22-46-g367bca413b,2026-03-16 19:21:00.330062,2026-03-17 00:49:12.320826,,true,true,49,49,244,0,0,505941788,114074052,651409236,0,707599,244,false,active,false
4,[fd3e:7225:64f1:103::3]:32221,[fd3e:7225:64f1:103::3]:32221,v22.1.22-46-g367bca413b,2026-03-16 19:21:00.432083,2026-03-17 00:49:12.422705,,true,true,48,48,244,0,0,505987858,114074293,651455102,0,707599,244,false,active,false
5,[fd3e:7225:64f1:104::4]:32221,[fd3e:7225:64f1:104::4]:32221,v22.1.22-46-g367bca413b,2026-03-16 19:21:00.683945,2026-03-17 00:49:12.67174,,true,true,45,45,244,0,0,505896463,114074052,651363911,0,707599,244,false,active,false
6,NULL,NULL,NULL,NULL,2026-03-17 00:28:07.845325,NULL,false,false,54,54,227,0,21,241750790,74592314,326610733,0,275612,NULL,false,active,false
7,NULL,NULL,NULL,NULL,2026-03-17 00:28:07.845326,NULL,false,false,54,54,244,0,0,274322449,84865180,370993320,0,433888,NULL,false,active,false
8,[fd3e:7225:64f1:101::33]:32221,[fd3e:7225:64f1:101::33]:32221,v22.1.22-46-g367bca413b,2026-03-16 22:13:14.330106,2026-03-17 00:28:07.845328,,false,false,54,54,244,0,0,465351282,99646220,595058198,0,647190,0,false,active,false
9,[fd3e:7225:64f1:101::3a]:32221,[fd3e:7225:64f1:101::3a]:32221,v22.1.22-46-g367bca413b,2026-03-17 00:26:13.672751,2026-03-17 00:49:15.258134,,true,true,54,54,244,0,0,505641887,114109608,651500710,0,707599,244,false,active,false"#;

        let expected = vec![
            NodeStatus {
                node_id: "2".to_owned(),
                address: "[fd3e:7225:64f1:102::3]:32221".parse().unwrap(),
                sql_address: "[fd3e:7225:64f1:102::3]:32221".parse().unwrap(),
                build: "v22.1.22-46-g367bca413b".to_owned(),
                started_at: "2026-03-16T19:20:59.488975Z".parse().unwrap(),
                updated_at: "2026-03-17T00:49:12.573435Z".parse().unwrap(),
                locality: String::new(),
                is_available: true,
                is_live: true,
                replicas_leaders: 48,
                replicas_leaseholders: 48,
                ranges: 244,
                ranges_unavailable: 0,
                ranges_underreplicated: 0,
                live_bytes: 505851138,
                key_bytes: 114074028,
                value_bytes: 651318542,
                intent_bytes: 0,
                system_bytes: 707599,
                gossiped_replicas: 244,
                is_decommissioning: false,
                membership: "active".to_owned(),
                is_draining: false,
            },
            NodeStatus {
                node_id: "3".to_owned(),
                address: "[fd3e:7225:64f1:104::3]:32221".parse().unwrap(),
                sql_address: "[fd3e:7225:64f1:104::3]:32221".parse().unwrap(),
                build: "v22.1.22-46-g367bca413b".to_owned(),
                started_at: "2026-03-16T19:21:00.330062Z".parse().unwrap(),
                updated_at: "2026-03-17T00:49:12.320826Z".parse().unwrap(),
                locality: String::new(),
                is_available: true,
                is_live: true,
                replicas_leaders: 49,
                replicas_leaseholders: 49,
                ranges: 244,
                ranges_unavailable: 0,
                ranges_underreplicated: 0,
                live_bytes: 505941788,
                key_bytes: 114074052,
                value_bytes: 651409236,
                intent_bytes: 0,
                system_bytes: 707599,
                gossiped_replicas: 244,
                is_decommissioning: false,
                membership: "active".to_owned(),
                is_draining: false,
            },
            NodeStatus {
                node_id: "4".to_owned(),
                address: "[fd3e:7225:64f1:103::3]:32221".parse().unwrap(),
                sql_address: "[fd3e:7225:64f1:103::3]:32221".parse().unwrap(),
                build: "v22.1.22-46-g367bca413b".to_owned(),
                started_at: "2026-03-16T19:21:00.432083Z".parse().unwrap(),
                updated_at: "2026-03-17T00:49:12.422705Z".parse().unwrap(),
                locality: String::new(),
                is_available: true,
                is_live: true,
                replicas_leaders: 48,
                replicas_leaseholders: 48,
                ranges: 244,
                ranges_unavailable: 0,
                ranges_underreplicated: 0,
                live_bytes: 505987858,
                key_bytes: 114074293,
                value_bytes: 651455102,
                intent_bytes: 0,
                system_bytes: 707599,
                gossiped_replicas: 244,
                is_decommissioning: false,
                membership: "active".to_owned(),
                is_draining: false,
            },
            NodeStatus {
                node_id: "5".to_owned(),
                address: "[fd3e:7225:64f1:104::4]:32221".parse().unwrap(),
                sql_address: "[fd3e:7225:64f1:104::4]:32221".parse().unwrap(),
                build: "v22.1.22-46-g367bca413b".to_owned(),
                started_at: "2026-03-16T19:21:00.683945Z".parse().unwrap(),
                updated_at: "2026-03-17T00:49:12.671740Z".parse().unwrap(),
                locality: String::new(),
                is_available: true,
                is_live: true,
                replicas_leaders: 45,
                replicas_leaseholders: 45,
                ranges: 244,
                ranges_unavailable: 0,
                ranges_underreplicated: 0,
                live_bytes: 505896463,
                key_bytes: 114074052,
                value_bytes: 651363911,
                intent_bytes: 0,
                system_bytes: 707599,
                gossiped_replicas: 244,
                is_decommissioning: false,
                membership: "active".to_owned(),
                is_draining: false,
            },
            NodeStatus {
                node_id: "8".to_owned(),
                address: "[fd3e:7225:64f1:101::33]:32221".parse().unwrap(),
                sql_address: "[fd3e:7225:64f1:101::33]:32221".parse().unwrap(),
                build: "v22.1.22-46-g367bca413b".to_owned(),
                started_at: "2026-03-16T22:13:14.330106Z".parse().unwrap(),
                updated_at: "2026-03-17T00:28:07.845328Z".parse().unwrap(),
                locality: String::new(),
                is_available: false,
                is_live: false,
                replicas_leaders: 54,
                replicas_leaseholders: 54,
                ranges: 244,
                ranges_unavailable: 0,
                ranges_underreplicated: 0,
                live_bytes: 465351282,
                key_bytes: 99646220,
                value_bytes: 595058198,
                intent_bytes: 0,
                system_bytes: 647190,
                gossiped_replicas: 0,
                is_decommissioning: false,
                membership: "active".to_owned(),
                is_draining: false,
            },
            NodeStatus {
                node_id: "9".to_owned(),
                address: "[fd3e:7225:64f1:101::3a]:32221".parse().unwrap(),
                sql_address: "[fd3e:7225:64f1:101::3a]:32221".parse().unwrap(),
                build: "v22.1.22-46-g367bca413b".to_owned(),
                started_at: "2026-03-17T00:26:13.672751Z".parse().unwrap(),
                updated_at: "2026-03-17T00:49:15.258134Z".parse().unwrap(),
                locality: String::new(),
                is_available: true,
                is_live: true,
                replicas_leaders: 54,
                replicas_leaseholders: 54,
                ranges: 244,
                ranges_unavailable: 0,
                ranges_underreplicated: 0,
                live_bytes: 505641887,
                key_bytes: 114109608,
                value_bytes: 651500710,
                intent_bytes: 0,
                system_bytes: 707599,
                gossiped_replicas: 244,
                is_decommissioning: false,
                membership: "active".to_owned(),
                is_draining: false,
            },
        ];
        let (statuses, errs) =
            NodeStatus::parse_from_csv(input).expect("parsed input");
        assert_eq!(statuses.len(), expected.len());
        for (status, expected) in statuses.iter().zip(&expected) {
            assert_eq!(status, expected);
        }
        assert_eq!(errs.len(), 3);
        for err in errs {
            let err = InlineErrorChain::new(&err).to_string();
            assert!(
                err.contains("failed to parse node status row")
                    && err.contains("invalid socket address syntax"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn test_node_decommission_parse_with_no_trailing_notes() {
        let input =
            br#"id,is_live,replicas,is_decommissioning,membership,is_draining
6,true,24,true,decommissioning,false"#;
        let expected = NodeDecommission {
            node_id: "6".to_string(),
            is_live: true,
            replicas: 24,
            is_decommissioning: true,
            membership: NodeMembership::Decommissioning,
            is_draining: false,
            notes: vec![],
        };

        let statuses =
            NodeDecommission::parse_from_csv(input).expect("parsed input");
        assert_eq!(statuses, expected);
    }

    #[test]
    fn test_node_decommission_parse_with_trailing_notes() {
        let input =
            br#"id,is_live,replicas,is_decommissioning,membership,is_draining
6,false,0,true,decommissioned,false

No more data reported on target nodes. Please verify cluster health before removing the nodes.
"#;
        let expected = NodeDecommission {
            node_id: "6".to_string(),
            is_live: false,
            replicas: 0,
            is_decommissioning: true,
            membership: NodeMembership::Decommissioned,
            is_draining: false,
            notes: vec![
                "No more data reported on target nodes. \
                Please verify cluster health before removing the nodes."
                    .to_string(),
            ],
        };

        let statuses =
            NodeDecommission::parse_from_csv(input).expect("parsed input");
        assert_eq!(statuses, expected);
    }

    #[test]
    fn test_node_decommission_parse_with_unexpected_membership_value() {
        let input =
            br#"id,is_live,replicas,is_decommissioning,membership,is_draining
6,false,0,true,foobar,false"#;
        let expected = NodeDecommission {
            node_id: "6".to_string(),
            is_live: false,
            replicas: 0,
            is_decommissioning: true,
            membership: NodeMembership::Unknown { value: "foobar".to_string() },
            is_draining: false,
            notes: vec![],
        };

        let statuses =
            NodeDecommission::parse_from_csv(input).expect("parsed input");
        assert_eq!(statuses, expected);
    }

    // TODO: the proptests below should probably be fuzz targets instead to
    // allow for guided fuzzing.

    #[proptest]
    fn node_status_parse_doesnt_panic_on_arbitrary_input(input: Vec<u8>) {
        _ = NodeStatus::parse_from_csv(&input);
    }

    #[proptest]
    fn node_decommission_parse_doesnt_panic_on_arbitrary_input(input: Vec<u8>) {
        _ = NodeDecommission::parse_from_csv(&input);
    }
}
