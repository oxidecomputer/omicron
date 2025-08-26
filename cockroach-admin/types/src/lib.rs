// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, NaiveDateTime, Utc};
use csv::StringRecord;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de};
use std::{io, net::SocketAddr};

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

/// CockroachDB Node ID
///
/// This field is stored internally as a String to avoid questions
/// about size, signedness, etc - it can be treated as an arbitrary
/// unique identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for NodeId {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

// When parsing the underlying NodeId, we force it to be interpreted
// as a String. Without this custom Deserialize implementation, we
// encounter parsing errors when querying endpoints which return the
// NodeId as an integer.
impl<'de> serde::Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{Error, Visitor};
        use std::fmt;

        struct NodeIdVisitor;

        impl<'de> Visitor<'de> for NodeIdVisitor {
            type Value = NodeId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter
                    .write_str("a string or integer representing a node ID")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(NodeId(value.to_string()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(NodeId(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(NodeId(value.to_string()))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(NodeId(value.to_string()))
            }
        }

        deserializer.deserialize_any(NodeIdVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NodeStatus {
    // TODO use NodeId
    pub node_id: String,
    pub address: SocketAddr,
    pub sql_address: SocketAddr,
    pub build: String,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub locality: String,
    pub is_available: bool,
    pub is_live: bool,
    pub replicas_leaders: i64,
    pub replicas_leaseholders: i64,
    pub ranges: i64,
    pub ranges_unavailable: i64,
    pub ranges_underreplicated: i64,
    pub live_bytes: i64,
    pub key_bytes: i64,
    pub value_bytes: i64,
    pub intent_bytes: i64,
    pub system_bytes: i64,
    pub gossiped_replicas: i64,
    pub is_decommissioning: bool,
    pub membership: String,
    pub is_draining: bool,
}

impl NodeStatus {
    pub fn parse_from_csv(data: &[u8]) -> Result<Vec<Self>, ParseError> {
        let mut statuses = Vec::new();
        let mut reader = csv::Reader::from_reader(io::Cursor::new(data));

        // We can't naively deserialize every record as a `CliNodeStatus`
        // directly, because the `node status --all` flag to get all details
        // also causes cockroach to emit statuses for decommissioned nodes,
        // which report `NULL` for most fields. For now, we want to skip
        // decommissioned nodes entirely, so we'll parse each record
        // individually after checking first for whether it's decommissioned.
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
            let row = row.map_err(NodeStatusError::ParseRecordRow)?;

            // Skip decommissioned nodes without attempting to parse them
            // further, as noted above
            let Some(membership) = row.get(membership_idx) else {
                return Err(NodeStatusError::StatusRowMissingFields(row).into());
            };
            if membership == "decommissioned" {
                continue;
            }

            let record: CliNodeStatus =
                row.deserialize(Some(&headers)).map_err(|err| {
                    NodeStatusError::ParseStatusRow { row: row.clone(), err }
                })?;
            statuses.push(record.into());
        }

        Ok(statuses)
    }
}

// Slightly different `NodeStatus` that matches what we get from `cockroach`:
// timestamps are a fixed format with no timezone (but are actually UTC), so we
// have a custom deserializer, and the ID column is `id` instead of `node_id`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct CliNodeStatus {
    id: String,
    address: SocketAddr,
    sql_address: SocketAddr,
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NodeDecommission {
    pub node_id: String,
    pub is_live: bool,
    pub replicas: i64,
    pub is_decommissioning: bool,
    pub membership: NodeMembership,
    pub is_draining: bool,
    pub notes: Vec<String>,
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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

// The cockroach CLI and `crdb_internal.gossip_liveness` table use a string for
// node membership, but there are only three meaningful values per
// https://github.com/cockroachdb/cockroach/blob/0c92c710d2baadfdc5475be8d2238cf26cb152ca/pkg/kv/kvserver/liveness/livenesspb/liveness.go#L96,
// so we'll convert into a Rust enum and leave the "unknown" case for future
// changes that expand or reword these values.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum NodeMembership {
    Active,
    Decommissioning,
    Decommissioned,
    Unknown { value: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
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

        let statuses = NodeStatus::parse_from_csv(input).expect("parsed input");
        assert_eq!(statuses, vec![expected]);
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

        let statuses = NodeStatus::parse_from_csv(input).expect("parsed input");
        assert_eq!(statuses.len(), expected.len());
        for (status, expected) in statuses.iter().zip(&expected) {
            assert_eq!(status, expected);
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
