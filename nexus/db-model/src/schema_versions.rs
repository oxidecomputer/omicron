// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database schema versions and upgrades
//!
//! For details, see schema/crdb/README.adoc in the root of this repository.

use anyhow::{Context, bail, ensure};
use camino::Utf8Path;
use regex::Regex;
use semver::Version;
use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnOption, Expr,
    GeneratedExpressionMode, Statement, TableConstraint, Value, ValueWithSpan,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::{collections::BTreeMap, sync::LazyLock};

/// The version of the database schema this particular version of Nexus was
/// built against
///
/// This must be updated when you change the database schema.  Refer to
/// schema/crdb/README.adoc in the root of this repository for details.
pub const SCHEMA_VERSION: Version = Version::new(231, 0, 0);

/// List of all past database schema versions, in *reverse* order
///
/// If you want to change the Omicron database schema, you must update this.
static KNOWN_VERSIONS: LazyLock<Vec<KnownVersion>> = LazyLock::new(|| {
    vec![
        // +- The next version goes here!  Duplicate this line, uncomment
        // |  the *second* copy, then update that copy for your version,
        // |  leaving the first copy as an example for the next person.
        // v
        // KnownVersion::new(next_int, "unique-dirname-with-the-sql-files"),
        KnownVersion::new(231, "bgp-config-max-paths-not-null"),
        KnownVersion::new(230, "bgp-unnumbered-peers"),
        KnownVersion::new(229, "fix-session-token-column-order"),
        KnownVersion::new(228, "read-only-crucible-disks"),
        KnownVersion::new(227, "local-storage-unencrypted-dataset"),
        KnownVersion::new(226, "measurement-proper-inventory"),
        KnownVersion::new(225, "dual-stack-ephemeral-ip"),
        KnownVersion::new(224, "add-external-subnets"),
        KnownVersion::new(223, "ip-pool-range-by-pool-id-index"),
        KnownVersion::new(222, "audit-log-credential-id"),
        KnownVersion::new(221, "audit-log-auth-method-enum"),
        KnownVersion::new(220, "multicast-implicit-lifecycle"),
        KnownVersion::new(219, "blueprint-sled-last-used-ip"),
        KnownVersion::new(218, "measurements"),
        KnownVersion::new(217, "multiple-default-ip-pools-per-silo"),
        KnownVersion::new(216, "add-trust-quorum"),
        KnownVersion::new(215, "support-up-to-12-disks"),
        KnownVersion::new(214, "separate-transit-ips-by-version"),
        KnownVersion::new(213, "fm-cases"),
        KnownVersion::new(212, "local-storage-disk-type"),
        KnownVersion::new(211, "blueprint-sled-config-subnet"),
        KnownVersion::new(210, "one-big-ereport-table"),
        KnownVersion::new(209, "multicast-group-support"),
        KnownVersion::new(208, "disable-tuf-repo-pruner"),
        KnownVersion::new(207, "disk-types"),
        KnownVersion::new(206, "fm-sitreps-by-parent-id-index"),
        KnownVersion::new(205, "fm-sitrep"),
        KnownVersion::new(204, "local-storage-dataset"),
        KnownVersion::new(203, "scim-actor-audit-log"),
        KnownVersion::new(202, "add-ip-to-external-ip-index"),
        KnownVersion::new(201, "scim-client-bearer-token"),
        KnownVersion::new(200, "dual-stack-network-interfaces"),
        KnownVersion::new(199, "multicast-pool-support"),
        KnownVersion::new(198, "add-ip-pool-reservation-type-column"),
        KnownVersion::new(197, "scim-users-and-groups"),
        KnownVersion::new(196, "user-provision-type-for-silo-user-and-group"),
        KnownVersion::new(195, "tuf-pruned-index"),
        KnownVersion::new(194, "tuf-pruned"),
        KnownVersion::new(193, "nexus-lockstep-port"),
        KnownVersion::new(192, "blueprint-source"),
        KnownVersion::new(191, "debug-log-blueprint-planner"),
        KnownVersion::new(190, "add-instance-cpu-platform"),
        KnownVersion::new(189, "reconfigurator-chicken-switches-to-config"),
        KnownVersion::new(188, "positive-quotas"),
        KnownVersion::new(187, "no-default-pool-for-internal-silo"),
        KnownVersion::new(186, "nexus-generation"),
        KnownVersion::new(185, "populate-db-metadata-nexus"),
        KnownVersion::new(184, "store-silo-admin-group-name"),
        KnownVersion::new(183, "add-ip-version-to-pools"),
        KnownVersion::new(182, "add-tuf-artifact-board"),
        KnownVersion::new(181, "rename-nat-table"),
        KnownVersion::new(180, "sled-cpu-family"),
        KnownVersion::new(179, "add-pending-mgs-updates-host-phase-1"),
        KnownVersion::new(178, "change-lldp-management-ip-to-inet"),
        KnownVersion::new(177, "add-host-ereport-part-number"),
        KnownVersion::new(176, "audit-log"),
        KnownVersion::new(175, "inv-host-phase-1-active-slot"),
        KnownVersion::new(174, "add-tuf-rot-by-sign"),
        KnownVersion::new(173, "inv-internal-dns"),
        KnownVersion::new(172, "add-zones-with-mupdate-override"),
        KnownVersion::new(171, "inv-clear-mupdate-override"),
        KnownVersion::new(170, "add-pending-mgs-updates-rot-bootloader"),
        KnownVersion::new(169, "inv-ntp-timesync"),
        KnownVersion::new(168, "add-inv-host-phase-1-flash-hash"),
        KnownVersion::new(167, "add-pending-mgs-updates-rot"),
        KnownVersion::new(166, "bundle-user-comment"),
        KnownVersion::new(165, "route-config-rib-priority"),
        KnownVersion::new(164, "fix-leaked-bp-oximeter-read-policy-rows"),
        KnownVersion::new(163, "bp-desired-host-phase-2"),
        KnownVersion::new(162, "bundle-by-creation"),
        KnownVersion::new(161, "inv_cockroachdb_status"),
        KnownVersion::new(160, "tuf-trust-root"),
        KnownVersion::new(159, "sled-config-desired-host-phase-2"),
        KnownVersion::new(158, "drop-builtin-roles"),
        KnownVersion::new(157, "user-data-export"),
        KnownVersion::new(156, "boot-partitions-inventory"),
        KnownVersion::new(155, "vpc-firewall-icmp"),
        KnownVersion::new(154, "add-pending-mgs-updates"),
        KnownVersion::new(153, "chicken-switches"),
        KnownVersion::new(152, "ereports"),
        KnownVersion::new(151, "zone-image-resolver-inventory"),
        KnownVersion::new(150, "add-last-reconciliation-orphaned-datasets"),
        KnownVersion::new(149, "bp-add-target-release-min-gen"),
        KnownVersion::new(148, "clean-misplaced-m2s"),
        KnownVersion::new(147, "device-auth-request-ttl"),
        KnownVersion::new(146, "silo-settings-token-expiration"),
        KnownVersion::new(145, "token-and-session-ids"),
        KnownVersion::new(144, "inventory-omicron-sled-config"),
        KnownVersion::new(143, "alerts-renamening"),
        KnownVersion::new(142, "bp-add-remove-mupdate-override"),
        KnownVersion::new(141, "caboose-sign-value"),
        KnownVersion::new(140, "instance-intended-state"),
        KnownVersion::new(139, "webhooks"),
        KnownVersion::new(138, "saga-abandoned-state"),
        KnownVersion::new(137, "oximeter-read-policy"),
        KnownVersion::new(136, "do-not-provision-flag-for-crucible-dataset"),
        KnownVersion::new(135, "blueprint-zone-image-source"),
        KnownVersion::new(134, "crucible-agent-reservation-overhead"),
        KnownVersion::new(133, "delete-defunct-reservations"),
        KnownVersion::new(132, "bp-omicron-zone-filesystem-pool-not-null"),
        KnownVersion::new(131, "tuf-generation"),
        KnownVersion::new(130, "bp-sled-agent-generation"),
        KnownVersion::new(129, "create-target-release"),
        KnownVersion::new(128, "sled-resource-for-vmm"),
        KnownVersion::new(127, "bp-disk-disposition-expunged-cleanup"),
        KnownVersion::new(126, "affinity"),
        KnownVersion::new(125, "blueprint-disposition-expunged-cleanup"),
        KnownVersion::new(124, "support-read-only-region-replacement"),
        KnownVersion::new(123, "vpc-subnet-contention"),
        KnownVersion::new(122, "tuf-artifact-replication"),
        KnownVersion::new(121, "dataset-to-crucible-dataset"),
        KnownVersion::new(120, "rendezvous-debug-dataset"),
        KnownVersion::new(119, "tuf-artifact-key-uuid"),
        KnownVersion::new(118, "support-bundles"),
        KnownVersion::new(117, "add-completing-and-new-region-volume"),
        KnownVersion::new(116, "bp-physical-disk-disposition"),
        KnownVersion::new(115, "inv-omicron-physical-disks-generation"),
        KnownVersion::new(114, "crucible-ref-count-records"),
        KnownVersion::new(113, "add-tx-eq"),
        KnownVersion::new(112, "blueprint-dataset"),
        KnownVersion::new(111, "drop-omicron-zone-underlay-address"),
        KnownVersion::new(110, "clickhouse-policy"),
        KnownVersion::new(109, "inv-clickhouse-keeper-membership"),
        KnownVersion::new(108, "internet-gateway"),
        KnownVersion::new(107, "add-instance-boot-disk"),
        KnownVersion::new(106, "dataset-kinds-update"),
        KnownVersion::new(105, "inventory-nvme-firmware"),
        KnownVersion::new(104, "lookup-bgp-config-indexes"),
        KnownVersion::new(103, "lookup-instances-by-state-index"),
        KnownVersion::new(102, "add-instance-auto-restart-cooldown"),
        KnownVersion::new(101, "auto-restart-policy-v2"),
        KnownVersion::new(100, "add-instance-last-auto-restarted-timestamp"),
        KnownVersion::new(99, "blueprint-add-clickhouse-tables"),
        KnownVersion::new(98, "oximeter-add-time-expunged"),
        KnownVersion::new(97, "lookup-region-snapshot-by-region-id"),
        KnownVersion::new(96, "inv-dataset"),
        KnownVersion::new(95, "turn-boot-on-fault-into-auto-restart"),
        KnownVersion::new(94, "put-back-creating-vmm-state"),
        KnownVersion::new(93, "dataset-kinds-zone-and-debug"),
        KnownVersion::new(92, "lldp-link-config-nullable"),
        KnownVersion::new(91, "add-management-gateway-producer-kind"),
        KnownVersion::new(90, "lookup-bgp-config-by-asn"),
        KnownVersion::new(89, "collapse_lldp_settings"),
        KnownVersion::new(88, "route-local-pref"),
        KnownVersion::new(87, "add-clickhouse-server-enum-variants"),
        KnownVersion::new(86, "snapshot-replacement"),
        KnownVersion::new(85, "add-migrations-by-time-created-index"),
        KnownVersion::new(84, "region-read-only"),
        KnownVersion::new(83, "dataset-address-optional"),
        KnownVersion::new(82, "region-port"),
        KnownVersion::new(81, "add-nullable-filesystem-pool"),
        KnownVersion::new(80, "add-instance-id-to-migrations"),
        KnownVersion::new(79, "nic-spoof-allow"),
        KnownVersion::new(78, "vpc-subnet-routing"),
        KnownVersion::new(77, "remove-view-for-v2p-mappings"),
        KnownVersion::new(76, "lookup-region-snapshot-by-snapshot-id"),
        KnownVersion::new(75, "add-cockroach-zone-id-to-node-id"),
        KnownVersion::new(74, "add-migration-table"),
        KnownVersion::new(73, "add-vlan-to-uplink"),
        KnownVersion::new(72, "fix-provisioning-counters"),
        KnownVersion::new(71, "add-saga-unwound-vmm-state"),
        KnownVersion::new(70, "separate-instance-and-vmm-states"),
        KnownVersion::new(69, "expose-stage0"),
        KnownVersion::new(68, "filter-v2p-mapping-by-instance-state"),
        KnownVersion::new(67, "add-instance-updater-lock"),
        KnownVersion::new(66, "blueprint-crdb-preserve-downgrade"),
        KnownVersion::new(65, "region-replacement"),
        KnownVersion::new(64, "add-view-for-v2p-mappings"),
        KnownVersion::new(63, "remove-producer-base-route-column"),
        KnownVersion::new(62, "allocate-subnet-decommissioned-sleds"),
        KnownVersion::new(61, "blueprint-add-sled-state"),
        KnownVersion::new(60, "add-lookup-vmm-by-sled-id-index"),
        KnownVersion::new(59, "enforce-first-as-default"),
        KnownVersion::new(58, "insert-default-allowlist"),
        KnownVersion::new(57, "add-allowed-source-ips"),
        KnownVersion::new(56, "bgp-oxpop-features"),
        KnownVersion::new(55, "add-lookup-sled-by-policy-and-state-index"),
        KnownVersion::new(54, "blueprint-add-external-ip-id"),
        KnownVersion::new(53, "drop-service-table"),
        KnownVersion::new(52, "blueprint-physical-disk"),
        KnownVersion::new(51, "blueprint-disposition-column"),
        KnownVersion::new(50, "add-lookup-disk-by-volume-id-index"),
        KnownVersion::new(49, "physical-disk-state-and-policy"),
        KnownVersion::new(48, "add-metrics-producers-time-modified-index"),
        KnownVersion::new(47, "add-view-for-bgp-peer-configs"),
        KnownVersion::new(46, "first-named-migration"),
        // The first many schema versions only vary by major or patch number and
        // their path is predictable based on the version number.  (This was
        // historically a problem because two pull requests both adding a new
        // schema version might merge cleanly but produce an invalid result.)
        KnownVersion::legacy(45, 0),
        KnownVersion::legacy(44, 0),
        KnownVersion::legacy(43, 0),
        KnownVersion::legacy(42, 0),
        KnownVersion::legacy(41, 0),
        KnownVersion::legacy(40, 0),
        KnownVersion::legacy(39, 0),
        KnownVersion::legacy(38, 0),
        KnownVersion::legacy(37, 1),
        KnownVersion::legacy(37, 0),
        KnownVersion::legacy(36, 0),
        KnownVersion::legacy(35, 0),
        KnownVersion::legacy(34, 0),
        KnownVersion::legacy(33, 1),
        KnownVersion::legacy(33, 0),
        KnownVersion::legacy(32, 0),
        KnownVersion::legacy(31, 0),
        KnownVersion::legacy(30, 0),
        KnownVersion::legacy(29, 0),
        KnownVersion::legacy(28, 0),
        KnownVersion::legacy(27, 0),
        KnownVersion::legacy(26, 0),
        KnownVersion::legacy(25, 0),
        KnownVersion::legacy(24, 0),
        KnownVersion::legacy(23, 1),
        KnownVersion::legacy(23, 0),
        KnownVersion::legacy(22, 0),
        KnownVersion::legacy(21, 0),
        KnownVersion::legacy(20, 0),
        KnownVersion::legacy(19, 0),
        KnownVersion::legacy(18, 0),
        KnownVersion::legacy(17, 0),
        KnownVersion::legacy(16, 0),
        KnownVersion::legacy(15, 0),
        KnownVersion::legacy(14, 0),
        KnownVersion::legacy(13, 0),
        KnownVersion::legacy(12, 0),
        KnownVersion::legacy(11, 0),
        KnownVersion::legacy(10, 0),
        KnownVersion::legacy(9, 0),
        KnownVersion::legacy(8, 0),
        KnownVersion::legacy(7, 0),
        KnownVersion::legacy(6, 0),
        KnownVersion::legacy(5, 0),
        KnownVersion::legacy(4, 0),
        KnownVersion::legacy(3, 3),
        KnownVersion::legacy(3, 2),
        KnownVersion::legacy(3, 1),
        KnownVersion::legacy(3, 0),
        KnownVersion::legacy(2, 0),
        KnownVersion::legacy(1, 0),
    ]
});

/// The earliest supported schema version.
pub const EARLIEST_SUPPORTED_VERSION: Version = Version::new(1, 0, 0);

/// The version where "db_metadata_nexus" was added.
pub const DB_METADATA_NEXUS_SCHEMA_VERSION: Version = Version::new(185, 0, 0);

/// The last schema version that is allowed to have multiple DDL statements
/// per migration file.
///
/// Versions after this one must have at most one DDL per file, which enables
/// us to verify that CockroachDB async backfill operations (CREATE INDEX,
/// ALTER TABLE ADD CONSTRAINT, etc.) complete successfully before moving on
/// to the next migration step.
const LAST_MULTI_DDL_VERSION: u64 = 220;

/// A validated SQL identifier (or enum variant value) that is safe to
/// interpolate into SQL strings.
///
/// Permits only `[a-zA-Z0-9_]` characters, which covers all valid
/// SQL identifiers and our enum variant values.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlIdentifier(String);

impl SqlIdentifier {
    pub fn new(s: impl Into<String>) -> Result<Self, anyhow::Error> {
        let s = s.into();
        if s.is_empty() {
            bail!("SQL identifier must not be empty");
        }
        if !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            bail!("SQL identifier contains invalid characters: {s:?}");
        }
        Ok(SqlIdentifier(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SqlIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A schema-changing (DDL) operation detected in a migration file.
///
/// Used to:
/// 1. Enforce at-most-one DDL per migration file (for versions >
///    `LAST_MULTI_DDL_VERSION`)
/// 2. Generate verification queries for operations that involve async
///    backfill in CockroachDB (e.g., CREATE INDEX, ADD CONSTRAINT)
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaChangeInfo {
    /// `ALTER TABLE ... ADD COLUMN` where CockroachDB's
    /// `ColumnNeedsBackfill` returns true.
    ///
    /// This covers:
    /// - NOT NULL columns (with or without a default)
    /// - Nullable columns with a non-NULL default expression
    /// - Stored computed columns (`GENERATED ALWAYS AS (expr) STORED`)
    ///
    /// CockroachDB runs an async column backfill job to write the
    /// default/computed value (or validate the NOT NULL constraint)
    /// for all existing rows.
    AddColumnNeedsBackfill {
        table_name: SqlIdentifier,
        column_name: SqlIdentifier,
    },

    /// `ALTER TABLE ... ALTER COLUMN ... SET NOT NULL`
    ///
    /// CockroachDB adds a NOT-NULL check constraint in `Validating`
    /// state, then runs an async validation job that scans the table
    /// to verify no NULL values exist.
    AlterColumnSetNotNull {
        table_name: SqlIdentifier,
        column_name: SqlIdentifier,
    },

    /// `ALTER TABLE ... ADD CONSTRAINT ...` (CHECK, UNIQUE, FK)
    ///
    /// CockroachDB adds the constraint in `Validating` state, then
    /// runs an async validation job that scans existing rows to
    /// verify they satisfy the constraint.
    AlterTableAddConstraint {
        table_name: SqlIdentifier,
        constraint_name: SqlIdentifier,
    },

    /// `CREATE [UNIQUE] INDEX ...`
    ///
    /// CockroachDB runs an async backfill job to populate the index
    /// over all existing rows. See issue #9866.
    CreateIndex { table_name: SqlIdentifier, index_name: SqlIdentifier },

    /// Any DDL that does not involve an async backfill or validation
    /// job (e.g., CREATE TABLE, DROP TABLE, ALTER TYPE ADD VALUE).
    OtherDdl,
}

impl SchemaChangeInfo {
    /// Returns a verification SQL query for backfill-prone operations.
    ///
    /// Only operations that involve an async data backfill or validation in
    /// CockroachDB need verification. Returns `None` for metadata-only
    /// operations.
    ///
    /// The returned query uses the `SELECT CAST(IF(...) AS BOOL)` pattern:
    /// it succeeds silently when the condition is met and throws an error
    /// (causing `batch_execute_async` to fail) when it is not.
    pub fn verification_query(&self) -> Option<String> {
        match self {
            SchemaChangeInfo::CreateIndex { table_name, index_name } => {
                Some(format!(
                    "SELECT CAST(\
                        IF(\
                            (\
                                SELECT true WHERE EXISTS (\
                                    SELECT index_name \
                                    FROM omicron.crdb_internal.table_indexes \
                                    WHERE descriptor_name = '{table_name}' \
                                    AND index_name = '{index_name}'\
                                )\
                            ),\
                            'true',\
                            'Schema change verification failed: \
                            index {index_name} on table {table_name} \
                            does not exist'\
                        ) AS BOOL\
                    );"
                ))
            }
            SchemaChangeInfo::AlterColumnSetNotNull {
                table_name,
                column_name,
            } => Some(format!(
                "SELECT CAST(\
                    IF(\
                        (\
                            SELECT true WHERE EXISTS (\
                                SELECT column_name \
                                FROM information_schema.columns \
                                WHERE table_schema = 'public' \
                                AND table_name = '{table_name}' \
                                AND column_name = '{column_name}' \
                                AND is_nullable = 'NO'\
                            )\
                        ),\
                        'true',\
                        'Schema change verification failed: \
                        column {column_name} on table {table_name} \
                        is still nullable'\
                    ) AS BOOL\
                );"
            )),
            SchemaChangeInfo::AddColumnNeedsBackfill {
                table_name,
                column_name,
            } => Some(format!(
                "SELECT CAST(\
                    IF(\
                        (\
                            SELECT true WHERE EXISTS (\
                                SELECT column_name \
                                FROM information_schema.columns \
                                WHERE table_schema = 'public' \
                                AND table_name = '{table_name}' \
                                AND column_name = '{column_name}'\
                            )\
                        ),\
                        'true',\
                        'Schema change verification failed: \
                        column {column_name} on table {table_name} \
                        does not exist \
                        (backfill may not have completed)'\
                    ) AS BOOL\
                );"
            )),
            SchemaChangeInfo::AlterTableAddConstraint {
                table_name,
                constraint_name,
            } => Some(format!(
                "SELECT CAST(\
                    IF(\
                        (\
                            SELECT true WHERE EXISTS (\
                                SELECT constraint_name \
                                FROM information_schema.table_constraints \
                                WHERE table_schema = 'public' \
                                AND table_name = '{table_name}' \
                                AND constraint_name = '{constraint_name}'\
                            )\
                        ),\
                        'true',\
                        'Schema change verification failed: \
                        constraint {constraint_name} not found \
                        on table {table_name}'\
                    ) AS BOOL\
                );"
            )),
            // OtherDdl operations are metadata-only or synchronous.
            SchemaChangeInfo::OtherDdl => None,
        }
    }
}

/// The result of classifying all DDL in a single migration file.
#[derive(Debug, Clone)]
pub struct ClassifiedDdl {
    /// Individual schema-change operations detected in the file.
    ///
    /// A single DDL statement may produce multiple entries (e.g., an
    /// `ALTER TABLE ... DROP CONSTRAINT ..., ADD CONSTRAINT ...`
    /// produces two entries).
    pub changes: Vec<SchemaChangeInfo>,
    /// Number of distinct DDL **statements** in the file.
    ///
    /// A single `ALTER TABLE` with multiple operations counts as 1.
    pub statement_count: usize,
}

/// Extract the last identifier from an `ObjectName` (e.g., `omicron.public.foo` → `foo`).
fn last_ident(
    name: &sqlparser::ast::ObjectName,
) -> anyhow::Result<SqlIdentifier> {
    let s = name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.clone())
        .with_context(|| format!("empty ObjectName: {name}"))?;
    SqlIdentifier::new(s)
}

/// Extract a constraint name from a `TableConstraint`, if one is present.
fn constraint_name(
    tc: &TableConstraint,
) -> Option<anyhow::Result<SqlIdentifier>> {
    let name = match tc {
        TableConstraint::Unique(c) => c.name.as_ref(),
        TableConstraint::PrimaryKey(c) => c.name.as_ref(),
        TableConstraint::ForeignKey(c) => c.name.as_ref(),
        TableConstraint::Check(c) => c.name.as_ref(),
        // Other variants (Index, FulltextOrSpatial) don't carry a
        // constraint name.
        _ => return None,
    };
    name.map(|i| SqlIdentifier::new(i.value.clone()))
}

/// Pre-process a single DDL statement so that `sqlparser` (PostgreSQL
/// dialect) can parse it.
///
/// Only applies the regexes needed for CREATE INDEX and ALTER TABLE
/// statements. The original SQL is always what gets executed against
/// the database; this preprocessing is only for classification.
fn preprocess_for_sqlparser(stmt: &str) -> String {
    let mut result = stmt.to_string();

    // Strip STORING(...) clauses from CREATE INDEX.
    let storing_re = Regex::new(r"(?is)\bSTORING\s*\([^)]*\)").unwrap();
    result = storing_re.replace_all(&result, "").to_string();

    // Replace `<type> ARRAY` with `<type>[]` in column definitions.
    // Both are valid PostgreSQL, but sqlparser only handles the
    // bracket notation.  We match `ARRAY` followed by `,` or end-of-
    // definition (not `ARRAY[` which is the array literal syntax).
    let type_array_re = Regex::new(r"(?i)(\w+)\s+ARRAY\s*([,)\n])").unwrap();
    result = type_array_re.replace_all(&result, "$1[] $2").to_string();

    // Strip IF NOT EXISTS from ADD CONSTRAINT.
    // CockroachDB supports this but standard PostgreSQL / sqlparser
    // does not.
    let add_constraint_ine_re =
        Regex::new(r"(?i)(ADD\s+CONSTRAINT)\s+IF\s+NOT\s+EXISTS").unwrap();
    result = add_constraint_ine_re.replace_all(&result, "$1").to_string();

    result
}

/// Strip SQL comments (`-- ...` and `/* ... */`) from the input.
fn strip_sql_comments(sql: &str) -> String {
    // Strip line comments.
    let line_comment_re = Regex::new(r"--[^\n]*").unwrap();
    let result = line_comment_re.replace_all(sql, "");
    // Strip block comments.
    let block_comment_re = Regex::new(r"(?s)/\*.*?\*/").unwrap();
    block_comment_re.replace_all(&result, "").to_string()
}

/// Parse a SQL migration file and classify all schema-changing (DDL)
/// statements it contains.
///
/// `label` is a human-readable identifier for error messages (typically
/// the filename).
///
/// DML statements (INSERT, UPDATE, DELETE, SELECT, SET, etc.) are
/// ignored — only DDL is returned.  Only CREATE INDEX and ALTER TABLE
/// statements are parsed with `sqlparser`; all other DDL is classified
/// as `OtherDdl` without parsing.
pub fn classify_sql_statements(
    sql: &str,
    label: &str,
) -> Result<ClassifiedDdl, anyhow::Error> {
    let without_comments = strip_sql_comments(sql);

    // Skip DML, session settings, and other non-DDL statements.
    let dml_start = Regex::new(
        r"(?is)^\s*(?:INSERT|UPDATE|DELETE|SELECT|SET|WITH|EXPLAIN|SHOW|USE|BEGIN|COMMIT|ROLLBACK|GRANT|REVOKE)\b",
    )
    .unwrap();

    // Detect unparseable CockroachDB-specific ALTER PRIMARY KEY.
    let alter_table_pk_re = Regex::new(
        r"(?is)^\s*ALTER\s+TABLE\s+\S+\s+ALTER\s+PRIMARY\s+KEY\s+USING\s+COLUMNS",
    )
    .unwrap();

    // Detect statements that need sqlparser for verification extraction.
    let create_index_re =
        Regex::new(r"(?is)^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\b").unwrap();
    let alter_table_re = Regex::new(r"(?is)^\s*ALTER\s+TABLE\b").unwrap();

    let mut changes = Vec::new();
    let mut statement_count = 0;

    for raw_stmt in without_comments.split(';') {
        let trimmed = raw_stmt.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Skip DML, session settings, and other non-DDL statements.
        if dml_start.is_match(trimmed) {
            continue;
        }

        statement_count += 1;

        let might_have_async_backfill = create_index_re.is_match(trimmed)
            || alter_table_re.is_match(trimmed);

        if !might_have_async_backfill {
            changes.push(SchemaChangeInfo::OtherDdl);
            continue;
        }

        // CockroachDB-specific ALTER PRIMARY KEY USING COLUMNS
        // can't be parsed by sqlparser; just mark as OtherDdl.
        if alter_table_pk_re.is_match(trimmed) {
            changes.push(SchemaChangeInfo::OtherDdl);
            continue;
        }

        // CREATE INDEX and ALTER TABLE may produce verification
        // queries, so we parse them with sqlparser.
        let preprocessed = preprocess_for_sqlparser(trimmed);
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, &preprocessed)
            .with_context(|| format!("failed to parse SQL in {label}"))?;

        for stmt in &stmts {
            match stmt {
                Statement::CreateIndex(ci) => {
                    let idx_name =
                        last_ident(ci.name.as_ref().with_context(|| {
                            format!(
                                "CREATE INDEX without a name \
                                     in {label}"
                            )
                        })?)?;
                    let tbl = last_ident(&ci.table_name)?;
                    changes.push(SchemaChangeInfo::CreateIndex {
                        table_name: tbl,
                        index_name: idx_name,
                    });
                }
                Statement::AlterTable(at) => {
                    let tbl = last_ident(&at.name)?;
                    for op in &at.operations {
                        match op {
                            AlterTableOperation::AlterColumn {
                                column_name,
                                op: AlterColumnOperation::SetNotNull,
                            } => {
                                changes.push(
                                    SchemaChangeInfo::AlterColumnSetNotNull {
                                        table_name: tbl.clone(),
                                        column_name: SqlIdentifier::new(
                                            column_name.value.clone(),
                                        )?,
                                    },
                                );
                            }
                            AlterTableOperation::AddConstraint {
                                constraint: tc,
                                ..
                            } => {
                                let cname = constraint_name(tc).with_context(
                                    || {
                                        format!(
                                            "ADD CONSTRAINT without \
                                             a name on table {tbl} \
                                             in {label}"
                                        )
                                    },
                                )??;
                                changes.push(
                                    SchemaChangeInfo::AlterTableAddConstraint {
                                        table_name: tbl.clone(),
                                        constraint_name: cname,
                                    },
                                );
                            }
                            AlterTableOperation::AddColumn {
                                column_def,
                                ..
                            } => {
                                let is_not_null =
                                    column_def.options.iter().any(|opt| {
                                        matches!(
                                            opt.option,
                                            ColumnOption::NotNull
                                        )
                                    });
                                let has_non_null_default =
                                    column_def.options.iter().any(|opt| {
                                        matches!(
                                            &opt.option,
                                            ColumnOption::Default(expr)
                                                if !matches!(
                                                    expr,
                                                    Expr::Value(ValueWithSpan {
                                                        value: Value::Null,
                                                        ..
                                                    })
                                                )
                                        )
                                    });
                                let is_stored_computed =
                                    column_def.options.iter().any(|opt| {
                                        matches!(
                                            &opt.option,
                                            ColumnOption::Generated {
                                                generation_expr: Some(_),
                                                generation_expr_mode: Some(
                                                    GeneratedExpressionMode::Stored
                                                ),
                                                ..
                                            }
                                        )
                                    });
                                if is_not_null
                                    || has_non_null_default
                                    || is_stored_computed
                                {
                                    changes.push(
                                        SchemaChangeInfo::AddColumnNeedsBackfill {
                                            table_name: tbl.clone(),
                                            column_name: SqlIdentifier::new(
                                                column_def
                                                    .name
                                                    .value
                                                    .clone(),
                                            )?,
                                        },
                                    );
                                } else {
                                    changes.push(SchemaChangeInfo::OtherDdl);
                                }
                            }
                            _ => {
                                changes.push(SchemaChangeInfo::OtherDdl);
                            }
                        }
                    }
                }
                // Shouldn't normally happen (we only parse CREATE
                // INDEX and ALTER TABLE), but handle gracefully.
                _ => {
                    changes.push(SchemaChangeInfo::OtherDdl);
                }
            }
        }
    }

    Ok(ClassifiedDdl { changes, statement_count })
}

/// Describes one version of the database schema
#[derive(Debug, Clone)]
struct KnownVersion {
    /// All versions have an associated SemVer.  We only use the major number in
    /// terms of determining compatibility.
    semver: Version,

    /// Path relative to the root of the schema ("schema/crdb" in the root of
    /// this repo) where this version's update SQL files are stored
    relative_path: String,
}

impl KnownVersion {
    /// Generate a `KnownVersion` for a new schema version
    ///
    /// `major` should be the next available integer (one more than the previous
    /// version's major number).
    ///
    /// `relative_path` is the path relative to "schema/crdb" (from the root of
    /// this repository) where the SQL files live that will update the schema
    /// from the previous version to this version.
    fn new(major: u64, relative_path: &str) -> KnownVersion {
        let semver = Version::new(major, 0, 0);
        KnownVersion { semver, relative_path: relative_path.to_owned() }
    }

    /// Generate a `KnownVersion` for a version that predates the current
    /// directory naming scheme
    ///
    /// These versions varied in both major and patch numbers and the path to
    /// their SQL files was predictable based solely on the version.
    ///
    /// **This should not be used for new schema versions.**
    fn legacy(major: u64, patch: u64) -> KnownVersion {
        let semver = Version::new(major, 0, patch);
        let relative_path = semver.to_string();
        KnownVersion { semver, relative_path }
    }
}

impl std::fmt::Display for KnownVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.semver.fmt(f)
    }
}

/// Load and inspect the set of all known schema versions
#[derive(Debug, Clone)]
pub struct AllSchemaVersions {
    versions: BTreeMap<Version, SchemaVersion>,
}

impl AllSchemaVersions {
    /// Load the set of all known schema versions from the given directory tree
    ///
    /// The directory should contain exactly one directory for each version.
    /// Each version's directory should contain the SQL files that carry out
    /// schema migration from the previous version.  See schema/crdb/README.adoc
    /// for details.
    pub fn load(
        schema_directory: &Utf8Path,
    ) -> Result<AllSchemaVersions, anyhow::Error> {
        Self::load_known_versions(schema_directory, KNOWN_VERSIONS.iter())
    }

    /// Load a specific set of known schema versions using the legacy
    /// conventions from the given directory tree
    ///
    /// This is only provided for certain integration tests.
    #[doc(hidden)]
    pub fn load_specific_legacy_versions<'a>(
        schema_directory: &Utf8Path,
        versions: impl Iterator<Item = &'a Version>,
    ) -> Result<AllSchemaVersions, anyhow::Error> {
        let known_versions: Vec<_> = versions
            .map(|v| {
                assert_eq!(v.minor, 0);
                KnownVersion::legacy(v.major, v.patch)
            })
            .collect();

        Self::load_known_versions(schema_directory, known_versions.iter())
    }

    fn load_known_versions<'a>(
        schema_directory: &Utf8Path,
        known_versions: impl Iterator<Item = &'a KnownVersion>,
    ) -> Result<AllSchemaVersions, anyhow::Error> {
        let mut versions = BTreeMap::new();
        for known_version in known_versions {
            let version_path =
                schema_directory.join(&known_version.relative_path);
            let schema_version = SchemaVersion::load_from_directory(
                known_version.semver.clone(),
                &version_path,
            )
            .with_context(|| {
                format!(
                    "loading schema version {} from {:?}",
                    known_version.semver, schema_directory,
                )
            })?;

            versions.insert(known_version.semver.clone(), schema_version);
        }

        Ok(AllSchemaVersions { versions })
    }

    /// Iterate over the set of all known schema versions in order starting with
    /// the earliest supported version
    pub fn iter_versions(&self) -> impl Iterator<Item = &SchemaVersion> {
        self.versions.values()
    }

    /// Return whether `version` is a known schema version
    pub fn contains_version(&self, version: &Version) -> bool {
        self.versions.contains_key(version)
    }

    /// Iterate over the known schema versions within `bounds`
    ///
    /// This is generally used to iterate over all the schema versions between
    /// two specific versions.
    pub fn versions_range<R>(
        &self,
        bounds: R,
    ) -> impl Iterator<Item = &'_ SchemaVersion>
    where
        R: std::ops::RangeBounds<Version>,
    {
        self.versions.range(bounds).map(|(_, v)| v)
    }
}

/// Describes a single version of the schema, including the SQL steps to get
/// from the previous version to the current one
#[derive(Debug, Clone)]
pub struct SchemaVersion {
    semver: Version,
    upgrade_from_previous: Vec<SchemaUpgradeStep>,
}

impl SchemaVersion {
    /// Reads a "version directory" and reads all SQL changes into a result Vec.
    ///
    /// Files that do not begin with "up" and end with ".sql" are ignored. The
    /// collection of `up*.sql` files must fall into one of these two
    /// conventions:
    ///
    /// * "up.sql" with no other files
    /// * "up1.sql", "up2.sql", ..., beginning from 1, optionally with leading
    ///   zeroes (e.g., "up01.sql", "up02.sql", ...). There is no maximum value,
    ///   but there may not be any gaps (e.g., if "up2.sql" and "up4.sql" exist,
    ///   so must "up3.sql") and there must not be any repeats (e.g., if
    ///   "up1.sql" exists, "up01.sql" must not exist).
    ///
    /// Any violation of these two rules will result in an error. Collections of
    /// the second form (`up1.sql`, ...) will be sorted numerically.
    fn load_from_directory(
        semver: Version,
        directory: &Utf8Path,
    ) -> Result<SchemaVersion, anyhow::Error> {
        let mut up_sqls = vec![];
        let entries = directory
            .read_dir_utf8()
            .with_context(|| format!("Failed to readdir {directory}"))?;
        for entry in entries {
            let entry = entry.with_context(|| {
                format!("Reading {directory:?}: invalid entry")
            })?;
            let pathbuf = entry.into_path();

            // Ensure filename ends with ".sql"
            if pathbuf.extension() != Some("sql") {
                continue;
            }

            // Ensure filename begins with "up", and extract anything in between
            // "up" and ".sql".
            let Some(remaining_filename) = pathbuf
                .file_stem()
                .and_then(|file_stem| file_stem.strip_prefix("up"))
            else {
                continue;
            };

            // Ensure the remaining filename is either empty (i.e., the filename
            // is exactly "up.sql") or parseable as an unsigned integer. We give
            // "up.sql" the "up_number" 0 (checked in the loop below), and
            // require any other number to be nonzero.
            if remaining_filename.is_empty() {
                up_sqls.push((0, pathbuf));
            } else {
                let Ok(up_number) = remaining_filename.parse::<u64>() else {
                    bail!(
                        "invalid filename (non-numeric `up*.sql`): {pathbuf}",
                    );
                };
                ensure!(
                    up_number != 0,
                    "invalid filename (`up*.sql` numbering must start at 1): \
                     {pathbuf}",
                );
                up_sqls.push((up_number, pathbuf));
            }
        }
        up_sqls.sort();

        // Validate that we have a reasonable sequence of `up*.sql` numbers.
        match up_sqls.as_slice() {
            [] => bail!("no `up*.sql` files found"),
            [(up_number, path)] => {
                // For a single file, we allow either `up.sql` (keyed as
                // up_number=0) or `up1.sql`; reject any higher number.
                ensure!(
                    *up_number <= 1,
                    "`up*.sql` numbering must start at 1: found first file \
                     {path}"
                );
            }
            _ => {
                for (i, (up_number, path)) in up_sqls.iter().enumerate() {
                    // We have 2 or more `up*.sql`; they should be numbered
                    // exactly 1..=up_sqls.len().
                    if i as u64 + 1 != *up_number {
                        // We know we have at least two elements, so report an
                        // error referencing either the next item (if we're
                        // first) or the previous item (if we're not first).
                        let (path_a, path_b) = if i == 0 {
                            let (_, next_path) = &up_sqls[1];
                            (path, next_path)
                        } else {
                            let (_, prev_path) = &up_sqls[i - 1];
                            (prev_path, path)
                        };
                        bail!("invalid `up*.sql` sequence: {path_a}, {path_b}");
                    }
                }
            }
        }

        // This collection of `up*.sql` files is valid.  Read them all, in
        // order, and classify DDL statements.
        let mut steps = vec![];
        for (_, path) in up_sqls.into_iter() {
            let sql = std::fs::read_to_string(&path)
                .with_context(|| format!("Cannot read {path}"))?;
            // unwrap: `file_name()` is documented to return `None` only when
            // the path is `..`.  But we got this path from reading the
            // directory, and that process explicitly documents that it skips
            // `..`.
            let label = path.file_name().unwrap().to_string();

            // Classify DDL statements in the file.
            let classified = classify_sql_statements(&sql, &label)
                .with_context(|| {
                    format!(
                        "migration file {label} in version {semver} \
                         must be parseable"
                    )
                })?;
            let schema_changes = if classified.statement_count > 1 {
                if semver.major > LAST_MULTI_DDL_VERSION {
                    // New migrations must have at most one DDL.
                    bail!(
                        "migration file {label} in version \
                         {semver} contains {} DDL statements, \
                         but at most 1 is allowed for versions \
                         after {LAST_MULTI_DDL_VERSION}",
                        classified.statement_count,
                    );
                }
                // Old versions may have multiple DDL — skip
                // verification for those files entirely.
                Vec::new()
            } else {
                classified.changes
            };

            steps.push(SchemaUpgradeStep { label, sql, schema_changes });
        }

        Ok(SchemaVersion { semver, upgrade_from_previous: steps })
    }

    /// Returns the semver for this schema version
    pub fn semver(&self) -> &Version {
        &self.semver
    }

    /// Returns true if this schema version is the one that the current program
    /// thinks is the latest (current) one
    pub fn is_current_software_version(&self) -> bool {
        self.semver == SCHEMA_VERSION
    }

    /// Iterate over the SQL steps required to update the database schema from
    /// the previous version to this one
    pub fn upgrade_steps(&self) -> impl Iterator<Item = &SchemaUpgradeStep> {
        self.upgrade_from_previous.iter()
    }
}

impl std::fmt::Display for SchemaVersion {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> Result<(), std::fmt::Error> {
        self.semver.fmt(f)
    }
}

/// Describes a single file containing a schema change, as SQL.
#[derive(Debug, Clone)]
pub struct SchemaUpgradeStep {
    label: String,
    sql: String,
    /// DDL operations detected in this file.
    ///
    /// New migrations (versions > `LAST_MULTI_DDL_VERSION`)
    /// are limited to at most one DDL **statement** per file, but a single
    /// statement may contain multiple operations (e.g., `ALTER TABLE ...
    /// DROP CONSTRAINT ..., ADD CONSTRAINT ...`).  Older migrations that
    /// had multiple DDL statements get an empty vec (verification skipped).
    schema_changes: Vec<SchemaChangeInfo>,
}

impl SchemaUpgradeStep {
    /// Returns a human-readable name for this step (the name of the file it
    /// came from)
    pub fn label(&self) -> &str {
        self.label.as_ref()
    }

    /// Returns the actual SQL to execute for this step
    pub fn sql(&self) -> &str {
        self.sql.as_ref()
    }

    /// Returns the DDL operations detected in this migration file.
    ///
    /// A single DDL statement may produce multiple entries (e.g.,
    /// `ALTER TABLE ... DROP CONSTRAINT ..., ADD CONSTRAINT ...`).
    /// Empty for old migrations with multiple DDL statements (verification
    /// is skipped for those).
    pub fn schema_changes(&self) -> &[SchemaChangeInfo] {
        &self.schema_changes
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use camino_tempfile::Utf8TempDir;

    #[test]
    fn test_known_versions() {
        if let Err(error) = verify_known_versions(
            // The real list is defined in reverse order for developer
            // convenience so we reverse it before processing.
            KNOWN_VERSIONS.iter().rev(),
            &EARLIEST_SUPPORTED_VERSION,
            &SCHEMA_VERSION,
            // Versions after 45 obey our modern, stricter rules.
            45,
        ) {
            panic!("problem with static configuration: {:#}", error);
        }
    }

    // (Test the test function)
    #[test]
    fn test_verify() {
        // EARLIEST_SUPPORTED_VERSION is somehow wrong
        let error = verify_known_versions(
            [&KnownVersion::legacy(2, 0), &KnownVersion::legacy(3, 0)],
            &Version::new(1, 0, 0),
            &Version::new(3, 0, 0),
            100,
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "EARLIEST_SUPPORTED_VERSION is not the earliest in KNOWN_VERSIONS"
        );

        // SCHEMA_VERSION was not updated
        let error = verify_known_versions(
            [&KnownVersion::legacy(1, 0), &KnownVersion::legacy(2, 0)],
            &Version::new(1, 0, 0),
            &Version::new(1, 0, 0),
            100,
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "latest KNOWN_VERSION is 2.0.0, but SCHEMA_VERSION is 1.0.0"
        );

        // Latest version was duplicated instead of bumped (legacy)
        let error = verify_known_versions(
            [
                &KnownVersion::legacy(1, 0),
                &KnownVersion::legacy(2, 0),
                &KnownVersion::legacy(2, 0),
            ],
            &EARLIEST_SUPPORTED_VERSION,
            &Version::new(2, 0, 0),
            100,
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "KNOWN_VERSION 2.0.0 appears directly after 2.0.0, but is not later"
        );

        // Latest version was duplicated instead of bumped (modern case)
        let error = verify_known_versions(
            [
                &KnownVersion::legacy(1, 0),
                &KnownVersion::new(2, "dir1"),
                &KnownVersion::new(2, "dir2"),
            ],
            &EARLIEST_SUPPORTED_VERSION,
            &Version::new(2, 0, 0),
            100,
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "KNOWN_VERSION 2.0.0 appears directly after 2.0.0, but is not later"
        );

        // Version added out of order
        let error = verify_known_versions(
            [
                &KnownVersion::legacy(1, 0),
                &KnownVersion::legacy(2, 0),
                &KnownVersion::legacy(1, 3),
            ],
            &EARLIEST_SUPPORTED_VERSION,
            &Version::new(3, 0, 0),
            100,
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "KNOWN_VERSION 1.0.3 appears directly after 2.0.0, but is not later"
        );

        // Gaps are not allowed.
        let error = verify_known_versions(
            [
                &KnownVersion::legacy(1, 0),
                &KnownVersion::legacy(2, 0),
                &KnownVersion::legacy(4, 0),
            ],
            &EARLIEST_SUPPORTED_VERSION,
            &Version::new(4, 0, 0),
            100,
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "KNOWN_VERSION 4.0.0 appears directly after 2.0.0, but its major \
            number is neither the same nor one greater"
        );

        // For the strict case, the patch level can't be non-zero.  You can only
        // make this mistake by using `KnownVersion::legacy()` for a new
        // version.
        let error = verify_known_versions(
            [
                &KnownVersion::legacy(1, 0),
                &KnownVersion::legacy(2, 0),
                &KnownVersion::legacy(3, 2),
            ],
            &EARLIEST_SUPPORTED_VERSION,
            &Version::new(3, 0, 2),
            2,
        )
        .unwrap_err();
        assert_eq!(format!("{error:#}"), "new patch versions must be zero");

        // For the strict case, the directory name cannot contain the version at
        // all.  You can only make this mistake by using
        // `KnownVersion::legacy()` for a new version.
        let error = verify_known_versions(
            [
                &KnownVersion::legacy(1, 0),
                &KnownVersion::legacy(2, 0),
                &KnownVersion::legacy(3, 0),
            ],
            &EARLIEST_SUPPORTED_VERSION,
            &Version::new(3, 0, 0),
            2,
        )
        .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "the relative path for a version should not contain the \
            version itself"
        );
    }

    fn verify_known_versions<'a, I>(
        // list of known versions in order from earliest to latest
        known_versions: I,
        earliest: &Version,
        latest: &Version,
        min_strict_major: u64,
    ) -> Result<(), anyhow::Error>
    where
        I: IntoIterator<Item = &'a KnownVersion>,
    {
        let mut known_versions = known_versions.into_iter();

        // All known versions should be unique and increasing.
        let first =
            known_versions.next().expect("expected at least one KNOWN_VERSION");
        ensure!(
            first.semver == *earliest,
            "EARLIEST_SUPPORTED_VERSION is not the earliest in KNOWN_VERSIONS"
        );

        let mut prev = first;
        for v in known_versions {
            println!("checking known version: {} -> {}", prev, v);
            ensure!(
                v.semver > prev.semver,
                "KNOWN_VERSION {} appears directly after {}, but is not later",
                v,
                prev
            );

            // We currently make sure there are no gaps in the major number.
            // This is not strictly necessary but if this isn't true then it was
            // probably a mistake.
            //
            // It's allowed for the major numbers to be the same because a few
            // past schema versions only bumped the patch number for whatever
            // reason.
            ensure!(
                v.semver.major == prev.semver.major
                    || v.semver.major == prev.semver.major + 1,
                "KNOWN_VERSION {} appears directly after {}, but its major \
                number is neither the same nor one greater",
                v,
                prev
            );

            // We never allowed minor versions to be zero and it is not
            // currently possible to even construct one that had a non-zero
            // minor number.
            ensure!(v.semver.minor == 0, "new minor versions must be zero");

            // We changed things after version 45 to require that:
            //
            // (1) the major always be bumped (the minor and patch must be zero)
            // (2) users choose a unique directory name for the SQL files.  It
            //     would defeat the point if people used the semver for
            //
            // After version 45, we do not allow non-zero minor or patch
            // numbers.
            if v.semver.major > min_strict_major {
                ensure!(v.semver.patch == 0, "new patch versions must be zero");
                ensure!(
                    !v.relative_path.contains(&v.semver.to_string()),
                    "the relative path for a version should not contain the \
                    version itself"
                );
            }

            prev = v;
        }

        ensure!(
            prev.semver == *latest,
            "latest KNOWN_VERSION is {}, but SCHEMA_VERSION is {}",
            prev,
            latest
        );

        Ok(())
    }

    // Confirm that `SchemaVersion::load_from_directory()` rejects `up*.sql`
    // files where the `*` doesn't contain a positive integer.
    #[tokio::test]
    async fn test_reject_invalid_up_sql_names() {
        for (invalid_filename, error_prefix) in [
            ("upA.sql", "invalid filename (non-numeric `up*.sql`)"),
            ("up1a.sql", "invalid filename (non-numeric `up*.sql`)"),
            ("upaaa1.sql", "invalid filename (non-numeric `up*.sql`)"),
            ("up-3.sql", "invalid filename (non-numeric `up*.sql`)"),
            (
                "up0.sql",
                "invalid filename (`up*.sql` numbering must start at 1)",
            ),
            (
                "up00.sql",
                "invalid filename (`up*.sql` numbering must start at 1)",
            ),
            (
                "up000.sql",
                "invalid filename (`up*.sql` numbering must start at 1)",
            ),
        ] {
            let tempdir = Utf8TempDir::new().unwrap();
            let filename = tempdir.path().join(invalid_filename);
            _ = tokio::fs::File::create(&filename).await.unwrap();
            let maybe_schema = SchemaVersion::load_from_directory(
                Version::new(12, 0, 0),
                tempdir.path(),
            );
            match maybe_schema {
                Ok(upgrade) => {
                    panic!(
                        "unexpected success on {invalid_filename} \
                         (produced {upgrade:?})"
                    );
                }
                Err(error) => {
                    assert_eq!(
                        format!("{error:#}"),
                        format!("{error_prefix}: {filename}")
                    );
                }
            }
        }
    }

    // Confirm that `SchemaVersion::load_from_directory()` rejects a directory
    // with no appropriately-named files.
    #[tokio::test]
    async fn test_reject_no_up_sql_files() {
        for filenames in [
            &[] as &[&str],
            &["README.md"],
            &["foo.sql", "bar.sql"],
            &["up1sql", "up2sql"],
        ] {
            let tempdir = Utf8TempDir::new().unwrap();
            for filename in filenames {
                _ = tokio::fs::File::create(tempdir.path().join(filename))
                    .await
                    .unwrap();
            }

            let maybe_schema = SchemaVersion::load_from_directory(
                Version::new(12, 0, 0),
                tempdir.path(),
            );
            match maybe_schema {
                Ok(upgrade) => {
                    panic!(
                        "unexpected success on {filenames:?} \
                         (produced {upgrade:?})"
                    );
                }
                Err(error) => {
                    assert_eq!(
                        format!("{error:#}"),
                        "no `up*.sql` files found"
                    );
                }
            }
        }
    }

    // Confirm that `SchemaVersion::load_from_directory()` rejects collections
    // of `up*.sql` files with individually-valid names but that do not pass the
    // rules of the entire collection.
    #[tokio::test]
    async fn test_reject_invalid_up_sql_collections() {
        for invalid_filenames in [
            &["up.sql", "up1.sql"] as &[&str],
            &["up1.sql", "up01.sql"],
            &["up1.sql", "up3.sql"],
            &["up1.sql", "up2.sql", "up3.sql", "up02.sql"],
        ] {
            let tempdir = Utf8TempDir::new().unwrap();
            for filename in invalid_filenames {
                _ = tokio::fs::File::create(tempdir.path().join(filename))
                    .await
                    .unwrap();
            }

            let maybe_schema = SchemaVersion::load_from_directory(
                Version::new(12, 0, 0),
                tempdir.path(),
            );
            match maybe_schema {
                Ok(upgrade) => {
                    panic!(
                        "unexpected success on {invalid_filenames:?} \
                         (produced {upgrade:?})"
                    );
                }
                Err(error) => {
                    let message = format!("{error:#}");
                    assert!(
                        message.starts_with("invalid `up*.sql` sequence: "),
                        "message did not start with expected prefix: \
                         {message:?}"
                    );
                }
            }
        }
    }

    // Confirm that `SchemaVersion::load_from_directory()` accepts legal
    // collections of `up*.sql` filenames.
    #[tokio::test]
    async fn test_allows_valid_up_sql_collections() {
        for filenames in [
            &["up.sql"] as &[&str],
            &["up1.sql", "up2.sql"],
            &[
                "up01.sql", "up02.sql", "up03.sql", "up04.sql", "up05.sql",
                "up06.sql", "up07.sql", "up08.sql", "up09.sql", "up10.sql",
                "up11.sql",
            ],
            &["up00001.sql", "up00002.sql", "up00003.sql"],
        ] {
            let tempdir = Utf8TempDir::new().unwrap();
            for filename in filenames {
                _ = tokio::fs::File::create(tempdir.path().join(filename))
                    .await
                    .unwrap();
            }

            let maybe_schema = SchemaVersion::load_from_directory(
                Version::new(12, 0, 0),
                tempdir.path(),
            );
            match maybe_schema {
                Ok(_) => (),
                Err(message) => {
                    panic!("unexpected failure on {filenames:?}: {message:?}");
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Tests for SQL classification, preprocessing, and verification
    // ---------------------------------------------------------------

    #[test]
    fn test_classify_create_index() {
        let sql = "CREATE UNIQUE INDEX IF NOT EXISTS my_idx \
                    ON omicron.public.my_table (col1, col2) \
                    WHERE col1 IS NOT NULL;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        let changes = classified.changes;
        assert_eq!(
            changes,
            vec![SchemaChangeInfo::CreateIndex {
                table_name: SqlIdentifier::new("my_table").unwrap(),
                index_name: SqlIdentifier::new("my_idx").unwrap(),
            }]
        );
    }

    #[test]
    fn test_classify_create_index_with_storing() {
        // STORING is CRDB-specific; after preprocessing it should be
        // stripped and the index should still classify correctly.
        let sql = "CREATE INDEX IF NOT EXISTS my_idx \
                    ON omicron.public.my_table (col1) \
                    STORING (col2, col3);";
        let classified = classify_sql_statements(sql, "test").unwrap();
        let changes = classified.changes;
        assert_eq!(
            changes,
            vec![SchemaChangeInfo::CreateIndex {
                table_name: SqlIdentifier::new("my_table").unwrap(),
                index_name: SqlIdentifier::new("my_idx").unwrap(),
            }]
        );
    }

    #[test]
    fn test_classify_create_index_unnamed_is_error() {
        // An index without a name can't be verified, so it should be
        // rejected rather than silently accepted.
        let sql = "CREATE INDEX ON my_table (col1);";
        let result = classify_sql_statements(sql, "test");
        let err = result.unwrap_err();
        assert!(
            format!("{err:#}").contains("CREATE INDEX without a name"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn test_classify_create_table() {
        let sql = "CREATE TABLE IF NOT EXISTS omicron.public.widget (\
                        id UUID PRIMARY KEY\
                    );";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(classified.changes, vec![SchemaChangeInfo::OtherDdl]);
    }

    #[test]
    fn test_classify_dml_ignored() {
        // DML (INSERT, SELECT, SET, UPDATE) should not produce any
        // SchemaChangeInfo entries.
        let sql = "SET LOCAL disallow_full_table_scans = OFF;\n\
                    INSERT INTO t(id) VALUES (1);\n\
                    SELECT true;\n\
                    UPDATE t SET x = 1;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        let changes = classified.changes;
        assert!(
            changes.is_empty(),
            "DML should not be classified as DDL: {changes:?}"
        );
    }

    #[test]
    fn test_classify_alter_table_add_column() {
        let sql = "ALTER TABLE omicron.public.sled \
                    ADD COLUMN IF NOT EXISTS cpu_family TEXT;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(classified.changes, vec![SchemaChangeInfo::OtherDdl]);
    }

    #[test]
    fn test_classify_alter_table_drop_column() {
        let sql = "ALTER TABLE omicron.public.instance \
                    DROP COLUMN IF EXISTS active_sled_id;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(classified.changes, vec![SchemaChangeInfo::OtherDdl]);
    }

    #[test]
    fn test_classify_alter_column_set_not_null() {
        let sql = "ALTER TABLE omicron.public.metric_producer \
                    ALTER COLUMN kind SET NOT NULL;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        let changes = classified.changes;
        assert_eq!(
            changes,
            vec![SchemaChangeInfo::AlterColumnSetNotNull {
                table_name: SqlIdentifier::new("metric_producer").unwrap(),
                column_name: SqlIdentifier::new("kind").unwrap(),
            }]
        );
    }

    #[test]
    fn test_classify_alter_table_add_constraint() {
        // Note: IF NOT EXISTS is stripped during preprocessing (CRDB-specific).
        let sql = "ALTER TABLE omicron.public.external_ip \
                    ADD CONSTRAINT IF NOT EXISTS null_project_id \
                    CHECK (project_id IS NOT NULL);";
        let classified = classify_sql_statements(sql, "test").unwrap();
        let changes = classified.changes;
        assert_eq!(
            changes,
            vec![SchemaChangeInfo::AlterTableAddConstraint {
                table_name: SqlIdentifier::new("external_ip").unwrap(),
                constraint_name: SqlIdentifier::new("null_project_id").unwrap(),
            }]
        );
    }

    #[test]
    fn test_classify_add_constraint_unnamed_is_error() {
        // A constraint without a name can't be verified, so it should be
        // rejected rather than silently accepted.
        let sql = "ALTER TABLE my_table ADD CHECK (col > 0);";
        let result = classify_sql_statements(sql, "test");
        let err = result.unwrap_err();
        assert!(
            format!("{err:#}").contains("ADD CONSTRAINT without a name"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn test_classify_alter_table_drop_constraint() {
        let sql = "ALTER TABLE omicron.public.external_ip \
                    DROP CONSTRAINT IF EXISTS null_non_fip_parent_id;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(classified.changes, vec![SchemaChangeInfo::OtherDdl]);
    }

    #[test]
    fn test_classify_alter_index_rename() {
        // ALTER INDEX is DDL but doesn't need verification — OtherDdl.
        let sql = "ALTER INDEX omicron.public.old_idx RENAME TO new_idx;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(classified.changes, vec![SchemaChangeInfo::OtherDdl]);
    }

    #[test]
    fn test_classify_create_type_enum() {
        let sql = "CREATE TYPE IF NOT EXISTS omicron.public.sled_policy \
                    AS ENUM ('in_service', 'no_provision', 'expunged');";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(classified.changes, vec![SchemaChangeInfo::OtherDdl]);
    }

    #[test]
    fn test_classify_alter_type_add_value() {
        let sql = "ALTER TYPE omicron.public.dataset_kind \
                    ADD VALUE IF NOT EXISTS 'clickhouse_keeper2' \
                    AFTER 'clickhouse';";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(classified.changes, vec![SchemaChangeInfo::OtherDdl]);
    }

    #[test]
    fn test_classify_drop_index() {
        let sql = "DROP INDEX IF EXISTS my_idx;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(classified.changes, vec![SchemaChangeInfo::OtherDdl]);
    }

    #[test]
    fn test_classify_drop_index_table_at_notation() {
        // CRDB table@index notation — DROP INDEX is now OtherDdl
        // (no parsing needed).
        let sql = "DROP INDEX IF EXISTS \
                    omicron.public.sw_caboose@caboose_properties;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(classified.changes, vec![SchemaChangeInfo::OtherDdl]);
    }

    #[test]
    fn test_classify_drop_table() {
        let sql = "DROP TABLE IF EXISTS omicron.public.widget;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(classified.changes, vec![SchemaChangeInfo::OtherDdl]);
    }

    #[test]
    fn test_classify_create_drop_view() {
        let sql = "CREATE VIEW IF NOT EXISTS omicron.public.my_view \
                    AS SELECT 1;\n\
                    DROP VIEW IF EXISTS omicron.public.my_view;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 2);
        assert_eq!(
            classified.changes,
            vec![SchemaChangeInfo::OtherDdl, SchemaChangeInfo::OtherDdl]
        );
    }

    #[test]
    fn test_preprocess_storing() {
        let input = "CREATE INDEX foo ON bar (col1) STORING (col2, col3)";
        let output = preprocess_for_sqlparser(input);
        assert!(
            !output.contains("STORING"),
            "STORING should be stripped: {output}"
        );
        assert!(output.contains("CREATE INDEX foo ON bar (col1)"));
    }

    #[test]
    fn test_preprocess_type_array() {
        let input =
            "ALTER TABLE t ADD COLUMN addrs INET ARRAY, ADD COLUMN x INT";
        let output = preprocess_for_sqlparser(input);
        assert!(
            !output.contains("ARRAY"),
            "ARRAY keyword should be replaced with []: {output}"
        );
        assert!(output.contains("INET[]"));
    }

    #[test]
    fn test_preprocess_add_constraint_if_not_exists() {
        let input =
            "ALTER TABLE t ADD CONSTRAINT IF NOT EXISTS my_ck CHECK (x > 0)";
        let output = preprocess_for_sqlparser(input);
        assert!(
            !output.contains("IF NOT EXISTS"),
            "IF NOT EXISTS should be stripped: {output}"
        );
        assert!(output.contains("ADD CONSTRAINT my_ck"));
    }

    #[test]
    fn test_classify_alter_primary_key() {
        // CockroachDB-specific ALTER PRIMARY KEY USING COLUMNS can't
        // be parsed by sqlparser, but should still count as DDL.
        let sql = "ALTER TABLE omicron.public.t \
                    ALTER PRIMARY KEY USING COLUMNS (a, b);";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(classified.changes, vec![SchemaChangeInfo::OtherDdl]);
    }

    #[test]
    fn test_classify_mixed_dml_and_ddl() {
        // DML should be ignored; only DDL should be counted.
        let sql = "SET LOCAL disallow_full_table_scans = off;\n\
                    INSERT INTO t (a) VALUES (1);\n\
                    CREATE TABLE t2 (id INT PRIMARY KEY);\n\
                    UPDATE t SET a = 2;\n\
                    CREATE INDEX my_idx ON t2 (id);";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 2, "Only DDL should be counted");
        // CREATE TABLE → OtherDdl, CREATE INDEX → CreateIndex
        assert_eq!(classified.changes.len(), 2);
        assert_eq!(classified.changes[0], SchemaChangeInfo::OtherDdl);
        assert!(
            matches!(
                &classified.changes[1],
                SchemaChangeInfo::CreateIndex { .. }
            ),
            "CREATE INDEX should be classified: {:?}",
            classified.changes[1]
        );
    }

    #[test]
    fn test_classify_other_ddl_varieties() {
        // Various DDL statements that don't need verification should
        // all produce OtherDdl with the correct statement_count.
        for (sql, expected_count) in [
            ("CREATE TABLE t (id INT PRIMARY KEY);", 1),
            ("DROP TABLE IF EXISTS t;", 1),
            ("DROP INDEX IF EXISTS my_idx;", 1),
            ("CREATE TYPE my_enum AS ENUM ('a', 'b');", 1),
            ("ALTER TYPE my_enum ADD VALUE 'c';", 1),
            (
                "CREATE TABLE t1 (id INT PRIMARY KEY);\n\
                 DROP TABLE IF EXISTS t2;",
                2,
            ),
        ] {
            let classified = classify_sql_statements(sql, "test").unwrap();
            assert_eq!(
                classified.statement_count, expected_count,
                "wrong statement_count for: {sql}"
            );
            assert_eq!(
                classified.changes.len(),
                expected_count,
                "wrong changes count for: {sql}"
            );
            for change in &classified.changes {
                assert_eq!(
                    *change,
                    SchemaChangeInfo::OtherDdl,
                    "expected OtherDdl for: {sql}"
                );
            }
        }
    }

    #[test]
    fn test_one_ddl_enforcement() {
        // For versions > LAST_MULTI_DDL_VERSION,
        // multiple DDL per file should be rejected.
        let tempdir = Utf8TempDir::new().unwrap();
        let path = tempdir.path().join("up.sql");
        std::fs::write(
            &path,
            "CREATE TABLE t1 (id INT PRIMARY KEY);\n\
             CREATE TABLE t2 (id INT PRIMARY KEY);",
        )
        .unwrap();

        let result = SchemaVersion::load_from_directory(
            Version::new(LAST_MULTI_DDL_VERSION + 1, 0, 0),
            tempdir.path(),
        );
        assert!(result.is_err(), "Multiple DDL should be rejected");
        let err = format!("{:#}", result.unwrap_err());
        assert!(
            err.contains("contains 2 DDL statements"),
            "Error message should mention DDL count: {err}"
        );
    }

    #[test]
    fn test_multi_ddl_grandfathered() {
        // For old versions (<= LAST_MULTI_DDL_VERSION),
        // multiple DDL per file is allowed but verification is skipped.
        let tempdir = Utf8TempDir::new().unwrap();
        let path = tempdir.path().join("up.sql");
        std::fs::write(
            &path,
            "CREATE TABLE t1 (id INT PRIMARY KEY);\n\
             CREATE TABLE t2 (id INT PRIMARY KEY);",
        )
        .unwrap();

        let result = SchemaVersion::load_from_directory(
            Version::new(LAST_MULTI_DDL_VERSION, 0, 0),
            tempdir.path(),
        );
        assert!(
            result.is_ok(),
            "Old versions should allow multi-DDL: {:#}",
            result.unwrap_err()
        );
        let version = result.unwrap();
        let step = version.upgrade_steps().next().unwrap();
        // Multiple DDL in an old version → verification skipped (empty).
        assert!(step.schema_changes().is_empty());
    }

    #[test]
    fn test_verification_query_create_index() {
        let change = SchemaChangeInfo::CreateIndex {
            table_name: SqlIdentifier::new("sled").unwrap(),
            index_name: SqlIdentifier::new("sled_by_rack").unwrap(),
        };
        let query = change.verification_query();
        assert!(query.is_some());
        let query = query.unwrap();
        assert!(query.contains("crdb_internal.table_indexes"));
        assert!(query.contains("sled"));
        assert!(query.contains("sled_by_rack"));
    }

    #[test]
    fn test_verification_query_set_not_null() {
        let change = SchemaChangeInfo::AlterColumnSetNotNull {
            table_name: SqlIdentifier::new("metric_producer").unwrap(),
            column_name: SqlIdentifier::new("kind").unwrap(),
        };
        let query = change.verification_query();
        assert!(query.is_some());
        let query = query.unwrap();
        assert!(query.contains("information_schema.columns"));
        assert!(query.contains("metric_producer"));
        assert!(query.contains("kind"));
        assert!(query.contains("is_nullable = 'NO'"));
    }

    #[test]
    fn test_verification_query_add_constraint() {
        let change = SchemaChangeInfo::AlterTableAddConstraint {
            table_name: SqlIdentifier::new("external_ip").unwrap(),
            constraint_name: SqlIdentifier::new("null_project_id").unwrap(),
        };
        let query = change.verification_query();
        assert!(query.is_some());
        let query = query.unwrap();
        assert!(query.contains("information_schema.table_constraints"));
        assert!(query.contains("external_ip"));
        assert!(query.contains("null_project_id"));
    }

    #[test]
    fn test_verification_query_other_ddl_returns_none() {
        assert!(
            SchemaChangeInfo::OtherDdl.verification_query().is_none(),
            "OtherDdl should return None"
        );
    }

    // The most important regression test: verify that ALL existing
    // migration files can be successfully classified.
    #[test]
    fn test_all_existing_migrations_parseable() {
        // Find the schema/crdb directory relative to this source file.
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let schema_dir =
            camino::Utf8PathBuf::from(manifest_dir).join("../../schema/crdb");

        // Load all schema versions — this calls load_from_directory for
        // each, which calls classify_sql_statements internally.
        match AllSchemaVersions::load(&schema_dir) {
            Ok(all_versions) => {
                // Verify we loaded a reasonable number of versions.
                let count = all_versions.iter_versions().count();
                assert!(
                    count > 100,
                    "Expected > 100 schema versions, found {count}"
                );
            }
            Err(e) => {
                panic!("Failed to load and classify all migrations: {:#}", e);
            }
        }
    }

    #[test]
    fn test_classify_alter_table_multi_op() {
        // A single ALTER TABLE with multiple operations should produce
        // statement_count == 1 but multiple changes.
        let sql = "ALTER TABLE omicron.public.bgp_peer_config \
                    DROP CONSTRAINT IF EXISTS bgp_peer_config_addr_unique, \
                    ADD CONSTRAINT bgp_peer_config_addr_unique \
                    CHECK (addr IS NOT NULL);";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(
            classified.statement_count, 1,
            "A single ALTER TABLE is one DDL statement"
        );
        assert_eq!(
            classified.changes.len(),
            2,
            "DROP CONSTRAINT + ADD CONSTRAINT = 2 changes"
        );
        // DROP CONSTRAINT is now OtherDdl (no verification needed).
        assert_eq!(classified.changes[0], SchemaChangeInfo::OtherDdl);
        assert_eq!(
            classified.changes[1],
            SchemaChangeInfo::AlterTableAddConstraint {
                table_name: SqlIdentifier::new("bgp_peer_config").unwrap(),
                constraint_name: SqlIdentifier::new(
                    "bgp_peer_config_addr_unique"
                )
                .unwrap(),
            }
        );
    }

    #[test]
    fn test_one_ddl_multi_op_allowed() {
        // A single ALTER TABLE with multiple operations in a post-
        // LAST_MULTI_DDL_VERSION migration should be accepted (not
        // rejected), and all changes stored for verification.
        let tempdir = Utf8TempDir::new().unwrap();
        let path = tempdir.path().join("up.sql");
        std::fs::write(
            &path,
            "ALTER TABLE omicron.public.t \
             DROP CONSTRAINT IF EXISTS old_check, \
             ADD CONSTRAINT new_check CHECK (x > 0);",
        )
        .unwrap();

        let result = SchemaVersion::load_from_directory(
            Version::new(LAST_MULTI_DDL_VERSION + 1, 0, 0),
            tempdir.path(),
        );
        assert!(
            result.is_ok(),
            "Single ALTER TABLE with multiple ops should be allowed: {:#}",
            result.unwrap_err()
        );
        let version = result.unwrap();
        let step = version.upgrade_steps().next().unwrap();
        // Both operations should be stored for verification.
        assert_eq!(
            step.schema_changes().len(),
            2,
            "Both operations should be stored"
        );
        // DROP CONSTRAINT is now OtherDdl.
        assert_eq!(step.schema_changes()[0], SchemaChangeInfo::OtherDdl);
        assert_eq!(
            step.schema_changes()[1],
            SchemaChangeInfo::AlterTableAddConstraint {
                table_name: SqlIdentifier::new("t").unwrap(),
                constraint_name: SqlIdentifier::new("new_check").unwrap(),
            }
        );
    }

    #[test]
    fn test_classify_add_column_needs_backfill_not_null_default() {
        let sql = "ALTER TABLE omicron.public.bgp_config \
                    ADD COLUMN IF NOT EXISTS max_paths INT NOT NULL DEFAULT 1;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(
            classified.changes,
            vec![SchemaChangeInfo::AddColumnNeedsBackfill {
                table_name: SqlIdentifier::new("bgp_config").unwrap(),
                column_name: SqlIdentifier::new("max_paths").unwrap(),
            }]
        );
    }

    #[test]
    fn test_classify_add_column_needs_backfill_not_null_no_default() {
        let sql = "ALTER TABLE omicron.public.my_table \
                    ADD COLUMN my_col INT NOT NULL;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(
            classified.changes,
            vec![SchemaChangeInfo::AddColumnNeedsBackfill {
                table_name: SqlIdentifier::new("my_table").unwrap(),
                column_name: SqlIdentifier::new("my_col").unwrap(),
            }]
        );
    }

    #[test]
    fn test_classify_add_column_needs_backfill_nullable_non_null_default() {
        // Nullable column with a non-NULL default triggers backfill
        // (CockroachDB's ColumnNeedsBackfill: HasDefault && !HasNullDefault)
        let sql = "ALTER TABLE omicron.public.my_table \
                    ADD COLUMN foo INT DEFAULT 42;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(
            classified.changes,
            vec![SchemaChangeInfo::AddColumnNeedsBackfill {
                table_name: SqlIdentifier::new("my_table").unwrap(),
                column_name: SqlIdentifier::new("foo").unwrap(),
            }]
        );
    }

    #[test]
    fn test_classify_add_column_nullable_default_null() {
        // Nullable column with DEFAULT NULL does NOT trigger backfill
        let sql = "ALTER TABLE omicron.public.my_table \
                    ADD COLUMN foo INT DEFAULT NULL;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(classified.changes, vec![SchemaChangeInfo::OtherDdl]);
    }

    #[test]
    fn test_classify_add_column_needs_backfill_stored_computed() {
        // Stored computed column triggers backfill
        let sql = "ALTER TABLE omicron.public.my_table \
                    ADD COLUMN foo INT GENERATED ALWAYS AS (bar + 1) STORED;";
        let classified = classify_sql_statements(sql, "test").unwrap();
        assert_eq!(classified.statement_count, 1);
        assert_eq!(
            classified.changes,
            vec![SchemaChangeInfo::AddColumnNeedsBackfill {
                table_name: SqlIdentifier::new("my_table").unwrap(),
                column_name: SqlIdentifier::new("foo").unwrap(),
            }]
        );
    }

    #[test]
    fn test_verification_query_add_column_needs_backfill() {
        let change = SchemaChangeInfo::AddColumnNeedsBackfill {
            table_name: SqlIdentifier::new("bgp_config").unwrap(),
            column_name: SqlIdentifier::new("max_paths").unwrap(),
        };
        let query = change.verification_query();
        assert!(query.is_some());
        let query = query.unwrap();
        assert!(query.contains("information_schema.columns"));
        assert!(query.contains("bgp_config"));
        assert!(query.contains("max_paths"));
        // This query checks column existence, not nullability
        assert!(!query.contains("is_nullable"));
    }
}
