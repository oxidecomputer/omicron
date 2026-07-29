// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The adjacent `tuf_artifact.csv` is an export of the `tuf_artifact` database
//! from the dogfood rack as of 2026-07-08. This serves as a nearly-complete set
//! of artifacts from all repositories we've ever shipped to customers, as well
//! as plenty of bonus in-development repositories.
//!
//! This migration test imports the artifacts, performs the migration, then
//! checks that:
//! - The expected number of artifacts is present
//! - Each artifact's sha256 has an entry in the new `tuf_artifact_file` table
//! - The set of tags for each artifact deserializes into `KnownArtifactTags`,
//!   and that deserialized value serializes back into the same set of tags

use std::collections::{BTreeMap, BTreeSet};

use futures::future::BoxFuture;
use tufaceous_artifact_v2::{KnownArtifactTags, RotBootloaderTags, RotTags};
use uuid::Uuid;

use super::super::schema::{DataMigrationFns, MigrationContext};

struct KindCount {
    /// A SQL pattern that can be used in a `LIKE` statement to match against
    /// the Tufaceous v1 kind.
    kind_like: &'static str,
    /// The number of artifacts matching this kind in `tuf_artifact.sql`.
    count: usize,
    /// The number of tags this kind of artifact will generate when converted to
    /// Tufaceous v2.
    tags: usize,
}

/// `tuf_artifact.sql` contains these kinds of artifacts, from which we will
/// calculate the correct number of tags in the database after the migration
/// completes.
const KINDS: [KindCount; 8] = [
    KindCount { kind_like: "installinator_document", count: 86, tags: 1 },
    KindCount { kind_like: "measurement_corpus", count: 16, tags: 1 },
    KindCount { kind_like: "%_phase_1", count: 344, tags: 3 },
    KindCount { kind_like: "%_phase_2", count: 172, tags: 2 },
    KindCount { kind_like: "%_rot_image_%", count: 54, tags: 4 },
    KindCount { kind_like: "%_rot_bootloader", count: 20, tags: 3 },
    KindCount { kind_like: "%_sp", count: 240, tags: 2 },
    KindCount { kind_like: "zone", count: 1032, tags: 2 },
];
/// The total number of rows in `tuf_artifact.sql`.
const ARTIFACT_COUNT: usize = {
    let mut n = 0;
    let mut i = 0;
    while i < KINDS.len() {
        n += KINDS[i].count;
        i += 1;
    }
    n
};
/// There are 6 sets of bart-signed images in the sample, each of which is
/// listed with the same contents for 3 different kinds (gimlet/switch/psc). The
/// number of distinct files in the database is thus 12 fewer.
const ARTIFACT_FILE_COUNT: usize = ARTIFACT_COUNT - (6 * 2);
/// The number of tags we expect the database to contain after the migration
/// completes.
const TAG_COUNT: usize = {
    let mut n = 0;
    let mut i = 0;
    while i < KINDS.len() {
        n += KINDS[i].count * KINDS[i].tags;
        i += 1;
    }
    n
};

pub(crate) fn checks() -> DataMigrationFns {
    DataMigrationFns::new().before(before).after(after)
}

fn before<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let rows_inserted = ctx
            .client
            .execute(include_str!("tuf_artifact.sql"), &[])
            .await
            .expect("failed to execute tuf_artifact.sql");
        assert_eq!(rows_inserted, u64::try_from(ARTIFACT_COUNT).unwrap());

        // We expect that the number of rows returned here will match
        // the number of rows in `tuf_artifact_file` after the migration
        // completes; otherwise two artifacts had the same hash but different
        // versions or lengths.
        assert_eq!(
            ctx.client
                .query(
                    "SELECT DISTINCT sha256, version, artifact_size \
                    FROM tuf_artifact",
                    &[]
                )
                .await
                .expect("failed to query tuf_artifact (pre-migration)")
                .len(),
            ARTIFACT_FILE_COUNT
        );

        // Check our per-kind counts above.
        for KindCount { kind_like, count, .. } in KINDS {
            assert_eq!(
                ctx.client
                    .query_one_scalar::<i64, _>(
                        "SELECT COUNT(*) FROM tuf_artifact WHERE kind LIKE $1",
                        &[&kind_like]
                    )
                    .await
                    .expect("failed to count per-kind artifacts"),
                i64::try_from(count).unwrap(),
            );
        }
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let artifact_rows = ctx
            .client
            .query("SELECT sha256 FROM tuf_artifact", &[])
            .await
            .expect("failed to query tuf_artifact");
        assert_eq!(artifact_rows.len(), ARTIFACT_COUNT);
        let artifact_sha256 = artifact_rows
            .into_iter()
            .map(|row| row.get("sha256"))
            .collect::<BTreeSet<String>>();
        let artifact_file_rows = ctx
            .client
            .query("SELECT sha256 FROM tuf_artifact_file", &[])
            .await
            .expect("failed to query tuf_artifact_file");
        assert_eq!(artifact_file_rows.len(), ARTIFACT_FILE_COUNT); // matches above
        let artifact_file_sha256 = artifact_file_rows
            .into_iter()
            .map(|row| row.get("sha256"))
            .collect::<BTreeSet<String>>();
        assert_eq!(artifact_sha256, artifact_file_sha256);

        let artifact_tag_rows = ctx
            .client
            .query(
                "SELECT tuf_artifact_id, key, value FROM tuf_artifact_tag",
                &[],
            )
            .await
            .expect("failed to query tuf_artifact_tag");
        assert_eq!(artifact_tag_rows.len(), TAG_COUNT);
        let mut artifacts = BTreeMap::<_, BTreeMap<_, _>>::new();
        for row in artifact_tag_rows {
            let id = row.get::<_, Uuid>("tuf_artifact_id");
            let key = row.get::<_, String>("key");
            let value = row.get::<_, String>("value");
            artifacts.entry(id).or_default().insert(key, value);
        }
        for (id, tags) in artifacts {
            let known = match KnownArtifactTags::from_tags(tags.clone()) {
                Ok(known) => known,
                Err(err) => {
                    panic!("failed to deserialize tags for {id}: {err}")
                }
            };
            let round_trip =
                known.to_tags().expect("failed to serialize known tags");
            assert_eq!(tags, round_trip, "tags for {id} did not round trip");

            // The old column type for `sign` was BYTES; make sure the value is
            // still a 64-character hex string.
            if let KnownArtifactTags::Rot(RotTags {
                rot_rkth: Some(rot_rkth),
                ..
            })
            | KnownArtifactTags::RotBootloader(RotBootloaderTags {
                rot_rkth: Some(rot_rkth),
                ..
            }) = known
            {
                let Ok(bytes) = hex::decode(rot_rkth.as_str()) else {
                    panic!(
                        "{id}'s rot_rkth tag ({rot_rkth:?}) is not a hex string"
                    );
                };
                assert_eq!(
                    bytes.len(),
                    32,
                    "{id}'s rot_rkth tag is an unexpected length"
                );
            }
        }

        // be kind, rewind
        for table in ["tuf_artifact", "tuf_artifact_file", "tuf_artifact_tag"] {
            if let Err(err) =
                ctx.client.execute(&format!("DELETE FROM {table}"), &[]).await
            {
                panic!("failed to delete all rows from {table}: {err:?}");
            }
        }
    })
}
