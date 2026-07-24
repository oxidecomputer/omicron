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
use tufaceous_artifact_v2::KnownArtifactTags;
use uuid::Uuid;

use super::super::schema::{DataMigrationFns, MigrationContext};

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
        assert_eq!(rows_inserted, 1964);

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
            1952
        );
    })
}

fn after<'a>(ctx: &'a MigrationContext<'a>) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        let artifact_rows = ctx
            .client
            .query("SELECT sha256 FROM tuf_artifact", &[])
            .await
            .expect("failed to query tuf_artifact");
        assert_eq!(artifact_rows.len(), 1964); // matches above
        let artifact_sha256 = artifact_rows
            .into_iter()
            .map(|row| row.get("sha256"))
            .collect::<BTreeSet<String>>();
        let artifact_file_rows = ctx
            .client
            .query("SELECT sha256 FROM tuf_artifact_file", &[])
            .await
            .expect("failed to query tuf_artifact_file");
        assert_eq!(artifact_file_rows.len(), 1952); // matches above
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
        }
    })
}
