// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db target-release` subcommand
//!
//! Shows the current target release and date when update was requested.
//! When `--list` option is used, lists all target release records.

use crate::db::DbFetchOptions;
use crate::db::check_limit;
use crate::helpers::datetime_rfc3339_concise;
use anyhow::Context;
use async_bb8_diesel::AsyncRunQueryDsl;
use clap::Args;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use nexus_db_model::TargetRelease;
use nexus_db_model::TargetReleaseSource;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_schema::schema::target_release::dsl;
use tabled::Tabled;

#[derive(Debug, Args, Clone)]
pub(super) struct TargetReleaseArgs {
    /// List the most recent target releases, sorted from oldest to newest
    #[clap(long)]
    list: bool,
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct TargetReleaseRow {
    target_release: String,
    time_requested: String,
}

pub(super) async fn cmd_db_target_release(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &TargetReleaseArgs,
) -> Result<(), anyhow::Error> {
    let releases = if args.list {
        let limit = fetch_opts.fetch_limit;
        let conn = datastore.pool_connection_for_tests().await?;
        let mut releases: Vec<TargetRelease> = dsl::target_release
            .select(TargetRelease::as_select())
            .order_by(dsl::generation.desc())
            .limit(i64::from(u32::from(limit)))
            .load_async(&*conn)
            .await
            .context("listing target releases")?;
        check_limit(&releases, limit, || {
            String::from("listing target releases")
        });
        releases.reverse();
        releases
    } else {
        vec![
            datastore
                .target_release_get_current(opctx)
                .await
                .context("fetching current target release")?,
        ]
    };

    let mut rows = Vec::with_capacity(releases.len());
    for release in releases {
        let target_release = match release
            .release_source()
            .context("interpreting target release source")?
        {
            TargetReleaseSource::Unspecified => "<unspecified>".to_string(),
            TargetReleaseSource::SystemVersion(tuf_repo_id) => datastore
                .tuf_repo_get_version(opctx, &tuf_repo_id)
                .await
                .with_context(|| format!("fetching TUF repo {tuf_repo_id}"))?
                .to_string(),
        };
        rows.push(TargetReleaseRow {
            target_release,
            time_requested: datetime_rfc3339_concise(&release.time_requested),
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(1, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}
