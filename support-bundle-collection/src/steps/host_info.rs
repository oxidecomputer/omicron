// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collect host information from sleds for support bundles

use crate::cache::Cache;
use crate::collection::BundleCollection;
use crate::step::CollectionStep;
use crate::step::CollectionStepOutput;
use crate::zip::MERGE_ZIP_SUFFIX;

use anyhow::Context;
use anyhow::bail;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::Future;
use nexus_db_model::Sled;
use nexus_networking;
use nexus_types::identity::Asset;
use nexus_types::support_bundle::BundleTimeRange;
use slog::error;
use slog::info;
use slog_error_chain::InlineErrorChain;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

/// The maximum number of concurrent requests to a single sled-agent
/// from one sled-data collection step, applied independently to the
/// diagnostics-command fan-out and to zone-log downloads.
const MAX_CONCURRENT_SLED_AGENT_REQUESTS: usize = 10;

pub async fn spawn_query_all_sleds(
    collection: &BundleCollection,
    cache: &Cache,
) -> anyhow::Result<CollectionStepOutput> {
    let Some(sled_selection) = collection.data_selection().sled_selection()
    else {
        return Ok(CollectionStepOutput::Skipped);
    };
    let time_range = collection.data_selection().time_range().clone();

    let all_sleds = tokio::select! {
        _ = collection.cancelled() => return Ok(CollectionStepOutput::None),
        result = cache.get_or_initialize_all_sleds(collection) => result,
    };

    let Some(all_sleds) = all_sleds else {
        bail!("Could not read list of sleds");
    };

    let mut extra_steps: Vec<CollectionStep> = vec![];
    for sled in all_sleds {
        if !sled_selection.contains(sled.id()) {
            continue;
        }

        let sled = sled.clone();
        let time_range = time_range.clone();
        extra_steps.push(CollectionStep::new(
            format!("sled data for sled {}", sled.id()),
            Box::new({
                move |collection, dir| {
                    async move {
                        collect_data_from_sled(
                            collection, sled, time_range, dir,
                        )
                        .await
                    }
                    .boxed()
                }
            }),
        ))
    }

    Ok(CollectionStepOutput::Spawn { extra_steps })
}

// Collect data from a sled, storing it into a directory that will
// be turned into a support bundle.
//
// - "sled" is the sled from which we should collect data.
// - "time_range" bounds which zone logs are collected, by file mtime.
// - "dir" is a directory where data can be stored, to be turned
// into a bundle after collection completes.
//
// # Cancel safety
//
// Cancel-**unsafe**: writes to the filesystem and delegates to
// cancel-unsafe helpers. HTTP requests within helpers and between
// phases are eagerly cancelled via `select!`.
async fn collect_data_from_sled(
    collection: &BundleCollection,
    sled: Sled,
    time_range: BundleTimeRange,
    dir: &Utf8Path,
) -> anyhow::Result<CollectionStepOutput> {
    let (log, opctx, datastore) =
        (collection.log(), collection.opctx(), collection.datastore());

    let excluded = collection
        .data_selection()
        .sled_selection()
        .map_or(true, |sel| !sel.contains(sled.id()));
    if excluded {
        return Ok(CollectionStepOutput::Skipped);
    }

    info!(log, "Collecting bundle info from sled"; "sled" => %sled.id());

    let sled_client_result = tokio::select! {
        _ = collection.cancelled() => return Ok(CollectionStepOutput::None),
        result = nexus_networking::sled_client(
            &datastore, &opctx, sled.id(), log,
        ) => result,
    };

    let sled_path = dir
        .join("rack")
        .join(sled.rack_id.to_string())
        .join("sled")
        .join(sled.id().to_string());
    tokio::fs::create_dir_all(&sled_path).await?;
    tokio::fs::write(sled_path.join("sled.txt"), format!("{sled:?}")).await?;

    let sled_client = match sled_client_result {
        Ok(client) => client,
        Err(err) => {
            tokio::fs::write(
                sled_path.join("error.txt"),
                "Could not contact sled",
            )
            .await.with_context(|| {
                format!("Failed to save 'error.txt' to bundle when recording error: {err}")
            })?;
            bail!("Could not contact sled: {err}");
        }
    };

    // Each helper function handles its own cancellation internally:
    // HTTP requests are eagerly cancelled via select!, while filesystem
    // writes complete cooperatively. This means the buffered streams
    // below drain quickly on cancellation without dropping in-flight
    // file writes.

    // NB: As new sled-diagnostic commands are added they should
    // be added to this array so that their output can be saved
    // within the support bundle.
    let cancellation_token = collection.cancellation_token();
    let mut diag_cmds = futures::stream::iter([
        save_diag_cmd_output_or_error(
            &sled_path,
            "zoneadm",
            sled_client.support_zoneadm_info(),
            cancellation_token,
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "dladm",
            sled_client.support_dladm_info(),
            cancellation_token,
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "ipadm",
            sled_client.support_ipadm_info(),
            cancellation_token,
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "nvmeadm",
            sled_client.support_nvmeadm_info(),
            cancellation_token,
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "pargs",
            sled_client.support_pargs_info(),
            cancellation_token,
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "pfiles",
            sled_client.support_pfiles_info(),
            cancellation_token,
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "pstack",
            sled_client.support_pstack_info(),
            cancellation_token,
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "zfs",
            sled_client.support_zfs_info(),
            cancellation_token,
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "zpool",
            sled_client.support_zpool_info(),
            cancellation_token,
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "health-check",
            sled_client.support_health_check(),
            cancellation_token,
        )
        .boxed(),
    ])
    // Commands executing concurrently might be doing their own
    // concurrent work on the sled, for example collecting `pstack`
    // output of every Oxide process that is found on a sled.
    .buffer_unordered(MAX_CONCURRENT_SLED_AGENT_REQUESTS);

    while let Some(result) = diag_cmds.next().await {
        // Log that we failed to write the diag command output to a
        // file but don't return early as we wish to get as much
        // information as we can.
        if let Err(e) = result {
            error!(
                log,
                "failed to write diagnostic command output to \
                file: {e}"
            );
        }
    }

    let zones = tokio::select! {
        _ = collection.cancelled() => return Ok(CollectionStepOutput::None),
        result = sled_client.support_logs() => result?.into_inner(),
    };

    // For each zone we fire off a request to its sled-agent to collect
    // its logs in a zip file and write the result to the support
    // bundle.
    //
    // The number of zones on a sled is unbounded (it grows with every
    // zone that has ever had logs on the sled), and each request makes
    // the sled-agent take ZFS snapshots and assemble a zip file before
    // it can respond, so we cap the number of in-flight requests.
    let sled_client = &sled_client;
    let sled_path = &sled_path;
    let time_range = &time_range;
    let mut log_futs = futures::stream::iter(zones)
        .map(|zone| async move {
            save_zone_log_zip_or_error(
                sled_client,
                &zone,
                sled_path,
                time_range,
                cancellation_token,
            )
            .await
        })
        .buffer_unordered(MAX_CONCURRENT_SLED_AGENT_REQUESTS);

    while let Some(log_collection_result) = log_futs.next().await {
        // We log any errors saving the zip file to disk and
        // continue on.
        if let Err(e) = log_collection_result {
            error!(log, "failed to write logs output: {e}");
        }
    }
    Ok(CollectionStepOutput::None)
}

// Run a `sled-diagnostics` future and save its output to a corresponding file.
//
// # Cancel safety
//
// Cancel-**unsafe**: writes to the filesystem via `tokio::fs`.
// The `future` argument must be cancel-safe (it is dropped via `select!`
// if cancellation occurs before the HTTP response arrives). The future
// return by this function must not be dropped mid-flight.
async fn save_diag_cmd_output_or_error<F, S: serde::Serialize>(
    path: &Utf8Path,
    command: &str,
    future: F,
    cancellation_token: &CancellationToken,
) -> anyhow::Result<()>
where
    F: Future<
            Output = Result<
                sled_agent_client::ResponseValue<S>,
                sled_agent_client::Error<sled_agent_client::types::Error>,
            >,
        > + Send,
{
    let result = tokio::select! {
        _ = cancellation_token.cancelled() => return Ok(()),
        result = future => result,
    };

    match result {
        Ok(result) => {
            let output = result.into_inner();
            let json = serde_json::to_string(&output).with_context(|| {
                format!("failed to serialize {command} output as json")
            })?;
            tokio::fs::write(path.join(format!("{command}.json")), json)
                .await
                .with_context(|| {
                    format!("failed to write output of {command} to file")
                })?;
        }
        Err(err) => {
            let err_string = InlineErrorChain::new(&err).to_string();
            tokio::fs::write(
                path.join(format!("{command}_err.txt")),
                err_string,
            )
            .await?;
        }
    }
    Ok(())
}

// Download zone logs from a sled-agent into the bundle.
//
// # Cancel safety
//
// Cancel-**unsafe**: writes to the filesystem.
// The initial HTTP download is cancel-safe and uses `select!` internally.
// All filesystem operations after the download must not be dropped.
async fn save_zone_log_zip_or_error(
    client: &sled_agent_client::Client,
    zone: &str,
    path: &Utf8Path,
    time_range: &BundleTimeRange,
    cancellation_token: &CancellationToken,
) -> anyhow::Result<()> {
    // Bind with names so the positional Progenitor call below can't
    // accidentally swap start and end: query parameters are supplied in
    // alphabetical order, which is why "end time" comes before
    // "start time".
    let (start, end) = (time_range.start(), time_range.end());

    let download_result = tokio::select! {
        _ = cancellation_token.cancelled() => return Ok(()),
        result = client.support_logs_download(
            zone,
            end.as_ref(),
            None,
            start.as_ref(),
        ) => result,
    };

    match download_result {
        Ok(res) => {
            let output_dir = path.join(format!("logs/{zone}"));
            if let Err(err) =
                save_zone_log_zip(res.into_inner(), &output_dir).await
            {
                // Leave an error in the bundle in place of the logs, rather
                // than a zip that could not be saved.
                let err_string =
                    InlineErrorChain::new(err.as_ref()).to_string();
                tokio::fs::write(
                    path.join(format!("{zone}.logs.err")),
                    err_string,
                )
                .await?;
                return Err(err);
            }
        }
        Err(err) => {
            let err_string = InlineErrorChain::new(&err).to_string();
            tokio::fs::write(path.join(format!("{zone}.logs.err")), err_string)
                .await?;
        }
    };

    Ok(())
}

// The path of a zone's log zip within its logs directory.
//
// The name marks the zip for merging, so that the bundle includes its entries,
// as-is, rather than the zip itself.
fn zone_log_zip_path(output_dir: &Utf8Path) -> Utf8PathBuf {
    output_dir.join(format!("logs{MERGE_ZIP_SUFFIX}"))
}

// Stream a zone's log zip to `output_dir`.
//
// On failure, removes what it wrote, so that neither a partial zip nor an
// empty directory for it ends up in the bundle.
async fn save_zone_log_zip(
    bytestream: sled_agent_client::ByteStream,
    output_dir: &Utf8Path,
) -> anyhow::Result<()> {
    let zipfile_path = zone_log_zip_path(output_dir);

    let result = async {
        // Ensure the logs output directory exists.
        tokio::fs::create_dir_all(&output_dir).await.with_context(|| {
            format!("failed to create output directory: {output_dir}")
        })?;

        // Stream the log zip file to disk.
        let mut file =
            tokio::fs::File::create(&zipfile_path).await.with_context(
                || format!("failed to create log zip file: {zipfile_path}"),
            )?;

        let stream = bytestream
            .into_inner()
            .map(|chunk| chunk.map_err(|e| std::io::Error::other(e)));
        let mut reader = tokio_util::io::StreamReader::new(stream);
        let _nbytes =
            tokio::io::copy(&mut reader, &mut file).await.with_context(
                || format!("failed to download log zip: {zipfile_path}"),
            )?;
        file.flush().await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;

    if result.is_err() {
        // This is best-effort: if removal fails, the bundle skips whatever it
        // cannot read of the partial zip, and records what it skipped.
        let _ = tokio::fs::remove_file(&zipfile_path).await;
        // Fails, leaving the directory alone, unless it is empty.
        let _ = tokio::fs::remove_dir(output_dir).await;
    }
    result
}
