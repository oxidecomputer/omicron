// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collect host information from sleds for support bundles

use crate::cache::Cache;
use crate::collection::BundleCollection;
use crate::output::BundleOutput;
use crate::step::CollectionStep;
use crate::step::CollectionStepOutput;
use crate::step::StepError;
use crate::step::StepResultExt;
use crate::zip::MERGE_ZIP_SUFFIX;

use anyhow::Context;
use anyhow::anyhow;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::Future;
use nexus_db_model::Sled;
use nexus_networking;
use nexus_types::identity::Asset;
use nexus_types::support_bundle::BundleTimeRange;
use slog::Logger;
use slog::error;
use slog::info;
use slog_error_chain::InlineErrorChain;
use tokio_util::sync::CancellationToken;

/// The maximum number of concurrent requests to a single sled-agent
/// from one sled-data collection step, applied independently to the
/// diagnostics-command fan-out and to zone-log downloads.
const MAX_CONCURRENT_SLED_AGENT_REQUESTS: usize = 10;

pub async fn spawn_query_all_sleds(
    collection: &BundleCollection,
    cache: &Cache,
) -> Result<CollectionStepOutput, StepError> {
    let Some(sled_selection) = collection.data_selection().sled_selection()
    else {
        return Ok(CollectionStepOutput::Skipped);
    };
    let time_range = collection.data_selection().time_range().clone();

    let all_sleds = tokio::select! {
        _ = collection.stopped() => return Ok(CollectionStepOutput::None),
        result = cache.get_or_initialize_all_sleds(collection) => result,
    };

    let Some(all_sleds) = all_sleds else {
        return Err(anyhow!("Could not read list of sleds").into());
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
                move |collection, output| {
                    async move {
                        collect_data_from_sled(
                            collection, sled, time_range, output,
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
// - "output" is the bundle directory where data can be stored.
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
    output: &BundleOutput,
) -> Result<CollectionStepOutput, StepError> {
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
        _ = collection.stopped() => return Ok(CollectionStepOutput::None),
        result = nexus_networking::sled_client(
            &datastore, &opctx, sled.id(), log,
        ) => result,
    };

    let sled_path =
        output.join(format!("rack/{}/sled/{}", sled.rack_id, sled.id()));
    sled_path.write("sled.txt", format!("{sled:?}")).await?;

    let sled_client = match sled_client_result {
        Ok(client) => client,
        Err(err) => {
            sled_path
                .write("error.txt", "Could not contact sled")
                .await.with_context(|| {
                format!("Failed to save 'error.txt' to bundle when recording error: {err}")
            })?;
            return Err(anyhow!("Could not contact sled: {err}").into());
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
    let cancellation_token = collection.stop_token();
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
        //
        // Running out of space is reported by the step framework, rather
        // than for each file that didn't fit.
        if let Err(StepError::Other(e)) = result {
            error!(
                log,
                "failed to write diagnostic command output to \
                file: {e}"
            );
        }
    }

    let zones = tokio::select! {
        _ = collection.stopped() => return Ok(CollectionStepOutput::None),
        result = sled_client.support_logs() => {
            result.context("failed to list zone logs")?.into_inner()
        }
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
                log,
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
        if let Err(StepError::Other(e)) = log_collection_result {
            error!(
                log,
                "failed to write logs output";
                "error" => InlineErrorChain::new(e.as_ref()),
            );
        }
    }
    Ok(CollectionStepOutput::None)
}

// Run a `sled-diagnostics` future and save its output to a corresponding file.
//
// # Cancel safety
//
// Cancel-**unsafe**: writes to the bundle.
// The `future` argument must be cancel-safe (it is dropped via `select!`
// if cancellation occurs before the HTTP response arrives). The future
// return by this function must not be dropped mid-flight.
async fn save_diag_cmd_output_or_error<F, S: serde::Serialize>(
    path: &BundleOutput,
    command: &str,
    future: F,
    cancellation_token: &CancellationToken,
) -> Result<(), StepError>
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
            path.write(format!("{command}.json"), json).await.with_context(
                || format!("failed to write output of {command} to file"),
            )?;
        }
        Err(err) => {
            let err_string = InlineErrorChain::new(&err).to_string();
            path.write(format!("{command}_err.txt"), err_string).await?;
        }
    }
    Ok(())
}

// Download zone logs from a sled-agent into the bundle.
//
// # Cancel safety
//
// Cancel-**unsafe**: writes to the bundle.
// The initial HTTP download is cancel-safe and uses `select!` internally.
// All filesystem operations after the download must not be dropped.
async fn save_zone_log_zip_or_error(
    log: &Logger,
    client: &sled_agent_client::Client,
    zone: &str,
    path: &BundleOutput,
    time_range: &BundleTimeRange,
    cancellation_token: &CancellationToken,
) -> Result<(), StepError> {
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
                // than a zip that could not be saved. A bundle that's out of
                // space has no room for the error either, and records what it
                // left out elsewhere.
                if let StepError::Other(err) = &err {
                    let err_string =
                        InlineErrorChain::new(err.as_ref()).to_string();
                    save_zone_log_error(log, zone, path, err_string).await;
                }
                return Err(err);
            }
        }
        Err(err) => {
            let err_string = InlineErrorChain::new(&err).to_string();
            save_zone_log_error(log, zone, path, err_string).await;
        }
    };

    Ok(())
}

// Record, in the bundle, an error that kept a zone's logs out of it.
//
// If the error cannot be written to the bundle, it is logged instead, so that
// the failure to write it does not hide the error itself.
async fn save_zone_log_error(
    log: &Logger,
    zone: &str,
    path: &BundleOutput,
    err_string: String,
) {
    let err_name = format!("{zone}.logs.err");
    if let Err(write_err) = path.write(&err_name, &err_string).await {
        error!(
            log,
            "failed to save zone log error to bundle";
            "zone" => zone,
            "file" => %err_name,
            "error" => %err_string,
            "write_error" => InlineErrorChain::new(write_err.as_error()),
        );
    }
}

// Stream a zone's log zip to `output_dir`.
//
// The zip's name marks it for merging, so that the bundle includes its
// entries, as-is, rather than the zip itself.
//
// On failure, removes what it wrote, so that neither a partial zip nor an
// empty directory for it ends up in the bundle.
//
// # Cancel safety
//
// Cancel-**unsafe**: if dropped, a partial zip may be left in the bundle
// without being cleaned up.
async fn save_zone_log_zip(
    bytestream: sled_agent_client::ByteStream,
    output_dir: &BundleOutput,
) -> Result<(), StepError> {
    let stream = bytestream
        .into_inner()
        .map(|chunk| chunk.map_err(|e| std::io::Error::other(e)));
    let reader = tokio_util::io::StreamReader::new(stream);
    let result = output_dir
        .write_stream(format!("logs{MERGE_ZIP_SUFFIX}"), reader)
        .await
        .context("failed to download log zip");

    if result.is_err() {
        output_dir.remove_dir_if_empty().await;
    }
    result.map(|_nbytes| ())
}
