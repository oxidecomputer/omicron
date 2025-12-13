// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collect host information from sleds for support bundles

use crate::app::background::tasks::support_bundle::cache::Cache;
use crate::app::background::tasks::support_bundle::collection::BundleCollection;
use crate::app::background::tasks::support_bundle::step::CollectionStep;
use crate::app::background::tasks::support_bundle::step::CollectionStepOutput;

use anyhow::Context;
use anyhow::bail;
use camino::Utf8Path;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::Future;
use futures::stream::FuturesUnordered;
use nexus_db_model::Sled;
use nexus_networking;
use nexus_types::identity::Asset;
use tokio::io::AsyncWriteExt;

pub async fn spawn_query_all_sleds(
    collection: &BundleCollection,
    cache: &Cache,
) -> anyhow::Result<CollectionStepOutput> {
    let request = collection.request();

    if !request.include_host_info() {
        return Ok(CollectionStepOutput::Skipped);
    }

    let all_sleds = cache.get_or_initialize_all_sleds(collection).await;

    let Some(all_sleds) = all_sleds else {
        bail!("Could not read list of sleds");
    };

    let mut extra_steps: Vec<CollectionStep> = vec![];
    for sled in all_sleds {
        if !request.include_sled_host_info(sled.id()) {
            continue;
        }

        let sled = sled.clone();
        extra_steps.push(CollectionStep::new(
            format!("sled data for sled {}", sled.id()),
            Box::new({
                move |collection, dir| {
                    async move {
                        collect_data_from_sled(collection, sled, dir).await
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
// - "dir" is a directory where data can be stored, to be turned
// into a bundle after collection completes.
async fn collect_data_from_sled(
    collection: &BundleCollection,
    sled: Sled,
    dir: &Utf8Path,
) -> anyhow::Result<CollectionStepOutput> {
    let (log, opctx, datastore, request) = (
        collection.log(),
        collection.opctx(),
        collection.datastore(),
        collection.request(),
    );

    if !request.include_sled_host_info(sled.id()) {
        return Ok(CollectionStepOutput::Skipped);
    }

    info!(log, "Collecting bundle info from sled"; "sled" => %sled.id());
    let sled_path = dir
        .join("rack")
        .join(sled.rack_id.to_string())
        .join("sled")
        .join(sled.id().to_string());
    tokio::fs::create_dir_all(&sled_path).await?;
    tokio::fs::write(sled_path.join("sled.txt"), format!("{sled:?}")).await?;

    let sled_client = match nexus_networking::sled_client(
        &datastore,
        &opctx,
        sled.id(),
        log,
    )
    .await
    {
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

    // NB: As new sled-diagnostic commands are added they should
    // be added to this array so that their output can be saved
    // within the support bundle.
    let mut diag_cmds = futures::stream::iter([
        save_diag_cmd_output_or_error(
            &sled_path,
            "zoneadm",
            sled_client.support_zoneadm_info(),
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "dladm",
            sled_client.support_dladm_info(),
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "ipadm",
            sled_client.support_ipadm_info(),
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "nvmeadm",
            sled_client.support_nvmeadm_info(),
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "pargs",
            sled_client.support_pargs_info(),
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "pfiles",
            sled_client.support_pfiles_info(),
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "pstack",
            sled_client.support_pstack_info(),
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "zfs",
            sled_client.support_zfs_info(),
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "zpool",
            sled_client.support_zpool_info(),
        )
        .boxed(),
        save_diag_cmd_output_or_error(
            &sled_path,
            "health-check",
            sled_client.support_health_check(),
        )
        .boxed(),
    ])
    // Currently we execute up to 10 commands concurrently which
    // might be doing their own concurrent work, for example
    // collectiong `pstack` output of every Oxide process that is
    // found on a sled.
    .buffer_unordered(10);

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

    // For each zone we concurrently fire off a request to its
    // sled-agent to collect its logs in a zip file and write the
    // result to the support bundle.
    let zones = sled_client.support_logs().await?.into_inner();
    let mut log_futs: FuturesUnordered<_> = zones
        .iter()
        .map(|zone| {
            save_zone_log_zip_or_error(log, &sled_client, zone, &sled_path)
        })
        .collect();

    while let Some(log_collection_result) = log_futs.next().await {
        // We log any errors saving the zip file to disk and
        // continue on.
        if let Err(e) = log_collection_result {
            error!(log, "failed to write logs output: {e}");
        }
    }
    Ok(CollectionStepOutput::None)
}

// Run a `sled-dianostics` future and save its output to a corresponding file.
async fn save_diag_cmd_output_or_error<F, S: serde::Serialize>(
    path: &Utf8Path,
    command: &str,
    future: F,
) -> anyhow::Result<()>
where
    F: Future<
            Output = Result<
                sled_agent_client::ResponseValue<S>,
                sled_agent_client::Error<sled_agent_client::types::Error>,
            >,
        > + Send,
{
    let result = future.await;
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
            tokio::fs::write(
                path.join(format!("{command}_err.txt")),
                err.to_string(),
            )
            .await?;
        }
    }
    Ok(())
}

async fn save_zone_log_zip_or_error(
    logger: &slog::Logger,
    client: &sled_agent_client::Client,
    zone: &str,
    path: &Utf8Path,
) -> anyhow::Result<()> {
    // In the future when support bundle collection exposes tuning parameters
    // this can turn into a collection parameter.
    const DEFAULT_MAX_ROTATED_LOGS: u32 = 5;

    match client.support_logs_download(zone, DEFAULT_MAX_ROTATED_LOGS).await {
        Ok(res) => {
            let bytestream = res.into_inner();
            let output_dir = path.join(format!("logs/{zone}"));
            let output_path = output_dir.join("logs.zip");

            // Ensure the logs output directory exists.
            tokio::fs::create_dir_all(&output_dir).await.with_context(
                || format!("failed to create output directory: {output_dir}"),
            )?;

            // Stream the log zip file to disk.
            let mut file =
                tokio::fs::File::create(&output_path).await.with_context(
                    || format!("failed to create log zip file: {output_path}"),
                )?;

            let stream = bytestream.into_inner().map(|chunk| {
                chunk.map_err(|e| std::io::Error::other(e.to_string()))
            });
            let mut reader = tokio_util::io::StreamReader::new(stream);
            let _nbytes = tokio::io::copy(&mut reader, &mut file).await?;
            file.flush().await?;

            // Unzip the log file into the same directory.
            let output_path_unzip = output_dir.join("unzipped_logs");
            let zipfile_path = output_path.clone();
            tokio::task::spawn_blocking(move || {
                extract_zip_file(&output_path_unzip, &zipfile_path)
            })
            .await
            .map_err(|join_error| {
                anyhow::anyhow!(join_error)
                    .context("unzipping support bundle logs zip panicked")
            })??;

            // Clean up the zip file that was written to disk.
            if let Err(e) = tokio::fs::remove_file(&output_path).await {
                error!(
                    logger,
                    "failed to cleanup temporary logs zip file";
                    "error" => %e,
                    "file" => %output_path,

                );
            }
        }
        Err(err) => {
            tokio::fs::write(
                path.join(format!("{zone}.logs.err")),
                err.to_string(),
            )
            .await?;
        }
    };

    Ok(())
}

fn extract_zip_file(
    output_dir: &Utf8Path,
    zip_file: &Utf8Path,
) -> Result<(), anyhow::Error> {
    let mut zip = std::fs::File::open(&zip_file)
        .with_context(|| format!("failed to open zip file: {zip_file}"))?;
    let mut archive = zip::ZipArchive::new(&mut zip)?;
    archive.extract(&output_dir).with_context(|| {
        format!("failed to extract log zip file to: {output_dir}")
    })?;
    Ok(())
}
