// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collect SP task dumps for support bundles

use crate::cache::Cache;
use crate::collection::BundleCollection;
use crate::output::BundleOutput;
use crate::step::CollectionStep;
use crate::step::CollectionStepOutput;
use crate::step::StepError;
use crate::step::StepResultExt;
use crate::steps;

use anyhow::Context;
use anyhow::bail;
use base64::Engine;
use futures::FutureExt;
use gateway_client::Client as MgsClient;
use gateway_types::component::SpIdentifier;

pub async fn spawn_collection_steps(
    collection: &BundleCollection,
    cache: &Cache,
    output: &BundleOutput,
) -> Result<CollectionStepOutput, StepError> {
    if !collection.data_selection().contains_sp_dumps() {
        return Ok(CollectionStepOutput::Skipped);
    }

    let init_future = async {
        let Some(mgs_client) =
            cache.get_or_initialize_mgs_client(collection).await
        else {
            bail!("Could not initialize MGS client");
        };
        let sps = steps::sled_cubby::get_available_sps(&mgs_client).await?;
        Ok::<_, anyhow::Error>((mgs_client, sps))
    };
    let (mgs_client, available_sps) = tokio::select! {
        _ = collection.stopped() => return Ok(CollectionStepOutput::None),
        result = init_future => result?,
    };

    let sp_dumps_dir = output.join("sp_task_dumps");
    sp_dumps_dir.create_dir_all().await?;

    let mut extra_steps: Vec<CollectionStep> = vec![];
    for sp in available_sps {
        let mgs_client = mgs_client.clone();
        let sp_dumps_dir = sp_dumps_dir.clone();
        extra_steps.push(CollectionStep::new(
            format!("SP dump for {:?}", sp),
            Box::new(move |collection, _output| {
                async move {
                    collect_sp_dump(collection, &mgs_client, sp, &sp_dumps_dir)
                        .await
                }
                .boxed()
            }),
        ));
    }

    Ok(CollectionStepOutput::Spawn { extra_steps })
}

async fn collect_sp_dump(
    collection: &BundleCollection,
    mgs_client: &MgsClient,
    sp: SpIdentifier,
    dir: &BundleOutput,
) -> Result<CollectionStepOutput, StepError> {
    if !collection.data_selection().contains_sp_dumps() {
        return Ok(CollectionStepOutput::Skipped);
    }

    save_sp_dumps(collection, mgs_client, sp, dir).await.with_context(
        || format!("failed to save SP dump from: {} {}", sp.typ, sp.slot),
    )?;

    Ok(CollectionStepOutput::None)
}

// Download and save SP task dumps.
//
// # Cancel safety
//
// Cancel-**unsafe**: writes to the bundle.
// HTTP requests to MGS are eagerly cancelled via `select!`.
async fn save_sp_dumps(
    collection: &BundleCollection,
    mgs_client: &MgsClient,
    sp: SpIdentifier,
    sp_dumps_dir: &BundleOutput,
) -> Result<(), StepError> {
    let fetch_dumps = async {
        let dump_count = mgs_client
            .sp_task_dump_count(&sp.typ, sp.slot)
            .await
            .context("failed to get task dump count from SP")?
            .into_inner();

        let mut dumps = Vec::with_capacity(dump_count as usize);
        for i in 0..dump_count {
            let task_dump = mgs_client
                .sp_task_dump_get(&sp.typ, sp.slot, i)
                .await
                .with_context(|| {
                    format!("failed to get task dump {i} from SP")
                })?
                .into_inner();

            let zip_bytes = base64::engine::general_purpose::STANDARD
                .decode(task_dump.base64_zip)
                .context("failed to decode base64-encoded SP task dump zip")?;
            dumps.push((i, zip_bytes));
        }
        Ok::<_, anyhow::Error>(dumps)
    };
    let dumps = tokio::select! {
        _ = collection.stopped() => return Ok(()),
        result = fetch_dumps => result?,
    };

    let output_dir = sp_dumps_dir.join(format!("{}_{}", sp.typ, sp.slot));
    output_dir.create_dir_all().await?;

    for (i, zip_bytes) in dumps {
        output_dir
            .write(format!("dump-{i}.zip"), zip_bytes)
            .await
            .context("failed to write SP task dump zip to disk")?;
    }
    Ok(())
}
