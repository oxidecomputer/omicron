// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collect SP task dumps for support bundles

use crate::app::background::tasks::support_bundle::cache::Cache;
use crate::app::background::tasks::support_bundle::collection::BundleCollection;
use crate::app::background::tasks::support_bundle::step::CollectionStep;
use crate::app::background::tasks::support_bundle::step::CollectionStepOutput;
use crate::app::background::tasks::support_bundle::steps;

use anyhow::Context;
use anyhow::bail;
use base64::Engine;
use camino::Utf8Path;
use futures::FutureExt;
use gateway_client::Client as MgsClient;
use gateway_client::types::SpIdentifier;

pub async fn spawn_collection_steps(
    collection: &BundleCollection,
    cache: &Cache,
    dir: &Utf8Path,
) -> anyhow::Result<CollectionStepOutput> {
    let request = collection.request();

    if !request.include_sp_dumps() {
        return Ok(CollectionStepOutput::Skipped);
    }

    let Some(mgs_client) = cache.get_or_initialize_mgs_client(collection).await
    else {
        bail!("Could not initialize MGS client");
    };

    let sp_dumps_dir = dir.join("sp_task_dumps");
    tokio::fs::create_dir_all(&sp_dumps_dir).await.with_context(|| {
        format!("Failed to create SP task dump directory {sp_dumps_dir}")
    })?;

    let mut extra_steps: Vec<CollectionStep> = vec![];
    for sp in steps::sled_cubby::get_available_sps(&mgs_client).await? {
        extra_steps.push(CollectionStep::new(
            format!("SP dump for {:?}", sp),
            Box::new({
                let mgs_client = mgs_client.clone();
                move |collection, dir| {
                    async move {
                        collect_sp_dump(collection, &mgs_client, sp, dir).await
                    }
                    .boxed()
                }
            }),
        ));
    }

    Ok(CollectionStepOutput::Spawn { extra_steps })
}

async fn collect_sp_dump(
    collection: &BundleCollection,
    mgs_client: &MgsClient,
    sp: SpIdentifier,
    dir: &Utf8Path,
) -> anyhow::Result<CollectionStepOutput> {
    if !collection.request().include_sp_dumps() {
        return Ok(CollectionStepOutput::Skipped);
    }

    save_sp_dumps(mgs_client, sp, dir).await.with_context(|| {
        format!("failed to save SP dump from: {} {}", sp.type_, sp.slot)
    })?;

    Ok(CollectionStepOutput::None)
}

async fn save_sp_dumps(
    mgs_client: &MgsClient,
    sp: SpIdentifier,
    sp_dumps_dir: &Utf8Path,
) -> anyhow::Result<()> {
    let dump_count = mgs_client
        .sp_task_dump_count(&sp.type_, sp.slot)
        .await
        .context("failed to get task dump count from SP")?
        .into_inner();

    let output_dir = sp_dumps_dir.join(format!("{}_{}", sp.type_, sp.slot));
    tokio::fs::create_dir_all(&output_dir).await.with_context(|| {
        format!("Failed to create output directory {output_dir}")
    })?;

    for i in 0..dump_count {
        let task_dump = mgs_client
            .sp_task_dump_get(&sp.type_, sp.slot, i)
            .await
            .with_context(|| format!("failed to get task dump {i} from SP"))?
            .into_inner();

        let zip_bytes = base64::engine::general_purpose::STANDARD
            .decode(task_dump.base64_zip)
            .context("failed to decode base64-encoded SP task dump zip")?;

        tokio::fs::write(output_dir.join(format!("dump-{i}.zip")), zip_bytes)
            .await
            .context("failed to write SP task dump zip to disk")?;
    }
    Ok(())
}
