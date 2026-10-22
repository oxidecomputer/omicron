// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collect SP component details for support bundles

use crate::cache::Cache;
use crate::collection::BundleCollection;
use crate::step::CollectionStep;
use crate::step::CollectionStepOutput;
use crate::steps;

use anyhow::Context;
use anyhow::bail;
use camino::Utf8Path;
use futures::FutureExt;
use gateway_client::Client as MgsClient;
use gateway_client::types::SpComponentDetails;
use gateway_client::types::SpComponentInfo;
use gateway_types::component::SpIdentifier;
use serde::Serialize;
use slog_error_chain::InlineErrorChain;

pub async fn spawn_collection_steps(
    collection: &BundleCollection,
    cache: &Cache,
    dir: &Utf8Path,
) -> anyhow::Result<CollectionStepOutput> {
    if !collection.data_selection().contains_sp_components() {
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
        _ = collection.cancelled() => return Ok(CollectionStepOutput::None),
        result = init_future => result?,
    };

    let sp_components_dir = dir.join("sp_components");
    tokio::fs::create_dir_all(&sp_components_dir).await.with_context(|| {
        format!("Failed to create SP components directory {sp_components_dir}")
    })?;

    let mut extra_steps: Vec<CollectionStep> = vec![];
    for sp in available_sps {
        let mgs_client = mgs_client.clone();
        let sp_components_dir = sp_components_dir.clone();
        extra_steps.push(CollectionStep::new(
            format!("SP components for {:?}", sp),
            Box::new(move |collection, _dir| {
                async move {
                    collect_sp_components(
                        collection,
                        &mgs_client,
                        sp,
                        &sp_components_dir,
                    )
                    .await
                }
                .boxed()
            }),
        ));
    }

    Ok(CollectionStepOutput::Spawn { extra_steps })
}

async fn collect_sp_components(
    collection: &BundleCollection,
    mgs_client: &MgsClient,
    sp: SpIdentifier,
    dir: &Utf8Path,
) -> anyhow::Result<CollectionStepOutput> {
    save_sp_components(collection, mgs_client, sp, dir).await.with_context(
        || format!("failed to save SP components from: {} {}", sp.typ, sp.slot),
    )?;

    Ok(CollectionStepOutput::None)
}

/// The information MGS reports for an SP component.
#[derive(Serialize)]
struct SpComponentEntry {
    #[serde(flatten)]
    info: SpComponentInfo,
    /// Empty if the component does not support details, or if fetching
    /// them failed.
    details: Vec<SpComponentDetails>,
    /// Set if fetching details failed, or if the component was not queried
    /// because an earlier request to the SP failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

// Fetch details for every component of an SP and save them as JSON.
//
// # Cancel safety
//
// Cancel-**unsafe**: writes to the filesystem via `tokio::fs`.
// HTTP requests to MGS are eagerly cancelled via `select!`.
async fn save_sp_components(
    collection: &BundleCollection,
    mgs_client: &MgsClient,
    sp: SpIdentifier,
    sp_components_dir: &Utf8Path,
) -> anyhow::Result<()> {
    let file_stem = format!("{}_{}", sp.typ, sp.slot);
    let fetch_components = async {
        let components = mgs_client
            .sp_component_list(&sp.typ, sp.slot)
            .await?
            .into_inner()
            .components;

        let mut output = Vec::with_capacity(components.len());
        let mut sp_error = None;

        for info in components {
            // If calling the SP has failed, assume it will continue to do so
            // and skip any remaining components.
            if let Some(sp_error) = &sp_error {
                output.push(SpComponentEntry {
                    info,
                    details: Vec::new(),
                    error: Some(format!("not queried: {sp_error}")),
                });
                continue;
            }

            let (details, error) = match mgs_client
                .sp_component_get(&sp.typ, sp.slot, &info.component)
                .await
            {
                Ok(details) => (details.into_inner(), None),
                Err(gateway_client::Error::ErrorResponse(rsp))
                    if rsp.error_code.as_deref()
                        == Some("RequestUnsupportedForComponent") =>
                {
                    (Vec::new(), None)
                }
                Err(e) => {
                    let error = InlineErrorChain::new(&e).to_string();
                    if is_sp_unavailable(&e) {
                        sp_error = Some(error.clone());
                    }
                    (Vec::new(), Some(error))
                }
            };
            output.push(SpComponentEntry { info, details, error });
        }
        Ok::<_, gateway_client::Error<gateway_client::types::Error>>((
            output, sp_error,
        ))
    };
    let result = tokio::select! {
        _ = collection.cancelled() => return Ok(()),
        result = fetch_components => result,
    };
    let (components, sp_error) = match result {
        Ok(fetched) => fetched,
        Err(e) => {
            let error = format!(
                "failed to list SP components: {}",
                InlineErrorChain::new(&e)
            );
            let error_path = sp_components_dir.join(format!("{file_stem}.err"));
            tokio::fs::write(&error_path, &error).await.with_context(|| {
                format!("failed to write SP component error to {error_path}")
            })?;
            bail!(error);
        }
    };

    let json = serde_json::to_string_pretty(&components)
        .context("failed to serialize SP component details to JSON")?;
    let output_path = sp_components_dir.join(format!("{file_stem}.json"));
    tokio::fs::write(&output_path, json).await.with_context(|| {
        format!("failed to write SP component details to {output_path}")
    })?;

    if let Some(sp_error) = sp_error {
        bail!("stopped querying components after SP failure: {sp_error}");
    }
    Ok(())
}

/// Returns true if MGS is unreachable or returned a server error, in which
/// case further requests to the same SP are likely to fail too.
fn is_sp_unavailable(
    err: &gateway_client::Error<gateway_client::types::Error>,
) -> bool {
    matches!(err, gateway_client::Error::CommunicationError(_))
        || err.status().is_some_and(|status| status.is_server_error())
}
