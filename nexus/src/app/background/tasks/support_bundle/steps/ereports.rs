// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collect ereports for support bundles

use crate::app::background::tasks::support_bundle::collection::BundleCollection;
use crate::app::background::tasks::support_bundle::step::CollectionStepOutput;

use anyhow::Context;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore;
use nexus_db_queries::db::datastore::EreportFilters;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::fm::Ereport;
use nexus_types::internal_api::background::SupportBundleEreportStatus;
use omicron_uuid_kinds::GenericUuid;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;

pub async fn collect(
    collection: &BundleCollection,
    dir: &Utf8Path,
) -> anyhow::Result<CollectionStepOutput> {
    let (log, opctx, datastore, request) = (
        collection.log(),
        collection.opctx(),
        collection.datastore(),
        collection.request(),
    );
    let ereport_filters = request.get_ereport_filters();

    let Some(ereport_filters) = ereport_filters else {
        debug!(log, "Support bundle: ereports not requested");
        return Ok(CollectionStepOutput::Skipped);
    };
    let ereports_dir = dir.join("ereports");
    let mut status = SupportBundleEreportStatus::default();
    if let Err(err) = save_ereports(
        log,
        opctx,
        datastore,
        ereport_filters.clone(),
        ereports_dir,
        &mut status,
    )
    .await
    {
        warn!(
            log,
            "Support bundle: ereport collection failed \
             ({} collected successfully)",
             status.n_collected;
            InlineErrorChain::new(err.as_ref())
        );
        status.errors.push(InlineErrorChain::new(err.as_ref()).to_string());
    };

    Ok(CollectionStepOutput::Ereports(status))
}

async fn save_ereports(
    log: &Logger,
    opctx: &OpContext,
    datastore: &Arc<DataStore>,
    filters: EreportFilters,
    dir: Utf8PathBuf,
    status: &mut SupportBundleEreportStatus,
) -> anyhow::Result<()> {
    let mut paginator = Paginator::new(
        datastore::SQL_BATCH_SIZE,
        dropshot::PaginationOrder::Ascending,
    );
    while let Some(p) = paginator.next() {
        let ereports = datastore
            .ereport_fetch_matching(&opctx, &filters, &p.current_pagparams())
            .await
            .map_err(|e| e.internal_context("failed to query for ereports"))?;
        paginator = p.found_batch(&ereports, &|ereport| {
            (ereport.restart_id.into_untyped_uuid(), ereport.ena)
        });

        let prev_n_collected = status.n_collected;
        let n_ereports = ereports.len();
        status.n_found += n_ereports;

        for ereport in ereports {
            match ereport.try_into() {
                Ok(ereport) => {
                    write_ereport(ereport, &dir).await?;
                    status.n_collected += 1;
                }
                Err(err) => {
                    warn!(log, "invalid ereport"; "error" => %err);
                    status.errors.push(err.to_string());
                }
            }
        }
        debug!(
            log,
            "Support bundle: added {} ereports ({} found)",
            status.n_collected - prev_n_collected,
            n_ereports
        );
    }

    info!(
        log,
        "Support bundle: collected {} total ereports", status.n_collected
    );
    Ok(())
}

async fn write_ereport(ereport: Ereport, dir: &Utf8Path) -> anyhow::Result<()> {
    // Here's where we construct the file path for each ereport JSON file,
    // given the top-level ereport directory path.  Each ereport is stored in a
    // subdirectory for the part and serial numbers of the system that produced
    // the ereport.  Part numbers must be included in addition to serial
    // numbers, as the v1 serial scheme only guarantees uniqueness within a
    // part number.  These paths take the following form:
    //
    //   {part-number}-{serial_number}/{restart_id}/{ENA}.json
    //
    // We can assume that the restart ID and ENA consist only of
    // filesystem-safe characters, as the restart ID is known to be a UUID, and
    // the ENA is just an integer.  For the serial and part numbers, which
    // Nexus doesn't have full control over --- it came from the ereport
    // metadata --- we must check that it doesn't contain any characters
    // unsuitable for use in a filesystem path.
    let pn = ereport
        .data
        .part_number
        .as_deref()
        // If the part or serial numbers contain any unsavoury characters, it
        // goes in the `unknown_serial` hole! Note that the alleged serial
        // number from the ereport will still be present in the JSON as a
        // string, so we're not *lying* about what was received; we're just
        // giving up on using it in the path.
        .filter(|&s| is_fs_safe_single_path_component(s))
        .unwrap_or("unknown_part");
    let sn = ereport
        .data
        .serial_number
        .as_deref()
        .filter(|&s| is_fs_safe_single_path_component(s))
        .unwrap_or("unknown_serial");
    let id = &ereport.data.id;

    let dir = dir
        .join(format!("{pn}-{sn}"))
        // N.B. that we call `into_untyped_uuid()` here, as the `Display`
        // implementation for a typed UUID appends " (ereporter_restart)", which
        // we don't want.
        .join(id.restart_id.into_untyped_uuid().to_string());
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("failed to create directory '{dir}'"))?;
    let file_path = dir.join(format!("{}.json", id.ena));
    let json = serde_json::to_vec(&ereport).with_context(|| {
        format!("failed to serialize ereport {pn}:{sn}/{id}")
    })?;
    tokio::fs::write(&file_path, json)
        .await
        .with_context(|| format!("failed to write '{file_path}'"))
}

fn is_fs_safe_single_path_component(s: &str) -> bool {
    // Might be path traversal...
    if s == "." || s == ".." {
        return false;
    }

    if s == "~" {
        return false;
    }

    const BANNED_CHARS: &[char] = &[
        // Check for path separators.
        //
        // Naively, we might reach for `std::path::is_separator()` here.
        // However, this function only checks if a path is a permitted
        // separator on the *current* platform --- so, running on illumos, we
        // will only check for Unix path separators.  But, because the support
        // bundle may be extracted on a workstation system by Oxide support
        // personnel or by the customer, we should also make sure we don't
        // allow the use of Windows path separators, which `is_separator()`
        // won't check for on Unix systems.
        '/', '\\',
        // Characters forbidden on Windows, per:
        // https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-file#naming-conventions
        '<', '>', ':', '"', '|', '?', '*',
    ];

    // Rather than using `s.contains()`, we do all the checks in one pass.
    for c in s.chars() {
        if BANNED_CHARS.contains(&c) {
            return false;
        }

        // Definitely no control characters!
        if c.is_control() {
            return false;
        }
    }

    true
}
