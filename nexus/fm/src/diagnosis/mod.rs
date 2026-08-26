// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management diagnosis engines.
//!
//! Each submodule defines one diagnosis engine (DE). `analyze` dispatches to
//! each engine in turn; engines are deterministic and idempotent per RFD 603.

use crate::SitrepBuilder;

mod physical_disk;
mod power_shelf;
mod saga;

pub fn analyze(builder: &mut SitrepBuilder<'_>) -> anyhow::Result<()> {
    physical_disk::analyze(builder)?;
    power_shelf::analyze(builder)?;
    saga::analyze(builder)?;
    Ok(())
}

/// Ereport classes that any diagnosis engine in this build of Nexus knows
/// how to consume. The background task uses this to filter loaded ereports.
///
/// **NULL-class ereports are intentionally excluded by the loader's SQL
/// filter** (`class = ANY(...)` never matches NULL). If FM analysis ever
/// needs to handle the "couldn't extract a class" or "reporter doesn't know
/// its identity" cases, that's an explicit decision (e.g. a sentinel
/// loader path), not a default of this list.
///
/// # Scaling
///
/// The loader filters ereports via `WHERE class = ANY($1::text[])` against
/// the existing `lookup_ereports_by_class` index. This is comfortable up to
/// a few hundred entries; past ~1000 entries, prefer either prefix matching
/// (`class LIKE 'ereport.cpu.amd.%'`) or a `known_ereport_class` lookup
/// table joined into the query. Revisit this if the list grows that large.
pub fn known_ereport_classes() -> &'static [&'static str] {
    &[power_shelf::PSU_INSERT_EREPORT, power_shelf::PSU_REMOVE_EREPORT]
}
