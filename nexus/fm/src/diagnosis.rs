// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::SitrepBuilder;
use crate::analysis_input::Input;

pub fn analyze(
    _input: &Input,
    _builder: &mut SitrepBuilder<'_>,
) -> anyhow::Result<()> {
    anyhow::bail!("FM analysis is not yet implemented")
}

/// Ereport classes that the diagnosis engine currently understands.
/// Preparation only surfaces ereports whose class is in this set — there is
/// no value in loading ereports FM analysis cannot consume.
///
/// Empty until [`analyze`] gains real handling. Grow this alongside FM
/// analysis as new classes gain support.
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
    &[]
}
