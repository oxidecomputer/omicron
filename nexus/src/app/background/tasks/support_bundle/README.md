# Support Bundles

**Support Bundles** provide a mechanism for extracting information about a
running Oxide system, and giving operators control over the exfiltration of that
data.

This README is intended for developers trying to add data to the bundle.

## Step Execution Framework

Support Bundles are collected using **steps**, which are named functions acting
on the `BundleCollection` that can:

* Read from the database, or query arbitrary services
* Emit data to the output zipfile
* Produce additional follow-up **steps**, if necessary

If you're interested in adding data to a support bundle, you will probably be
adding data to an existing **step**, or creating a new one.

The set of all initial steps is defined in
`nexus/src/app/background/tasks/support_bundle/steps/mod.rs`, within a function
called `all()`. Some of these steps may themselves spawn additional steps,
such as `STEP_SPAWN_SLEDS`, which spawns a per-sled step to query the sled
host OS itself.

### Tracing

**Steps** are automatically instrumented, and their durations are emitted to an
output file in the bundle named `meta/trace.json`. These traces are in a format
which can be understood by **Perfetto**, a trace-viewer, and which provides
a browser-based interface at <https://ui.perfetto.dev/>.

## Filtering Bundle Contents

Support Bundles are collected by the `support_bundle_collector`
background task. They are collected as zipfiles within a single Nexus instance,
which are then transferred to durable storage.

The contents of a bundle may be controlled by modifying the **BundleRequest**
structure. This request provides filters for controlling the categories of
data which are collected (e.g., "Host OS info") as well as arguments for
more specific constraints (e.g., "Collect info from a specific Sled").

Bundle **steps** may query the `BundleRequest` to identify whether or not their
contents should be included.

## Overview for adding new data

* **Determine if your data should exist in a new step**. The existing set of
  steps exists in `support_bundle/steps`. Adding a new step provides a new unit
  of execution (it can be executed concurrently with other steps), and a unit of
  tracing (it will be instrumented independently of other steps).
* If you're adding a new step...
  * **Add it as a new module**, within `support_bundle/steps`.
  * **Ensure it's part of `steps::all()`, or spawned by an existing step**. This
  will be necessary for your step to be executed.
  * **Provide a way for bundles to opt-out of collecting this data**. Check the
  `BundleRequest` to see if your data exists in one of the current filters, or
  consider adding a new one if your step involves a new category of data. Either
  way, your new step should read `BundleRequest` to decide if it should trigger
  before performing any subsequent operations.
* **Consider Caching**. If your new data requires performing any potentially
  expensive operations which might be shared with other steps (e.g., reading
  from the database, creating and using progenitor clients, etc) consider adding
  that data to `support_bundle/cache`.

## Bundle Directory Structure

The following is the convention for Support Bundle files. It can, and should,
change over time. However, we list it here to make sure data is located
somewhere consistent and predictable.

(Please keep this list alphabetized)

* `bundle_id.txt` - UUID of the bundle itself
* `ereports/` - All requested error reports
* `ereports/{part number}-{serial number}/{id}.json` - Individual reports
* `meta/` - Metadata about the bundle
* `meta/trace.json` - Perfetto-formatted trace of the bundle's collection
* `omdb/` - Output from omdb commands
* `rack/{rack id}/sled/{sled id}/` - Sled-specific host OS info
* `reconfigurator_state.json` - A dump of all reconfigurator state
* `sled_info.json` - Mapping of sled identifiers to cubby location
* `sp_task_dumps/` - All SP dumps
* `sp_task_dumps/{SP type}_{SP slot}/dump-{id}.zip` - Individual SP dumps
