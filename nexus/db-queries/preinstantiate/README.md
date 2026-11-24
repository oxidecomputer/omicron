# DataStore pre-instantiation shards

The crates in this directory compile the bodies of `DataStore`'s async methods
ahead of time, so that `omicron-nexus` doesn't have to compile them itself. This
speeds up incremental builds of Nexus after an edit to `nexus-db-queries`.

For how this works, see the [`preinstantiate`](https://docs.rs/preinstantiate)
documentation.
