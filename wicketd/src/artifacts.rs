// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use buf_list::BufList;
use bytes::{BufMut, BytesMut};
use debug_ignore::DebugIgnore;
use futures::stream;
use hyper::Body;
use installinator_artifactd::{ArtifactGetter, ArtifactId};

/// The artifact store for wicketd.
///
/// This can be cheaply cloned, and is intended to be shared across the parts of artifactd that
/// upload artifacts and the parts that fetch them.
#[derive(Clone, Debug)]
pub(crate) struct WicketdArtifactStore {
    log: slog::Logger,
    // NOTE: this is a `std::sync::Mutex` rather than a `tokio::sync::Mutex` because the critical
    // sections are extremely small.
    artifacts: Arc<Mutex<DebugIgnore<HashMap<ArtifactId, BufList>>>>,
}

impl WicketdArtifactStore {
    pub(crate) fn new(log: &slog::Logger) -> Self {
        let log = log.new(slog::o!("component" => "wicketd artifact store"));
        Self { log, artifacts: Default::default() }
    }

    fn get(&self, id: &ArtifactId) -> Option<BufList> {
        // NOTE: cloning a `BufList` is cheap since it's just a bunch of reference count bumps.
        // Cloning it here also means we can release the lock quickly.
        self.artifacts.lock().unwrap().get(id).cloned()
    }

    #[allow(dead_code)]
    fn insert(&self, id: ArtifactId, buf: BufList) {
        self.artifacts.lock().unwrap().insert(id, buf);
    }
}

#[async_trait]
impl ArtifactGetter for WicketdArtifactStore {
    async fn get(&self, id: &ArtifactId) -> Option<Body> {
        // This is a test artifact name used by the installinator.
        if id.name == "__installinator-test" {
            // For testing, the version is the size of the artifact.
            let size: usize = id
                        .version
                        .parse()
                        .map_err(|err| {
                            slog::warn!(
                                self.log,
                                "for installinator-test, version should be a usize indicating the size but found {}: {err}",
                                id.version
                            );
                        })
                        .ok()?;
            let mut bytes = BytesMut::with_capacity(size as usize);
            bytes.put_bytes(0, size);
            return Some(Body::from(bytes.freeze()));
        }

        let buf_list = self.get(id)?;
        // Return the list as a stream of bytes.
        Some(Body::wrap_stream(stream::iter(
            buf_list.into_iter().map(|bytes| Ok::<_, Infallible>(bytes)),
        )))
    }
}
