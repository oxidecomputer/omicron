// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use futures::Future;
use tufaceous_artifact::{ArtifactHash, ArtifactHashId, KnownArtifactKind};

pub(crate) fn dummy_artifact_hash_id(
    kind: KnownArtifactKind,
) -> ArtifactHashId {
    ArtifactHashId {
        kind: kind.into(),
        hash: ArtifactHash(
            hex_literal::hex!("b5bb9d8014a0f9b1d61e21e796d78dcc" "df1352f23cd32812f4850b878ae4944c"),
        ),
    }
}

pub(crate) fn with_test_runtime<Fut, T>(fut: Fut) -> T
where
    Fut: Future<Output = T>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("tokio Runtime built successfully");
    runtime.block_on(fut)
}
