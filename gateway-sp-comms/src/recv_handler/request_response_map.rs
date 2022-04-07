// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use futures::Future;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Mutex;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;
use tokio::sync::oneshot::Sender;

/// Possible outcomes of ingesting a response.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum ResponseIngestResult {
    Ok,
    UnknownRequestId,
}

/// An in-memory map providing paired request/response functionality to async
/// tasks. Both tasks must know the key of the request.
#[derive(Debug)]
pub(super) struct RequestResponseMap<K, T> {
    requests: Mutex<HashMap<K, Sender<T>>>,
}

// this can't be derived for T: !Default, but we don't need T: Default
impl<K, T> Default for RequestResponseMap<K, T> {
    fn default() -> Self {
        Self { requests: Mutex::default() }
    }
}

impl<K, T> RequestResponseMap<K, T>
where
    K: Hash + Eq + Clone,
{
    /// Register a key to be paired with a future call to `ingest_response()`.
    /// Returns a [`Future`] that will complete once that call is made.
    ///
    /// Panics if `key` is currently in use by another
    /// `wait_for_response()` call on `self`.
    pub(super) fn wait_for_response(
        &self,
        key: K,
    ) -> impl Future<Output = T> + '_ {
        // construct a oneshot channel, and wrap the receiving half in a type
        // that will clean up `self.requests` if the future we return is dropped
        // (typically due to timeout/cancellation)
        let (tx, rx) = oneshot::channel();
        let mut wrapped_rx =
            RemoveOnDrop { rx, requests: &self.requests, key: key.clone() };

        // insert the sending half into `self.requests` for use by
        // `ingest_response()` later.
        let old = self.requests.lock().unwrap().insert(key, tx);

        // we always clean up `self.requests` after receiving a response (or
        // timing out), so we should never see request ID reuse even if they
        // roll over. assert that here to ensure we don't get mysterious
        // misbehavior if that turns out to be incorrect
        assert!(old.is_none(), "request ID reuse");

        async move {
            // wait for someone to call `ingest_response()` with our request id
            match (&mut wrapped_rx.rx).await {
                Ok(inner_result) => inner_result,
                Err(_recv_error) => {
                    // we hold the sending half in `self.requests` until either
                    // `wrapped_rx`'s `Drop` impl removes it (in which case
                    // we're not running anymore), or it's consumed to send the
                    // result to us. receiving therefore can't fail
                    unreachable!()
                }
            }
        }
    }

    /// Ingest a response, which will cause the corresponding
    /// `wait_for_response()` future to be fulfilled (if it exists).
    pub(super) fn ingest_response(
        &self,
        key: &K,
        response: T,
    ) -> ResponseIngestResult {
        // get the sending half of the channel created in `wait_for_response()`,
        // if it exists (i.e., `wait_for_response()` was actually called with
        // `key`, and the returned future from it hasn't been dropped)
        let tx = match self.requests.lock().unwrap().remove(key) {
            Some(tx) => tx,
            None => return ResponseIngestResult::UnknownRequestId,
        };

        // we got `tx`, so the receiving end existed a moment ago, but there's a
        // race here where it could be dropped before we're able to send
        // `result` through, so we can't unwrap this send; we treat this failure
        // the same as not finding `tx`, because if we had tried to get `tx` a
        // moment later it would not have been there.
        match tx.send(response) {
            Ok(()) => ResponseIngestResult::Ok,
            Err(_) => ResponseIngestResult::UnknownRequestId,
        }
    }
}

/// `RemoveOnDrop` is a light wrapper around a [`Receiver`] that removes the
/// associated key from the `HashMap` it's given once the response has been
/// received (or the `RemoveOnDrop` itself is dropped).
#[derive(Debug)]
struct RemoveOnDrop<'a, K, T>
where
    K: Hash + Eq,
{
    rx: Receiver<T>,
    requests: &'a Mutex<HashMap<K, Sender<T>>>,
    key: K,
}

impl<K, T> Drop for RemoveOnDrop<'_, K, T>
where
    K: Hash + Eq,
{
    fn drop(&mut self) {
        // we don't care to check the return value here; this will be `Some(_)`
        // if we're being dropped before a response has been received and
        // forwarded on to our caller, and `None` if not (because a caller will
        // have already extracted the sending channel)
        let _ = self.requests.lock().unwrap().remove(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[tokio::test]
    async fn basic_usage() {
        let os = RequestResponseMap::default();

        // ingesting a response before waiting for it doesn't work
        assert_eq!(
            os.ingest_response(&1, "hi"),
            ResponseIngestResult::UnknownRequestId
        );

        // ingesting a response after waiting for it works, and the receiving
        // half gets the ingested response
        let resp = os.wait_for_response(1);
        assert_eq!(os.ingest_response(&1, "hello"), ResponseIngestResult::Ok);
        assert_eq!(resp.await, "hello");
    }

    #[tokio::test]
    async fn dropping_future_cleans_up_key() {
        let os = RequestResponseMap::default();

        // register interest in a response, but then drop the returned future
        let resp = os.wait_for_response(1);
        mem::drop(resp);

        // attempting to ingest the corresponding response should now fail
        assert_eq!(
            os.ingest_response(&1, "hi"),
            ResponseIngestResult::UnknownRequestId
        );
    }
}
