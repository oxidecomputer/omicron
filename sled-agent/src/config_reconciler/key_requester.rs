// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use key_manager::StorageKeyRequester;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::oneshot;

#[derive(Debug, Clone, Copy)]
pub enum KeyRequesterStatus {
    WaitingForKeyManager,
    Ready,
}

#[derive(Clone, Debug)]
pub struct KeyManagerWaiter {
    release_from_escrow: Arc<Mutex<ReleaseFromEscrow>>,
}

impl KeyManagerWaiter {
    #[cfg(test)]
    pub(super) fn fake_key_manager_waiter() -> Self {
        let (release_from_escrow, _rx) = spawn_escrow_task(());
        let release_from_escrow = Arc::new(Mutex::new(release_from_escrow));
        Self { release_from_escrow }
    }

    pub fn hold_requester_until_key_manager_ready(
        key_requester: StorageKeyRequester,
    ) -> (Self, oneshot::Receiver<StorageKeyRequester>) {
        let (release_from_escrow, rx) = spawn_escrow_task(key_requester);
        let release_from_escrow = Arc::new(Mutex::new(release_from_escrow));
        (Self { release_from_escrow }, rx)
    }

    pub fn status(&self) -> KeyRequesterStatus {
        if self.release_from_escrow.lock().unwrap().is_released() {
            KeyRequesterStatus::Ready
        } else {
            KeyRequesterStatus::WaitingForKeyManager
        }
    }

    pub fn notify_key_manager_ready(&self) {
        self.release_from_escrow.lock().unwrap().release();
    }
}

#[derive(Debug)]
struct ReleaseFromEscrow(Option<oneshot::Sender<()>>);

impl ReleaseFromEscrow {
    fn is_released(&self) -> bool {
        self.0.is_none()
    }

    fn release(&mut self) {
        let Some(tx) = self.0.take() else {
            // If the tx channel is already gone, we've already released it.
            return;
        };
        // We don't care whether the spawned escrow task is gone.
        _ = tx.send(());
    }
}

fn spawn_escrow_task<T: Send + 'static>(
    item: T,
) -> (ReleaseFromEscrow, oneshot::Receiver<T>) {
    let (notify_tx, notify_rx) = oneshot::channel();
    let (released_tx, released_rx) = oneshot::channel();

    tokio::spawn(async move {
        match notify_rx.await {
            Ok(()) => (),
            Err(_) => return,
        }

        _ = released_tx.send(item);
    });

    (ReleaseFromEscrow(Some(notify_tx)), released_rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_escrow_basic() {
        let (mut releaser, mut rx) = spawn_escrow_task(10);

        assert!(rx.try_recv().is_err());
        assert!(!releaser.is_released());

        releaser.release();
        assert!(releaser.is_released());

        let released = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("received held value before timeout");

        assert_eq!(released, Ok(10));
    }

    #[tokio::test]
    async fn test_escrow_release_multiple_times() {
        let (mut releaser, mut rx) = spawn_escrow_task(10);

        assert!(rx.try_recv().is_err());
        assert!(!releaser.is_released());

        releaser.release();
        assert!(releaser.is_released());
        releaser.release();
        assert!(releaser.is_released());
        releaser.release();
        assert!(releaser.is_released());

        let released = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("received held value before timeout");

        assert_eq!(released, Ok(10));
    }
}
