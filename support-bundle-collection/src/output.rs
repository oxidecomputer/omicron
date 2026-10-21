// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Writing collected data into a bundle, within a size limit.
//!
//! Files are written through [`BundleOutput`], which charges the size of each
//! write to a [`SizeBudget`] shared by the whole collection. A write is
//! accepted only if it fits under the limit. When a write is refused, the
//! stop token is cancelled, so that no new collection steps are started.
//! Any writes already in progress are allowed to continue as long as they fit
//! within the limit.
//!
//! The limit applies to the collected directory, not the zip. Compressed
//! data (zone logs, SP dumps) is stored as-is and the rest is deflated, so
//! the zip is no larger than the directory, plus small per-entry headers.

use crate::step::StepError;
use anyhow::Context;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use chrono::DateTime;
use chrono::Utc;
use nexus_types::support_bundle::SupportBundleSizeLimit;
use slog::Logger;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::cell::Cell;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

/// A write was refused because it would exceed the bundle's size limit.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error(
    "write would exceed the bundle's size limit of {}",
    .limit.byte_count()
)]
pub struct WriteRefused {
    limit: SupportBundleSizeLimit,
}

/// When a bundle first exceeded its size limit, and which files were refused.
#[derive(Clone, Debug)]
pub struct Refusals {
    pub at: DateTime<Utc>,
    /// Files whose writes were refused, relative to the bundle's root, in the
    /// order refused.
    pub paths: Vec<Utf8PathBuf>,
}

/// Tracks the bytes written to a bundle against its size limit.
pub struct SizeBudget {
    limit: SupportBundleSizeLimit,
    used: AtomicU64,
    refusals: Mutex<Option<Refusals>>,
    stop: CancellationToken,
}

impl SizeBudget {
    /// Creates a budget of `limit` bytes. `stop` is cancelled when a write is
    /// first refused.
    pub fn new(limit: SupportBundleSizeLimit, stop: CancellationToken) -> Self {
        Self {
            limit,
            used: AtomicU64::new(0),
            refusals: Mutex::new(None),
            stop,
        }
    }

    pub fn limit(&self) -> SupportBundleSizeLimit {
        self.limit
    }

    /// The number of bytes written to the bundle.
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    pub fn refusals(&self) -> Option<Refusals> {
        self.refusals.lock().unwrap().clone()
    }

    pub fn is_exceeded(&self) -> bool {
        self.refusals.lock().unwrap().is_some()
    }

    /// Charges `n` bytes, written to `path`, against the budget.
    ///
    /// Fails, without charging anything, if these bytes would exceed the
    /// limit. Each refusal records `path`, and cancels the stop token.
    fn charge(&self, path: &Utf8Path, n: u64) -> Result<(), WriteRefused> {
        let limit = self.limit.bytes();
        // Check and add in one step, so `used` never includes a refused
        // write. Otherwise a concurrent write could be refused, and blamed,
        // instead.
        let fits = self.used.try_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |used| used.checked_add(n).filter(|&total| total <= limit),
        );
        if fits.is_err() {
            self.refusals
                .lock()
                .unwrap()
                .get_or_insert_with(|| Refusals {
                    at: Utc::now(),
                    paths: Vec::new(),
                })
                .paths
                .push(path.to_owned());
            self.stop.cancel();
            return Err(WriteRefused { limit: self.limit });
        }
        Ok(())
    }

    /// Returns `n` previously-charged bytes to the budget, for data that was
    /// removed from the bundle.
    fn refund(&self, n: u64) {
        self.used.fetch_sub(n, Ordering::Relaxed);
    }
}

/// A directory within a bundle, into which steps write their data.
///
/// Writes are charged against the bundle's [`SizeBudget`]. A failed write
/// removes its partial file and is refunded. If removal fails, it's logged
/// and stays charged.
///
/// Each file may be written once. Writing a file that already exists fails,
/// and leaves that file as it was.
#[derive(Clone)]
pub struct BundleOutput {
    // The root of the collected bundle directory.
    root: Arc<Utf8PathBuf>,
    // The path of this directory, relative to `root`.
    dir: Utf8PathBuf,
    budget: Arc<SizeBudget>,
    // The bundle's logger, from which each directory's `log` is built.
    bundle_log: Logger,
    log: Logger,
}

impl BundleOutput {
    /// Returns the root directory of a bundle being collected in `root`.
    pub fn new(root: &Utf8Path, budget: Arc<SizeBudget>, log: Logger) -> Self {
        Self {
            root: Arc::new(root.to_owned()),
            dir: Utf8PathBuf::new(),
            budget,
            bundle_log: log.clone(),
            log,
        }
    }

    /// Returns the subdirectory `path` of this directory.
    ///
    /// The subdirectory is not created until something is written to it, or
    /// [`BundleOutput::create_dir_all`] is called.
    pub fn join(&self, path: impl AsRef<Utf8Path>) -> Self {
        let dir = self.dir.join(path);
        let log = self.bundle_log.new(slog::o!("dir" => dir.to_string()));
        Self {
            root: self.root.clone(),
            dir,
            budget: self.budget.clone(),
            bundle_log: self.bundle_log.clone(),
            log,
        }
    }

    // Returns the path of `name` within this directory: absolute, and
    // relative to the bundle's root.
    fn resolve(&self, name: &Utf8Path) -> (Utf8PathBuf, Utf8PathBuf) {
        let rel = self.dir.join(name);
        (self.root.join(&rel), rel)
    }

    /// Creates this directory, and any missing parents.
    pub async fn create_dir_all(&self) -> anyhow::Result<()> {
        let path = self.root.join(&self.dir);
        tokio::fs::create_dir_all(&path)
            .await
            .with_context(|| format!("failed to create directory {path}"))
    }

    /// Removes this directory, if it is empty.
    ///
    /// This is best-effort: if the directory is missing, not empty, or cannot
    /// be removed, it is left alone.
    pub async fn remove_dir_if_empty(&self) {
        let _ = tokio::fs::remove_dir(self.root.join(&self.dir)).await;
    }

    /// Writes `contents` to the new file `name` within this directory,
    /// creating any missing parent directories.
    ///
    /// # Cancel safety
    ///
    /// Cancel-**unsafe**: if dropped, a partial file may be left behind.
    pub async fn write(
        &self,
        name: impl AsRef<Utf8Path>,
        contents: impl AsRef<[u8]>,
    ) -> Result<(), StepError> {
        let (path, rel) = self.resolve(name.as_ref());
        let contents = contents.as_ref();
        let len = contents.len() as u64;
        self.budget.charge(&rel, len)?;
        let mut file = match create_new_file(&path).await {
            Ok(file) => file,
            Err(err) => {
                self.budget.refund(len);
                return Err(err.into());
            }
        };

        let result = async {
            file.write_all(contents)
                .await
                .with_context(|| format!("failed to write {path}"))?;
            file.flush()
                .await
                .with_context(|| format!("failed to flush {path}"))
        }
        .await
        .map_err(StepError::Other);
        if result.is_err() && remove_partial_file(&self.log, &path).await {
            self.budget.refund(len);
        }
        result
    }

    /// Copies everything from `reader` to the new file `name` within this
    /// directory, creating any missing parent directories.
    ///
    /// # Cancel safety
    ///
    /// Cancel-**unsafe**: if dropped, a partial file may be left behind.
    pub async fn write_stream<R: AsyncRead + Unpin>(
        &self,
        name: impl AsRef<Utf8Path>,
        mut reader: R,
    ) -> Result<(), StepError> {
        // Many streams may be copied at once, each with its own buffer, so this
        // is kept modest.
        const BUF_SIZE: usize = 64 * 1024;

        let (path, rel) = self.resolve(name.as_ref());
        let mut file = create_new_file(&path).await?;
        let mut charged = 0;
        let result = async {
            let mut buf = vec![0; BUF_SIZE];
            loop {
                let n = reader.read(&mut buf).await.with_context(|| {
                    format!("failed to read data for {path}")
                })?;
                if n == 0 {
                    break;
                }
                self.budget.charge(&rel, n as u64)?;
                charged += n as u64;
                file.write_all(&buf[..n])
                    .await
                    .with_context(|| format!("failed to write {path}"))?;
            }
            file.flush()
                .await
                .with_context(|| format!("failed to flush {path}"))?;
            Ok::<_, StepError>(())
        }
        .await;
        if result.is_err() && remove_partial_file(&self.log, &path).await {
            self.budget.refund(charged);
        }
        result
    }

    /// Runs `f` on a blocking thread, to write the new file `name` within this
    /// directory, creating any missing parent directories.
    ///
    /// `f` gets a buffered writer. If a write exceeds the size limit, this
    /// returns `StepError::WriteRefused`, whatever `f` returned.
    ///
    /// # Cancel safety
    ///
    /// Cancel-**unsafe**: if dropped, `f` runs to completion anyway, and its
    /// file is left behind.
    pub async fn write_with<F>(
        &self,
        name: impl AsRef<Utf8Path>,
        f: F,
    ) -> Result<(), StepError>
    where
        F: FnOnce(&mut dyn Write) -> anyhow::Result<()> + Send + 'static,
    {
        let (path, rel) = self.resolve(name.as_ref());
        let budget = self.budget.clone();
        let log = self.log.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create directory {parent}")
                })?;
            }
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .with_context(|| format!("failed to create {path}"))?;

            let charged = Cell::new(0);
            let refused = Cell::new(None);
            let result = (|| {
                let mut w = std::io::BufWriter::new(BudgetedWriter {
                    file,
                    budget: &budget,
                    rel: &rel,
                    charged: &charged,
                    refused: &refused,
                });
                let result = f(&mut w).and_then(|()| {
                    w.flush().with_context(|| format!("failed to flush {path}"))
                });
                // Discard buffered bytes instead of flushing them on drop.
                // After a failure the file is removed, and flushing could
                // trip the limit and hide the real error.
                let _ = w.into_parts();
                result
            })();
            // A refused write fails the file, even if `f` returned Ok.
            let err = match (refused.take(), result) {
                (None, Ok(())) => return Ok(()),
                (Some(refused), _) => StepError::WriteRefused(refused),
                (None, Err(err)) => StepError::Other(err),
            };
            if is_removed(&log, &path, std::fs::remove_file(&path)) {
                budget.refund(charged.get());
            }
            Err(err)
        })
        .await
        .context("writing to the bundle panicked")?
    }
}

// A file writer for `BundleOutput::write_with`, which charges what it writes
// against the bundle's budget.
struct BudgetedWriter<'a> {
    file: std::fs::File,
    budget: &'a SizeBudget,
    rel: &'a Utf8Path,
    // The total number of bytes charged for this file.
    charged: &'a Cell<u64>,
    // Set if a write was refused because the bundle reached its limit.
    refused: &'a Cell<Option<WriteRefused>>,
}

impl Write for BudgetedWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Once refused, the file will be removed, so stop charging for it.
        if let Some(refused) = self.refused.get() {
            return Err(std::io::Error::other(refused.to_string()));
        }
        let len = buf.len() as u64;
        if let Err(refused) = self.budget.charge(self.rel, len) {
            let err = std::io::Error::other(refused.to_string());
            self.refused.set(Some(refused));
            return Err(err);
        }
        match self.file.write(buf) {
            Ok(n) => {
                self.budget.refund(len - n as u64);
                self.charged.set(self.charged.get() + n as u64);
                Ok(n)
            }
            Err(err) => {
                self.budget.refund(len);
                Err(err)
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

// Creates the file at `path`, and any missing parent directories. Fails if the
// file already exists.
async fn create_new_file(path: &Utf8Path) -> anyhow::Result<tokio::fs::File> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create directory {parent}"))?;
    }
    tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .with_context(|| format!("failed to create {path}"))
}

// Removes a failed write's partial file. Returns whether it's gone.
async fn remove_partial_file(log: &Logger, path: &Utf8Path) -> bool {
    is_removed(log, path, tokio::fs::remove_file(path).await)
}

// Returns whether `path` is gone after `result`. Logs if not.
fn is_removed(
    log: &Logger,
    path: &Utf8Path,
    result: std::io::Result<()>,
) -> bool {
    match result {
        Ok(()) => true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
        Err(err) => {
            warn!(
                log,
                "failed to remove partial file from support bundle";
                "path" => %path,
                InlineErrorChain::new(&err),
            );
            false
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use camino_tempfile::Utf8TempDir;
    use camino_tempfile::tempdir;

    fn output_with_limit(
        limit: u64,
    ) -> (Utf8TempDir, BundleOutput, CancellationToken) {
        let dir = tempdir().unwrap();
        let stop = CancellationToken::new();
        let limit = SupportBundleSizeLimit::new(limit).unwrap();
        let budget = Arc::new(SizeBudget::new(limit, stop.clone()));
        let log = Logger::root(slog::Discard, slog::o!());
        let output = BundleOutput::new(dir.path(), budget, log);
        (dir, output, stop)
    }

    fn assert_size_limit_exceeded(err: &StepError) {
        assert!(
            matches!(err, StepError::WriteRefused(_)),
            "expected WriteRefused, got: {err:?}"
        );
    }

    fn other_error(err: StepError) -> anyhow::Error {
        match err {
            StepError::Other(err) => err,
            StepError::WriteRefused(_) => {
                panic!("expected an error other than WriteRefused")
            }
        }
    }

    #[test]
    fn test_context_keeps_size_limit_exceeded() {
        use crate::step::StepResultExt;

        let refused: Result<(), StepError> = Err(WriteRefused {
            limit: SupportBundleSizeLimit::new(1).unwrap(),
        }
        .into());
        assert_size_limit_exceeded(
            &refused.context("writing a file").unwrap_err(),
        );

        let other: Result<(), StepError> =
            Err(anyhow::anyhow!("the disk broke").into());
        let err =
            other_error(other.with_context(|| "writing a file").unwrap_err());
        assert_eq!(format!("{err:#}"), "writing a file: the disk broke");
    }

    // The budget never goes past its limit, counts exactly what it accepted,
    // and records exactly the writes it refused.
    #[test]
    fn test_concurrent_charges() {
        const LIMIT: u64 = 10_000;
        const THREADS: u64 = 8;
        const CHARGES_PER_THREAD: u64 = 1_000;

        let budget = SizeBudget::new(
            SupportBundleSizeLimit::new(LIMIT).unwrap(),
            CancellationToken::new(),
        );
        let results: Vec<(Utf8PathBuf, u64, bool)> = std::thread::scope(|s| {
            let threads: Vec<_> = (0..THREADS)
                .map(|t| {
                    let budget = &budget;
                    s.spawn(move || {
                        let mut results = Vec::new();
                        for i in 0..CHARGES_PER_THREAD {
                            let path = Utf8PathBuf::from(format!("{t}/{i}"));
                            // Vary the sizes, so that some writes fit where
                            // others don't.
                            let n = 1 + (t * 7 + i) % 13;
                            let ok = budget.charge(&path, n).is_ok();
                            assert!(budget.used() <= LIMIT);
                            results.push((path, n, ok));
                        }
                        results
                    })
                })
                .collect();
            threads.into_iter().flat_map(|t| t.join().unwrap()).collect()
        });

        let accepted: u64 =
            results.iter().filter(|(_, _, ok)| *ok).map(|(_, n, _)| n).sum();
        assert_eq!(budget.used(), accepted);
        assert!(accepted <= LIMIT);

        let refusals = budget.refusals().expect("limit should be refusals");
        let mut recorded = refusals.paths;
        recorded.sort();
        let mut refused: Vec<Utf8PathBuf> = results
            .into_iter()
            .filter(|(_, _, ok)| !ok)
            .map(|(path, _, _)| path)
            .collect();
        refused.sort();
        assert_eq!(recorded, refused);
    }

    #[tokio::test]
    async fn test_write_within_limit() {
        let (dir, output, stop) = output_with_limit(10);
        output.join("a/b").write("file", "12345").await.unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("a/b/file")).unwrap(),
            "12345"
        );
        assert_eq!(output.budget.used(), 5);
        assert!(!output.budget.is_exceeded());
        assert!(!stop.is_cancelled());
    }

    // Each kind of write fails on an existing file, without removing it or
    // refunding its charge.
    #[tokio::test]
    async fn test_write_existing_file_fails() {
        let (dir, output, stop) = output_with_limit(100);
        output.write("file", "12345").await.unwrap();

        let results = [
            output.write("file", "abc").await,
            output.write_stream("file", &b"abc"[..]).await,
            output.write_with("file", |w| Ok(w.write_all(b"abc")?)).await,
        ];
        for result in results {
            let err = other_error(result.unwrap_err());
            assert!(
                format!("{err:#}").contains("failed to create"),
                "unexpected error: {err:#}"
            );
        }

        assert_eq!(
            std::fs::read_to_string(dir.path().join("file")).unwrap(),
            "12345"
        );
        assert_eq!(output.budget.used(), 5);
        assert!(!output.budget.is_exceeded());
        assert!(!stop.is_cancelled());
    }

    // After a refusal, writes that fit still succeed. Every refusal is
    // recorded, in order.
    #[tokio::test]
    async fn test_write_exceeding_limit_trips_budget() {
        let (dir, output, stop) = output_with_limit(10);
        output.write("first", "12345").await.unwrap();

        let err =
            output.join("sub").write("second", "123456").await.unwrap_err();
        assert_size_limit_exceeded(&err);
        assert!(!dir.path().join("sub/second").exists());
        assert_eq!(output.budget.used(), 5);
        assert!(stop.is_cancelled());
        let at = output.budget.refusals().unwrap().at;

        output.write("third", "1").await.unwrap();
        assert_eq!(output.budget.used(), 6);

        let err = output.write("fourth", "12345").await.unwrap_err();
        assert_size_limit_exceeded(&err);
        assert!(!dir.path().join("fourth").exists());
        let refusals = output.budget.refusals().unwrap();
        assert_eq!(refusals.at, at);
        assert_eq!(refusals.paths, ["sub/second", "fourth"]);
    }

    #[tokio::test]
    async fn test_write_stream() {
        let (dir, output, _stop) = output_with_limit(1 << 20);
        let data = vec![7u8; 600 * 1024];
        output.write_stream("stream", &data[..]).await.unwrap();
        assert_eq!(std::fs::read(dir.path().join("stream")).unwrap(), data);
        assert_eq!(output.budget.used(), data.len() as u64);
    }

    // Bytes already charged are refunded.
    #[tokio::test]
    async fn test_write_stream_exceeding_limit_removes_partial_file() {
        let (dir, output, stop) = output_with_limit(1 << 20);
        output.write("small", "12345").await.unwrap();

        let data = vec![7u8; 2 << 20];
        let err = output.write_stream("stream", &data[..]).await.unwrap_err();
        assert_size_limit_exceeded(&err);
        assert!(!dir.path().join("stream").exists());
        assert_eq!(output.budget.used(), 5);
        assert!(stop.is_cancelled());
        assert_eq!(output.budget.refusals().unwrap().paths, ["stream"]);
    }

    // Refunded, without tripping the budget.
    #[tokio::test]
    async fn test_write_stream_read_error_removes_partial_file() {
        let (dir, output, stop) = output_with_limit(1 << 20);
        let reader = failing_reader(b"some data", "the network broke");
        let err = other_error(
            output.write_stream("stream", reader).await.unwrap_err(),
        );
        assert!(
            format!("{err:#}").contains("the network broke"),
            "unexpected error: {err:#}"
        );
        assert!(!dir.path().join("stream").exists());
        assert_eq!(output.budget.used(), 0);
        assert!(!output.budget.is_exceeded());
        assert!(!stop.is_cancelled());
    }

    // Returns a reader which yields `data`, then fails with `error`.
    fn failing_reader(
        data: &'static [u8],
        error: &'static str,
    ) -> impl AsyncRead + Unpin {
        let chunks: Vec<std::io::Result<&'static [u8]>> =
            vec![Ok(data), Err(std::io::Error::other(error))];
        tokio_util::io::StreamReader::new(futures::stream::iter(chunks))
    }

    // A stream in progress when another write is refused finishes, if it
    // fits.
    #[tokio::test]
    async fn test_write_stream_continues_after_refusal() {
        let (dir, output, stop) = output_with_limit(100);
        let (tx, rx) = futures::channel::mpsc::unbounded::<
            std::io::Result<&'static [u8]>,
        >();
        let stream = tokio::spawn({
            let output = output.clone();
            async move {
                output
                    .write_stream(
                        "stream",
                        tokio_util::io::StreamReader::new(rx),
                    )
                    .await
            }
        });

        tx.unbounded_send(Ok(&b"0123456789"[..])).unwrap();
        while output.budget.used() < 10 {
            tokio::task::yield_now().await;
        }
        assert_size_limit_exceeded(
            &output.write("big", [0; 100]).await.unwrap_err(),
        );
        assert!(stop.is_cancelled());

        tx.unbounded_send(Ok(&b"abcdefghij"[..])).unwrap();
        drop(tx);
        stream.await.unwrap().unwrap();
        assert_eq!(
            std::fs::read(dir.path().join("stream")).unwrap(),
            b"0123456789abcdefghij"
        );
        assert_eq!(output.budget.used(), 20);
        assert_eq!(output.budget.refusals().unwrap().paths, ["big"]);
    }

    #[tokio::test]
    async fn test_write_with() {
        let (dir, output, _stop) = output_with_limit(100);
        output
            .write_with("dir/file.json", |w| {
                serde_json::to_writer(w, &[1, 2, 3])?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("dir/file.json")).unwrap(),
            "[1,2,3]"
        );
        assert_eq!(output.budget.used(), 7);
    }

    // Reports the size limit, not the closure's error.
    #[tokio::test]
    async fn test_write_with_exceeding_limit_removes_partial_file() {
        let (dir, output, stop) = output_with_limit(1 << 20);
        let err = output
            .write_with("big.json", |w| {
                serde_json::to_writer(w, &vec![0u8; 1 << 20])
                    .context("failed to serialize")
            })
            .await
            .unwrap_err();
        assert_size_limit_exceeded(&err);
        assert!(!dir.path().join("big.json").exists());
        assert_eq!(output.budget.used(), 0);
        assert!(stop.is_cancelled());
        assert_eq!(output.budget.refusals().unwrap().paths, ["big.json"]);
    }

    // Flushing what's buffered would trip the limit and hide `f`'s error.
    #[tokio::test]
    async fn test_write_with_error_discards_buffered_bytes() {
        let (dir, output, stop) = output_with_limit(50);
        let err = output
            .write_with("file", |w| {
                // Fewer bytes than the writer buffers, but more than the limit.
                w.write_all(&[0; 100])?;
                anyhow::bail!("serialization failed")
            })
            .await
            .unwrap_err();
        assert_eq!(other_error(err).to_string(), "serialization failed");
        assert!(!dir.path().join("file").exists());
        assert_eq!(output.budget.used(), 0);
        assert!(!output.budget.is_exceeded());
        assert!(!stop.is_cancelled());
    }

    // Later writes to a refused file are refused too, even if they'd fit.
    #[tokio::test]
    async fn test_write_with_ignored_refusal() {
        let (dir, output, stop) = output_with_limit(1000);
        let err = output
            .write_with("file", |w| {
                // Larger than the writer's buffer, so it goes straight to the
                // budget, which refuses it.
                let _ = w.write_all(&[0; 16 * 1024]);
                // Buffered, then refused when flushed.
                w.write_all(&[0; 10]).unwrap();
                assert!(w.flush().is_err());
                Ok(())
            })
            .await
            .unwrap_err();
        assert_size_limit_exceeded(&err);
        assert!(!dir.path().join("file").exists());
        assert_eq!(output.budget.used(), 0);
        assert!(stop.is_cancelled());
        assert_eq!(output.budget.refusals().unwrap().paths, ["file"]);
    }

    #[tokio::test]
    async fn test_write_with_error_removes_partial_file() {
        let (dir, output, stop) = output_with_limit(100);
        let err = output
            .write_with("file", |w| {
                w.write_all(b"partial")?;
                anyhow::bail!("serialization failed")
            })
            .await
            .unwrap_err();
        assert_eq!(other_error(err).to_string(), "serialization failed");
        assert!(!dir.path().join("file").exists());
        assert_eq!(output.budget.used(), 0);
        assert!(!stop.is_cancelled());
    }
}
