// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Writing collected data into a bundle, within a size limit.
//!
//! Steps write files through a [`BundleOutput`], which charges them against
//! a [`SizeBudget`] shared by the whole collection. The first write that
//! doesn't fit is refused, along with every write after it, and the stop
//! token is cancelled.
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
use slog::Logger;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::cell::Cell;
use std::io::Write;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

/// The reason a write is refused because the bundle has reached its size
/// limit. Writes return it as `StepError::SizeLimitExceeded`.
#[derive(Debug, thiserror::Error)]
#[error("bundle reached its size limit of {limit} bytes")]
pub struct SizeLimitExceeded {
    limit: u64,
}

/// Records when, and on which file, a bundle exceeded its size limit.
#[derive(Clone, Debug)]
pub struct Exceeded {
    pub at: DateTime<Utc>,
    /// The file whose write was refused, relative to the bundle's root.
    pub path: Utf8PathBuf,
}

/// Tracks the bytes written to a bundle against its size limit.
pub struct SizeBudget {
    limit: Option<u64>,
    used: AtomicU64,
    exceeded: OnceLock<Exceeded>,
    stop: CancellationToken,
}

impl SizeBudget {
    /// Creates a budget of `limit` bytes, or an unlimited one if `limit` is
    /// `None`. `stop` is cancelled when the limit is exceeded.
    pub fn new(limit: Option<u64>, stop: CancellationToken) -> Self {
        Self { limit, used: AtomicU64::new(0), exceeded: OnceLock::new(), stop }
    }

    pub fn limit(&self) -> Option<u64> {
        self.limit
    }

    /// The number of bytes written to the bundle.
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    /// Returns when, and on which file, the limit was exceeded, if it has
    /// been.
    pub fn exceeded(&self) -> Option<&Exceeded> {
        self.exceeded.get()
    }

    pub fn is_exceeded(&self) -> bool {
        self.exceeded.get().is_some()
    }

    /// Charges `n` bytes, written to `path`, against the budget.
    ///
    /// Fails, without charging anything, if the limit has already been
    /// exceeded, or if these bytes would exceed it.
    fn charge(&self, path: &Utf8Path, n: u64) -> Result<(), SizeLimitExceeded> {
        let Some(limit) = self.limit else {
            self.used.fetch_add(n, Ordering::Relaxed);
            return Ok(());
        };
        if self.is_exceeded() {
            return Err(SizeLimitExceeded { limit });
        }
        // Add the bytes only if they fit, in one atomic step, so that `used`
        // never counts a write that's refused. Otherwise, a concurrent write
        // could see those bytes, and be refused, and blamed, in its place.
        let fits = self.used.try_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |used| used.checked_add(n).filter(|&total| total <= limit),
        );
        if fits.is_err() {
            let _ = self
                .exceeded
                .set(Exceeded { at: Utc::now(), path: path.to_owned() });
            self.stop.cancel();
            return Err(SizeLimitExceeded { limit });
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
/// All writes are charged against the bundle's [`SizeBudget`]. A write that
/// fails, for any reason, removes its partial file, and is not charged for it.
/// A partial file that cannot be removed is logged, and stays charged.
#[derive(Clone)]
pub struct BundleOutput {
    // The root of the collected bundle directory.
    root: Arc<Utf8PathBuf>,
    // The path of this directory, relative to `root`.
    dir: Utf8PathBuf,
    budget: Arc<SizeBudget>,
    log: Logger,
}

impl BundleOutput {
    /// Returns the root directory of a bundle being collected in `root`.
    pub fn new(root: &Utf8Path, budget: Arc<SizeBudget>, log: Logger) -> Self {
        Self {
            root: Arc::new(root.to_owned()),
            dir: Utf8PathBuf::new(),
            budget,
            log,
        }
    }

    /// Returns the subdirectory `path` of this directory.
    ///
    /// The subdirectory is not created until something is written to it, or
    /// [`BundleOutput::create_dir_all`] is called.
    pub fn join(&self, path: impl AsRef<Utf8Path>) -> Self {
        Self {
            root: self.root.clone(),
            dir: self.dir.join(path),
            budget: self.budget.clone(),
            log: self.log.clone(),
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

    /// Writes `contents` to the file `name` within this directory, creating
    /// any missing parent directories.
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

        let result = async {
            create_parent_dir(&path).await?;
            tokio::fs::write(&path, contents)
                .await
                .with_context(|| format!("failed to write {path}"))
        }
        .await
        .map_err(StepError::Other);
        if result.is_err() && remove_partial_file(&self.log, &path).await {
            self.budget.refund(len);
        }
        result
    }

    /// Copies everything from `reader` to the file `name` within this
    /// directory, creating any missing parent directories. Returns the number
    /// of bytes copied.
    ///
    /// Bytes are charged as they are copied, so the budget is enforced even
    /// when the size of the data is not known upfront.
    ///
    /// # Cancel safety
    ///
    /// Cancel-**unsafe**: if dropped, a partial file may be left behind.
    pub async fn write_stream<R: AsyncRead + Unpin>(
        &self,
        name: impl AsRef<Utf8Path>,
        mut reader: R,
    ) -> Result<u64, StepError> {
        // Many streams may be copied at once (zone log downloads from every
        // sled being collected), each with its own buffer, so this is kept
        // modest. Reads from a network stream rarely fill a larger one anyway.
        const BUF_SIZE: usize = 64 * 1024;

        let (path, rel) = self.resolve(name.as_ref());
        // Refuse upfront if the bundle has already reached its limit. Without
        // this, a stream would only be refused at its first charge: after
        // creating its file, and waiting for data, and an empty stream would
        // never be refused at all.
        self.budget.charge(&rel, 0)?;
        let mut charged = 0;
        let result = async {
            create_parent_dir(&path).await?;
            let mut file = tokio::fs::File::create(&path)
                .await
                .with_context(|| format!("failed to create {path}"))?;
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
            Ok::<_, StepError>(charged)
        }
        .await;
        if result.is_err() && remove_partial_file(&self.log, &path).await {
            self.budget.refund(charged);
        }
        result
    }

    /// Runs `f` on a blocking thread, to write the file `name` within this
    /// directory, creating any missing parent directories.
    ///
    /// The writer given to `f` is buffered. Bytes are charged as they are
    /// written, and once the bundle reaches its size limit, writes fail. This
    /// returns `StepError::SizeLimitExceeded` in that case, rather than
    /// whatever error `f` made of it, and even if `f` ignored it.
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
        // As in `write_stream`, refuse upfront if the bundle has already
        // reached its limit, so that `f` doesn't run, and a file it writes
        // nothing to isn't added to a bundle that has stopped.
        self.budget.charge(&rel, 0)?;
        let budget = self.budget.clone();
        let log = self.log.clone();
        tokio::task::spawn_blocking(move || {
            let charged = Cell::new(0);
            let refused = Cell::new(None);
            let result = (|| {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).with_context(|| {
                        format!("failed to create directory {parent}")
                    })?;
                }
                let file = std::fs::File::create(&path)
                    .with_context(|| format!("failed to create {path}"))?;
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
                // Drop the writer without writing what it still holds, which
                // `BufWriter`'s own `Drop` would do. After a failure the file
                // is removed anyway, and writing it could exceed the size
                // limit for nothing, hiding the real error.
                let _ = w.into_parts();
                result
            })();
            // A refused write fails the whole file, even if `f` ignored the
            // error and returned Ok.
            let err = match (refused.take(), result) {
                (None, Ok(())) => return Ok(()),
                (Some(exceeded), _) => StepError::SizeLimitExceeded(exceeded),
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
    refused: &'a Cell<Option<SizeLimitExceeded>>,
}

impl Write for BudgetedWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len() as u64;
        if let Err(exceeded) = self.budget.charge(self.rel, len) {
            let err = std::io::Error::other(exceeded.to_string());
            self.refused.set(Some(exceeded));
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

async fn create_parent_dir(path: &Utf8Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create directory {parent}"))?;
    }
    Ok(())
}

// Removes a file left by a failed write, returning true if it is gone.
//
// This is best-effort: a file that cannot be removed is logged, stays in the
// bundle, and stays charged against its budget.
async fn remove_partial_file(log: &Logger, path: &Utf8Path) -> bool {
    is_removed(log, path, tokio::fs::remove_file(path).await)
}

// Returns true if `result`, of removing the partial file `path`, left it gone,
// logging why not otherwise.
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
        limit: Option<u64>,
    ) -> (Utf8TempDir, BundleOutput, CancellationToken) {
        let dir = tempdir().unwrap();
        let stop = CancellationToken::new();
        let budget = Arc::new(SizeBudget::new(limit, stop.clone()));
        let log = Logger::root(slog::Discard, slog::o!());
        let output = BundleOutput::new(dir.path(), budget, log);
        (dir, output, stop)
    }

    fn assert_size_limit_exceeded(err: &StepError) {
        assert!(
            matches!(err, StepError::SizeLimitExceeded(_)),
            "expected SizeLimitExceeded, got: {err:?}"
        );
    }

    fn other_error(err: StepError) -> anyhow::Error {
        match err {
            StepError::Other(err) => err,
            StepError::SizeLimitExceeded(_) => {
                panic!("expected an error other than SizeLimitExceeded")
            }
        }
    }

    // Adding context to a step's error keeps whether the bundle reached its
    // size limit, and adds the context to any other error.
    #[test]
    fn test_context_keeps_size_limit_exceeded() {
        use crate::step::StepResultExt;

        let exceeded: Result<(), StepError> =
            Err(SizeLimitExceeded { limit: 1 }.into());
        assert_size_limit_exceeded(
            &exceeded.context("writing a file").unwrap_err(),
        );

        let other: Result<(), StepError> =
            Err(anyhow::anyhow!("the disk broke").into());
        let err =
            other_error(other.with_context(|| "writing a file").unwrap_err());
        assert_eq!(format!("{err:#}"), "writing a file: the disk broke");
    }

    // Many threads charging the budget at once never take it past its limit,
    // and it counts exactly the bytes it accepted. The write it blames for
    // exceeding the limit is one it refused, rather than one it accepted.
    #[test]
    fn test_concurrent_charges() {
        const LIMIT: u64 = 10_000;
        const THREADS: u64 = 8;
        const CHARGES_PER_THREAD: u64 = 1_000;

        let budget = SizeBudget::new(Some(LIMIT), CancellationToken::new());
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

        let exceeded = budget.exceeded().expect("limit should be exceeded");
        let blamed = results
            .iter()
            .find(|(path, _, _)| *path == exceeded.path)
            .expect("blamed path should be one that was charged");
        assert!(!blamed.2, "blamed {} for a write that was accepted", blamed.0);
    }

    #[tokio::test]
    async fn test_write_within_limit() {
        let (dir, output, stop) = output_with_limit(Some(10));
        output.join("a/b").write("file", "12345").await.unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("a/b/file")).unwrap(),
            "12345"
        );
        assert_eq!(output.budget.used(), 5);
        assert!(!output.budget.is_exceeded());
        assert!(!stop.is_cancelled());
    }

    #[tokio::test]
    async fn test_unlimited_budget_counts_bytes() {
        let (_dir, output, stop) = output_with_limit(None);
        output.write("file", vec![0; 1 << 20]).await.unwrap();
        assert_eq!(output.budget.used(), 1 << 20);
        assert!(!output.budget.is_exceeded());
        assert!(!stop.is_cancelled());
    }

    // Once a write is refused, the budget stops collection, and refuses all
    // later writes, even those which would fit.
    #[tokio::test]
    async fn test_write_exceeding_limit_trips_budget() {
        let (dir, output, stop) = output_with_limit(Some(10));
        output.write("first", "12345").await.unwrap();

        let err =
            output.join("sub").write("second", "123456").await.unwrap_err();
        assert_size_limit_exceeded(&err);
        assert!(!dir.path().join("sub/second").exists());
        assert_eq!(output.budget.used(), 5);
        assert!(stop.is_cancelled());
        let exceeded = output.budget.exceeded().unwrap();
        assert_eq!(exceeded.path, "sub/second");

        let err = output.write("third", "1").await.unwrap_err();
        assert_size_limit_exceeded(&err);
        assert!(!dir.path().join("third").exists());
        assert_eq!(output.budget.used(), 5);
        // The first file to exceed the limit is the one recorded.
        assert_eq!(output.budget.exceeded().unwrap().path, "sub/second");
    }

    #[tokio::test]
    async fn test_write_stream() {
        let (dir, output, _stop) = output_with_limit(Some(1 << 20));
        let data = vec![7u8; 600 * 1024];
        let n = output.write_stream("stream", &data[..]).await.unwrap();
        assert_eq!(n, data.len() as u64);
        assert_eq!(std::fs::read(dir.path().join("stream")).unwrap(), data);
        assert_eq!(output.budget.used(), data.len() as u64);
    }

    // A stream that exceeds the limit partway through is removed, and the
    // bytes already written for it are returned to the budget.
    #[tokio::test]
    async fn test_write_stream_exceeding_limit_removes_partial_file() {
        let (dir, output, stop) = output_with_limit(Some(1 << 20));
        output.write("small", "12345").await.unwrap();

        let data = vec![7u8; 2 << 20];
        let err = output.write_stream("stream", &data[..]).await.unwrap_err();
        assert_size_limit_exceeded(&err);
        assert!(!dir.path().join("stream").exists());
        assert_eq!(output.budget.used(), 5);
        assert!(stop.is_cancelled());
        assert_eq!(output.budget.exceeded().unwrap().path, "stream");
    }

    // A stream that fails for reasons other than the size limit is removed,
    // and returned to the budget, without tripping it.
    #[tokio::test]
    async fn test_write_stream_read_error_removes_partial_file() {
        let (dir, output, stop) = output_with_limit(Some(1 << 20));
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

    // Once the bundle has reached its limit, a stream is refused before its
    // file is created or its reader is read, even if the stream is empty.
    #[tokio::test]
    async fn test_write_stream_after_limit_exceeded() {
        let (dir, output, _stop) = output_with_limit(Some(1));
        assert_size_limit_exceeded(
            &output.write("big", "12").await.unwrap_err(),
        );

        let err = output
            .join("sub")
            .write_stream("empty", tokio::io::empty())
            .await
            .unwrap_err();
        assert_size_limit_exceeded(&err);
        assert!(!dir.path().join("sub").exists());

        // A reader which would fail the test if it were read.
        let err = output
            .write_stream("never", failing_reader(b"", "reader was read"))
            .await
            .unwrap_err();
        assert_size_limit_exceeded(&err);
        assert!(!dir.path().join("never").exists());
        assert_eq!(output.budget.used(), 0);
        assert_eq!(output.budget.exceeded().unwrap().path, "big");
    }

    #[tokio::test]
    async fn test_write_with() {
        let (dir, output, _stop) = output_with_limit(Some(100));
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

    // When the size limit is exceeded, `write_with` reports that, rather than
    // the error its closure made of the failed write, and removes the file.
    #[tokio::test]
    async fn test_write_with_exceeding_limit_removes_partial_file() {
        let (dir, output, stop) = output_with_limit(Some(1 << 20));
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
        assert_eq!(output.budget.exceeded().unwrap().path, "big.json");
    }

    // When `f` fails for reasons other than the size limit, the bytes it left
    // buffered are discarded rather than written. Writing them could exceed
    // the limit for a file that's about to be removed anyway, stopping the
    // collection and hiding the real error.
    #[tokio::test]
    async fn test_write_with_error_discards_buffered_bytes() {
        let (dir, output, stop) = output_with_limit(Some(50));
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

    // A closure that ignores a refused write, and returns Ok, still has its
    // write reported as refused, and its truncated file removed.
    #[tokio::test]
    async fn test_write_with_ignored_refusal() {
        let (dir, output, stop) = output_with_limit(Some(1000));
        let err = output
            .write_with("file", |w| {
                // Larger than the writer's buffer, so it goes straight to the
                // budget, which refuses it.
                let _ = w.write_all(&[0; 16 * 1024]);
                Ok(())
            })
            .await
            .unwrap_err();
        assert_size_limit_exceeded(&err);
        assert!(!dir.path().join("file").exists());
        assert_eq!(output.budget.used(), 0);
        assert!(stop.is_cancelled());
    }

    // Once the bundle has reached its limit, `write_with` is refused without
    // running `f`, even if `f` would write nothing.
    #[tokio::test]
    async fn test_write_with_after_limit_exceeded() {
        let (dir, output, _stop) = output_with_limit(Some(1));
        assert_size_limit_exceeded(
            &output.write("big", "12").await.unwrap_err(),
        );

        let err = output
            .join("sub")
            .write_with("empty", |_w| panic!("f should not run"))
            .await
            .unwrap_err();
        assert_size_limit_exceeded(&err);
        assert!(!dir.path().join("sub").exists());
        assert_eq!(output.budget.used(), 0);
        assert_eq!(output.budget.exceeded().unwrap().path, "big");
    }

    #[tokio::test]
    async fn test_write_with_error_removes_partial_file() {
        let (dir, output, stop) = output_with_limit(Some(100));
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
