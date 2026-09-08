// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Read files from the debug dropbox in tests

use crate::dev::TestTempDir;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use omicron_debug_dropbox::DebugDropbox;
use omicron_debug_dropbox::Producer;
use serde::de::DeserializeOwned;
use slog::Logger;
use std::collections::BTreeSet;

/// Helper for reading files stored into a debug dropbox
///
/// This doesn't handle most errors and is intended only for testing.
pub struct DropboxReader {
    producer_path: Utf8PathBuf,
    seen: BTreeSet<String>,
}

impl DropboxReader {
    pub fn new(dropbox_path: &Utf8Path, producer_name: &str) -> DropboxReader {
        let producer_path = dropbox_path.join(producer_name);
        DropboxReader { producer_path, seen: BTreeSet::new() }
    }

    /// Return a list of any files in the dropbox directory that were not
    /// present the last time this function was invoked.
    ///
    /// Files are assumed to be JSON and parsed as type `T`.
    ///
    /// Panics on any filesystem or parse error.
    pub fn load_new<T: DeserializeOwned>(&mut self) -> Vec<T> {
        let mut rv = Vec::new();

        // Since this is a test environment, we have control over the conditions
        // that might cause transient failures or any unexpected data to appear.
        // That's why we assert that the world looks precisely like we expect.
        let dirents = self
            .producer_path
            .read_dir_utf8()
            .expect("successfully list directory");
        for maybe_entry in dirents {
            let entry = maybe_entry.expect("successfully traverse directory");

            // Ignore anything we've seen before.
            if self.seen.contains(entry.file_name()) {
                continue;
            }
            self.seen.insert(entry.file_name().to_string());

            // We don't expect to find any non-files here.
            assert!(entry.file_type().unwrap().is_file());
            let file_str = std::fs::read_to_string(entry.path())
                .expect("read dropbox file");
            let state_file: T =
                serde_json::from_str(&file_str).expect("valid dropbox file");
            rv.push(state_file);
        }

        rv
    }

    /// Returns the count of distinct files that were previously returned by any
    /// `load_new()` invocation and that are now not present.
    ///
    /// Panics on any filesystem or parse error.
    pub fn count_removed(&self) -> usize {
        let mut expected = self.seen.clone();

        // See the comments in `load_new()` about error handling.
        let dirents = self
            .producer_path
            .read_dir_utf8()
            .expect("successfully list directory");
        for maybe_entry in dirents {
            let entry = maybe_entry.expect("successfully traverse directory");
            let _ = expected.remove(entry.file_name());
        }

        expected.len()
    }
}

const PRODUCER_NAME: &str = "test-producer";

/// Encapsulates a whole debug dropbox for testing
///
/// - a temporary directory under the consumer's control
/// - a Dropbox for depositing files
/// - any number of DropboxReaders for loading its contents
pub struct TestDropbox {
    directory: TestTempDir,
    producer: Producer,
}

impl TestDropbox {
    pub async fn new(log: Logger) -> TestDropbox {
        let directory = TestTempDir::new(&log);
        let dropbox = DebugDropbox::for_tests(&log, directory.path())
            .await
            .expect("creating test dropbox");
        let producer = dropbox
            .initialize_producer(PRODUCER_NAME)
            .await
            .expect("initializing test producer");
        TestDropbox { directory, producer }
    }

    pub fn producer(&self) -> &Producer {
        &self.producer
    }

    pub fn new_reader(&self) -> DropboxReader {
        DropboxReader::new(self.directory.path(), PRODUCER_NAME)
    }

    pub fn into_parts(self) -> (TestTempDir, Producer) {
        (self.directory, self.producer)
    }

    pub fn cleanup_successful(self) {
        self.directory.cleanup_successful();
    }
}
