// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A mechanism for recording [`crate::Event`]s

use super::Event;
use camino::Utf8Path;
use std::fs::File;
use std::io::{Seek, Write};

pub struct EventLog {
    file: File,
}

impl EventLog {
    pub fn new(path: &Utf8Path) -> EventLog {
        let mut file = File::create(path).unwrap();
        // We want to incremntally write an array of `Event`s.
        // Start the array
        file.write_all(b"[\n").expect("opening brace written");
        EventLog { file }
    }

    pub fn record(&mut self, event: &Event) {
        serde_json::to_writer_pretty(&mut self.file, event)
            .expect("writing event succeeded");
        self.file.write_all(b",\n").expect("write succeeded");
    }
}

impl Drop for EventLog {
    fn drop(&mut self) {
        // Backup over the trailing comma and newline
        let _ = self.file.seek_relative(-2);
        // Finish writing the array of events
        let _ = self.file.write_all(b"\n]\n");
        let _ = self.file.sync_data();
    }
}
