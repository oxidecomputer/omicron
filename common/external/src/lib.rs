// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub mod address;
pub mod backoff;
pub mod progenitor;

/// A type that allows adding file and line numbers to log messages
/// automatically. It should be instantiated at the root logger of each
/// executable that desires this functionality, as in the following example.
/// ```ignore
///     slog::Logger::root(drain, o!(FileKv))
/// ```
pub struct FileKv;

impl slog::KV for FileKv {
    fn serialize(
        &self,
        record: &slog::Record,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        // Only log file information when severity is at least info level
        if record.level() > slog::Level::Info {
            return Ok(());
        }
        serializer.emit_arguments(
            "file".into(),
            &format_args!("{}:{}", record.file(), record.line()),
        )
    }
}
