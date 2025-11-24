// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! in-memory slog drain

use colored::Color;
use colored::Colorize;
use slog::Drain;
use slog::KV as _;
use slog::Level;
use slog::LevelFilter;
use slog::Logger;
use slog_term::Decorator;
use slog_term::PlainSyncDecorator;
use slog_term::Serializer;
use std::io;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;

/// `LogCapture` creates a `slog::Logger` where log lines are buffered in memory
/// and can be fetched when desired via [`LogCapture::take_log_lines()`].
pub struct LogCapture {
    lines: Arc<Mutex<Vec<String>>>,
}

impl LogCapture {
    /// Create a new [`Logger`] that emits to the returned `LogCapture`.
    pub fn new(colorize: bool) -> (Self, Logger) {
        let lines = Arc::default();
        let slf = LogCapture { lines };

        let line_buf = LineBuf::default();
        let drain = LogCaptureDrain {
            colorize,
            lines: Arc::clone(&slf.lines),
            line_buf: line_buf.clone(),
            decorator: PlainSyncDecorator::new(line_buf),
        };

        let drain = slog::Fuse(LevelFilter::new(drain, Level::Info));
        let logger = Logger::root(drain, slog::o!());

        (slf, logger)
    }

    /// Create a new `LogCapture` that does not actually capture any logs.
    ///
    /// This exists to allow use of functions that require a `LogCapture`
    /// instance, but when logging is handled by something else (e.g., an outer
    /// Rust test calling into reconfigurator-cli).
    pub fn new_noop() -> Self {
        Self { lines: Arc::new(Mutex::new(Vec::new())) }
    }

    /// Take all buffered log lines out of the `LogCapture`.
    ///
    /// Additional logs emitted after this is called will continue to be
    /// buffered and can be fetched by subsequent calls to `take_log_lines()`.
    pub fn take_log_lines(&self) -> Vec<String> {
        mem::take(&mut *self.lines.lock().unwrap())
    }
}

struct LogCaptureDrain {
    colorize: bool,
    lines: Arc<Mutex<Vec<String>>>,
    line_buf: LineBuf,
    decorator: PlainSyncDecorator<LineBuf>,
}

impl Drain for LogCaptureDrain {
    type Ok = ();
    type Err = io::Error;

    fn log(
        &self,
        record: &slog::Record,
        values: &slog::OwnedKVList,
    ) -> Result<Self::Ok, Self::Err> {
        self.decorator.with_record(record, values, |d| {
            // This is heavily derived from `slog_term::print_msg_header()`, but
            // we omit some options we don't care about and intentionally omit
            // any kind of timestamp (so expectorate-based reconfigurator-cli
            // tests don't have to deal with redacting log timestamps).
            d.start_level()?;
            let level = record.level();
            if self.colorize {
                let color = match level {
                    Level::Critical => Color::Magenta,
                    Level::Error => Color::Red,
                    Level::Warning => Color::Yellow,
                    Level::Info => Color::Green,
                    Level::Debug => Color::Cyan,
                    Level::Trace => Color::Blue,
                };
                write!(d, "{}", level.as_short_str().color(color))?;
            } else {
                write!(d, "{}", level.as_short_str())?;
            }

            d.start_whitespace()?;
            write!(d, " ")?;

            d.start_msg()?;
            write!(d, "{}", record.msg())?;

            let mut serializer = Serializer::new(d, true, true);
            record.kv().serialize(record, &mut serializer)?;
            values.serialize(record, &mut serializer)?;
            serializer.finish()?;
            d.flush()
        })?;
        let line = self.line_buf.take_as_string()?;
        self.lines.lock().unwrap().push(line);
        Ok(())
    }
}

#[derive(Clone, Default)]
struct LineBuf(Arc<Mutex<Vec<u8>>>);

impl LineBuf {
    fn take_as_string(&self) -> io::Result<String> {
        let contents = mem::take(&mut *self.0.lock().unwrap());
        String::from_utf8(contents)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
    }
}

impl io::Write for LineBuf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}
