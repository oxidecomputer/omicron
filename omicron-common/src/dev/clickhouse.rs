//! Tools for managing ClickHouse during development

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use tempfile::TempDir;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
    time::{sleep, Instant},
};

use crate::dev::poll;

// Timeout used when starting up ClickHouse subprocess.
const CLICKHOUSE_TIMEOUT: Duration = Duration::from_secs(10);

/// A `ClickHouseInstance` is used to start and manage a ClickHouse server process.
#[derive(Debug)]
pub struct ClickHouseInstance {
    // Directory in which all data, logs, etc are stored.
    data_dir: Option<TempDir>,
    data_path: PathBuf,
    // The HTTP port the server is listening on
    port: u16,
    // Full list of command-line arguments
    args: Vec<String>,
    // Subprocess handle
    child: Option<tokio::process::Child>,
}

impl ClickHouseInstance {
    /// Start a new ClickHouse server
    pub async fn new(port: u16) -> Result<Self, anyhow::Error> {
        let data_dir = TempDir::new()?;
        let log_path = data_dir.path().join("clickhouse-server.log");
        let err_log_path = data_dir.path().join("clickhouse-server.errlog");
        let args = vec![
            "server".to_string(),
            "--log-file".to_string(),
            log_path.display().to_string(),
            "--errorlog-file".to_string(),
            err_log_path.display().to_string(),
            "--".to_string(),
            "--http_port".to_string(),
            format!("{}", port),
            "--path".to_string(),
            data_dir.path().display().to_string(),
        ];

        let child = tokio::process::Command::new("clickhouse")
            .args(&args)
            // ClickHouse internall tees its logs to a file, so we throw away std{in,out,err}
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            // By default ClickHouse forks a child if it's been explicitly requested via the
            // following environment variable, _or_ if it's not attached to a TTY. Avoid this
            // behavior, so that we can correctly deliver SIGINT. The "watchdog" masks SIGINT,
            // meaning we'd have to deliver that to the _child_, which is more complicated.
            .env("CLICKHOUSE_WATCHDOG_ENABLE", "0")
            .spawn()?;

        // Wait for the "status" file to become available, at least with a newline.
        let data_path = data_dir.path().to_path_buf();
        let status_file = data_path.join("status");
        poll::wait_for_condition(
            || async {
                let contents =
                    match tokio::fs::read_to_string(&status_file).await {
                        Ok(contents) => contents,
                        Err(e) => match e.kind() {
                            std::io::ErrorKind::NotFound => {
                                return Err(poll::CondCheckError::NotYet)
                            }
                            _ => return Err(poll::CondCheckError::from(e)),
                        },
                    };
                if !contents.is_empty() && contents.contains('\n') {
                    Ok(())
                } else {
                    Err(poll::CondCheckError::NotYet)
                }
            },
            &Duration::from_millis(500),
            &CLICKHOUSE_TIMEOUT,
        )
        .await?;

        // Discover the HTTP port on which we're listening, if it's not provided explicitly by the
        // caller.
        let port = if port != 0 {
            port
        } else {
            discover_local_listening_port(&log_path, CLICKHOUSE_TIMEOUT).await?
        };

        Ok(Self {
            data_dir: Some(data_dir),
            data_path,
            port,
            args,
            child: Some(child),
        })
    }

    /// Wait for the ClickHouse server process to shutdown, after it's been killed.
    pub async fn wait_for_shutdown(&mut self) -> Result<(), anyhow::Error> {
        if let Some(mut child) = self.child.take() {
            child.wait().await?;
        }
        self.cleanup().await
    }

    /// Kill the ClickHouse server process and cleanup the data directory.
    pub async fn cleanup(&mut self) -> Result<(), anyhow::Error> {
        if let Some(mut child) = self.child.take() {
            child.start_kill().context("Sending SIGKILL to child")?;
            child.wait().await.context("waiting for child")?;
        }
        if let Some(dir) = self.data_dir.take() {
            dir.close().context("Cleaning up temporary directory")?;

            // ClickHouse doesn't fully respect the `--path` flag, and still seems
            // to put the `preprocessed_configs` directory in $CWD.
            let _ = std::fs::remove_dir_all("./preprocessed_configs");
        }
        Ok(())
    }

    /// Return the full path to the directory used for the server's data.
    pub fn data_path(&self) -> &Path {
        &self.data_path
    }

    /// Return the command-line used to start the ClickHouse server process
    pub fn cmdline(&self) -> &Vec<String> {
        &self.args
    }

    /// Return the child PID, if any
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(|child| child.id())
    }

    /// Return the HTTP port the server is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for ClickHouseInstance {
    fn drop(&mut self) {
        if self.child.is_some() || self.data_dir.is_some() {
            eprintln!(
                "WARN: dropped ClickHouseInstance without cleaning it up first \
                (there may still be a child process running and a \
                temporary directory leaked)"
            );
            if let Some(child) = self.child.as_mut() {
                let _ = child.start_kill();
            }
            if let Some(dir) = self.data_dir.take() {
                let _ = dir.close();
            }
        }
    }
}

// Parse the ClickHouse log file at the given path, looking for a line reporting the port number of
// the HTTP server. This is only used if the port is chosen by the OS, not the caller.
async fn discover_local_listening_port(
    path: &Path,
    timeout: Duration,
) -> Result<u16, anyhow::Error> {
    let timeout = Instant::now() + timeout;
    tokio::time::timeout_at(timeout, find_clickhouse_port_in_log(path))
        .await
        .context("Failed to find ClickHouse port within timeout")?
}

// Parse the clickhouse log for a port number.
//
// NOTE: This function loops forever until the expected line is found. It should be run under a
// timeout, or some other mechanism for cancelling it.
async fn find_clickhouse_port_in_log(
    path: &Path,
) -> Result<u16, anyhow::Error> {
    let reader = BufReader::new(
        File::open(path).await.context("Failed to open ClickHouse log file")?,
    );
    const NEEDLE: &str = "<Information> Application: Listening for http://";
    let mut lines = reader.lines();
    loop {
        let line = lines
            .next_line()
            .await
            .context("Failed to read line from ClickHouse log file")?;
        match line {
            Some(line) => {
                if let Some(needle_start) = line.find(&NEEDLE) {
                    // The address is currently written as ":PORT", but may in the future be written as
                    // "ADDR:PORT" or "HOST:PORT". Split on the colon, and parse the port number, rather
                    // than assuming the address conforms to a specific syntax.
                    let address_start = needle_start + NEEDLE.len();
                    return line[address_start..]
                        .trim()
                        .split(':')
                        .last()
                        .context("ClickHouse log file does not contain the expected HTTP listening address")?
                        .parse()
                        .context("Failed to parse ClickHouse port number from log");
                }
            }
            None => {
                // Reached EOF, just sleep for an interval and check again.
                sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{discover_local_listening_port, CLICKHOUSE_TIMEOUT};
    use std::{io::Write, time::Duration};
    use tempfile::NamedTempFile;
    use tokio::{task::spawn, time::sleep};

    const EXPECTED_PORT: u16 = 12345;

    #[tokio::test]
    async fn test_discover_local_listening_port() {
        // Write some data to a fake log file
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "A garbage line").unwrap();
        writeln!(
            file,
            "<Information> Application: Listening for http://127.0.0.1:{}",
            EXPECTED_PORT
        )
        .unwrap();
        writeln!(file, "Another garbage line").unwrap();
        file.flush().unwrap();

        assert_eq!(
            discover_local_listening_port(file.path(), CLICKHOUSE_TIMEOUT)
                .await
                .unwrap(),
            EXPECTED_PORT
        );
    }

    // A regression test for #131.
    //
    // The function `discover_local_listening_port` initially read from the log file until EOF, but
    // there's no guarantee that ClickHouse has written the port we're searching for before the
    // reader consumes the whole file. This test confirms that the file is read until the line is
    // found, ignoring EOF, at least until the timeout is hit.
    #[tokio::test]
    async fn test_discover_local_listening_port_slow_write() {
        // In this case the writer is slightly "slower" than the reader.
        let writer_interval = Duration::from_millis(20);
        assert_eq!(
            read_log_file(CLICKHOUSE_TIMEOUT, writer_interval).await.unwrap(),
            EXPECTED_PORT
        );
    }

    // An extremely slow write test, to verify the timeout handling.
    #[tokio::test]
    async fn test_discover_local_listening_port_timeout() {
        // In this case, the writer is _much_ slower than the reader, so that the reader times out
        // entirely before finding the desired line.
        let reader_timeout = Duration::from_millis(1);
        let writer_interval = Duration::from_millis(100);
        assert!(read_log_file(reader_timeout, writer_interval).await.is_err());
    }

    // Implementation of the above tests, simulating simultaneous reading/writing of the log file
    //
    // This uses Tokio's test utilities to manage time, rather than relying on timeouts.
    async fn read_log_file(
        reader_timeout: Duration,
        writer_interval: Duration,
    ) -> Result<u16, anyhow::Error> {
        async fn write_and_wait(
            file: &mut NamedTempFile,
            line: String,
            interval: Duration,
        ) {
            println!("Writing to log file");
            writeln!(file, "{}", line).unwrap();
            file.flush().unwrap();
            sleep(interval).await;
        }

        // Start a task that slowly writes lines to the log file.
        let mut file = NamedTempFile::new()?;
        let path = file.path().to_path_buf();
        let writer_task = spawn(async move {
            write_and_wait(
                &mut file,
                "A garbage line".to_string(),
                writer_interval,
            )
            .await;
            write_and_wait(
                &mut file,
                format!(
                    "<Information> Application: Listening for http://127.0.0.1:{}",
                    EXPECTED_PORT
                ),
                writer_interval,
            )
            .await;
            write_and_wait(
                &mut file,
                "Another garbage line".to_string(),
                writer_interval,
            )
            .await;
        });
        println!("Starting reader task");
        let reader_task = discover_local_listening_port(&path, reader_timeout);

        // "Run" the test.
        //
        // We pause tokio's internal timer and advance it by the writer interval. This simulates
        // the writer sleeping for a time between the write of each line, without explicitly
        // sleeping the whole test thread. Note that the futures for the reader/writer tasks must
        // be pinned to the stack, so that they may be polled on multiple passes through the select
        // loop without consuming them.
        tokio::time::pause();
        tokio::pin!(writer_task);
        tokio::pin!(reader_task);
        let reader_result = loop {
            tokio::select! {
                reader_result = &mut reader_task => {
                    println!("Reader finished");
                    break reader_result;
                },
                writer_result = &mut writer_task => {
                    println!("Writer finished");
                    let _ = writer_result.unwrap();
                },
                _ = tokio::time::advance(writer_interval) => {
                    println!("Advancing time by {:#?}", writer_interval);
                }
            }
        };
        // Resume Tokio's timer
        tokio::time::resume();
        reader_result
    }
}
