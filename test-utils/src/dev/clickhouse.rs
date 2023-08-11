// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tools for managing ClickHouse during development

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use tempfile::TempDir;
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
    time::{sleep, Instant},
};

use crate::dev::poll;

// Timeout used when starting up ClickHouse subprocess.
const CLICKHOUSE_TIMEOUT: Duration = Duration::from_secs(30);

/// A `ClickHouseInstance` is used to start and manage a ClickHouse single node server process.
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

#[derive(Debug, Error)]
pub enum ClickHouseError {
    #[error("Failed to open ClickHouse log file")]
    Io(#[from] std::io::Error),

    #[error("Invalid ClickHouse port number")]
    InvalidPort,

    #[error("Invalid ClickHouse listening address")]
    InvalidAddress,

    #[error("Invalid ClickHouse Keeper ID")]
    InvalidKeeperId,

    #[error("Failed to detect ClickHouse subprocess within timeout")]
    Timeout,
}

impl ClickHouseInstance {
    /// Start a new single node ClickHouse server on the given IPv6 port.
    pub async fn new_single_node(port: u16) -> Result<Self, anyhow::Error> {
        let data_dir = TempDir::new()
            .context("failed to create tempdir for ClickHouse data")?;
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
            // ClickHouse internally tees its logs to a file, so we throw away
            // std{in,out,err}
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            // By default ClickHouse forks a child if it's been explicitly
            // requested via the following environment variable, _or_ if it's
            // not attached to a TTY. Avoid this behavior, so that we can
            // correctly deliver SIGINT. The "watchdog" masks SIGINT, meaning
            // we'd have to deliver that to the _child_, which is more
            // complicated.
            .env("CLICKHOUSE_WATCHDOG_ENABLE", "0")
            .spawn()
            .with_context(|| {
                format!("failed to spawn `clickhouse` (with args: {:?})", &args)
            })?;

        let data_path = data_dir.path().to_path_buf();
        let port = wait_for_port(log_path).await?;

        Ok(Self {
            data_dir: Some(data_dir),
            data_path,
            port,
            args,
            child: Some(child),
        })
    }

    /// Start a new replicated ClickHouse server on the given IPv6 port.
    pub async fn new_replicated(
        port: String,
        tcp_port: String,
        name: String,
        r_number: String,
        //config_path: String,
        config_path: PathBuf,
    ) -> Result<Self, anyhow::Error> {
        let data_dir = TempDir::new()
            .context("failed to create tempdir for ClickHouse data")?;

        // Copy config template to new temporary directory
     //   let cur_dir = std::env::current_dir()?;
       // let config_path = cur_dir.as_path().join("clickhouse_configs/replica_config.xml");
        let con = data_dir.path().join("replica_config.xml");

        // debug
        println!("config source: {}", config_path.display());
        println!("config destination: {}", con.display());

        std::fs::copy(&config_path, &con)?;

        assert!(con.exists());

        use tokio::io::AsyncReadExt;
        
 //debug
 let mut file = File::open(&con).await
 .expect("err File not found");
 let mut data = String::new();
file.read_to_string(&mut data).await
 .expect("Error while reading file");
println!("{}", data); 

        let log_path = data_dir.path().join("clickhouse-server.log");
        let err_log_path = data_dir.path().join("clickhouse-server.errlog");
        let tmp_path = data_dir.path().join("/tmp/");
        let user_files_path = data_dir.path().join("/user_files/");
        let access_path = data_dir.path().join("/access/");
        let format_schemas_path = data_dir.path().join("/format_schemas/");
        let args = vec![
            "server".to_string(),
            "--config-file".to_string(),
            //format!("{}", config_path),
            format!("{}", con.display()),
        ];

        let child = tokio::process::Command::new("clickhouse")
            .args(&args)
        //    .stdin(Stdio::null())
          //  .stdout(Stdio::null())
        //    .stderr(Stdio::null())
            .env("CLICKHOUSE_WATCHDOG_ENABLE", "0")
            .env("CH_LOG", &log_path)
            .env("CH_ERROR_LOG", err_log_path)
            .env("CH_REPLICA_DISPLAY_NAME", name)
            .env("CH_LISTEN_ADDR", "::1")
            .env("CH_LISTEN_PORT", port.clone())
            .env("CH_TCP_PORT", tcp_port)
            .env("CH_DATASTORE", data_dir.path())
            .env("CH_TMP_PATH", tmp_path)
            .env("CH_USER_FILES_PATH", user_files_path)
            .env("CH_USER_LOCAL_DIR", access_path)
            .env("CH_FORMAT_SCHEMA_PATH", format_schemas_path)
            .env("CH_REPLICA_NUMBER", r_number)
            .env("CH_REPLICA_HOST_01", "::1")
            .env("CH_REPLICA_HOST_02", "::1")
            .env("CH_KEEPER_HOST_01", "::1")
            .env("CH_KEEPER_HOST_02", "::1")
            .env("CH_KEEPER_HOST_03", "::1")
            .spawn()
            .with_context(|| {
                format!("failed to spawn `clickhouse` (with args: {:?})", &args)
            })?;

         println!("{}", log_path.display());
        // println!("config path: {}", config_path);

        //debugging
    //    let srcdir = PathBuf::from(config_path);
      //  println!("{:?}", std::fs::canonicalize(&srcdir));




        let data_path = data_dir.path().to_path_buf();
      //  let port = wait_for_port(log_path).await?;
      let port: u16 = port.parse()?;

        Ok(Self {
            data_dir: Some(data_dir),
            data_path,
            port,
            args,
            child: Some(child),
        })
    }

    /// Start a new ClickHouse keeper on the given IPv6 port.
    pub async fn new_keeper(
        port: String,
        k_id: String,
        config_path: String,
    ) -> Result<Self, anyhow::Error> {
        // We assume that only 3 keepers will be run, and the ID of the keeper can only
        // be one of "1", "2" or "3". This is to avoid having to pass the IDs of the
        // other keepers as part of the function's parameters.
        if !["1", "2", "3"].contains(&k_id.as_str()) {
            return Err(ClickHouseError::InvalidKeeperId.into());
        }
        let data_dir = TempDir::new()
            .context("failed to create tempdir for ClickHouse Keeper data")?;
        let log_path = data_dir.path().join("clickhouse-keeper.log");
        let err_log_path = data_dir.path().join("clickhouse-keeper.err.log");
        let log_storage_path = data_dir.path().join("/log");
        let snapshot_storage_path = data_dir.path().join("/snapshots");
        let args = vec![
            "keeper".to_string(),
            "--config-file".to_string(),
            format!("{}", config_path),
        ];

        let child = tokio::process::Command::new("clickhouse")
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .env("CLICKHOUSE_WATCHDOG_ENABLE", "0")
            .env("CH_LOG", &log_path)
            .env("CH_ERROR_LOG", err_log_path)
            .env("CH_LISTEN_ADDR", "::1")
            .env("CH_LISTEN_PORT", &port)
            .env("CH_KEEPER_ID_CURRENT", k_id)
            .env("CH_DATASTORE", data_dir.path())
            .env("CH_LOG_STORAGE_PATH", log_storage_path)
            .env("CH_SNAPSHOT_STORAGE_PATH", snapshot_storage_path)
            .env("CH_KEEPER_ID_01", "1")
            .env("CH_KEEPER_ID_02", "2")
            .env("CH_KEEPER_ID_03", "3")
            .env("CH_KEEPER_HOST_01", "::1")
            .env("CH_KEEPER_HOST_02", "::1")
            .env("CH_KEEPER_HOST_03", "::1")
            .spawn()
            .with_context(|| {
                format!(
                    "failed to spawn `clickhouse keeper` (with args: {:?})",
                    &args
                )
            })?;

        let data_path = data_dir.path().to_path_buf();
        let port: u16 = port.parse()?;

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

// Wait for the ClickHouse log file to become available, including the
// port number.
//
// We extract the port number from the log-file regardless of whether we
// know it already, as this is a more reliable check that the server is
// up and listening. Previously we only did this in the case we need to
// _learn_ the port, which introduces the possibility that we return
// from this function successfully, but the server itself is not yet
// ready to accept connections.
pub async fn wait_for_port(log_path: PathBuf) -> Result<u16, anyhow::Error> {
    let p = poll::wait_for_condition(
        || async {
            let result =
                discover_local_listening_port(&log_path, CLICKHOUSE_TIMEOUT)
                    .await;
            match result {
                // Successfully extracted the port, return it.
                Ok(port) => Ok(port),
                Err(e) => {
                    match e {
                        ClickHouseError::Io(ref inner) => {
                            if matches!(
                                inner.kind(),
                                std::io::ErrorKind::NotFound
                            ) {
                                return Err(poll::CondCheckError::NotYet);
                            }
                        }
                        _ => {}
                    }
                    Err(poll::CondCheckError::from(e))
                }
            }
        },
        &Duration::from_millis(500),
        &CLICKHOUSE_TIMEOUT,
    )
    .await
    .context("waiting to discover ClickHouse port")?;
    Ok(p)
}

// Parse the ClickHouse log file at the given path, looking for a line reporting the port number of
// the HTTP server. This is only used if the port is chosen by the OS, not the caller.
async fn discover_local_listening_port(
    path: &Path,
    timeout: Duration,
) -> Result<u16, ClickHouseError> {
    let timeout = Instant::now() + timeout;
    tokio::time::timeout_at(timeout, find_clickhouse_port_in_log(path))
        .await
        .map_err(|_| ClickHouseError::Timeout)?
}

// Parse the clickhouse log for a port number.
//
// NOTE: This function loops forever until the expected line is found. It should be run under a
// timeout, or some other mechanism for cancelling it.
async fn find_clickhouse_port_in_log(
    path: &Path,
) -> Result<u16, ClickHouseError> {
    let mut reader = BufReader::new(File::open(path).await?);
    const NEEDLE: &str =
        "<Information> Application: Listening for http://[::1]";
    let mut lines = reader.lines();
    loop {
        let line = lines.next_line().await?;
        match line {
            Some(line) => {
                if let Some(needle_start) = line.find(NEEDLE) {
                    // Our needle ends with `http://[::1]`; we'll split on the
                    // colon we expect to follow it to find the port.
                    let address_start = needle_start + NEEDLE.len();
                    return line[address_start..]
                        .trim()
                        .split(':')
                        .last()
                        .ok_or_else(|| ClickHouseError::InvalidAddress)?
                        .parse()
                        .map_err(|_| ClickHouseError::InvalidPort);
                }
            }
            None => {
                // Reached EOF, just sleep for an interval and check again.
                sleep(Duration::from_millis(10)).await;

                // We might have gotten a partial line; close the file, reopen
                // it, and start reading again from the beginning.
                reader = BufReader::new(File::open(path).await?);
                lines = reader.lines();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        discover_local_listening_port, ClickHouseError, CLICKHOUSE_TIMEOUT,
    };
    use std::process::Stdio;
    use std::{io::Write, sync::Arc, time::Duration};
    use tempfile::NamedTempFile;
    use tokio::{sync::Mutex, task::spawn, time::sleep};

    const EXPECTED_PORT: u16 = 12345;

    #[tokio::test]
    async fn test_clickhouse_in_path() {
        // With no arguments, we expect to see the default help message.
        tokio::process::Command::new("clickhouse")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Cannot find 'clickhouse' on PATH. Refer to README.md for installation instructions");
    }

    #[tokio::test]
    async fn test_discover_local_listening_port() {
        // Write some data to a fake log file
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "A garbage line").unwrap();
        writeln!(
            file,
            "<Information> Application: Listening for http://[::1]:{}",
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
    ) -> Result<u16, ClickHouseError> {
        async fn write_and_wait(
            file: &mut NamedTempFile,
            line: String,
            interval: Duration,
        ) {
            println!(
                "Writing to log file: {:?}, contents: '{}'",
                file.path(),
                line
            );
            write!(file, "{}", line).unwrap();
            file.flush().unwrap();
            sleep(interval).await;
        }

        // Start a task that slowly writes lines to the log file.
        //
        // NOTE: This looks overly complicated, and it is. We have to wrap this in a mutex because
        // both this function, and the writer task we're spawning, need access to the file. They
        // may complete in any order, and so it's not possible to give one of them ownership over
        // the `NamedTempFile`. If the owning task completes, that may delete the file before the
        // other task accesses it. So we need interior mutability (because one of the references is
        // mutable for writing), and _this_ scope must own it.
        let file = Arc::new(Mutex::new(NamedTempFile::new()?));
        let path = file.lock().await.path().to_path_buf();
        let writer_file = file.clone();
        let writer_task = spawn(async move {
            let mut file = writer_file.lock().await;
            write_and_wait(
                &mut file,
                "A garbage line\n".to_string(),
                writer_interval,
            )
            .await;

            // Ensure we can still parse the line even if our buf reader hits
            // EOF in the middle of the line
            // (https://github.com/oxidecomputer/omicron/issues/3580).
            write_and_wait(
                &mut file,
                "<Information> Application: List".to_string(),
                writer_interval,
            )
            .await;
            write_and_wait(
                &mut file,
                format!("ening for http://[::1]:{}\n", EXPECTED_PORT),
                writer_interval,
            )
            .await;

            write_and_wait(
                &mut file,
                "Another garbage line\n".to_string(),
                writer_interval,
            )
            .await;
        });
        println!("Starting reader task");
        let reader_task = discover_local_listening_port(&path, reader_timeout);

        // "Run" the test.
        //
        // Note that the futures for the reader/writer tasks must be pinned to the stack, so that
        // they may be polled on multiple passes through the select loop without consuming them.
        tokio::pin!(writer_task);
        tokio::pin!(reader_task);
        let mut poll_writer = true;
        let reader_result = loop {
            tokio::select! {
                reader_result = &mut reader_task => {
                    println!("Reader finished");
                    break reader_result;
                },
                writer_result = &mut writer_task, if poll_writer => {
                    println!("Writer finished");
                    let _ = writer_result.unwrap();
                    poll_writer = false;
                },
            }
        };
        reader_result
    }
}
