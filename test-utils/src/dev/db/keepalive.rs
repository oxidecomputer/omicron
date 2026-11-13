// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Regression coverage for the production SQL keepalive setting. The manifest
//! check runs everywhere; the socket check requires illumos's libproc.
//!
//! Run with the pinned CockroachDB binary on PATH:
//! `cargo nextest run -p omicron-test-utils -E 'test(sql_keepalive)'`.
//! The illumos test inspects its own child process without forcing attachment
//! or skipping failures when process inspection is unavailable. It does not
//! exercise SMF delivery or measure failure-detection timing.

const KEEPALIVE_ENV: &str = "COCKROACH_SQL_TCP_KEEP_ALIVE";

fn production_keepalive() -> String {
    use quick_xml::events::Event;

    // Parse XML so comments and attribute order cannot mask a missing setting.
    // This checks the declaration, not SMF's environment delivery.
    let manifest = include_str!("../../../../smf/cockroachdb/manifest.xml");
    let mut reader = quick_xml::Reader::from_str(manifest);
    reader.config_mut().expand_empty_elements = true;
    let mut service = false;
    let mut start = false;
    let mut environment = false;
    let mut values = Vec::new();
    loop {
        match reader.read_event().expect("valid manifest XML") {
            Event::Start(element) => {
                let attribute = |name: &str| {
                    element.try_get_attribute(name).unwrap().map(|a| {
                        a.decoded_and_normalized_value(
                            quick_xml::XmlVersion::Implicit1_0,
                            reader.decoder(),
                        )
                        .unwrap()
                        .into_owned()
                    })
                };
                match element.name().as_ref() {
                    b"service" => {
                        service = attribute("name").as_deref()
                            == Some("oxide/cockroachdb");
                    }
                    b"exec_method" => {
                        start = service
                            && attribute("name").as_deref() == Some("start");
                    }
                    b"method_environment" => environment = start,
                    b"envvar"
                        if environment
                            && attribute("name").as_deref()
                                == Some(KEEPALIVE_ENV) =>
                    {
                        values.push(attribute("value").expect("envvar value"));
                    }
                    _ => {}
                }
            }
            Event::End(element) => match element.name().as_ref() {
                b"service" => service = false,
                b"exec_method" => start = false,
                b"method_environment" => environment = false,
                _ => {}
            },
            Event::Eof => break,
            _ => {}
        }
    }
    assert_eq!(values.len(), 1, "expected exactly one {KEEPALIVE_ENV}");
    let value = values.pop().unwrap();
    assert_eq!(value, "30s", "production SQL keepalive idle threshold");
    value
}

#[test]
fn test_sql_keepalive_manifest() {
    production_keepalive();
}

#[cfg(target_os = "illumos")]
mod illumos {
    use anyhow::{Context, Result, bail, ensure};
    use libc::{c_int, c_void, pid_t, sockaddr, sockaddr_storage, socklen_t};
    use std::net::SocketAddr;

    // Only the libproc interfaces needed to read the child server's socket.
    // The descriptor belongs to CockroachDB, not to this test process.
    #[repr(C)]
    struct ProcHandle {
        _private: [u8; 0],
    }

    #[link(name = "proc")]
    unsafe extern "C" {
        fn Pgrab(
            pid: pid_t,
            flags: c_int,
            error: *mut c_int,
        ) -> *mut ProcHandle;
        fn Pcreate_agent(process: *mut ProcHandle) -> c_int;
        fn Pdestroy_agent(process: *mut ProcHandle);
        fn Prelease(process: *mut ProcHandle, flags: c_int);
        fn pr_getpeername(
            process: *mut ProcHandle,
            fd: c_int,
            address: *mut sockaddr,
            len: *mut socklen_t,
        ) -> c_int;
        fn pr_getsockopt(
            process: *mut ProcHandle,
            fd: c_int,
            level: c_int,
            option: c_int,
            value: *mut c_void,
            len: *mut c_int,
        ) -> c_int;
    }

    struct Process {
        handle: *mut ProcHandle,
        agent: bool,
    }

    impl Process {
        fn grab(pid: u32) -> Result<Self> {
            let mut error = 0;
            // SAFETY: error is writable; flags=0 does not force attachment if
            // another debugger already controls the process.
            let handle = unsafe { Pgrab(pid.try_into()?, 0, &mut error) };
            ensure!(!handle.is_null(), "Pgrab({pid}) failed: code {error}");
            let mut process = Self { handle, agent: false };
            // SAFETY: handle is owned and valid until Process is dropped.
            if unsafe { Pcreate_agent(handle) } != 0 {
                return Err(std::io::Error::last_os_error())
                    .context("Pcreate_agent");
            }
            process.agent = true;
            Ok(process)
        }

        fn peer(&self, fd: c_int) -> Option<SocketAddr> {
            // SAFETY: sockaddr_storage can be zero-initialized and is large
            // and aligned enough for either supported address family.
            let mut storage: sockaddr_storage = unsafe { std::mem::zeroed() };
            let mut len = size_of::<sockaddr_storage>() as socklen_t;
            // SAFETY: both output buffers are valid for the supplied lengths.
            if unsafe {
                pr_getpeername(
                    self.handle,
                    fd,
                    (&mut storage as *mut sockaddr_storage).cast(),
                    &mut len,
                )
            } != 0
            {
                // Most descriptors are not connected sockets.
                return None;
            }
            if len as usize > size_of::<sockaddr_storage>() {
                return None;
            }
            // SAFETY: a successful getpeername initialized the address and
            // its family/length; the buffer is large enough for that length.
            unsafe { socket2::SockAddr::new(storage, len) }.as_socket()
        }

        fn option(
            &self,
            fd: c_int,
            level: c_int,
            option: c_int,
        ) -> Result<c_int> {
            let mut value: c_int = 0;
            let mut len = size_of::<c_int>() as c_int;
            // SAFETY: value and len are writable and sized for an integer
            // socket option. All callers request integer options.
            if unsafe {
                pr_getsockopt(
                    self.handle,
                    fd,
                    level,
                    option,
                    (&mut value as *mut c_int).cast(),
                    &mut len,
                )
            } != 0
            {
                return Err(std::io::Error::last_os_error()).with_context(
                    || format!("getsockopt(fd={fd}, option={option})"),
                );
            }
            ensure!(len as usize == size_of::<c_int>(), "socket option size");
            Ok(value)
        }
    }

    impl Drop for Process {
        fn drop(&mut self) {
            // SAFETY: this owns the handle. Always release the server, even
            // when inspection fails or an assertion unwinds.
            unsafe {
                if self.agent {
                    Pdestroy_agent(self.handle);
                }
                Prelease(self.handle, 0);
            }
        }
    }

    fn sql_socket_options(
        pid: u32,
        client: SocketAddr,
    ) -> Result<(c_int, c_int)> {
        let process = Process::grab(pid)?;
        for entry in std::fs::read_dir(format!("/proc/{pid}/fd"))? {
            let entry = entry?;
            let Some(fd) =
                entry.file_name().to_str().and_then(|s| s.parse().ok())
            else {
                continue;
            };
            // Match the exact client endpoint, excluding the listener and
            // CockroachDB's internal connections to its own SQL/RPC port.
            if process.peer(fd) == Some(client) {
                return Ok((
                    process.option(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE)?,
                    process.option(
                        fd,
                        libc::IPPROTO_TCP,
                        libc::TCP_KEEPALIVE_THRESHOLD,
                    )?,
                ));
            }
        }
        bail!("no accepted SQL socket in pid {pid} with peer {client}")
    }

    #[tokio::test]
    async fn test_sql_keepalive_socket() -> Result<()> {
        let mut builder = super::super::CockroachStarterBuilder::new();
        builder.env(super::KEEPALIVE_ENV, super::production_keepalive());
        let mut database = builder.build()?.start().await?;

        let result = async {
            let stream =
                tokio::net::TcpStream::connect(database.pg_config().address())
                    .await?;
            let client_address = stream.local_addr()?;
            let mut config = tokio_postgres::Config::new();
            config.user("root").dbname("defaultdb");
            let client = super::super::Client::from(
                config.connect_raw(stream, tokio_postgres::NoTls).await?,
            );
            // A successful SQL exchange ensures cmux has dispatched to the
            // SQL handler, which applies the keepalive setting.
            client.simple_query("SELECT 1").await?;
            let (enabled, idle_ms) =
                sql_socket_options(database.pid(), client_address)?;
            client.cleanup().await?;
            ensure!(enabled != 0, "server SQL socket keepalive is disabled");
            ensure!(
                idle_ms == 30_000,
                "server SQL idle threshold: {idle_ms}ms"
            );
            // Do not assert kernel defaults for the interval/count/abort
            // policy: this PR only configures the idle threshold.
            Ok::<_, anyhow::Error>(())
        }
        .await;
        database.cleanup().await?;
        result
    }
}
