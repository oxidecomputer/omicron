/// Miscellaneous FFI wrapper functions for libc

/// sysconf(3c)
pub fn sysconf(arg: i32) -> std::io::Result<i64> {
    let res = unsafe { libc::sysconf(arg) };
    if res == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(res)
}
