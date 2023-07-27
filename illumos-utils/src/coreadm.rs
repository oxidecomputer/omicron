use crate::{execute, ExecutionError};
use std::process::Command;

const COREADM: &str = "/usr/bin/coreadm";

pub struct CoreAdm {
    cmd: Command,
}

pub enum CoreFileOption {
    Global,
    GlobalSetid,
    Log,
    Process,
    ProcSetid,
}

impl AsRef<str> for CoreFileOption {
    fn as_ref(&self) -> &str {
        match self {
            CoreFileOption::Global => "global",
            CoreFileOption::GlobalSetid => "global-setid",
            CoreFileOption::Log => "log",
            CoreFileOption::Process => "process",
            CoreFileOption::ProcSetid => "proc-setid",
        }
    }
}

impl CoreAdm {
    pub fn new() -> Self {
        let mut cmd = Command::new(COREADM);
        cmd.env_clear();
        Self { cmd }
    }

    pub fn disable(&mut self, opt: CoreFileOption) {
        self.cmd.arg("-d").arg(opt.as_ref());
    }

    pub fn enable(&mut self, opt: CoreFileOption) {
        self.cmd.arg("-e").arg(opt.as_ref());
    }

    pub fn global_pattern(&mut self, pat: impl AsRef<std::ffi::OsStr>) {
        self.cmd.arg("-g").arg(pat);
    }

    pub fn global_contents(&mut self, contents: &str) {
        self.cmd.arg("-G").arg(contents);
    }

    pub fn execute(mut self) -> Result<(), ExecutionError> {
        execute(&mut self.cmd)?;
        Ok(())
    }
}
