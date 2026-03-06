#[cfg(target_os = "illumos")]
mod illumos;
#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "illumos")]
use crate::illumos as sys;
#[cfg(target_os = "linux")]
use crate::linux as sys;

use anyhow::{Context, Error, bail};
use std::fs::File;
use std::io::Write;
use std::iter::{Sum, zip};
use std::ops::Sub;
use std::time::{Duration, SystemTime};

fn main() -> Result<(), Error> {
    let mut args = std::env::args_os().skip(1);
    let mut dest: Box<dyn Write> = match args.next() {
        Some(path) => Box::new(File::create_new(&path).with_context(|| {
            format!("failed to write to {}", path.display())
        })?),
        None => Box::new(std::io::stdout().lock()),
    };
    if args.next().is_some() {
        bail!("usage: ci-resource-usage [output-path]");
    }

    let mut prev_cores = sys::gather_cpu_cores()?;
    let mut prev_io = sys::gather_io()?;
    let mut prev_swap = sys::gather_swap()?;
    writeln!(
        dest,
        "timestamp,memory_used,load_1min,swap_read,swap_write,iops_read,\
         iops_write,cpu_wait,cpu_used{}",
        (0..prev_cores.len())
            .map(|idx| format!(",cpu{idx}_used"))
            .collect::<String>()
    )?;
    loop {
        std::thread::sleep(Duration::from_secs(1));

        let mem = sys::gather_memory()?;
        let curr_cores = sys::gather_cpu_cores()?;
        let curr_io = sys::gather_io()?;
        let io = curr_io - prev_io;
        let curr_swap = sys::gather_swap()?;
        let swap = curr_swap - prev_swap;

        let curr_cpu: CpuCore = curr_cores.iter().copied().sum();
        let prev_cpu: CpuCore = prev_cores.iter().copied().sum();
        let cpu = curr_cpu - prev_cpu;

        writeln!(
            dest,
            "{},{:.3},{:.2},{},{},{},{},{:.3},{:.3}{}",
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs(),
            (mem.total - mem.available) as f64 / mem.total as f64,
            sys::gather_load_1min()?,
            swap.read,
            swap.write,
            io.read_ops,
            io.write_ops,
            cpu.wait_ticks as f64 / cpu.total_ticks as f64,
            cpu.used_ticks as f64 / cpu.total_ticks as f64,
            zip(&curr_cores, &prev_cores)
                .map(|(curr, prev)| *curr - *prev)
                .map(|core| core.used_ticks as f64 / core.total_ticks as f64)
                .map(|percentage| format!(",{percentage:.3}"))
                .collect::<String>()
        )?;
        dest.flush()?;

        prev_cores = curr_cores;
        prev_io = curr_io;
        prev_swap = curr_swap;
    }
}

#[derive(Debug, Clone, Copy)]
struct Memory {
    total: u64,
    available: u64,
}

#[derive(Debug, Clone, Copy)]
struct CpuCore {
    total_ticks: u64,
    used_ticks: u64,
    wait_ticks: u64,
}

impl Sub<CpuCore> for CpuCore {
    type Output = CpuCore;

    fn sub(self, rhs: CpuCore) -> Self::Output {
        CpuCore {
            total_ticks: self.total_ticks - rhs.total_ticks,
            used_ticks: self.used_ticks - rhs.used_ticks,
            wait_ticks: self.wait_ticks - rhs.wait_ticks,
        }
    }
}

impl Sum for CpuCore {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(
            CpuCore { total_ticks: 0, used_ticks: 0, wait_ticks: 0 },
            |acc, entry| CpuCore {
                total_ticks: acc.total_ticks + entry.total_ticks,
                used_ticks: acc.used_ticks + entry.used_ticks,
                wait_ticks: acc.wait_ticks + entry.wait_ticks,
            },
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct Swap {
    read: u64,
    write: u64,
}

impl Sub<Swap> for Swap {
    type Output = Swap;

    fn sub(self, rhs: Swap) -> Self::Output {
        Swap {
            read: self.read - rhs.read,
            write: self.write - rhs.write,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Io {
    read_ops: u64,
    write_ops: u64,
}

impl Sub<Io> for Io {
    type Output = Io;

    fn sub(self, rhs: Io) -> Self::Output {
        Io {
            read_ops: self.read_ops - rhs.read_ops,
            write_ops: self.write_ops - rhs.write_ops,
        }
    }
}
