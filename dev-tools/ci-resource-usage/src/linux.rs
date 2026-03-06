use crate::{CpuCore, Io, Memory, Swap};
use anyhow::{Context, Error, anyhow, bail};

pub(crate) fn gather_cpu_cores() -> Result<Vec<CpuCore>, Error> {
    fn field(iter: &mut dyn Iterator<Item = &str>) -> Result<u64, Error> {
        let value = iter
            .next()
            .ok_or_else(|| anyhow!("there are less fields than expected"))?;
        Ok(value.parse()?)
    }

    // man 5 proc_stat
    let stat = std::fs::read_to_string("/proc/stat")
        .context("failed to read /proc/stat")?;

    let mut per_core = Vec::new();
    for line in stat.lines() {
        let mut parser = line.split(' ').filter(|w| !w.is_empty());
        let Some(key) = parser.next() else { continue };
        if !key.starts_with("cpu") || key == "cpu" {
            continue;
        }

        let user = field(&mut parser)?;
        let nice = field(&mut parser)?;
        let system = field(&mut parser)?;
        let idle = field(&mut parser)?;
        let iowait = field(&mut parser)?;
        let irq = field(&mut parser)?;
        let softirq = field(&mut parser)?;
        let steal = field(&mut parser)?;
        let guest = field(&mut parser)?;
        let guest_nice = field(&mut parser)?;

        let used_ticks =
            user + nice + system + irq + softirq + steal + guest + guest_nice;
        per_core.push(CpuCore {
            total_ticks: used_ticks + idle + iowait,
            wait_ticks: iowait,
            used_ticks,
        });
    }

    Ok(per_core)
}

pub(crate) fn gather_io() -> Result<Io, Error> {
    // https://www.kernel.org/doc/html/latest/admin-guide/iostats.html
    let diskstats = std::fs::read_to_string("/proc/diskstats")
        .context("failed to read /proc/diskstats")?;

    let mut io = Io { read_ops: 0, write_ops: 0 };
    for line in diskstats.lines() {
        let mut parser = line.split(' ').filter(|w| !w.is_empty());
        let Some(device_minor) = parser.nth(1) else { continue };
        let _name = parser.next();
        let reads_completed: u64 =
            parser.nth(1).ok_or_else(|| anyhow!("too few fields"))?.parse()?;
        let writes_completed: u64 =
            parser.nth(3).ok_or_else(|| anyhow!("too few fields"))?.parse()?;

        // /proc/diskstats contains data for both disks as a whole and
        // individual partitions. The whole disk is always partition zero, so
        // ignore everything that is a partition (device minor number > 0).
        if device_minor != "0" {
            continue;
        }

        io.read_ops += reads_completed;
        io.write_ops += writes_completed;
    }

    Ok(io)
}

pub(crate) fn gather_load_1min() -> Result<f64, Error> {
    let loadavg = std::fs::read_to_string("/proc/loadavg")
        .context("failed to read /proc/loadavg")?;
    let Some((load_1min, _rest)) = loadavg.split_once(' ') else {
        bail!("invalid contents for /proc/loadavg");
    };
    Ok(load_1min.parse()?)
}

pub(crate) fn gather_memory() -> Result<Memory, Error> {
    fn parse_value(raw: &str) -> Result<u64, Error> {
        let Some((value, unit)) = raw.trim().split_once(' ') else {
            bail!("missing space in value");
        };
        if unit != "kB" {
            bail!("unexpected unit: {unit}");
        }
        Ok(value.parse::<u64>()? * 1000)
    }

    // man 5 proc_meminfo
    let meminfo = std::fs::read_to_string("/proc/meminfo")
        .context("failed to read /proc/meminfo")?;

    let mut total = None;
    let mut available = None;
    for line in meminfo.lines() {
        let Some((key, value)) = line.split_once(':') else { continue };
        match key {
            "MemTotal" => total = Some(parse_value(value)?),
            "MemAvailable" => available = Some(parse_value(value)?),
            _ => {}
        }
    }

    Ok(Memory {
        total: total
            .ok_or_else(|| anyhow!("missing MemTotal in /proc/meminfo"))?,
        available: available
            .ok_or_else(|| anyhow!("missing MemAvailable in /proc/meminfo"))?,
    })
}

pub(crate) fn gather_swap() -> Result<Swap, Error> {
    let vmstat = std::fs::read_to_string("/proc/vmstat")
        .context("failed to read /proc/vmstat")?;

    let mut read = None;
    let mut write = None;
    for line in vmstat.lines() {
        let Some((key, value)) = line.split_once(' ') else { continue };
        match key {
            "pswpin" => read = Some(value.parse()?),
            "pswpout" => write = Some(value.parse()?),
            _ => {}
        }
    }

    Ok(Swap {
        read: read.ok_or_else(|| anyhow!("missing pswpin in /proc/vmstat"))?,
        write: write
            .ok_or_else(|| anyhow!("missing pswpout in /proc/vmstat"))?,
    })
}
