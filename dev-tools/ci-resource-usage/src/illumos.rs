use crate::{CpuCore, Io, Memory, Swap};
use anyhow::{Context as _, Error, anyhow, bail};
use kstat_rs::{Ctl, Data, Kstat, NamedData};

pub(crate) fn gather_cpu_cores() -> Result<Vec<CpuCore>, Error> {
    let mut cores = Vec::new();
    let ctl = Ctl::new()?;
    for mut entry in ctl.filter(Some("cpu"), None, Some("sys")) {
        let parsed = KstatCpuCore::read(&ctl, &mut entry)?;

        let used_ticks = parsed.cpu_ticks_user + parsed.cpu_ticks_kernel;
        let wait_ticks = parsed.cpu_ticks_wait;
        cores.push(CpuCore {
            total_ticks: used_ticks + wait_ticks + parsed.cpu_ticks_idle,
            used_ticks,
            wait_ticks,
        });
    }
    Ok(cores)
}

pub(crate) fn gather_io() -> Result<Io, Error> {
    let mut io = Io { read_ops: 0, write_ops: 0 };
    let ctl = Ctl::new()?;
    for mut entry in ctl.filter(Some("blkdev"), None, None) {
        let Data::Io(data) = ctl.read(&mut entry)? else {
            bail!("expected io for {}", entry.ks_name);
        };
        io.read_ops += u64::from(data.reads);
        io.write_ops += u64::from(data.writes);
    }
    Ok(io)
}

pub(crate) fn gather_load_1min() -> Result<f64, Error> {
    let data: KstatSystemMisc = get("unix", 0, "system_misc")?;
    Ok(data.avenrun_1min as f64 / 256.0)
}

pub(crate) fn gather_memory() -> Result<Memory, Error> {
    let data: KstatSystemPages = get("unix", 0, "system_pages")?;
    Ok(Memory { total: data.physmem * 4096, available: data.freemem * 4096 })
}

pub(crate) fn gather_swap() -> Result<Swap, Error> {
    let mut swap = Swap { read: 0, write: 0 };
    let ctl = Ctl::new()?;
    for mut entry in ctl.filter(Some("cpu"), None, Some("vm")) {
        let parsed = KstatCpuVm::read(&ctl, &mut entry)?;
        swap.read += parsed.anonpgin;
        swap.write += parsed.anonpgout;
    }
    Ok(swap)
}

fn get<T: ReadNamed>(module: &str, inst: i32, name: &str) -> Result<T, Error> {
    let ctl = Ctl::new()?;
    let mut kstat = ctl
        .filter(Some(module), Some(inst), Some(name))
        .next()
        .ok_or_else(|| anyhow!("missing {module}:{inst}:{name} kstat"))?;
    T::read(&ctl, &mut kstat)
}

macro_rules! kstat_named {
    (struct $ident:ident { $($field:ident: $ty:ty,)* }) => {
        struct $ident {
            $($field: $ty,)*
        }

        impl ReadNamed for $ident {
            fn read(ctl: &Ctl, entry: &mut Kstat<'_>) -> Result<Self, Error> {
                let id = format!(
                    "{}:{}:{}",
                    entry.ks_module,
                    entry.ks_instance,
                    entry.ks_name,
                );

                let Data::Named(vec_named) = ctl.read(entry)? else {
                    bail!("expected named data for {id}");
                };

                $(let mut $field = None;)*
                for named in vec_named {
                    match named.name {
                        $(stringify!($field) => {
                            $field = Some(
                                <$ty>::from_named_data(named.value)
                                    .with_context(|| format!(
                                        "failed to parse field {} of {id}",
                                        named.name,
                                    ))?
                            );
                        })*
                        _ => {}
                    }
                }

                Ok(Self {
                    $(
                        $field: $field.ok_or_else(|| anyhow!(
                            "missing kstat field named {} in {id}",
                            stringify!($field),
                        ))?,
                    )*
                })
            }
        }
    }
}

kstat_named! {
    struct KstatCpuCore {
        cpu_ticks_idle: u64,
        cpu_ticks_kernel: u64,
        cpu_ticks_user: u64,
        cpu_ticks_wait: u64,
    }
}

kstat_named! {
    struct KstatCpuVm {
        anonpgin: u64,
        anonpgout: u64,
    }
}

kstat_named! {
    struct KstatSystemMisc {
        avenrun_1min: u64,
    }
}

kstat_named! {
    struct KstatSystemPages {
        physmem: u64,
        freemem: u64,
    }
}

trait ReadNamed: Sized {
    fn read(ctl: &Ctl, kstat: &mut Kstat<'_>) -> Result<Self, Error>;
}

trait FromNamedData: Sized {
    fn from_named_data(named_data: NamedData<'_>) -> Result<Self, Error>;
}

impl FromNamedData for u64 {
    fn from_named_data(named_data: NamedData<'_>) -> Result<Self, Error> {
        match named_data {
            NamedData::Char(_) => bail!("expected unsigned int, found Char"),
            NamedData::Int32(_) => bail!("expected unsigned int, found Int32"),
            NamedData::UInt32(inner) => Ok(inner.into()),
            NamedData::Int64(_) => bail!("expetced unsigned int, found Int64"),
            NamedData::UInt64(inner) => Ok(inner),
            NamedData::String(_) => {
                bail!("expected unsigned int, found String");
            }
        }
    }
}
