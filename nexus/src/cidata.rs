use crate::db::{identity::Resource, model::Instance};
use fatfs::{FileSystem, FormatVolumeOptions, FsOptions};
use num_integer::Integer;
use omicron_common::api::external::Error;
use serde::Serialize;
use std::io::{Cursor, Write};
use uuid::Uuid;

pub const MAX_USER_DATA_BYTES: usize = 32 * 1024; // 32 KiB

impl Instance {
    pub fn generate_cidata(&self) -> Result<Vec<u8>, Error> {
        // cloud-init meta-data is YAML, but YAML is a strict superset of JSON.
        let meta_data = serde_json::to_vec(&MetaData {
            instance_id: self.id(),
            local_hostname: &self.runtime().hostname,
            public_keys: &[], // TODO
        })
        .map_err(|_| Error::internal_error("failed to serialize meta-data"))?;
        let cidata =
            build_vfat(&meta_data, &self.user_data).map_err(|err| {
                Error::internal_error(&format!(
                    "failed to create cidata volume: {}",
                    err
                ))
            })?;
        Ok(cidata)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct MetaData<'a> {
    instance_id: Uuid,
    local_hostname: &'a str,
    public_keys: &'a [String],
}

fn build_vfat(meta_data: &[u8], user_data: &[u8]) -> std::io::Result<Vec<u8>> {
    // requires #![feature(int_roundings)].
    // https://github.com/rust-lang/rust/issues/88581
    let file_sectors =
        meta_data.len().div_ceil(&512) + user_data.len().div_ceil(&512);
    // this was reverse engineered by making the numbers go lower until the
    // code failed (usually because of low disk space). this only works for
    // FAT12 filesystems
    let sectors = 42.max(file_sectors + 35 + ((file_sectors + 1) / 341 * 2));

    let mut disk = Cursor::new(vec![0; sectors * 512]);
    fatfs::format_volume(
        &mut disk,
        FormatVolumeOptions::new()
            .bytes_per_cluster(512)
            .volume_label(*b"cidata     "),
    )?;

    {
        let fs = FileSystem::new(&mut disk, FsOptions::new())?;
        let root_dir = fs.root_dir();
        for (file, data) in [("meta-data", meta_data), ("user-data", user_data)]
        {
            if !data.is_empty() {
                let mut file = root_dir.create_file(file)?;
                file.write_all(data)?;
            }
        }
    }

    Ok(disk.into_inner())
}

#[cfg(test)]
mod tests {
    /// the fatfs crate has some unfortunate panics if you ask it to do
    /// incredibly stupid things, like format a filesystem with an invalid
    /// cluster size.
    ///
    /// to ensure that our math for the filesystem size is correct, and also to
    /// ensure that we don't ask fatfs to do incredibly stupid things, this
    /// test checks that `build_vfat` works on a representative sample of weird
    /// file sizes. (32 KiB is our enforced limit for user_data, so push it a
    /// little further.)
    #[test]
    fn build_vfat_works_with_arbitrarily_sized_input() {
        let upper = crate::cidata::MAX_USER_DATA_BYTES + 4096;
        // somewhat arbitrarily-chosen prime numbers near 1 KiB and 256 bytes
        for md_size in (0..upper).step_by(1019) {
            for ud_size in (0..upper).step_by(269) {
                assert!(super::build_vfat(
                    &vec![0x5a; md_size],
                    &vec![0xa5; ud_size]
                )
                .is_ok());
            }
        }
    }
}
