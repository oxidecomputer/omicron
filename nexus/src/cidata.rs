use fatfs::{FatType, FileSystem, FormatVolumeOptions, FsOptions};
use nexus_db_queries::db::{identity::Resource, model::Instance};
use num_integer::Integer;
use omicron_common::api::external::Error;
use serde::Serialize;
use std::io::{self, Cursor, Write};
use uuid::Uuid;

pub trait InstanceCiData {
    fn generate_cidata(&self, public_keys: &[String])
        -> Result<Vec<u8>, Error>;
}

impl InstanceCiData for Instance {
    fn generate_cidata(
        &self,
        public_keys: &[String],
    ) -> Result<Vec<u8>, Error> {
        // cloud-init meta-data is YAML, but YAML is a strict superset of JSON.
        let meta_data = serde_json::to_vec(&MetaData {
            instance_id: self.id(),
            local_hostname: &self.hostname,
            public_keys,
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

fn build_vfat(meta_data: &[u8], user_data: &[u8]) -> io::Result<Vec<u8>> {
    let file_sectors = Integer::div_ceil(&meta_data.len(), &512)
        + Integer::div_ceil(&user_data.len(), &512);
    // vfat can hold more data than this, but we don't expect to ever need that for cloud-init
    // purposes.
    if file_sectors > 512 {
        return Err(io::Error::new(io::ErrorKind::Other, "too much vfat data"));
    }

    // https://github.com/oxidecomputer/omicron/pull/911#discussion_r851354213
    // If we're storing < 170 KiB of clusters, the FAT overhead is 35 sectors;
    // if we're storing < 341 KiB of clusters, the overhead is 37. With a limit
    // of 512 sectors (error check above), we can assume an overhead of 37.
    // Additionally, fatfs refuses to format a disk that is smaller than 42
    // sectors.
    let sectors = 42.max(file_sectors + 37);
    // Some tools also require that the number of sectors is a multiple of the
    // sectors-per-track. fatfs uses a default of 32 which won't evenly divide
    // sectors as we compute above generally. To fix that we simply set it to
    // match the number of sectors to make it trivially true.
    let sectors_per_track = sectors.try_into().unwrap();

    let mut disk = Cursor::new(vec![0; sectors * 512]);
    fatfs::format_volume(
        &mut disk,
        FormatVolumeOptions::new()
            .bytes_per_cluster(512)
            .sectors_per_track(sectors_per_track)
            .fat_type(FatType::Fat12)
            .volume_label(*b"cidata     "),
    )?;

    {
        let fs = FileSystem::new(&mut disk, FsOptions::new())?;
        let root_dir = fs.root_dir();
        for (file, data) in [("meta-data", meta_data), ("user-data", user_data)]
        {
            // Cloud-init requires the files `meta-data` and `user-data`
            // to be present, even if empty.
            let mut file = root_dir.create_file(file)?;
            file.write_all(data)?;
        }
    }

    Ok(disk.into_inner())
}

#[cfg(test)]
mod tests {
    use nexus_types::external_api::params::MAX_USER_DATA_BYTES;

    /// the fatfs crate has some unfortunate panics if you ask it to do
    /// incredibly stupid things, like format an empty disk or create a
    /// filesystem with an invalid cluster size.
    ///
    /// to ensure that our math for the filesystem size is correct, and also to
    /// ensure that we don't ask fatfs to do incredibly stupid things, this
    /// test checks that `build_vfat` works on a representative sample of weird
    /// file sizes. (32 KiB is our enforced limit for user_data, so push it a
    /// little further.)
    #[test]
    fn build_vfat_works_with_arbitrarily_sized_input() {
        let upper = MAX_USER_DATA_BYTES + 4096;
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
