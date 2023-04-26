use anyhow::Result;
use camino::Utf8PathBuf;
use camino_tempfile::NamedUtf8TempFile;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Write;
use tough::editor::RepositoryEditor;
use tough::schema::{Hashes, Target};

pub(crate) struct TargetWriter {
    file: NamedUtf8TempFile,
    targets_dir: Utf8PathBuf,
    name: String,
    length: u64,
    hasher: Sha256,
}

impl TargetWriter {
    pub(crate) fn new(
        targets_dir: impl Into<Utf8PathBuf>,
        name: impl Into<String>,
    ) -> Result<TargetWriter> {
        let targets_dir = targets_dir.into();
        Ok(TargetWriter {
            file: NamedUtf8TempFile::new_in(&targets_dir)?,
            targets_dir,
            name: name.into(),
            length: 0,
            hasher: Sha256::default(),
        })
    }

    pub(crate) fn finish(self, editor: &mut RepositoryEditor) -> Result<()> {
        let digest = self.hasher.finalize();
        self.file.persist(self.targets_dir.join(format!(
            "{}.{}",
            hex::encode(digest),
            self.name
        )))?;
        editor.add_target(
            self.name,
            Target {
                length: self.length,
                hashes: Hashes {
                    sha256: digest.to_vec().into(),
                    _extra: HashMap::new(),
                },
                custom: HashMap::new(),
                _extra: HashMap::new(),
            },
        )?;
        Ok(())
    }
}

impl Write for TargetWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.file.write(buf)?;
        self.length += u64::try_from(n).unwrap();
        self.hasher.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
