/*!
 * Utilities for packaging and unpackaging control plane services.
 */

use ring::digest::{Context, Digest, SHA256};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

/// Calculates the SHA256 Digest of a file.
pub fn sha256_digest(file: &mut File) -> Result<Digest, std::io::Error> {
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::new(file);
    let mut context = Context::new(&SHA256);
    let mut buffer = [0; 1024];

    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        context.update(&buffer[..count]);
    }

    Ok(context.finish())
}
