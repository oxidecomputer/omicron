/*!
 * Utilities for packaging and unpackaging OXCP services.
 */

use ring::digest::{Context, Digest, SHA256};
use std::io::{BufReader, Read};
use std::path::Path;

/// Calculates the SHA256 Digest of a file.
pub fn sha256_digest<P: AsRef<Path>>(
    path: P,
) -> Result<Digest, std::io::Error> {
    let file = std::fs::File::open(path.as_ref())?;
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
