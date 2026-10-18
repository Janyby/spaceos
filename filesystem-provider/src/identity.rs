use std::path::Path;

/// Best-effort stable fingerprint for rename detection.
/// content-size + modified time + hash of first bytes.
pub fn fingerprint(path: &Path) -> Option<String> {
    let md = std::fs::metadata(path).ok()?;
    if md.is_dir() {
        return None;
    }
    let size = md.len();
    let mut head = Vec::new();
    if size > 0 {
        use std::io::Read;
        let mut f = std::fs::File::open(path).ok()?;
        let mut buf = vec![0u8; (size.min(4096)) as usize];
        // Breakage B4: `read` may return short reads (allowed by the Read
        // contract, observed under concurrent load), producing a DIFFERENT
        // fingerprint for IDENTICAL bytes. That phantom mismatch defeats
        // orphan matching and mints duplicate rows, which later starve the
        // original into a confirmed delete (+cascade). Read exactly.
        f.read_exact(&mut buf).ok()?;
        head.extend_from_slice(&buf);
    }
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    head.hash(&mut h);
    size.hash(&mut h);
    Some(format!("{:016x}:{}", h.finish(), size))
}
