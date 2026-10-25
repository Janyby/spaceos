use crate::FormatInfo;
use std::path::Path;

/// Layered detection: extension -> MIME -> magic bytes -> semantic analysis.
pub fn detect(path: &Path) -> FormatInfo {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_lowercase())
        .unwrap_or_default();

    let (format, semantic, by, conf) = match ext.as_str() {
        "txt" => (
            "text/plain".into(),
            "Document".into(),
            "extension".into(),
            0.9,
        ),
        "md" | "markdown" => (
            "text/markdown".into(),
            "Document".into(),
            "extension".into(),
            0.95,
        ),
        "json" => (
            "application/json".into(),
            "Data".into(),
            "extension".into(),
            0.95,
        ),
        "csv" => ("text/csv".into(), "Data".into(), "extension".into(), 0.95),
        "rs" => (
            "text/x-rust".into(),
            "SourceCode".into(),
            "extension".into(),
            0.95,
        ),
        "py" => (
            "text/x-python".into(),
            "SourceCode".into(),
            "extension".into(),
            0.95,
        ),
        "png" => ("image/png".into(), "Image".into(), "extension".into(), 0.9),
        "jpg" | "jpeg" => ("image/jpeg".into(), "Image".into(), "extension".into(), 0.9),
        "pdf" => (
            "application/pdf".into(),
            "Document".into(),
            "extension".into(),
            0.9,
        ),
        "zip" => (
            "application/zip".into(),
            "Archive".into(),
            "extension".into(),
            0.9,
        ),
        _ => detect_magic(path),
    };

    // Boost/confirm with magic bytes when extension is unknown or unreliable.
    FormatInfo {
        physical_type: "File".into(),
        format,
        semantic_type: semantic,
        detected_by: by,
        confidence: conf,
    }
}

fn detect_magic(path: &Path) -> (String, String, String, f64) {
    use std::io::Read;
    let mut buf = [0u8; 8];
    let read = std::fs::File::open(path)
        .and_then(|mut f| f.read(&mut buf))
        .unwrap_or(0);
    if read >= 4 && &buf[0..4] == b"\x89PNG" {
        return ("image/png".into(), "Image".into(), "magic".into(), 0.99);
    }
    if read >= 3 && &buf[0..3] == [0xFF, 0xD8, 0xFF] {
        return ("image/jpeg".into(), "Image".into(), "magic".into(), 0.99);
    }
    if read >= 5 && &buf[0..5] == b"%PDF-" {
        return (
            "application/pdf".into(),
            "Document".into(),
            "magic".into(),
            0.99,
        );
    }
    if read >= 4 && &buf[0..4] == b"PK\x03\x04" {
        return (
            "application/zip".into(),
            "Archive".into(),
            "magic".into(),
            0.99,
        );
    }
    if read >= 4 && (&buf[0..4] == b"{\r\n" || &buf[0..4] == b"{\n\r" || buf[0] == b'{') {
        return (
            "application/json".into(),
            "Data".into(),
            "magic".into(),
            0.6,
        );
    }
    // Fallback: MIME guess then octet-stream.
    let mime = mime_guess::from_path(path)
        .first_raw()
        .unwrap_or("application/octet-stream")
        .to_string();
    (mime, "Unknown".into(), "mime".into(), 0.5)
}
