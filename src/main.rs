//! Extract every Loghub archive in the current directory into its **own** consolidated text file.
//!
//! * `Spark.tar.gz`  →  `Spark_logs.txt`
//! * `Android_v2.zip`  →  `Android_v2_logs.txt`
//!
//! All log entries remain *one per line*; sub-directories inside the archive are flattened.
//!

use std::{
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use tar::Archive;
use walkdir::WalkDir;
use zip::read::ZipArchive;

/// Append every regular file contained in a `.tar.gz` archive to `writer`.
/// Adds a single `\n` after each file so logs remain one-per-line.
fn stream_tar_gz(path: &Path, writer: &mut BufWriter<File>) -> Result<()> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);

    for entry in archive.entries()? {
        let entry = entry?;
        if entry.header().entry_type().is_file() {
            let reader = BufReader::new(entry);
            for line in reader.lines() {
                // Check if the line is valid UTF-8
                match line {
                    Ok(line) => {
                        // Write the line to the output file
                        writer.write_all(line.as_bytes())?;
                        writer.write_all(b"\n")?; // Ensure each log entry is on a new line
                    }
                    Err(e) => {
                        eprintln!("Skipping invalid UTF-8 line ({})", e);
                        continue;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Append every regular file contained in a `.zip` archive to `writer`.
fn stream_zip(path: &Path, writer: &mut BufWriter<File>) -> Result<()> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut archive = ZipArchive::new(file)?;

    for i in 0..archive.len() {
        let mut zf = archive.by_index(i)?;
        if zf.is_file() {
            io::copy(&mut zf, writer)?;
            writer.write_all(b"\n")?;
        }
    }
    Ok(())
}

/// Derive a stem suitable for naming the output file.
fn dataset_stem(p: &Path) -> String {
    let fname = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
    if fname.ends_with(".tar.gz") {
        fname[..fname.len() - 7].to_string() // strip .tar.gz
    } else if fname.ends_with(".zip") {
        fname[..fname.len() - 4].to_string() // strip .zip
    } else {
        fname.to_string()
    }
}

fn main() -> Result<()> {
    // Scan current directory (non-recursive) for archives.
    for entry in WalkDir::new(".").max_depth(1) {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        // Only process files with .tar.gz or .zip extensions
        if path.extension() != Some("gz".as_ref()) && path.extension() != Some("zip".as_ref()) {
            continue;
        }

        let out_stem = dataset_stem(path);
        let out_path = PathBuf::from(format!("{}_logs.txt", out_stem));
        let out_file =
            File::create(&out_path).with_context(|| format!("creating {}", out_path.display()))?;
        let mut writer = BufWriter::new(out_file);

        match path.extension().and_then(|ext| ext.to_str()) {
            Some("gz")
                if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .ends_with(".tar.gz") =>
            {
                eprintln!("→ {}  →  {}", path.display(), out_path.display());
                stream_tar_gz(path, &mut writer)?;
            }
            Some("zip") => {
                eprintln!("→ {}  →  {}", path.display(), out_path.display());
                stream_zip(path, &mut writer)?;
            }
            _ => continue,
        }

        writer.flush()?;
        eprintln!("✔ wrote {}", out_path.display());
    }

    eprintln!("All datasets processed.");
    Ok(())
}
