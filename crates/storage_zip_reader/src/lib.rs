use std::{
    io,
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{
        ready,
        Context,
        Poll,
    },
    time::Duration,
};

use anyhow::Context as _;
use common::types::{
    FullyQualifiedObjectKey,
    ObjectKey,
};
use errors::ErrorMetadata;
pub use rc_zip;
use rc_zip::{
    fsm::{
        ArchiveFsm,
        EntryFsm,
        FsmResult,
    },
    parse::{
        Archive,
        Entry,
    },
};
use storage::{
    Storage,
    StorageExt as _,
};
use tempfile::NamedTempFile;
use tokio::io::{
    AsyncRead,
    AsyncReadExt,
    AsyncSeekExt,
    AsyncWriteExt,
    ReadBuf,
};
use tokio_util::io::StreamReader;

const MATERIALIZED_OBJECT_ATTRIBUTES_TIMEOUT: Duration = Duration::from_secs(120);

pub struct StorageZipArchive {
    source: ZipSource,
    archive: Archive,
}

enum ZipSource {
    Storage {
        storage: Arc<dyn Storage>,
        object_key: FullyQualifiedObjectKey,
    },
    LocalFile {
        temp_file: Arc<NamedTempFile>,
    },
}

impl Deref for StorageZipArchive {
    type Target = Archive;

    fn deref(&self) -> &Self::Target {
        &self.archive
    }
}

impl StorageZipArchive {
    /// Reads the central directory of the given zip file in object storage.
    pub async fn open(storage: Arc<dyn Storage>, object_key: &ObjectKey) -> anyhow::Result<Self> {
        let fq_key = storage.fully_qualified_key(object_key);
        Self::open_fq(storage, fq_key).await
    }

    pub async fn open_fq(
        storage: Arc<dyn Storage>,
        object_key: FullyQualifiedObjectKey,
    ) -> anyhow::Result<Self> {
        let attributes = storage
            .get_fq_object_attributes(&object_key)
            .await?
            .with_context(|| format!("Could not find object with key {object_key:?}"))?;
        let archive = read_archive_from_storage(&storage, &object_key, attributes.size).await?;
        Ok(Self {
            source: ZipSource::Storage {
                storage,
                object_key,
            },
            archive,
        })
    }

    /// Reads the object into a local temporary file before parsing the ZIP.
    ///
    /// This keeps S3 streams out of the row parsing path, where long pauses
    /// between reads can trigger SDK stalled-stream protection.
    pub async fn open_fq_materialized(
        storage: Arc<dyn Storage>,
        object_key: FullyQualifiedObjectKey,
    ) -> anyhow::Result<Self> {
        let attributes = tokio::time::timeout(
            MATERIALIZED_OBJECT_ATTRIBUTES_TIMEOUT,
            storage.get_fq_object_attributes(&object_key),
        )
        .await
        .with_context(|| format!("timed out reading attributes for object {object_key:?}"))??
        .with_context(|| format!("Could not find object with key {object_key:?}"))?;
        let temp_file = Arc::new(NamedTempFile::new().context("create temporary zip file")?);
        let mut local_file = tokio::fs::File::from_std(
            temp_file
                .reopen()
                .context("reopen temporary zip file for materialization")?,
        );
        let copied_bytes = storage
            .download_fq_object_to_file(&object_key, &mut local_file, attributes.size)
            .await
            .with_context(|| format!("download zip object {object_key:?}"))?;
        anyhow::ensure!(
            copied_bytes == attributes.size,
            "Downloaded {copied_bytes} bytes for {object_key:?}, expected {} bytes",
            attributes.size
        );
        local_file
            .flush()
            .await
            .context("flush temporary zip file")?;
        let archive = read_archive_from_file(local_file, attributes.size).await?;
        Ok(Self {
            source: ZipSource::LocalFile { temp_file },
            archive,
        })
    }

    /// Creates a reader for an entry in the archive.
    /// To get an `Entry`, use [`Archive::entries`] via `StorageZipArchive`'s
    /// `Deref` impl.
    pub fn read_entry(&self, entry: Entry) -> StorageZipEntryReader {
        let start = entry.header_offset;
        // The absolute max amount of data that could be read includes the local
        // file header, compressed data, and data descriptor. The local file
        // header is variable-size but could contain up to 2 64KiB fields (file
        // name & extra fields), and then we add 1KiB for the remaining
        // fixed-size stuff.
        const MAX_HEADER_SIZE: u64 = (1 << 16) * 2 + 1024;
        let end = self.archive.size().min(
            start
                .saturating_add(entry.compressed_size)
                .saturating_add(MAX_HEADER_SIZE),
        );
        let read_stream = match &self.source {
            ZipSource::Storage {
                storage,
                object_key,
            } => Box::pin(StreamReader::new(
                storage
                    .get_fq_object_exact_range(object_key, start..end)
                    .stream,
            )) as Pin<Box<dyn AsyncRead + Send>>,
            ZipSource::LocalFile { temp_file } => Box::pin(LazyLocalFileReader {
                temp_file: temp_file.clone(),
                unopened_range: Some(start..end),
                reader: None,
            }) as Pin<Box<dyn AsyncRead + Send>>,
        };
        StorageZipEntryReader {
            read_stream,
            entry_fsm: Some(EntryFsm::new(Some(entry), None)),
        }
    }
}

async fn read_archive_from_storage(
    storage: &Arc<dyn Storage>,
    object_key: &FullyQualifiedObjectKey,
    object_size: u64,
) -> anyhow::Result<Archive> {
    let mut fsm = ArchiveFsm::new(object_size);
    let mut read_position = u64::MAX; // arbitrary value that would never be used
    let mut read_stream: Option<StreamReader<_, _>> = None;
    let mut read_stream_end = 0;
    loop {
        if let Some(offset) = fsm.wants_read() {
            anyhow::ensure!(
                offset < object_size,
                ErrorMetadata::bad_request(
                    "InvalidZip",
                    format!("zip parser requested byte {offset} outside the archive"),
                )
            );
            if offset == read_position
                && let Some(reader) = &mut read_stream
            {
                // Continue reading
                anyhow::ensure!(!fsm.space().is_empty(), "wants read but no buffer?");
                let read_bytes = reader.read(fsm.space()).await?;
                if read_bytes == 0 {
                    anyhow::ensure!(
                        read_position >= read_stream_end,
                        "storage stream ended at byte {read_position} before requested end byte \
                         {read_stream_end}"
                    );
                    anyhow::ensure!(
                        read_position < object_size,
                        ErrorMetadata::bad_request(
                            "InvalidZip",
                            format!("zip archive ended unexpectedly at byte {read_position}"),
                        )
                    );
                    // The parser consumed the small range exactly and still
                    // needs sequential bytes. Open the next range instead of
                    // treating the range boundary as archive EOF.
                    read_stream = None;
                    continue;
                }
                fsm.fill(read_bytes);
                read_position += read_bytes as u64;
            } else {
                let (stream, end) = if read_position == offset {
                    // If we are continuing a sequential read, then assume
                    // that we're reading the central directory; read more
                    // data at once
                    (
                        storage
                            .get_fq_object_exact_range(object_key, offset..object_size)
                            .stream,
                        object_size,
                    )
                } else {
                    let read_len = fsm.space().len() as u64;
                    let end = object_size.min(offset.saturating_add(read_len));
                    (
                        storage
                            .get_small_range_with_retries(object_key, offset..end)
                            .await?
                            .stream,
                        end,
                    )
                };
                read_position = offset;
                read_stream_end = end;
                read_stream = Some(StreamReader::new(stream));
            }
        }
        match fsm
            .process()
            .context(ErrorMetadata::bad_request("InvalidZip", "invalid zip file"))?
        {
            FsmResult::Continue(next) => fsm = next,
            FsmResult::Done(archive) => return validate_archive(archive),
        }
    }
}

async fn read_archive_from_file(
    mut file: tokio::fs::File,
    object_size: u64,
) -> anyhow::Result<Archive> {
    let mut fsm = ArchiveFsm::new(object_size);
    let mut read_position = u64::MAX;
    loop {
        if let Some(offset) = fsm.wants_read() {
            if offset != read_position {
                file.seek(io::SeekFrom::Start(offset))
                    .await
                    .with_context(|| format!("seek temporary zip file to byte {offset}"))?;
                read_position = offset;
            }
            anyhow::ensure!(!fsm.space().is_empty(), "wants read but no buffer?");
            let read_bytes = file
                .read(fsm.space())
                .await
                .context("read temporary zip file")?;
            anyhow::ensure!(
                read_bytes != 0,
                ErrorMetadata::bad_request(
                    "InvalidZip",
                    format!("zip archive ended unexpectedly at byte {read_position}"),
                )
            );
            fsm.fill(read_bytes);
            read_position += read_bytes as u64;
        }
        match fsm
            .process()
            .context(ErrorMetadata::bad_request("InvalidZip", "invalid zip file"))?
        {
            FsmResult::Continue(next) => fsm = next,
            FsmResult::Done(archive) => return validate_archive(archive),
        }
    }
}

fn validate_archive(archive: Archive) -> anyhow::Result<Archive> {
    // Every entry needs at least the fixed 30-byte local file header before
    // its compressed payload. Validate this before constructing byte ranges.
    const MIN_LOCAL_FILE_HEADER_SIZE: u64 = 30;
    for entry in archive.entries() {
        let minimum_entry_end = entry
            .header_offset
            .checked_add(MIN_LOCAL_FILE_HEADER_SIZE)
            .and_then(|offset| offset.checked_add(entry.compressed_size));
        anyhow::ensure!(
            minimum_entry_end.is_some_and(|end| end <= archive.size()),
            ErrorMetadata::bad_request("InvalidZip", "zip entry points outside the archive")
        );
    }
    Ok(archive)
}

struct LazyLocalFileReader {
    // Keep the owner after opening the read handle. Otherwise dropping the
    // archive can unlink the path while this reader is still active.
    temp_file: Arc<NamedTempFile>,
    unopened_range: Option<std::ops::Range<u64>>,
    reader: Option<tokio::io::Take<tokio::fs::File>>,
}

impl AsyncRead for LazyLocalFileReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // A parsed import keeps one reader per table. Delay opening the file so
        // large snapshots do not hold one descriptor for every table at once.
        if let Some(bytes_range) = self.unopened_range.take() {
            match self.temp_file.reopen() {
                Ok(mut file) => {
                    use std::io::Seek as _;
                    if let Err(e) = file.seek(io::SeekFrom::Start(bytes_range.start)) {
                        return Poll::Ready(Err(e));
                    }
                    self.reader = Some(
                        tokio::fs::File::from_std(file).take(bytes_range.end - bytes_range.start),
                    );
                },
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
        let Some(reader) = self.reader.as_mut() else {
            return Poll::Ready(Err(io::Error::other("reader failed before reading")));
        };
        Pin::new(reader).poll_read(cx, buf)
    }
}

/// Reads the content of a single file in a zip archive in storage.
pub struct StorageZipEntryReader {
    read_stream: Pin<Box<dyn AsyncRead + Send>>,
    entry_fsm: Option<EntryFsm>,
}

impl AsyncRead for StorageZipEntryReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        loop {
            let Some(fsm) = &mut this.entry_fsm else {
                // we previously hit EOF or an error
                return Poll::Ready(Ok(()));
            };
            let mut read_stream_eof = false;
            if fsm.wants_read() {
                let mut read_buf = ReadBuf::new(fsm.space());
                ready!(this.read_stream.as_mut().poll_read(cx, &mut read_buf))?;
                let read_bytes = read_buf.filled().len();
                fsm.fill(read_bytes);
                if read_bytes == 0 {
                    read_stream_eof = true;
                }
            }
            if buf.remaining() == 0 {
                // Defensive check; this is mostly invalid but we should not
                // infinite loop here
                return Poll::Ready(Ok(()));
            }
            let fsm = this.entry_fsm.take().unwrap();
            // N.B.: use block_in_place because decompression is happening here
            match common::runtime::block_in_place(|| fsm.process(buf.initialize_unfilled())) {
                Ok(FsmResult::Continue((fsm, outcome))) => {
                    let fsm = this.entry_fsm.insert(fsm);
                    buf.advance(outcome.bytes_written);
                    if outcome.bytes_written == 0 && buf.remaining() > 0 {
                        if read_stream_eof {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "Hit EOF while reading zip entry",
                            )));
                        }
                        // This would otherwise signal EOF; try reading again instead.
                        if !fsm.wants_read() {
                            // guard against an infinite loop
                            return Poll::Ready(Err(io::Error::other(
                                "bug: EntryFsm wrote nothing but doesn't want read?",
                            )));
                        }
                        continue;
                    }
                    return Poll::Ready(Ok(()));
                },
                Ok(FsmResult::Done(_buffer)) => return Poll::Ready(Ok(())),
                // zip parse or decompression error
                Err(e) => return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, e))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write as _,
        sync::Arc,
    };

    use anyhow::Context as _;
    use async_zip::{
        tokio::write::ZipFileWriter,
        Compression,
        ZipEntryBuilder,
    };
    use common::types::ObjectKey;
    use runtime::prod::ProdRuntime;
    use storage::{
        LocalDirStorage,
        Storage,
    };
    use tempfile::NamedTempFile;
    use tokio::io::AsyncReadExt as _;

    use super::{
        read_archive_from_file,
        StorageZipArchive,
        ZipSource,
    };

    #[test]
    fn materialized_archive_reads_local_entries() -> anyhow::Result<()> {
        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let rt = ProdRuntime::new(&tokio_rt);
        let storage = LocalDirStorage::new(rt)?;
        let key: ObjectKey = "materialized-test".try_into()?;
        let source_path = storage.path().join("materialized-test.blob");
        tokio_rt.block_on(async {
            let file = tokio::fs::File::create(source_path).await?;
            let mut writer = ZipFileWriter::with_tokio(file);
            let entry = ZipEntryBuilder::new("entry.txt".into(), Compression::Deflate).build();
            writer.write_entry_whole(entry, b"materialized").await?;
            writer.close().await?;
            anyhow::Ok(())
        })?;
        let fq_key = storage.fully_qualified_key(&key);

        let archive = tokio_rt.block_on(StorageZipArchive::open_fq_materialized(
            Arc::new(storage),
            fq_key,
        ))?;
        let entry = archive
            .entries()
            .next()
            .cloned()
            .context("missing zip entry")?;
        let temp_path = match &archive.source {
            ZipSource::LocalFile { temp_file } => temp_file.path().to_owned(),
            ZipSource::Storage { .. } => anyhow::bail!("expected materialized archive"),
        };
        let mut entry_reader = archive.read_entry(entry.clone());
        drop(archive);
        assert!(temp_path.exists());
        let mut first_byte = [0];
        tokio_rt.block_on(entry_reader.read_exact(&mut first_byte))?;
        assert_eq!(&first_byte, b"m");
        assert!(temp_path.exists());
        let mut contents = String::new();
        tokio_rt.block_on(entry_reader.read_to_string(&mut contents))?;
        assert_eq!(entry.name, "entry.txt");
        assert_eq!(contents, "aterialized");
        assert!(temp_path.exists());
        drop(entry_reader);
        assert!(!temp_path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn materialized_archive_rejects_truncated_central_directory() -> anyhow::Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        let mut bytes = vec![0x50, 0x4b, 0x01, 0x02]; // Partial central directory header.
        bytes.extend_from_slice(&[
            0x50, 0x4b, 0x05, 0x06, // EOCD signature.
            0x00, 0x00, // Disk number.
            0x00, 0x00, // Central directory start disk.
            0x01, 0x00, // Records on this disk.
            0x01, 0x00, // Total records.
            0x04, 0x00, 0x00, 0x00, // Central directory size.
            0x00, 0x00, 0x00, 0x00, // Central directory offset.
            0x00, 0x00, // Comment length.
        ]);
        temp_file.write_all(&bytes)?;
        temp_file.flush()?;

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            read_archive_from_file(
                tokio::fs::File::from_std(temp_file.reopen()?),
                bytes.len() as u64,
            ),
        )
        .await
        .context("read_archive_from_file timed out")?;

        let err = match result {
            Ok(_) => anyhow::bail!("invalid zip should fail"),
            Err(err) => err,
        };
        assert!(
            format!("{err:#}").contains("zip archive ended unexpectedly"),
            "unexpected error: {err:#}",
        );
        Ok(())
    }

    #[tokio::test]
    async fn materialized_archive_rejects_out_of_bounds_entry_offset() -> anyhow::Result<()> {
        let temp_file = NamedTempFile::new()?;
        let file = tokio::fs::File::from_std(temp_file.reopen()?);
        let mut writer = ZipFileWriter::with_tokio(file);
        let entry = ZipEntryBuilder::new("entry.txt".into(), Compression::Stored).build();
        writer.write_entry_whole(entry, b"contents").await?;
        writer.close().await?;

        let mut bytes = std::fs::read(temp_file.path())?;
        let central_header = bytes
            .windows(4)
            .position(|window| window == b"PK\x01\x02")
            .context("missing central directory header")?;
        let invalid_offset = u32::try_from(bytes.len() - 1)?;
        bytes[central_header + 42..central_header + 46]
            .copy_from_slice(&invalid_offset.to_le_bytes());
        std::fs::write(temp_file.path(), &bytes)?;

        let result = read_archive_from_file(
            tokio::fs::File::from_std(temp_file.reopen()?),
            bytes.len() as u64,
        )
        .await;
        let err = match result {
            Ok(_) => anyhow::bail!("invalid zip should fail"),
            Err(err) => err,
        };
        assert!(
            format!("{err:#}").contains("zip entry points outside the archive"),
            "unexpected error: {err:#}",
        );
        Ok(())
    }

    #[test]
    fn storage_archive_rejects_out_of_bounds_central_directory_offset() -> anyhow::Result<()> {
        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let rt = ProdRuntime::new(&tokio_rt);
        let storage = LocalDirStorage::new(rt)?;
        let key: ObjectKey = "invalid-central-directory-offset".try_into()?;
        let source_path = storage.path().join("invalid-central-directory-offset.blob");
        tokio_rt.block_on(async {
            let file = tokio::fs::File::create(&source_path).await?;
            let mut writer = ZipFileWriter::with_tokio(file);
            let entry = ZipEntryBuilder::new("entry.txt".into(), Compression::Stored).build();
            writer.write_entry_whole(entry, b"contents").await?;
            writer.close().await?;
            anyhow::Ok(())
        })?;

        let mut bytes = std::fs::read(&source_path)?;
        let eocd = bytes
            .windows(4)
            .rposition(|window| window == b"PK\x05\x06")
            .context("missing end of central directory")?;
        bytes[eocd + 16..eocd + 20].copy_from_slice(&u32::MAX.to_le_bytes());
        std::fs::write(&source_path, bytes)?;

        let result = tokio_rt.block_on(StorageZipArchive::open(Arc::new(storage), &key));
        let err = match result {
            Ok(_) => anyhow::bail!("invalid zip should fail"),
            Err(err) => err,
        };
        assert!(
            format!("{err:#}").contains("outside the archive"),
            "unexpected error: {err:#}",
        );
        Ok(())
    }
}
