use std::{
    cmp,
    env,
    io::SeekFrom,
    pin::Pin,
    time::{
        Duration,
        SystemTime,
    },
};

use anyhow::Context;
use async_trait::async_trait;
use aws_config::retry::RetryConfig;
use aws_sdk_s3::{
    config::{
        IdentityCache,
        StalledStreamProtectionConfig,
    },
    error::ProvideErrorMetadata,
    operation::{
        create_multipart_upload::builders::CreateMultipartUploadFluentBuilder,
        get_object::GetObjectError,
        head_object::{
            HeadObjectError,
            HeadObjectOutput,
        },
        upload_part::builders::UploadPartFluentBuilder,
    },
    presigning::PresigningConfig,
    primitives::ByteStream,
    types::{
        ChecksumAlgorithm,
        CompletedMultipartUpload,
        CompletedPart,
        ServerSideEncryption,
    },
    Client,
};
use aws_utils::{
    are_checksums_disabled,
    is_range_prefetch_disabled,
    is_sse_disabled,
    must_s3_config_from_env,
    s3::S3Client,
};
use bytes::Bytes;
use common::{
    errors::report_error,
    knobs::{
        AWS_S3_MIN_IDENTITY_VALIDITY,
        STORAGE_MAX_INTERMEDIATE_PART_SIZE,
    },
    runtime::Runtime,
    types::{
        FullyQualifiedObjectKey,
        ObjectKey,
    },
};
use errors::ErrorMetadata;
use futures::{
    future::{
        self,
        BoxFuture,
        Either,
    },
    stream,
    Future,
    FutureExt,
    Stream,
    StreamExt,
    TryStreamExt,
};
use serde_json::{
    json,
    Value as JsonValue,
};
use storage::{
    BufferedUpload,
    ClientDrivenUploadPartToken,
    ClientDrivenUploadToken,
    InvalidGetRangeError,
    ObjectAttributes,
    ObjectListing,
    Storage,
    StorageCacheKey,
    StorageGetStream,
    StorageUseCase,
    Upload,
    UploadId,
    DOWNLOAD_CHUNK_SIZE,
    MAXIMUM_PARALLEL_UPLOADS,
};
use tokio::io::{
    AsyncSeekExt,
    AsyncWriteExt,
};

use crate::{
    metrics::sign_url_timer,
    types::{
        ObjectPart,
        PartNumber,
    },
    ByteStreamCompat,
};

pub const ACCESS_KEY_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
pub const ACCESS_KEY_MAX_BACKOFF: Duration = Duration::from_secs(5);

/// The following are not knobs because they are fixed by S3.
/// The part size we use starts at the min and doubles until the max,
/// which allows very large files but still supports fast uploads for small
/// files.
/// S3 minimum part size for multipart upload is 5MiB
const MIN_S3_INTERMEDIATE_PART_SIZE: usize = 5 * (1 << 20);
/// S3 maximum part size for multipart upload is 5GiB
const MAX_S3_INTERMEDIATE_PART_SIZE: usize = 5 * (1 << 30);
const SNAPSHOT_IMPORT_S3_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const SNAPSHOT_IMPORT_S3_READ_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const SNAPSHOT_IMPORT_S3_RANGE_RETRIES: usize = 5;

#[derive(Clone)]
pub struct S3Storage<RT: Runtime> {
    client: Client,
    bucket: String,

    // Prefix gets added as prefix to all keys.
    key_prefix: String,
    runtime: RT,
}

impl<RT: Runtime> std::fmt::Debug for S3Storage<RT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Storage")
            .field("bucket", &self.bucket)
            .field("key_prefix", &self.key_prefix)
            .finish()
    }
}

impl<RT: Runtime> S3Storage<RT> {
    pub fn new_from_client(
        client: S3Client,
        use_case: StorageUseCase,
        key_prefix: String,
        runtime: RT,
    ) -> anyhow::Result<Self> {
        let bucket = s3_bucket_name(&use_case)?;
        Ok(Self {
            client: client.0,
            bucket,
            key_prefix,
            runtime,
        })
    }

    pub async fn new_with_prefix(
        bucket: String,
        key_prefix: String,
        runtime: RT,
    ) -> anyhow::Result<Self> {
        let client = s3_client().await?;
        let storage = Self {
            client,
            bucket,
            key_prefix,
            runtime,
        };
        Ok(storage)
    }

    pub async fn for_use_case(
        use_case: StorageUseCase,
        key_prefix: String,
        runtime: RT,
    ) -> anyhow::Result<Self> {
        let bucket_name = s3_bucket_name(&use_case)?;
        S3Storage::new_with_prefix(bucket_name, key_prefix, runtime).await
    }

    /// Helper method to configure multipart upload builder with optional AWS
    /// headers for S3 compatibility with non-AWS services
    fn configure_multipart_upload_builder(
        &self,
        mut upload_builder: CreateMultipartUploadFluentBuilder,
    ) -> CreateMultipartUploadFluentBuilder {
        // Add server-side encryption if not disabled for S3 compatibility
        if !is_sse_disabled() {
            upload_builder = upload_builder.server_side_encryption(ServerSideEncryption::Aes256);
        }

        // Add checksum algorithm if not disabled for S3 compatibility
        if !are_checksums_disabled() {
            // Because we're using multipart uploads, we're really specifying the part
            // checksum algorithm here, so it needs to match what we use for
            // each part.
            upload_builder = upload_builder.checksum_algorithm(ChecksumAlgorithm::Crc32);
        }

        upload_builder
    }

    async fn start_upload_with_key(&self, key: ObjectKey) -> anyhow::Result<S3Upload<RT>> {
        let s3_key = S3Key(self.key_prefix.clone() + &key);
        let upload_builder = self
            .client
            .create_multipart_upload()
            .bucket(self.bucket.clone())
            .key(&s3_key.0);

        let upload_builder = self.configure_multipart_upload_builder(upload_builder);

        let output = upload_builder
            .send()
            .await
            .context("Failed to create multipart upload")?;
        let upload_id = output
            .upload_id()
            .ok_or_else(|| anyhow::anyhow!("Multipart upload is missing an upload_id."))?;
        let s3_upload = S3Upload::new(
            self.client.clone(),
            self.bucket.clone(),
            upload_id.to_string().into(),
            key,
            s3_key,
            self.runtime.clone(),
        )
        .await?;
        Ok(s3_upload)
    }

    async fn download_snapshot_import_range_to_file(
        &self,
        bucket: &str,
        s3_key: &str,
        key: &FullyQualifiedObjectKey,
        file: &mut tokio::fs::File,
        range_start: u64,
        range_end: u64,
        object_size: u64,
        expected_etag: &mut Option<String>,
    ) -> Result<(), SnapshotImportRangeAttemptError> {
        let mut request = self
            .client
            .get_object()
            .bucket(bucket)
            .key(s3_key)
            .range(format!("bytes={range_start}-{}", range_end - 1));
        if let Some(etag) = expected_etag.as_ref() {
            request = request.if_match(etag);
        }
        let output = tokio::time::timeout(
            SNAPSHOT_IMPORT_S3_REQUEST_TIMEOUT,
            request
                .customize()
                .config_override(
                    aws_sdk_s3::config::Builder::new()
                        .stalled_stream_protection(StalledStreamProtectionConfig::disabled()),
                )
                .send(),
        )
        .await
        .with_context(|| {
            format!("timed out requesting object range {range_start}..{range_end} for {key:?}")
        })
        .map_err(SnapshotImportRangeAttemptError::Retryable)?
        .map_err(|error| {
            classify_snapshot_import_request_error(key, range_start, range_end, error)
        })?;
        validate_snapshot_import_range_response(
            key,
            range_start,
            range_end,
            object_size,
            output.content_length(),
            output.content_range(),
        )
        .map_err(SnapshotImportRangeAttemptError::Fatal)?;
        validate_snapshot_import_etag(key, expected_etag, output.e_tag())
            .map_err(SnapshotImportRangeAttemptError::Fatal)?;
        write_snapshot_import_range_attempt(
            key,
            file,
            range_start,
            range_end,
            output.body.into_stream(),
            SNAPSHOT_IMPORT_S3_READ_IDLE_TIMEOUT,
        )
        .await?;
        Ok(())
    }
}

#[derive(Debug)]
enum SnapshotImportRangeAttemptError {
    // The SDK retries request failures internally. The bounded outer retry also
    // covers an exhausted SDK request and starts a new request after a body
    // transport failure.
    Retryable(anyhow::Error),
    Fatal(anyhow::Error),
}

impl SnapshotImportRangeAttemptError {
    #[cfg(test)]
    fn into_inner(self) -> anyhow::Error {
        match self {
            Self::Retryable(error) | Self::Fatal(error) => error,
        }
    }
}

fn classify_snapshot_import_request_error(
    key: &FullyQualifiedObjectKey,
    range_start: u64,
    range_end: u64,
    error: aws_sdk_s3::error::SdkError<GetObjectError>,
) -> SnapshotImportRangeAttemptError {
    let retryable = match &error {
        aws_sdk_s3::error::SdkError::TimeoutError(_)
        | aws_sdk_s3::error::SdkError::DispatchFailure(_)
        | aws_sdk_s3::error::SdkError::ResponseError(_) => true,
        aws_sdk_s3::error::SdkError::ServiceError(service_error) => {
            let status = service_error.raw().status().as_u16();
            status == 408 || status == 429 || status >= 500
        },
        aws_sdk_s3::error::SdkError::ConstructionFailure(_) => false,
        // New SDK error variants need an explicit retry decision before they
        // are safe to retry in a restore workflow.
        _ => false,
    };
    let error = anyhow::Error::new(error).context(format!(
        "download object range {range_start}..{range_end} for {key:?}"
    ));
    if retryable {
        SnapshotImportRangeAttemptError::Retryable(error)
    } else {
        SnapshotImportRangeAttemptError::Fatal(error)
    }
}

fn validate_snapshot_import_etag(
    key: &FullyQualifiedObjectKey,
    expected_etag: &mut Option<String>,
    response_etag: Option<&str>,
) -> anyhow::Result<()> {
    let response_etag = response_etag
        .with_context(|| format!("Missing ETag while materializing object {key:?}"))?;
    match expected_etag.as_deref() {
        Some(expected_etag) => {
            anyhow::ensure!(
                response_etag == expected_etag,
                "Object {key:?} changed while it was being materialized: range response ETag \
                 {response_etag:?} does not match {expected_etag:?}"
            );
        },
        None => *expected_etag = Some(response_etag.to_owned()),
    }
    Ok(())
}

fn validate_snapshot_import_range_response(
    key: &FullyQualifiedObjectKey,
    range_start: u64,
    range_end: u64,
    object_size: u64,
    content_length: Option<i64>,
    content_range: Option<&str>,
) -> anyhow::Result<()> {
    let expected_range_len = range_end - range_start;
    let content_length = content_length.with_context(|| {
        format!("Missing content length for object range {range_start}..{range_end} for {key:?}")
    })?;
    anyhow::ensure!(
        content_length >= 0 && content_length as u64 == expected_range_len,
        "Downloaded object range {range_start}..{range_end} for {key:?} has content length \
         {content_length}, expected {expected_range_len}"
    );
    let expected_content_range = format!("bytes {range_start}-{}/{object_size}", range_end - 1);
    let content_range = content_range.with_context(|| {
        format!("Missing content range for object range {range_start}..{range_end} for {key:?}")
    })?;
    let parsed_content_range =
        parse_snapshot_import_content_range(content_range).with_context(|| {
            format!(
                "Downloaded object range {range_start}..{range_end} for {key:?} has invalid \
                 content range {content_range:?}, expected {expected_content_range:?}"
            )
        })?;
    anyhow::ensure!(
        parsed_content_range == (range_start, range_end - 1, object_size),
        "Downloaded object range {range_start}..{range_end} for {key:?} has content range \
         {content_range:?}, expected {expected_content_range:?}"
    );
    Ok(())
}

fn parse_snapshot_import_content_range(content_range: &str) -> anyhow::Result<(u64, u64, u64)> {
    let parse_decimal = |value: &str| -> anyhow::Result<u64> {
        anyhow::ensure!(
            !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
            "expected an unsigned decimal integer"
        );
        value.parse().context("content range integer is too large")
    };
    let (range_unit, range) = content_range
        .split_once(' ')
        .context("content range is missing its range unit")?;
    anyhow::ensure!(
        range_unit.eq_ignore_ascii_case("bytes"),
        "content range unit is not bytes"
    );
    let (range, total) = range
        .split_once('/')
        .context("content range is missing its object size")?;
    let (start, end) = range
        .split_once('-')
        .context("content range is missing its byte interval")?;
    Ok((
        parse_decimal(start)?,
        parse_decimal(end)?,
        parse_decimal(total)?,
    ))
}

async fn write_snapshot_import_range_attempt<S, E>(
    key: &FullyQualifiedObjectKey,
    file: &mut tokio::fs::File,
    range_start: u64,
    range_end: u64,
    mut stream: S,
    read_idle_timeout: Duration,
) -> Result<(), SnapshotImportRangeAttemptError>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    // A failed attempt may already have written a prefix of this range.
    file.seek(SeekFrom::Start(range_start))
        .await
        .with_context(|| format!("seek materialized S3 object to byte {range_start}"))
        .map_err(SnapshotImportRangeAttemptError::Fatal)?;
    let expected_range_len = range_end - range_start;
    let mut range_bytes = 0;
    let mut read_idle_deadline = tokio::time::Instant::now() + read_idle_timeout;
    loop {
        // `timeout_at` can prefer an immediately-ready stream item even after
        // its deadline, so check explicitly before polling another empty frame.
        if tokio::time::Instant::now() >= read_idle_deadline {
            return Err(SnapshotImportRangeAttemptError::Retryable(anyhow::anyhow!(
                "timed out reading object range {range_start}..{range_end} for {key:?}"
            )));
        }
        let next_chunk = tokio::time::timeout_at(read_idle_deadline, stream.try_next())
            .await
            .with_context(|| {
                format!("timed out reading object range {range_start}..{range_end} for {key:?}")
            })
            .map_err(SnapshotImportRangeAttemptError::Retryable)?
            .with_context(|| format!("read object range {range_start}..{range_end} for {key:?}"))
            .map_err(SnapshotImportRangeAttemptError::Retryable)?;
        let Some(bytes) = next_chunk else {
            break;
        };
        if bytes.is_empty() {
            // Empty HTTP data frames are not byte progress. Yield so an
            // immediately-ready stream cannot starve the idle deadline.
            tokio::task::yield_now().await;
            continue;
        }
        range_bytes += bytes.len() as u64;
        if range_bytes > expected_range_len {
            return Err(SnapshotImportRangeAttemptError::Fatal(anyhow::anyhow!(
                "Downloaded too many bytes for object range {range_start}..{range_end} for \
                 {key:?}: {range_bytes}, expected {expected_range_len}"
            )));
        }
        file.write_all(&bytes)
            .await
            .context("write materialized S3 object range")
            .map_err(SnapshotImportRangeAttemptError::Fatal)?;
        read_idle_deadline = tokio::time::Instant::now() + read_idle_timeout;
    }
    if range_bytes != expected_range_len {
        return Err(SnapshotImportRangeAttemptError::Retryable(anyhow::anyhow!(
            "Downloaded {range_bytes} bytes for object range {range_start}..{range_end} for \
             {key:?}, expected {expected_range_len}"
        )));
    }
    Ok(())
}

async fn s3_client() -> Result<Client, anyhow::Error> {
    static S3_CLIENT: tokio::sync::OnceCell<Client> = tokio::sync::OnceCell::const_new();
    let client = S3_CLIENT
        .get_or_try_init(|| async {
            let config = must_s3_config_from_env()
                .await
                .context("AWS env variables are required when using S3 storage")?;
            let s3_config = config
                .identity_cache(
                    IdentityCache::lazy()
                        .buffer_time(*AWS_S3_MIN_IDENTITY_VALIDITY)
                        .build(),
                )
                .retry_config(RetryConfig::standard())
                .build();
            anyhow::Ok(Client::from_conf(s3_config))
        })
        .await?
        .clone();
    Ok(client)
}

struct ClientDrivenUpload {
    object_key: ObjectKey,
    upload_id: UploadId,
}

impl TryFrom<ClientDrivenUpload> for ClientDrivenUploadToken {
    type Error = anyhow::Error;

    fn try_from(value: ClientDrivenUpload) -> Result<Self, Self::Error> {
        let v = json!({
            "objectKey": value.object_key.to_string(),
            "uploadId": value.upload_id.to_string(),
        });
        Ok(ClientDrivenUploadToken(serde_json::to_string(&v)?))
    }
}

impl TryFrom<ClientDrivenUploadToken> for ClientDrivenUpload {
    type Error = anyhow::Error;

    fn try_from(value: ClientDrivenUploadToken) -> Result<Self, Self::Error> {
        let v: JsonValue = serde_json::from_str(&value.0)?;
        let object_key = v
            .get("objectKey")
            .context("missing objectKey")?
            .as_str()
            .context("objectKey should be str")?
            .try_into()?;
        let upload_id = v
            .get("uploadId")
            .context("missing uploadId")?
            .as_str()
            .context("uploadId should be str")?
            .to_string()
            .into();
        Ok(Self {
            object_key,
            upload_id,
        })
    }
}

#[async_trait]
impl<RT: Runtime> Storage for S3Storage<RT> {
    #[fastrace::trace]
    async fn start_upload(&self) -> anyhow::Result<Box<BufferedUpload>> {
        let key: ObjectKey = self.runtime.new_uuid_v4().to_string().try_into()?;
        let upload = BufferedUpload::new(
            self.start_upload_with_key(key).await?,
            MIN_S3_INTERMEDIATE_PART_SIZE,
            std::cmp::min(
                MAX_S3_INTERMEDIATE_PART_SIZE,
                *STORAGE_MAX_INTERMEDIATE_PART_SIZE,
            ),
        );
        Ok(Box::new(upload))
    }

    async fn start_client_driven_upload(&self) -> anyhow::Result<ClientDrivenUploadToken> {
        let key: ObjectKey = self.runtime.new_uuid_v4().to_string().try_into()?;
        let s3_key = S3Key(self.key_prefix.clone() + &key);
        let upload_builder = self
            .client
            .create_multipart_upload()
            .bucket(self.bucket.clone())
            .key(&s3_key.0);

        let upload_builder = self.configure_multipart_upload_builder(upload_builder);

        let output = upload_builder
            .send()
            .await
            .context("Failed to create multipart upload")?;
        let upload_id = output
            .upload_id()
            .ok_or_else(|| anyhow::anyhow!("Multipart upload is missing an upload_id."))?;
        ClientDrivenUpload {
            object_key: key,
            upload_id: upload_id.to_string().into(),
        }
        .try_into()
    }

    async fn upload_part(
        &self,
        token: ClientDrivenUploadToken,
        part_number: u16,
        part: Bytes,
    ) -> anyhow::Result<ClientDrivenUploadPartToken> {
        let ClientDrivenUpload {
            object_key,
            upload_id,
        } = token.try_into()?;
        let s3_key = S3Key(self.key_prefix.clone() + &object_key);
        PartNumber::try_from(part_number + 1)
            .map_err(|e| ErrorMetadata::bad_request("Invalid part number", e.to_string()))?;
        let mut s3_upload = S3Upload::new_client_driven(
            self.client.clone(),
            self.bucket.clone(),
            upload_id.to_string().into(),
            object_key,
            s3_key,
            self.runtime.clone(),
            vec![],
            part_number.try_into()?,
        )?;
        s3_upload.write(part).await?;
        let object_part = s3_upload
            .uploaded_parts
            .pop()
            .context("should have written part")?;
        object_part.try_into()
    }

    async fn finish_client_driven_upload(
        &self,
        token: ClientDrivenUploadToken,
        mut part_tokens: Vec<ClientDrivenUploadPartToken>,
    ) -> anyhow::Result<ObjectKey> {
        if part_tokens.is_empty() {
            // S3 doesn't like multi-part uploads with zero parts, so create
            // an empty part.
            part_tokens.push(self.upload_part(token.clone(), 1, Bytes::new()).await?);
        }
        let ClientDrivenUpload {
            object_key,
            upload_id,
        } = token.try_into()?;
        let s3_key = S3Key(self.key_prefix.clone() + &object_key);
        let uploaded_parts: Vec<_> = part_tokens
            .into_iter()
            .map(ObjectPart::try_from)
            .try_collect()?;
        let next_part_number = 1; // unused
        let s3_upload = Box::new(S3Upload::new_client_driven(
            self.client.clone(),
            self.bucket.clone(),
            upload_id.to_string().into(),
            object_key,
            s3_key,
            self.runtime.clone(),
            uploaded_parts,
            next_part_number.try_into()?,
        )?);
        s3_upload.complete().await
    }

    async fn signed_url(&self, key: ObjectKey, expires_in: Duration) -> anyhow::Result<String> {
        let timer = sign_url_timer();
        let s3_key = S3Key(self.key_prefix.clone() + &key);
        let presigning_config = PresigningConfig::builder().expires_in(expires_in).build()?;
        let presigned_request = self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(&s3_key.0)
            .presigned(presigning_config)
            .await?;
        timer.finish();
        Ok(presigned_request.uri().to_owned())
    }

    async fn presigned_upload_url(
        &self,
        expires_in: Duration,
    ) -> anyhow::Result<(ObjectKey, String)> {
        let key: ObjectKey = self.runtime.new_uuid_v4().to_string().try_into()?;
        let s3_key = S3Key(self.key_prefix.clone() + &key);
        let presigning_config = PresigningConfig::builder().expires_in(expires_in).build()?;
        // TODO(CX-4921): figure out how to add SSE/checksums here
        let presigned_request = self
            .client
            .put_object()
            .bucket(self.bucket.clone())
            .key(&s3_key.0)
            .presigned(presigning_config)
            .await?;
        Ok((key, presigned_request.uri().to_owned()))
    }

    fn cache_key(&self, key: &ObjectKey) -> StorageCacheKey {
        StorageCacheKey::new(self.key_prefix.clone() + key)
    }

    fn fully_qualified_key(&self, key: &ObjectKey) -> FullyQualifiedObjectKey {
        format!("{}/{}{}", self.bucket, self.key_prefix, &**key).into()
    }

    fn test_only_decompose_fully_qualified_key(
        &self,
        _key: FullyQualifiedObjectKey,
    ) -> anyhow::Result<ObjectKey> {
        unimplemented!();
    }

    fn get_small_range(
        &self,
        key: &FullyQualifiedObjectKey,
        bytes_range: std::ops::Range<u64>,
    ) -> BoxFuture<'static, anyhow::Result<StorageGetStream>> {
        if bytes_range.start >= bytes_range.end {
            return async {
                Ok(StorageGetStream {
                    content_length: 0,
                    stream: Box::pin(stream::empty()),
                })
            }
            .boxed();
        }
        self.get_small_range_internal(key, bytes_range)
            .map(|r| Ok(r?.context("No such key")?.0))
            .boxed()
    }

    fn get_small_range_and_total_size(
        &self,
        key: &FullyQualifiedObjectKey,
        bytes_range: std::ops::Range<u64>,
    ) -> Option<BoxFuture<'static, anyhow::Result<Option<(StorageGetStream, u64)>>>> {
        if is_range_prefetch_disabled() {
            return None;
        }
        Some(
            self.get_small_range_internal(key, bytes_range)
                .map(|r| {
                    let Some((stream, content_range)) = r? else {
                        return Ok(None);
                    };
                    // header looks like:
                    //   Content-Range: bytes 0-39/12345678
                    let total_size: u64 = try {
                        content_range
                            .as_deref()?
                            .strip_prefix("bytes ")?
                            .split_once('/')?
                            .1
                            .parse()
                            .ok()?
                    }
                    .with_context(|| {
                        format!(
                            "invalid Content-Range header: {content_range:?}. If your \
                             S3-compatible storage provider doesn't return this header, set \
                             AWS_S3_DISABLE_RANGE_PREFETCH=true"
                        )
                    })?;
                    Ok(Some((stream, total_size)))
                })
                .boxed(),
        )
    }

    async fn get_fq_object_attributes(
        &self,
        key: &FullyQualifiedObjectKey,
    ) -> anyhow::Result<Option<ObjectAttributes>> {
        let (bucket, s3_key) = key
            .as_str()
            .split_once('/')
            .with_context(|| format!("Invalid fully qualified S3 key {key:?}"))?;
        let result: Result<HeadObjectOutput, aws_sdk_s3::error::SdkError<HeadObjectError>> = self
            .client
            .head_object()
            .bucket(bucket)
            .key(s3_key)
            .send()
            .await;
        match result {
            Ok(head_attributes) => {
                let content_length = head_attributes
                    .content_length
                    .context("Object is missing size")?;
                let size =
                    u64::try_from(content_length).context("Object has invalid negative size")?;
                Ok(Some(ObjectAttributes { size }))
            },
            Err(aws_sdk_s3::error::SdkError::ServiceError(err)) => match err.err() {
                HeadObjectError::NotFound(_) => Ok(None),
                // Other service errors from S3
                _ => Err(err.into_err().into()),
            },
            // Unable to get a response from S3 (e.g. timeout error)
            Err(err) => Err(err.into()),
        }
    }

    async fn download_fq_object_to_file(
        &self,
        key: &FullyQualifiedObjectKey,
        file: &mut tokio::fs::File,
        expected_size: u64,
    ) -> anyhow::Result<u64> {
        let (bucket, s3_key) = key
            .as_str()
            .split_once('/')
            .with_context(|| format!("Invalid fully qualified S3 key {key:?}"))?;
        file.set_len(0)
            .await
            .context("truncate materialized S3 object")?;
        file.seek(SeekFrom::Start(0))
            .await
            .context("seek materialized S3 object to start")?;
        let mut copied_bytes = 0;
        let mut expected_etag = None;
        while copied_bytes < expected_size {
            let range_start = copied_bytes;
            let range_end = cmp::min(
                range_start.saturating_add(DOWNLOAD_CHUNK_SIZE),
                expected_size,
            );
            let mut retries_remaining = SNAPSHOT_IMPORT_S3_RANGE_RETRIES;
            loop {
                match self
                    .download_snapshot_import_range_to_file(
                        bucket,
                        s3_key,
                        key,
                        file,
                        range_start,
                        range_end,
                        expected_size,
                        &mut expected_etag,
                    )
                    .await
                {
                    Ok(()) => break,
                    Err(SnapshotImportRangeAttemptError::Retryable(e)) if retries_remaining > 0 => {
                        let mut toreport = e.context(format!(
                            "failed to materialize snapshot import range \
                             {range_start}..{range_end} for {key:?}. {retries_remaining} attempts \
                             remaining"
                        ));
                        report_error(&mut toreport).await;
                        retries_remaining -= 1;
                    },
                    Err(SnapshotImportRangeAttemptError::Retryable(e)) => {
                        return Err(e).with_context(|| {
                            format!(
                                "failed to materialize snapshot import range \
                                 {range_start}..{range_end} for {key:?} after \
                                 {SNAPSHOT_IMPORT_S3_RANGE_RETRIES} retries"
                            )
                        });
                    },
                    Err(SnapshotImportRangeAttemptError::Fatal(e)) => {
                        return Err(e).with_context(|| {
                            format!(
                                "failed to materialize snapshot import range \
                                 {range_start}..{range_end} for {key:?}"
                            )
                        });
                    },
                }
            }
            copied_bytes = range_end;
        }
        Ok(copied_bytes)
    }

    fn storage_type_proto(&self) -> pb::searchlight::StorageType {
        let prefix = self.key_prefix.clone();
        let bucket = self.bucket.clone();
        pb::searchlight::StorageType {
            storage_type: Some(pb::searchlight::storage_type::StorageType::S3(
                pb::searchlight::S3Storage { prefix, bucket },
            )),
        }
    }

    async fn delete_object(&self, key: &ObjectKey) -> anyhow::Result<()> {
        let s3_key = S3Key(self.key_prefix.clone() + key);
        self.client
            .delete_object()
            .bucket(self.bucket.clone())
            .key(&s3_key.0)
            .send()
            .await
            .context(format!("Failed to delete object {key:?}"))?;
        Ok(())
    }

    async fn put_object(&self, key: ObjectKey, bytes: Bytes) -> anyhow::Result<()> {
        let mut upload = self.start_upload_with_key(key).await?;
        upload.write(bytes).await?;
        let _ = Box::new(upload).complete().await?;
        Ok(())
    }

    async fn list_objects(&self, key_prefix: &str) -> anyhow::Result<Vec<ObjectListing>> {
        let s3_prefix = format!("{}{}", self.key_prefix, key_prefix);
        let mut paginator = self
            .client
            .list_objects_v2()
            .bucket(self.bucket.clone())
            .prefix(&s3_prefix)
            .into_paginator()
            .send();

        let mut objects = Vec::new();
        while let Some(page) = paginator.next().await {
            let page =
                page.context(format!("Failed to list objects with prefix {key_prefix:?}"))?;
            for object in page.contents.unwrap_or_default() {
                let Some(s3_key) = object.key.as_ref() else {
                    continue;
                };
                let relative_key = s3_key
                    .strip_prefix(&self.key_prefix)
                    .with_context(|| format!("S3 key {s3_key:?} missing storage prefix"))?;
                let key = ObjectKey::try_from(relative_key)?;
                let last_modified = *object
                    .last_modified()
                    .context("S3 object missing last_modified")?;
                let last_modified = SystemTime::try_from(last_modified)
                    .context("S3 last_modified isn't valid SystemTime")?;
                objects.push(ObjectListing { key, last_modified });
            }
        }
        Ok(objects)
    }
}

impl<RT: Runtime> S3Storage<RT> {
    /// Also returns the raw `Content-Range` response header, which carries
    /// the object's total size.
    fn get_small_range_internal(
        &self,
        key: &FullyQualifiedObjectKey,
        bytes_range: std::ops::Range<u64>,
    ) -> impl Future<Output = anyhow::Result<Option<(StorageGetStream, Option<String>)>>> + use<RT>
    {
        let get_object = self.client.get_object();
        let key = key.clone();
        async move {
            let (bucket, s3_key) = key
                .as_str()
                .split_once('/')
                .with_context(|| format!("Invalid fully qualified S3 key {key:?}"))?;
            anyhow::ensure!(bytes_range.start < bytes_range.end);
            let output = match get_object
                .bucket(bucket)
                .key(s3_key)
                .range(format!(
                    "bytes={}-{}",
                    bytes_range.start,
                    bytes_range.end - 1
                ))
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) if e.as_service_error().is_some_and(|e| e.is_no_such_key()) => {
                    return Ok(None)
                },
                Err(e) if e.as_service_error().and_then(|e| e.code()) == Some("InvalidRange") => {
                    anyhow::bail!(InvalidGetRangeError);
                },
                Err(e) => return Err(e.into()),
            };
            let content_range = output.content_range().map(String::from);
            Ok(Some((
                StorageGetStream {
                    content_length: output
                        .content_length()
                        .context("Missing content length for object")?,
                    stream: output.body.into_stream().boxed(),
                },
                content_range,
            )))
        }
    }
}

struct S3Key(String);

pub struct S3Upload<RT: Runtime> {
    client: Client,
    bucket: String,
    upload_id: UploadId,
    key: ObjectKey,
    s3_key: S3Key,
    uploaded_parts: Vec<ObjectPart>,
    next_part_number: PartNumber,
    /// Initialized to true - set to fault if cleanly completed or cleanly
    /// aborted explicitly. Aborting helps save space by cleaning out
    /// incomplete multipart uploads.
    needs_abort_on_drop: bool,
    runtime: RT,
}

impl<RT: Runtime> S3Upload<RT> {
    async fn new(
        client: Client,
        bucket: String,
        upload_id: UploadId,
        key: ObjectKey,
        s3_key: S3Key,
        runtime: RT,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client,
            bucket,
            upload_id,
            key,
            s3_key,
            uploaded_parts: vec![],
            next_part_number: 1.try_into()?,
            needs_abort_on_drop: true,
            runtime,
        })
    }

    fn new_client_driven(
        client: Client,
        bucket: String,
        upload_id: UploadId,
        key: ObjectKey,
        s3_key: S3Key,
        runtime: RT,
        uploaded_parts: Vec<ObjectPart>,
        next_part_number: PartNumber,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client,
            bucket,
            upload_id,
            key,
            s3_key,
            uploaded_parts,
            next_part_number,
            needs_abort_on_drop: false,
            runtime,
        })
    }

    fn next_part_number(&mut self) -> anyhow::Result<PartNumber> {
        let part_number = self.next_part_number;
        self.next_part_number = (Into::<u16>::into(self.next_part_number) + 1).try_into()?;
        Ok(part_number)
    }

    fn start_write(&mut self, data: Bytes) -> anyhow::Result<UploadPart> {
        let part_number = self.next_part_number()?;
        crate::metrics::log_aws_s3_part_upload_size_bytes(data.len());

        let mut builder = self
            .client
            .upload_part()
            .body(ByteStream::from(data))
            .bucket(self.bucket.clone())
            .key(&self.s3_key.0)
            .part_number(Into::<u16>::into(part_number) as i32)
            .upload_id(self.upload_id.to_string());

        // Add checksum algorithm if not disabled for S3 compatibility
        if !are_checksums_disabled() {
            builder = builder.checksum_algorithm(ChecksumAlgorithm::Crc32);
        }

        Ok(UploadPart {
            part_number,
            builder,
        })
    }
}

struct UploadPart {
    part_number: PartNumber,
    builder: UploadPartFluentBuilder,
}

impl UploadPart {
    async fn upload(self, size: u64) -> anyhow::Result<ObjectPart> {
        let output = self.builder.send().await?;
        ObjectPart::new(self.part_number, size, output)
    }
}

#[async_trait]
impl<RT: Runtime> Upload for S3Upload<RT> {
    #[fastrace::trace]
    async fn try_write_parallel<'a>(
        &'a mut self,
        receiver: &mut Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'a>>,
    ) -> anyhow::Result<()> {
        let mut uploaded_parts = receiver
            .map(|result| {
                let size = match &result {
                    Ok(buf) => buf.len() as u64,
                    Err(_) => 0,
                };
                match result.and_then(|buf| self.start_write(buf)) {
                    Ok(upload) => Either::Left(upload.upload(size)),
                    Err(e) => Either::Right(future::err(e)),
                }
            })
            .buffer_unordered(MAXIMUM_PARALLEL_UPLOADS)
            .try_collect::<Vec<_>>()
            .await?;
        self.uploaded_parts.append(&mut uploaded_parts);

        Ok(())
    }

    async fn write(&mut self, data: Bytes) -> anyhow::Result<()> {
        let size = data.len() as u64;
        let upload_part = self.start_write(data)?;
        let object_part = upload_part.upload(size).await?;
        self.uploaded_parts.push(object_part);
        Ok(())
    }

    async fn abort(mut self: Box<Self>) -> anyhow::Result<()> {
        self._abort().await?;
        self.needs_abort_on_drop = false;
        Ok(())
    }

    #[fastrace::trace]
    async fn complete(mut self: Box<Self>) -> anyhow::Result<ObjectKey> {
        let key = self.key.clone();
        let mut completed_parts = Vec::new();
        for part in &self.uploaded_parts {
            let mut builder = CompletedPart::builder()
                .part_number(Into::<u16>::into(part.part_number()) as i32)
                .e_tag(part.etag());

            if !are_checksums_disabled() {
                builder = builder.checksum_crc32(part.checksum());
            }

            let part = builder.build();
            completed_parts.push(part);
        }
        // parallel_writes will write out of order.
        completed_parts.sort_by_key(|part| part.part_number());
        let completed_multipart_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();
        self.client
            .complete_multipart_upload()
            .bucket(self.bucket.clone())
            .key(&self.s3_key.0)
            .upload_id(self.upload_id.to_string())
            .multipart_upload(completed_multipart_upload)
            .send()
            .await?;
        self.needs_abort_on_drop = false;
        Ok(key)
    }
}

impl<RT: Runtime> S3Upload<RT> {
    fn _abort(&mut self) -> impl Future<Output = anyhow::Result<()>> + use<RT> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let upload_id = self.upload_id.to_string();
        let s3_key = self.s3_key.0.clone();
        async move {
            client
                .abort_multipart_upload()
                .bucket(bucket)
                .upload_id(upload_id)
                .key(s3_key)
                .send()
                .await?;
            Ok(())
        }
    }
}

impl<RT: Runtime> Drop for S3Upload<RT> {
    fn drop(&mut self) {
        if self.needs_abort_on_drop {
            let fut = self._abort();
            self.runtime
                .spawn_background("abort_multipart_upload", async move {
                    if let Err(e) = fut.await {
                        // abort-multipart-upload is idempotent. It has the following properties.
                        //
                        // abort after a successful abort - succeeds
                        // abort after a successful complete - succeeds
                        // complete after a successful abort - fails with a descriptive error.
                        report_error(
                            &mut anyhow::anyhow!(e)
                                .context("Couldn't async abort multipart upload"),
                        )
                        .await;
                    }
                });
        }
    }
}

pub fn s3_bucket_name(use_case: &StorageUseCase) -> anyhow::Result<String> {
    let env_var_name = format!("S3_STORAGE_{}_BUCKET", use_case.to_string().to_uppercase());
    env::var(&env_var_name).context(format!(
        "{env_var_name} env variable is required when using S3 storage"
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        io::{
            self,
            SeekFrom,
        },
        time::Duration,
    };

    use bytes::Bytes;
    use futures::stream;
    use tempfile::NamedTempFile;
    use tokio::io::{
        AsyncReadExt,
        AsyncSeekExt,
        AsyncWriteExt,
    };

    use super::{
        validate_snapshot_import_etag,
        validate_snapshot_import_range_response,
        write_snapshot_import_range_attempt,
        SnapshotImportRangeAttemptError,
    };

    #[test]
    fn snapshot_import_range_response_requires_exact_content_range() {
        let key = "bucket/object".to_string().into();
        validate_snapshot_import_range_response(&key, 8, 12, 20, Some(4), Some("bytes 8-11/20"))
            .unwrap();
        validate_snapshot_import_range_response(
            &key,
            8,
            12,
            20,
            Some(4),
            Some("Bytes 008-011/020"),
        )
        .unwrap();

        for invalid_content_range in [
            "bytes 0-3/20",
            "bytes 8-12/20",
            "bytes 8-11/21",
            "bytes 8-11/*",
            "items 8-11/20",
        ] {
            let error = validate_snapshot_import_range_response(
                &key,
                8,
                12,
                20,
                Some(4),
                Some(invalid_content_range),
            )
            .unwrap_err();
            assert!(
                format!("{error:#}").contains("expected \"bytes 8-11/20\""),
                "unexpected error: {error:#}",
            );
        }
    }

    #[test]
    fn snapshot_import_ranges_require_one_object_version() {
        let key = "bucket/object".to_string().into();
        let mut expected_etag = None;
        validate_snapshot_import_etag(&key, &mut expected_etag, Some("etag-a")).unwrap();
        assert_eq!(expected_etag.as_deref(), Some("etag-a"));

        let error =
            validate_snapshot_import_etag(&key, &mut expected_etag, Some("etag-b")).unwrap_err();
        assert!(
            format!("{error:#}").contains("changed while it was being materialized"),
            "unexpected error: {error:#}",
        );
    }

    #[tokio::test]
    async fn snapshot_import_range_retry_overwrites_partial_attempt() -> anyhow::Result<()> {
        let temp_file = NamedTempFile::new()?;
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(temp_file.path())
            .await?;
        file.write_all(b"01......89").await?;
        let key = "bucket/object".to_string().into();

        let short_stream = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"bad"))]);
        let error = write_snapshot_import_range_attempt(
            &key,
            &mut file,
            2,
            8,
            short_stream,
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            SnapshotImportRangeAttemptError::Retryable(_)
        ));
        let error = error.into_inner();
        assert!(
            format!("{error:#}").contains("Downloaded 3 bytes"),
            "unexpected error: {error:#}",
        );

        let overread_stream = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"toolong"))]);
        let error = write_snapshot_import_range_attempt(
            &key,
            &mut file,
            2,
            8,
            overread_stream,
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, SnapshotImportRangeAttemptError::Fatal(_)));

        let complete_stream = stream::iter([
            Ok::<_, io::Error>(Bytes::from_static(b"ABC")),
            Ok::<_, io::Error>(Bytes::from_static(b"DEF")),
        ]);
        write_snapshot_import_range_attempt(
            &key,
            &mut file,
            2,
            8,
            complete_stream,
            Duration::from_secs(1),
        )
        .await
        .map_err(SnapshotImportRangeAttemptError::into_inner)?;
        file.flush().await?;
        file.seek(SeekFrom::Start(0)).await?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        assert_eq!(bytes, b"01ABCDEF89");
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_import_empty_chunks_do_not_reset_read_idle_timeout() -> anyhow::Result<()> {
        let temp_file = NamedTempFile::new()?;
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(temp_file.path())
            .await?;
        let key = "bucket/object".to_string().into();
        let empty_stream = stream::repeat_with(|| Ok::<_, io::Error>(Bytes::new()));

        let error = write_snapshot_import_range_attempt(
            &key,
            &mut file,
            0,
            1,
            empty_stream,
            Duration::from_millis(10),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            SnapshotImportRangeAttemptError::Retryable(_)
        ));
        let error = error.into_inner();
        assert!(
            format!("{error:#}").contains("timed out reading object range"),
            "unexpected error: {error:#}",
        );
        Ok(())
    }
}
