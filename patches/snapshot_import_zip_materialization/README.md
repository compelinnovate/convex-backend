# Materialize Snapshot Import ZIPs

This patch changes snapshot import parsing so ZIP archives are downloaded into a
local temporary file before the ZIP central directory and entries are parsed. It
is intended for self-hosted deployments that store import/export objects in
DigitalOcean Spaces or another S3-compatible object store whose streaming
behavior is not identical to AWS S3 in the ways the upstream import path
assumes.

The import code treats a remote object as a seekable ZIP file while the storage
API exposes it as a sequence of HTTP range streams. ZIP parsing does many small
reads around the central directory and later opens entry streams lazily while
table parsing, validation, and database writes happen in between. That shape is
straightforward against a local file. It is fragile when each read is backed by
an SDK-managed HTTP stream with stalled-stream protection, provider-specific
timeouts, provider-specific range response behavior, and long pauses between
reads.

The patch keeps the provider-specific hardening inside the snapshot import path.
It does not change production file-storage byte streams. Snapshot import is an
operator workflow, so each parse can spend local disk and one sequential download
pass to finish transport verification and central-directory parsing before
database mutation begins. Entry parsing then reads only the local file.

## What Changes

For ZIP snapshot imports, `parse_import_file` now calls
`StorageZipArchive::open_fq_materialized` instead of opening the archive
directly from object storage. The materialized path:

- reads object attributes and records the expected object size;
- downloads the whole object to a `NamedTempFile`;
- verifies the copied byte count against the expected object size;
- parses the ZIP central directory from the local temporary file;
- retains the verified size for import egress accounting;
- later reads ZIP entries from the same local file.

After materialization succeeds, ZIP import execution does not read the source
object or its attributes again. In particular, egress accounting uses the size
already verified during parsing instead of issuing a second attributes request
after hidden tables and file-storage rows have been written. A provider failure
or source-object deletion after materialization therefore cannot fail the import
solely at that accounting step. This remains logical source-size accounting:
the separate confirmation materialization and bytes repeated by range retries
consume provider bandwidth but do not add another usage charge.

For S3-backed storage, `download_fq_object_to_file` downloads the object in
bounded ranges. Each range checks `Content-Length`, parses and verifies the
numeric start, end, and total from `Content-Range`, counts bytes read from the
stream, rejects short reads and overreads, and retries the
whole range from its start after request failures that exhaust the SDK's own
retries, request timeouts before response headers, response-body transport
errors, body-read idle timeouts, or early EOF. Decoded
response metadata that fails `Content-Length`, `Content-Range`, or ETag
validation, as well as overreads and local-file errors, fails immediately. The
first range records the object ETag, later ranges use `If-Match`, and every
response must report that same ETag so a same-size object replacement cannot
produce a mixed local file. The S3 SDK stalled-stream protection is disabled
only for this materialization path, and the code uses an explicit 120-second
read idle timeout instead. Only non-empty body bytes reset that timeout; empty
HTTP data frames cannot keep a stalled range alive. A separate 120-second
timeout bounds the wait for each ranged request to return response headers.

The materialized path also gives the initial object-attributes lookup a
120-second timeout. This timeout is local to snapshot ZIP materialization and
does not change ordinary storage attribute or byte-stream reads.

The local ZIP reader also rejects zero-byte reads while the ZIP FSM still wants
more input. Without that check, a truncated central directory can keep the FSM
waiting for bytes that will never arrive. After parsing the central directory,
both ZIP reader modes reject entries whose advertised local-header offset or
compressed payload lies outside the archive instead of allowing an invalid
range to reach the storage reader.

The main code paths are:

- `crates/application/src/snapshot_import/parse.rs`, where ZIP import parsing
  selects the materialized archive path;
- `crates/storage_zip_reader/src/lib.rs`, where `open_fq_materialized` downloads
  the object and parses the local ZIP file;
- `crates/aws_s3/src/storage.rs`, where the S3-backed materializer performs
  bounded range downloads with response-range and byte-count verification and
  retry.

## Provider Behavior

DigitalOcean Spaces exposed this issue: its S3-compatible behavior does not
fully match the AWS S3 behavior assumed by this import path. We have not
validated whether Hetzner, Vultr, GCS S3-compatible endpoints, Azure-backed
S3-compatible gateways, or other providers have the same behavior.

The import path is sensitive to differences that ordinary upload/download paths
may never notice:

- range responses must identify the requested offset and object size and
  deliver exactly the requested byte count;
- every range must identify the same object version by ETag;
- stream EOF must mean the requested range was fully delivered, not merely that
  the HTTP stream ended;
- SDK stalled-stream protection must not classify a valid but paused import
  stream as broken while the importer is doing CPU or database work;
- ZIP central directory reads must fail cleanly on truncation or short reads.

Materializing the object first converts those provider behaviors into one simple
contract: either the complete object is present locally with the expected size,
or the import fails before it starts parsing table data. After that point, ZIP
parsing is ordinary local-file seek/read logic.

Use this patch when snapshot imports come from DigitalOcean Spaces or another
S3-compatible provider with observed short-read, range-read, stalled-stream, or
idle-stream issues.

## Scope

This is not a broad replacement for Convex object storage reads. It leaves
normal production file download behavior, normal file upload behavior, multipart
upload behavior, and the general ranged reader unchanged outside snapshot import
materialization.

CSV and JSON snapshot imports remain streamed and are not materialized. Their
parsed-import state also retains the source stream's declared content length so
egress accounting does not require a second attributes lookup after writes.

`LocalDirStorage` now obtains object size with filesystem metadata instead of
reading the whole object into memory. Successful attribute results are
unchanged, while filesystem errors other than a missing path now propagate
instead of being reported as a missing object. Its object byte streams are
unchanged. Its materialized copy reads at most one byte beyond the size observed
from metadata, so a concurrently growing source fails the size check without
consuming unbounded temporary disk.

The ZIP parser selects materialization for every storage implementation. With
`LocalDirStorage`, that means copying the source ZIP to a separate temporary
file even though the source is already local. There is no provider-specific
runtime switch in this patch; the provider-specific behavior is confined to the
S3 implementation of the materialization method.

A provider quirk that affects snapshot import can also affect other object
storage paths. Hardening those paths is a separate patch. Snapshot import can
use local-file materialization because imports are explicit operator workflows.
User-facing file reads should not first copy an entire object to local disk.

The patch also does not implement resume across backend restarts while the ZIP
is being materialized. If the process restarts during the download, the next
attempt starts materialization from the beginning. Snapshot import checkpoints
still apply after parsing and row import begins; they are not a partial-object
download checkpoint.

Confirmation and execution materialize the ZIP in separate passes. For S3,
ETag pinning keeps each pass internally consistent, but the import record does
not persist an ETag across those passes. `LocalDirStorage` has no corresponding
object-version token: the size check detects growth and truncation, but an
in-place same-size rewrite during a copy is not detectable and can mix source
bytes. Application-created snapshot import keys are immutable. A caller that
supplies a fully qualified object key must keep that object unchanged from
import creation through finalization; replacing it between confirmation and
execution can make execution import different content from the content shown in
the confirmation summary.

## Operational Tradeoffs

The main cost is local temporary disk. The Convex backend host needs enough free
space for at least one full import ZIP plus normal runtime headroom. A snapshot
ZIP is compressed and can be much smaller than the restored database footprint,
because the database stores indexes, pages, metadata, and storage-engine
overhead that are not represented one-for-one in the ZIP. The temporary file is
created in the process's default temporary directory; on Unix, operators can
set `TMPDIR` before starting the backend to select that location. The file is
owned by `NamedTempFile`. Downloads and lazy entry reads use verified reopened
handles rather than opening its raw path. Lazy entry readers retain the owner,
so the file is removed after the archive and its last entry reader or
storage-file stream are dropped. This is process-lifetime cleanup, not crash
recovery: an abrupt process termination can leave a temporary file until the
host or container cleans its temporary directory.

The import also changes from lazy remote reads to an up-front sequential
download. Transport truncation and inconsistent range responses are detected
before table import starts, and later table parsing no longer depends on remote
stream behavior. Entry decompression and CRC failures are still detected lazily
when each entry is read. For very large ZIPs, the up-front download can make the
beginning of the import look slower even if the full import is more predictable.

The retry unit is one bounded range, not the entire ZIP. On a request failure
that exhausts the SDK's retries, timeout while waiting for response headers,
response-body transport error, body-read idle timeout, or early EOF, the failed
range is rewritten from its start so the local file remains contiguous and
byte-exact. Invalid response metadata, ETag changes, overreads, and local seek
or write failures are not retried. This is a small range retry loop, not a
general multi-part download manager.

## Failure Model

The patch prefers early, explicit failures over continuing with uncertain input.
It checks:

- the object exists and has an expected size;
- each downloaded range reports the expected `Content-Length`;
- each downloaded range reports the exact requested interval and object size in
  `Content-Range`;
- every downloaded range reports the same ETag, with later requests also using
  `If-Match`;
- each range delivers exactly the requested number of bytes;
- no range delivers more bytes than requested;
- the final copied byte count equals the object size;
- the local ZIP reader does not hit EOF while the ZIP FSM still needs bytes;
- central-directory entries do not point outside the downloaded archive.

These checks are important for restore workflows. A snapshot import should fail
before mutating deployment data when the object cannot be materialized exactly.
It should not partially parse an archive after a provider stream ended early.
Corruption discovered only while decompressing a ZIP entry remains subject to
the existing snapshot import checkpoint and failure behavior.

## When to Adopt

Adopt this patch when:

- the deployment is self-hosted;
- snapshot import ZIPs are stored in DigitalOcean Spaces or another
  S3-compatible provider;
- streaming ZIP imports fail or stall on provider range streams;
- the backend host has enough temporary disk for the largest expected import
  ZIP;
- operators want byte-exact ZIP materialization before database mutation begins.

Do not adopt it when the extra local copy is not justified because the storage
provider is known to work with the upstream streaming ZIP reader, or when import
archives are too large for the backend's temporary disk budget. Local-directory
storage also pays for the extra copy because this patch does not select the path
by provider.

For DigitalOcean Spaces specifically, this patch is a practical default. It
keeps the rest of Convex's storage layer close to upstream while making the
backup-restore path deterministic enough for production migration and recovery
workflows.
