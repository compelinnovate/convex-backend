# Atomic Node Executor Source Packages

This patch fixes a local Node executor source-package cache race that can break
self-hosted Node actions after a backend restart, cold startup, or burst of
concurrent Node action execution. The failure usually appears as Node import
errors such as `Cannot find module ... /source/<key>/modules/...`, sometimes
near `ENOTEMPTY` cleanup errors, with a source cache directory that exists but
has only part of the expected package contents.

The broken state is inside the local Node executor cache. It is not caused by a
scheduled job storing a stale temp path, and lowering Node action concurrency
does not repair the cache invariant. Lower concurrency reduces the chance that
several cold requests hit the same missing package at once. A single cold miss
can still expose a mutable directory, and a later request can still import from
a directory that another cache path is deleting or replacing.

## Runtime Shape

In the self-hosted local path, Rust owns the local Node executor process. It
creates a fresh temp directory, writes `local.cjs`, starts Node with
`--tempdir`, and sends requests to the Node process through `/invoke`.

The Node process overrides `os.tmpdir()` to that Rust-owned temp directory. It
serves many `/invoke` requests during its lifetime. `execute` and `analyze`
acquire a package lease before importing user modules. The local resilience
patch also uses the same acquisition boundary through `/prepare` to materialize
a candidate's packages without importing an application module. Source
packages are materialized lazily under:

```text
<tempdir>/source/<source-package-cache-id>/
```

External dependencies are materialized under:

```text
<tempdir>/external_deps/<external-package-cache-id>/node_modules/
```

For source packages with external dependencies, the source package gets a
`node_modules` symlink to the matching external dependency package. Concurrent
Node actions in the same local Node process share the same in-memory cache maps
and the same tempdir package directories. A source cache ID is the hexadecimal
SHA-256 digest of its source key and checksum plus its external key and checksum
when present. An external cache ID is the digest of its key and checksum.
Storage keys can contain path separators and dot segments, so using this bounded
single path component prevents one key from aliasing or deleting another package
path.

## Failure Mode

The old dynamic package path treated cache directories as mutable hot-path
working directories. Cold requests could download the same source package more
than once, extract directly into the final cache directory, and clean other
dynamic package directories on cache miss.

That broke under concurrent local executor traffic. One request could receive a
source directory for import while another request was still extracting,
deleting, or replacing the same directory. Another request could fail an
external-deps download while the source-side download kept running and later
published a source tree without the retry's `node_modules` link. The visible
result was a final `source/<key>` directory with partial contents, followed by
dynamic imports from missing files under `source/<key>/modules/...`.

The cache contract has to be stricter: a final cache directory must either be a
complete package that can be reused, or absent. Request code must never import
from a directory that is still being downloaded, linked, validated, cleaned, or
replaced.

## Patch Behavior

This patch makes dynamic source and external dependency packages publish as
complete units.

- Source package identity now uses `sourcePackage.bundled_source.key`. The
  deprecated top-level `sourcePackage.key` mirror no longer controls the cache
  key or same-key single-flight boundary.
- Same-key source package downloads share one in-flight promise. Same-key
  external dependency downloads also share one in-flight promise.
- Dynamically downloaded packages retain their archive checksum. A concurrent or
  retained-cache request that supplies the same storage key with a different
  checksum fails instead of silently receiving bytes that do not match its
  descriptor. Published source records also retain the checksum of the external
  package they link to. Prebuilt Lambda packages do not retain original archive
  checksums and skip these checks.
- Each source archive's `externalDepsStorageKey` must match the requested
  external dependency package before the source is linked, published, or
  returned from cache. A same-source-key waiter with inconsistent dependency
  metadata fails instead of receiving the first caller's package. Cached
  metadata is checked before completeness validation, and neither failure path
  removes a package that another invocation may still be using.
- Source archives extract into private staging directories named under
  `source/.<cache-id>.<random>` created atomically with `mkdtemp()`.
- External dependency archives extract into private staging directories named
  `external_deps/.<cache-id>.<random>` created atomically with `mkdtemp()`.
- Source and external archive entry paths and package-specific prefixes are
  validated before extraction writes any files into staging.
- After validation, archive entries use AdmZip metadata, Node's asynchronous
  zlib decompression, and asynchronous filesystem writes. The executor owns the
  zlib callback boundary because AdmZip's asynchronous inflater does not handle
  zlib error events. Malformed compressed entries therefore return a fixed
  extraction error instead of reaching the process-global fatal handler. The
  executor retains ZIP CRC validation and yields between bounded CRC chunks.
  Entries settle sequentially to bound peak memory and ensure cleanup never
  races extraction work that continues after an earlier entry failure. Large
  external packages therefore leave the Node event loop available to the local
  executor `/health` watchdog while the package is materialized.
- A source package is published to `source/<cache-id>` only after the source
  archive has validated, external dependencies have settled, required
  `node_modules` has been linked, and the linked source tree has validated.
- If the external dependency side fails after the source side succeeds, the
  staged source directory is removed and no final source package is published.
- Source and external download, checksum, and extraction failures remove their
  private staging directories before the same-key single-flight entry clears.
- Lambda runtime initialization clears dynamic package roots and abandoned
  dependency-build staging left in `/tmp` by a prior runtime reset, when the
  filesystem can survive after the in-memory ownership maps have disappeared.
- Lambda cache-miss cleanup waits for every selected recursive removal to settle
  before it returns an error, so a later warm invocation cannot publish to a
  path that an earlier failed cleanup is still traversing.
- On a true in-memory cache miss, a stale final path from an earlier executor
  lifetime is removed before download starts. Publication is then a rename into
  an absent final path.
- Published paths include the complete package identity. Node retains ESM and
  CommonJS module objects after package files retire, so a retired storage key
  requested with changed source or external content receives a different module
  path. Reacquiring the same complete identity reuses the same path and remains
  compatible with Node's module cache.
- Execute and analyze entry-module URLs both include the environment-variable
  hash, so retained source paths do not make analysis reuse an entry module
  evaluated under an earlier environment. Analysis also has a separate fixed
  cache-key component so its user initialization does not seed the entry-module
  instance later used by execution.
- Failed or superseded in-flight work cannot later publish over a successful
  retry.
- Cached source reuse verifies `package.json`, `modules/`, every module file
  declared in `metadata.json` including `_deps` chunks and source maps, and
  required `node_modules`. If a published package is incomplete, the request
  fails without deleting or replacing the path because an earlier invocation may
  still be using it. File checks settle asynchronously and sequentially, so a
  package with many bundled files does not block `/health` or issue an unbounded
  batch of filesystem operations. A package that retires during validation is
  retried instead of being misreported as corrupt.
- Cached external dependency reuse verifies `node_modules` and likewise fails
  without deleting the published path, which may still be the target of source
  package symlinks.
- HTTP and local-file package downloads have a 120 second abort timeout that
  remains active until the package stream ends, closes, or errors.
- Downloads reject 90 MB or larger compressed archives before buffering them,
  and archive validation rejects 230 MB or larger declared extracted contents
  before AdmZip allocates entry buffers. These limits match the backend package
  creation contract and also apply when an endpoint returns bad bytes for a
  valid descriptor.
- Timeout aborts raised while consuming a response body are translated to the
  same sanitized timeout error as aborts raised before response headers arrive.
- Non-success HTTP responses are aborted before returning a sanitized status
  error, so callers do not leave response bodies unread.
- HTTP fetch errors report package fetch status or timeout without including the
  raw package URL, which may be a signed URL.
- Package downloads accept only HTTP, HTTPS, and local file URLs. Invalid or
  unsupported package URLs and provider-controlled HTTP status text are not
  included in errors, so those paths cannot disclose a signed package URL
  either.
- Unexpected filesystem, archive, and local-file read failures cross the package
  boundary with fixed error text rather than package paths, archive-controlled
  values, or signed query strings.
- Local execute and analyze requests acquire a source-package lease before
  module import and release the lease only after execution, analysis, and
  syscall disposal settle. Lookup and ownership acquisition verify the same map
  entry without an intervening asynchronous operation, so a zero-owner cache hit
  cannot retire while a new request is acquiring it.
- Candidate preparation validates the complete request shape, acquires and
  releases the same source-package lease, and carries no function path or
  arguments. It therefore exercises source and external package publication
  without importing or invoking application code. A preparation failure is a
  typed endpoint result and cannot make a candidate ready.
- The local cache targets at most eight dynamic source packages and 512 MiB of
  source-package data, plus at most eight dynamic external packages and 2 GiB of
  external-package data. Byte accounting uses successfully extracted
  uncompressed file bytes at publication time.
- Retirement selects the least-recently-used zero-owner source package. The
  source-package map entry and stack root are removed before filesystem
  deletion. External dependencies receive an owner before source linking begins
  and retire only after no retained or publishing source package references
  them.
- External count or byte pressure can retire an inactive owning source even when
  the source cache is within its own limits. Releasing that source owner makes
  the external package eligible for retirement; active and publishing sources
  can still keep either cache temporarily above a limit.
- A failed source publication releases its external-package owner and enforces
  the external cache bounds. Repeated source failures with successful parallel
  external downloads therefore cannot grow the external cache without bound.
- A per-key retirement promise prevents a new download and publication from
  racing deletion of the old final path. Failed source publication does not
  consume source-cache capacity or register a stack root. A successful external
  download may remain as an ordinary bounded external-cache entry.
- Stack-frame mapping ref-counts each retained source-package module root. It
  derives the enclosing `modules` root from each frame path and verifies that
  root with a map lookup, so lookup work does not grow with the number of
  retained packages. Query-string and file-URL variants normalize to the same
  root. Each execute and analyze invocation reinstalls the executor formatter
  without incrementing root ownership, so a user library cannot leave later
  cached-package invocations on a replaced process-global formatter. A formatter
  property that cannot be restored, or a replaced global `Error` constructor,
  marks the shared process invalid; the next invocation returns an
  exiting-process response and terminates that generation.
- Fatal process-event responses use async-local ownership to select the
  invocation that produced an unhandled rejection, uncaught exception, or
  `process.exit` call. Concurrent local requests therefore cannot redirect that
  response to whichever invocation started last. If the originating stream has
  already ended or closes while the fatal response is flushing, the contaminated
  process exits instead of writing to a closed stream and remaining selected.
  The private `process.exit` sentinel cannot recursively re-enter a fatal
  handler before that response flushes.
- Local requests retain their executor generation while they are in flight, and
  failure cleanup removes that generation only if it is still current. A late
  connection failure or exit response from an old request cannot remove a newer
  Node process and its separate tempdir.
- The shared source-package module retains cache-miss eviction for AWS Lambda,
  where one execution environment handles invocations sequentially and the
  bounded `/tmp` filesystem cannot retain every dynamic package indefinitely.
  Source cleanup releases matching external-package ownership before external
  cleanup removes zero-owner packages.
- Malformed prebuilt Lambda package metadata or incomplete prebuilt contents
  reject executor warmup before any invocation runs instead of silently omitting
  a required package. Metadata validation also rejects module paths that are not
  canonical relative POSIX paths, so prebuilt completeness checks and imports
  cannot escape the package's `modules/` directory.
- External dependency builds use a private temporary directory and npm cache per
  request. Concurrent local builds cannot delete or overwrite each other's npm
  install, archive, or cache state. The completed top-level `node_modules` must
  be the owned directory itself rather than a symlink, so archive traversal
  cannot leave the private build tree. The signed upload URL is no longer stored
  as the temporary package name.
- Dependency builds reject 230 MB or larger extracted trees before archive
  creation and stop archive output at 90 MB. These creation-side limits match
  the backend package-size contract and the package download boundary.
- `npm install` runs in an asynchronous child with a 450 second deadline instead
  of blocking the local Node event loop. Long dependency builds therefore keep
  `/health` and unrelated local invocations responsive to the resilience
  watchdog. The child receives request-local npm cache variables without
  mutating the executor's process-global environment. On Unix, a process-group
  supervisor reports ordinary npm completion, signals the remaining owned group
  with `SIGKILL`, and closes before `installDependencies` settles. Install
  timeout also signals the group and waits for the observed supervisor close
  before build staging is removed. Neither path receives a separate
  descendant-exit acknowledgement. If the Node generation dies, IPC closure
  makes the supervisor attempt the same group kill, but Rust receives no
  descendant-exit acknowledgement and can remove the generation tempdir
  concurrently with that best-effort cleanup.
- External archive uploads accept only HTTPS and local file URLs. HTTPS uploads
  reject redirects, have a 120 second abort deadline, reject non-success status,
  cancel the response body, and always close their archive stream. Build
  failures return fixed text without raw npm output, package names, paths, stack
  frames, or the signed upload URL.
- Extracted-size calculation completes before dependency archive creation so an
  oversized npm tree cannot fill local disk with an archive that must be
  rejected. Archive output is independently size-bounded. Archive hashing and
  file-size lookup settle together after archive creation. Every build that
  settles removes its private install, npm cache, and archive staging tree after
  success or failure. Abrupt generation death does not settle the build; the
  supervisor attempts to stop its Unix npm group while the independent
  generation tempdir owner removes abandoned staging without waiting for that
  attempt.

Complete cached directories are reused and are not replaced by the internal hot
path. A final directory unknown to the current in-memory cache is stale and is
removed before archive download; publication uses rename without another
removal. A final directory already registered in the current cache is never
deleted or replaced, even if validation finds corruption, because prior
invocations may still use it. External deletion or mutation of the tempdir is
unsupported and requires restarting the executor to obtain a fresh tempdir.

## Package lifetime

The local lease covers the supported invocation, analysis, or preparation
boundary. Preparation releases its lease after publication because it does not
evaluate application code. User syscalls are disposed when an invocation or
analysis boundary returns success, an error, or a timeout. The Node timeout
race cannot cancel the underlying JavaScript promise, and timed-out work and
un-awaited callbacks are not supported after the boundary. A future lazy import
is safe while its invocation owns the lease. Work that resumes after the
boundary cannot rely on historical package files remaining present.

The count and byte limits are process-local constants rather than deployment
labels or package-key configuration. When every package needed to reduce the
cache is active, the cache can temporarily exceed a limit. The next lease
release runs retirement again. A single package larger than its byte budget
remains available while active and can retire after release.

These filesystem and stack-root limits do not evict Node's ESM or CommonJS
module objects. Distinct evaluated package identities and their process-global
state can remain in JavaScript memory until the executor generation retires.
This patch bounds retained package files and registered stack roots; it does not
provide periodic healthy-process rotation or a general Node module-cache
eviction mechanism.

Retirement does not provide cross-process ownership. Each local Node executor
generation owns a private tempdir and private maps. Generation replacement
starts with an empty cache. The resilience patch terminates the old direct child
immediately when retiring a failed generation and retains its tempdir through
direct-child reaping.

The Unix npm supervisor is containment rather than an extension of Rust's
generation ownership. Ordinary npm completion and install timeout wait for the
supervisor to close before build cleanup. Abrupt Node-process death only closes
IPC and triggers a best-effort group kill; Rust does not receive an
acknowledgement and does not delay tempdir removal for npm descendants.

The retained AWS Lambda eviction is not a lease protocol. Timed-out or unawaited
user work can survive handler completion when `callbackWaitsForEmptyEventLoop`
is false. If that work resumes during a later cache miss, Lambda eviction can
remove a package it still references. This is an existing Lambda isolation
limitation and is outside this local cache patch.

Atomic package publication also does not make all Node executor state
request-local. Local invocations still share process-global environment and
package-relative `require` state in `runWithEnvironmentVariables()` and
`setupGlobals()`. The environment hash cache-busts each entry module, but
relative ESM chunks and CommonJS dependencies remain process-cached. Concurrent
actions with different environment variables or external dependency roots retain
that separate existing isolation risk.

Deleting or editing the executor tempdir outside the cache owner remains
unsupported and can break active imports.

## Metrics

The Node health response reports aggregate retained source/external count and
bytes, active source owners, registered stack roots, package
hit/publish/retire/failed-publication counters, and stack-format
invocation/frame/duration counters. A failed-publication event covers a package
download, validation, linking, or publication attempt that does not publish the
requested package. The local resilience patch exports those values as backend
metrics. Package keys, paths, module names, and function names are not metric
labels or lifecycle log fields.

When application-declared pinned local executor pools are enabled, each pool has
its own package cache and tempdir, and these metrics carry that pool's bounded
`pool_name`. Named generations rotate on complete source-package identity and
effective environment changes. Source archives also retain module pool
metadata, but package leases and atomic publication remain request scoped; the
pool lifecycle does not extend a filesystem lease.

## Tests

`source_package.test.ts`, `errors.test.ts`, and `build_deps.test.ts` add focused
Vitest coverage for the package, stack, and dependency-build contracts:

- Candidate preparation publishes the source and external packages without
  evaluating an application module, leaves no active package owner, and rejects
  extra request or package fields before downloading.
- Sixteen concurrent requests for the same source package share one source
  download and one external dependency download, publish one final directory,
  and reuse it in a second concurrent wave.
- A cold source download exposes only its private staging directory until the
  complete source package is published.
- Different source packages sharing the same external dependency share one
  external download. A later miss for different source and external keys does
  not remove an already published package or break its `node_modules` symlink.
- Requests with the same `bundled_source.key` and different deprecated wrapper
  keys reuse the same cached package and do not redownload.
- Concurrent and cached requests with one source storage key but different
  archive checksums fail without replacing the published source package.
- A same-source-key waiter with a different external dependency key is rejected
  after the shared download rather than receiving a package linked for the first
  waiter.
- A cached source request or a second source package that supplies an external
  dependency key with a different archive checksum fails without replacing the
  external package or publishing a mismatched source package.
- A cached source package without external dependencies is preserved when an
  inconsistent same-key request claims that external dependencies are required.
- Source and external dependency keys containing path separators and dot
  segments remain inside their own cache roots and cannot replace another key's
  published package.
- A failed external dependency download cannot let an abandoned delayed source
  download later publish over a successful retry, and both staging directories
  are removed before retry.
- A failed source download removes its staging directory, does not disclose the
  signed URL, and reuses an external dependency package that completed in
  parallel when the source request is retried.
- A response that returns successful headers and then stalls its body times out
  with sanitized text, removes staging, clears single-flight state, and permits
  a successful retry.
- A malformed package URL fails without exposing its signed query string and
  removes its private staging directory.
- An oversized HTTP response or ZIP-declared extracted size fails before the
  executor buffers or allocates the oversized package and removes staging.
- A checksum-valid archive with malformed compressed data or an invalid entry
  CRC returns a fixed extraction error, removes staging, and does not terminate
  the executor through the process-global fatal handler.
- A failed local-file package read has the same bounded timeout and sanitized
  error boundary and does not expose its path or query string.
- If a cached package is missing a declared bundled `_deps` chunk, source map,
  or `package.json`, the next request fails without deleting or redownloading
  the published package. The generated package marker matters because user
  modules are `.js` files that Node must import as ESM.
- If a cached external dependency package is missing `node_modules`, the next
  source request fails without deleting that external package or a source
  package that still links to it.
- Concurrent registrations of different module roots preserve source-map frame
  identities for errors created by both packages.
- Unregistering an evicted source-package root removes it from process-global
  stack-frame mapping.
- Active source-package files survive turnover beyond the configured count,
  released packages retire back to the count and byte bounds, paired external
  dependencies retire after their final source owner, and owner count returns to
  zero.
- Lease acquisition retries if a cache hit retires before the asynchronous
  caller resumes.
- Source publication reserves a cached external package before linking, and
  repeated failed source downloads keep successful external downloads within the
  external cache bounds.
- Retired source and external keys requested with changed checksums publish at
  different paths, so Node cannot return modules cached for the retired package
  identity.
- A registered root remains until its final owner unregisters it, invocation
  setup restores a user-replaced formatter without adding an owner, and a user
  module under a nested directory named `modules` still maps through the package
  root without scanning unrelated roots.
- Frozen errors, thrown Proxies, and user-controlled `__frameData` properties
  cannot break or spoof private action and analysis stack-frame capture. A
  replaced global `Error` constructor is also detected before invocation.
- Prebuild initialization removes dynamic source, external, and dependency-build
  roots left in Lambda `/tmp` by a prior runtime reset and waits for all
  selected removals before reporting a cleanup failure.
- Concurrent external dependency builds retain separate install/cache trees,
  reject a symlinked top-level `node_modules` and an oversized extracted tree,
  and leave the event loop responsive while npm runs. Ordinary completion and
  install timeout both signal an owned lifecycle descendant and observe
  supervisor close before cleanup; the regression fixtures verify that the
  descendant cannot perform its scheduled late write. Install failures, invalid
  upload URLs, non-success HTTP responses, and stalled uploads return fixed
  sanitized errors, remove staging, and enforce their configured deadlines.

## Adoption and rollback

The local package and stack lifecycle activates automatically with this patch
and has no deployment-specific configuration. The local resilience patch is
optional; when both patches are present, its watchdog exports the package and
stack aggregates from `/health`.

Rolling back the complete patch to upstream restores mutable hot-path cache
deletion and is unsafe unless an equivalent atomic publication and ownership
protocol replaces it. If bounded retirement itself needs correction, retain
atomic publication and same-key single-flight while disabling or reverting only
retirement in the replacement image. Restarting the Node executor is the safe
way to clear a corrupted cache or unsupported out-of-process tempdir mutation.

The source-package tests use local zip fixtures and a local HTTP package server,
so they assert both filesystem shape and request counts.

## Verification

The targeted test command for this parcel is:

```sh
npm exec vitest -- run src/build_deps.test.ts src/source_package.test.ts src/errors.test.ts
```

It passed with nine dependency-build tests, thirty source-package tests, and
nine stack-frame registration tests for the changed Node executor files.

A self-hosted cold-cache smoke run after backend restart also exercised the
runtime failure class: 256 concurrent Node actions in two waves, 512/512
successful responses, and no targeted backend log matches for
`Cannot find module`, `Incomplete source package`, or
`Incomplete external deps package`.
