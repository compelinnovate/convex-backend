import { CanonicalizedModulePath } from "./convex";
import * as fs from "node:fs";
import * as stream from "node:stream";
import os from "node:os";
import path from "node:path";
import AdmZip from "adm-zip";
import concat from "concat-stream";
import { z } from "zod";

import { createHash } from "node:crypto";
import { logDebug, logDurationMs } from "./log";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { inflateRaw } from "node:zlib";
import {
  registerPrepareStackTrace,
  unregisterPrepareStackTrace,
} from "./errors";

export type SourcePackage = {
  // Deprecated fields
  uri: string;
  key: string;
  sha256: string;

  bundled_source: Package;
  external_deps?: Package | null;
};

export type Package = {
  uri: string;
  key: string;
  sha256: string;
};

const moduleEnvironmentSchema = z.union([
  z.enum(["node", "isolate"]),
  z
    .string()
    .regex(/^node:pool:(?!default$)[a-z][a-z0-9_]{0,31}$/)
    .transform(() => "node" as const),
]);
type ModuleEnvironment = z.infer<typeof moduleEnvironmentSchema>;
const modulePathSchema = z.string().refine((modulePath) => {
  const normalizedModulePath = path.posix.normalize(modulePath);
  return (
    modulePath.length > 0 &&
    !modulePath.includes("\\") &&
    !path.posix.isAbsolute(modulePath) &&
    normalizedModulePath === modulePath &&
    normalizedModulePath !== ".." &&
    !normalizedModulePath.startsWith("../")
  );
});

type MetadataJson = {
  modulePaths: string[];
  moduleEnvironments: Map<string, ModuleEnvironment>;
  externalDepsStorageKey?: string;
};

const metadataJsonSchema = z.object({
  modulePaths: z.array(modulePathSchema),
  moduleEnvironments: z
    .array(z.tuple([modulePathSchema, moduleEnvironmentSchema]))
    .optional(),
  externalDepsStorageKey: z.string().optional(),
});

class PackageCacheError extends Error {}

function sanitizePackageError(
  error: unknown,
  fallbackMessage: string,
): PackageCacheError {
  try {
    if (error instanceof PackageCacheError) {
      return error;
    }
  } catch {
    // A thrown Proxy can reject the instanceof check.
  }
  return new PackageCacheError(fallbackMessage);
}

const PACKAGE_DOWNLOAD_TIMEOUT_MS = 120_000;
// Keep these consumer-side limits aligned with PackageSize::verify_size in
// crates/model/src/source_packages/types.rs. The checksum is known only after
// download, so enforce the limits before buffering or decompressing bad data.
const MAX_PACKAGE_ARCHIVE_BYTES = 90_000_000;
const MAX_PACKAGE_UNCOMPRESSED_BYTES = 230_000_000;
const MAX_RETAINED_SOURCE_PACKAGES = 8;
const MAX_RETAINED_SOURCE_PACKAGE_BYTES = 512 * 1024 * 1024;
const MAX_RETAINED_EXTERNAL_PACKAGES = 8;
const MAX_RETAINED_EXTERNAL_PACKAGE_BYTES = 2 * 1024 * 1024 * 1024;
const ZIP_FLAG_ENCRYPTED = 1;
const ZIP_METHOD_STORED = 0;
const ZIP_METHOD_DEFLATED = 8;
const CRC32_YIELD_BYTES = 4 * 1024 * 1024;
const CRC32_TABLE = new Uint32Array(256);
for (let value = 0; value < CRC32_TABLE.length; value += 1) {
  let crc = value;
  for (let bit = 0; bit < 8; bit += 1) {
    crc = (crc & 1) === 1 ? (crc >>> 1) ^ 0xedb88320 : crc >>> 1;
  }
  CRC32_TABLE[value] = crc >>> 0;
}
const RUNNING_IN_AWS_LAMBDA =
  process.env.AWS_LAMBDA_FUNCTION_NAME !== undefined;

let packageUseSequence = 0;
const packageEvents = {
  sourceHits: 0,
  sourcePublishes: 0,
  sourceRetirements: 0,
  sourceFailedPublications: 0,
  externalHits: 0,
  externalPublishes: 0,
  externalRetirements: 0,
  externalFailedPublications: 0,
};

async function download(uri: string): Promise<stream.Readable> {
  let url: URL;
  try {
    url = new URL(uri);
  } catch {
    // URL errors include the input, which may contain a signed query string.
    throw new PackageCacheError("Invalid package URL");
  }
  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    PACKAGE_DOWNLOAD_TIMEOUT_MS,
  );
  timeout.unref();

  let readable: stream.Readable;
  if (url.protocol === "file:") {
    try {
      readable = fs.createReadStream(fileURLToPath(url), {
        signal: controller.signal,
      });
    } catch {
      clearTimeout(timeout);
      throw new PackageCacheError("Failed to read local package");
    }
  } else if (url.protocol === "http:" || url.protocol === "https:") {
    let response: Response;
    try {
      response = await fetch(uri, { signal: controller.signal });
    } catch (error) {
      clearTimeout(timeout);
      if (
        controller.signal.aborted ||
        (error instanceof Error && error.name === "AbortError")
      ) {
        throw new PackageCacheError(
          `Timed out downloading package after ${PACKAGE_DOWNLOAD_TIMEOUT_MS}ms`,
        );
      }
      throw new PackageCacheError("Failed to fetch package");
    }
    if (!response.ok) {
      // Abort the response body before rejecting. Package endpoints can return
      // large or stalled error bodies, and callers will not consume them.
      controller.abort();
      clearTimeout(timeout);
      throw new PackageCacheError(
        `Failed to fetch package: HTTP ${response.status}`,
      );
    }
    if (response.body === null) {
      controller.abort();
      clearTimeout(timeout);
      throw new PackageCacheError(
        "Failed to fetch package: response body is empty",
      );
    }
    const contentLengthHeader = response.headers.get("content-length");
    if (contentLengthHeader !== null) {
      const contentLength = Number(contentLengthHeader);
      if (
        Number.isSafeInteger(contentLength) &&
        contentLength >= MAX_PACKAGE_ARCHIVE_BYTES
      ) {
        controller.abort();
        clearTimeout(timeout);
        throw new PackageCacheError("Package archive exceeds the size limit");
      }
    }

    try {
      // @ts-expect-error DOM and Node declare incompatible ReadableStream helpers.
      readable = stream.Readable.fromWeb(response.body);
    } catch {
      controller.abort();
      clearTimeout(timeout);
      throw new PackageCacheError("Failed to read package response");
    }
  } else {
    clearTimeout(timeout);
    throw new PackageCacheError("Invalid package URL");
  }

  // `fetch()` resolves after headers arrive, and file-stream failures are also
  // asynchronous. Keep one timeout and one sanitized error boundary active
  // until the selected stream has completely settled.
  return stream.Readable.from(
    (async function* () {
      let downloadedBytes = 0;
      try {
        for await (const chunk of readable) {
          downloadedBytes += Buffer.byteLength(chunk);
          if (downloadedBytes >= MAX_PACKAGE_ARCHIVE_BYTES) {
            controller.abort();
            throw new PackageCacheError(
              "Package archive exceeds the size limit",
            );
          }
          yield chunk;
        }
      } catch (error) {
        if (error instanceof PackageCacheError) {
          throw error;
        }
        if (controller.signal.aborted) {
          throw new PackageCacheError(
            `Timed out downloading package after ${PACKAGE_DOWNLOAD_TIMEOUT_MS}ms`,
          );
        }
        throw new PackageCacheError("Failed while downloading package");
      } finally {
        clearTimeout(timeout);
        readable.destroy();
      }
    })(),
  );
}

function parseMetadataFile(contents: string): MetadataJson {
  let metadataJson: z.infer<typeof metadataJsonSchema>;
  try {
    const parsed: unknown = JSON.parse(contents);
    metadataJson = metadataJsonSchema.parse(parsed);
  } catch {
    throw new PackageCacheError("Source package metadata is invalid");
  }

  metadataJson.modulePaths.sort();
  for (let index = 1; index < metadataJson.modulePaths.length; index += 1) {
    if (
      metadataJson.modulePaths[index - 1] === metadataJson.modulePaths[index]
    ) {
      throw new PackageCacheError(
        "Source package metadata contains duplicate module paths",
      );
    }
  }

  // Old versions didn't populate moduleEnvironments.
  if (metadataJson.moduleEnvironments === undefined) {
    metadataJson.moduleEnvironments = [];
    for (const modulePath of metadataJson.modulePaths) {
      if (modulePath.endsWith(".js")) {
        const environment = modulePath.startsWith("actions/")
          ? "node"
          : "isolate";
        metadataJson.moduleEnvironments.push([modulePath, environment]);
      }
    }
  }

  const moduleEnvironmentsMap = new Map<string, ModuleEnvironment>();
  for (const [modulePath, environment] of metadataJson.moduleEnvironments) {
    if (moduleEnvironmentsMap.has(modulePath)) {
      throw new PackageCacheError(
        "Source package metadata contains duplicate module environments",
      );
    }
    moduleEnvironmentsMap.set(modulePath, environment);
  }
  const modulePaths = new Set(metadataJson.modulePaths);
  for (const modulePath of metadataJson.modulePaths) {
    if (
      modulePath.endsWith(".js.map") &&
      !modulePaths.has(modulePath.slice(0, -".map".length))
    ) {
      throw new PackageCacheError(
        "Source package metadata contains a source map for a missing module",
      );
    }
    if (modulePath.endsWith(".js") && !moduleEnvironmentsMap.has(modulePath)) {
      throw new PackageCacheError(
        "Source package module is missing an environment",
      );
    }
  }
  for (const modulePath of moduleEnvironmentsMap.keys()) {
    if (!modulePath.endsWith(".js") || !modulePaths.has(modulePath)) {
      throw new PackageCacheError(
        "Source package metadata contains an environment for a missing module",
      );
    }
  }

  return {
    modulePaths: metadataJson.modulePaths,
    moduleEnvironments: moduleEnvironmentsMap,
    externalDepsStorageKey: metadataJson.externalDepsStorageKey,
  };
}

/// Downloads source package and external deps package, if necessary,
/// populating cache with result. Links external deps package into
/// local source package directory.
export async function maybeDownloadAndLinkPackages(
  sourcePackage: SourcePackage,
): Promise<LocalSourcePackage> {
  try {
    return await maybeDownloadAndLinkPackagesInner(sourcePackage);
  } catch (error) {
    throw sanitizePackageError(error, "Failed to prepare source package");
  }
}

async function maybeDownloadAndLinkPackagesInner(
  sourcePackage: SourcePackage,
): Promise<LocalSourcePackage> {
  const sourcePackageKey = sourcePackage.bundled_source.key;
  const requiresNodeModules =
    sourcePackage.external_deps !== null &&
    sourcePackage.external_deps !== undefined;
  for (;;) {
    const retirement = sourcePackageRetirements.get(sourcePackageKey);
    if (retirement !== undefined) {
      await retirement;
    }
    // If we've previously downloaded and cached this source package, we've already linked the necessary
    // external modules and so there is no more work left to do, so return.
    const local = availableSourcePackages.get(sourcePackageKey);
    if (local !== undefined) {
      validatePackageChecksum(
        local.archiveSha256,
        sourcePackage.bundled_source,
      );
      validateExternalDepsMatch(local, sourcePackage.external_deps);
      // Published packages are immutable and may still back lazy imports from
      // earlier invocations. Fail on corruption instead of deleting a live path.
      const complete = await localSourcePackageIsComplete(
        local,
        requiresNodeModules,
      );
      // Validation yields between filesystem operations. A zero-owner package
      // can retire in that interval, so retry instead of reporting normal
      // retirement as cache corruption.
      if (availableSourcePackages.get(sourcePackageKey) !== local) {
        continue;
      }
      if (!complete) {
        throw new PackageCacheError("Incomplete source package");
      }
      local.lastUsed = ++packageUseSequence;
      packageEvents.sourceHits += 1;
      return local;
    }

    const inFlight = sourcePackageDownloads.get(sourcePackageKey);
    if (inFlight !== undefined) {
      validatePackageChecksum(
        inFlight.archiveSha256,
        sourcePackage.bundled_source,
      );
      const result = await inFlight.promise;
      validateExternalDepsMatch(result, sourcePackage.external_deps);
      return result;
    }

    const download = {
      archiveSha256: sourcePackage.bundled_source.sha256,
      promise: downloadAndLinkPackages(sourcePackage),
    };
    sourcePackageDownloads.set(sourcePackageKey, download);
    try {
      return await download.promise;
    } finally {
      if (sourcePackageDownloads.get(sourcePackageKey) === download) {
        sourcePackageDownloads.delete(sourcePackageKey);
      }
    }
  }
}

async function downloadAndLinkPackages(
  sourcePackage: SourcePackage,
): Promise<LocalSourcePackage> {
  // Keep source startup first: in Lambda, its cleanup synchronously releases
  // external owners before an external cache miss can start external cleanup.
  const sourcePackagePromise = downloadSourcePackage(
    sourcePackage.bundled_source,
    sourcePackageCacheIdentity(sourcePackage),
  );
  const externalPackagePromise: Promise<ExternalDepsPackage | null> =
    sourcePackage.external_deps
      ? acquireExternalPackageForSource(sourcePackage.external_deps)
      : Promise.resolve(null);
  let stagedPackage: StagedLocalSourcePackage | null = null;
  let localPackage: LocalSourcePackage | null = null;
  let externalPackage: ExternalDepsPackage | null = null;
  let registeredStackRoot = false;
  try {
    const [sourcePackageResult, externalPackageResult] =
      await Promise.allSettled([sourcePackagePromise, externalPackagePromise]);
    if (externalPackageResult.status === "fulfilled") {
      externalPackage = externalPackageResult.value;
    }
    // Wait for both downloads so a successful external package can release its
    // source-publication owner even when the source side fails.
    if (sourcePackageResult.status === "rejected") {
      throw sourcePackageResult.reason;
    }
    stagedPackage = sourcePackageResult.value;
    if (externalPackageResult.status === "rejected") {
      throw externalPackageResult.reason;
    }

    // A new source staging directory cannot already contain node_modules. Link
    // its reserved external package before publishing the complete source tree.
    validateExternalDepsMatch(stagedPackage, sourcePackage.external_deps);
    stagedPackage.externalDepsArchiveSha256 =
      sourcePackage.external_deps?.sha256 ?? null;
    if (externalPackage) {
      logDebug("Linking external dependencies into source package");
      await fs.promises.symlink(
        `${externalPackage.dir}/node_modules`,
        `${stagedPackage.dir}/node_modules`,
        "dir",
      );
    }
    await validateLocalSourcePackage(stagedPackage, externalPackage !== null);
    localPackage = await publishStagedSourcePackage(stagedPackage);
    registerPrepareStackTrace(path.join(localPackage.dir, "modules"));
    registeredStackRoot = true;
    if (externalPackage !== null) {
      externalPackage.lastUsed = ++packageUseSequence;
    }
    availableSourcePackages.set(sourcePackage.bundled_source.key, localPackage);
    packageEvents.sourcePublishes += 1;

    return localPackage;
  } catch (error) {
    packageEvents.sourceFailedPublications += 1;
    if (externalPackage !== null) {
      if (externalPackage.sourceOwners <= 0) {
        throw new PackageCacheError(
          "External package source-owner count is invalid",
        );
      }
      externalPackage.sourceOwners -= 1;
      externalPackage.lastUsed = ++packageUseSequence;
    }
    if (registeredStackRoot && localPackage !== null) {
      unregisterPrepareStackTrace(path.join(localPackage.dir, "modules"));
    }
    const cleanupDirectories: string[] = [];
    if (stagedPackage !== null) {
      cleanupDirectories.push(stagedPackage.dir);
    }
    if (localPackage !== null) {
      cleanupDirectories.push(localPackage.dir);
    }
    // Ownership is already released, so cache enforcement must run even when
    // a failed publication directory cannot be removed.
    const [cleanupResult, boundsResult] = await Promise.allSettled([
      removePackageDirectories(
        cleanupDirectories,
        "Failed to clean failed source package publication",
      ),
      enforceLocalPackageCacheBounds(),
    ]);
    if (cleanupResult.status === "rejected") {
      throw cleanupResult.reason;
    }
    if (boundsResult.status === "rejected") {
      throw boundsResult.reason;
    }
    throw error;
  }
}

type StagedLocalSourcePackage = LocalSourcePackage & {
  finalDir: string;
};

// Downloads sourcePackage and unzips it into a private staging directory.
async function downloadSourcePackage(
  sourcePackage: Package,
  cacheIdentity: string,
): Promise<StagedLocalSourcePackage> {
  const start = performance.now();
  logDebug("Downloading source package...");

  // Do not clean other cached source packages here. The local executor serves
  // concurrent requests, and callers can still be importing or executing code
  // from a package after this function has returned it.
  // Create a staging directory before downloading. The completed
  // package is published to source/<cache-id> only after source validation and
  // external-deps linking, so same-key retries cannot observe an abandoned
  // source-side download that outlived a failed external-deps download.
  if (RUNNING_IN_AWS_LAMBDA) {
    await cleanupDynamicSourcePackages();
  }
  const { finalDir, stagingDir } = await createPackageDirectories(
    "source",
    cacheIdentity,
  );
  try {
    const sourcePackageStream = await download(sourcePackage.uri);
    const result = await processSourcePackageStream(
      stagingDir,
      sourcePackage,
      sourcePackageStream,
    );
    logDurationMs("sourceDownloadTime", start);

    return {
      ...result,
      finalDir,
    };
  } catch (error) {
    await fs.promises.rm(stagingDir, { recursive: true, force: true });
    throw error;
  }
}

async function publishStagedSourcePackage(
  stagedPackage: StagedLocalSourcePackage,
): Promise<LocalSourcePackage> {
  const { finalDir, ...localPackage } = stagedPackage;
  await fs.promises.rename(localPackage.dir, finalDir);
  return {
    ...localPackage,
    dir: finalDir,
  };
}

// Downloads externalPackage and unzips it into its external dependency cache directory.
async function maybeDownloadExternalPackage(
  externalPackage: Package,
): Promise<ExternalDepsPackage> {
  const start = performance.now();
  for (;;) {
    const retirement = externalPackageRetirements.get(externalPackage.key);
    if (retirement !== undefined) {
      await retirement;
    }
    const externalDeps = availableExternalPackages.get(externalPackage.key);

    if (externalDeps !== undefined) {
      validatePackageChecksum(externalDeps.archiveSha256, externalPackage);
      // Source packages retain symlinks to published external packages. Replacing
      // this path would break lazy imports from every source package using it.
      const complete = await localExternalPackageIsComplete(externalDeps);
      // A zero-owner package can retire while the asynchronous stat settles.
      if (availableExternalPackages.get(externalPackage.key) !== externalDeps) {
        continue;
      }
      if (!complete) {
        throw new PackageCacheError("Incomplete external deps package");
      }
      externalDeps.lastUsed = ++packageUseSequence;
      packageEvents.externalHits += 1;
      logDebug("External Package available locally");
      return externalDeps;
    }

    const inFlight = externalPackageDownloads.get(externalPackage.key);
    if (inFlight !== undefined) {
      validatePackageChecksum(inFlight.archiveSha256, externalPackage);
      return await inFlight.promise;
    }

    logDebug("External Package not available locally");
    const download = {
      archiveSha256: externalPackage.sha256,
      promise: downloadExternalPackage(externalPackage, start),
    };
    externalPackageDownloads.set(externalPackage.key, download);
    try {
      return await download.promise;
    } catch (error) {
      packageEvents.externalFailedPublications += 1;
      throw error;
    } finally {
      if (externalPackageDownloads.get(externalPackage.key) === download) {
        externalPackageDownloads.delete(externalPackage.key);
      }
    }
  }
}

async function acquireExternalPackageForSource(
  externalPackage: Package,
): Promise<ExternalDepsPackage> {
  for (;;) {
    const localPackage = await maybeDownloadExternalPackage(externalPackage);
    // Lookup and ownership acquisition are separated by an await. Retirement
    // can win that interval, so retry unless this exact package is still current.
    if (availableExternalPackages.get(externalPackage.key) !== localPackage) {
      continue;
    }
    localPackage.sourceOwners += 1;
    localPackage.lastUsed = ++packageUseSequence;
    return localPackage;
  }
}

async function downloadExternalPackage(
  externalPackage: Package,
  start: number,
): Promise<ExternalDepsPackage> {
  // Do not clean other cached external packages here. Source packages symlink
  // into these directories, and active executions may still need them.
  // Create a staging directory before downloading so every failure path can
  // remove it without abandoning a response stream.
  const downloadStart = performance.now();
  if (RUNNING_IN_AWS_LAMBDA) {
    await cleanupDynamicExternalPackages();
  }
  const { finalDir: dir, stagingDir } = await createPackageDirectories(
    "external_deps",
    packageCacheIdentity(externalPackage),
  );
  try {
    const externalPackageStream = await download(externalPackage.uri);
    logDurationMs("downloadExternalsTime", downloadStart);

    // Process the external package download readable stream by checking hash and writing to dir
    const retainedBytes = await processExternalPackageStream(
      stagingDir,
      externalPackage,
      externalPackageStream,
    );
    await validateLocalExternalPackage({ dir: stagingDir });
    await fs.promises.rename(stagingDir, dir);
    const result: ExternalDepsPackage = {
      dir,
      archiveSha256: externalPackage.sha256,
      dynamicallyDownloaded: true,
      retainedBytes,
      sourceOwners: 0,
      lastUsed: ++packageUseSequence,
    };
    availableExternalPackages.set(externalPackage.key, result);
    packageEvents.externalPublishes += 1;
    logDurationMs("externalDepsProcessingTime", start);
    return result;
  } catch (error) {
    await fs.promises.rm(stagingDir, { recursive: true, force: true });
    throw error;
  }
}

function packageCacheIdentity(packageDescriptor: Package): string {
  return JSON.stringify([packageDescriptor.key, packageDescriptor.sha256]);
}

function sourcePackageCacheIdentity(sourcePackage: SourcePackage): string {
  return JSON.stringify([
    sourcePackage.bundled_source.key,
    sourcePackage.bundled_source.sha256,
    sourcePackage.external_deps?.key ?? null,
    sourcePackage.external_deps?.sha256 ?? null,
  ]);
}

async function createPackageDirectories(
  cacheName: "source" | "external_deps",
  cacheIdentity: string,
) {
  const cacheRoot = path.join(os.tmpdir(), cacheName);
  await fs.promises.mkdir(cacheRoot, { recursive: true, mode: 0o744 });

  // The identity includes checksums and, for source, external dependencies.
  // Hash it into one bounded component so changed content cannot reuse a URL
  // that Node's ESM or CommonJS cache still associates with retired modules.
  const cacheKey = createHash("sha256").update(cacheIdentity).digest("hex");
  const finalDir = path.join(cacheRoot, cacheKey);
  // On a true map miss, no request in this process has received this final path.
  // Remove a stale directory left by an earlier executor lifetime before staging.
  await fs.promises.rm(finalDir, { recursive: true, force: true });
  const stagingDir = await fs.promises.mkdtemp(
    path.join(cacheRoot, `.${cacheKey}.`),
  );
  return {
    finalDir,
    stagingDir,
  };
}

async function removePackageDirectories(
  directories: string[],
  failureMessage: string,
): Promise<void> {
  const results = await Promise.allSettled(
    directories.map((directory) =>
      fs.promises.rm(directory, { recursive: true, force: true }),
    ),
  );
  if (results.some((result) => result.status === "rejected")) {
    throw new PackageCacheError(failureMessage);
  }
}

async function streamToBuffer(
  readableStream: stream.Readable,
): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    // Use concat-stream to collect the stream data into a single buffer
    const concatStream = concat((data) => {
      resolve(data);
    });

    // Handle the stream with pipeline
    stream.promises.pipeline(readableStream, concatStream).catch(reject);
  });
}

async function processPackageStream(
  sha256Digest: string,
  packageStream: stream.Readable,
): Promise<Buffer> {
  // Create hashing stream
  const hash = createHash("sha256");
  packageStream.on("data", (chunk) => hash.update(chunk));
  const hashDone = new Promise((resolve, reject) => {
    packageStream
      .on("end", () => {
        resolve(null);
      })
      .on("error", (err) => {
        reject(err);
      });
  });

  const bufWriteDone = streamToBuffer(packageStream);

  // Make sure that all promises have been populated and the hash is done calculating
  const [buf, _] = await Promise.all([bufWriteDone, hashDone]);

  // Assert checksum matches
  const digest = hash.digest().toString("base64url");
  if (digest !== sha256Digest) {
    throw new PackageCacheError(
      "Package checksum does not match downloaded package",
    );
  }

  return buf;
}

async function unzipFile(
  zipBuffer: Buffer,
  outputDir: string,
  entryValidator: (entry: AdmZip.IZipEntry) => void,
) {
  let zipEntries: AdmZip.IZipEntry[];
  try {
    zipEntries = new AdmZip(zipBuffer).getEntries();
  } catch {
    throw new PackageCacheError("Package archive is invalid");
  }

  const results: string[] = [];
  const entryNames = new Set<string>();
  let declaredUncompressedBytes = 0;
  let uncompressedBytes = 0;
  for (const zipEntry of zipEntries) {
    // Validate every path before extraction writes any archive contents.
    const normalizedEntryName = path.posix.normalize(zipEntry.entryName);
    if (
      zipEntry.entryName.length === 0 ||
      zipEntry.entryName.includes("\\") ||
      path.posix.isAbsolute(zipEntry.entryName) ||
      normalizedEntryName !== zipEntry.entryName ||
      normalizedEntryName === ".." ||
      normalizedEntryName.startsWith("../") ||
      entryNames.has(normalizedEntryName)
    ) {
      throw new PackageCacheError(
        "Package archive contains an invalid entry path",
      );
    }
    entryNames.add(normalizedEntryName);
    const entrySize = zipEntry.header.size;
    if (!Number.isSafeInteger(entrySize) || entrySize < 0) {
      throw new PackageCacheError("Package archive is invalid");
    }
    declaredUncompressedBytes += entrySize;
    if (declaredUncompressedBytes >= MAX_PACKAGE_UNCOMPRESSED_BYTES) {
      throw new PackageCacheError(
        "Package archive exceeds the extracted size limit",
      );
    }
    entryValidator(zipEntry);
    results.push(zipEntry.entryName);
  }

  try {
    // Decompress entries sequentially to bound peak memory and make file versus
    // directory conflicts deterministic. Node's asynchronous inflater and
    // asynchronous filesystem writes keep the event loop available to /health.
    for (const zipEntry of zipEntries) {
      const entryPath = path.join(outputDir, ...zipEntry.entryName.split("/"));
      if (zipEntry.isDirectory || zipEntry.entryName.endsWith("/")) {
        await fs.promises.mkdir(entryPath, { recursive: true });
        continue;
      }

      const contents = await decompressZipEntry(zipEntry);
      await fs.promises.mkdir(path.dirname(entryPath), { recursive: true });
      await fs.promises.writeFile(entryPath, contents, { flag: "wx" });
      await fs.promises.utimes(
        entryPath,
        zipEntry.header.time,
        zipEntry.header.time,
      );
      uncompressedBytes += contents.byteLength;
      if (uncompressedBytes >= MAX_PACKAGE_UNCOMPRESSED_BYTES) {
        throw new PackageCacheError(
          "Package archive exceeds the extracted size limit",
        );
      }
    }
  } catch (error) {
    throw sanitizePackageError(error, "Failed to extract package archive");
  }
  return { entries: results, uncompressedBytes };
}

async function decompressZipEntry(zipEntry: AdmZip.IZipEntry): Promise<Buffer> {
  let compressedData: Buffer;
  try {
    // @types/adm-zip misspells the runtime `encrypted` getter as `encripted`.
    // Read the standard general-purpose flag directly instead.
    if ((zipEntry.header.flags & ZIP_FLAG_ENCRYPTED) !== 0) {
      throw new PackageCacheError("Failed to extract package archive");
    }
    compressedData = zipEntry.getCompressedData();
  } catch {
    throw new PackageCacheError("Failed to extract package archive");
  }

  let contents: Buffer;
  if (zipEntry.header.method === ZIP_METHOD_STORED) {
    if (compressedData.byteLength !== zipEntry.header.size) {
      throw new PackageCacheError("Failed to extract package archive");
    }
    contents = compressedData;
  } else if (zipEntry.header.method === ZIP_METHOD_DEFLATED) {
    contents = await new Promise<Buffer>((resolve, reject) => {
      // AdmZip's async inflater does not listen for zlib errors, so malformed
      // entries escape as uncaught exceptions. Use Node's callback boundary and
      // the already-validated declared size as a hard output limit instead.
      inflateRaw(
        compressedData,
        { maxOutputLength: Math.max(zipEntry.header.size, 1) },
        (error, inflated) => {
          if (error !== null || inflated.byteLength !== zipEntry.header.size) {
            reject(new PackageCacheError("Failed to extract package archive"));
          } else {
            resolve(inflated);
          }
        },
      );
    });
  } else {
    throw new PackageCacheError("Failed to extract package archive");
  }

  let crc = 0xffffffff;
  for (let offset = 0; offset < contents.byteLength; ) {
    const end = Math.min(offset + CRC32_YIELD_BYTES, contents.byteLength);
    for (; offset < end; offset += 1) {
      crc = CRC32_TABLE[(crc ^ contents[offset]) & 0xff] ^ (crc >>> 8);
    }
    if (offset < contents.byteLength) {
      // CRC validation used to run synchronously inside AdmZip after inflate.
      // Yield between bounded chunks so a large entry cannot starve /health.
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
  }
  if (~crc >>> 0 !== zipEntry.header.crc) {
    throw new PackageCacheError("Failed to extract package archive");
  }
  return contents;
}

async function processExternalPackageStream(
  dir: string,
  externalPackage: Package,
  externalStream: stream.Readable,
): Promise<number> {
  const entryValidator = (entry: AdmZip.IZipEntry) => {
    if (!entry.entryName.startsWith("node_modules/")) {
      throw new PackageCacheError(
        "External package archive contains an invalid entry",
      );
    }
  };

  const startUnzip = performance.now();
  const zipBuffer = await processPackageStream(
    externalPackage.sha256,
    externalStream,
  );
  logDurationMs("unzipExternalsTime", startUnzip);

  const startWrites = performance.now();
  const { uncompressedBytes } = await unzipFile(zipBuffer, dir, entryValidator);
  logDurationMs("externalsWritesTime", startWrites);
  return uncompressedBytes;
}

export type LocalSourcePackage = {
  dir: string;
  /** The archive checksum is absent only for packages bundled into a Lambda. */
  archiveSha256: string | null;
  /**
   * Every file declared in metadata.json, including bundler chunks and source maps.
   */
  modulePaths: Set<CanonicalizedModulePath>;
  /**
   * The modules included in the package and that could contain Convex functions.
   * This doesn’t include bundler chunks (files in /_deps/).
   */
  modules: Set<CanonicalizedModulePath>;
  externalDepsStorageKey?: string;
  /** The linked archive checksum is absent for prebuilt or source-only packages. */
  externalDepsArchiveSha256: string | null;
  dynamicallyDownloaded: boolean;
  retainedBytes: number;
  activeOwners: number;
  lastUsed: number;
};

type ExternalDepsPackage = {
  dir: string;
  /** The archive checksum is absent only for packages bundled into a Lambda. */
  archiveSha256: string | null;
  dynamicallyDownloaded: boolean;
  retainedBytes: number;
  /** Retained source packages plus source publications currently linking it. */
  sourceOwners: number;
  lastUsed: number;
};

async function processSourcePackageStream(
  dir: string,
  sourcePackage: Package,
  sourceStream: stream.Readable,
): Promise<LocalSourcePackage> {
  const startUnzip = performance.now();
  const zipBuffer = await processPackageStream(
    sourcePackage.sha256,
    sourceStream,
  );
  logDurationMs("unzipSourceTime", startUnzip);

  // After finishing the pipeline, await on each File's buffer.
  const startWrites = performance.now();
  const { entries, uncompressedBytes } = await unzipFile(
    zipBuffer,
    dir,
    (entry) => {
      if (
        entry.entryName !== "metadata.json" &&
        !entry.entryName.startsWith("modules/")
      ) {
        throw new PackageCacheError(
          "Source package archive contains an invalid entry",
        );
      }
    },
  );
  const actualModulePaths = entries
    .filter(
      (entry) =>
        entry !== "metadata.json" &&
        // Some ZIP implementations store entries for directories themselves
        // (https://unix.stackexchange.com/a/743512/485280)
        // The Rust implementation we use in production doesn’t do it, but some
        // implementations (including the `archiver` npm package used in
        // node-executor integration tests) do so, so we are filtering them out.
        !entry.endsWith("/"),
    )
    .map((entry) => entry.substring("modules/".length));
  await fs.promises.chmod(`${dir}/metadata.json`, "444");
  const metadataJson = parseMetadataFile(
    await fs.promises.readFile(`${dir}/metadata.json`, {
      encoding: "utf-8",
    }),
  );

  // Old packages don't have package.json.
  createPackageJsonIfMissing(dir);
  logDurationMs("sourceWritesTime", startWrites);
  actualModulePaths.sort();
  if (
    JSON.stringify(metadataJson.modulePaths) !==
    JSON.stringify(actualModulePaths)
  ) {
    throw new PackageCacheError(
      "Source package metadata does not match archive contents",
    );
  }

  const modules = modulesFromMetadataJson(metadataJson);
  return {
    dir,
    archiveSha256: sourcePackage.sha256,
    modulePaths: new Set(metadataJson.modulePaths),
    modules,
    externalDepsStorageKey: metadataJson.externalDepsStorageKey,
    externalDepsArchiveSha256: null,
    dynamicallyDownloaded: true,
    retainedBytes: uncompressedBytes,
    activeOwners: 0,
    lastUsed: ++packageUseSequence,
  };
}

export const availableSourcePackages = new Map<string, LocalSourcePackage>();
export const availableExternalPackages = new Map<string, ExternalDepsPackage>();
// Node's ESM cache retains module graphs by URL after the corresponding
// disk-cache entry is retired. Package directories are unique publication
// roots, so this lifetime count tracks that non-evictable generation growth
// without exporting the paths.
const importedSourcePackageRoots = new Set<string>();
const sourcePackageRetirements = new Map<string, Promise<void>>();
const externalPackageRetirements = new Map<string, Promise<void>>();
type PackageDownload<T> = {
  archiveSha256: string;
  promise: Promise<T>;
};

const sourcePackageDownloads = new Map<
  string,
  PackageDownload<LocalSourcePackage>
>();
const externalPackageDownloads = new Map<
  string,
  PackageDownload<ExternalDepsPackage>
>();

export type SourcePackageLease = {
  package: LocalSourcePackage;
  release: () => Promise<void>;
};

export async function acquireSourcePackage(
  sourcePackage: SourcePackage,
): Promise<SourcePackageLease> {
  let localPackage: LocalSourcePackage;
  for (;;) {
    localPackage = await maybeDownloadAndLinkPackages(sourcePackage);
    // A zero-owner cache hit can retire while this async caller is resuming.
    // Check map identity and increment ownership without another await.
    if (
      availableSourcePackages.get(sourcePackage.bundled_source.key) !==
      localPackage
    ) {
      continue;
    }
    localPackage.activeOwners += 1;
    break;
  }
  localPackage.lastUsed = ++packageUseSequence;
  try {
    await enforceLocalPackageCacheBounds();
  } catch (error) {
    localPackage.activeOwners -= 1;
    throw sanitizePackageError(
      error,
      "Failed to enforce source package cache bounds",
    );
  }

  let released = false;
  return {
    package: localPackage,
    release: async () => {
      if (released) {
        throw new PackageCacheError(
          "Source package lease was released more than once",
        );
      }
      released = true;
      if (localPackage.activeOwners <= 0) {
        throw new PackageCacheError("Source package owner count is invalid");
      }
      localPackage.activeOwners -= 1;
      localPackage.lastUsed = ++packageUseSequence;
      try {
        await enforceLocalPackageCacheBounds();
      } catch (error) {
        throw sanitizePackageError(
          error,
          "Failed to enforce source package cache bounds",
        );
      }
    },
  };
}

export function recordSourcePackageImport(packageRoot: string) {
  importedSourcePackageRoots.add(packageRoot);
}

export function getPackageCacheStats() {
  const sourcePackages = [...availableSourcePackages.values()].filter(
    (localPackage) => localPackage.dynamicallyDownloaded,
  );
  const externalPackages = [...availableExternalPackages.values()].filter(
    (localPackage) => localPackage.dynamicallyDownloaded,
  );
  return {
    importedSourcePackages: importedSourcePackageRoots.size,
    retainedSourcePackages: sourcePackages.length,
    retainedSourceBytes: sourcePackages.reduce(
      (total, localPackage) => total + localPackage.retainedBytes,
      0,
    ),
    activeSourceOwners: sourcePackages.reduce(
      (total, localPackage) => total + localPackage.activeOwners,
      0,
    ),
    retainedExternalPackages: externalPackages.length,
    retainedExternalBytes: externalPackages.reduce(
      (total, localPackage) => total + localPackage.retainedBytes,
      0,
    ),
    ...packageEvents,
  };
}

export function resetPackageCachesForTests() {
  if (
    sourcePackageDownloads.size !== 0 ||
    externalPackageDownloads.size !== 0 ||
    sourcePackageRetirements.size !== 0 ||
    externalPackageRetirements.size !== 0
  ) {
    throw new PackageCacheError(
      "Cannot reset package caches while package work is active",
    );
  }
  const expectedExternalOwners = new Map<string, number>();
  for (const localPackage of availableSourcePackages.values()) {
    if (localPackage.activeOwners !== 0) {
      throw new PackageCacheError(
        "Cannot reset a package cache with active owners",
      );
    }
    if (localPackage.externalDepsStorageKey !== undefined) {
      expectedExternalOwners.set(
        localPackage.externalDepsStorageKey,
        (expectedExternalOwners.get(localPackage.externalDepsStorageKey) ?? 0) +
          1,
      );
    }
  }
  for (const [key, expectedOwners] of expectedExternalOwners) {
    if (availableExternalPackages.get(key)?.sourceOwners !== expectedOwners) {
      throw new PackageCacheError(
        "Source and external package ownership is inconsistent",
      );
    }
  }
  for (const [key, externalPackage] of availableExternalPackages) {
    if (
      !expectedExternalOwners.has(key) &&
      externalPackage.sourceOwners !== 0
    ) {
      throw new PackageCacheError(
        "Source and external package ownership is inconsistent",
      );
    }
  }
  for (const localPackage of availableSourcePackages.values()) {
    unregisterPrepareStackTrace(path.join(localPackage.dir, "modules"));
  }
  availableSourcePackages.clear();
  availableExternalPackages.clear();
  importedSourcePackageRoots.clear();
  packageUseSequence = 0;
  for (const event of Object.keys(packageEvents) as Array<
    keyof typeof packageEvents
  >) {
    packageEvents[event] = 0;
  }
}

async function enforceLocalPackageCacheBounds() {
  if (RUNNING_IN_AWS_LAMBDA) {
    return;
  }

  const retirements: Promise<void>[] = [];
  while (sourcePackageCacheExceedsBounds()) {
    const candidate = [...availableSourcePackages.entries()]
      .filter(
        ([, localPackage]) =>
          localPackage.dynamicallyDownloaded && localPackage.activeOwners === 0,
      )
      .sort(([, a], [, b]) => a.lastUsed - b.lastUsed)[0];
    if (candidate === undefined) {
      break;
    }
    retirements.push(retireSourcePackage(candidate[0], candidate[1]));
  }

  while (externalPackageCacheExceedsBounds()) {
    const candidate = [...availableExternalPackages.entries()]
      .filter(
        ([, localPackage]) =>
          localPackage.dynamicallyDownloaded && localPackage.sourceOwners === 0,
      )
      .sort(([, a], [, b]) => a.lastUsed - b.lastUsed)[0];
    if (candidate !== undefined) {
      retirements.push(retireExternalPackage(candidate[0], candidate[1]));
      continue;
    }

    // External ownership belongs to retained or publishing source packages.
    // If every over-budget external still has an owner, retire an inactive
    // retained source first; its synchronous owner release can make the
    // external eligible on the next iteration. Publishing and active sources
    // remain protected and can temporarily keep the cache over its limit.
    const sourceCandidate = [...availableSourcePackages.entries()]
      .filter(([, localPackage]) => {
        if (
          !localPackage.dynamicallyDownloaded ||
          localPackage.activeOwners !== 0 ||
          localPackage.externalDepsStorageKey === undefined
        ) {
          return false;
        }
        return (
          availableExternalPackages.get(localPackage.externalDepsStorageKey)
            ?.dynamicallyDownloaded === true
        );
      })
      .sort(([, a], [, b]) => a.lastUsed - b.lastUsed)[0];
    if (sourceCandidate === undefined) {
      break;
    }
    retirements.push(
      retireSourcePackage(sourceCandidate[0], sourceCandidate[1]),
    );
  }
  // Map, root, and owner transitions happen synchronously above. Once no
  // package owns either path, source and external deletion can proceed together.
  await Promise.all(retirements);
}

function sourcePackageCacheExceedsBounds() {
  const packages = [...availableSourcePackages.values()].filter(
    (localPackage) => localPackage.dynamicallyDownloaded,
  );
  return (
    packages.length > MAX_RETAINED_SOURCE_PACKAGES ||
    packages.reduce(
      (total, localPackage) => total + localPackage.retainedBytes,
      0,
    ) > MAX_RETAINED_SOURCE_PACKAGE_BYTES
  );
}

function externalPackageCacheExceedsBounds() {
  const packages = [...availableExternalPackages.values()].filter(
    (localPackage) => localPackage.dynamicallyDownloaded,
  );
  return (
    packages.length > MAX_RETAINED_EXTERNAL_PACKAGES ||
    packages.reduce(
      (total, localPackage) => total + localPackage.retainedBytes,
      0,
    ) > MAX_RETAINED_EXTERNAL_PACKAGE_BYTES
  );
}

function retireSourcePackage(
  key: string,
  localPackage: LocalSourcePackage,
): Promise<void> {
  if (localPackage.activeOwners !== 0) {
    throw new PackageCacheError("Cannot retire an active source package");
  }
  if (availableSourcePackages.get(key) !== localPackage) {
    throw new PackageCacheError(
      "Cannot retire a source package that is not current",
    );
  }

  const externalPackage =
    localPackage.externalDepsStorageKey !== undefined
      ? availableExternalPackages.get(localPackage.externalDepsStorageKey)
      : undefined;
  if (
    localPackage.externalDepsStorageKey !== undefined &&
    externalPackage === undefined
  ) {
    throw new PackageCacheError(
      "Retained source package has no external dependency owner",
    );
  }
  if (externalPackage !== undefined && externalPackage.sourceOwners <= 0) {
    throw new PackageCacheError(
      "External package source-owner count is invalid",
    );
  }

  availableSourcePackages.delete(key);
  unregisterPrepareStackTrace(path.join(localPackage.dir, "modules"));
  if (externalPackage !== undefined) {
    externalPackage.sourceOwners -= 1;
    externalPackage.lastUsed = ++packageUseSequence;
  }
  packageEvents.sourceRetirements += 1;
  const retirement = fs.promises
    .rm(localPackage.dir, { recursive: true, force: true })
    .finally(() => {
      if (sourcePackageRetirements.get(key) === retirement) {
        sourcePackageRetirements.delete(key);
      }
    });
  sourcePackageRetirements.set(key, retirement);
  return retirement;
}

function retireExternalPackage(
  key: string,
  localPackage: ExternalDepsPackage,
): Promise<void> {
  if (localPackage.sourceOwners !== 0) {
    throw new PackageCacheError("Cannot retire a referenced external package");
  }
  if (availableExternalPackages.get(key) !== localPackage) {
    throw new PackageCacheError(
      "Cannot retire an external package that is not current",
    );
  }

  availableExternalPackages.delete(key);
  packageEvents.externalRetirements += 1;
  const retirement = fs.promises
    .rm(localPackage.dir, { recursive: true, force: true })
    .finally(() => {
      if (externalPackageRetirements.get(key) === retirement) {
        externalPackageRetirements.delete(key);
      }
    });
  externalPackageRetirements.set(key, retirement);
  return retirement;
}

async function statIfExists(filePath: string): Promise<fs.Stats | null> {
  try {
    return await fs.promises.stat(filePath);
  } catch (error: unknown) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

async function localSourcePackageIsComplete(
  local: LocalSourcePackage,
  requiresNodeModules = false,
): Promise<boolean> {
  const packageJsonStat = await statIfExists(
    path.join(local.dir, "package.json"),
  );
  if (!packageJsonStat?.isFile()) {
    return false;
  }

  const modulesDir = path.join(local.dir, "modules");
  const modulesDirStat = await statIfExists(modulesDir);
  if (!modulesDirStat?.isDirectory()) {
    return false;
  }
  // Package metadata can contain many files. Keep this sequential to bound
  // filesystem concurrency while yielding to /health between checks.
  for (const modulePath of local.modulePaths) {
    const moduleStat = await statIfExists(path.join(modulesDir, modulePath));
    if (!moduleStat?.isFile()) {
      return false;
    }
  }
  if (requiresNodeModules) {
    const nodeModulesStat = await statIfExists(
      path.join(local.dir, "node_modules"),
    );
    if (!nodeModulesStat?.isDirectory()) {
      return false;
    }
  }
  return true;
}

async function validateLocalSourcePackage(
  local: LocalSourcePackage,
  requiresNodeModules = false,
): Promise<void> {
  if (await localSourcePackageIsComplete(local, requiresNodeModules)) {
    return;
  }
  throw new PackageCacheError("Incomplete source package");
}

function validateExternalDepsMatch(
  local: LocalSourcePackage,
  externalPackage: Package | null | undefined,
) {
  if (local.externalDepsStorageKey !== externalPackage?.key) {
    throw new PackageCacheError(
      "Source package external dependencies do not match package metadata",
    );
  }
  if (externalPackage !== null && externalPackage !== undefined) {
    validatePackageChecksum(local.externalDepsArchiveSha256, externalPackage);
  }
}

function validatePackageChecksum(
  cachedArchiveSha256: string | null,
  requestedPackage: Package,
) {
  if (
    cachedArchiveSha256 !== null &&
    cachedArchiveSha256 !== requestedPackage.sha256
  ) {
    throw new PackageCacheError(
      "Package checksum does not match cached package identity",
    );
  }
}

async function localExternalPackageIsComplete(local: {
  dir: string;
}): Promise<boolean> {
  const nodeModulesDir = path.join(local.dir, "node_modules");
  const nodeModulesStat = await statIfExists(nodeModulesDir);
  return nodeModulesStat?.isDirectory() ?? false;
}

async function validateLocalExternalPackage(local: {
  dir: string;
}): Promise<void> {
  if (await localExternalPackageIsComplete(local)) {
    return;
  }
  throw new PackageCacheError("Incomplete external deps package");
}

/**
 * Prepopulates source and external deps caches if this Lambda was pushed with source and, optionally,
 * an external deps package.
 *
 * This source is pushed in ${__dirname}/source/ and includes the following folders:
 * - modules/ storing all user code
 * - [optional] node_modules/ storing external dependencies
 * - metadata.json storing MetadataJson object
 * - package.json
 */
export async function populatePrebuildPackages() {
  try {
    await populatePrebuildPackagesInner();
  } catch (error) {
    throw sanitizePackageError(error, "Failed to initialize prebuilt packages");
  }
}

async function populatePrebuildPackagesInner() {
  if (
    availableSourcePackages.size !== 0 ||
    availableExternalPackages.size !== 0
  ) {
    throw new PackageCacheError(
      "Cannot populate prebuilt packages after cache initialization",
    );
  }
  // Lambda can preserve /tmp across a runtime reset even though the new Node
  // process has empty ownership maps. Prebuild initialization is the only safe
  // point to remove every dynamic package root without racing an invocation.
  await removePackageDirectories(
    ["source", "external_deps", "build_deps"].map((cacheName) =>
      path.join(os.tmpdir(), cacheName),
    ),
    "Failed to clear package caches during Lambda initialization",
  );
  const sourceDir = path.join(__dirname, "/source");
  if (fs.statSync(sourceDir, { throwIfNoEntry: false }) === undefined) {
    // If we weren't pushed with source, skip prepopulations
    return;
  }

  const pkgs = fs.readdirSync(sourceDir);
  for (const pkg of pkgs) {
    const pkgDir = path.join(sourceDir, pkg);
    const metadata = parseMetadataFile(
      fs.readFileSync(`${pkgDir}/metadata.json`, { encoding: "utf-8" }),
    );
    const modules = modulesFromMetadataJson(metadata);
    const localPackage: LocalSourcePackage = {
      dir: pkgDir,
      archiveSha256: null,
      modulePaths: new Set(metadata.modulePaths),
      modules,
      externalDepsStorageKey: metadata.externalDepsStorageKey,
      externalDepsArchiveSha256: null,
      dynamicallyDownloaded: false,
      retainedBytes: 0,
      activeOwners: 0,
      lastUsed: ++packageUseSequence,
    };
    await validateLocalSourcePackage(
      localPackage,
      metadata.externalDepsStorageKey !== undefined,
    );
    availableSourcePackages.set(pkg, localPackage);
    registerPrepareStackTrace(path.join(localPackage.dir, "modules"));

    if (metadata.externalDepsStorageKey !== undefined) {
      logDebug("Prepopulating external dependencies");
      const externalPackage = availableExternalPackages.get(
        metadata.externalDepsStorageKey,
      );
      if (externalPackage === undefined) {
        availableExternalPackages.set(metadata.externalDepsStorageKey, {
          dir: pkgDir,
          archiveSha256: null,
          dynamicallyDownloaded: false,
          retainedBytes: 0,
          sourceOwners: 1,
          lastUsed: ++packageUseSequence,
        });
      } else {
        externalPackage.sourceOwners += 1;
        externalPackage.lastUsed = ++packageUseSequence;
      }
    }
  }
}

function createPackageJsonIfMissing(dir: string) {
  // Ensure package.json exists. This is required so Node knows to execute
  // the user modules as ESM, since they have .js and not .mjs extension.
  const packageJsonPath = path.join(dir, "package.json");
  if (fs.existsSync(packageJsonPath)) {
    return;
  }
  fs.writeFileSync(packageJsonPath, `{ "type": "module" }`);
}

function modulesFromMetadataJson(
  metadataJson: MetadataJson,
): Set<CanonicalizedModulePath> {
  const modules: Set<string> = new Set();
  for (const path of metadataJson.modulePaths) {
    if (path.startsWith("_deps/")) {
      // Ignore bundler chunks since they don’t contain Convex function definitions.
      continue;
    } else if (path.endsWith(".js")) {
      // Only load node files.
      const environment = metadataJson.moduleEnvironments.get(path);
      if (!environment) {
        throw new PackageCacheError(
          "Source package module is missing an environment",
        );
      }
      if (environment !== "node") {
        continue;
      }
      modules.add(path);
    } else if (path.endsWith(".js.map")) {
      continue;
    } else {
      throw new PackageCacheError(
        "Source package metadata contains an invalid module path",
      );
    }
  }
  return modules;
}

async function cleanupDynamicSourcePackages() {
  const removedPackages: LocalSourcePackage[] = [];
  for (const [key, localPackage] of availableSourcePackages) {
    if (localPackage.dynamicallyDownloaded) {
      if (localPackage.activeOwners !== 0) {
        throw new PackageCacheError(
          "Cannot clean an active Lambda source package",
        );
      }
      const externalPackage =
        localPackage.externalDepsStorageKey !== undefined
          ? availableExternalPackages.get(localPackage.externalDepsStorageKey)
          : undefined;
      if (
        localPackage.externalDepsStorageKey !== undefined &&
        externalPackage === undefined
      ) {
        throw new PackageCacheError(
          "Retained source package has no external dependency owner",
        );
      }
      if (externalPackage !== undefined) {
        if (externalPackage.sourceOwners <= 0) {
          throw new PackageCacheError(
            "External package source-owner count is invalid",
          );
        }
        externalPackage.sourceOwners -= 1;
      }
      availableSourcePackages.delete(key);
      unregisterPrepareStackTrace(path.join(localPackage.dir, "modules"));
      removedPackages.push(localPackage);
    }
  }
  // Ownership is already gone. Wait for every removal even if one fails so a
  // later warm invocation cannot publish while an earlier rm still traverses it.
  await removePackageDirectories(
    removedPackages.map((localPackage) => localPackage.dir),
    "Failed to clean Lambda source packages",
  );
}

async function cleanupDynamicExternalPackages() {
  const removedPackages: ExternalDepsPackage[] = [];
  for (const [key, localPackage] of availableExternalPackages) {
    if (localPackage.dynamicallyDownloaded) {
      if (localPackage.sourceOwners !== 0) {
        throw new PackageCacheError(
          "Cannot clean a referenced Lambda external package",
        );
      }
      availableExternalPackages.delete(key);
      removedPackages.push(localPackage);
    }
  }
  await removePackageDirectories(
    removedPackages.map((localPackage) => localPackage.dir),
    "Failed to clean Lambda external packages",
  );
}
