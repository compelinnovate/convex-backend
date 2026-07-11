import archiver from "archiver";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { fileURLToPath } from "node:url";

import { logDurationMs } from "./log";

export type BuildDepsRequest = {
  type: "build_deps";
  requestId: string;

  deps: NodeDependency[];
  uploadUrl: string;
};

export type BuildDepsResponse =
  | {
      type: "success";
      sha256Digest: number[];
      zippedSizeBytes: number;
      unzippedSizeBytes: number;
    }
  | {
      type: "error";
      message: string;
    };

export type NodeDependency = {
  package: string;
  version: string;
};

const NPM_INSTALL_TIMEOUT_MS = 450_000;
const PACKAGE_UPLOAD_TIMEOUT_MS = 120_000;
// Keep these creation-side limits aligned with PackageSize::verify_size in
// crates/model/src/source_packages/types.rs and the download-side limits in
// source_package.ts.
const MAX_PACKAGE_ARCHIVE_BYTES = 90_000_000;
const MAX_PACKAGE_UNCOMPRESSED_BYTES = 230_000_000;

// Run npm behind a small process-group owner. Ordinary completion and timeout
// cleanup wait for this supervisor to close. If the executor generation dies,
// IPC closure asks the supervisor to kill the group, but Rust does not wait for
// or acknowledge that best-effort cleanup.
const NPM_INSTALL_SUPERVISOR = String.raw`
const { spawn } = require("node:child_process");

const npm = spawn(process.argv[1], ["install"], {
  stdio: "ignore",
  windowsHide: true,
});

function terminateOwnedGroup() {
  try {
    process.kill(-process.pid, "SIGKILL");
  } catch {
    npm.kill("SIGKILL");
    process.exit(1);
  }
}

let settled = false;
function finish(exitCode) {
  if (settled) return;
  settled = true;
  if (!process.connected || typeof process.send !== "function") {
    terminateOwnedGroup();
    return;
  }
  try {
    process.send({ type: "npmExit", exitCode }, terminateOwnedGroup);
  } catch {
    terminateOwnedGroup();
  }
}

process.once("disconnect", terminateOwnedGroup);
if (!process.connected) {
  terminateOwnedGroup();
}
npm.once("error", () => finish(1));
npm.once("close", (code) => finish(code ?? 1));
`;

class BuildDepsError extends Error {}

export async function buildDeps(
  request: BuildDepsRequest,
): Promise<BuildDepsResponse> {
  try {
    const url = parseUploadUrl(request.uploadUrl);
    return await buildDepsInner(url, request.deps);
  } catch (error: unknown) {
    return {
      type: "error",
      message: buildDepsErrorMessage(error),
    };
  }
}

function parseUploadUrl(uploadUrl: string): URL {
  let url: URL;
  try {
    url = new URL(uploadUrl);
    if (url.protocol === "file:") {
      fileURLToPath(url);
    }
  } catch {
    throw new BuildDepsError("Invalid external dependency upload URL");
  }
  if (url.protocol !== "file:" && url.protocol !== "https:") {
    throw new BuildDepsError("Invalid external dependency upload URL");
  }
  return url;
}

function buildDepsErrorMessage(error: unknown): string {
  if (isBuildDepsError(error)) {
    return error.message;
  }
  return "Failed to build external dependencies";
}

function isBuildDepsError(error: unknown): error is BuildDepsError {
  try {
    return error instanceof BuildDepsError;
  } catch {
    // A thrown Proxy can reject the instanceof check.
    return false;
  }
}

async function hashFromFile(file: string): Promise<Buffer> {
  const hash = createHash("sha256");
  for await (const chunk of fs.createReadStream(file)) {
    hash.update(chunk);
  }
  return hash.digest();
}

async function directorySize(directory: string): Promise<number> {
  let total = 0;

  async function visit(currentDirectory: string): Promise<void> {
    const entries = await fs.promises.readdir(currentDirectory, {
      withFileTypes: true,
    });
    // Keep traversal sequential. A dependency tree can contain many thousands
    // of files, and issuing every stat at once can exhaust the executor's file
    // limit.
    for (const entry of entries) {
      const entryPath = path.join(currentDirectory, entry.name);
      if (entry.isDirectory()) {
        await visit(entryPath);
        continue;
      }

      // Do not follow package-created symlinks outside node_modules or into a
      // cycle while calculating the retained package size.
      const entrySize = (await fs.promises.lstat(entryPath)).size;
      if (
        !Number.isSafeInteger(entrySize) ||
        entrySize < 0 ||
        total >= MAX_PACKAGE_UNCOMPRESSED_BYTES - entrySize
      ) {
        throw new BuildDepsError(
          "External dependencies exceed the extracted size limit",
        );
      }
      total += entrySize;
    }
  }

  await visit(directory);
  return total;
}

async function installDependencies(dir: string): Promise<void> {
  const npmCache = path.join(dir, ".npm");
  const npmCommand = process.platform === "win32" ? "npm.cmd" : "npm";
  const ownsProcessGroup = process.platform !== "win32";
  const startInstall = performance.now();

  await new Promise<void>((resolve, reject) => {
    const child = spawn(
      ownsProcessGroup ? process.execPath : npmCommand,
      ownsProcessGroup
        ? ["-e", NPM_INSTALL_SUPERVISOR, npmCommand]
        : ["install"],
      {
        cwd: dir,
        env: {
          ...process.env,
          NPM_CONFIG_CACHE: npmCache,
          // NPM configuration is case-insensitive, but packages such as Sharp
          // read only the lowercase spelling for their own build cache.
          npm_config_cache: npmCache,
        },
        detached: ownsProcessGroup,
        // The supervisor treats IPC disconnect as generation cancellation.
        stdio: ownsProcessGroup
          ? ["ignore", "ignore", "ignore", "ipc"]
          : "ignore",
        windowsHide: true,
      },
    );
    let timedOut = false;
    let npmExitCode: number | undefined;
    child.on("message", (message: unknown) => {
      if (
        typeof message === "object" &&
        message !== null &&
        "type" in message &&
        message.type === "npmExit" &&
        "exitCode" in message &&
        typeof message.exitCode === "number" &&
        Number.isInteger(message.exitCode)
      ) {
        npmExitCode = message.exitCode;
      }
    });
    const timeout = setTimeout(() => {
      timedOut = true;
      if (ownsProcessGroup && child.pid !== undefined) {
        try {
          // npm lifecycle scripts can spawn children. Signal the complete
          // owned group before waiting for supervisor close and allowing cleanup.
          process.kill(-child.pid, "SIGKILL");
        } catch (error: unknown) {
          if ((error as NodeJS.ErrnoException).code !== "ESRCH") {
            if (child.connected) {
              // Closing IPC asks the supervisor to terminate the same group
              // even if the parent could not signal the group directly.
              try {
                child.disconnect();
              } catch {
                child.kill("SIGKILL");
              }
            } else {
              child.kill("SIGKILL");
            }
          }
        }
      } else {
        child.kill("SIGKILL");
      }
    }, NPM_INSTALL_TIMEOUT_MS);
    timeout.unref();

    child.once("error", () => {
      clearTimeout(timeout);
      reject(new BuildDepsError("Failed to start dependency installation"));
    });
    child.once("close", (code) => {
      clearTimeout(timeout);
      // On Unix the supervisor reports npm's status before killing its group
      // and closing. Both successful installs and timeout cleanup settle only
      // after that observed close; descendant exit itself is not acknowledged.
      if (timedOut) {
        reject(
          new BuildDepsError(
            `Dependency installation timed out after ${NPM_INSTALL_TIMEOUT_MS}ms`,
          ),
        );
      } else if ((ownsProcessGroup ? npmExitCode : code) !== 0) {
        reject(new BuildDepsError("Failed to install external dependencies"));
      } else {
        resolve();
      }
    });
  });

  logDurationMs("npm install", startInstall);
}

async function createDependencyArchive(
  nodeModulesDir: string,
  archivePath: string,
): Promise<void> {
  const output = fs.createWriteStream(archivePath, { flags: "wx" });
  const zip = archiver("zip");
  let archiveBytes = 0;
  const sizeLimit = new Transform({
    transform(chunk: Buffer, _encoding, callback) {
      archiveBytes += chunk.byteLength;
      if (archiveBytes >= MAX_PACKAGE_ARCHIVE_BYTES) {
        callback(
          new BuildDepsError(
            "External dependency archive exceeds the size limit",
          ),
        );
        return;
      }
      callback(null, chunk);
    },
  });
  // Archiver reports recoverable filesystem failures through `warning`. They
  // still make this package incomplete, so fail the same pipeline boundary.
  zip.on("warning", (error) => zip.emit("error", error));
  const pipelinePromise = pipeline(zip, sizeLimit, output);
  zip.directory(nodeModulesDir, "node_modules");
  const [finalizeResult, pipelineResult] = await Promise.allSettled([
    zip.finalize(),
    pipelinePromise,
  ]);
  if (
    pipelineResult.status === "rejected" &&
    isBuildDepsError(pipelineResult.reason)
  ) {
    throw pipelineResult.reason;
  }
  if (
    finalizeResult.status === "rejected" ||
    pipelineResult.status === "rejected"
  ) {
    throw new BuildDepsError("Failed to archive external dependencies");
  }
}

async function uploadDependencyArchive(
  url: URL,
  archivePath: string,
  zippedSizeBytes: number,
): Promise<void> {
  if (url.protocol === "file:") {
    const filePath = fileURLToPath(url);
    await fs.promises.mkdir(path.dirname(filePath), {
      recursive: true,
      mode: 0o744,
    });
    await fs.promises.rename(archivePath, filePath);
    return;
  }

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    PACKAGE_UPLOAD_TIMEOUT_MS,
  );
  timeout.unref();
  const readStream = fs.createReadStream(archivePath, {
    signal: controller.signal,
  });
  try {
    const response = await fetch(url, {
      method: "PUT",
      headers: {
        "Content-Length": zippedSizeBytes.toString(),
      },
      // @ts-expect-error DOM and Node declare incompatible ReadableStream types.
      body: Readable.toWeb(readStream),
      duplex: "half",
      redirect: "error",
      signal: controller.signal,
    });
    if (!response.ok) {
      controller.abort();
      throw new BuildDepsError(
        `Failed to upload external dependencies: HTTP ${response.status}`,
      );
    }
    await response.body?.cancel();
  } catch (error: unknown) {
    if (isBuildDepsError(error)) {
      throw error;
    }
    if (controller.signal.aborted) {
      throw new BuildDepsError(
        `External dependency upload timed out after ${PACKAGE_UPLOAD_TIMEOUT_MS}ms`,
      );
    }
    throw new BuildDepsError("Failed to upload external dependencies");
  } finally {
    clearTimeout(timeout);
    readStream.destroy();
  }
}

async function buildDepsInner(
  url: URL,
  deps: NodeDependency[],
): Promise<BuildDepsResponse> {
  // Local executors accept concurrent requests. Each build needs its own tree;
  // a shared directory lets one request delete another request's npm install.
  const buildRoot = path.join(os.tmpdir(), "build_deps");
  await fs.promises.mkdir(buildRoot, { recursive: true, mode: 0o700 });
  const dir = await fs.promises.mkdtemp(path.join(buildRoot, "build-"));
  try {
    const packageJson = {
      name: "convex-external-dependencies",
      version: "0.0.0",
      dependencies: Object.fromEntries(
        deps.map((dependency) => [dependency.package, dependency.version]),
      ),
    };
    await fs.promises.writeFile(
      path.join(dir, "package.json"),
      JSON.stringify(packageJson),
    );

    // Use an asynchronous child so dependency installation does not block the
    // local executor's /health watchdog or unrelated Node actions.
    await installDependencies(dir);

    const nodeModulesDir = path.join(dir, "node_modules");
    let nodeModulesStat: fs.Stats;
    try {
      // A lifecycle script can replace the root after npm creates it. Require
      // the owned directory itself so archive traversal cannot follow a root
      // symlink outside this request's private build tree.
      nodeModulesStat = await fs.promises.lstat(nodeModulesDir);
    } catch {
      throw new BuildDepsError("Failed to generate external dependencies");
    }
    if (!nodeModulesStat.isDirectory()) {
      throw new BuildDepsError("Failed to generate external dependencies");
    }

    // Validate the extracted-size contract before starting archive writes. The
    // two reads are otherwise parallel-safe, but sequencing them prevents an
    // oversized npm tree from filling local disk with a ZIP that must be
    // rejected.
    let unzippedSizeBytes: number;
    try {
      unzippedSizeBytes = await directorySize(nodeModulesDir);
    } catch (error: unknown) {
      if (isBuildDepsError(error)) {
        throw error;
      }
      throw new BuildDepsError("Failed to inspect external dependencies");
    }

    const archivePath = path.join(dir, "node_modules.zip");
    const startZip = performance.now();
    await createDependencyArchive(nodeModulesDir, archivePath);
    logDurationMs("buildDepsZipDone", startZip);

    const [hashResult, archiveStatResult] = await Promise.allSettled([
      hashFromFile(archivePath),
      fs.promises.stat(archivePath),
    ]);
    if (
      hashResult.status === "rejected" ||
      archiveStatResult.status === "rejected"
    ) {
      throw new BuildDepsError("Failed to inspect dependency archive");
    }
    const hash = hashResult.value;
    const archiveStat = archiveStatResult.value;
    const zippedSizeBytes = archiveStat.size;
    if (zippedSizeBytes >= MAX_PACKAGE_ARCHIVE_BYTES) {
      throw new BuildDepsError(
        "External dependency archive exceeds the size limit",
      );
    }

    const startUpload = performance.now();
    await uploadDependencyArchive(url, archivePath, zippedSizeBytes);
    logDurationMs("externalPackageUpload", startUpload);

    return {
      type: "success",
      sha256Digest: Array.from(hash),
      unzippedSizeBytes,
      zippedSizeBytes,
    };
  } finally {
    try {
      await fs.promises.rm(dir, { recursive: true, force: true });
    } catch {
      throw new BuildDepsError(
        "Failed to clean external dependency build directory",
      );
    }
  }
}
