import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

import AdmZip from "adm-zip";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { buildDeps, BuildDepsRequest, BuildDepsResponse } from "./build_deps";

let tempDir: string;
let originalPath: string | undefined;

beforeEach(async () => {
  tempDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "node-executor-build-deps-test-"),
  );
  originalPath = process.env.PATH;
  const binDir = path.join(tempDir, "bin");
  await fs.promises.mkdir(binDir);
  process.env.PATH = `${binDir}${path.delimiter}${originalPath ?? ""}`;
  vi.spyOn(os, "tmpdir").mockImplementation(() => tempDir);
  await writeFakeNpm();
});

afterEach(async () => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  if (originalPath === undefined) {
    delete process.env.PATH;
  } else {
    process.env.PATH = originalPath;
  }
  await fs.promises.rm(tempDir, { recursive: true, force: true });
});

test("concurrent builds use private directories without blocking the event loop", async () => {
  const firstArchive = path.join(tempDir, "first.zip");
  const secondArchive = path.join(tempDir, "second.zip");
  let firstSettled = false;
  const firstBuild = buildDeps(
    requestFor(pathToFileURL(firstArchive).href, "first-package"),
  ).finally(() => {
    firstSettled = true;
  });
  const secondBuild = buildDeps(
    requestFor(pathToFileURL(secondArchive).href, "second-package"),
  );

  await new Promise((resolve) => setTimeout(resolve, 25));
  expect(firstSettled).toBe(false);

  const [firstResult, secondResult] = await Promise.all([
    firstBuild,
    secondBuild,
  ]);
  expect(firstResult.type).toBe("success");
  expect(secondResult.type).toBe("success");

  const firstManifest = archiveManifest(firstArchive);
  const secondManifest = archiveManifest(secondArchive);
  expect(firstManifest.dependencies).toEqual({ "first-package": "1.0.0" });
  expect(secondManifest.dependencies).toEqual({ "second-package": "1.0.0" });
  expect(firstManifest.cache).not.toBe(secondManifest.cache);
  expect(firstManifest.name).toBe("convex-external-dependencies");
  expect(firstManifest.name).not.toContain(firstArchive);
  expect(buildDirectories()).toEqual([]);
});

test("dependency installation failures return fixed text and clean staging", async () => {
  await writeFakeNpm({ exitCode: 1, diagnostic: "token=signed-secret" });

  const result = await buildDeps(
    requestFor(
      pathToFileURL(path.join(tempDir, "failed.zip")).href,
      "private-package-name",
    ),
  );

  expect(result).toEqual({
    type: "error",
    message: "Failed to install external dependencies",
  });
  expect(errorMessage(result)).not.toContain("signed-secret");
  expect(errorMessage(result)).not.toContain("private-package-name");
  expect(buildDirectories()).toEqual([]);
});

test("oversized dependency trees fail before archive creation or upload", async () => {
  await writeFakeNpm({ nodeModulesFileSize: 230_000_000 });
  const archivePath = path.join(tempDir, "oversized.zip");

  const result = await buildDeps(
    requestFor(pathToFileURL(archivePath).href, "example"),
  );

  expect(result).toEqual({
    type: "error",
    message: "External dependencies exceed the extracted size limit",
  });
  expect(fs.existsSync(archivePath)).toBe(false);
  expect(buildDirectories()).toEqual([]);
});

test.skipIf(process.platform === "win32")(
  "dependency builds reject a symlinked node_modules root",
  async () => {
    const outsideDir = path.join(tempDir, "outside");
    await fs.promises.mkdir(outsideDir);
    await fs.promises.writeFile(
      path.join(outsideDir, "private-file"),
      "must not be archived",
    );
    await writeFakeNpm({ nodeModulesSymlinkTarget: outsideDir });
    const archivePath = path.join(tempDir, "symlinked.zip");

    const result = await buildDeps(
      requestFor(pathToFileURL(archivePath).href, "example"),
    );

    expect(result).toEqual({
      type: "error",
      message: "Failed to generate external dependencies",
    });
    expect(fs.existsSync(archivePath)).toBe(false);
    expect(buildDirectories()).toEqual([]);
  },
);

test.skipIf(process.platform === "win32")(
  "ordinary dependency completion signals lifecycle descendants before cleanup",
  async () => {
    const descendantMarker = path.join(tempDir, "ordinary-descendant-write");
    const descendantStartedMarker = path.join(
      tempDir,
      "ordinary-descendant-started",
    );
    await writeFakeNpm({ descendantMarker, descendantStartedMarker });

    const result = await buildDeps(
      requestFor(
        pathToFileURL(path.join(tempDir, "completed.zip")).href,
        "example",
      ),
    );

    expect(result.type).toBe("success");
    expect(fs.existsSync(descendantStartedMarker)).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 1_100));
    expect(fs.existsSync(descendantMarker)).toBe(false);
    expect(buildDirectories()).toEqual([]);
  },
);

test.skipIf(process.platform === "win32")(
  "dependency installation timeout signals lifecycle descendants before cleanup",
  async () => {
    const realSetTimeout = globalThis.setTimeout;
    vi.spyOn(globalThis, "setTimeout").mockImplementation(((
      ...args: Parameters<typeof setTimeout>
    ) => {
      const [callback, delay, ...callbackArgs] = args;
      return realSetTimeout(
        callback,
        delay === 450_000 ? 500 : delay,
        ...callbackArgs,
      );
    }) as typeof setTimeout);
    const descendantMarker = path.join(tempDir, "late-descendant-write");
    const descendantStartedMarker = path.join(tempDir, "descendant-started");
    await writeFakeNpm({
      descendantMarker,
      descendantStartedMarker,
      installDelayMs: 5_000,
    });

    const result = await buildDeps(
      requestFor(
        pathToFileURL(path.join(tempDir, "timed-out.zip")).href,
        "example",
      ),
    );

    expect(result).toEqual({
      type: "error",
      message: "Dependency installation timed out after 450000ms",
    });
    expect(fs.existsSync(descendantStartedMarker)).toBe(true);
    await new Promise((resolve) => realSetTimeout(resolve, 750));
    expect(fs.existsSync(descendantMarker)).toBe(false);
    expect(buildDirectories()).toEqual([]);
  },
);

test("upload failures reject non-success status without disclosing the signed URL", async () => {
  let uploadSignal: AbortSignal | null | undefined;
  let uploadRedirect: RequestRedirect | undefined;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (_input, init) => {
    uploadSignal = init?.signal;
    uploadRedirect = init?.redirect;
    return new Response(null, { status: 403 });
  });
  const uploadUrl = "https://packages.invalid/external.zip?token=signed-secret";

  const result = await buildDeps(requestFor(uploadUrl, "example"));

  expect(result).toEqual({
    type: "error",
    message: "Failed to upload external dependencies: HTTP 403",
  });
  expect(errorMessage(result)).not.toContain("signed-secret");
  expect(uploadSignal).toBeDefined();
  expect(uploadRedirect).toBe("error");
  expect(buildDirectories()).toEqual([]);
});

test("stalled uploads have one bounded abort deadline", async () => {
  const realSetTimeout = globalThis.setTimeout;
  vi.spyOn(globalThis, "setTimeout").mockImplementation(((
    ...args: Parameters<typeof setTimeout>
  ) => {
    const [callback, delay, ...callbackArgs] = args;
    return realSetTimeout(
      callback,
      delay === 120_000 ? 10 : delay,
      ...callbackArgs,
    );
  }) as typeof setTimeout);
  let markFetchStarted!: () => void;
  const fetchStarted = new Promise<void>((resolve) => {
    markFetchStarted = resolve;
  });
  vi.spyOn(globalThis, "fetch").mockImplementation((_input, init) => {
    const signal = init?.signal;
    if (signal === undefined || signal === null) {
      throw new Error("Expected the upload to have an abort signal");
    }
    markFetchStarted();
    return new Promise<Response>((_resolve, reject) => {
      signal.addEventListener(
        "abort",
        () => reject(new DOMException("aborted", "AbortError")),
        { once: true },
      );
    });
  });
  const uploadUrl = "https://packages.invalid/external.zip?token=signed-secret";
  const build = buildDeps(requestFor(uploadUrl, "example"));
  await fetchStarted;

  const result = await build;
  expect(result).toEqual({
    type: "error",
    message: "External dependency upload timed out after 120000ms",
  });
  expect(errorMessage(result)).not.toContain("signed-secret");
  expect(buildDirectories()).toEqual([]);
});

test("invalid upload URLs fail before dependency installation", async () => {
  const result = await buildDeps(
    requestFor("https://[invalid?token=signed-secret", "example"),
  );

  expect(result).toEqual({
    type: "error",
    message: "Invalid external dependency upload URL",
  });
  expect(errorMessage(result)).not.toContain("signed-secret");
  expect(buildDirectories()).toEqual([]);
});

function requestFor(uploadUrl: string, packageName: string): BuildDepsRequest {
  return {
    type: "build_deps",
    requestId: "test-request",
    deps: [{ package: packageName, version: "1.0.0" }],
    uploadUrl,
  };
}

function errorMessage(response: BuildDepsResponse): string {
  if (response.type !== "error") {
    throw new Error("Expected build-deps to return an error response");
  }
  return response.message;
}

function buildDirectories(): string[] {
  const buildRoot = path.join(tempDir, "build_deps");
  return fs.existsSync(buildRoot) ? fs.readdirSync(buildRoot) : [];
}

function archiveManifest(archivePath: string): {
  cache: string;
  dependencies: Record<string, string>;
  name: string;
} {
  const zip = new AdmZip(archivePath);
  const contents = zip.readAsText("node_modules/example/build.json");
  return JSON.parse(contents) as {
    cache: string;
    dependencies: Record<string, string>;
    name: string;
  };
}

async function writeFakeNpm(
  options: {
    descendantMarker?: string;
    descendantStartedMarker?: string;
    diagnostic?: string;
    exitCode?: number;
    installDelayMs?: number;
    nodeModulesFileSize?: number;
    nodeModulesSymlinkTarget?: string;
  } = {},
): Promise<void> {
  const npmPath = path.join(tempDir, "bin", "npm");
  const exitCode = options.exitCode ?? 0;
  const diagnostic = JSON.stringify(options.diagnostic ?? "");
  const descendantMarker = JSON.stringify(options.descendantMarker ?? "");
  const descendantStartedMarker = JSON.stringify(
    options.descendantStartedMarker ?? "",
  );
  const installDelayMs = options.installDelayMs ?? 100;
  const nodeModulesFileSize = options.nodeModulesFileSize ?? 0;
  const nodeModulesSymlinkTarget = JSON.stringify(
    options.nodeModulesSymlinkTarget ?? "",
  );
  const script = `#!/usr/bin/env node
const { spawn } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");
const descendantMarker = ${descendantMarker};
const descendantStartedMarker = ${descendantStartedMarker};
if (descendantMarker !== "") {
  const descendant = spawn(process.execPath, [
    "-e",
    'setTimeout(() => require("node:fs").writeFileSync(process.argv[1], "late"), 1000)',
    descendantMarker,
  ], { stdio: "ignore" });
  descendant.unref();
  fs.writeFileSync(descendantStartedMarker, "started");
}
setTimeout(() => {
  const diagnostic = ${diagnostic};
  if (diagnostic !== "") process.stderr.write(diagnostic);
  if (${exitCode} !== 0) process.exit(${exitCode});
  const nodeModulesSymlinkTarget = ${nodeModulesSymlinkTarget};
  if (nodeModulesSymlinkTarget !== "") {
    fs.symlinkSync(nodeModulesSymlinkTarget, "node_modules", "dir");
    return;
  }
  const packageJson = JSON.parse(fs.readFileSync("package.json", "utf8"));
  const moduleDir = path.join("node_modules", "example");
  fs.mkdirSync(moduleDir, { recursive: true });
  fs.writeFileSync(path.join(moduleDir, "build.json"), JSON.stringify({
    cache: process.env.NPM_CONFIG_CACHE,
    dependencies: packageJson.dependencies,
    name: packageJson.name,
  }));
  if (${nodeModulesFileSize} > 0) {
    const sparsePackageData = path.join(moduleDir, "sparse-package-data");
    fs.writeFileSync(sparsePackageData, "");
    fs.truncateSync(sparsePackageData, ${nodeModulesFileSize});
  }
}, ${installDelayMs});
`;
  await fs.promises.writeFile(npmPath, script, { mode: 0o755 });
}
