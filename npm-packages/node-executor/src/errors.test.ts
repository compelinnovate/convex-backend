import * as fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { afterEach, expect, test } from "vitest";

import {
  captureErrorFrames,
  FrameData,
  getPrepareStackTraceStats,
  installPrepareStackTrace,
  registerPrepareStackTrace,
  tryInstallPrepareStackTrace,
  unregisterPrepareStackTrace,
} from "./errors";

const tempDirs: string[] = [];
const registrations = new Map<string, number>();

afterEach(async () => {
  for (const [modulesDir, count] of registrations) {
    for (let registration = 0; registration < count; registration += 1) {
      unregisterPrepareStackTrace(modulesDir);
    }
  }
  registrations.clear();
  await Promise.all(
    tempDirs.splice(0).map((dir) => {
      return fs.promises.rm(dir, {
        recursive: true,
        force: true,
      });
    }),
  );
});

test("concurrent package registrations retain each source-map root", async () => {
  const first = await createModuleReturningError("first");
  const second = await createModuleReturningError("second");

  registerRoot(first.modulesDir);
  const firstError = await first.loadError();
  registerRoot(second.modulesDir);
  const secondError = await second.loadError();

  expect(firstFrameFileName(firstError)).toBe("convex:/user/action.mjs");
  expect(firstFrameFileName(secondError)).toBe("convex:/user/action.mjs");
});

test("an evicted package root is removed from stack-frame mapping", async () => {
  const pkg = await createModuleReturningError("evicted");

  registerRoot(pkg.modulesDir);
  const error = await pkg.loadError();
  unregisterRoot(pkg.modulesDir);

  expect(firstFrameFileName(error)).not.toBe("convex:/user/action.mjs");
});

test("stack roots remain registered until the final owner releases them", async () => {
  const pkg = await createModuleReturningError("shared");

  registerRoot(pkg.modulesDir);
  registerRoot(pkg.modulesDir);
  unregisterRoot(pkg.modulesDir);

  expect(getPrepareStackTraceStats().registeredRoots).toBe(1);
  expect(firstFrameFileName(await pkg.loadError())).toBe(
    "convex:/user/action.mjs",
  );
});

test("invocation setup restores the formatter without adding a root owner", async () => {
  const pkg = await createModuleReturningError("restored-formatter");
  registerRoot(pkg.modulesDir);
  Error.prepareStackTrace = () => "replaced by user code";

  installPrepareStackTrace();

  expect(getPrepareStackTraceStats().registeredRoots).toBe(1);
  expect(firstFrameFileName(await pkg.loadError())).toBe(
    "convex:/user/action.mjs",
  );
});

test("user-controlled Error metadata cannot break or spoof stack capture", async () => {
  const pkg = await createModuleReturningError("protected-frame-data");
  registerRoot(pkg.modulesDir);

  const frozenError = Object.freeze(await pkg.loadError());
  expect(firstFrameFileName(frozenError)).toBe("convex:/user/action.mjs");

  const metadataError = await pkg.loadError();
  Object.defineProperty(metadataError, "__frameData", {
    value: "user-controlled",
    configurable: false,
  });
  expect(firstFrameFileName(metadataError)).toBe("convex:/user/action.mjs");
});

test("a thrown Proxy cannot break stack capture", () => {
  const proxyError = new Proxy(new Error("test"), {
    getPrototypeOf() {
      throw new Error("user-controlled prototype trap");
    },
  });

  expect(captureErrorFrames(proxyError)).toEqual([]);
});

test("nested modules path components do not hide the registered package root", async () => {
  const pkg = await createModuleReturningError(
    "nested-modules",
    "nested/modules/action.mjs",
  );
  registerRoot(pkg.modulesDir);

  expect(firstFrameFileName(await pkg.loadError())).toBe(
    "convex:/user/nested/modules/action.mjs",
  );
});

test("a non-writable formatter is detected without throwing", () => {
  const originalDescriptor = Object.getOwnPropertyDescriptor(
    Error,
    "prepareStackTrace",
  );
  try {
    Object.defineProperty(Error, "prepareStackTrace", {
      value: () => "user formatter",
      writable: false,
      configurable: true,
    });
    expect(tryInstallPrepareStackTrace()).toBe(false);
  } finally {
    if (originalDescriptor === undefined) {
      Reflect.deleteProperty(Error, "prepareStackTrace");
    } else {
      Object.defineProperty(Error, "prepareStackTrace", originalDescriptor);
    }
  }
  expect(tryInstallPrepareStackTrace()).toBe(true);
});

test("a replaced global Error constructor is detected", () => {
  const originalErrorConstructor = globalThis.Error;
  try {
    globalThis.Error = EvalError;
    expect(tryInstallPrepareStackTrace()).toBe(false);
  } finally {
    globalThis.Error = originalErrorConstructor;
  }
  expect(tryInstallPrepareStackTrace()).toBe(true);
});

async function createModuleReturningError(
  packageName: string,
  relativeModulePath = "action.mjs",
): Promise<{
  modulesDir: string;
  loadError: () => Promise<Error>;
}> {
  const packageDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), `node-executor-errors-${packageName}-`),
  );
  tempDirs.push(packageDir);
  const modulesDir = path.join(packageDir, "modules");
  const modulePath = path.join(modulesDir, relativeModulePath);
  await fs.promises.mkdir(path.dirname(modulePath), { recursive: true });
  await fs.promises.writeFile(
    modulePath,
    'export function makeError() { return new Error("test"); }\n',
  );

  return {
    modulesDir,
    loadError: async () => {
      const moduleUrl = pathToFileURL(modulePath);
      moduleUrl.searchParams.set("envHash", "test");
      const module = (await import(moduleUrl.href)) as {
        makeError: () => Error;
      };
      return module.makeError();
    },
  };
}

function firstFrameFileName(error: Error): string | null {
  const frameData: FrameData[] = captureErrorFrames(error);
  return frameData[0].fileName;
}

function registerRoot(modulesDir: string) {
  registerPrepareStackTrace(modulesDir);
  registrations.set(modulesDir, (registrations.get(modulesDir) ?? 0) + 1);
}

function unregisterRoot(modulesDir: string) {
  const count = registrations.get(modulesDir);
  if (count === undefined) {
    throw new Error("Test tried to unregister an unknown stack root");
  }
  unregisterPrepareStackTrace(modulesDir);
  if (count === 1) {
    registrations.delete(modulesDir);
  } else {
    registrations.set(modulesDir, count - 1);
  }
}
