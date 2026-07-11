import path from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";

export interface FrameData {
  typeName: string | null;
  functionName: string | null;
  methodName: string | null;
  fileName: string | null;
  lineNumber: number | null;
  columnNumber: number | null;
  evalOrigin: string | null;
  isToplevel: boolean | null;
  isEval: boolean;
  isNative: boolean;
  isConstructor: boolean;
  isAsync: boolean;
  isPromiseAll: boolean;
  promiseIndex: number | null;
}

type ExtendedCallSite = NodeJS.CallSite & {
  isAsync(): boolean;
  isPromiseAll(): boolean;
  getPromiseIndex(): number | null;
};

// V8 resolves prepareStackTrace through the current global Error constructor.
// Retain the pristine constructor so user code cannot redirect hook ownership
// by replacing globalThis.Error.
const executorErrorConstructor = Error;
const userModulesDirs = new Map<string, number>();
const errorFrames = new WeakMap<Error, FrameData[]>();
const stackTraceStats = {
  invocations: 0,
  framesProcessed: 0,
  durationMs: 0,
};

export function getPrepareStackTraceStats() {
  return {
    registeredRoots: userModulesDirs.size,
    ...stackTraceStats,
  };
}

function registeredFrameFileName(fileName: string): string | null {
  let normalizedFileName: string;
  try {
    if (fileName.startsWith("file:")) {
      const fileUrl = new URL(fileName);
      fileUrl.search = "";
      normalizedFileName = path.normalize(fileURLToPath(fileUrl));
    } else {
      normalizedFileName = path.normalize(fileName.split("?", 1)[0]);
    }
  } catch {
    return null;
  }

  // Every registered package root ends in `modules`. Derive that root from
  // the frame path and verify it directly instead of scanning deployment
  // history for a matching prefix.
  const modulesMarker = `${path.sep}modules${path.sep}`;
  let markerIndex = normalizedFileName.indexOf(modulesMarker);
  let modulesDir: string | null = null;
  while (markerIndex !== -1) {
    const candidate = normalizedFileName.slice(
      0,
      markerIndex + modulesMarker.length - path.sep.length,
    );
    if (userModulesDirs.has(candidate)) {
      modulesDir = candidate;
      break;
    }
    markerIndex = normalizedFileName.indexOf(
      modulesMarker,
      markerIndex + modulesMarker.length,
    );
  }
  if (modulesDir === null) {
    return null;
  }
  const relativeFileName = path.relative(modulesDir, normalizedFileName);
  if (
    relativeFileName === "" ||
    relativeFileName === ".." ||
    relativeFileName.startsWith(`..${path.sep}`) ||
    path.isAbsolute(relativeFileName)
  ) {
    return null;
  }
  return `convex:/user/${relativeFileName.split(path.sep).join("/")}`;
}

// https://v8.dev/docs/stack-trace-api#appendix%3A-stack-trace-format
function formatTraceLine(frame: FrameData) {
  let displayFile = frame.fileName;

  // strip query params used for cachebusting environment
  displayFile = (displayFile || "").replace(/\.js\?.*/, ".js");

  // if it doesn't start with convex:/ or node:/ then it might be
  // bundled file or it might be external.
  // TODO deal with external dependencies (I think node_modules/*)
  if (!displayFile) {
    displayFile = "";
  } else if (displayFile.startsWith("convex:")) {
    // leave it alone
  } else if (displayFile.startsWith("node:")) {
    // leave it alone
  } else {
    displayFile = "bundledFunctions.js";
  }

  const location = frame.fileName
    ? `${displayFile}:${frame.lineNumber}:${frame.columnNumber}`
    : "<unknown location>";

  // TODO [as methodName]

  const func = frame.functionName || frame.methodName || "";

  if (func) {
    return `    at${frame.isAsync ? " async" : ""} ${func} (${location})`;
  } else {
    // When code doesn't have a name called show only the location.
    return `    at ${location}`;
  }
}

const prepareStackTrace: NonNullable<ErrorConstructor["prepareStackTrace"]> = (
  error,
  stackFrames,
) => {
  // This function is called on-demand when the `stack` property of an `Error` is accessed for the first time.
  // See https://v8.dev/docs/stack-trace-api for more details.
  const start = performance.now();
  stackTraceStats.invocations += 1;
  stackTraceStats.framesProcessed += stackFrames.length;
  try {
    const frameData: FrameData[] = stackFrames.map((v8Frame) => {
      const extendedFrame = v8Frame as ExtendedCallSite;
      const originalFileName = v8Frame.getFileName();
      const fileName = originalFileName
        ? (registeredFrameFileName(originalFileName) ?? originalFileName)
        : null;
      return {
        typeName: v8Frame.getTypeName(),
        functionName: v8Frame.getFunctionName(),
        methodName: v8Frame.getMethodName(),
        fileName,
        lineNumber: v8Frame.getLineNumber(),
        columnNumber: v8Frame.getColumnNumber(),
        evalOrigin: v8Frame.getEvalOrigin() ?? null,
        isToplevel: v8Frame.isToplevel(),
        isEval: v8Frame.isEval(),
        isNative: v8Frame.isNative(),
        isConstructor: v8Frame.isConstructor(),
        isAsync: extendedFrame.isAsync(),
        isPromiseAll: extendedFrame.isPromiseAll(),
        promiseIndex: extendedFrame.getPromiseIndex(),
      };
    });
    errorFrames.set(error, frameData);
    // We currently always go through JSON when going over the JS <-> Rust boundary. Eventually we can make this more efficient by accessing the V8 objects directly in Rust.
    const frameJSON = JSON.stringify(frameData);
    // Save the structured frame data on the exception so we can use it from Rust later.
    try {
      Object.defineProperties(error, {
        __frameData: { value: frameJSON, configurable: true },
      });
    } catch {
      // User code can freeze an Error or reserve this compatibility property.
      // The private WeakMap remains authoritative for action and analysis
      // error handling.
    }
    // For now, we don't expose the source mapped stack to userspace: The only way to get a good traceback is to throw an exception and have the Rust layer catch it.
    // After evaluating a UDF and catching its error, the Rust layer loads the source map and does its best to get a good traceback.
    //
    // Some libraries like https://github.com/TooTallNate/proxy-agents/blob/c169ced054272e30d619746c0d0673d0b8337e06/packages/agent-base/src/index.ts#L8-L18 rely
    // on Node.js-formatted stack traces to work. This doesn't require anything be mapped to original sources.
    //
    // TODO find a library to do this properly: once we provide it libraries will depend on it matching Node.js stack traces.

    const errorMessage = extractErrorMessage(error);

    return `Error${errorMessage !== "" ? `: ${errorMessage}` : ""}\n${frameData
      .map((frame) => formatTraceLine(frame))
      .join("\n")}`;
  } finally {
    stackTraceStats.durationMs += performance.now() - start;
  }
};

export function tryInstallPrepareStackTrace(): boolean {
  try {
    if (globalThis.Error !== executorErrorConstructor) {
      return false;
    }
    executorErrorConstructor.prepareStackTrace = prepareStackTrace;
    return executorErrorConstructor.prepareStackTrace === prepareStackTrace;
  } catch {
    return false;
  }
}

export function installPrepareStackTrace() {
  if (!tryInstallPrepareStackTrace()) {
    throw new executorErrorConstructor(
      "Cannot install the executor stack-trace formatter",
    );
  }
}

export function captureErrorFrames(error: unknown): FrameData[] {
  if (!tryInstallPrepareStackTrace()) {
    return [];
  }
  try {
    // `instanceof` can invoke a Proxy's getPrototypeOf trap. Keep every
    // operation on the untrusted thrown value inside this boundary.
    if (!(error instanceof executorErrorConstructor)) {
      return [];
    }
    // Accessing stack invokes prepareStackTrace unless user code already did so.
    void error.stack;
  } catch {
    // User-defined Proxy traps or stack getters must not replace the original
    // action error.
    return [];
  }
  return errorFrames.get(error) ?? [];
}

export function registerPrepareStackTrace(modulesDir: string) {
  const normalizedModulesDir = path.resolve(modulesDir);
  // Install first so a user-defined non-writable hook cannot leave a root
  // registered for a source package whose publication then fails.
  installPrepareStackTrace();
  userModulesDirs.set(
    normalizedModulesDir,
    (userModulesDirs.get(normalizedModulesDir) ?? 0) + 1,
  );
}

export function unregisterPrepareStackTrace(modulesDir: string) {
  const normalizedModulesDir = path.resolve(modulesDir);
  const registrations = userModulesDirs.get(normalizedModulesDir);
  if (registrations === undefined) {
    throw new executorErrorConstructor(
      "Cannot unregister an unknown stack-trace root",
    );
  }
  if (registrations === 1) {
    userModulesDirs.delete(normalizedModulesDir);
  } else {
    userModulesDirs.set(normalizedModulesDir, registrations - 1);
  }
}

export function extractErrorName(e: unknown): string {
  try {
    if (typeof e === "object" && e !== null && "name" in e) {
      const name = (e as { name?: unknown }).name;
      return typeof name === "string" ? name : "";
    }
  } catch {
    // A thrown Proxy can reject property access.
  }
  return "";
}

// Extract an error message from an exception thrown by untrusted source.
export function extractErrorMessage(e: unknown): string {
  if (e === null || e === undefined) {
    return "unknown error";
  }

  try {
    const errorLike = e as {
      message?: unknown;
      toString?: () => unknown;
    };
    const message = errorLike.message;
    const messageLike = message as { toString?: () => unknown } | null;
    if (typeof messageLike?.toString === "function") {
      const errorMessage = messageLike.toString();
      // Make sure toString() returns a string.
      if (typeof errorMessage === "string") {
        return errorMessage;
      }
    } else if (typeof errorLike.toString === "function") {
      const errorMessage = errorLike.toString();
      // Make sure toString() returns a string.
      if (typeof errorMessage === "string") {
        return errorMessage;
      }
    }
    return "unknown error";
  } catch {
    // toString threw an error?!
    return "unknown error";
  }
}
