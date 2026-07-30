import { Worker } from "node:worker_threads";

const PROFILER_STARTUP_TIMEOUT_MS = 5_000;
const MAX_PROFILE_DURATION_MS = 10_000;

const profilerWorkerSource = String.raw`
const fs = require("node:fs");
const inspector = require("node:inspector");
const net = require("node:net");
const { parentPort, workerData } = require("node:worker_threads");

const PROFILE_COMMAND = Buffer.from("profile\n");
const CONTROL_REQUEST_TIMEOUT_MS = 1_000;
const PROFILE_COMPLETION_GRACE_MS = 2_000;
const CONTROL_RESPONSE_GRACE_MS = 250;
const MAX_PROFILE_BYTES = 32 * 1024 * 1024;
let profileStarted = false;
let startupReported = false;

function respond(socket, outcome) {
  if (!socket.destroyed) {
    socket.end(outcome + "\n");
  }
}

function reportStartup(outcome) {
  if (startupReported) {
    return;
  }
  startupReported = true;
  parentPort.postMessage(outcome);
}

function writeProfile(socket, encoded) {
  fs.open(workerData.outputPath, "wx", 0o600, (openError, fd) => {
    if (openError) {
      respond(socket, "write_failed");
      return;
    }
    // User actions share the process and can change its umask after startup.
    // Set the descriptor mode explicitly so Rust can always validate the
    // generation-local source as private.
    fs.fchmod(fd, 0o600, (chmodError) => {
      if (chmodError) {
        fs.close(fd, () => {
          fs.unlink(workerData.outputPath, () =>
            respond(socket, "write_failed"),
          );
        });
        return;
      }
      fs.writeFile(fd, encoded, (writeError) => {
        fs.close(fd, (closeError) => {
          if (!writeError && !closeError) {
            respond(socket, "completed");
            return;
          }
          // Only unlink after this worker successfully created the path. An
          // exclusive-open failure must never remove a pre-existing artifact.
          fs.unlink(workerData.outputPath, () =>
            respond(socket, "write_failed"),
          );
        });
      });
    });
  });
}

function startProfile(socket) {
  const session = new inspector.Session();
  let finished = false;
  let stopTimer;
  let completionTimer;
  let timeoutOutcome = "enable_failed";

  function closeSession() {
    if (finished) {
      return false;
    }
    finished = true;
    clearTimeout(stopTimer);
    clearTimeout(completionTimer);
    session.disconnect();
    return true;
  }

  function fail(outcome) {
    if (closeSession()) {
      respond(socket, outcome);
    }
  }

  function complete(encoded) {
    if (closeSession()) {
      writeProfile(socket, encoded);
    }
  }

  try {
    session.connectToMainThread();
  } catch {
    respond(socket, "enable_failed");
    return;
  }
  // Inspector callbacks normally run in this Worker even while the main
  // thread is blocked. Bound the session as well as the control socket in case
  // an Inspector operation itself never answers and the main thread recovers.
  completionTimer = setTimeout(
    () => fail(timeoutOutcome),
    workerData.durationMs + PROFILE_COMPLETION_GRACE_MS,
  );
  session.post("Profiler.enable", (enableError) => {
    if (finished) {
      return;
    }
    if (enableError) {
      fail("enable_failed");
      return;
    }
    timeoutOutcome = "start_failed";
    session.post("Profiler.start", (startError) => {
      if (finished) {
        return;
      }
      if (startError) {
        fail("start_failed");
        return;
      }
      timeoutOutcome = "stop_failed";
      stopTimer = setTimeout(() => {
        session.post("Profiler.stop", (stopError, result) => {
          if (finished) {
            return;
          }
          if (stopError) {
            fail("stop_failed");
            return;
          }
          let encoded;
          try {
            encoded = JSON.stringify(result.profile);
          } catch {
            fail("write_failed");
            return;
          }
          if (typeof encoded !== "string") {
            fail("write_failed");
            return;
          }
          if (Buffer.byteLength(encoded) > MAX_PROFILE_BYTES) {
            fail("profile_too_large");
            return;
          }
          complete(encoded);
        });
      }, workerData.durationMs);
    });
  });
}

const server = net.createServer({ allowHalfOpen: true }, (socket) => {
  socket.on("error", () => socket.destroy());
  socket.setTimeout(CONTROL_REQUEST_TIMEOUT_MS, () => socket.destroy());

  if (profileStarted) {
    respond(socket, "already_started");
    return;
  }

  let matchedCommandBytes = 0;
  socket.on("data", (chunk) => {
    if (profileStarted) {
      // This socket can have connected before another client claimed the
      // attempt. Stop consuming an arbitrary trailing request stream once the
      // bounded outcome is known.
      socket.removeAllListeners("data");
      respond(socket, "already_started");
      return;
    }
    for (const byte of chunk) {
      if (
        matchedCommandBytes >= PROFILE_COMMAND.length ||
        byte !== PROFILE_COMMAND[matchedCommandBytes]
      ) {
        socket.destroy();
        return;
      }
      matchedCommandBytes += 1;
    }
    if (matchedCommandBytes !== PROFILE_COMMAND.length) {
      return;
    }
  });

  socket.on("end", () => {
    if (profileStarted) {
      respond(socket, "already_started");
      return;
    }
    if (matchedCommandBytes !== PROFILE_COMMAND.length) {
      socket.destroy();
      return;
    }
    // Claim the one attempt only after receiving the complete fixed command.
    // Waiting for EOF makes validity independent of stream chunk boundaries.
    // Empty, partial, trailing, and malformed local clients must not consume it.
    profileStarted = true;
    socket.removeAllListeners("data");
    socket.setTimeout(
      workerData.durationMs +
        PROFILE_COMPLETION_GRACE_MS +
        CONTROL_RESPONSE_GRACE_MS,
      () => socket.destroy(),
    );
    startProfile(socket);
  });
});

server.on("error", () => {
  server.close();
  reportStartup("startup_failed");
});
server.listen(workerData.controlPath, () => {
  if (process.platform === "win32") {
    reportStartup("ready");
    return;
  }
  fs.chmod(workerData.controlPath, 0o600, (error) => {
    if (error || !server.listening) {
      server.close();
      reportStartup("startup_failed");
      return;
    }
    reportStartup("ready");
  });
});
`;

let profilerWorker: Worker | undefined;

export async function startMainThreadProfiler(
  controlPath: string,
  outputPath: string,
  durationMs: number,
): Promise<void> {
  if (profilerWorker !== undefined) {
    throw new Error("Main-thread profiler worker was started twice");
  }
  if (
    !Number.isSafeInteger(durationMs) ||
    durationMs <= 0 ||
    durationMs > MAX_PROFILE_DURATION_MS
  ) {
    throw new Error(
      `Main-thread profiler duration must be an integer from 1 to ${MAX_PROFILE_DURATION_MS}`,
    );
  }

  const worker = new Worker(profilerWorkerSource, {
    eval: true,
    workerData: { controlPath, outputPath, durationMs },
  });
  // Publish the owner before awaiting readiness so concurrent starts cannot
  // create two workers and race to bind the same or different control paths.
  profilerWorker = worker;
  try {
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error("Main-thread profiler worker startup timed out"));
      }, PROFILER_STARTUP_TIMEOUT_MS);
      worker.once("error", () => {
        clearTimeout(timeout);
        reject(new Error("Main-thread profiler worker failed during startup"));
      });
      worker.once("message", (message: unknown) => {
        clearTimeout(timeout);
        if (message === "ready") {
          resolve();
        } else {
          reject(
            new Error("Main-thread profiler worker failed during startup"),
          );
        }
      });
      worker.once("exit", () => {
        clearTimeout(timeout);
        reject(new Error("Main-thread profiler worker exited during startup"));
      });
    });
  } catch (error) {
    profilerWorker = undefined;
    await worker.terminate();
    throw error;
  }
  worker.unref();
}
