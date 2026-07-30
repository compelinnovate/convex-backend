import { Command, Option } from "@commander-js/extra-typings";
import { invoke } from "./executor";
import { v4 as uuidv4 } from "uuid";
import { log, setDebugLogging } from "./log";
import os from "node:os";
import http from "node:http";
import express, { Request, Response } from "express";
import { lockDiagnosticReportConfiguration } from "./diagnostic_report";
import { extractErrorMessage, getPrepareStackTraceStats } from "./errors";
import { startMainThreadProfiler } from "./main_thread_profiler";
import { getPackageCacheStats } from "./source_package";
import { prepareSourcePackage } from "./prepare";

const DEFAULT_PORT = 3002;

async function startServer(
  listenTarget: number | { path: string },
  debug: boolean,
  tempdir: string,
  diagnosticControlPath: string | undefined,
  diagnosticProfilePath: string | undefined,
  diagnosticProfileDurationMs: number,
) {
  setDebugLogging(debug);
  if (
    (diagnosticControlPath === undefined) !==
    (diagnosticProfilePath === undefined)
  ) {
    throw new Error(
      "Diagnostic control and profile paths must be configured together",
    );
  }
  if (
    diagnosticControlPath !== undefined &&
    diagnosticProfilePath !== undefined
  ) {
    lockDiagnosticReportConfiguration();
    void startMainThreadProfiler(
      diagnosticControlPath,
      diagnosticProfilePath,
      diagnosticProfileDurationMs,
    ).catch(() => {
      // Profiling is best-effort evidence. A failed diagnostic worker must not
      // delay or prevent the supervisor from starting a replacement executor.
    });
  }
  const app = express();
  app.use(express.json({ limit: "6MB" })); // 5 MiB for args (https://docs.convex.dev/production/state/limits#functions) + extra space

  // Override os.tmpdir to use the provided tempdir
  os.tmpdir = () => tempdir;
  log("Node executor tempdir configured");

  // Add health check endpoint
  app.get("/health", (_req: Request, res: Response) => {
    res.json({
      status: "ok",
      packageCache: getPackageCacheStats(),
      stackTrace: getPrepareStackTraceStats(),
    });
  });

  app.post("/prepare", async (req: Request, res: Response) => {
    try {
      await prepareSourcePackage(req.body);
      res.json({ type: "success" });
    } catch (_error: unknown) {
      // Package failure is a typed protocol outcome. Keep HTTP errors for
      // failures that prevent the endpoint from producing that response.
      res.json({
        type: "error",
        message: "Package preparation failed",
      });
    }
  });

  app.post("/invoke", async (req: Request, res: Response) => {
    try {
      const request = req.body;
      request.requestId = uuidv4();

      // Set up streaming response
      res.setHeader("Content-Type", "application/x-ndjson");
      res.setHeader("Transfer-Encoding", "chunked");

      await invoke(request, res);
    } catch (err: unknown) {
      const message = extractErrorMessage(err) || "Internal server error";
      // If we haven't written anything yet, send an error response
      if (!res.headersSent) {
        res.status(500).json({
          type: "error",
          message,
        });
      } else {
        // If we've already started streaming, try to write an error line
        res.write(
          JSON.stringify({
            type: "error",
            message,
          }) + "\n",
        );
      }
    } finally {
      res.end();
    }
  });

  const server = http.createServer(app);
  server.listen(listenTarget, () => {
    const addr = server.address();
    const addrStr =
      typeof addr === "object" && addr
        ? `port ${addr.port}`
        : typeof listenTarget === "object" && "path" in listenTarget
          ? "IPC endpoint"
          : String(listenTarget);
    log(`Node executor server listening on ${addrStr}`);
  });
}

const program = new Command();
program
  .name("node-executor")
  .description(
    "node-executor runs an HTTP server for executing actions locally",
  )
  .usage("command [options]")
  .option("--debug", "print debug output", false)
  .option("--port <number>", "port to listen on", DEFAULT_PORT.toString())
  .addOption(
    new Option(
      "--ipc-path <path>",
      "listen on a Unix domain socket or Windows named pipe path",
    ).conflicts(["port"]),
  )
  .option(
    "--tempdir <path>",
    "temporary directory to use for downloading code and dependencies",
    "",
  )
  .option(
    "--diagnostic-control-path <path>",
    "private IPC path used to trigger a bounded main-thread CPU profile",
  )
  .option(
    "--diagnostic-profile-path <path>",
    "exclusive output path for the bounded main-thread CPU profile",
  )
  .option(
    "--diagnostic-profile-duration-ms <number>",
    "bounded main-thread CPU profile duration in milliseconds",
    "4000",
  )
  .action(async (options) => {
    const listenTarget =
      options.ipcPath !== undefined
        ? { path: options.ipcPath }
        : parseInt(options.port, 10);
    await startServer(
      listenTarget,
      options.debug,
      options.tempdir,
      options.diagnosticControlPath,
      options.diagnosticProfilePath,
      Number(options.diagnosticProfileDurationMs),
    );
  });

program.parseAsync(process.argv);
