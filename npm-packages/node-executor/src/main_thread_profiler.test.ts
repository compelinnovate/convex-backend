import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeAll, expect, test } from "vitest";
import { startMainThreadProfiler } from "./main_thread_profiler";

let tempDirectory: string;

beforeAll(async () => {
  tempDirectory = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "node-executor-profiler-"),
  );
});
afterAll(async () => {
  await fs.promises.rm(tempDirectory, { recursive: true, force: true });
});

test.skipIf(process.platform === "win32")(
  "captures a wedged main thread once through private IPC",
  async () => {
    const controlPath = path.join(tempDirectory, "control.sock");
    const profilePath = path.join(tempDirectory, "profile.cpuprofile");
    await startMainThreadProfiler(controlPath, profilePath, 100);

    expect((await fs.promises.stat(controlPath)).mode & 0o777).toBe(0o600);

    const exchangeControl = (
      command: string | readonly string[],
      afterWrite?: () => void,
    ): Promise<string> =>
      new Promise((resolve, reject) => {
        const chunks = typeof command === "string" ? [command] : command;
        const socket = net.createConnection(controlPath);
        let response = "";
        socket.setEncoding("utf8");
        socket.setTimeout(2_000, () => {
          socket.destroy(new Error("Profiler control response timed out"));
        });
        socket.once("error", reject);
        socket.on("data", (chunk) => {
          response += chunk;
        });
        socket.once("close", () => resolve(response.trim()));
        socket.once("connect", () => {
          const writeChunk = (index: number) => {
            socket.write(chunks[index]);
            if (index + 1 < chunks.length) {
              setTimeout(() => writeChunk(index + 1), 10);
            } else {
              socket.end();
              afterWrite?.();
            }
          };
          writeChunk(0);
        });
      });

    await exchangeControl("malformed\n");
    await exchangeControl("profile\njunk");
    await exchangeControl(["profile\n", "junk"]);
    expect(fs.existsSync(profilePath)).toBe(false);

    // Actions share this process and can change its umask after the diagnostic
    // Worker starts. The Worker must still create a Rust-readable private file.
    const originalUmask = process.umask(0o777);
    try {
      let completedWhileMainThreadBlocked = false;
      const outcomes = await Promise.all([
        exchangeControl(["pro", "file\n"], () => {
          const blockedUntil = Date.now() + 500;
          function blockMainThreadForProfilerTest() {
            while (Date.now() < blockedUntil) {
              // Keep the main JavaScript thread busy past the profile duration.
            }
          }
          blockMainThreadForProfilerTest();
          // This synchronous observation runs before the main event loop can
          // process a profiler callback; only the inspector Worker can write it.
          completedWhileMainThreadBlocked = fs.existsSync(profilePath);
        }),
        exchangeControl("profile\n"),
      ]);
      expect(outcomes.sort()).toEqual(["already_started", "completed"]);
      expect(completedWhileMainThreadBlocked).toBe(true);
      expect(await exchangeControl("profile\n")).toBe("already_started");

      const profile = JSON.parse(
        await fs.promises.readFile(profilePath, "utf8"),
      ) as {
        nodes?: Array<{ callFrame?: { functionName?: string } }>;
        startTime?: number;
        endTime?: number;
      };
      expect(profile.nodes?.length).toBeGreaterThan(0);
      expect(
        profile.nodes?.some(
          (node) =>
            node.callFrame?.functionName === "blockMainThreadForProfilerTest",
        ),
      ).toBe(true);
      expect(profile.startTime).toEqual(expect.any(Number));
      expect(profile.endTime).toEqual(expect.any(Number));
      expect((await fs.promises.stat(profilePath)).mode & 0o777).toBe(0o600);
    } finally {
      process.umask(originalUmask);
    }
  },
);
