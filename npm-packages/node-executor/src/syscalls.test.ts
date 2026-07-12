import { expect, test } from "vitest";

import { SyscallsImpl } from "./syscalls";

function makeSyscalls(hasIsolateWorkerAncestor: boolean): SyscallsImpl {
  return new SyscallsImpl(
    { canonicalizedPath: "actions.js", function: "run" },
    "lambda-execute-id",
    "http://127.0.0.1:3210",
    "callback-token",
    null,
    null,
    {
      requestId: "request-id",
      executionId: "execution-id",
      isRoot: false,
      parentScheduledJob: null,
      parentScheduledJobComponentId: null,
      ip: null,
      userAgent: null,
    },
    hasIsolateWorkerAncestor,
    null,
    { name: "local-test", region: null, class: "s16" },
  );
}

test("action callbacks propagate isolate-worker ancestry", () => {
  expect(makeSyscalls(true).headers("1.0")).toMatchObject({
    "Convex-Isolate-Worker-Ancestor": "true",
  });
  expect(makeSyscalls(false).headers("1.0")).not.toHaveProperty(
    "Convex-Isolate-Worker-Ancestor",
  );
});
