import path from "path";
import { expect, test } from "vitest";
import { Context } from "./context.js";
import { nodeFs, withTmpDir } from "./fs.js";
import {
  Bundle,
  BundleHash,
  bundle,
  entryPointsByEnvironment,
} from "./index.js";
import { hash, partitionModulesByChanges } from "../cli/lib/components.js";

function testContext(): Context {
  return {
    fs: nodeFs,
    deprecationMessagePrinted: false,
    crash: async ({ printedMessage }) => {
      throw new Error(printedMessage ?? "CLI crashed without a message");
    },
    registerCleanup: () => "test-cleanup",
    removeCleanup: () => async () => {},
    bigBrainAuth: () => null,
    _updateBigBrainAuth: () => {},
  };
}

test("Node pool directives require Node and a valid non-default name", async () => {
  await withTmpDir(async (tmpDir) => {
    const modulePath = path.join(tmpDir.path, "consumer.ts");
    nodeFs.writeUtf8File(
      modulePath,
      '"use node";\n"use node pool:consumer";\nexport const run = 1;\n',
    );
    const entryPoints = await entryPointsByEnvironment(
      testContext(),
      tmpDir.path,
    );
    expect(entryPoints.node).toEqual([modulePath]);
    expect(entryPoints.nodePools).toEqual(new Map([[modulePath, "consumer"]]));

    nodeFs.writeUtf8File(
      modulePath,
      '"use node pool:consumer";\nexport const run = 1;\n',
    );
    await expect(
      entryPointsByEnvironment(testContext(), tmpDir.path),
    ).rejects.toThrow('requires a separate "use node" directive');

    nodeFs.writeUtf8File(
      modulePath,
      '"use node";\n"use node pool:default";\nexport const run = 1;\n',
    );
    await expect(
      entryPointsByEnvironment(testContext(), tmpDir.path),
    ).rejects.toThrow('"default" is reserved');
  });
});

test("only a bundled entry module inherits its declared pool", async () => {
  await withTmpDir(async (tmpDir) => {
    nodeFs.writeUtf8File(
      path.join(tmpDir.path, "shared.ts"),
      "export const shared = () => 1;\n",
    );
    const consumerPath = path.join(tmpDir.path, "consumer.ts");
    nodeFs.writeUtf8File(
      consumerPath,
      '"use node";\n"use node pool:consumer";\nimport { shared } from "./shared";\nexport const run = shared;\n',
    );
    nodeFs.writeUtf8File(
      path.join(tmpDir.path, "ordinary.ts"),
      '"use node";\nimport { shared } from "./shared";\nexport const run = shared;\n',
    );

    const entryPoints = await entryPointsByEnvironment(
      testContext(),
      tmpDir.path,
    );
    const result = await bundle({
      ctx: testContext(),
      dir: tmpDir.path,
      entryPoints: entryPoints.node,
      nodePoolsByEntryPoint: entryPoints.nodePools,
      generateSourceMaps: false,
      platform: "node",
      chunksFolder: path.join("_deps", "node"),
    });

    expect(
      result.modules.find((module) => module.path === "consumer.js"),
    ).toMatchObject({
      environment: "node:pool:consumer",
      nodePool: "consumer",
    });
    expect(
      result.modules.find((module) => module.path === "ordinary.js"),
    ).toMatchObject({ environment: "node" });
    const dependencyChunks = result.modules.filter((module) =>
      module.path.startsWith("_deps/node/"),
    );
    expect(dependencyChunks.length).toBeGreaterThan(0);
    expect(
      dependencyChunks.every(
        (module) =>
          module.environment === "node" && module.nodePool === undefined,
      ),
    ).toBe(true);
  });
});

test("unchanged-module comparison includes the explicit pool", () => {
  const pooled: Bundle = {
    path: "consumer.js",
    source: "export const run = 1;",
    environment: "node:pool:consumer",
    nodePool: "consumer",
  };
  const remote = new Map<string, BundleHash>([
    [
      pooled.path,
      {
        path: pooled.path,
        hash: hash(pooled),
        environment: pooled.environment,
      },
    ],
  ]);

  expect(partitionModulesByChanges([pooled], remote)).toEqual({
    unchangedModuleHashes: [],
    changedModules: [pooled],
  });

  remote.get(pooled.path)!.nodePool = "consumer";
  expect(partitionModulesByChanges([pooled], remote)).toEqual({
    unchangedModuleHashes: [
      {
        path: pooled.path,
        environment: pooled.environment,
        nodePool: "consumer",
        sha256: hash(pooled),
      },
    ],
    changedModules: [],
  });
});
