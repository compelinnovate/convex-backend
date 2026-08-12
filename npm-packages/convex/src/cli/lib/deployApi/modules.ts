import { z } from "zod";
import { looseObject } from "./utils.js";

export const moduleEnvironment = z.union([
  z.literal("isolate"),
  z.literal("node"),
  z.string().regex(/^node:pool:(?!default$)[a-z][a-z0-9_]{0,31}$/),
]);
export type ModuleEnvironment = z.infer<typeof moduleEnvironment>;

const nodePoolName = z.string().regex(/^(?!default$)[a-z][a-z0-9_]{0,31}$/);
const validateModulePoolMetadata = (
  value: { environment: ModuleEnvironment; nodePool?: string | undefined },
  ctx: z.RefinementCtx,
) => {
  const environmentPool = value.environment.startsWith("node:pool:")
    ? value.environment.slice("node:pool:".length)
    : undefined;
  if (value.nodePool !== undefined && environmentPool !== value.nodePool) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ["nodePool"],
      message: "Node pool metadata must match the module environment",
    });
  }
};

export const moduleConfig = looseObject({
  path: z.string(),
  source: z.string(),
  sourceMap: z.optional(z.string()),
  environment: moduleEnvironment,
  nodePool: z.optional(nodePoolName),
}).superRefine(validateModulePoolMetadata);
export type ModuleConfig = z.infer<typeof moduleConfig>;

export const moduleHashConfig = looseObject({
  path: z.string(),
  environment: moduleEnvironment,
  nodePool: z.optional(nodePoolName),
  sha256: z.string(),
}).superRefine(validateModulePoolMetadata);
export type ModuleHashConfig = z.infer<typeof moduleHashConfig>;

export const nodeDependency = looseObject({
  name: z.string(),
  version: z.string(),
});
export type NodeDependency = z.infer<typeof nodeDependency>;

export const udfConfig = looseObject({
  serverVersion: z.string(),
  // RNG seed encoded as Convex bytes in JSON.
  importPhaseRngSeed: z.any(),
  // Timestamp encoded as a Convex Int64 in JSON.
  importPhaseUnixTimestamp: z.any(),
});
export type UdfConfig = z.infer<typeof udfConfig>;

export const sourcePackage = z.any();
export type SourcePackage = z.infer<typeof sourcePackage>;

export const visibility = z.union([
  looseObject({ kind: z.literal("public") }),
  looseObject({ kind: z.literal("internal") }),
]);
export type Visibility = z.infer<typeof visibility>;

export const analyzedFunction = looseObject({
  name: z.string(),
  pos: z.any(),
  udfType: z.union([
    z.literal("Query"),
    z.literal("Mutation"),
    z.literal("Action"),
  ]),
  visibility: z.nullable(visibility),
  args: z.nullable(z.string()),
  returns: z.nullable(z.string()),
});
export type AnalyzedFunction = z.infer<typeof analyzedFunction>;

export const analyzedModule = looseObject({
  functions: z.array(analyzedFunction),
  httpRoutes: z.any(),
  cronSpecs: z.any(),
  sourceMapped: z.any(),
});
export type AnalyzedModule = z.infer<typeof analyzedModule>;
