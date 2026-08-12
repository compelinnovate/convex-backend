import { expect, test } from "vitest";
import { moduleConfig, moduleHashConfig } from "./modules.js";

const moduleMetadata = {
  path: "consumer.js",
  source: "export const run = 1;",
  environment: "node:pool:consumer",
};

test("module schemas validate Node pool metadata agreement", () => {
  expect(moduleConfig.safeParse(moduleMetadata).success).toBe(true);
  expect(
    moduleConfig.safeParse({ ...moduleMetadata, nodePool: "consumer" }).success,
  ).toBe(true);
  expect(
    moduleConfig.safeParse({ ...moduleMetadata, nodePool: "other" }).success,
  ).toBe(false);
  expect(
    moduleHashConfig.safeParse({
      path: moduleMetadata.path,
      environment: moduleMetadata.environment,
      nodePool: "other",
      sha256: "hash",
    }).success,
  ).toBe(false);
});

test("module schemas reject reserved Node pool names", () => {
  expect(
    moduleConfig.safeParse({
      ...moduleMetadata,
      environment: "node:pool:default",
    }).success,
  ).toBe(false);
  expect(
    moduleConfig.safeParse({ ...moduleMetadata, nodePool: "default" }).success,
  ).toBe(false);
});
