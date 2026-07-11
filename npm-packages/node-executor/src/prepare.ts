import { z } from "zod";
import { acquireSourcePackage } from "./source_package";

const packageSchema = z
  .object({
    uri: z.string(),
    key: z.string(),
    sha256: z.string(),
  })
  .strict();

const sourcePackageSchema = z
  .object({
    uri: z.string(),
    key: z.string(),
    sha256: z.string(),
    bundled_source: packageSchema,
    external_deps: packageSchema.nullish(),
  })
  .strict();

const preparationRequestSchema = z
  .object({
    sourcePackage: sourcePackageSchema,
  })
  .strict();

export async function prepareSourcePackage(request: unknown): Promise<void> {
  const { sourcePackage } = preparationRequestSchema.parse(request);
  const lease = await acquireSourcePackage(sourcePackage);
  await lease.release();
}
