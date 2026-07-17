import type { DatabaseReader } from "../../_generated/server";
import { queryPrivateSystem } from "../secretSystemTables";
import { v } from "convex/values";

const maxSafeInteger = BigInt(Number.MAX_SAFE_INTEGER);

type UniqueSchemaState = "pending" | "validated" | "active";

export const getSchemaByState = (
  db: DatabaseReader,
  state: UniqueSchemaState,
) =>
  db
    .query("_schemas")
    .withIndex("by_state", (q) => q.eq("state", { state }))
    .unique();

export default queryPrivateSystem("ViewData")({
  args: { componentId: v.optional(v.union(v.string(), v.null())) },
  handler: async function ({ db }): Promise<{
    active?: string;
    inProgress?: string;
  }> {
    const [active, pending, validated] = await Promise.all([
      getSchemaByState(db, "active"),
      getSchemaByState(db, "pending"),
      getSchemaByState(db, "validated"),
    ]);

    if (pending && validated) {
      throw new Error("Unexpectedly found both pending and validated schemas");
    }

    return {
      active: active?.schema,
      inProgress: pending?.schema ?? validated?.schema,
    };
  },
});

export const schemaValidationProgress = queryPrivateSystem("ViewData")({
  args: { componentId: v.optional(v.union(v.string(), v.null())) },
  handler: async function ({
    db,
  }): Promise<{ numDocsValidated: number; totalDocs: number | null } | null> {
    const [pending, validated] = await Promise.all([
      getSchemaByState(db, "pending"),
      getSchemaByState(db, "validated"),
    ]);
    if (pending && validated) {
      throw new Error("Unexpectedly found both pending and validated schemas");
    }
    if (!pending) {
      return null;
    }
    const schemaValidationProgressDoc = await db
      .query("_schema_validation_progress")
      .withIndex("by_schema_id", (q) => q.eq("schemaId", pending._id))
      .unique();
    if (!schemaValidationProgressDoc) {
      return null;
    }
    if (
      schemaValidationProgressDoc.numDocsValidated < BigInt(0) ||
      (schemaValidationProgressDoc.totalDocs !== null &&
        schemaValidationProgressDoc.totalDocs < BigInt(0))
    ) {
      throw new Error("Schema validation progress counts must be nonnegative");
    }
    if (
      schemaValidationProgressDoc.numDocsValidated > maxSafeInteger ||
      (schemaValidationProgressDoc.totalDocs !== null &&
        schemaValidationProgressDoc.totalDocs > maxSafeInteger)
    ) {
      throw new Error(
        "Schema validation progress counts exceed the safe integer range",
      );
    }
    return {
      numDocsValidated: Number(schemaValidationProgressDoc.numDocsValidated),
      totalDocs:
        schemaValidationProgressDoc.totalDocs !== null
          ? Number(schemaValidationProgressDoc.totalDocs)
          : null,
    };
  },
});
