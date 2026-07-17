import { describe, expect, test, vi } from "vitest";
import type { QueryCtx } from "../../_generated/server";

vi.mock("../server", () => import("../__mocks__/server"));

import getSchemas, { schemaValidationProgress } from "./getSchemas";

type SchemaState =
  | "pending"
  | "validated"
  | "active"
  | "failed"
  | "overwritten";

type SchemaDocument = {
  _id: string;
  state: { state: SchemaState };
};

type ProgressDocument = {
  schemaId: string;
  numDocsValidated: bigint;
  totalDocs: bigint | null;
};

type Equality = {
  field: string;
  value: unknown;
};

function databaseReader(
  schemas: SchemaDocument[],
  progress: ProgressDocument[],
): QueryCtx["db"] {
  const query = (tableName: string) => ({
    withIndex(
      indexName: string,
      defineRange: (range: {
        eq(field: string, value: unknown): unknown;
      }) => unknown,
    ) {
      let equality: Equality | undefined;
      const range = {
        eq(field: string, value: unknown) {
          equality = { field, value };
          return range;
        },
      };
      defineRange(range);
      return {
        async unique() {
          if (!equality) {
            throw new Error("Expected an exact index equality");
          }
          const { field, value } = equality;
          if (tableName === "_schemas") {
            expect(indexName).toBe("by_state");
            expect(field).toBe("state");
            const matches = schemas.filter(
              (schema) =>
                JSON.stringify(schema.state) === JSON.stringify(value),
            );
            if (matches.length > 1) {
              throw new Error("Expected a unique schema state");
            }
            return matches[0] ?? null;
          }
          if (tableName === "_schema_validation_progress") {
            expect(indexName).toBe("by_schema_id");
            expect(field).toBe("schemaId");
            const matches = progress.filter(
              (progressDocument) => progressDocument.schemaId === value,
            );
            if (matches.length > 1) {
              throw new Error("Expected unique progress for a schema");
            }
            return matches[0] ?? null;
          }
          throw new Error(`Unexpected private system table ${tableName}`);
        },
      };
    },
  });

  return {
    system: { query },
  } as unknown as QueryCtx["db"];
}

async function queryProgress(
  schemas: SchemaDocument[],
  progress: ProgressDocument[],
) {
  return await schemaValidationProgress._handler(
    { db: databaseReader(schemas, progress) } as QueryCtx,
    {},
  );
}

async function querySchemas(schemas: SchemaDocument[]) {
  return await getSchemas._handler(
    { db: databaseReader(schemas, []) } as QueryCtx,
    {},
  );
}

test("getSchemas rejects contradictory in-progress schema states", async () => {
  await expect(
    querySchemas([
      { _id: "pending-schema", state: { state: "pending" } },
      { _id: "validated-schema", state: { state: "validated" } },
    ]),
  ).rejects.toThrow("Unexpectedly found both pending and validated schemas");
});

describe("schemaValidationProgress", () => {
  test("returns progress for the exact pending schema", async () => {
    const result = await queryProgress(
      [
        { _id: "pending-schema", state: { state: "pending" } },
        { _id: "failed-schema", state: { state: "failed" } },
      ],
      [
        {
          schemaId: "failed-schema",
          numDocsValidated: BigInt(99),
          totalDocs: BigInt(100),
        },
        {
          schemaId: "pending-schema",
          numDocsValidated: BigInt(4),
          totalDocs: BigInt(10),
        },
      ],
    );

    expect(result).toEqual({ numDocsValidated: 4, totalDocs: 10 });
  });

  test("preserves a zero total document count", async () => {
    const result = await queryProgress(
      [{ _id: "pending-schema", state: { state: "pending" } }],
      [
        {
          schemaId: "pending-schema",
          numDocsValidated: BigInt(0),
          totalDocs: BigInt(0),
        },
      ],
    );

    expect(result).toEqual({ numDocsValidated: 0, totalDocs: 0 });
  });

  test("rejects contradictory in-progress schema states", async () => {
    await expect(
      queryProgress(
        [
          { _id: "pending-schema", state: { state: "pending" } },
          { _id: "validated-schema", state: { state: "validated" } },
        ],
        [
          {
            schemaId: "pending-schema",
            numDocsValidated: BigInt(4),
            totalDocs: BigInt(10),
          },
        ],
      ),
    ).rejects.toThrow("Unexpectedly found both pending and validated schemas");
  });

  test.each([
    { numDocsValidated: BigInt(-1), totalDocs: BigInt(10) },
    { numDocsValidated: BigInt(1), totalDocs: BigInt(-1) },
  ])("rejects negative stored progress counts", async (storedProgress) => {
    await expect(
      queryProgress(
        [{ _id: "pending-schema", state: { state: "pending" } }],
        [{ schemaId: "pending-schema", ...storedProgress }],
      ),
    ).rejects.toThrow("Schema validation progress counts must be nonnegative");
  });

  test.each([
    {
      numDocsValidated: BigInt(Number.MAX_SAFE_INTEGER) + BigInt(1),
      totalDocs: BigInt(10),
    },
    {
      numDocsValidated: BigInt(1),
      totalDocs: BigInt(Number.MAX_SAFE_INTEGER) + BigInt(1),
    },
  ])(
    "rejects progress counts outside the safe integer range",
    async (storedProgress) => {
      await expect(
        queryProgress(
          [{ _id: "pending-schema", state: { state: "pending" } }],
          [{ schemaId: "pending-schema", ...storedProgress }],
        ),
      ).rejects.toThrow(
        "Schema validation progress counts exceed the safe integer range",
      );
    },
  );

  test("does not return progress belonging to a different schema", async () => {
    const result = await queryProgress(
      [{ _id: "pending-schema", state: { state: "pending" } }],
      [
        {
          schemaId: "stale-schema",
          numDocsValidated: BigInt(9),
          totalDocs: BigInt(10),
        },
      ],
    );

    expect(result).toBeNull();
  });

  test("switches visibility to replacement pending schema progress", async () => {
    const result = await queryProgress(
      [
        { _id: "old-schema", state: { state: "overwritten" } },
        { _id: "new-schema", state: { state: "pending" } },
      ],
      [
        {
          schemaId: "old-schema",
          numDocsValidated: BigInt(9),
          totalDocs: BigInt(10),
        },
        {
          schemaId: "new-schema",
          numDocsValidated: BigInt(1),
          totalDocs: BigInt(20),
        },
      ],
    );

    expect(result).toEqual({ numDocsValidated: 1, totalDocs: 20 });
  });

  test.each(["validated", "failed", "overwritten", "active"] as const)(
    "hides physically retained progress after the schema is no longer pending (%s)",
    async (state) => {
      const result = await queryProgress(
        [{ _id: "non-pending-schema", state: { state } }],
        [
          {
            schemaId: "non-pending-schema",
            numDocsValidated: BigInt(9),
            totalDocs: BigInt(10),
          },
        ],
      );

      expect(result).toBeNull();
    },
  );
});
