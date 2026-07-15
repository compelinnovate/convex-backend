import { render, renderHook } from "@testing-library/react";
import {
  useFunctionConcurrency,
  useSchedulerLag,
} from "@common/lib/appMetrics";
import { ChartData } from "@common/lib/charts/types";
import { useConcurrencyStatus } from "./ConcurrencyStatus";

jest.mock("@common/lib/appMetrics", () => ({
  ...jest.requireActual("@common/lib/appMetrics"),
  useFunctionConcurrency: jest.fn(),
  useSchedulerLag: jest.fn(),
}));

const mockUseFunctionConcurrency = jest.mocked(useFunctionConcurrency);
const mockUseSchedulerLag = jest.mocked(useSchedulerLag);

function chartData(functionType: string, values: number[]): ChartData {
  return {
    data: values.map((value, index) => ({
      time: `${index}`,
      [functionType]: value,
    })),
    xAxisKey: "time",
    lineKeys: [
      { key: functionType, name: functionType, color: "var(--chart-line-1)" },
    ],
  };
}

function concurrencyIssues({
  functionType,
  queued,
  running,
}: {
  functionType: string;
  queued: number[];
  running: number[];
}) {
  mockUseFunctionConcurrency.mockReturnValue({
    queued: chartData(functionType, queued),
    running: chartData(functionType, running),
  });

  return renderHook(() => useConcurrencyStatus()).result.current.issues;
}

beforeEach(() => {
  mockUseSchedulerLag.mockReturnValue({
    data: [],
    xAxisKey: "time",
    lineKeys: [{ key: "lag", name: "Lag", color: "var(--brand-yellow)" }],
  });
});

test("reports transient queueing with low running concurrency without claiming a limit hit", () => {
  expect(
    concurrencyIssues({
      functionType: "Actions",
      queued: [0, 1, 0],
      running: [5, 7, 4],
    }),
  ).toEqual([
    {
      severity: "warning",
      message: "Queueing observed for Actions in the last hour",
      type: "concurrency",
    },
  ]);
});

test("keeps high-running queueing visible without claiming a configured-limit hit", () => {
  expect(
    concurrencyIssues({
      functionType: "Queries",
      queued: [0, 3, 0],
      running: [120, 124, 122],
    }),
  ).toEqual([
    {
      severity: "warning",
      message: "Queueing observed for Queries in the last hour",
      type: "concurrency",
    },
  ]);
});

test("reports queueing in the latest minute", () => {
  expect(
    concurrencyIssues({
      functionType: "Mutations",
      queued: [0, 0, 1],
      running: [4, 5, 6],
    }),
  ).toEqual([
    {
      severity: "warning",
      message: "Queueing observed for Mutations in the latest minute",
      type: "concurrency",
    },
  ]);
});

test("keeps the queue warning visible when running concurrency is absent", () => {
  mockUseFunctionConcurrency.mockReturnValue({
    queued: chartData("Actions", [0, 0, 1]),
    running: null,
  });

  const { result } = renderHook(() => useConcurrencyStatus());
  const view = render(<>{result.current.closedDescription}</>);

  expect(
    view.getByText("Queueing observed for Actions in the latest minute"),
  ).toBeVisible();
});

test("keeps the queue warning visible when scheduler lag is absent", () => {
  mockUseSchedulerLag.mockReturnValue(undefined);
  mockUseFunctionConcurrency.mockReturnValue({
    queued: chartData("Actions", [0, 0, 1]),
    running: null,
  });

  const { result } = renderHook(() => useConcurrencyStatus());
  const view = render(<>{result.current.closedDescription}</>);

  expect(
    view.getByText("Queueing observed for Actions in the latest minute"),
  ).toBeVisible();
});

test("keeps a scheduler failure visible when concurrency data is absent", () => {
  mockUseSchedulerLag.mockReturnValue({
    data: [{ time: "0", lag: 360 }],
    xAxisKey: "time",
    lineKeys: [{ key: "lag", name: "Lag", color: "var(--brand-yellow)" }],
  });
  mockUseFunctionConcurrency.mockReturnValue({ queued: null, running: null });

  const { result } = renderHook(() => useConcurrencyStatus());
  const view = render(<>{result.current.closedDescription}</>);

  expect(view.getByText("Scheduler is 6 min behind")).toBeVisible();
});

test("reports sub-minute scheduler lag without rounding it to a minute", () => {
  mockUseSchedulerLag.mockReturnValue({
    data: [{ time: "0", lag: 45 }],
    xAxisKey: "time",
    lineKeys: [{ key: "lag", name: "Lag", color: "var(--brand-yellow)" }],
  });
  mockUseFunctionConcurrency.mockReturnValue({ queued: null, running: null });

  const { result } = renderHook(() => useConcurrencyStatus());
  const view = render(<>{result.current.closedDescription}</>);

  expect(view.getByText("Scheduler is 45 s behind")).toBeVisible();
});

test("does not infer scheduler recovery from a missing latest sample", () => {
  mockUseSchedulerLag.mockReturnValue({
    data: [
      { time: "0", lag: 45 },
      { time: "1", lag: null },
    ],
    xAxisKey: "time",
    lineKeys: [{ key: "lag", name: "Lag", color: "var(--brand-yellow)" }],
  });
  mockUseFunctionConcurrency.mockReturnValue({ queued: null, running: null });

  expect(
    renderHook(() => useConcurrencyStatus()).result.current.issues,
  ).toEqual([]);
});

test("reports no concurrency issue when no queueing was observed", () => {
  expect(
    concurrencyIssues({
      functionType: "Actions (Node)",
      queued: [0, 0, 0],
      running: [5, 6, 4],
    }),
  ).toEqual([]);
});
