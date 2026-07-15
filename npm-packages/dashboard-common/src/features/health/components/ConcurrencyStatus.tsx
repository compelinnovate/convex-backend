import { useMemo } from "react";
import {
  CrossCircledIcon,
  ExclamationTriangleIcon,
} from "@radix-ui/react-icons";
import {
  formatSchedulerLag,
  SCHEDULER_LAG_CRITICAL_SECONDS,
  SCHEDULER_LAG_WARNING_SECONDS,
  useSchedulerLag,
  useFunctionConcurrency,
} from "@common/lib/appMetrics";
import { ChartData } from "@common/lib/charts/types";

type ConcurrencyIssue = {
  severity: "warning" | "critical";
  message: string;
  type: "scheduler" | "concurrency";
};

export function useConcurrencyStatus(
  showConcurrencyIssues: boolean = true,
  chartCount: number = 3,
): {
  issues: ConcurrencyIssue[];
  closedDescription: React.ReactNode;
  lag: ChartData | null | undefined;
  running: ChartData | null;
  queued: ChartData | null;
} {
  const lag = useSchedulerLag();
  const { queued, running } = useFunctionConcurrency();

  const issues = useMemo(() => {
    const result: ConcurrencyIssue[] = [];

    // Check scheduler status
    if (lag && lag.data.length > 0) {
      const currentLagSeconds = lag.data[lag.data.length - 1].lag;
      const wasBehind = lag.data.some(
        (d) => d.lag !== null && d.lag > SCHEDULER_LAG_WARNING_SECONDS,
      );

      if (
        currentLagSeconds !== null &&
        currentLagSeconds > SCHEDULER_LAG_CRITICAL_SECONDS
      ) {
        result.push({
          severity: "critical",
          message: `Scheduler is ${formatSchedulerLag(
            currentLagSeconds,
          )} behind`,
          type: "scheduler",
        });
      } else if (
        currentLagSeconds !== null &&
        currentLagSeconds > SCHEDULER_LAG_WARNING_SECONDS
      ) {
        result.push({
          severity: "warning",
          message: `Scheduler is ${formatSchedulerLag(
            currentLagSeconds,
          )} behind`,
          type: "scheduler",
        });
      } else if (currentLagSeconds !== null && wasBehind) {
        result.push({
          severity: "warning",
          message: "Scheduler was behind but has recovered",
          type: "scheduler",
        });
      }
    }

    // Report observed queueing without treating it as proof that a configured
    // concurrency limit was reached. The metrics do not include those limits.
    if (showConcurrencyIssues && queued) {
      const queuedData = queued.data as Array<Record<string, number>>;

      for (const lineKey of queued.lineKeys) {
        const functionType = lineKey.name;
        const latestBucketQueued =
          queuedData[queuedData.length - 1]?.[functionType] ?? 0;
        const hasBeenQueued = queuedData.some(
          (d) => (d[functionType] ?? 0) > 0,
        );

        if (latestBucketQueued > 0) {
          result.push({
            severity: "warning",
            message: `Queueing observed for ${functionType} in the latest minute`,
            type: "concurrency",
          });
        } else if (hasBeenQueued) {
          result.push({
            severity: "warning",
            message: `Queueing observed for ${functionType} in the last hour`,
            type: "concurrency",
          });
        }
      }
    }

    return result;
  }, [lag, queued, showConcurrencyIssues]);

  const closedDescription = useMemo(() => {
    // Missing data must not hide an issue established by another series. Wait
    // for every relevant series only before showing the healthy chart count.
    if (
      issues.length === 0 &&
      (!lag || (showConcurrencyIssues && (!queued || !running)))
    ) {
      return null;
    }

    if (issues.length === 0) {
      return (
        <span className="animate-fadeInFromLoading text-xs text-content-secondary">
          {chartCount} charts
        </span>
      );
    }

    const criticalIssues = issues.filter((i) => i.severity === "critical");
    const warningIssues = issues.filter((i) => i.severity === "warning");

    return (
      <span className="flex animate-fadeInFromLoading items-center gap-3 text-xs">
        {criticalIssues.length > 0 && (
          <span className="flex items-center gap-1 text-content-error">
            <CrossCircledIcon className="size-3 min-w-3" />
            {criticalIssues[0].message}
          </span>
        )}
        {warningIssues.length > 0 && (
          <span className="flex items-center gap-1 text-content-warning">
            <ExclamationTriangleIcon className="size-3 min-w-3" />
            {warningIssues[0].message}
          </span>
        )}
      </span>
    );
  }, [chartCount, issues, lag, queued, running, showConcurrencyIssues]);

  return { issues, closedDescription, lag, running, queued };
}
