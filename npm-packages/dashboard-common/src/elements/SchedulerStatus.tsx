import { cn } from "@ui/cn";
import { HealthCard } from "@common/elements/HealthCard";
import {
  formatSchedulerLag,
  SCHEDULER_LAG_CRITICAL_SECONDS,
  SCHEDULER_LAG_WARNING_SECONDS,
  useSchedulerLag,
} from "@common/lib/appMetrics";
import { ChartForFunctionRate } from "@common/features/health/components/ChartForFunctionRate";
import { ChartData } from "@common/lib/charts/types";

export function SchedulerStatus({
  small = false,
  lag: lagProp,
}: {
  small?: boolean;
  lag?: ChartData | null;
}) {
  const lagFromHook = useSchedulerLag();
  const lag = lagProp ?? lagFromHook;
  const lagData = lag?.data as
    | Array<{ time: string; lag: number | null }>
    | undefined;
  const behindBySeconds = lagData?.[lagData.length - 1]?.lag ?? 0;

  const health =
    behindBySeconds <= SCHEDULER_LAG_WARNING_SECONDS
      ? "healthy"
      : behindBySeconds > SCHEDULER_LAG_CRITICAL_SECONDS
      ? "error"
      : "warning";

  if (small) {
    if (health === "healthy") {
      return null;
    }
    return (
      <div className="flex animate-fadeInFromLoading flex-col place-content-center items-center justify-center text-xs">
        <p
          className={cn("font-semibold", {
            "text-content-warning": health === "warning",
            "text-content-error": health === "error",
          })}
        >
          Overdue
        </p>
        <div className="truncate text-center text-pretty text-content-secondary">
          <div className="flex gap-1">
            <p className="text-content-secondary">
              Scheduling is behind by {formatSchedulerLag(behindBySeconds)}.
            </p>
          </div>
        </div>
      </div>
    );
  }
  return (
    <HealthCard
      title="Scheduler Status"
      tip="The ready-queue lag for scheduled functions. Sustained lag means a pending function remained ready past its target time."
    >
      <ChartForFunctionRate chartData={lag} kind="schedulerStatus" />
    </HealthCard>
  );
}
