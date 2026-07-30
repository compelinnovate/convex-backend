const lockedReportSettings = [
  "directory",
  "filename",
  "excludeEnv",
  "excludeNetwork",
  "signal",
  "reportOnSignal",
] as const;

export function lockDiagnosticReportConfiguration(): void {
  const report = process.report;
  const settings = lockedReportSettings.map((setting) => {
    const descriptor = Object.getOwnPropertyDescriptor(report, setting);
    if (
      descriptor?.get === undefined ||
      descriptor.set === undefined ||
      !descriptor.configurable
    ) {
      throw new Error(
        `Node diagnostic report setting ${setting} cannot be locked`,
      );
    }
    const configuredValue: unknown = Reflect.get(report, setting);
    return { configuredValue, descriptor, setting };
  });
  const reportDescriptor = Object.getOwnPropertyDescriptor(process, "report");
  if (reportDescriptor?.configurable !== true) {
    throw new Error("Node diagnostic report object cannot be locked");
  }

  // Validate and read every native accessor before making any irreversible
  // descriptor changes.
  for (const { configuredValue, descriptor, setting } of settings) {
    Object.defineProperty(report, setting, {
      configurable: false,
      enumerable: descriptor.enumerable,
      get: () => configuredValue,
      set: () => {
        throw new Error(
          `Node diagnostic report setting ${setting} is managed by the executor`,
        );
      },
    });
  }

  // User actions share this process. Publish the configured report object as a
  // fixed property so an action cannot replace it and bypass the locked native
  // report settings before a watchdog signal arrives.
  Object.defineProperty(process, "report", {
    configurable: false,
    enumerable: reportDescriptor.enumerable,
    value: report,
    writable: false,
  });
}
