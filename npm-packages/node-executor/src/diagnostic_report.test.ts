import { expect, test } from "vitest";
import { lockDiagnosticReportConfiguration } from "./diagnostic_report";

test("locks the watchdog report settings without changing their configured values", () => {
  const report = process.report;
  const configured = {
    directory: report.directory,
    filename: report.filename,
    excludeEnv: Reflect.get(report, "excludeEnv"),
    excludeNetwork: Reflect.get(report, "excludeNetwork"),
    signal: report.signal,
    reportOnSignal: report.reportOnSignal,
  };

  lockDiagnosticReportConfiguration();

  expect(process.report).toBe(report);
  expect({
    directory: report.directory,
    filename: report.filename,
    excludeEnv: Reflect.get(report, "excludeEnv"),
    excludeNetwork: Reflect.get(report, "excludeNetwork"),
    signal: report.signal,
    reportOnSignal: report.reportOnSignal,
  }).toEqual(configured);
  expect(() => {
    report.directory = "changed";
  }).toThrow("Node diagnostic report setting directory is managed");
  expect(() => {
    report.filename = "changed";
  }).toThrow("Node diagnostic report setting filename is managed");
  expect(() => {
    Reflect.set(report, "excludeEnv", !configured.excludeEnv);
  }).toThrow("Node diagnostic report setting excludeEnv is managed");
  expect(() => {
    Reflect.set(report, "excludeNetwork", !configured.excludeNetwork);
  }).toThrow("Node diagnostic report setting excludeNetwork is managed");
  expect(() => {
    report.signal = configured.signal === "SIGUSR2" ? "SIGUSR1" : "SIGUSR2";
  }).toThrow("Node diagnostic report setting signal is managed");
  expect(() => {
    report.reportOnSignal = !configured.reportOnSignal;
  }).toThrow("Node diagnostic report setting reportOnSignal is managed");
  for (const setting of Object.keys(configured)) {
    expect(Object.getOwnPropertyDescriptor(report, setting)?.configurable).toBe(
      false,
    );
  }
  expect(() => {
    Object.defineProperty(process, "report", { value: {} });
  }).toThrow();
  expect(Object.getOwnPropertyDescriptor(process, "report")).toMatchObject({
    configurable: false,
    writable: false,
  });
});
