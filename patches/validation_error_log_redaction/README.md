# Redact Validator Values from External Log Sinks

This patch prevents automatic Convex validator errors from copying function arguments or return
values into durable external log sinks.

## Background

Argument validation constructs a `JsError` whose message starts with
`ArgumentValidationError:`. Return validation constructs a `JsError` whose message starts with
`ReturnsValidationError:`. Both messages include the rejected value and validator details. A
rejected argument can therefore place names, phone numbers, account values, or other sensitive
application data in a function-execution log record.

Each failed function execution can produce two external log events:

- `FunctionExecution`, used by general log sinks; and
- `Exception`, used by exception sinks and the local-file sink.

General sinks share the V1/V2 `LogEvent` serializer. Sentry and PostHog Error Tracking construct
their exception payloads separately. Redacting only the function-execution field leaves the same
validator message available to the exception paths.

## Required Behavior

- V1 function-execution `reason` and V2 function-execution `error_message` replace both automatic
  validator error classes with fixed messages.
- V1/V2 serialized exception messages, Sentry exception messages, and PostHog Error Tracking
  exception messages use the same replacement.
- The replacement preserves `ArgumentValidationError` or `ReturnsValidationError` as a stable
  classification without retaining the rejected value or validator details.
- Non-validation errors retain their existing message and stack formatting.
- The protection is unconditional and does not depend on `SHOW_PII_IN_ERRORS`.
- Console output and application-defined error messages remain application responsibilities.

The classifier intentionally matches the fixed prefixes produced in
`crates/udf/src/validation.rs` and `crates/model/src/modules/function_validators.rs`. A user-defined
error beginning with either reserved prefix receives the same redaction.

## Scope

The patch covers durable log-sink serialization for Axiom, Datadog, webhook, PostHog Logs, Sentry,
PostHog Error Tracking, and the local-file sink. The authenticated CLI/dashboard function-log API
and caller-visible function errors retain upstream behavior. The patch does not alter previously
ingested events.

## Rollout and Rollback

The protection activates automatically when the patched backend image starts. No environment
variable or log-sink reconfiguration is required.

Restoring an upstream image without equivalent redaction restores the validator-value leak. Treat
rollback as unsafe while applications can send sensitive values in function arguments or return
values. Historical removal follows the retention or deletion controls of the destination log
service.

## Verification

The common log-stream tests serialize argument and return validation failures through V1 and V2,
verify the fixed fields, and reject sentinel payload values in the complete serialized objects.
The tests cover both validation classes in V1/V2 exception serialization. They also verify the two
existing ordinary-error formats: function-execution records retain the full `JsError` display,
while exception events and exception sinks retain `JsError.message` with stack frames in their
separate structured fields.

```sh
scripts/run_cargo.sh test -p common log_streaming::tests
scripts/run_cargo.sh clippy -p common --lib --tests -- -D warnings
scripts/run_cargo.sh clippy -p log_streaming --lib --tests -- -D warnings
```

During an upstream rebase, verify that the two validator constructors retain their classified
prefixes and that every external exception sink still obtains its message through
`error_message_for_log_stream`.
