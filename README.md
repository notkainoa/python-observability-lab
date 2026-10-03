# Python observability boundary lab

Two runnable experiments about context that disappears when Python work crosses
a thread boundary. These are technical-writing samples, not commissioned
articles or claims of production deployment.

## Read the samples

1. **[Why an executor breaks your trace tree](articles/trace-context.md)**
   Reproduce missing parent spans, compare two fixes, and test the less obvious
   difference between creating a coroutine and scheduling a task.
2. **[Why queued logs lose their trace IDs](articles/queued-logs.md)**
   Reproduce context loss in a logging listener and move context capture to the
   producer. Keep the example's JSON output limited to an explicit event schema.

## Reproduce

Tested on CPython 3.12.3 with OpenTelemetry SDK 1.45.0. The code uses manual
instrumentation, real Python threads, and an in-memory span exporter. No hosted
service, account, API key, container, or paid resource is needed.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m observability_lab.context_boundaries
.venv/bin/python -m observability_lab.queued_logging
```

The test suite contains **11 tests**. Expected executor results:

| Boundary | Same trace as the request? | Request is the parent? |
| --- | --- | --- |
| `run_in_executor` without propagation | No | No |
| `asyncio.to_thread` | Yes | Yes |
| Executor with a fresh copied context | Yes | Yes |

The logging example prints two JSON records. The first has trace and span IDs;
the second, emitted outside a span, has `null` IDs. Generated IDs differ each run.

## Scope and limitations

- This is a local laboratory, not a benchmark or production incident report.
- No SigNoz deployment, OTLP transport, sampling policy, dashboards, or alerts
  have been tested here.
- No auto-instrumentation package is enabled. Threading instrumentation can
  change the baseline behavior.
- Thread/process cancellation and queue-overflow policies are not implemented
  by this demo. See each article before adapting the snippets.
- The event formatter is an allowlisted example, not a universal privacy or
  secret-redaction solution.

## Authorship

Created for the `notkainoa` account with Factory Droid. **The prose and code were
AI-generated; the agent also executed the local tests and demonstrations.**
No human professional history, employer experience, editorial acceptance, or
endorsement by OpenTelemetry or SigNoz is claimed.

The code and writing samples are provided under the [MIT license](LICENSE).
