# Why queued logs lose their trace IDs

*A local, AI-authored writing sample with executable tests. Tested on CPython
3.12.3 and OpenTelemetry SDK 1.45.0. The JSON output was not ingested into a hosted
observability product.*

Moving log handling off a request thread can make tracing information disappear.
The request emits a message while its span is current, but the JSON formatter
sees no current span. Those observations are compatible: the formatter now runs
in a different thread.

This lab reproduces the failure using Python's standard `QueueHandler` and
`QueueListener`. It then moves context capture to the producer side of the queue.
The final JSON schema is deliberately small, so fixing correlation does not
quietly become an excuse to export every attribute on a log record.

## 1. Draw the thread boundary

The relevant data path is:

```text
request thread                    listener thread
logger.info(...)
  -> QueueHandler
       -> queue -----------------> StreamHandler
                                     -> JSON formatter
```

The request span is current on the left. Reading OpenTelemetry's current span
on the right asks about the listener thread's context, not about the request
that originally emitted the record.

Adding a formatter that calls `get_current_span()` is therefore insufficient.
The formatter's location matters just as much as its code.

Install and run the local example from the repository root:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m observability_lab.queued_logging
```

These commands create an isolated Python environment, install pinned dependencies,
and run one short-lived example. They do not configure your application's root
logger or contact a network service. The implementation lives in
[`queued_logging.py`](../observability_lab/queued_logging.py).

The example prints two JSON lines. The first was emitted inside a span and has a
32-character trace ID plus a 16-character span ID. The second was emitted outside
the span and has `null` for both. The exact IDs change each run.

## 2. Make the failing design a test

`build_pipeline(..., capture_at_sink=True)` deliberately chooses the wrong
boundary. In that configuration, the formatter calls `current_ids()` on the
listener thread. The test emits a record under a real SDK span and checks that
the resulting IDs are missing.

This is an intentional failing *design*, not a test that fails the suite:
`test_capture_on_listener_reproduces_missing_context` asserts the baseline
behavior. Keeping the broken case alongside the fix makes the claim
reproducible. Without it, a test that only checks for arbitrary nonempty strings
could validate a formatter that invents IDs.

The corrected case checks equality against the actual emitting span's IDs.
Correlation requires that exact match, not merely a correctly sized hexadecimal
string.

## 3. Capture IDs before enqueueing

The fix is a filter attached to the **queue handler**:

```python
class CaptureTraceContext(logging.Filter):
    def filter(self, record):
        record.trace_id, record.span_id = current_ids()
        return True
```

Then install it on the producer-side handler:

```python
handler = logging.handlers.QueueHandler(records)
handler.addFilter(CaptureTraceContext())
```

Handler filters run while handling the record, before `QueueHandler` prepares
and enqueues it. At that moment the request's context is still current.
The listener formatter subsequently reads the captured scalar values from the
record instead of consulting its own thread-local context.

The example uses hexadecimal strings rather than placing a span object on the
record. Strings are sufficient for this JSON output and avoid keeping live
tracing objects in the queue. This is still a thread-queue demonstration, not
validation of a multiprocessing serialization pipeline.

Put the filter on the handler that actually enqueues records. A filter installed
on some other logger or on the final sink does not necessarily execute at the
boundary you intended.

## 4. Do not serialize the entire log record

An attractive shortcut is to serialize `record.__dict__`. That can include
unplanned `extra` fields, exception information, or values unsuitable for JSON.
The demo instead constructs a fresh dictionary with a narrow schema:

```python
{
    "event": approved_event_name,
    "level": record.levelno,
    "trace_id": captured_trace_id,
    "span_id": captured_span_id,
}
```

Only `job.accepted`, `job.completed`, and `job.failed` are recognized event names.
Anything else becomes `unrecognized.event`. A `job_count` field is included only
when its type is exactly `int` and its value lies between zero and one million.
That excludes booleans, which otherwise pass Python's `isinstance(value, int)`
check.

The formatter does not export arbitrary extras or exception text. A regression
uses a clearly synthetic secret marker in all three places and verifies that the
marker does not reach the JSON output.

This is **not a general-purpose secret scrubber**. The original record and its
formatted message can still exist in process memory and in the queue. Another
handler may independently export them. An overly restrictive event schema can
also discard diagnostic information you need. Real applications must choose and
review their own schemas, retention rules, and exception policy.

There is another standard-library detail worth checking: `QueueHandler.prepare`
formats and copies the record before enqueueing. Its treatment of `msg`, `args`,
and exception fields can affect custom structured logging conventions. This
sample's allowed event messages have no interpolation arguments or traceback
text; its tests exercise the actual queue preparation path rather than passing
fabricated records directly to every test.

## 5. Prove that old context does not linger

A worker or listener can outlive one request. That makes stale IDs as concerning
as absent IDs: a record attributed to the wrong request is actively misleading.

The suite emits one record inside a span, exits the span, and emits another.
The second must have no trace or span ID. The filter writes both fields on
**every** record, including the no-span case, rather than using cached values.

The pipeline also stops its listener after producing records. In this example,
`QueueListener.stop()` places a sentinel behind already-enqueued records and
waits for the listener to finish. The drain test enqueues forty records, stops
the listener, and checks that all forty reached the stream in order.

That guarantee assumes producers have stopped. A producer that continues
enqueueing after the sentinel can leave records unprocessed. The demo uses an
unbounded in-process queue to keep the context experiment focused; it does not
provide a production memory limit, overflow policy, timeout, or crash-durability
guarantee. Define those separately before deploying a queued logging pipeline.

## 6. Check the result without overstating it

Run all tests:

```sh
.venv/bin/python -m unittest discover -s tests -v
```

The logging tests cover listener-side context loss, exact producer-side IDs,
absence of stale IDs, orderly draining, bounded field types, and suppression of
the synthetic marker in this formatter's output.

Those tests prove properties of locally generated JSON. They do not prove that
any observability backend recognizes `trace_id` and `span_id` automatically.
Ingestion pipelines can require explicit mapping into a backend's correlation
fields, or use OpenTelemetry's log data model instead of this custom JSON schema.

A follow-up tutorial could connect this controlled example to a local SigNoz
deployment, document the exact ingestion and mapping configuration, and verify
that a log opens the matching trace. That end-to-end work is proposed, **not
claimed as completed here**.

## References

- [Python logging handlers, including QueueHandler and QueueListener](https://docs.python.org/3/library/logging.handlers.html)
- [Python logging filters](https://docs.python.org/3/library/logging.html#filter-objects)
- [OpenTelemetry Python tracing API](https://opentelemetry-python.readthedocs.io/en/latest/api/trace.html)
- [Executable regression tests](../tests/test_queued_logging.py)
