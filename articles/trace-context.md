# Why an executor breaks your trace tree

*A local, AI-authored writing sample with executable tests. Tested on CPython
3.12.3 and OpenTelemetry SDK 1.45.0; no hosted observability backend was used.*

A request span is open. A worker function creates a child span. Both spans reach
the exporter, but the worker appears in a different trace. Adding retries to the
exporter will not fix this example: the parent was already missing when the
worker span started.

This lab makes that distinction visible without a dashboard. It compares three
ways to run synchronous work from an async function and asserts exact trace and
parent IDs. The point is not that threads cannot carry tracing context. It is
that different APIs carry it at different times.

## 1. Start with a falsifiable expectation

The intended relationship is:

```text
request
  └─ worker
```

That means two separate things:

- The worker and request share a trace ID.
- The worker's parent span ID equals the request's span ID.

Checking only whether the worker has a nonempty trace ID proves very little.
A perfectly valid root span also has a trace ID. Our test checks both properties,
using OpenTelemetry's `InMemorySpanExporter` to inspect finished spans.

From the repository root, create the environment and run the example:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m observability_lab.context_boundaries
```

The virtual environment isolates the pinned dependencies. The module command
runs the experiment, not a server. It makes no requests to an OTLP endpoint.

You should see `false` for both relationships on `raw-executor` and `true` for
both on `to-thread` and `copied-context`. The complete implementation is
[`context_boundaries.py`](../observability_lab/context_boundaries.py).

## 2. Reproduce the broken boundary

Here is the significant part of the first case:

```python
with tracer.start_as_current_span("request"):
    await asyncio.get_running_loop().run_in_executor(
        None, record_worker_span, tracer, "raw-executor"
    )
```

The first argument, `None`, selects the event loop's default executor.
`record_worker_span` starts a span using the supplied tracer. Supplying the same
tracer does **not** supply the current parent context.

In this manually instrumented CPython 3.12.3 setup, `run_in_executor` does not copy
the caller's context into the worker thread. When the worker asks OpenTelemetry
for the current span, the request span is not current there. The SDK therefore
creates a new root span.

This is why the in-memory experiment is useful. No collector, network retry, or
dashboard configuration can explain the missing parent in these finished span
objects. The baseline is deliberately narrow: auto-instrumentation that patches
thread submission could change the result.

## 3. Use the boundary that already propagates context

For work that can use the default thread pool, `asyncio.to_thread` is the simpler
option:

```python
with tracer.start_as_current_span("request"):
    await asyncio.to_thread(record_worker_span, tracer, "to-thread")
```

Python documents that `to_thread` propagates the current `contextvars.Context`.
OpenTelemetry's current-span state follows that context in this setup. The
worker becomes a child of the request.

This does not turn CPU-bound Python code into parallel execution across cores.
It also does not make cancellation kill an already-running worker. Choose a
thread API for the workload first, then verify its tracing behavior.

If an application requires an explicitly selected executor, or already has
executor-specific integration, copying context provides the other mechanism.
The lab wraps the default executor to isolate that mechanism:

```python
async def run_with_context(function):
    context = contextvars.copy_context()
    return await asyncio.get_running_loop().run_in_executor(
        None, context.run, function
    )
```

`context.run` enters the copied context while the function executes and restores
the worker's previous context afterward, including when the function raises.
The caller receives the function's result or exception through the awaited
future.

Take a **fresh copy for each submission**. Reusing one `Context` object across
concurrent calls is not a pool-wide tracing strategy: a context cannot be entered
concurrently. The lab exercises twenty concurrent requests, each with its own
copy, and checks each worker against its particular request.

## 4. Pay attention to when the copy occurs

An async helper's body does not run merely because you call it. This distinction
applies to both the wrapper above and `asyncio.to_thread`.

Consider a coroutine created under one span and awaited under another:

```python
with tracer.start_as_current_span("creation"):
    deferred = asyncio.to_thread(record_worker_span, tracer, "deferred-worker")

with tracer.start_as_current_span("execution"):
    await deferred
```

The worker belongs to `execution` in this experiment. The coroutine starts
executing when awaited, so that is when `to_thread` copies the current context.
Saving an unawaited coroutine is not the same as saving the caller's context.

Now schedule the coroutine as a task while `creation` is current:

```python
with tracer.start_as_current_span("creation"):
    task = asyncio.create_task(
        asyncio.to_thread(record_worker_span, tracer, "task-worker")
    )

with tracer.start_as_current_span("execution"):
    await task
```

By default, `create_task` copies the context at task creation. The task later
runs `to_thread` in that copied context, making `creation` the worker's parent.
The parent span may already have ended; retaining its context does not extend
its duration or manage the lifetime of the background work.

Both cases have explicit regression tests. This is more precise than a rule such
as “always pass context,” which misses the timing question.

## 5. Check restoration as well as propagation

Run the full suite:

```sh
.venv/bin/python -m unittest discover -s tests -v
```

The context tests cover the broken baseline, both propagation mechanisms,
concurrent request isolation, coroutine/task timing, and exception restoration.
The exception case uses a single-thread executor. After a copied-context worker
raises, a raw submission reuses that executor and must start without a request
parent. This guards against accidentally leaving request state on a pooled
thread.

These are relationship tests, not throughput measurements. They also do not
prove that a process pool, third-party queue, or remote worker transports context.
Across a process or network boundary, use the appropriate propagation format and
decide which incoming context is trusted.

## A practical diagnosis order

When a child seems to disappear, first inspect whether it was created at all.
Then compare trace IDs and parent IDs before changing export settings. Next,
identify the exact boundary and the moment work is scheduled. Finally, test two
concurrent requests so a fix cannot pass by attaching every worker to one shared
parent.

For this particular boundary, those steps distinguish missing propagation from
sampling or transport failure. A future end-to-end tutorial could send the same
experiment to SigNoz and inspect the resulting traces there. That integration
has **not** been performed in this sample.

## References

- [Python 3.12 tasks and running work in threads](https://docs.python.org/3.12/library/asyncio-task.html)
- [Python context variables](https://docs.python.org/3.12/library/contextvars.html)
- [OpenTelemetry Python tracing API](https://opentelemetry-python.readthedocs.io/en/latest/api/trace.html)
- [Executable regression tests](../tests/test_context_boundaries.py)
