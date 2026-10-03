"""Compare trace parentage at three asyncio-to-thread boundaries."""

import asyncio
import contextvars
import json
from functools import partial
from typing import Callable, TypeVar

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import Tracer

T = TypeVar("T")


async def run_with_context(function: Callable[[], T]) -> T:
    """Capture the current context when this coroutine starts executing."""
    context = contextvars.copy_context()
    return await asyncio.get_running_loop().run_in_executor(None, context.run, function)


def record_worker_span(tracer: Tracer, name: str) -> None:
    with tracer.start_as_current_span(name):
        pass


async def exercise(tracer: Tracer) -> None:
    with tracer.start_as_current_span("request"):
        await asyncio.get_running_loop().run_in_executor(
            None, record_worker_span, tracer, "raw-executor"
        )
        await asyncio.to_thread(record_worker_span, tracer, "to-thread")
        await run_with_context(partial(record_worker_span, tracer, "copied-context"))


def main() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    try:
        asyncio.run(exercise(provider.get_tracer(__name__)))
        spans = {span.name: span for span in exporter.get_finished_spans()}
        request = spans["request"]
        rows = []
        for name in ("raw-executor", "to-thread", "copied-context"):
            span = spans[name]
            rows.append(
                {
                    "boundary": name,
                    "same_trace": span.context.trace_id == request.context.trace_id,
                    "parent_is_request": span.parent is not None
                    and span.parent.span_id == request.context.span_id,
                }
            )
        print(json.dumps(rows, indent=2))
    finally:
        provider.shutdown()


if __name__ == "__main__":
    main()
