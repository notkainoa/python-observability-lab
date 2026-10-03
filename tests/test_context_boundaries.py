import asyncio
import unittest
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from observability_lab.context_boundaries import (
    exercise,
    record_worker_span,
    run_with_context,
)


class ContextTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.exporter = InMemorySpanExporter()
        self.provider = TracerProvider()
        self.provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.tracer = self.provider.get_tracer(__name__)

    def tearDown(self) -> None:
        self.provider.shutdown()

    def spans(self):
        return {span.name: span for span in self.exporter.get_finished_spans()}

    async def test_raw_executor_loses_parent_while_both_fixes_preserve_it(self):
        await exercise(self.tracer)
        spans = self.spans()
        root = spans["request"]
        self.assertIsNone(spans["raw-executor"].parent)
        self.assertNotEqual(
            spans["raw-executor"].context.trace_id, root.context.trace_id
        )
        for name in ("to-thread", "copied-context"):
            with self.subTest(boundary=name):
                self.assertEqual(spans[name].context.trace_id, root.context.trace_id)
                self.assertEqual(spans[name].parent.span_id, root.context.span_id)

    async def test_concurrent_requests_keep_distinct_parents(self):
        async def request(index):
            with self.tracer.start_as_current_span(f"request-{index}"):
                await run_with_context(
                    partial(record_worker_span, self.tracer, f"worker-{index}")
                )

        await asyncio.gather(*(request(index) for index in range(20)))
        spans = self.spans()
        self.assertEqual(
            len({spans[f"request-{i}"].context.trace_id for i in range(20)}), 20
        )
        for index in range(20):
            self.assertEqual(
                spans[f"worker-{index}"].parent.span_id,
                spans[f"request-{index}"].context.span_id,
            )

    async def test_to_thread_captures_when_awaited_not_when_created(self):
        with self.tracer.start_as_current_span("creation"):
            deferred = asyncio.to_thread(
                record_worker_span, self.tracer, "deferred-worker"
            )
        with self.tracer.start_as_current_span("execution"):
            await deferred
        spans = self.spans()
        self.assertEqual(
            spans["deferred-worker"].parent.span_id, spans["execution"].context.span_id
        )

    async def test_create_task_captures_the_creation_context(self):
        with self.tracer.start_as_current_span("creation"):
            task = asyncio.create_task(
                asyncio.to_thread(record_worker_span, self.tracer, "task-worker")
            )
        with self.tracer.start_as_current_span("execution"):
            await task
        spans = self.spans()
        self.assertEqual(
            spans["task-worker"].parent.span_id, spans["creation"].context.span_id
        )

    async def test_exception_does_not_leak_context_into_reused_executor(self):
        asyncio.get_running_loop().set_default_executor(
            ThreadPoolExecutor(max_workers=1)
        )

        def fail():
            with self.tracer.start_as_current_span("failed-worker"):
                raise ValueError("synthetic failure")

        with self.tracer.start_as_current_span("request"):
            with self.assertRaisesRegex(ValueError, "synthetic failure"):
                await run_with_context(fail)
        await asyncio.get_running_loop().run_in_executor(
            None, record_worker_span, self.tracer, "after-failure"
        )
        self.assertIsNone(self.spans()["after-failure"].parent)


if __name__ == "__main__":
    unittest.main()
