"""Capture trace context before a logging queue crosses into another thread."""

import io
import json
import logging
import logging.handlers
import queue
from dataclasses import dataclass
from typing import TextIO

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

ALLOWED_EVENTS = frozenset({"job.accepted", "job.completed", "job.failed"})


def current_ids() -> tuple[str | None, str | None]:
    context = trace.get_current_span().get_span_context()
    if not context.is_valid:
        return None, None
    return f"{context.trace_id:032x}", f"{context.span_id:016x}"


class CaptureTraceContext(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id, record.span_id = current_ids()
        return True


class EventJSONFormatter(logging.Formatter):
    """Intentionally narrow schema, not a general-purpose secret scrubber."""

    def __init__(self, *, capture_at_sink: bool = False) -> None:
        super().__init__()
        self.capture_at_sink = capture_at_sink

    def format(self, record: logging.LogRecord) -> str:
        trace_id, span_id = (
            current_ids()
            if self.capture_at_sink
            else (getattr(record, "trace_id", None), getattr(record, "span_id", None))
        )
        event = record.msg if isinstance(record.msg, str) else ""
        payload: dict[str, str | int | None] = {
            "event": event if event in ALLOWED_EVENTS else "unrecognized.event",
            "level": record.levelno,
            "trace_id": trace_id,
            "span_id": span_id,
        }
        count = getattr(record, "job_count", None)
        if type(count) is int and 0 <= count <= 1_000_000:
            payload["job_count"] = count
        return json.dumps(payload, sort_keys=True)


@dataclass
class Pipeline:
    logger: logging.Logger
    handler: logging.handlers.QueueHandler
    sink: logging.Handler
    listener: logging.handlers.QueueListener

    def close(self) -> None:
        self.listener.stop()
        self.logger.removeHandler(self.handler)
        self.handler.close()
        self.sink.close()


def build_pipeline(stream: TextIO, *, capture_at_sink: bool = False) -> Pipeline:
    records: queue.Queue[logging.LogRecord] = queue.Queue()
    handler = logging.handlers.QueueHandler(records)
    if not capture_at_sink:
        handler.addFilter(CaptureTraceContext())
    sink = logging.StreamHandler(stream)
    sink.setFormatter(EventJSONFormatter(capture_at_sink=capture_at_sink))
    listener = logging.handlers.QueueListener(records, sink)
    # An unregistered logger avoids modifying the application's global configuration.
    logger = logging.Logger("observability-lab", level=logging.INFO)
    logger.propagate = False
    logger.addHandler(handler)
    listener.start()
    return Pipeline(logger, handler, sink, listener)


def main() -> None:
    output = io.StringIO()
    provider = TracerProvider()
    pipeline = build_pipeline(output)
    try:
        with provider.get_tracer(__name__).start_as_current_span("job-request"):
            pipeline.logger.info("job.accepted", extra={"job_count": 2})
        pipeline.logger.info("job.completed", extra={"job_count": 2})
    finally:
        pipeline.close()
        provider.shutdown()
    print(output.getvalue(), end="")


if __name__ == "__main__":
    main()
