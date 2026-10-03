import io
import json
import logging
import unittest

from opentelemetry.sdk.trace import TracerProvider

from observability_lab.queued_logging import EventJSONFormatter, build_pipeline


class QueueLoggingTests(unittest.TestCase):
    def setUp(self):
        self.provider = TracerProvider()
        self.tracer = self.provider.get_tracer(__name__)

    def tearDown(self):
        self.provider.shutdown()

    def record_one(self, capture_at_sink):
        output = io.StringIO()
        pipeline = build_pipeline(output, capture_at_sink=capture_at_sink)
        try:
            with self.tracer.start_as_current_span("request") as span:
                expected = span.get_span_context()
                pipeline.logger.info("job.accepted", extra={"job_count": 3})
        finally:
            pipeline.close()
        return json.loads(output.getvalue()), expected

    def test_capture_on_producer_preserves_exact_context(self):
        record, expected = self.record_one(False)
        self.assertEqual(record["trace_id"], f"{expected.trace_id:032x}")
        self.assertEqual(record["span_id"], f"{expected.span_id:016x}")
        self.assertEqual(record["job_count"], 3)

    def test_capture_on_listener_reproduces_missing_context(self):
        record, _ = self.record_one(True)
        self.assertIsNone(record["trace_id"])
        self.assertIsNone(record["span_id"])

    def test_outside_span_has_no_stale_ids(self):
        output = io.StringIO()
        pipeline = build_pipeline(output)
        try:
            with self.tracer.start_as_current_span("request"):
                pipeline.logger.info("job.accepted")
            pipeline.logger.info("job.completed")
        finally:
            pipeline.close()
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertIsNotNone(records[0]["trace_id"])
        self.assertIsNone(records[1]["trace_id"])
        self.assertIsNone(records[1]["span_id"])

    def test_listener_stop_drains_already_enqueued_records(self):
        output = io.StringIO()
        pipeline = build_pipeline(output)
        try:
            for index in range(40):
                pipeline.logger.info("job.completed", extra={"job_count": index})
        finally:
            pipeline.close()
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([record["job_count"] for record in records], list(range(40)))

    def test_unknown_messages_extras_and_exception_text_are_not_exported(self):
        output = io.StringIO()
        pipeline = build_pipeline(output)
        try:
            try:
                raise ValueError("SYNTHETIC-SECRET-NOT-A-CREDENTIAL")
            except ValueError:
                pipeline.logger.exception(
                    "untrusted %s",
                    "SYNTHETIC-SECRET-NOT-A-CREDENTIAL",
                    extra={"authorization": "SYNTHETIC-SECRET-NOT-A-CREDENTIAL"},
                )
        finally:
            pipeline.close()
        record = json.loads(output.getvalue())
        self.assertEqual(record["event"], "unrecognized.event")
        self.assertNotIn("SYNTHETIC-SECRET", output.getvalue())
        self.assertEqual(set(record), {"event", "level", "trace_id", "span_id"})

    def test_count_accepts_only_bounded_integers(self):
        for count in (True, -1, 1_000_001, 1.5, "3", None):
            with self.subTest(count=count):
                record = logging.makeLogRecord(
                    {"msg": "job.accepted", "levelno": logging.INFO, "job_count": count}
                )
                self.assertNotIn(
                    "job_count", json.loads(EventJSONFormatter().format(record))
                )


if __name__ == "__main__":
    unittest.main()
