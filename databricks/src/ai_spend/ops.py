"""Run log (observability): one row per task execution with rows in/out and status."""
from __future__ import annotations

import json
import time
import traceback
from datetime import datetime, timezone

from pyspark.sql import SparkSession

RUN_LOG_SCHEMA = ("run_id STRING, task STRING, status STRING, started_at TIMESTAMP, finished_at TIMESTAMP, "
                  "duration_s DOUBLE, metrics STRING, error STRING")


class RunLog:
    """Context manager: records success or failure of a task and re-raises errors.

        with RunLog(spark, "cat.sch.ops_run_log", "silver", run_id) as log:
            ...
            log.metrics["rows_out"] = 123
    """

    def __init__(self, spark: SparkSession, table: str, task: str, run_id: str) -> None:
        self.spark, self.table, self.task, self.run_id = spark, table, task, run_id
        self.metrics: dict = {}

    def __enter__(self) -> "RunLog":
        self._t0, self._start = time.time(), datetime.now(timezone.utc)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        end = datetime.now(timezone.utc)
        row = (self.run_id, self.task, "FAILED" if exc_type else "SUCCESS", self._start, end,
               round(time.time() - self._t0, 3), json.dumps(self.metrics, default=str),
               "".join(traceback.format_exception_only(exc_type, exc)).strip() if exc_type else None)
        self.spark.createDataFrame([row], RUN_LOG_SCHEMA).write.mode("append").saveAsTable(self.table)
        return False  # never swallow the exception
