#!/usr/bin/env python3
"""Spark event log checker for OCI-downloaded Spark History Server logs.

The existing project downloads and normalizes object-storage logs into
Spark History Server event-log files under /shs-logs. This agent reads
those normalized JSON-lines event logs and writes JSON/Markdown health
reports without requiring a running History Server.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import statistics
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}
SUCCESS_REASONS = {"Success", "TaskCommitDenied"}
DEFAULT_MAX_SAMPLES = 5000


SIGNATURES = [
    {
        "code": "CONTAINER_OOM_OR_137",
        "severity": "critical",
        "patterns": [
            r"\boomkilled\b",
            r"exit code 137",
            r"container.*(?:killed|kill).*memory",
            r"killed by yarn for exceeding memory",
            r"exceed(?:ed|ing).*physical memory",
            r"memory overhead",
        ],
        "title": "Container or memory-overhead OOM signature found",
        "recommendation": (
            "Check executor loss timestamps first, then size spark.executor.memoryOverhead "
            "for shuffle, direct, native, and Python memory. If task concurrency is high, "
            "use fewer executor cores or more shuffle partitions."
        ),
    },
    {
        "code": "JVM_HEAP_OOM",
        "severity": "critical",
        "patterns": [
            r"outofmemoryerror",
            r"java heap space",
            r"gc overhead limit exceeded",
            r"requested array size exceeds vm limit",
        ],
        "title": "JVM heap OOM signature found",
        "recommendation": (
            "Inspect the failed stage operators and per-task input size. Increase executor "
            "memory only after reducing skew, broadcast size, aggregate state, or per-task data."
        ),
    },
    {
        "code": "DIRECT_OR_NATIVE_MEMORY",
        "severity": "high",
        "patterns": [
            r"direct buffer memory",
            r"cannot reserve.*direct buffer",
            r"native memory",
            r"netty.*memory",
        ],
        "title": "Direct/native memory pressure signature found",
        "recommendation": (
            "Increase executor memory overhead and reduce concurrent shuffle/network pressure."
        ),
    },
    {
        "code": "FETCH_FAILED",
        "severity": "high",
        "patterns": [
            r"fetchfailedexception",
            r"failed to connect.*shuffle",
            r"missing an output location for shuffle",
            r"shuffle block.*not found",
        ],
        "title": "Shuffle fetch failure signature found",
        "recommendation": (
            "Treat FetchFailed as a symptom until executor-loss evidence is ruled out. "
            "Find the earlier executor/container death that held the missing shuffle blocks."
        ),
    },
    {
        "code": "EXECUTOR_LOST",
        "severity": "high",
        "patterns": [
            r"executordeadexception",
            r"executor.*lost",
            r"remote rpc client disassociated",
            r"container.*lost",
            r"worker lost",
        ],
        "title": "Executor loss signature found",
        "recommendation": (
            "Correlate executor removal reasons with failed stages and shuffle fetch failures."
        ),
    },
]

SIGNATURE_TEXT_RE = re.compile(
    "|".join(
        f"(?:{pattern})" for signature in SIGNATURES for pattern in signature["patterns"]
    ),
    flags=re.IGNORECASE,
)
SIGNATURE_PREFILTER_TERMS = (
    "Exception",
    "exception",
    "Error",
    "error",
    "OOM",
    "oom",
    "Killed",
    "killed",
    "137",
    "lost",
    "Lost",
    "dead",
    "Dead",
    "disassociated",
    "overhead",
    "physical memory",
    "Physical memory",
    "Direct buffer",
    "direct buffer",
    "Netty",
    "netty",
    "native memory",
    "Native memory",
    "failed to connect",
    "Failed to connect",
    "missing an output location",
    "Missing an output location",
    "not found",
    "Not found",
    "exceed",
    "Exceed",
    "requested array",
    "Requested array",
)
FAST_EVENT_NAMES = {
    "SparkListenerLogStart",
    "SparkListenerApplicationStart",
    "SparkListenerApplicationEnd",
    "SparkListenerEnvironmentUpdate",
    "SparkListenerJobStart",
    "SparkListenerJobEnd",
    "SparkListenerStageSubmitted",
    "SparkListenerStageCompleted",
    "SparkListenerExecutorAdded",
    "SparkListenerExecutorRemoved",
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
}
TASK_END_MARKER = "SparkListenerTaskEnd"
EVENT_NAME_LINE_RE = re.compile(r'"Event"\s*:\s*"([^"]+)"')
EVENT_FILE_RE = re.compile(r"^events_(\d+)(?:_|$)")
STAGE_ID_LINE_RE = re.compile(r'"Stage ID"\s*:\s*(-?\d+)')
STAGE_ATTEMPT_LINE_RE = re.compile(r'"Stage Attempt ID"\s*:\s*(-?\d+)')


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def ms_to_iso(value: Any) -> str | None:
    try:
        millis = int(value)
    except (TypeError, ValueError):
        return None
    if millis <= 0:
        return None
    return dt.datetime.fromtimestamp(millis / 1000, tz=dt.timezone.utc).isoformat(
        timespec="seconds"
    )


def to_int(value: Any, default: int = 0) -> int:
    if value is None or isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default


def safe_ratio(numerator: int, denominator: int) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def format_bytes(value: int) -> str:
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(amount) < 1024.0 or unit == "PiB":
            return f"{amount:.1f} {unit}" if unit != "B" else f"{int(amount)} B"
        amount /= 1024.0
    return f"{value} B"


def format_ms(value: int) -> str:
    if value < 1000:
        return f"{value} ms"
    seconds = value / 1000
    if seconds < 120:
        return f"{seconds:.1f} s"
    minutes = seconds / 60
    if minutes < 120:
        return f"{minutes:.1f} min"
    return f"{minutes / 60:.1f} hr"


def duration_ms_between(start_ms: int, end_ms: int) -> int:
    return max(0, end_ms - start_ms) if start_ms and end_ms else 0


def sanitize_report_name(path: Path) -> str:
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", path.name).strip("._")
    return clean or "spark-event-log"


def flatten_text(value: Any, limit: int = 12000) -> str:
    """Collect text from nested Spark event fields for signature matching."""

    chunks: list[str] = []

    def visit(item: Any) -> None:
        if sum(len(chunk) for chunk in chunks) >= limit:
            return
        if isinstance(item, str):
            chunks.append(item)
        elif isinstance(item, dict):
            for child in item.values():
                visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)
        elif item is not None:
            chunks.append(str(item))

    visit(value)
    return " ".join(chunks)[:limit]


def first_sentence(text: str, limit: int = 220) -> str:
    compact = re.sub(r"\s+", " ", text).strip()
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


def event_file_sort_key(path: Path) -> tuple[int, str]:
    match = EVENT_FILE_RE.match(path.name)
    if match:
        return (to_int(match.group(1)), path.name)
    return (10**9, path.name)


def event_files_for(source: Path) -> list[Path]:
    if not source.is_dir():
        return []
    return sorted(
        [
            path
            for path in source.iterdir()
            if path.is_file()
            and path.name.startswith("events_")
            and not path.name.startswith(".")
            and not path.name.endswith(".inprogress")
        ],
        key=event_file_sort_key,
    )


def extract_stage_key_from_line(line: str) -> tuple[int, int] | None:
    stage_match = STAGE_ID_LINE_RE.search(line)
    if not stage_match:
        return None
    stage_id = to_int(stage_match.group(1), -1)
    if stage_id < 0:
        return None
    attempt_match = STAGE_ATTEMPT_LINE_RE.search(line)
    attempt_id = to_int(attempt_match.group(1), 0) if attempt_match else 0
    return (stage_id, attempt_id)


def might_have_signature(line: str) -> bool:
    return any(term in line for term in SIGNATURE_PREFILTER_TERMS)


@dataclass
class MetricSample:
    max_samples: int
    count: int = 0
    total: int = 0
    maximum: int = 0
    samples: list[int] = field(default_factory=list)

    def add(self, value: Any) -> None:
        number = to_int(value)
        if number < 0:
            return
        self.count += 1
        self.total += number
        self.maximum = max(self.maximum, number)
        if len(self.samples) < self.max_samples:
            self.samples.append(number)
            return
        # Deterministic bounded sampling to avoid unbounded memory on huge logs.
        self.samples[(self.count - 1) % self.max_samples] = number

    def median(self) -> float:
        if not self.samples:
            return 0.0
        return float(statistics.median(self.samples))

    def as_dict(self, unit: str) -> dict[str, Any]:
        return {
            "unit": unit,
            "count": self.count,
            "total": self.total,
            "max": self.maximum,
            "median_sample": self.median(),
        }


@dataclass
class StageStats:
    stage_id: int
    attempt_id: int
    max_samples: int
    name: str = ""
    num_tasks_declared: int = 0
    submission_time: int = 0
    completion_time: int = 0
    failure_reason: str = ""
    tasks_seen: int = 0
    failed_tasks: int = 0
    end_reasons: Counter[str] = field(default_factory=Counter)
    executor_run_time: MetricSample = field(init=False)
    jvm_gc_time: MetricSample = field(init=False)
    fetch_wait_time: MetricSample = field(init=False)
    memory_spilled: MetricSample = field(init=False)
    disk_spilled: MetricSample = field(init=False)
    peak_execution_memory: MetricSample = field(init=False)
    input_bytes: MetricSample = field(init=False)
    shuffle_read_bytes: MetricSample = field(init=False)
    shuffle_write_bytes: MetricSample = field(init=False)

    def __post_init__(self) -> None:
        self.executor_run_time = MetricSample(self.max_samples)
        self.jvm_gc_time = MetricSample(self.max_samples)
        self.fetch_wait_time = MetricSample(self.max_samples)
        self.memory_spilled = MetricSample(self.max_samples)
        self.disk_spilled = MetricSample(self.max_samples)
        self.peak_execution_memory = MetricSample(self.max_samples)
        self.input_bytes = MetricSample(self.max_samples)
        self.shuffle_read_bytes = MetricSample(self.max_samples)
        self.shuffle_write_bytes = MetricSample(self.max_samples)

    @property
    def key(self) -> str:
        return f"{self.stage_id}.{self.attempt_id}"

    def update_from_info(self, info: dict[str, Any]) -> None:
        self.name = str(info.get("Stage Name") or self.name or "")
        self.num_tasks_declared = to_int(info.get("Number of Tasks"), self.num_tasks_declared)
        self.submission_time = to_int(info.get("Submission Time"), self.submission_time)
        self.completion_time = to_int(info.get("Completion Time"), self.completion_time)
        failure = info.get("Failure Reason")
        if failure:
            self.failure_reason = first_sentence(flatten_text(failure), 500)

    def add_task(self, event: dict[str, Any]) -> None:
        self.tasks_seen += 1
        reason = event.get("Task End Reason")
        reason_name = "Unknown"
        if isinstance(reason, dict):
            reason_name = str(reason.get("Reason") or reason.get("Class Name") or "Unknown")
        elif reason:
            reason_name = str(reason)
        self.end_reasons[reason_name] += 1
        if reason_name not in SUCCESS_REASONS:
            self.failed_tasks += 1

        metrics = event.get("Task Metrics") or {}
        if not isinstance(metrics, dict):
            return

        self.executor_run_time.add(metrics.get("Executor Run Time"))
        self.jvm_gc_time.add(metrics.get("JVM GC Time"))
        self.memory_spilled.add(metrics.get("Memory Bytes Spilled"))
        self.disk_spilled.add(metrics.get("Disk Bytes Spilled"))
        self.peak_execution_memory.add(metrics.get("Peak Execution Memory"))

        input_metrics = metrics.get("Input Metrics") or {}
        output_metrics = metrics.get("Output Metrics") or {}
        shuffle_read = metrics.get("Shuffle Read Metrics") or {}
        shuffle_write = metrics.get("Shuffle Write Metrics") or {}
        if isinstance(input_metrics, dict):
            self.input_bytes.add(input_metrics.get("Bytes Read"))
        if isinstance(shuffle_read, dict):
            total_read = (
                to_int(shuffle_read.get("Total Bytes Read"))
                or to_int(shuffle_read.get("Remote Bytes Read"))
                + to_int(shuffle_read.get("Local Bytes Read"))
                + to_int(shuffle_read.get("Remote Bytes Read To Disk"))
            )
            self.shuffle_read_bytes.add(total_read)
            self.fetch_wait_time.add(shuffle_read.get("Fetch Wait Time"))
        if isinstance(shuffle_write, dict):
            self.shuffle_write_bytes.add(shuffle_write.get("Shuffle Bytes Written"))
        if isinstance(output_metrics, dict):
            # Output bytes are not a finding source yet, but keeping the read path
            # nearby makes future output-volume checks straightforward.
            pass

    def summary(self) -> dict[str, Any]:
        run_total = self.executor_run_time.total
        return {
            "stage_id": self.stage_id,
            "attempt_id": self.attempt_id,
            "name": self.name,
            "declared_tasks": self.num_tasks_declared,
            "tasks_seen": self.tasks_seen,
            "failed_tasks": self.failed_tasks,
            "failure_reason": self.failure_reason,
            "submission_time": ms_to_iso(self.submission_time),
            "completion_time": ms_to_iso(self.completion_time),
            "duration_ms": max(0, self.completion_time - self.submission_time)
            if self.completion_time and self.submission_time
            else 0,
            "end_reasons": dict(self.end_reasons),
            "executor_run_time": self.executor_run_time.as_dict("ms"),
            "jvm_gc_time": self.jvm_gc_time.as_dict("ms"),
            "gc_time_ratio": safe_ratio(self.jvm_gc_time.total, run_total),
            "fetch_wait_time": self.fetch_wait_time.as_dict("ms"),
            "memory_spilled": self.memory_spilled.as_dict("bytes"),
            "disk_spilled": self.disk_spilled.as_dict("bytes"),
            "peak_execution_memory": self.peak_execution_memory.as_dict("bytes"),
            "input_bytes": self.input_bytes.as_dict("bytes"),
            "shuffle_read_bytes": self.shuffle_read_bytes.as_dict("bytes"),
            "shuffle_write_bytes": self.shuffle_write_bytes.as_dict("bytes"),
        }


class SparkEventLogAnalyzer:
    def __init__(
        self,
        source: Path,
        max_samples: int,
        spill_warn_bytes: int,
        gc_warn_ratio: float,
        gc_critical_ratio: float,
        skew_ratio: float,
    ) -> None:
        self.source = source
        self.max_samples = max_samples
        self.spill_warn_bytes = spill_warn_bytes
        self.gc_warn_ratio = gc_warn_ratio
        self.gc_critical_ratio = gc_critical_ratio
        self.skew_ratio = skew_ratio

        self.event_counts: Counter[str] = Counter()
        self.event_lines_seen = 0
        self.parse_errors: list[dict[str, Any]] = []
        self.findings: list[dict[str, Any]] = []
        self.signature_hits: dict[str, dict[str, Any]] = {}
        self.analysis_mode = "full-file"
        self.analysis_metadata: dict[str, Any] = {}

        self.app: dict[str, Any] = {}
        self.spark_properties: dict[str, str] = {}
        self.jobs: dict[int, dict[str, Any]] = {}
        self.stages: dict[tuple[int, int], StageStats] = {}
        self.executors: dict[str, dict[str, Any]] = {}
        self.sql_executions: dict[int, dict[str, Any]] = {}

    def analyze(self) -> dict[str, Any]:
        with self.source.open("r", encoding="utf-8", errors="replace") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                event = self.parse_json_event(line, line_number)
                if event is None:
                    continue
                self.event_lines_seen += 1
                event_name = str(event.get("Event") or "Unknown")
                self.event_counts[event_name] += 1
                self.handle_event(event_name, event)

        self.add_derived_findings()
        return self.report()

    def parse_json_event(
        self, line: str, line_number: int, event_file: Path | None = None
    ) -> dict[str, Any] | None:
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            if len(self.parse_errors) < 25:
                sample = {
                    "line": line_number,
                    "message": exc.msg,
                    "sample": first_sentence(line, 180),
                }
                if event_file is not None:
                    sample["file"] = event_file.name
                self.parse_errors.append(sample)
            return None
        return event if isinstance(event, dict) else None

    def handle_event(self, event_name: str, event: dict[str, Any]) -> None:
        if event_name == "SparkListenerLogStart":
            self.app["spark_version"] = event.get("Spark Version")
        elif event_name == "SparkListenerApplicationStart":
            self.app.update(
                {
                    "app_name": event.get("App Name"),
                    "app_id": event.get("App ID"),
                    "user": event.get("User"),
                    "start_time": to_int(event.get("Timestamp")),
                    "start_time_iso": ms_to_iso(event.get("Timestamp")),
                }
            )
        elif event_name == "SparkListenerApplicationEnd":
            self.app.update(
                {
                    "end_time": to_int(event.get("Timestamp")),
                    "end_time_iso": ms_to_iso(event.get("Timestamp")),
                }
            )
        elif event_name == "SparkListenerEnvironmentUpdate":
            self.spark_properties.update(self.extract_spark_properties(event))
        elif event_name == "SparkListenerJobStart":
            job_id = to_int(event.get("Job ID"), -1)
            if job_id >= 0:
                stage_ids = [to_int(stage_id) for stage_id in event.get("Stage IDs", [])]
                properties = self.extract_event_properties(event.get("Properties"))
                sql_execution_id = to_int(properties.get("spark.sql.execution.id"), -1)
                self.jobs.setdefault(job_id, {}).update(
                    {
                        "job_id": job_id,
                        "submission_time_ms": to_int(event.get("Submission Time")),
                        "submission_time": ms_to_iso(event.get("Submission Time")),
                        "stage_ids": stage_ids,
                        "sql_execution_id": sql_execution_id if sql_execution_id >= 0 else None,
                        "job_group": properties.get("spark.jobGroup.id"),
                        "description": first_sentence(
                            str(properties.get("spark.job.description") or ""), 500
                        ),
                        "status": "running",
                    }
                )
        elif event_name == "SparkListenerJobEnd":
            self.handle_job_end(event)
        elif event_name == "SparkListenerStageSubmitted":
            info = event.get("Stage Info") or {}
            if isinstance(info, dict):
                self.stage_for(info).update_from_info(info)
        elif event_name == "SparkListenerStageCompleted":
            info = event.get("Stage Info") or {}
            if isinstance(info, dict):
                self.stage_for(info).update_from_info(info)
        elif event_name == "SparkListenerTaskEnd":
            stage_id = to_int(event.get("Stage ID"), -1)
            attempt_id = to_int(event.get("Stage Attempt ID"), 0)
            if stage_id >= 0:
                stage = self.stages.setdefault(
                    (stage_id, attempt_id), StageStats(stage_id, attempt_id, self.max_samples)
                )
                stage.add_task(event)
            self.check_signatures(event.get("Task End Reason"), "task_end_reason")
        elif event_name == "SparkListenerExecutorAdded":
            executor_id = str(event.get("Executor ID") or "unknown")
            self.executors.setdefault(executor_id, {}).update(
                {
                    "executor_id": executor_id,
                    "added_time": ms_to_iso(event.get("Timestamp")),
                    "host": event.get("Executor Info", {}).get("Host")
                    if isinstance(event.get("Executor Info"), dict)
                    else None,
                    "removed": False,
                }
            )
        elif event_name == "SparkListenerExecutorRemoved":
            executor_id = str(event.get("Executor ID") or "unknown")
            reason = first_sentence(flatten_text(event.get("Removed Reason")), 500)
            self.executors.setdefault(executor_id, {"executor_id": executor_id}).update(
                {
                    "removed_time": ms_to_iso(event.get("Timestamp")),
                    "removed_reason": reason,
                    "removed": True,
                }
            )
            self.check_signatures(reason, "executor_removed")
        elif event_name == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
            execution_id = to_int(event.get("executionId"), -1)
            if execution_id >= 0:
                self.sql_executions[execution_id] = {
                    "execution_id": execution_id,
                    "description": first_sentence(str(event.get("description") or ""), 500),
                    "details": first_sentence(str(event.get("details") or ""), 1000),
                    "start_time_ms": to_int(event.get("time")),
                    "start_time": ms_to_iso(event.get("time")),
                    "status": "running",
                }
                self.check_signatures(event, "sql_execution_start")
        elif event_name == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
            execution_id = to_int(event.get("executionId"), -1)
            if execution_id >= 0:
                self.sql_executions.setdefault(execution_id, {"execution_id": execution_id}).update(
                    {
                        "end_time_ms": to_int(event.get("time")),
                        "end_time": ms_to_iso(event.get("time")),
                        "status": "ended",
                        "error_message": first_sentence(str(event.get("errorMessage") or ""), 500),
                    }
                )
                self.check_signatures(event, "sql_execution_end")

    def extract_event_properties(self, properties: Any) -> dict[str, str]:
        extracted: dict[str, str] = {}
        if isinstance(properties, dict):
            for key, value in properties.items():
                extracted[str(key)] = str(value)
        elif isinstance(properties, list):
            for item in properties:
                if isinstance(item, list) and len(item) >= 2:
                    extracted[str(item[0])] = str(item[1])
        return extracted

    def extract_spark_properties(self, event: dict[str, Any]) -> dict[str, str]:
        environment_details = event.get("Environment Details") or {}
        if not isinstance(environment_details, dict):
            return {}
        spark_props = environment_details.get("Spark Properties") or {}
        return self.extract_event_properties(spark_props)

    def stage_for(self, info: dict[str, Any]) -> StageStats:
        stage_id = to_int(info.get("Stage ID"), -1)
        attempt_id = to_int(info.get("Stage Attempt ID"), 0)
        return self.stages.setdefault(
            (stage_id, attempt_id), StageStats(stage_id, attempt_id, self.max_samples)
        )

    def handle_job_end(self, event: dict[str, Any]) -> None:
        job_id = to_int(event.get("Job ID"), -1)
        if job_id < 0:
            return
        result = event.get("Job Result")
        result_name = "Unknown"
        exception_text = ""
        if isinstance(result, dict):
            result_name = str(result.get("Result") or "Unknown")
            exception_text = first_sentence(flatten_text(result.get("Exception")), 700)
        elif result:
            result_name = str(result)
        self.jobs.setdefault(job_id, {"job_id": job_id}).update(
            {
                "completion_time_ms": to_int(event.get("Completion Time")),
                "completion_time": ms_to_iso(event.get("Completion Time")),
                "result": result_name,
                "exception": exception_text,
                "status": "failed" if result_name != "JobSucceeded" else "succeeded",
            }
        )
        self.check_signatures(result, "job_end")

    def check_signatures(self, value: Any, source: str) -> None:
        text = flatten_text(value)
        if not text:
            return
        self.check_text_signatures(text, source)

    def check_text_signatures(self, text: str, source: str) -> None:
        for signature in SIGNATURES:
            if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in signature["patterns"]):
                self.record_signature_hit(signature, source, text)

    def record_signature_hit(self, signature: dict[str, Any], source: str, text: str) -> None:
        hit = self.signature_hits.setdefault(
            signature["code"],
            {
                "code": signature["code"],
                "severity": signature["severity"],
                "title": signature["title"],
                "recommendation": signature["recommendation"],
                "count": 0,
                "sources": Counter(),
                "examples": [],
            },
        )
        hit["count"] += 1
        hit["sources"][source] += 1
        if len(hit["examples"]) < 5:
            hit["examples"].append(first_sentence(text, 300))

    def add_finding(
        self,
        severity: str,
        code: str,
        title: str,
        detail: str,
        recommendation: str,
        evidence: dict[str, Any] | None = None,
    ) -> None:
        self.findings.append(
            {
                "severity": severity,
                "code": code,
                "title": title,
                "detail": detail,
                "recommendation": recommendation,
                "evidence": evidence or {},
            }
        )

    def add_derived_findings(self) -> None:
        total_events = sum(self.event_counts.values())
        if total_events == 0:
            self.add_finding(
                "high",
                "NO_SPARK_EVENTS",
                "No Spark events were parsed",
                "The file did not contain valid Spark JSON event lines.",
                "Check the OCI prefix, Fluent Bit unwrap setting, and decompression pipeline.",
            )
            return

        if self.parse_errors:
            self.add_finding(
                "medium",
                "PARSE_ERRORS",
                "Some event-log lines could not be parsed",
                f"{len(self.parse_errors)} malformed line samples were captured.",
                "Verify the OCI object is not truncated and that only Spark event lines are merged.",
                {"samples": self.parse_errors},
            )

        if self.app.get("start_time") and not self.app.get("end_time"):
            self.add_finding(
                "low",
                "APPLICATION_INCOMPLETE",
                "Application end event is missing",
                "The event log may belong to a running app or may be incomplete.",
                "If the Spark app has already finished, verify that all bucket objects for this app were downloaded.",
            )

        failed_jobs = [job for job in self.jobs.values() if job.get("status") == "failed"]
        if failed_jobs:
            self.add_finding(
                "high",
                "FAILED_JOBS",
                "One or more Spark jobs failed",
                f"{len(failed_jobs)} job(s) ended with a failed result.",
                "Inspect the first failed job and map its stages to the SQL or DataFrame operation.",
                {"job_ids": [job["job_id"] for job in failed_jobs[:20]]},
            )

        failed_stages = [stage for stage in self.stages.values() if stage.failure_reason]
        if failed_stages:
            self.add_finding(
                "high",
                "FAILED_STAGES",
                "One or more Spark stages failed",
                f"{len(failed_stages)} stage attempt(s) have failure reasons.",
                "Start with the earliest failed stage and correlate it with failed tasks and executor removals.",
                {
                    "stages": [
                        {
                            "stage": stage.key,
                            "name": stage.name,
                            "reason": stage.failure_reason,
                        }
                        for stage in failed_stages[:10]
                    ]
                },
            )

        for hit in self.signature_hits.values():
            sources = dict(hit["sources"])
            self.add_finding(
                hit["severity"],
                hit["code"],
                hit["title"],
                f"{hit['count']} matching event field(s) found across {len(sources)} source type(s).",
                hit["recommendation"],
                {"sources": sources, "examples": hit["examples"]},
            )

        if "FETCH_FAILED" in self.signature_hits and "EXECUTOR_LOST" in self.signature_hits:
            self.add_finding(
                "high",
                "FETCH_FAILED_AFTER_EXECUTOR_LOSS",
                "Fetch failure and executor loss both appear in the log",
                "This usually means a downstream stage discovered missing shuffle blocks after an executor died.",
                "Prioritize the executor loss root cause before tuning retry counts or fetch settings.",
            )

        for stage in self.stages.values():
            self.add_stage_findings(stage)

    def add_stage_findings(self, stage: StageStats) -> None:
        if stage.tasks_seen and stage.failed_tasks:
            failed_rate = safe_ratio(stage.failed_tasks, stage.tasks_seen)
            if stage.failed_tasks >= 10 or failed_rate >= 0.02:
                self.add_finding(
                    "medium",
                    "HIGH_FAILED_TASK_RATE",
                    "Stage has a high failed-task rate",
                    (
                        f"Stage {stage.key} ({stage.name or 'unnamed'}) has "
                        f"{stage.failed_tasks}/{stage.tasks_seen} failed task end events."
                    ),
                    "Inspect the most frequent task end reasons and failed executor IDs.",
                    {"stage": stage.key, "end_reasons": dict(stage.end_reasons)},
                )

        total_spill = stage.memory_spilled.total + stage.disk_spilled.total
        if total_spill >= self.spill_warn_bytes:
            severity = "high" if total_spill >= self.spill_warn_bytes * 10 else "medium"
            self.add_finding(
                severity,
                "HIGH_STAGE_SPILL",
                "Stage spilled a large amount of data",
                (
                    f"Stage {stage.key} ({stage.name or 'unnamed'}) spilled "
                    f"{format_bytes(total_spill)} across memory and disk."
                ),
                "Check partition sizing, skew, aggregate state, sort pressure, and executor memory.",
                {
                    "stage": stage.key,
                    "memory_spilled": stage.memory_spilled.total,
                    "disk_spilled": stage.disk_spilled.total,
                },
            )

        gc_ratio = safe_ratio(stage.jvm_gc_time.total, stage.executor_run_time.total)
        if stage.executor_run_time.total >= 60_000 and gc_ratio >= self.gc_warn_ratio:
            severity = "high" if gc_ratio >= self.gc_critical_ratio else "medium"
            self.add_finding(
                severity,
                "HIGH_GC_TIME",
                "Stage spent a high share of executor time in GC",
                (
                    f"Stage {stage.key} ({stage.name or 'unnamed'}) GC ratio is "
                    f"{gc_ratio:.1%}."
                ),
                "Look for heap pressure from large rows, aggregation state, broadcast joins, or too many concurrent tasks per executor.",
                {
                    "stage": stage.key,
                    "executor_run_time_ms": stage.executor_run_time.total,
                    "jvm_gc_time_ms": stage.jvm_gc_time.total,
                    "gc_ratio": gc_ratio,
                },
            )

        self.add_skew_finding(stage.executor_run_time, "TASK_RUNTIME_SKEW", "runtime", stage, "ms")
        self.add_skew_finding(stage.input_bytes, "INPUT_SKEW", "input bytes", stage, "bytes")
        self.add_skew_finding(
            stage.shuffle_read_bytes, "SHUFFLE_READ_SKEW", "shuffle-read bytes", stage, "bytes"
        )

        fetch_wait_ratio = safe_ratio(stage.fetch_wait_time.total, stage.executor_run_time.total)
        if stage.executor_run_time.total >= 60_000 and fetch_wait_ratio >= 0.2:
            self.add_finding(
                "medium",
                "HIGH_FETCH_WAIT",
                "Stage spent a high share of executor time waiting on shuffle fetches",
                f"Stage {stage.key} ({stage.name or 'unnamed'}) fetch wait ratio is {fetch_wait_ratio:.1%}.",
                "Check remote shuffle read volume, executor loss, network pressure, and partition sizing.",
                {
                    "stage": stage.key,
                    "fetch_wait_time_ms": stage.fetch_wait_time.total,
                    "executor_run_time_ms": stage.executor_run_time.total,
                },
            )

    def add_skew_finding(
        self, sample: MetricSample, code: str, label: str, stage: StageStats, unit: str
    ) -> None:
        median = sample.median()
        if median <= 0 or sample.maximum <= 0:
            return
        ratio = sample.maximum / median
        min_runtime_gate = code != "TASK_RUNTIME_SKEW" or sample.maximum >= 60_000
        min_byte_gate = code == "TASK_RUNTIME_SKEW" or sample.maximum >= 128 * 1024 * 1024
        if ratio < self.skew_ratio or not min_runtime_gate or not min_byte_gate:
            return
        max_value = format_ms(sample.maximum) if unit == "ms" else format_bytes(sample.maximum)
        median_value = format_ms(int(median)) if unit == "ms" else format_bytes(int(median))
        self.add_finding(
            "medium",
            code,
            f"Stage shows {label} skew",
            (
                f"Stage {stage.key} ({stage.name or 'unnamed'}) max {label} is "
                f"{ratio:.1f}x the sampled median ({max_value} vs {median_value})."
            ),
            "Inspect skewed keys, AQE coalescing, partition counts, and uneven input files.",
            {"stage": stage.key, "max": sample.maximum, "median_sample": median, "ratio": ratio},
        )

    def build_resolution_plan(
        self, findings: list[dict[str, Any]], stage_summaries: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        codes = {finding["code"] for finding in findings}
        plans: list[dict[str, Any]] = []

        def finding_details(*selected_codes: str) -> list[str]:
            selected = set(selected_codes)
            details: list[str] = []
            for finding in findings:
                if finding["code"] not in selected:
                    continue
                details.append(f"{finding['code']}: {finding['detail']}")
                examples = finding.get("evidence", {}).get("examples", [])
                details.extend(f"example: {example}" for example in examples[:2])
            return details[:8]

        def top_stage_by(metric: str) -> dict[str, Any] | None:
            if not stage_summaries:
                return None
            if metric == "spill":
                return max(
                    stage_summaries,
                    key=lambda item: item["memory_spilled"]["total"]
                    + item["disk_spilled"]["total"],
                )
            if metric == "gc":
                return max(stage_summaries, key=lambda item: item["gc_time_ratio"])
            if metric == "runtime":
                return max(stage_summaries, key=lambda item: item["executor_run_time"]["total"])
            return None

        def stage_evidence(stage: dict[str, Any] | None, label: str) -> list[str]:
            if not stage:
                return []
            spill = stage["memory_spilled"]["total"] + stage["disk_spilled"]["total"]
            return [
                (
                    f"{label} stage {stage['stage_id']}.{stage['attempt_id']} "
                    f"({stage.get('name') or 'unnamed'}): tasks={stage['tasks_seen']}, "
                    f"failed={stage['failed_tasks']}, spill={format_bytes(spill)}, "
                    f"gc_ratio={stage['gc_time_ratio']:.1%}"
                )
            ]

        def add_plan(
            priority: int,
            scope: str,
            title: str,
            root_cause: str,
            confidence: str,
            evidence: list[str],
            immediate_mitigation: list[str],
            durable_fix: list[str],
            validation: list[str],
            rollback: list[str],
        ) -> None:
            plans.append(
                {
                    "priority": priority,
                    "scope": scope,
                    "title": title,
                    "root_cause": root_cause,
                    "confidence": confidence,
                    "evidence": evidence,
                    "immediate_mitigation": immediate_mitigation,
                    "durable_fix": durable_fix,
                    "validation": validation,
                    "rollback": rollback,
                }
            )

        if codes & {"NO_SPARK_EVENTS", "PARSE_ERRORS"}:
            evidence = finding_details("NO_SPARK_EVENTS", "PARSE_ERRORS")
            if self.parse_errors:
                evidence.append(f"parse error samples captured: {len(self.parse_errors)}")
            add_plan(
                1,
                "data-health",
                "Repair event-log data before relying on SHS analysis",
                "The event-log file is not fully parseable as Spark JSON-lines data.",
                "high",
                evidence,
                [
                    "Keep SPARK_EVENT_REPAIR_ENABLED=true so preparation keeps only valid Spark event JSON lines.",
                    "Check /shs-logs/_repair-reports/*.repair.json for stopped_at_line and corruption_message.",
                    "Re-download or regenerate the OCI objects if corruption appears before the failing stage evidence.",
                ],
                [
                    "Fix the upstream log writer or object upload path so event logs are not truncated mid-line.",
                    "Keep oversized string truncation enabled to prevent Spark History Server JSON parser failures.",
                ],
                [
                    "Rerun preparation and confirm parse_error_samples is 0 in the agent report.",
                    "Confirm Spark History Server can list and open the application.",
                ],
                [
                    "Restore the original downloaded object or disable repair only if truncation removed evidence needed for diagnosis.",
                ],
            )

        if "APPLICATION_INCOMPLETE" in codes:
            add_plan(
                2,
                "data-health",
                "Verify the event log is complete",
                "The application start event is present but the application end event is missing.",
                "medium",
                finding_details("APPLICATION_INCOMPLETE"),
                [
                    "Confirm whether the Spark application is still running.",
                    "If it finished, verify all event-log objects for the app prefix were downloaded and merged.",
                ],
                [
                    "Ensure Fluent Bit or OCI object upload does not split or omit the final event-log segment.",
                ],
                [
                    "Rerun preparation and confirm SparkListenerApplicationEnd appears in event_counts.",
                ],
                [
                    "Do not tune application memory from an incomplete event log unless executor loss evidence is already decisive.",
                ],
            )

        if "CONTAINER_OOM_OR_137" in codes:
            props = self.selected_spark_properties()
            evidence = finding_details("CONTAINER_OOM_OR_137", "EXECUTOR_LOST")
            for key in ("spark.executor.memory", "spark.executor.memoryOverhead", "spark.executor.cores"):
                if key in props:
                    evidence.append(f"{key}={props[key]}")
            add_plan(
                10,
                "oom-resolution",
                "Resolve container or memory-overhead OOM",
                "An executor/container was killed outside the JVM heap, usually from overhead, direct/native memory, shuffle buffers, Python/native usage, or too many concurrent tasks.",
                "high",
                evidence,
                [
                    "Increase spark.executor.memoryOverhead for the next run; start around 10-15% of executor memory for large shuffle jobs, higher when evidence shows heavy direct/native pressure.",
                    "Reduce spark.executor.cores or use more smaller executors so fewer memory-heavy tasks share one container.",
                ],
                [
                    "Reduce per-task shuffle and spill pressure with more partitions, skew handling, and narrower rows before relying only on larger containers.",
                    "Keep broadcast enabled only when the build side is truly small and memory is sized for resident broadcast plus shuffle concurrency.",
                ],
                [
                    "Executor removals with OOMKilled/exit 137 should disappear.",
                    "GC ratio should not rise sharply; spill and fetch wait should stay flat or improve.",
                ],
                [
                    "Rollback if runtime or cost increases materially without reducing executor loss or failed task rate.",
                ],
            )

        if "DIRECT_OR_NATIVE_MEMORY" in codes:
            add_plan(
                11,
                "oom-resolution",
                "Resolve direct/native memory pressure",
                "Direct buffers, Netty, or native memory exhausted outside normal JVM heap accounting.",
                "high",
                finding_details("DIRECT_OR_NATIVE_MEMORY"),
                [
                    "Increase executor memory overhead or off-heap capacity.",
                    "Reduce executor cores to lower simultaneous shuffle/network buffer demand.",
                ],
                [
                    "Reduce concurrent shuffle fetch/write pressure through partition sizing and skew fixes.",
                ],
                [
                    "Direct-memory errors disappear and fetch wait/executor loss decline.",
                ],
                [
                    "Rollback if overhead increase does not reduce direct-memory signatures on a comparable run.",
                ],
            )

        if "FETCH_FAILED_AFTER_EXECUTOR_LOSS" in codes:
            add_plan(
                12,
                "shuffle-recovery",
                "Treat fetch failure as a symptom of earlier executor loss",
                "A downstream reducer likely failed because shuffle blocks were lost when an upstream executor died.",
                "high",
                finding_details("FETCH_FAILED_AFTER_EXECUTOR_LOSS", "FETCH_FAILED", "EXECUTOR_LOST"),
                [
                    "Prioritize the executor-loss root cause before changing retry or fetch settings.",
                    "Rerun after the executor-loss mitigation and check whether FetchFailed disappears.",
                ],
                [
                    "Consider shuffle tracking, decommissioning support, or external shuffle service where available for better recovery.",
                ],
                [
                    "No executor death should precede the fetch failure in the next event log.",
                    "The previously failed fetch stage should complete without missing shuffle blocks.",
                ],
                [
                    "Rollback fetch/retry setting changes that mask the symptom without eliminating executor loss.",
                ],
            )

        if "JVM_HEAP_OOM" in codes or "HIGH_GC_TIME" in codes:
            stage = top_stage_by("gc")
            add_plan(
                20,
                "oom-resolution",
                "Resolve JVM heap pressure",
                "The JVM heap live set or churn is too high for one or more stages.",
                "high" if "JVM_HEAP_OOM" in codes else "medium",
                finding_details("JVM_HEAP_OOM", "HIGH_GC_TIME") + stage_evidence(stage, "highest-GC"),
                [
                    "Reduce heap live set by projecting fewer columns, filtering earlier, reducing aggregate/join state, or lowering task concurrency.",
                    "Increase executor memory only after identifying the heap-heavy operator or skewed task pattern.",
                ],
                [
                    "Rewrite large aggregates or joins to reduce state size; address skew and cache pressure.",
                    "Use partition sizing that lowers max per-task input without creating excessive scheduler overhead.",
                ],
                [
                    "GC time ratio should drop and the failed stage should complete.",
                    "Memory/disk spill should not grow enough to offset the improvement.",
                ],
                [
                    "Rollback if added heap causes fewer executors, lower parallelism, or worse spill without reducing GC ratio.",
                ],
            )

        if codes & {"HIGH_STAGE_SPILL", "INPUT_SKEW", "SHUFFLE_READ_SKEW", "TASK_RUNTIME_SKEW", "HIGH_FETCH_WAIT"}:
            spill_stage = top_stage_by("spill")
            runtime_stage = top_stage_by("runtime")
            add_plan(
                30,
                "resource-pressure",
                "Reduce shuffle, spill, and skew pressure",
                "One or more stages have high per-task state, uneven data distribution, or large shuffle/fetch pressure.",
                "medium",
                finding_details(
                    "HIGH_STAGE_SPILL",
                    "INPUT_SKEW",
                    "SHUFFLE_READ_SKEW",
                    "TASK_RUNTIME_SKEW",
                    "HIGH_FETCH_WAIT",
                )
                + stage_evidence(spill_stage, "highest-spill")
                + stage_evidence(runtime_stage, "highest-runtime"),
                [
                    "Increase shuffle partitions for the affected stage and lower AQE advisory partition size if coalescing made tasks too large.",
                    "If one task dominates, add skew handling before increasing memory.",
                ],
                [
                    "Repartition by a better key, salt hot keys, split uneven input files, pre-aggregate/filter earlier, and reduce row width before shuffle.",
                ],
                [
                    "Max-to-median task runtime and input/shuffle bytes should shrink.",
                    "Spill bytes, fetch wait, and failed task count should fall on the comparable stage.",
                ],
                [
                    "Rollback partition increases if scheduler overhead rises without reducing max task size or spill.",
                ],
            )

        if not plans and codes & {"FAILED_JOBS", "FAILED_STAGES"}:
            add_plan(
                40,
                "failure-triage",
                "Map failed jobs and stages to the Spark plan",
                "The event log shows failures but not enough OOM-specific evidence for a targeted memory fix.",
                "medium",
                finding_details("FAILED_JOBS", "FAILED_STAGES"),
                [
                    "Open the failed stage in Spark History Server and collect task metrics, SQL DAG, and executor loss reasons.",
                ],
                [
                    "Add physical plan and executor stderr/stdout artifacts before changing memory or partition settings.",
                ],
                [
                    "The next agent report should include a concrete OOM, spill, skew, fetch, or data-health finding.",
                ],
                [
                    "Do not apply broad memory increases without identifying the failed operator or executor-loss reason.",
                ],
            )

        return sorted(plans, key=lambda item: item["priority"])

    def build_query_breakdown(
        self, stage_summaries: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        stages_by_id: dict[int, list[StageStats]] = {}
        for stage in self.stages.values():
            stages_by_id.setdefault(stage.stage_id, []).append(stage)

        query_ids = {
            execution_id
            for execution_id in self.sql_executions
            if isinstance(execution_id, int) and execution_id >= 0
        }
        for job in self.jobs.values():
            execution_id = job.get("sql_execution_id")
            if isinstance(execution_id, int) and execution_id >= 0:
                query_ids.add(execution_id)

        stage_summaries_by_key = {
            (summary["stage_id"], summary["attempt_id"]): summary for summary in stage_summaries
        }
        breakdown: list[dict[str, Any]] = []
        for execution_id in sorted(query_ids):
            query = dict(self.sql_executions.get(execution_id, {"execution_id": execution_id}))
            query_jobs = sorted(
                [
                    job
                    for job in self.jobs.values()
                    if job.get("sql_execution_id") == execution_id
                ],
                key=lambda item: item.get("job_id", -1),
            )
            stage_id_set = {
                stage_id
                for job in query_jobs
                for stage_id in job.get("stage_ids", [])
                if isinstance(stage_id, int) and stage_id >= 0
            }
            query_stages = sorted(
                [
                    stage
                    for stage_id in stage_id_set
                    for stage in stages_by_id.get(stage_id, [])
                ],
                key=lambda item: (item.stage_id, item.attempt_id),
            )
            query_stage_summaries = [
                stage_summaries_by_key.get((stage.stage_id, stage.attempt_id))
                for stage in query_stages
            ]
            query_stage_summaries = [stage for stage in query_stage_summaries if stage]

            query_start_ms = to_int(query.get("start_time_ms"))
            query_end_ms = to_int(query.get("end_time_ms"))
            duration_source = "sql_execution"
            if not query_start_ms:
                query_start_ms = min(
                    [to_int(job.get("submission_time_ms")) for job in query_jobs if job.get("submission_time_ms")]
                    or [0]
                )
                duration_source = "jobs" if query_start_ms else "unknown"
            if not query_end_ms:
                query_end_ms = max(
                    [to_int(job.get("completion_time_ms")) for job in query_jobs if job.get("completion_time_ms")]
                    or [0]
                )
                if query_end_ms and duration_source == "sql_execution":
                    duration_source = "sql_execution_start_and_jobs"
                elif query_end_ms:
                    duration_source = "jobs"
            duration_ms = duration_ms_between(query_start_ms, query_end_ms)

            job_durations = [
                duration_ms_between(
                    to_int(job.get("submission_time_ms")), to_int(job.get("completion_time_ms"))
                )
                for job in query_jobs
            ]
            job_wall_clock_ms = duration_ms_between(
                min([to_int(job.get("submission_time_ms")) for job in query_jobs if job.get("submission_time_ms")] or [0]),
                max([to_int(job.get("completion_time_ms")) for job in query_jobs if job.get("completion_time_ms")] or [0]),
            )

            stage_attempts_by_id: dict[int, list[int]] = {}
            for stage in query_stages:
                stage_attempts_by_id.setdefault(stage.stage_id, []).append(stage.attempt_id)
            retried_stage_ids = sorted(
                stage_id
                for stage_id, attempts in stage_attempts_by_id.items()
                if len(set(attempts)) > 1 or any(attempt > 0 for attempt in attempts)
            )
            task_retry_attempts = sum(
                max(0, stage.tasks_seen - stage.num_tasks_declared)
                for stage in query_stages
                if stage.num_tasks_declared > 0
            )
            failed_task_attempts = sum(stage.failed_tasks for stage in query_stages)
            retry_occurred = bool(retried_stage_ids or task_retry_attempts)

            failed_jobs = sum(1 for job in query_jobs if job.get("status") == "failed")
            succeeded_jobs = sum(1 for job in query_jobs if job.get("status") == "succeeded")
            error_message = str(query.get("error_message") or "")
            if failed_jobs or error_message:
                status = "failed"
            elif query.get("status") == "ended":
                status = "succeeded"
            elif query.get("status") == "running":
                status = "running"
            else:
                status = "unknown"

            stage_duration_total = sum(stage["duration_ms"] for stage in query_stage_summaries)
            stage_duration_max = max([stage["duration_ms"] for stage in query_stage_summaries] or [0])
            task_executor_run_time = sum(
                stage.executor_run_time.total for stage in query_stages
            )

            breakdown.append(
                {
                    "execution_id": execution_id,
                    "description": query.get("description") or "",
                    "details": query.get("details") or "",
                    "status": status,
                    "error_message": error_message,
                    "start_time": query.get("start_time") or ms_to_iso(query_start_ms),
                    "end_time": query.get("end_time") or ms_to_iso(query_end_ms),
                    "duration_ms": duration_ms,
                    "duration": format_ms(duration_ms) if duration_ms else "",
                    "duration_source": duration_source,
                    "jobs": {
                        "total": len(query_jobs),
                        "failed": failed_jobs,
                        "succeeded": succeeded_jobs,
                        "running": sum(
                            1 for job in query_jobs if job.get("status") == "running"
                        ),
                        "job_ids": [job["job_id"] for job in query_jobs],
                        "duration_ms_total": sum(job_durations),
                        "wall_clock_duration_ms": job_wall_clock_ms,
                    },
                    "stages": {
                        "unique": len(stage_attempts_by_id),
                        "attempts": len(query_stages),
                        "failed_attempts": sum(1 for stage in query_stages if stage.failure_reason),
                        "stage_ids": sorted(stage_attempts_by_id),
                        "retried_stage_ids": retried_stage_ids,
                        "duration_ms_total": stage_duration_total,
                        "max_duration_ms": stage_duration_max,
                    },
                    "tasks": {
                        "total": sum(stage.tasks_seen for stage in query_stages),
                        "failed": failed_task_attempts,
                        "declared": sum(stage.num_tasks_declared for stage in query_stages),
                        "executor_run_time_ms_total": task_executor_run_time,
                        "counts_complete": self.analysis_mode == "full-file",
                        "metrics_scope": "all_stages"
                        if self.analysis_mode == "full-file"
                        else "targeted_failed_or_signature_stages",
                    },
                    "retries": {
                        "occurred": retry_occurred,
                        "stage_retry_count": len(retried_stage_ids),
                        "retried_stage_ids": retried_stage_ids,
                        "task_retry_attempts": task_retry_attempts,
                        "failed_task_attempts": failed_task_attempts,
                    },
                }
            )

        return sorted(
            breakdown,
            key=lambda item: (
                item.get("start_time") or "",
                item.get("execution_id", -1),
            ),
        )

    def source_size_bytes(self) -> int:
        if self.source.is_file():
            return self.source.stat().st_size
        if self.source.is_dir():
            return sum(path.stat().st_size for path in event_files_for(self.source))
        return 0

    def report(self) -> dict[str, Any]:
        start_time = to_int(self.app.get("start_time"))
        end_time = to_int(self.app.get("end_time"))
        duration_ms = max(0, end_time - start_time) if start_time and end_time else 0
        stage_summaries = [stage.summary() for stage in self.stages.values()]
        findings = sorted(
            self.findings,
            key=lambda item: (SEVERITY_ORDER.get(item["severity"], 99), item["code"]),
        )
        resolution_plan = self.build_resolution_plan(findings, stage_summaries)
        query_breakdown = self.build_query_breakdown(stage_summaries)
        summary = {
            "source_file": str(self.source),
            "source_size_bytes": self.source_size_bytes(),
            "generated_at": utc_now(),
            "analysis_mode": self.analysis_mode,
            "analysis_metadata": self.analysis_metadata,
            "app_id": self.app.get("app_id") or self.spark_properties.get("spark.app.id"),
            "app_name": self.app.get("app_name") or self.spark_properties.get("spark.app.name"),
            "user": self.app.get("user"),
            "spark_version": self.app.get("spark_version"),
            "start_time": self.app.get("start_time_iso"),
            "end_time": self.app.get("end_time_iso"),
            "duration_ms": duration_ms,
            "event_count": self.event_lines_seen or sum(self.event_counts.values()),
            "parsed_event_count": sum(self.event_counts.values()),
            "parse_error_samples": len(self.parse_errors),
            "jobs": {
                "total": len(self.jobs),
                "failed": sum(1 for job in self.jobs.values() if job.get("status") == "failed"),
                "succeeded": sum(
                    1 for job in self.jobs.values() if job.get("status") == "succeeded"
                ),
            },
            "stages": {
                "total_attempts": len(self.stages),
                "failed_attempts": sum(1 for stage in self.stages.values() if stage.failure_reason),
            },
            "tasks": {
                "total": sum(stage.tasks_seen for stage in self.stages.values()),
                "failed": sum(stage.failed_tasks for stage in self.stages.values()),
            },
            "executors": {
                "seen": len(self.executors),
                "removed": sum(1 for executor in self.executors.values() if executor.get("removed")),
            },
            "queries": {
                "total": len(query_breakdown),
                "failed": sum(1 for query in query_breakdown if query.get("status") == "failed"),
                "with_retries": sum(
                    1 for query in query_breakdown if query.get("retries", {}).get("occurred")
                ),
            },
            "findings": {
                "total": len(findings),
                "by_severity": dict(Counter(item["severity"] for item in findings)),
            },
            "resolution_plan_items": len(resolution_plan),
        }
        return {
            "summary": summary,
            "findings": findings,
            "resolution_plan": resolution_plan,
            "event_counts": dict(self.event_counts),
            "spark_properties": self.selected_spark_properties(),
            "jobs": sorted(self.jobs.values(), key=lambda item: item.get("job_id", -1)),
            "stages": sorted(stage_summaries, key=lambda item: (item["stage_id"], item["attempt_id"])),
            "executors": sorted(self.executors.values(), key=lambda item: item["executor_id"]),
            "sql_executions": sorted(
                self.sql_executions.values(), key=lambda item: item["execution_id"]
            ),
            "query_breakdown": query_breakdown,
        }

    def selected_spark_properties(self) -> dict[str, str]:
        interesting_prefixes = (
            "spark.app.",
            "spark.executor.",
            "spark.driver.",
            "spark.sql.",
            "spark.memory.",
            "spark.dynamicAllocation.",
            "spark.kubernetes.",
            "spark.shuffle.",
        )
        return {
            key: value
            for key, value in sorted(self.spark_properties.items())
            if key.startswith(interesting_prefixes)
        }


class LargeShsDirectoryAnalyzer(SparkEventLogAnalyzer):
    """Streaming analyzer for Spark History Server eventlog_v2_* directories."""

    def __init__(
        self,
        source: Path,
        max_samples: int,
        spill_warn_bytes: int,
        gc_warn_ratio: float,
        gc_critical_ratio: float,
        skew_ratio: float,
        max_deep_stages: int,
    ) -> None:
        super().__init__(
            source,
            max_samples=max_samples,
            spill_warn_bytes=spill_warn_bytes,
            gc_warn_ratio=gc_warn_ratio,
            gc_critical_ratio=gc_critical_ratio,
            skew_ratio=skew_ratio,
        )
        self.analysis_mode = "large-shs-stream"
        self.max_deep_stages = max_deep_stages
        self.event_files = event_files_for(source)
        self.failed_stage_keys: set[tuple[int, int]] = set()
        self.signature_stage_keys: set[tuple[int, int]] = set()
        self.deep_stage_keys: list[tuple[int, int]] = []
        self.deep_task_events = 0
        self.raw_signature_lines = 0
        self.ignored_non_event_lines = 0
        self.fast_parsed_events = 0

    def analyze(self) -> dict[str, Any]:
        self.first_pass()
        self.deep_stage_keys = self.select_deep_stage_keys()
        self.deep_task_events = self.deep_parse_target_stages()
        self.analysis_metadata = {
            "event_file_count": len(self.event_files),
            "event_files": [path.name for path in self.event_files[:100]],
            "event_lines_seen": self.event_lines_seen,
            "fast_parsed_events": self.fast_parsed_events,
            "raw_signature_lines": self.raw_signature_lines,
            "failed_stage_count": len(self.failed_stage_keys),
            "signature_stage_count": len(self.signature_stage_keys),
            "deep_stage_count": len(self.deep_stage_keys),
            "deep_stage_keys": [
                f"{stage_id}.{attempt_id}" for stage_id, attempt_id in self.deep_stage_keys
            ],
            "deep_stage_limit": self.max_deep_stages,
            "deep_stage_limit_exceeded": len(self.failed_stage_keys | self.signature_stage_keys)
            > len(self.deep_stage_keys),
            "deep_task_events": self.deep_task_events,
            "ignored_non_event_lines": self.ignored_non_event_lines,
        }
        self.add_derived_findings()
        return self.report()

    def first_pass(self) -> None:
        for event_file in self.event_files:
            try:
                with event_file.open("r", encoding="utf-8", errors="replace") as handle:
                    for line_number, line in enumerate(handle, start=1):
                        self.scan_first_pass_line(event_file, line_number, line)
            except OSError as exc:
                if len(self.parse_errors) < 25:
                    self.parse_errors.append(
                        {"file": event_file.name, "line": 0, "message": str(exc), "sample": ""}
                    )

    def scan_first_pass_line(self, event_file: Path, line_number: int, line: str) -> None:
        if not line:
            return
        event_name_match = EVENT_NAME_LINE_RE.search(line)
        if not event_name_match:
            self.ignored_non_event_lines += 1
            return

        raw_event_name = event_name_match.group(1)
        self.event_lines_seen += 1
        if might_have_signature(line) and SIGNATURE_TEXT_RE.search(line):
            self.raw_signature_lines += 1
            self.check_text_signatures(line, "raw_event_line")
            if raw_event_name == TASK_END_MARKER:
                stage_key = extract_stage_key_from_line(line)
                if stage_key:
                    self.signature_stage_keys.add(stage_key)

        if raw_event_name not in FAST_EVENT_NAMES:
            return

        event = self.parse_json_event(line, line_number, event_file)
        if event is None:
            return
        event_name = str(event.get("Event") or "Unknown")
        if event_name != raw_event_name or event_name not in FAST_EVENT_NAMES:
            return

        self.fast_parsed_events += 1
        self.event_counts[event_name] += 1
        self.handle_event(event_name, event)
        if event_name == "SparkListenerStageCompleted":
            info = event.get("Stage Info") or {}
            if isinstance(info, dict) and info.get("Failure Reason"):
                stage_id = to_int(info.get("Stage ID"), -1)
                attempt_id = to_int(info.get("Stage Attempt ID"), 0)
                if stage_id >= 0:
                    self.failed_stage_keys.add((stage_id, attempt_id))

    def select_deep_stage_keys(self) -> list[tuple[int, int]]:
        if self.max_deep_stages <= 0:
            return []
        selected: list[tuple[int, int]] = sorted(self.failed_stage_keys)[: self.max_deep_stages]
        remaining = self.max_deep_stages - len(selected)
        if remaining <= 0:
            return selected
        for stage_key in sorted(self.signature_stage_keys - set(selected)):
            selected.append(stage_key)
            remaining -= 1
            if remaining <= 0:
                break
        return selected

    def deep_parse_target_stages(self) -> int:
        target_keys = set(self.deep_stage_keys)
        if not target_keys:
            return 0

        parsed_tasks = 0
        for event_file in self.event_files:
            try:
                with event_file.open("r", encoding="utf-8", errors="replace") as handle:
                    for line_number, line in enumerate(handle, start=1):
                        if TASK_END_MARKER not in line:
                            continue
                        stage_key = extract_stage_key_from_line(line)
                        if stage_key not in target_keys:
                            continue
                        event = self.parse_json_event(line.strip(), line_number, event_file)
                        if event is None:
                            continue
                        event_name = str(event.get("Event") or "Unknown")
                        if event_name != TASK_END_MARKER:
                            continue
                        self.event_counts[event_name] += 1
                        self.handle_event(event_name, event)
                        parsed_tasks += 1
            except OSError as exc:
                if len(self.parse_errors) < 25:
                    self.parse_errors.append(
                        {"file": event_file.name, "line": 0, "message": str(exc), "sample": ""}
                    )
        return parsed_tasks


def discover_logs(input_dir: Path, output_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    output_dir = output_dir.resolve()
    ignored_suffixes = {".json", ".md", ".crc", ".tmp"}
    for path in sorted(input_dir.rglob("*")):
        if not path.is_file():
            continue
        try:
            resolved = path.resolve()
        except OSError:
            continue
        if output_dir == resolved or output_dir in resolved.parents:
            continue
        if path.name.startswith(".") or path.name.endswith(".inprogress"):
            continue
        if path.suffix.lower() in ignored_suffixes:
            continue
        if path.stat().st_size == 0:
            continue
        candidates.append(path)
    return candidates


def discover_shs_eventlog_dirs(input_dir: Path, output_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    output_dir = output_dir.resolve()
    if input_dir.name.startswith("eventlog_v2_"):
        paths = [input_dir]
    else:
        paths = sorted(input_dir.rglob("eventlog_v2_*"))

    for path in paths:
        if not path.is_dir():
            continue
        try:
            resolved = path.resolve()
        except OSError:
            continue
        if output_dir == resolved or output_dir in resolved.parents:
            continue
        if event_files_for(path):
            candidates.append(path)
    return candidates


def write_report(report: dict[str, Any], output_dir: Path, source: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    name = sanitize_report_name(source)
    json_path = output_dir / f"{name}.report.json"
    md_path = output_dir / f"{name}.report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, md_path


def render_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    findings = report["findings"]
    resolution_plan = report.get("resolution_plan", [])
    lines = [
        f"# Spark Event Log Agent Report: {Path(summary['source_file']).name}",
        "",
        f"Generated: {summary['generated_at']}",
        "",
        "## Summary",
        "",
        f"- Application: {summary.get('app_name') or 'unknown'} ({summary.get('app_id') or 'unknown'})",
        f"- Spark version: {summary.get('spark_version') or 'unknown'}",
        f"- Runtime: {summary.get('start_time') or 'unknown'} to {summary.get('end_time') or 'unknown'}",
        f"- Analysis mode: {summary.get('analysis_mode') or 'full-file'}",
        f"- Event lines seen: {summary['event_count']}",
        f"- Events parsed: {summary.get('parsed_event_count', summary['event_count'])}",
        f"- Jobs: {summary['jobs']['total']} total, {summary['jobs']['failed']} failed",
        f"- Stages: {summary['stages']['total_attempts']} attempts, {summary['stages']['failed_attempts']} failed",
        f"- Tasks: {summary['tasks']['total']} total, {summary['tasks']['failed']} failed",
        f"- Queries: {summary.get('queries', {}).get('total', 0)} total, "
        f"{summary.get('queries', {}).get('failed', 0)} failed, "
        f"{summary.get('queries', {}).get('with_retries', 0)} with retries",
        f"- Executors: {summary['executors']['seen']} seen, {summary['executors']['removed']} removed",
        "",
        "## Findings",
        "",
    ]

    if not findings:
        lines.append("No findings.")
    else:
        lines.extend(["| Severity | Code | Finding |", "|---|---|---|"])
        for finding in findings:
            detail = first_sentence(finding["detail"], 160).replace("|", "\\|")
            lines.append(
                f"| {finding['severity']} | `{finding['code']}` | {finding['title']}: {detail} |"
            )

        lines.extend(["", "## Recommendations", ""])
        for finding in findings:
            lines.append(f"- `{finding['code']}`: {finding['recommendation']}")

    lines.extend(["", "## Resolution Plan", ""])
    if not resolution_plan:
        lines.append("No resolution plan generated from the current findings.")
    else:
        for plan in resolution_plan:
            lines.extend(
                [
                    f"### {plan['priority']}. {plan['title']}",
                    "",
                    f"- Scope: `{plan['scope']}`",
                    f"- Confidence: {plan['confidence']}",
                    f"- Root cause: {plan['root_cause']}",
                ]
            )
            lines.extend(render_plan_list("Evidence", plan.get("evidence", [])))
            lines.extend(render_plan_list("Immediate mitigation", plan.get("immediate_mitigation", [])))
            lines.extend(render_plan_list("Durable fix", plan.get("durable_fix", [])))
            lines.extend(render_plan_list("Validation", plan.get("validation", [])))
            lines.extend(render_plan_list("Rollback", plan.get("rollback", [])))

    lines.extend(["", "## Query Breakdown", ""])
    lines.extend(render_query_breakdown_table(report.get("query_breakdown", [])))

    top_spill = sorted(
        report["stages"],
        key=lambda item: item["memory_spilled"]["total"] + item["disk_spilled"]["total"],
        reverse=True,
    )[:5]
    top_runtime = sorted(
        report["stages"], key=lambda item: item["executor_run_time"]["total"], reverse=True
    )[:5]

    lines.extend(["", "## Top Stages By Spill", ""])
    lines.extend(render_stage_table(top_spill, value_kind="spill"))
    lines.extend(["", "## Top Stages By Executor Runtime", ""])
    lines.extend(render_stage_table(top_runtime, value_kind="runtime"))

    removed = [executor for executor in report["executors"] if executor.get("removed")]
    if removed:
        lines.extend(["", "## Removed Executors", ""])
        lines.extend(["| Executor | Removed At | Reason |", "|---|---|---|"])
        for executor in removed[:20]:
            reason = first_sentence(str(executor.get("removed_reason") or ""), 140).replace("|", "\\|")
            lines.append(
                f"| {executor['executor_id']} | {executor.get('removed_time') or ''} | {reason} |"
            )

    return "\n".join(lines) + "\n"


def render_query_breakdown_table(queries: list[dict[str, Any]]) -> list[str]:
    if not queries:
        return ["No SQL query executions were recorded."]
    lines = [
        "| Query | Status | Duration | Jobs | Stages | Tasks | Failed Tasks | Retries | Description |",
        "|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for query in queries:
        jobs = query.get("jobs", {})
        stages = query.get("stages", {})
        tasks = query.get("tasks", {})
        retries = query.get("retries", {})
        retry_text = "yes" if retries.get("occurred") else "no"
        if retries.get("stage_retry_count") or retries.get("task_retry_attempts"):
            retry_text = (
                f"{retry_text}; stages={retries.get('stage_retry_count', 0)}, "
                f"task_attempts={retries.get('task_retry_attempts', 0)}"
            )
        stage_text = f"{stages.get('unique', 0)} unique / {stages.get('attempts', 0)} attempts"
        description = first_sentence(str(query.get("description") or ""), 120).replace("|", "\\|")
        duration = query.get("duration") or format_ms(to_int(query.get("duration_ms")))
        lines.append(
            f"| `{query.get('execution_id')}` | {query.get('status') or 'unknown'} | "
            f"{duration} | {jobs.get('total', 0)} | {stage_text} | "
            f"{tasks.get('total', 0)} | {tasks.get('failed', 0)} | "
            f"{retry_text} | {description} |"
        )
    lines.extend(
        [
            "",
            "Task counts are complete for full-file analysis. In large SHS directory mode, task counts are limited to failed or signature-bearing stages.",
        ]
    )
    return lines


def render_plan_list(label: str, values: list[str]) -> list[str]:
    if not values:
        return []
    lines = [f"- {label}:"]
    lines.extend(f"  - {value}" for value in values)
    return lines


def render_stage_table(stages: list[dict[str, Any]], value_kind: str) -> list[str]:
    if not stages:
        return ["No stages recorded."]
    lines = ["| Stage | Name | Tasks | Failed | Value | GC Ratio |", "|---|---|---:|---:|---:|---:|"]
    for stage in stages:
        if value_kind == "spill":
            value = format_bytes(stage["memory_spilled"]["total"] + stage["disk_spilled"]["total"])
        else:
            value = format_ms(stage["executor_run_time"]["total"])
        name = first_sentence(stage.get("name") or "", 80).replace("|", "\\|")
        lines.append(
            f"| {stage['stage_id']}.{stage['attempt_id']} | {name} | "
            f"{stage['tasks_seen']} | {stage['failed_tasks']} | {value} | "
            f"{stage['gc_time_ratio']:.1%} |"
        )
    return lines


def write_index(reports: list[dict[str, Any]], output_dir: Path) -> None:
    severity_counts: Counter[str] = Counter()
    apps: list[dict[str, Any]] = []
    for report in reports:
        summary = report["summary"]
        severity_counts.update(summary["findings"]["by_severity"])
        apps.append(
            {
                "source_file": summary["source_file"],
                "analysis_mode": summary.get("analysis_mode"),
                "app_id": summary.get("app_id"),
                "app_name": summary.get("app_name"),
                "findings": summary["findings"],
                "resolution_plan_items": summary.get("resolution_plan_items", 0),
                "jobs": summary["jobs"],
                "stages": summary["stages"],
                "tasks": summary["tasks"],
                "queries": summary.get("queries", {"total": 0, "failed": 0, "with_retries": 0}),
            }
        )

    index = {
        "generated_at": utc_now(),
        "application_count": len(reports),
        "findings_by_severity": dict(severity_counts),
        "applications": apps,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "index.json").write_text(
        json.dumps(index, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output_dir / "latest.md").write_text(render_index_markdown(index), encoding="utf-8")


def render_index_markdown(index: dict[str, Any]) -> str:
    lines = [
        "# Spark Event Log Agent Index",
        "",
        f"Generated: {index['generated_at']}",
        "",
        f"Applications checked: {index['application_count']}",
        "",
    ]
    if not index["applications"]:
        lines.append("No Spark event logs were found.")
        return "\n".join(lines) + "\n"

    lines.extend(
        [
            "| Application | Source | Mode | Queries | Findings | Plans | Failed Jobs | Failed Tasks |",
            "|---|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for app in index["applications"]:
        name = app.get("app_name") or app.get("app_id") or "unknown"
        source = Path(app["source_file"]).name
        lines.append(
            f"| {name} | `{source}` | `{app.get('analysis_mode') or 'full-file'}` | "
            f"{app.get('queries', {}).get('total', 0)} | "
            f"{app['findings']['total']} | "
            f"{app.get('resolution_plan_items', 0)} | "
            f"{app['jobs']['failed']} | {app['tasks']['failed']} |"
        )
    lines.extend(["", f"Findings by severity: `{dict(index['findings_by_severity'])}`"])
    return "\n".join(lines) + "\n"


def analyze_all(args: argparse.Namespace) -> list[dict[str, Any]]:
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    logs = (
        discover_shs_eventlog_dirs(input_dir, output_dir)
        if args.large_shs_mode
        else discover_logs(input_dir, output_dir)
    )
    reports: list[dict[str, Any]] = []
    for log_path in logs:
        if args.large_shs_mode:
            analyzer = LargeShsDirectoryAnalyzer(
                log_path,
                max_samples=args.max_samples,
                spill_warn_bytes=args.spill_warn_bytes,
                gc_warn_ratio=args.gc_warn_ratio,
                gc_critical_ratio=args.gc_critical_ratio,
                skew_ratio=args.skew_ratio,
                max_deep_stages=args.large_shs_max_stages,
            )
        else:
            analyzer = SparkEventLogAnalyzer(
                log_path,
                max_samples=args.max_samples,
                spill_warn_bytes=args.spill_warn_bytes,
                gc_warn_ratio=args.gc_warn_ratio,
                gc_critical_ratio=args.gc_critical_ratio,
                skew_ratio=args.skew_ratio,
            )
        report = analyzer.analyze()
        write_report(report, output_dir, log_path)
        reports.append(report)
        summary = report["summary"]
        print(
            "[agent] "
            f"{log_path}: {summary['event_count']} event lines, "
            f"{summary.get('parsed_event_count', summary['event_count'])} parsed, "
            f"{summary['findings']['total']} findings"
        )
    write_index(reports, output_dir)
    if not logs:
        if args.large_shs_mode:
            print(f"[agent] no eventlog_v2_* directories found in {input_dir}")
        else:
            print(f"[agent] no candidate Spark event logs found in {input_dir}")
    return reports


def snapshot(
    input_dir: Path, output_dir: Path, large_shs_mode: bool
) -> dict[str, tuple[int, int]]:
    state: dict[str, tuple[int, int]] = {}
    if large_shs_mode:
        for directory in discover_shs_eventlog_dirs(input_dir, output_dir):
            for path in event_files_for(directory):
                stat = path.stat()
                state[str(path)] = (stat.st_mtime_ns, stat.st_size)
    else:
        for path in discover_logs(input_dir, output_dir):
            stat = path.stat()
            state[str(path)] = (stat.st_mtime_ns, stat.st_size)
    return state


def watch(args: argparse.Namespace) -> None:
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    previous: dict[str, tuple[int, int]] = {}
    while True:
        current = snapshot(input_dir, output_dir, args.large_shs_mode)
        if current != previous:
            analyze_all(args)
            previous = current
        time.sleep(args.interval)


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Spark event logs and write health reports.")
    parser.add_argument("--input-dir", default=os.getenv("OUTPUT_DIR", "/shs-logs"))
    parser.add_argument(
        "--output-dir",
        default=os.getenv("SPARK_EVENT_AGENT_REPORT_DIR", "/shs-logs/_agent-reports"),
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        default=env_bool("SPARK_EVENT_AGENT_WATCH", False),
        help="Continuously check for changed event-log files.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=to_int(os.getenv("SPARK_EVENT_AGENT_INTERVAL"), 300),
        help="Watch interval in seconds.",
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=to_int(os.getenv("SPARK_EVENT_AGENT_MAX_SAMPLES"), DEFAULT_MAX_SAMPLES),
        help="Maximum metric samples kept per stage for median/skew checks.",
    )
    parser.add_argument(
        "--spill-warn-bytes",
        type=int,
        default=to_int(os.getenv("SPARK_EVENT_AGENT_SPILL_WARN_BYTES"), 1024 * 1024 * 1024),
    )
    parser.add_argument(
        "--gc-warn-ratio",
        type=float,
        default=float(os.getenv("SPARK_EVENT_AGENT_GC_WARN_RATIO", "0.10")),
    )
    parser.add_argument(
        "--gc-critical-ratio",
        type=float,
        default=float(os.getenv("SPARK_EVENT_AGENT_GC_CRITICAL_RATIO", "0.25")),
    )
    parser.add_argument(
        "--skew-ratio",
        type=float,
        default=float(os.getenv("SPARK_EVENT_AGENT_SKEW_RATIO", "5.0")),
    )
    parser.add_argument(
        "--large-shs-mode",
        action="store_true",
        default=env_bool("SPARK_EVENT_AGENT_LARGE_SHS_MODE", False),
        help=(
            "Analyze eventlog_v2_* directories with a streaming first pass and "
            "deep-parse TaskEnd metrics only for failed or signature-bearing stages."
        ),
    )
    parser.add_argument(
        "--large-shs-max-stages",
        type=int,
        default=to_int(os.getenv("SPARK_EVENT_AGENT_LARGE_SHS_MAX_STAGES"), 50),
        help="Maximum failed/suspect stages to deep-parse in large SHS directory mode.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.interval < 1:
        raise SystemExit("--interval must be >= 1")
    if args.max_samples < 100:
        raise SystemExit("--max-samples must be >= 100")
    if args.large_shs_max_stages < 0:
        raise SystemExit("--large-shs-max-stages must be >= 0")
    if args.watch:
        watch(args)
    else:
        analyze_all(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
