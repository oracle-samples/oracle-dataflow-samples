#!/usr/bin/env python3
"""Dependency-free evals for the Spark event-log agent."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "analyze_event_logs.py"
sys.dont_write_bytecode = True


def load_agent_module() -> Any:
    spec = importlib.util.spec_from_file_location("analyze_event_logs", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load analyzer module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


AGENT = load_agent_module()
ANALYZER_KWARGS = {
    "max_samples": 5000,
    "spill_warn_bytes": 1024,
    "gc_warn_ratio": 0.10,
    "gc_critical_ratio": 0.25,
    "skew_ratio": 5.0,
}


@dataclass
class EvalResult:
    name: str
    passed: bool
    details: dict[str, Any]
    error: str = ""


def write_events(path: Path, events: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, separators=(",", ":")) + "\n")


def finding_codes(report: dict[str, Any]) -> set[str]:
    return {finding["code"] for finding in report["findings"]}


def plan_scopes(report: dict[str, Any]) -> set[str]:
    return {plan["scope"] for plan in report.get("resolution_plan", [])}


def require_contains(label: str, actual: set[str], expected: set[str]) -> None:
    missing = sorted(expected - actual)
    if missing:
        raise AssertionError(f"{label} missing expected values: {missing}; actual={sorted(actual)}")


def require_equal(label: str, actual: Any, expected: Any) -> None:
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def base_events(app_id: str = "spark-application-eval") -> list[dict[str, Any]]:
    return [
        {"Event": "SparkListenerLogStart", "Spark Version": "3.5.0"},
        {
            "Event": "SparkListenerApplicationStart",
            "App Name": "eval-app",
            "App ID": app_id,
            "User": "eval",
            "Timestamp": 1710000000000,
        },
        {
            "Event": "SparkListenerEnvironmentUpdate",
            "Environment Details": {
                "Spark Properties": {
                    "spark.executor.memory": "16g",
                    "spark.executor.memoryOverhead": "2g",
                    "spark.executor.cores": "8",
                    "spark.sql.shuffle.partitions": "200",
                }
            },
        },
    ]


def stage_info(
    stage_id: int,
    name: str,
    failure_reason: str = "",
    tasks: int = 2,
    attempt_id: int = 0,
) -> dict[str, Any]:
    info = {
        "Stage ID": stage_id,
        "Stage Attempt ID": attempt_id,
        "Stage Name": name,
        "Number of Tasks": tasks,
        "Submission Time": 1710000005000 + stage_id,
        "Completion Time": 1710000010000 + stage_id,
    }
    if failure_reason:
        info["Failure Reason"] = failure_reason
    return info


def failed_task_end(
    stage_id: int, class_name: str, description: str, attempt_id: int = 0
) -> dict[str, Any]:
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Stage Attempt ID": attempt_id,
        "Task End Reason": {
            "Reason": "ExceptionFailure",
            "Class Name": class_name,
            "Description": description,
        },
        "Task Metrics": {
            "Executor Run Time": 70000,
            "JVM GC Time": 2000,
            "Memory Bytes Spilled": 4096,
            "Disk Bytes Spilled": 4096,
            "Peak Execution Memory": 8192,
            "Input Metrics": {"Bytes Read": 1024},
            "Shuffle Read Metrics": {"Total Bytes Read": 2048, "Fetch Wait Time": 25000},
            "Shuffle Write Metrics": {"Shuffle Bytes Written": 1024},
        },
    }


def success_task_end(stage_id: int, attempt_id: int = 0) -> dict[str, Any]:
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Stage Attempt ID": attempt_id,
        "Task End Reason": {"Reason": "Success"},
        "Task Metrics": {
            "Executor Run Time": 1000,
            "JVM GC Time": 0,
            "Memory Bytes Spilled": 999999999,
            "Disk Bytes Spilled": 999999999,
        },
    }


def task_start(
    stage_id: int,
    task_id: int,
    index: int,
    executor_id: str,
    host: str = "10.0.0.1",
    attempt_id: int = 0,
) -> dict[str, Any]:
    return {
        "Event": "SparkListenerTaskStart",
        "Stage ID": stage_id,
        "Stage Attempt ID": attempt_id,
        "Task Info": {
            "Task ID": task_id,
            "Index": index,
            "Attempt": 0,
            "Partition ID": index,
            "Launch Time": 1710000010000 + index,
            "Executor ID": executor_id,
            "Host": host,
            "Locality": "PROCESS_LOCAL",
            "Speculative": False,
            "Getting Result Time": 0,
            "Finish Time": 0,
            "Failed": False,
            "Killed": False,
            "Accumulables": [],
        },
    }


def eval_full_file_oom_resolution(tmpdir: Path) -> dict[str, Any]:
    source = tmpdir / "normalized-event-log"
    failed_stage = stage_info(
        2,
        "failed reduce after executor loss",
        "org.apache.spark.shuffle.FetchFailedException after ExecutorDeadException",
    )
    retry_stage = stage_info(2, "retried reduce after executor loss", attempt_id=1)
    events = base_events() + [
        {
            "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "executionId": 42,
            "description": "select count(*) from eval_table",
            "details": "HashAggregate over eval_table",
            "time": 1710000000500,
        },
        {
            "Event": "SparkListenerJobStart",
            "Job ID": 10,
            "Submission Time": 1710000001000,
            "Stage IDs": [2],
            "Properties": {
                "spark.sql.execution.id": "42",
                "spark.job.description": "select count(*) from eval_table",
            },
        },
        {"Event": "SparkListenerStageSubmitted", "Stage Info": failed_stage},
        {
            "Event": "SparkListenerExecutorRemoved",
            "Timestamp": 1710000007000,
            "Executor ID": "7",
            "Removed Reason": "Container OOMKilled with exit code 137 while writing shuffle blocks",
        },
        failed_task_end(
            2,
            "org.apache.spark.shuffle.FetchFailedException",
            "FetchFailedException caused by ExecutorDeadException for executor 7",
        ),
        {"Event": "SparkListenerStageCompleted", "Stage Info": failed_stage},
        {"Event": "SparkListenerStageSubmitted", "Stage Info": retry_stage},
        success_task_end(2, attempt_id=1),
        {"Event": "SparkListenerStageCompleted", "Stage Info": retry_stage},
        {
            "Event": "SparkListenerJobEnd",
            "Job ID": 10,
            "Completion Time": 1710000011000,
            "Job Result": {
                "Result": "JobFailed",
                "Exception": "FetchFailedException after ExecutorDeadException",
            },
        },
        {
            "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
            "executionId": 42,
            "time": 1710000013000,
            "errorMessage": "FetchFailedException after ExecutorDeadException",
        },
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 1710000012000},
    ]
    write_events(source, events)

    report = AGENT.SparkEventLogAnalyzer(source, **ANALYZER_KWARGS).analyze()
    codes = finding_codes(report)
    scopes = plan_scopes(report)
    require_contains(
        "finding codes",
        codes,
        {
            "CONTAINER_OOM_OR_137",
            "FETCH_FAILED",
            "EXECUTOR_LOST",
            "FETCH_FAILED_AFTER_EXECUTOR_LOSS",
            "HIGH_STAGE_SPILL",
            "FAILED_STAGES",
        },
    )
    require_contains("plan scopes", scopes, {"oom-resolution", "shuffle-recovery", "resource-pressure"})
    require_equal("analysis mode", report["summary"]["analysis_mode"], "full-file")
    require_equal("query count", report["summary"]["queries"]["total"], 1)
    query = report["query_breakdown"][0]
    require_equal("query id", query["execution_id"], 42)
    require_equal("query jobs", query["jobs"]["total"], 1)
    require_equal("query unique stages", query["stages"]["unique"], 1)
    require_equal("query stage attempts", query["stages"]["attempts"], 2)
    require_equal("query tasks", query["tasks"]["total"], 2)
    require_equal("query retries", query["retries"]["occurred"], True)
    return {
        "event_count": report["summary"]["event_count"],
        "finding_codes": sorted(codes),
        "plan_scopes": sorted(scopes),
        "query_breakdown": report["query_breakdown"],
    }


def eval_unclosed_task_starts_write_stall(tmpdir: Path) -> dict[str, Any]:
    source = tmpdir / "write-stall-event-log"
    write_stage = stage_info(55, "write parquet files", tasks=12)
    write_stage.pop("Completion Time", None)
    events = base_events("spark-application-write-stall") + [
        {
            "Event": "SparkListenerJobStart",
            "Job ID": 99,
            "Submission Time": 1710000001000,
            "Stage IDs": [55],
            "Properties": {"spark.job.description": "write parquet"},
        },
        {"Event": "SparkListenerStageSubmitted", "Stage Info": write_stage},
    ]
    events.extend(
        task_start(55, task_id=172000 + index, index=index, executor_id=str(index % 3))
        for index in range(12)
    )
    write_events(source, events)

    report = AGENT.SparkEventLogAnalyzer(source, **ANALYZER_KWARGS).analyze()
    codes = finding_codes(report)
    scopes = plan_scopes(report)
    stages = {stage["stage_id"]: stage for stage in report["stages"]}
    require_contains(
        "finding codes",
        codes,
        {"APPLICATION_INCOMPLETE", "TASK_STARTS_WITHOUT_ENDS"},
    )
    require_contains("plan scopes", scopes, {"data-health", "write-stall-triage"})
    require_equal("started task count", report["summary"]["tasks"]["started"], 12)
    require_equal("ended task count", report["summary"]["tasks"]["total"], 0)
    require_equal("unclosed starts", report["summary"]["tasks"]["unclosed_starts"], 12)
    require_equal("stage unclosed starts", stages[55]["unclosed_task_starts"], 12)
    return {
        "event_count": report["summary"]["event_count"],
        "finding_codes": sorted(codes),
        "plan_scopes": sorted(scopes),
        "stage": stages[55],
    }


def eval_large_shs_targeted_deep_parse(tmpdir: Path) -> dict[str, Any]:
    event_dir = tmpdir / "cluster-a" / "eventlog_v2_spark-application-large-eval"
    healthy_stage = stage_info(1, "healthy shuffle map", tasks=1)
    failed_stage = stage_info(
        2,
        "failed reduce after executor loss",
        "org.apache.spark.shuffle.FetchFailedException: Missing an output location for shuffle 9 after ExecutorDeadException",
    )
    write_events(
        event_dir / "events_1_spark-application-large-eval",
        base_events("spark-application-large-eval")
        + [
            {"Event": "SparkListenerJobStart", "Job ID": 10, "Submission Time": 1710000001000, "Stage IDs": [1, 2]},
            {"Event": "SparkListenerStageSubmitted", "Stage Info": healthy_stage},
            {"Event": "SparkListenerStageCompleted", "Stage Info": healthy_stage},
            success_task_end(1),
        ],
    )
    write_events(
        event_dir / "events_2_spark-application-large-eval",
        [
            {"Event": "SparkListenerStageSubmitted", "Stage Info": failed_stage},
            {
                "Event": "SparkListenerExecutorRemoved",
                "Timestamp": 1710000007000,
                "Executor ID": "7",
                "Removed Reason": "Container OOMKilled with exit code 137 while writing shuffle blocks",
            },
            failed_task_end(
                2,
                "org.apache.spark.shuffle.FetchFailedException",
                "FetchFailedException caused by ExecutorDeadException for executor 7",
            ),
            failed_task_end(
                2,
                "java.lang.OutOfMemoryError",
                "java.lang.OutOfMemoryError: Java heap space",
            ),
            {"Event": "SparkListenerStageCompleted", "Stage Info": failed_stage},
            {
                "Event": "SparkListenerJobEnd",
                "Job ID": 10,
                "Completion Time": 1710000011000,
                "Job Result": {
                    "Result": "JobFailed",
                    "Exception": "FetchFailedException after ExecutorDeadException",
                },
            },
            {"Event": "SparkListenerApplicationEnd", "Timestamp": 1710000012000},
        ],
    )

    report = AGENT.LargeShsDirectoryAnalyzer(
        event_dir, **ANALYZER_KWARGS, max_deep_stages=50
    ).analyze()
    codes = finding_codes(report)
    metadata = report["summary"]["analysis_metadata"]
    stages = {stage["stage_id"]: stage for stage in report["stages"]}
    require_contains(
        "finding codes",
        codes,
        {
            "CONTAINER_OOM_OR_137",
            "JVM_HEAP_OOM",
            "FETCH_FAILED_AFTER_EXECUTOR_LOSS",
            "HIGH_STAGE_SPILL",
            "HIGH_FAILED_TASK_RATE",
        },
    )
    require_equal("analysis mode", report["summary"]["analysis_mode"], "large-shs-stream")
    require_equal("deep stage keys", metadata["deep_stage_keys"], ["2.0"])
    require_equal("deep task events", metadata["deep_task_events"], 2)
    require_equal("healthy stage task count", stages[1]["tasks_seen"], 0)
    require_equal("failed stage task count", stages[2]["tasks_seen"], 2)
    return {
        "event_count": report["summary"]["event_count"],
        "parsed_event_count": report["summary"]["parsed_event_count"],
        "deep_stage_keys": metadata["deep_stage_keys"],
        "finding_codes": sorted(codes),
    }


def eval_parse_error_data_health(tmpdir: Path) -> dict[str, Any]:
    source = tmpdir / "corrupt-event-log"
    source.write_text(
        "\n".join(
            [
                json.dumps(base_events()[0], separators=(",", ":")),
                '{"Event":"SparkListenerApplicationStart","App Name":"broken"',
                json.dumps(
                    {
                        "Event": "SparkListenerApplicationEnd",
                        "Timestamp": 1710000012000,
                    },
                    separators=(",", ":"),
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = AGENT.SparkEventLogAnalyzer(source, **ANALYZER_KWARGS).analyze()
    codes = finding_codes(report)
    scopes = plan_scopes(report)
    require_contains("finding codes", codes, {"PARSE_ERRORS"})
    require_contains("plan scopes", scopes, {"data-health"})
    require_equal("parse error samples", report["summary"]["parse_error_samples"], 1)
    return {
        "event_count": report["summary"]["event_count"],
        "parse_error_samples": report["summary"]["parse_error_samples"],
        "finding_codes": sorted(codes),
        "plan_scopes": sorted(scopes),
    }


EVALS: list[tuple[str, Callable[[Path], dict[str, Any]]]] = [
    ("full_file_oom_resolution", eval_full_file_oom_resolution),
    ("unclosed_task_starts_write_stall", eval_unclosed_task_starts_write_stall),
    ("large_shs_targeted_deep_parse", eval_large_shs_targeted_deep_parse),
    ("parse_error_data_health", eval_parse_error_data_health),
]


def run_eval(name: str, func: Callable[[Path], dict[str, Any]], keep_tmp: bool) -> EvalResult:
    if keep_tmp:
        tmpdir = Path(tempfile.mkdtemp(prefix=f"spark-event-agent-eval-{name}-"))
        try:
            details = func(tmpdir)
            details["tmpdir"] = str(tmpdir)
            return EvalResult(name=name, passed=True, details=details)
        except Exception as exc:  # noqa: BLE001 - eval runner should report all failures.
            return EvalResult(name=name, passed=False, details={"tmpdir": str(tmpdir)}, error=str(exc))

    with tempfile.TemporaryDirectory(prefix=f"spark-event-agent-eval-{name}-") as tmp:
        tmpdir = Path(tmp)
        try:
            return EvalResult(name=name, passed=True, details=func(tmpdir))
        except Exception as exc:  # noqa: BLE001 - eval runner should report all failures.
            return EvalResult(name=name, passed=False, details={}, error=str(exc))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spark event-log agent evals.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable eval results.")
    parser.add_argument("--keep-tmp", action="store_true", help="Keep generated eval fixtures.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    results = [run_eval(name, func, args.keep_tmp) for name, func in EVALS]
    if args.json:
        print(
            json.dumps(
                {
                    "passed": sum(result.passed for result in results),
                    "total": len(results),
                    "results": [result.__dict__ for result in results],
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        for result in results:
            status = "PASS" if result.passed else "FAIL"
            print(f"{status} {result.name}")
            if result.error:
                print(f"  {result.error}")
        print(f"{sum(result.passed for result in results)}/{len(results)} evals passed")
    return 0 if all(result.passed for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
