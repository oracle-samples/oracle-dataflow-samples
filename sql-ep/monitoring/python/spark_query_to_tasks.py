import os
import json
from collections import defaultdict
import pandas as pd

# --- Path to directory containing event logs ---
EVENT_LOG_DIR = "/Users/mmiola/Downloads/bbd41494bbf5384b9ca1720c5e8e70e2-2/eventlog_v2_spark-f067dbe8385743f186fd0c3c043d0423"

# Collect all files in directory
event_files = [
    os.path.join(EVENT_LOG_DIR, f)
    for f in os.listdir(EVENT_LOG_DIR)
    if os.path.isfile(os.path.join(EVENT_LOG_DIR, f))
]

print(f"📂 Found {len(event_files)} event files")

# --- Data Structures ---
execution_metadata = {}
execution_to_jobs = defaultdict(set)
execution_to_stages = defaultdict(set)
execution_times = defaultdict(lambda: {"start": None, "end": None})

job_to_stages = defaultdict(set)
job_times = defaultdict(lambda: {"start": None, "end": None})

stage_to_tasks = defaultdict(int)
stage_to_failed_tasks = defaultdict(int)
stage_times = defaultdict(lambda: {"start": None, "end": None})

# (Optional) Track task-level metrics
task_metrics = defaultdict(lambda: {
    "executor_run_time": 0,
    "executor_cpu_time": 0,
    "jvm_gc_time": 0,
    "memory_spill": 0,
    "disk_spill": 0,
    "shuffle_write": 0,
    "input_bytes": 0,
    "failed": 0
})

# --- Parse all event logs ---
# --- Parse all event logs ---
for file in event_files:
    print(f"🔎 Processing {file}")
    with open(file, "r") as f:
        for line in f:
            line = line.strip()
            if not line:  # skip empty lines
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Skipping invalid line in {file}: {e}")
                continue

            etype = event.get("Event")
            if not etype:
                continue

            # --- SQL Execution Start/End ---
            if etype == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                exec_id = event["executionId"]
                execution_metadata[exec_id] = {"desc": event.get("description", "")}
                execution_times[exec_id]["start"] = event.get("time")

            elif etype == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
                exec_id = event["executionId"]
                execution_times[exec_id]["end"] = event.get("time")

            # --- Job Events ---
            elif etype == "SparkListenerJobStart":
                job_id = event["Job ID"]
                job_times[job_id]["start"] = event.get("Submission Time")
                for sid in event["Stage IDs"]:
                    job_to_stages[job_id].add(sid)

                # link job → sql execution if available
                if "Properties" in event and "spark.sql.execution.id" in event["Properties"]:
                    exec_id = int(event["Properties"]["spark.sql.execution.id"])
                    execution_to_jobs[exec_id].add(job_id)

            elif etype == "SparkListenerJobEnd":
                job_id = event["Job ID"]
                job_times[job_id]["end"] = event.get("Completion Time")

            # --- Stage Events ---
            elif etype == "SparkListenerStageSubmitted":
                sid = event["Stage Info"]["Stage ID"]
                stage_times[sid]["start"] = event["Stage Info"].get("Submission Time")

            elif etype == "SparkListenerStageCompleted":
                sid = event["Stage Info"]["Stage ID"]
                stage_times[sid]["end"] = event["Stage Info"].get("Completion Time")

            # --- Task End Events ---
            elif etype == "SparkListenerTaskEnd":
                sid = event["Stage ID"]
                stage_to_tasks[sid] += 1

                reason = event["Task End Reason"]
                if isinstance(reason, dict) and reason.get("Reason", "").lower() != "success":
                    stage_to_failed_tasks[sid] += 1

                tm = event.get("Task Metrics")
                if tm:
                    task_metrics[sid]["executor_run_time"] += tm.get("Executor Run Time", 0)
                    task_metrics[sid]["executor_cpu_time"] += tm.get("Executor CPU Time", 0)
                    task_metrics[sid]["jvm_gc_time"] += tm.get("JVM GC Time", 0)
                    task_metrics[sid]["memory_spill"] += tm.get("Memory Bytes Spilled", 0)
                    task_metrics[sid]["disk_spill"] += tm.get("Disk Bytes Spilled", 0)
                    task_metrics[sid]["shuffle_write"] += tm.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
                    task_metrics[sid]["input_bytes"] += tm.get("Input Metrics", {}).get("Bytes Read", 0)

# --- Aggregate ---
job_rows, stage_rows, sql_rows = [], [], []

# Stages
for stage_id in sorted(stage_to_tasks.keys()):
    times = stage_times[stage_id]
    dur = (times["end"] - times["start"]) if times["start"] and times["end"] else None
    stage_rows.append({
        "stage_id": stage_id,
        "num_tasks": stage_to_tasks[stage_id],
        "failed_tasks": stage_to_failed_tasks[stage_id],
        "duration_ms": dur
    })

# Jobs
for job_id, times in job_times.items():
    stages = job_to_stages[job_id]
    total_tasks = sum(stage_to_tasks[s] for s in stages)
    total_failed = sum(stage_to_failed_tasks[s] for s in stages)
    dur = (times["end"] - times["start"]) if times["start"] and times["end"] else None
    job_rows.append({
        "job_id": job_id,
        "num_stages": len(stages),
        "num_tasks": total_tasks,
        "failed_tasks": total_failed,
        "duration_ms": dur
    })

# SQL Executions
for exec_id, meta in execution_metadata.items():
    jobs = execution_to_jobs[exec_id]
    stages = set()
    for job_id in jobs:
        stages |= job_to_stages[job_id]
    total_tasks = sum(stage_to_tasks[s] for s in stages)
    total_failed = sum(stage_to_failed_tasks[s] for s in stages)
    dur = (execution_times[exec_id]["end"] - execution_times[exec_id]["start"]) \
          if execution_times[exec_id]["start"] and execution_times[exec_id]["end"] else None
    sql_rows.append({
        "execution_id": exec_id,
        "description": meta["desc"],
        "num_jobs": len(jobs),
        "num_stages": len(stages),
        "num_tasks": total_tasks,
        "failed_tasks": total_failed,
        "duration_ms": dur
    })

# --- Export CSVs ---
pd.DataFrame(sql_rows).to_csv("sql_executions.csv", index=False)
pd.DataFrame(job_rows).to_csv("jobs.csv", index=False)
pd.DataFrame(stage_rows).to_csv("stages.csv", index=False)

print("Exported: sql_executions.csv, jobs.csv, stages.csv")
