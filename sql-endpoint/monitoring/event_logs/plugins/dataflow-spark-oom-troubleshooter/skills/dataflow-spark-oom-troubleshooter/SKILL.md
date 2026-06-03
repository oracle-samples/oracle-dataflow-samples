---
name: dataflow-spark-oom-troubleshooter
description: Diagnose Oracle Data Flow and Spark SQL OOM and executor-loss incidents. Use when troubleshooting Spark event logs, Spark History Server metrics, physical plans, OutOfMemoryError, Kubernetes OOMKilled or exit 137, FetchFailedException, ExecutorDeadException, broadcast or shuffle memory pressure, AQE plan changes, memoryOverhead sizing, driver/executor logs, or cluster sizing.
---

# Data Flow Spark OOM Troubleshooter

## Core Rule

Separate the **reported failed stage** from the **root failure**. `FetchFailedException` often marks the stage that discovered missing shuffle blocks; the root cause is usually the earlier executor/container death that held those blocks.

## Evidence To Gather

Prefer concrete artifacts over speculation:

- Final/current adaptive physical plan and initial plan when available.
- Failed stage IDs, task metrics, shuffle read/write, input, spill, GC time, failed task host/executor.
- Executor loss reason from Spark UI, driver log, or cluster manager.
- Executor stderr/stdout around the killed executor.
- Spark configs: executor memory, `memoryOverhead`, executor cores, instances, shuffle partitions, AQE, broadcast thresholds, advisory partition size, off-heap.
- Spark UI SQL DAG to map stage IDs to operators.
- For the `sql-endpoint/monitoring/event_logs` sample, Spark History Server REST outputs such as applications, stages, executors, and environment configs.

If a decisive artifact is missing, ask for that specific item. Avoid broad log requests.

## Workflow

1. Classify the failure signature:
   - `exit code 137`, `SIGKILL`, `OOMKilled`: container/cgroup memory exceeded.
   - `java.lang.OutOfMemoryError: Java heap space`: JVM heap pressure.
   - `GC overhead limit exceeded`: heap churn or excessive live set.
   - `Direct buffer memory` or Netty errors: off-heap/direct/network pressure.
   - `FetchFailedException` plus `ExecutorDeadException`: victim stage lost shuffle data from a dead executor.
2. Map the failed stage to physical operators using the Spark UI SQL DAG and plan:
   - `BroadcastExchange` / `BroadcastHashJoin`: broadcast collection and resident hashed relation.
   - `HashAggregate`: grouping state, distinct, spills.
   - `Generate explode`, especially `explode(arrays_zip(...))`: transient row/object expansion.
   - `AQEShuffleRead coalesced`: AQE may have enlarged per-task input.
   - `UnsafeShuffleWriter.write`: shuffle writer buffers, sorting, spill, direct/native pressure.
3. Compare data volume to concurrency:
   - Per-task pressure is roughly stage bytes divided by actual reducer/task count, then amplified by skew, row width, broadcast state, and shuffle writer overhead.
   - High executor cores multiply per-container pressure because many tasks share one JVM/container.
4. Check broadcast behavior:
   - Confirm whether repeated usage shows `ReusedExchange`.
   - Treat broadcast size as resident executor memory during downstream tasks, not only as a completed exchange stage.
5. Check Kubernetes/container memory:
   - Container limit is roughly `spark.executor.memory + spark.executor.memoryOverhead` on Kubernetes.
   - Low GC time with `OOMKilled` often indicates container/off-heap/native/direct/aggregate memory, not classic heap thrash.
6. Produce a root-cause statement with confidence:
   - Name the immediate failure, the upstream cause, and the plan/operator behavior that created memory pressure.
   - Distinguish mitigations that preserve broadcast from mitigations that remove it.

## Analyzer Script

When the user provides plan/log files, run:

```bash
python3 scripts/analyze_spark_oom.py <file> [more-files...]
```

Run it from this skill directory, or use the full path to this skill's `scripts/analyze_spark_oom.py`. Use the script output as a triage aid, then reason from the original artifacts.

## Common Recommendations

Keep recommendations tied to evidence:

- For Kubernetes `OOMKilled` with low GC: increase `spark.executor.memoryOverhead`; for large executors start near 10-15% of executor memory, higher for heavy shuffle/broadcast.
- For high shuffle read/write per task: increase `spark.sql.shuffle.partitions`; guard against AQE coalescing with a smaller `spark.sql.adaptive.advisoryPartitionSizeInBytes` or disable coalescing for the test.
- For high executor core count: prefer more executors with fewer cores, commonly 8-16 cores each, to reduce per-container concurrency and shuffle-loss blast radius.
- To preserve useful broadcast: keep broadcast enabled, but verify reuse, reduce concurrent shuffle pressure, and size overhead for resident broadcast plus shuffle/network buffers.
- For `explode(arrays_zip(...))`: prefer exploding/filtering the original array of structs when possible, for example `explode(filter(array_col, x -> ...))`, then select struct fields.
- For FetchFailed after executor death: fix the executor loss cause and consider external shuffle service, shuffle tracking, or decommissioning support if available.

For detailed signatures, read `references/spark-oom-signatures.md`.
For the event-log monitoring sample and Spark History Server REST workflow, read `references/dataflow-event-logs.md`.
