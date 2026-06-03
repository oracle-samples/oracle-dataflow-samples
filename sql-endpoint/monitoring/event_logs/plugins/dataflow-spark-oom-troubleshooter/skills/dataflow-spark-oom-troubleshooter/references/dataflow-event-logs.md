# Data Flow Event Log Workflow

Use this reference when a troubleshooting request involves the `sql-endpoint/monitoring/event_logs` sample, Spark event logs, or the Spark History Server container.

## Spark History Server Inputs

The event-log sample prepares Data Flow / Spark event logs from OCI Object Storage and exposes Spark History Server at:

```text
http://localhost:18080
```

Useful REST endpoints:

```bash
curl http://localhost:18080/api/v1/applications
curl http://localhost:18080/api/v1/applications/<appId>
curl http://localhost:18080/api/v1/applications/<appId>/jobs
curl http://localhost:18080/api/v1/applications/<appId>/stages
curl http://localhost:18080/api/v1/applications/<appId>/executors
curl http://localhost:18080/api/v1/applications/<appId>/environment
```

Ask for the JSON from these endpoints when the user has event logs but not Spark UI screenshots.

## What To Extract

- From `applications`: application ID, attempt ID, start/end time, duration.
- From `environment`: Spark configs for executor memory, overhead, cores, instances, AQE, broadcast thresholds, shuffle partitions, advisory partition size, off-heap, dynamic allocation, and event log settings.
- From `executors`: failed executors, peak memory metrics, total tasks, failed tasks, shuffle read/write, GC time, removed reason when available.
- From `stages`: failed stage ID, parent stages, task count, input, shuffle read/write, spill, failure reason, and skewed task metrics.
- From driver/executor logs: Kubernetes `OOMKilled`, exit 137, `FetchFailedException`, `ExecutorDeadException`, `OutOfMemoryError`, Netty/direct-buffer failures, disk failures, or heartbeat timeout.

## Diagnosis Pattern

1. Treat `FetchFailedException` as a symptom until executor loss is ruled out.
2. If an executor was `OOMKilled`, explain that Kubernetes killed the container and later fetch failures are downstream effects.
3. Compare executor concurrency to memory overhead. High executor cores can create container-level pressure even when total RAM is large.
4. Check if AQE coalesced shuffle partitions into large tasks.
5. Check if large broadcasts are resident while shuffle-heavy stages run.
6. Tie each recommendation to one observed metric or log line.
