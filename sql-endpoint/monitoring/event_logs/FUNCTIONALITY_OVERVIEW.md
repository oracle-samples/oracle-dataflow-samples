# Spark Event Log Monitoring Functionality Overview

## Abstract

This project provides an integrated workflow for collecting, preparing, serving,
and analyzing Apache Spark event logs produced by Oracle Cloud Infrastructure
Data Flow workloads. It is designed for environments where Spark event data is
exported by a Fluent Bit sidecar to OCI Object Storage and must then be made
available to Spark History Server for inspection.

The solution combines five capabilities in a single containerized workflow:
continuous event-log download from OCI Object Storage, event cleansing and
sanitation for Spark History Server compatibility, Spark UI availability through
Spark History Server, optional NVIDIA Spark RAPIDS qualification for GPU
migration assessment or output reporting, and automated analysis of common
runtime failures with a structured resolution plan. The event-log agent focuses
on practical root-cause signals such as out-of-memory failures, executor loss,
shuffle fetch failures, object-store write stalls, high spill, high garbage
collection time, skew, and malformed or incomplete log data.

## Functional Scope

The project supports the following primary functions:

- Continuous polling of OCI Object Storage for Spark event logs uploaded by a
  Fluent Bit sidecar.
- Decompression and normalization of supported log object formats.
- Cleansing and sanitation of Spark event JSON-lines data before Spark History
  Server reads it.
- Spark History Server startup with the prepared event-log directory mounted as
  its log source.
- Automated event-log analysis with JSON and Markdown reports.
- Optional NVIDIA Spark RAPIDS qualification of the same CPU event logs, plus
  parsing of existing qualification output.
- Large Spark History Server directory analysis through a streaming mode that
  avoids full parsing of every task event when logs are very large.
- Evaluation fixtures that validate the core diagnosis paths and data-health
  behavior without requiring Spark or OCI.

The Spark RAPIDS CLI is a build-time option, not a mandatory image dependency.
Default images can parse existing RAPIDS qualification output. Images built with
`INSTALL_SPARK_RAPIDS_TOOLS=true` can also execute `spark_rapids qualification`
before the agent writes reports.

## Current Additions

Recent additions focus on large event logs, stalled object-store writes, and GPU
migration assessment:

- Write-stall triage detects stages with `SparkListenerTaskStart` records that
  do not have matching `SparkListenerTaskEnd` records and directs operators to
  confirm blocked executor task threads with live thread dumps.
- Large SHS mode streams `eventlog_v2_*` directories and deep-parses
  `SparkListenerTaskEnd` metrics only for failed or signature-bearing stages.
- RAPIDS qualification context is folded into JSON, Markdown, and index reports
  when qualification output is available.
- RAPIDS tools are optional at image build time, keeping the default Spark
  History Server image smaller while still allowing a RAPIDS-enabled image to
  execute `spark_rapids qualification`.

## Schematic Process

```text
Spark application pod
        |
        | SparkListener event data
        v
Fluent Bit sidecar
        |
        | compressed log objects
        v
OCI Object Storage bucket
        |
        | continuous polling with OCI CLI
        | WATCH_MODE=true, POLL_INTERVAL=<seconds>
        v
Log preparation container
        |
        | download, decompress, unwrap Fluent Bit envelope
        v
Spark event cleansing and sanitation
        |
        | keep valid Spark event JSON lines
        | stop at corrupt Spark event prefix when necessary
        | truncate oversized string fields
        | write repair reports
        v
/shs-logs/<application-id>
        |
        +------------------------------+------------------------------+
        |                              |                              |
        v                              v                              v
Spark History Server              Spark Event Log Agent              RAPIDS Qualification
http://localhost:18080            /shs-logs/_agent-reports           /shs-logs/_rapids-qualification
        |                              |                              |
        | Spark UI and REST API        | JSON and Markdown findings   | GPU migration recommendation
        v                              v                              v
Interactive investigation          Root-cause feedback and resolution plan
```

## Continuous Event Download

Spark applications can produce event data that is exported by a Fluent Bit
sidecar. The sidecar writes log objects to an OCI Object Storage bucket, usually
as compressed files or wrapped log records. This project uses a background log
preparer process to continuously poll that bucket and bring new objects into the
Spark History Server log directory.

The continuous download behavior is controlled through the runtime environment:

| Setting | Purpose |
|---|---|
| `OCI_NAMESPACE` | Identifies the target OCI tenancy namespace. |
| `OCI_BUCKET` | Identifies the Object Storage bucket containing event logs. |
| `OCI_REGION` | Selects the OCI region. |
| `OCI_PREFIX` | Optionally restricts polling to a bucket prefix. |
| `OCI_CLI_AUTH` | Selects API key, security token, instance principal, or resource principal authentication. |
| `WATCH_MODE` | Enables continuous polling when set to `true`. |
| `POLL_INTERVAL` | Sets the interval between Object Storage checks. |
| `KEEP_RAW` | Controls whether raw downloaded objects are retained after processing. |

In watch mode, the container starts the log preparer in the background. It
downloads new objects, decompresses supported formats, normalizes their content,
and updates `/shs-logs` so Spark History Server can discover new or updated
applications.

## Cleansing and Sanitation

Raw log objects are not always immediately suitable for Spark History Server.
They can be compressed, wrapped by Fluent Bit JSON envelopes, partially written,
truncated, or contain oversized string fields that make Spark History Server
loading expensive or unreliable. The preparation pipeline therefore includes a
repair and sanitation phase before a file is exposed to Spark History Server.

The cleansing and sanitation process performs the following work:

1. Decompresses supported object formats such as gzip and lz4.
2. Detects whether Fluent Bit wrapping is present.
3. Extracts the Spark event payload from the wrapper when needed.
4. Keeps Spark event records that are valid JSON-lines data.
5. Stops at the first corrupt Spark event line when preserving the valid prefix
   is safer than passing malformed data to Spark History Server.
6. Truncates oversized string fields to avoid Spark History Server parser and
   memory pressure.
7. Writes repair metadata for auditability, including corruption line, stopped
   line, and truncation counts.

This phase is controlled by:

| Setting | Purpose |
|---|---|
| `SPARK_EVENT_REPAIR_ENABLED` | Enables validation and sanitation before Spark History Server reads the log. |
| `SPARK_EVENT_MAX_STRING_CHARS` | Defines the maximum string length retained in event JSON. |
| `SPARK_EVENT_REPAIR_REPORT_DIR` | Stores per-input repair reports. |
| `FLUENT_BIT_WRAPPED` | Controls wrapper detection with `auto`, `true`, or `false`. |

The output is a normalized Spark event-log file under `/shs-logs`. Spark History
Server reads that normalized file rather than the raw object downloaded from OCI.

## Spark UI Availability

After the preparation process starts, the container starts Spark History Server
in the foreground. Spark History Server reads from the prepared `/shs-logs`
directory and exposes the Spark UI on the configured port.

Primary access points are:

| Interface | Purpose |
|---|---|
| `http://localhost:18080` | Spark History Server UI for jobs, stages, SQL, executors, and environment. |
| `/api/v1/applications` | REST endpoint for application inventory. |
| `/api/v1/applications/<appId>/jobs` | REST endpoint for jobs. |
| `/api/v1/applications/<appId>/stages` | REST endpoint for stages. |
| `/api/v1/applications/<appId>/executors` | REST endpoint for executor-level information. |
| `/api/v1/applications/<appId>/environment` | REST endpoint for Spark configuration and runtime environment. |

Spark History Server remains the primary interactive investigation surface. The
agent complements it by summarizing common failure signals and producing a
resolution-oriented report.

## Automated Error Analysis and Resolution Feedback

The Spark Event Log Agent reads prepared Spark event logs and writes reports to
`/shs-logs/_agent-reports`. Each application receives a machine-readable JSON
report and a Markdown report. An index summarizes all checked applications.

The agent evaluates both data-health and runtime-failure signals:

| Finding Area | Examples |
|---|---|
| Data health | Parse errors, missing Spark events, incomplete applications. |
| Failed work | Failed jobs, failed stage attempts, high failed-task rate. |
| Memory pressure | Kubernetes or container `OOMKilled`, exit code `137`, JVM heap OOM, GC overhead, direct or native memory pressure. |
| Shuffle failures | `FetchFailedException`, missing shuffle blocks, executor death before fetch failure. |
| Resource pressure | High memory or disk spill, high fetch wait time, task runtime skew, input skew, shuffle-read skew. |
| Write stalls | Task starts without matching task-end metrics, with guidance to validate object-store or Parquet writer stalls using executor thread dumps. |
| GPU qualification | RAPIDS recommendation, estimated GPU speedup, unsupported execs/operators, write operations, and tuning recommendation excerpts when qualification output is present. |

The agent also generates a structured resolution plan. A plan can include:

- A root-cause statement with confidence level.
- Evidence from event-log fields, stages, executor removals, and Spark
  properties.
- Immediate mitigations to unblock a run.
- Durable fixes that reduce peak memory or shuffle pressure.
- Validation expectations for the next run.
- Rollback conditions when a mitigation increases cost or fails to reduce the
  observed failure.

### Example Output and Outcomes

The following examples illustrate the type of output produced by the agent. The
values are representative and are intended to show the report structure and the
operational feedback loop.

Example findings table:

| Severity | Code | Finding |
|---|---|---|
| critical | `CONTAINER_OOM_OR_137` | Container or memory-overhead OOM signature found in executor removal evidence. |
| high | `FETCH_FAILED_AFTER_EXECUTOR_LOSS` | A fetch failure and executor loss both appear, indicating that a downstream reducer likely discovered missing shuffle blocks after an executor died. |
| high | `TASK_STARTS_WITHOUT_ENDS` | A stage has task starts that never emitted task-end metrics, indicating live or incomplete work requiring executor thread dump validation. |
| high | `HIGH_STAGE_SPILL` | Stage `2.0` spilled a large amount of data across memory and disk. |
| medium | `HIGH_FETCH_WAIT` | The failed stage spent a high share of executor runtime waiting on shuffle fetches. |

Example query breakdown:

| Query | Status | Duration | Jobs | Stages | Tasks Ended | Unclosed Starts | Failed Tasks | Retries | Description |
|---|---|---:|---:|---:|---:|---:|---:|---|---|
| `42` | failed | 12.5 s | 1 | 1 unique / 2 attempts | 2 | 0 | 1 | yes; stages=1, task_attempts=0 | `select count(*) from eval_table` |

Example RAPIDS qualification excerpt:

| App | Recommendation | Speedup Category | Estimated Speedup | Notes |
|---|---|---|---:|---|
| `eval-app` | Recommended | Medium | 2.4 | Unsupported write path should be reviewed |

Example resolution-plan excerpt:

```text
Scope: oom-resolution
Title: Resolve container or memory-overhead OOM
Root cause: An executor/container was killed outside the JVM heap, usually from
overhead, direct/native memory, shuffle buffers, Python/native usage, or too
many concurrent tasks.
Immediate mitigation:
  - Increase spark.executor.memoryOverhead for the next run.
  - Reduce spark.executor.cores or use more smaller executors so fewer
    memory-heavy tasks share one container.
Durable fix:
  - Reduce per-task shuffle and spill pressure with more partitions, skew
    handling, and narrower rows.
Validation:
  - Executor removals with OOMKilled or exit 137 should disappear.
  - FetchFailed should not recur after the executor-loss root cause is removed.
```

Typical outcomes are:

| Input Evidence | Agent Output | Expected Operator Outcome |
|---|---|---|
| Corrupt or truncated Spark event lines | Data-health plan with parse-error samples and repair guidance | Re-download, repair, or regenerate event objects before relying on Spark UI analysis. |
| `OOMKilled`, exit code `137`, or memory-overhead text in executor removal | Container OOM plan with memory-overhead and concurrency recommendations | Resize executor memory overhead, reduce executor cores, or use more smaller executors. |
| `FetchFailedException` after executor loss | Shuffle-recovery plan that treats fetch failure as a symptom | Fix executor loss first, then rerun and verify missing shuffle blocks disappear. |
| Task starts without task ends in a write stage | Write-stall triage plan with executor thread-dump patterns | Confirm whether tasks are blocked in object-store metadata/write paths, then reduce final write fanout with `coalesce` or `repartition`. |
| RAPIDS qualification output for the same CPU event logs | RAPIDS section in the app report and index with recommendation, estimated speedup, unsupported execs, write operations, and tuning excerpts | Use GPU migration guidance for compute-heavy stages, while separately addressing object-store write stalls and unsupported operations. |
| High spill, high fetch wait, or skewed task metrics | Resource-pressure plan with partitioning and skew-handling recommendations | Increase or rebalance partitions, reduce row width, handle hot keys, and validate lower spill/fetch wait. |
| Large `eventlog_v2_*` directory with many task events | Large SHS streaming report with targeted failed-stage metrics | Obtain practical root-cause feedback without parsing every task event in the application. |

For very large Spark History Server directories, the agent can use large SHS
directory mode. In that mode it scans `eventlog_v2_*` directories in two passes:

1. First pass: parse only root-cause-relevant event types such as application,
   job, stage, executor, SQL, and signature-bearing events.
2. Second pass: deep-parse `SparkListenerTaskEnd` metrics only for failed or
   signature-bearing stages.

This makes analysis practical when event logs contain large numbers of task
events, while preserving the task metrics most relevant to root-cause analysis.

## Benefits

The project provides the following operational benefits:

- Single container workflow for OCI download, repair, Spark History Server, and
  automated diagnosis.
- Continuous Object Storage polling for event logs produced by Fluent Bit
  sidecars.
- Reduced Spark History Server failures by sanitizing malformed or oversized
  event data before UI loading.
- Direct Spark UI availability for detailed investigation.
- Automated first-pass triage for common Spark incidents.
- Optional RAPIDS qualification context in the same reports used for failure
  triage.
- Resolution plans that distinguish symptoms from likely root cause, especially
  fetch failures that follow executor loss.
- Large-directory analysis mode for practical handling of high-volume Spark
  History Server event data.
- Dependency-free evals that validate the agent's core behavior locally.

## Difficulties and Operational Considerations

The workflow also has limitations and operating considerations:

- Event-log completeness matters. Missing final objects can make an application
  appear incomplete or hide late executor failures.
- Fluent Bit wrapping and object layout can vary by deployment, so wrapper
  detection must be configured correctly when auto-detection is insufficient.
- Corrupt event data can only be repaired up to the point where a valid Spark
  event prefix is preserved. If the corrupt section contains the root failure,
  additional raw logs may still be required.
- Very large applications can still require significant streaming time,
  especially when a failed or suspect stage contains many task-end records.
- Spark event logs do not always include every artifact needed for definitive
  diagnosis. Executor stdout, stderr, Kubernetes pod events, and the final SQL
  physical plan may still be needed for high-confidence remediation.
- RAPIDS qualification estimates GPU migration opportunity from CPU event logs;
  it does not resolve object-store write stalls or unsupported operators by
  itself.
- Running RAPIDS qualification inside the container requires a RAPIDS-enabled
  image build; otherwise only existing qualification output is parsed.
- OCI authentication, network connectivity, and bucket permissions must be
  configured correctly before the downloader can operate continuously.
- Spark History Server resource sizing must match the number and size of
  retained applications.

## Conclusion

This project turns Spark event logs exported by Fluent Bit sidecars into an
operational troubleshooting workflow. It continuously downloads event data from
OCI Object Storage, repairs and sanitizes it for Spark History Server, exposes
the Spark UI for detailed inspection, and runs an event-log agent that identifies
common Spark failure modes and proposes resolution plans.

The result is a practical bridge between raw event-log collection and actionable
Spark operations. Spark History Server remains available for human inspection,
while the agent accelerates triage by highlighting data-health issues, memory
pressure, executor loss, shuffle failures, skew, spill, and other recurring
root-cause patterns.
