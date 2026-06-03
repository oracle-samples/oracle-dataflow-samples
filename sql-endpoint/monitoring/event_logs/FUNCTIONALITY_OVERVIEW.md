# Spark Event Log Monitoring Functionality Overview

## Abstract

This project provides an integrated workflow for collecting, preparing, serving,
and analyzing Apache Spark event logs produced by Oracle Cloud Infrastructure
Data Flow workloads. It is designed for environments where Spark event data is
exported by a Fluent Bit sidecar to OCI Object Storage and must then be made
available to Spark History Server for inspection.

The solution combines four capabilities in a single containerized workflow:
continuous event-log download from OCI Object Storage, event cleansing and
sanitation for Spark History Server compatibility, Spark UI availability through
Spark History Server, and automated analysis of common runtime failures with a
structured resolution plan. The event-log agent focuses on practical root-cause
signals such as out-of-memory failures, executor loss, shuffle fetch failures,
high spill, high garbage collection time, skew, and malformed or incomplete log
data.

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
- Large Spark History Server directory analysis through a streaming mode that
  avoids full parsing of every task event when logs are very large.
- Evaluation fixtures that validate the core diagnosis paths and data-health
  behavior without requiring Spark or OCI.

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
        +------------------------------+
        |                              |
        v                              v
Spark History Server              Spark Event Log Agent
http://localhost:18080            /shs-logs/_agent-reports
        |                              |
        | Spark UI and REST API        | JSON and Markdown findings
        v                              v
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

The agent also generates a structured resolution plan. A plan can include:

- A root-cause statement with confidence level.
- Evidence from event-log fields, stages, executor removals, and Spark
  properties.
- Immediate mitigations to unblock a run.
- Durable fixes that reduce peak memory or shuffle pressure.
- Validation expectations for the next run.
- Rollback conditions when a mitigation increases cost or fails to reduce the
  observed failure.

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
