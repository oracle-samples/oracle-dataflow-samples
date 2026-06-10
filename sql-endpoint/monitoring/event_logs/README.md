# spark-shs-oci

Oracle Cloud Infrastructure Data Flow

Copyright © 2025, Oracle and/or its affiliates.

Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/

## Agent Requirements

- Python 3.10 or later to run `spark-shs-prep/scripts/analyze_event_logs.py` directly.
- No third-party Python packages are required for the event-log agent or eval runner.
- Read access to Spark JSON-lines event logs, either normalized files or Spark History Server `eventlog_v2_*` directories.
- Write access to the report directory, for example `/shs-logs/_agent-reports` in the container or `/tmp/spark-history-agent-reports` locally.
- Docker or Podman is required only for the full OCI downloader + repair + agent + Spark History Server image.
- NVIDIA Spark RAPIDS user tools are optional. The default image can parse existing qualification output; running qualification inside the container requires a RAPIDS-enabled image build.
- OCI namespace, bucket, region, and an OCI auth method are required only when downloading event logs from OCI Object Storage.

A single Docker image that:
1. Downloads Spark event logs from **OCI Object Storage** (as shipped by Fluent Bit)
2. Decompresses, repairs, sanitizes, and merges them into SHS-compatible plain-text files
3. Runs a **Spark Event Log Agent** that checks the normalized logs for failures and resource-pressure signatures
4. Optionally runs **NVIDIA Spark RAPIDS qualification** when built with RAPIDS tools, or parses existing RAPIDS output into reports
5. Starts the **Spark History Server** — UI available at `http://localhost:18080`

---

## Architecture

```
OCI Bucket (.log.gz / .lz4)
        │
        ▼  [background, every POLL_INTERVAL seconds]
   oci os object get
        │
        ▼
   gunzip / lz4 -d
        │
        ▼
   Fluent Bit unwrap + JSON repair + string sanitization
        │
        ▼
   filter SparkListener events
        │
        ▼
   /shs-logs/<app-id>    ← plain text, no extension
        │
        ├── optionally: spark_rapids qualification
        │                 [requires RAPIDS-enabled build]
        │                 → /shs-logs/_rapids-qualification
        ▼
   Spark Event Log Agent → /shs-logs/_agent-reports/*.report.{json,md}
        │
        ▼  [foreground]
   Spark History Server → http://localhost:18080
```

---

## Quick start

```bash
# 1. Copy and fill in your config
cp .env.example .env
vim .env   # set OCI_NAMESPACE, OCI_BUCKET, OCI_REGION at minimum

# 2. Build and run
docker compose up --build

# 3. Open the UI
open http://localhost:18080

# 4. Read agent findings
ls shs-logs/_agent-reports
cat shs-logs/_agent-reports/latest.md
```

---

## Current additions

- **Write-stall triage:** reports flag stages with `SparkListenerTaskStart`
  records that do not have matching `SparkListenerTaskEnd` records, then point
  operators at executor thread-dump patterns for object-store and Parquet writer
  stalls.
- **Large SHS mode:** the agent can stream `eventlog_v2_*` Spark History Server
  directories and deep-parse task metrics only for failed or signature-bearing
  stages.
- **RAPIDS qualification reporting:** reports can include NVIDIA Spark RAPIDS
  recommendations, estimated GPU speedup, unsupported operators, write
  operations, and tuning excerpts.
- **Optional RAPIDS image dependency:** the default container image remains
  smaller and does not install RAPIDS tools; use `INSTALL_SPARK_RAPIDS_TOOLS=true`
  only when the container should execute `spark_rapids qualification`.

---

## Container build options

The default image does not install NVIDIA Spark RAPIDS user tools. The event-log
agent can still parse an existing RAPIDS qualification output directory when
`SPARK_EVENT_AGENT_RAPIDS_QUALIFICATION_OUTPUT_DIR` points at it.

To run `spark_rapids qualification` from inside this container, build with:

```bash
INSTALL_SPARK_RAPIDS_TOOLS=true docker compose build
```

Then enable runtime execution through `.env` or the shell:

```bash
SPARK_EVENT_AGENT_RAPIDS_QUALIFICATION_ENABLED=true docker compose up
```

You can pin the Python package version when needed:

```bash
INSTALL_SPARK_RAPIDS_TOOLS=true \
SPARK_RAPIDS_USER_TOOLS_VERSION=26.4.5 \
docker compose build
```

The direct Docker or Podman build equivalent is:

```bash
docker build \
  --build-arg INSTALL_SPARK_RAPIDS_TOOLS=true \
  -t spark-shs-oci:rapids \
  spark-shs-prep
```

---

## Run with docker directly

```bash
docker build -t spark-shs-oci:latest spark-shs-prep

docker run -d \
  --name spark-history-server \
  -p 18080:18080 \
  -e OCI_NAMESPACE=mytenancy \
  -e OCI_BUCKET=my-spark-logs \
  -e OCI_REGION=eu-frankfurt-1 \
  -e OCI_PREFIX=spark-logs/ \
  -e WATCH_MODE=true \
  -e POLL_INTERVAL=120 \
  -v ~/.oci:/root/.oci:ro \
  -v shs-logs:/shs-logs \
  spark-shs-oci:latest
```

---

## Authentication

| Method | How to configure |
|---|---|
| **API Key** (local dev) | Mount `~/.oci` — set `OCI_CONFIG_DIR=~/.oci` in `.env` |
| **Instance Principal** (OCI Compute / OKE) | Set `OCI_CLI_AUTH=instance_principal` |
| **Resource Principal** (OCI Functions / OKE Workload Identity) | Set `OCI_CLI_AUTH=resource_principal` |

---

## Environment variables

### OCI

| Variable | Required | Default | Description |
|---|---|---|---|
| `OCI_NAMESPACE` | ✅ | — | Tenancy namespace |
| `OCI_BUCKET` | ✅ | — | Bucket name |
| `OCI_REGION` | ✅ | — | Region (e.g. `eu-frankfurt-1`) |
| `OCI_PREFIX` | — | *(all)* | Object prefix filter |
| `OCI_CLI_AUTH` | — | API key | `instance_principal` / `resource_principal` |

### Log preparer

| Variable | Default | Description |
|---|---|---|
| `FLUENT_BIT_WRAPPED` | `auto` | `auto` / `true` / `false` |
| `WATCH_MODE` | `true` | Poll OCI continuously |
| `POLL_INTERVAL` | `300` | Seconds between polls |
| `KEEP_RAW` | `false` | Keep raw downloaded files |
| `SPARK_EVENT_REPAIR_ENABLED` | `true` | Validate/sanitize event JSON before SHS reads it |
| `SPARK_EVENT_MAX_STRING_CHARS` | `18000000` | Truncate string fields larger than this threshold |
| `SPARK_EVENT_REPAIR_REPORT_DIR` | `/shs-logs/_repair-reports` | Per-input repair stats |

The repair step incorporates the same behavior as the local `fix_spark_event_logs.py` and `sanitize_spark_events_log.py` helpers:

- keeps only valid Spark event JSON lines
- stops at the first corrupt Spark event record, keeping the valid prefix
- truncates oversized string fields so Spark History Server can load the file
- writes per-file stats such as `stopped_at_line`, `corruption_message`, and `truncated_strings`

### Spark Event Log Agent

| Variable | Default | Description |
|---|---|---|
| `SPARK_EVENT_AGENT_ENABLED` | `true` | Run the checker after log preparation |
| `SPARK_EVENT_AGENT_REPORT_DIR` | `/shs-logs/_agent-reports` | JSON and Markdown report directory |
| `SPARK_EVENT_AGENT_INTERVAL` | `300` | Seconds between checks in watch mode |
| `SPARK_EVENT_AGENT_MAX_SAMPLES` | `5000` | Per-stage samples retained for skew medians |
| `SPARK_EVENT_AGENT_LARGE_SHS_MODE` | `false` | Stream existing `eventlog_v2_*` SHS directories instead of full-parsing normalized event-log files |
| `SPARK_EVENT_AGENT_LARGE_SHS_MAX_STAGES` | `50` | Maximum failed or signature-bearing stages to deep-parse in large SHS mode |
| `SPARK_EVENT_AGENT_SPILL_WARN_BYTES` | `1073741824` | Stage spill warning threshold |
| `SPARK_EVENT_AGENT_GC_WARN_RATIO` | `0.10` | GC time share warning threshold |
| `SPARK_EVENT_AGENT_GC_CRITICAL_RATIO` | `0.25` | GC time share high-severity threshold |
| `SPARK_EVENT_AGENT_SKEW_RATIO` | `5.0` | Max-to-median skew threshold |
| `SPARK_EVENT_AGENT_RAPIDS_QUALIFICATION_ENABLED` | `false` | Run `spark_rapids qualification` before writing reports |
| `SPARK_EVENT_AGENT_RAPIDS_QUALIFICATION_PLATFORM` | `onprem` | Platform passed to RAPIDS qualification |
| `SPARK_EVENT_AGENT_RAPIDS_QUALIFICATION_OUTPUT_DIR` | `/shs-logs/_rapids-qualification` | RAPIDS output directory; existing latest run is parsed even when execution is disabled |
| `SPARK_EVENT_AGENT_RAPIDS_QUALIFICATION_EVENTLOGS` | *(auto)* | Optional explicit RAPIDS `--eventlogs` value; defaults to discovered event logs as `file://` URIs |
| `SPARK_EVENT_AGENT_RAPIDS_QUALIFICATION_EXTRA_ARGS` | *(empty)* | Extra shell-style arguments appended to `spark_rapids qualification` |

Running qualification inside the container requires an image built with
`INSTALL_SPARK_RAPIDS_TOOLS=true`. Without that build arg, the runtime
qualification step reports that `spark_rapids` is unavailable, but the agent
still parses existing RAPIDS output in the configured output directory.

The agent reads the normalized files in `/shs-logs` and writes:

| File | Description |
|---|---|
| `/shs-logs/_agent-reports/index.json` | Machine-readable summary for all checked applications |
| `/shs-logs/_agent-reports/latest.md` | Markdown index of the latest run |
| `/shs-logs/_agent-reports/<app>.report.json` | Detailed application report |
| `/shs-logs/_agent-reports/<app>.report.md` | Human-readable application findings |

Current checks include failed jobs/stages/tasks, task starts without matching task-end metrics, executor-loss and fetch-failure signatures, JVM/container/direct-memory OOM signatures, high spill, high GC time, high fetch wait, and task/input/shuffle skew. Reports include a per-query breakdown with job, stage, task, duration, and retry evidence when Spark SQL execution metadata is present, plus a structured resolution plan. When RAPIDS qualification output is available, reports add a RAPIDS section with the matched app recommendation, estimated speedup, unsupported execs/operators, stage details, write operations, and tuning recommendation excerpts. Data-health plans are emitted first when parse errors, missing events, or incomplete logs mean the SHS input should be fixed before application tuning.

Example run with RAPIDS qualification enabled:

```bash
python3 spark-shs-prep/scripts/analyze_event_logs.py \
  --input-dir /Users/mmiola/Downloads/spark_history \
  --output-dir /tmp/spark-history-agent-reports \
  --rapids-qualification \
  --rapids-platform onprem \
  --rapids-output-dir /tmp/rapids-qualification
```

If RAPIDS has already been run, omit `--rapids-qualification` and point `--rapids-output-dir` at the existing output directory. The agent parses the newest qualification run it finds.

When a full event log contains many `SparkListenerTaskStart` records without matching `SparkListenerTaskEnd` records, the agent emits `TASK_STARTS_WITHOUT_ENDS` and a `write-stall-triage` plan. This does not prove an object-store stall by itself. It tells operators to collect live executor thread dumps and look for Spark task threads blocked in OCI Object Storage or Parquet output paths such as `ObjectStorageClient.headObject`, `BmcDataStore.getFileStatus`, `BmcFilesystemImpl.create`, `HadoopOutputFile.create`, or `ParquetOutputWriter`. If confirmed, reduce final write fanout with `coalesce(N)` or `repartition(N)` before `write.parquet(...)`, target fewer larger files, and avoid allowing hundreds or thousands of concurrent writers to hit Object Storage metadata APIs.

Large SHS directory mode is intended for already-downloaded Spark History Server directories such as `eventlog_v2_spark-application-*`. It scans all `events_*` files in numeric order, parses only app/job/stage/executor/SQL/signature-bearing events first, then deep-parses `SparkListenerTaskEnd` metrics only for failed or signature-bearing stages. This keeps OOM and executor-loss root-cause analysis practical on very large SHS trees. In this mode, report task totals are the targeted failed/suspect-stage task totals, not full-application task totals.

Example local run:

```bash
python3 spark-shs-prep/scripts/analyze_event_logs.py \
  --input-dir /Users/mmiola/Downloads/spark_history \
  --output-dir /tmp/spark-history-agent-reports \
  --large-shs-mode
```

Run the agent evals with:

```bash
python3 spark-shs-prep/evals/event_log_agent_eval.py
```

The eval runner is dependency-free and creates temporary Spark event-log fixtures for:

- normalized OOM plus fetch-after-executor-loss resolution plans
- unclosed task starts with write-stall triage guidance
- large SHS directory mode with targeted failed-stage `TaskEnd` parsing
- malformed event-log data-health findings
- RAPIDS qualification output attached to JSON, Markdown, and index reports

### Spark History Server

| Variable | Default | Description |
|---|---|---|
| `SHS_PORT` | `18080` | UI port |
| `SHS_MAX_APPLICATIONS` | `500` | Max apps in UI |
| `SHS_RETAINED_APPLICATIONS` | `100` | Apps kept in memory |
| `SHS_UPDATE_INTERVAL` | `10s` | Log directory rescan interval |
| `SHS_CLEANER_ENABLED` | `false` | Auto-delete old logs |
| `SHS_ACLS_ENABLED` | `false` | Enable ACL enforcement |

---

## REST API

SHS exposes a REST API alongside the UI:

```bash
# List all applications
curl http://localhost:18080/api/v1/applications

# Get details for a specific app
curl http://localhost:18080/api/v1/applications/<appId>

# List jobs for an app
curl http://localhost:18080/api/v1/applications/<appId>/jobs

# List stages
curl http://localhost:18080/api/v1/applications/<appId>/stages

# Executor summary
curl http://localhost:18080/api/v1/applications/<appId>/executors

# Environment (Spark config used)
curl http://localhost:18080/api/v1/applications/<appId>/environment
```

---

## Supported input formats

| Format | Handled |
|---|---|
| `.log.gz` (Fluent Bit gzip) | ✅ |
| `.log.lz4` | ✅ |
| `.lz4` (bare) | ✅ |
| `.gz` (bare) | ✅ |
| `.log` (plain) | ✅ |
| Fluent Bit JSON envelope | ✅ auto-detected |

---

## File layout

```
spark-shs-prep/
├── Dockerfile                  # Multi-stage: OCI CLI + Spark base → final image
├── docker-compose.yml          # Single-service compose
├── .env.example                # Config template
├── conf/
│   └── spark-defaults.conf     # Baked-in SHS defaults (overridden at runtime)
├── evals/
│   └── event_log_agent_eval.py # Dependency-free agent eval runner
└── scripts/
    ├── entrypoint.sh           # Writes spark-defaults.conf, starts preparer + agent + SHS
    ├── prepare_logs.sh         # OCI download + decompress + repair + clean pipeline
    ├── repair_spark_event_log.py # Validates/sanitizes JSON-lines events for SHS
    └── analyze_event_logs.py   # Spark event log checker and report writer
```

## Troubleshooting Tips

# Check Logs 
podman logs -f spark-history-server

# Remove the container and volume
podman rm -f spark-history-server
podman volume rm shs-logs

# Rebuild
podman build -t spark-shs-oci:latest ./spark-shs-prep
