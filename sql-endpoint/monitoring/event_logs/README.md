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
- OCI namespace, bucket, region, and an OCI auth method are required only when downloading event logs from OCI Object Storage.

A single Docker image that:
1. Downloads Spark event logs from **OCI Object Storage** (as shipped by Fluent Bit)
2. Decompresses, repairs, sanitizes, and merges them into SHS-compatible plain-text files
3. Runs a **Spark Event Log Agent** that checks the normalized logs for failures and resource-pressure signatures
4. Starts the **Spark History Server** — UI available at `http://localhost:18080`

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

## Run with docker directly

```bash
docker build -t spark-shs-oci:latest .

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

The agent reads the normalized files in `/shs-logs` and writes:

| File | Description |
|---|---|
| `/shs-logs/_agent-reports/index.json` | Machine-readable summary for all checked applications |
| `/shs-logs/_agent-reports/latest.md` | Markdown index of the latest run |
| `/shs-logs/_agent-reports/<app>.report.json` | Detailed application report |
| `/shs-logs/_agent-reports/<app>.report.md` | Human-readable application findings |

Current checks include failed jobs/stages/tasks, executor-loss and fetch-failure signatures, JVM/container/direct-memory OOM signatures, high spill, high GC time, high fetch wait, and task/input/shuffle skew. Reports include a per-query breakdown with job, stage, task, duration, and retry evidence when Spark SQL execution metadata is present, plus a structured resolution plan. Data-health plans are emitted first when parse errors, missing events, or incomplete logs mean the SHS input should be fixed before application tuning.

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
- large SHS directory mode with targeted failed-stage `TaskEnd` parsing
- malformed event-log data-health findings

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

## Codex troubleshooting plugin

This sample includes a Codex plugin for diagnosing Spark OOM, executor loss, `FetchFailedException`, broadcast, shuffle, AQE, and `memoryOverhead` issues from Data Flow event logs and Spark History Server output.

Plugin files:

```text
.agents/plugins/marketplace.json
plugins/dataflow-spark-oom-troubleshooter/
```

From this directory, add the repository-local marketplace to Codex:

```bash
codex plugin marketplace add "$(pwd)"
```

Then install or enable the `dataflow-spark-oom-troubleshooter` plugin in Codex. In a new Codex session, ask for the skill explicitly or describe the Spark incident:

```text
Use $dataflow-spark-oom-troubleshooter to diagnose this Data Flow Spark OOM.
```

Useful inputs for the skill include:

- Physical plan output from Spark SQL or DataFrame execution.
- Spark History Server REST output for applications, stages, executors, and environment.
- Driver and executor logs around the failed stage or killed executor.
- Cluster sizing and Spark configs such as executor memory, `spark.executor.memoryOverhead`, executor cores, shuffle partitions, AQE, and broadcast thresholds.

The plugin also includes a standalone triage helper:

```bash
python3 plugins/dataflow-spark-oom-troubleshooter/skills/dataflow-spark-oom-troubleshooter/scripts/analyze_spark_oom.py \
  plan.txt driver.log executor.log
```

The script flags common signatures such as Kubernetes `OOMKilled` / exit 137, `FetchFailedException` caused by executor loss, large broadcast exchanges, `explode(arrays_zip(...))`, low shuffle partition counts, and AQE coalesced shuffle reads.

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
