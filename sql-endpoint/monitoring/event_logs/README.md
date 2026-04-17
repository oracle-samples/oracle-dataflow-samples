# spark-shs-oci

#Oracle Cloud Infrastrure Dataflow
#
#Copyright © 2025, Oracle and/or its affiliates.
#
#Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/


A single Docker image that:
1. Downloads Spark event logs from **OCI Object Storage** (as shipped by Fluent Bit)
2. Decompresses, cleans, and merges them into SHS-compatible plain-text files
3. Starts the **Spark History Server** — UI available at `http://localhost:18080`

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
   Fluent Bit unwrap (auto-detected)
        │
        ▼
   grep SparkListener events
        │
        ▼
   /shs-logs/<app-id>    ← plain text, no extension
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
└── scripts/
    ├── entrypoint.sh           # Writes spark-defaults.conf, starts preparer + SHS
    └── prepare_logs.sh         # OCI download + decompress + clean pipeline
```

## Troubleshooting Tips

# Check Logs 
podman logs -f spark-history-server

# Remove the container and volume
podman rm -f spark-history-server
podman volume rm shs-logs

# Rebuild
podman build -t spark-shs-oci:latest ./spark-shs-prep
