# Salesforce to OCI Object Storage

This sample verifies Salesforce connectivity and exports Salesforce records to
OCI Object Storage. It is designed to be tested interactively in OCI Cloud
Shell or on an OCI Compute VM before the same script is submitted to OCI Data
Flow.

The script is read-only in Salesforce. It supports standard objects such as
`Account` and `Opportunity`, custom objects such as `Project__c`, relationship
queries, bounded incremental extraction, and deleted records through
Salesforce `queryAll`.

## What the sample writes

An export produces immutable, gzip-compressed JSON Lines files followed by a
success manifest:

```text
salesforce/raw/Account/
  extract_date=2026-09-30/
    run_id=20260930T120000Z-a1b2c3d4e5f6/
      part-00000.jsonl.gz
      part-00001.jsonl.gz
      _SUCCESS.json
```

Downstream consumers should process only directories containing
`_SUCCESS.json`. The manifest contains record counts, part names, checksums,
and the incremental window, but it does not contain Salesforce credentials or
the SOQL text.

## Recommended test sequence

Use these four commands in order:

1. `check-salesforce` — validate credentials, network access, and object access.
2. `list-fields` — identify the exact fields the integration user can read.
3. `check-oci` — validate OCI authentication and bucket visibility without
   writing an object.
4. `export --dry-run`, followed by `export` — review the generated SOQL and
   then perform the extraction.

## Prerequisites

- Python 3.11 is recommended.
- HTTPS egress on port 443 to the Salesforce login/My Domain endpoint.
- A Salesforce integration user with API access and read permission for the
  required objects and fields.
- An existing OCI Object Storage bucket.
- OCI credentials in Cloud Shell, an instance principal on a VM, or a Data
  Flow resource principal.

Salesforce field-level security applies. A field that is not visible to the
integration user cannot be extracted by this sample.

## 1. Test from OCI Cloud Shell

From this directory, create an isolated Python environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Create a local credentials file:

```bash
cp .env.example .env
chmod 600 .env
vi .env
```

Load it into the current shell. Do not commit `.env`:

```bash
set -a
source .env
set +a
```

Verify Salesforce authentication and access to `Account`:

```bash
python salesforce_export.py check-salesforce --object Account
```

Expected output resembles:

```text
Salesforce connection: OK
  Instance: acme.my.salesforce.com
  Object: Account
  Readable fields: 68
  At least one record visible: True
```

List the fields the integration user can read:

```bash
python salesforce_export.py list-fields --object Account
```

Verify the target bucket. Cloud Shell normally uses the OCI SDK configuration
already available in `~/.oci/config`:

```bash
python salesforce_export.py check-oci \
  --destination 'oci://customer-landing@customer_namespace/salesforce/raw' \
  --oci-auth config
```

This bucket check is read-only. The first actual export is the write-permission
test.

## 2. Review and run a small export

Start with a few non-sensitive fields and a bounded query:

```bash
python salesforce_export.py export \
  --object Account \
  --fields Id Name BillingCountry SystemModstamp \
  --since 2026-09-30T00:00:00Z \
  --until 2026-09-30T00:05:00Z \
  --destination 'oci://customer-landing@customer_namespace/salesforce/raw' \
  --oci-auth config \
  --dry-run
```

A dry run with explicit `--fields` or `--query-file` is offline and does not
connect to Salesforce or OCI. A dry run with `--all-fields` connects only to
Salesforce because it must discover the object's readable fields.

After reviewing the destination and generated SOQL, remove `--dry-run`:

```bash
python salesforce_export.py export \
  --object Account \
  --fields Id Name BillingCountry SystemModstamp \
  --since 2026-09-30T00:00:00Z \
  --until 2026-09-30T00:05:00Z \
  --destination 'oci://customer-landing@customer_namespace/salesforce/raw' \
  --oci-auth config
```

Fields may be separated by spaces or commas. Use `--all-fields` only after a
small field projection succeeds; wide objects consume more Salesforce API and
network capacity, and some objects contain large fields.

## 3. Test from an OCI Compute VM

The Python setup and Salesforce environment variables are the same as Cloud
Shell. If the VM has an instance principal, use:

```bash
python salesforce_export.py check-oci \
  --destination 'oci://customer-landing@customer_namespace/salesforce/raw' \
  --oci-auth instance-principal
```

The VM needs:

- DNS and HTTPS egress to Salesforce;
- network access to the regional Object Storage endpoint; and
- a dynamic-group policy permitting the VM to use objects in the target bucket.

If `~/.oci/config` is intentionally installed on the VM, `--oci-auth config`
can be used instead. Avoid copying personal API keys onto shared or production
VMs.

## 4. Use a SOQL query file

Use a query file for relationship queries, aggregate expressions, or the
equivalent of a relational view. This also keeps query filters out of shell
history:

```bash
python salesforce_export.py export \
  --query-file queries/account_with_owner.soql \
  --dataset-name account_with_owner \
  --destination 'oci://customer-landing@customer_namespace/salesforce/raw' \
  --dry-run
```

If “view” means a Salesforce UI list view, retrieve or describe the list view
through Salesforce's List View REST API first. A UI list view is not queried as
an sObject table name.

## Incremental extraction

For object exports, `--since` is inclusive and `--until` is exclusive:

```text
SystemModstamp >= since AND SystemModstamp < until
```

This half-open window lets adjacent scheduled runs meet at the same boundary
without losing records. If `--since` is supplied without `--until`, the script
uses its UTC start time as the upper bound. Persist that upper bound only after
`_SUCCESS.json` has been written.

`SystemModstamp` is the default watermark because it is commonly used for
replication. If an object does not expose it, select an appropriate timestamp
with `--watermark-field`.

To include soft-deleted records and archived activities, add
`--include-deleted` and select `IsDeleted` when the object supports it.

## Command summary

```bash
python salesforce_export.py --help
python salesforce_export.py check-salesforce --help
python salesforce_export.py list-fields --help
python salesforce_export.py check-oci --help
python salesforce_export.py export --help
```

| Command | Purpose | Writes data? |
|---|---|---:|
| `check-salesforce` | Validate Salesforce login and object read access | No |
| `list-fields` | Show readable field names and types | No |
| `check-oci` | Validate OCI identity and bucket visibility | No |
| `export --dry-run` | Show the query and destination plan | No |
| `export` | Extract Salesforce rows and write Object Storage parts | Yes |

OCI authentication defaults to `auto`:

1. Data Flow resource principal when its environment is detected;
2. an OCI SDK config file when it exists; or
3. an instance principal otherwise.

For a customer test, specifying the mode explicitly makes troubleshooting
clearer.

## Data Flow deployment

After Cloud Shell or VM validation succeeds, follow
[docs/DATA_FLOW_DEPLOYMENT.md](docs/DATA_FLOW_DEPLOYMENT.md). Data Flow should
use resource-principal authentication and a least-privilege policy restricted
to the destination bucket.

## Security guidance

- Never put passwords, tokens, or SOQL containing sensitive values in command
  arguments, logs, the dependency archive, or Git.
- Use a dedicated least-privilege Salesforce integration user.
- Treat `.env` as a temporary local testing mechanism. For a production Data
  Flow deployment, obtain Salesforce credentials from the customer's approved
  secret-management process; do not store them as visible application
  parameters.
- `--debug` can expose query or SDK details in a traceback. Use it only in a
  controlled troubleshooting session.
- Object parts are created with `If-None-Match: *`, preventing accidental
  overwrite of a prior run.

## Tests

The tests do not contact Salesforce or OCI:

```bash
python -m unittest discover -s tests -v
```

## Scale guidance

This sample uses the paginated Salesforce REST Query API and runs extraction
on the Data Flow driver. It is suitable for connectivity tests, bounded
incremental loads, and small-to-medium snapshots. For a multi-million-row
initial load, use Salesforce Bulk API 2.0 and retain the same Object Storage
manifest convention.
