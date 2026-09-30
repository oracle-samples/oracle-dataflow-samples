# Deploy to OCI Data Flow

Complete the Cloud Shell or VM connectivity test in the main README before
creating a Data Flow application. This separates Salesforce/network problems
from Data Flow packaging and IAM problems.

## 1. Choose a compatible runtime

Match the dependency archive to the Python version used by the Data Flow Spark
runtime. For example, Oracle's current guidance maps Spark 3.5.0 to Python 3.11.
Confirm the runtime selected for the customer's application before packaging.

Do not add `pyspark` or `py4j` to `requirements.txt`; Data Flow supplies them.

## 2. Build the dependency archive

Run the official Data Flow Dependency Packager from the directory containing
`requirements.txt`.

For an AMD64 Spark 3.5/Python 3.11 application:

```bash
docker run --platform linux/amd64 --rm \
  -v "$(pwd):/opt/dataflow" \
  --pull always -it \
  phx.ocir.io/axmemlgtri2a/dataflow/dependency-packager-linux_x86_64:latest \
  -p 3.11
```

Validate the resulting archive:

```bash
docker run --platform linux/amd64 --rm \
  -v "$(pwd):/opt/dataflow" \
  --pull always -it \
  phx.ocir.io/axmemlgtri2a/dataflow/dependency-packager-linux_x86_64:latest \
  -p 3.11 --validate archive.zip
```

Oracle also publishes an ARM64 packager. Use the image and Python version that
match the selected Data Flow shape/runtime. See Oracle's “Providing a
Dependency Archive” documentation rather than copying an archive between
incompatible architectures.

## 3. Upload the application artifacts

Upload these files to a customer-controlled Object Storage bucket:

- `salesforce_export.py`
- `archive.zip`

Do not upload `.env`, a Salesforce token, a password, a security token, or an
OCI API private key.

## 4. Configure IAM

Use a Data Flow resource principal and restrict its access to the destination
bucket. Oracle's policy builder includes the template “Let Data Flow resource
use Object Storage.” The exact policy must be reviewed by the customer's IAM
administrator and scoped to the customer's compartment and bucket.

Store the External Client App's consumer secret as the plain content of one OCI
Vault secret. The script retrieves the API's Base64 representation and decodes
it in memory. Grant the Data Flow resource principal read access to only that
secret bundle. For a dynamic group containing the approved Data Flow runs, a
policy can be restricted by secret OCID:

```text
Allow dynamic-group <data-flow-dynamic-group> to read secret-bundles in
compartment <vault-compartment> where
target.secret.id='<client-secret-ocid>'
```

The customer's IAM administrator must review the dynamic-group rule, identity
domain syntax, compartment, and condition before creating the policy.

## 5. Create the Data Flow application

Create a Python/PySpark application with:

- the Object Storage URI of `salesforce_export.py` as the application file;
- the Object Storage URI of `archive.zip` as the dependency archive;
- the Spark/Python runtime used to build the archive; and
- application arguments beginning with the `export` command.

Data Flow does not support setting arbitrary environment variables for a job.
Use application parameters for the non-secret OAuth values and Vault secret
OCID. Example arguments:

```text
export
--sf-auth-mode
client-credentials
--sf-domain
acme.my
--sf-client-id
${SalesforceClientId}
--sf-client-secret-ocid
${SalesforceClientSecretOcid}
--sf-vault-oci-auth
resource-principal
--object
Account
--fields
Id,Name,BillingCountry,SystemModstamp
--since
2026-09-30T00:00:00Z
--until
2026-09-30T00:05:00Z
--destination
oci://customer-landing@customer_namespace/salesforce/raw
--oci-auth
resource-principal
--log-format
json
```

Do not place a Salesforce client secret, password, access token, or security
token in visible application arguments. The consumer key and Vault secret OCID
identify resources but do not reveal the secret value.

## 6. Validate the first run

For the first run:

1. Use a short time window and a small field list.
2. Confirm the run log contains `export_completed`.
3. Confirm `_SUCCESS.json` exists.
4. Compare the manifest's `record_count` with an equivalent Salesforce query.
5. Decompress and inspect one non-sensitive JSONL part.
6. Only then widen the time window or field list.

If a run fails after writing some parts, `_SUCCESS.json` is absent. Consumers
must ignore that run directory. Use an Object Storage lifecycle policy or an
operator-reviewed cleanup procedure for abandoned incomplete runs.
