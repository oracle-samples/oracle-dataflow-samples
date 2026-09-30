# Troubleshooting

## Salesforce authentication failed

Check:

- the integration username and password;
- whether the security token changed after a password reset;
- `SF_DOMAIN=test` for a sandbox;
- the My Domain value, if the organization requires it;
- whether the user has API access; and
- whether login IP restrictions require a security token or trusted network.

Do not paste credentials into a support ticket or shared log.

## Connection timeout, DNS failure, or TLS error

From the Cloud Shell or VM, verify that DNS and HTTPS port 443 are available to
the configured Salesforce domain. Review the customer's service gateway, NAT
gateway, firewall, proxy, and private-subnet routing as applicable.

Do not disable TLS certificate verification to bypass a certificate error.

## Object or field rejected by Salesforce

Run:

```bash
python salesforce_export.py list-fields --object Account
```

Use API names rather than display labels. Custom objects and fields normally
end in `__c`. Field-level security and permission sets determine visibility.

## OCI bucket check fails

Confirm:

- the bucket and namespace in the `oci://` URI;
- the selected region and OCI profile in Cloud Shell;
- the VM's dynamic-group membership for instance-principal authentication;
- the Data Flow resource-principal policy; and
- that the policy is restricted to, but includes, the target bucket.

The `check-oci` command is read-only. A successful check confirms bucket
visibility, but the first export is still needed to prove object-create access.

## Export created parts but no `_SUCCESS.json`

The run did not commit successfully. Do not process its parts. Retain the run
logs and safe identifiers, correct the cause, then run again with a new run ID.
Clean up abandoned parts only through the customer's approved lifecycle or
operator procedure.

## Salesforce API usage is high

- Select only needed fields.
- Use short, bounded incremental windows.
- Prefer an indexed watermark such as `SystemModstamp` when supported.
- Avoid repeatedly running `--all-fields` extracts.
- Use Bulk API 2.0 for very large initial snapshots.

## Getting diagnostic output

Use `--debug` only in a controlled session:

```bash
python salesforce_export.py --debug check-salesforce --object Account
```

Tracebacks may include query or endpoint details. Review and redact them before
sharing.
