#!/usr/bin/env python3
"""Customer-friendly Salesforce to OCI Object Storage extractor.

The script can be tested from OCI Cloud Shell or a VM before it is packaged as
an OCI Data Flow application. Dependencies are imported lazily so ``--help``
and ``--dry-run`` remain useful while troubleshooting an installation.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import datetime as dt
import gzip
import hashlib
import json
import os
import re
import sys
import tempfile
import traceback
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Mapping, Sequence
from urllib.parse import unquote, urlparse


VERSION = "1.1.0"
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
FIELD_PATH = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
)
PATH_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class UsageError(Exception):
    """A safe, actionable error that can be shown to a customer."""


@dataclass(frozen=True)
class OciLocation:
    bucket: str
    namespace: str | None
    prefix: str


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_z(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def parse_timestamp(value: str) -> dt.datetime:
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid timestamp {value!r}; use an ISO-8601 value such as "
            "2026-09-30T12:00:00Z"
        ) from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(
            "timestamp must include a timezone, for example 2026-09-30T12:00:00Z"
        )
    return parsed.astimezone(dt.timezone.utc)


def parse_oci_uri(value: str) -> OciLocation:
    parsed = urlparse(value)
    if parsed.scheme != "oci" or not parsed.netloc:
        raise argparse.ArgumentTypeError(
            "use oci://<bucket>@<namespace>/<prefix>"
        )
    if parsed.query or parsed.fragment:
        raise argparse.ArgumentTypeError(
            "an OCI destination cannot contain a query string or fragment"
        )
    authority = unquote(parsed.netloc)
    if "@" in authority:
        bucket, namespace = authority.rsplit("@", 1)
    else:
        bucket, namespace = authority, None
    if not bucket or namespace == "":
        raise argparse.ArgumentTypeError("bucket and namespace cannot be empty")
    return OciLocation(
        bucket=bucket,
        namespace=namespace,
        prefix=unquote(parsed.path).strip("/"),
    )


def checked_identifier(value: str, label: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise UsageError(f"Invalid Salesforce {label}: {value!r}")
    return value


def checked_field(value: str) -> str:
    if not FIELD_PATH.fullmatch(value):
        raise UsageError(
            f"Invalid field {value!r}. Use --query-file for functions, aliases, "
            "or other SOQL expressions."
        )
    return value


def checked_segment(value: str, label: str) -> str:
    if not PATH_SEGMENT.fullmatch(value):
        raise UsageError(
            f"Invalid {label} {value!r}. Use letters, digits, '.', '_', or '-'."
        )
    return value


def split_fields(values: Sequence[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        result.extend(item.strip() for item in value.split(",") if item.strip())
    if not result:
        raise UsageError("At least one field is required.")
    return [checked_field(value) for value in result]


def build_soql(
    object_name: str,
    fields: Sequence[str],
    watermark_field: str,
    since: dt.datetime | None,
    until: dt.datetime | None,
) -> str:
    object_name = checked_identifier(object_name, "object name")
    safe_fields = [checked_field(field) for field in fields]
    watermark_field = checked_field(watermark_field)
    if not safe_fields:
        raise UsageError("At least one field is required.")
    if since is not None and until is not None and since >= until:
        raise UsageError("--since must be earlier than --until.")

    filters: list[str] = []
    if since is not None:
        filters.append(f"{watermark_field} >= {iso_z(since)}")
    if until is not None:
        filters.append(f"{watermark_field} < {iso_z(until)}")
    query = f"SELECT {', '.join(safe_fields)} FROM {object_name}"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    if len(query) > 90_000:
        raise UsageError(
            "The generated SOQL is too large. Select fewer fields or use "
            "--query-file."
        )
    return query


def strip_salesforce_attributes(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            key: strip_salesforce_attributes(child)
            for key, child in value.items()
            if key != "attributes"
        }
    if isinstance(value, list):
        return [strip_salesforce_attributes(child) for child in value]
    return value


def join_object_name(*parts: str) -> str:
    return "/".join(part.strip("/") for part in parts if part.strip("/"))


class Reporter:
    def __init__(self, output_format: str = "text") -> None:
        self.output_format = output_format

    def event(self, event: str, message: str, **fields: Any) -> None:
        if self.output_format == "json":
            print(
                json.dumps(
                    {
                        "timestamp": iso_z(utc_now()),
                        "event": event,
                        "message": message,
                        **fields,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                flush=True,
            )
            return
        suffix = "" if not fields else "  " + "  ".join(
            f"{key}={value}" for key, value in fields.items()
        )
        print(f"{message}{suffix}", flush=True)


def build_requests_session(timeout: tuple[float, float]) -> Any:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    class TimeoutSession(requests.Session):
        def request(self, method: str, url: str, **kwargs: Any) -> Any:
            kwargs.setdefault("timeout", timeout)
            return super().request(method, url, **kwargs)

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(("GET", "POST")),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = TimeoutSession()
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session.mount("https://", adapter)
    return session


SALESFORCE_AUTH_ENVIRONMENT = {
    "client-credentials": (
        "SF_CLIENT_ID",
        "SF_CLIENT_SECRET",
        "SF_CLIENT_SECRET_OCID",
    ),
    "access-token": ("SF_ACCESS_TOKEN", "SF_INSTANCE_URL"),
    "password": ("SF_USERNAME", "SF_PASSWORD", "SF_SECURITY_TOKEN"),
}


def resolve_salesforce_auth_mode() -> str:
    requested = os.environ.get("SF_AUTH_MODE", "auto").strip().lower()
    supported = ("auto", *SALESFORCE_AUTH_ENVIRONMENT)
    if requested not in supported:
        raise UsageError(
            "Unsupported SF_AUTH_MODE. Choose auto, client-credentials, "
            "access-token, or password."
        )
    if requested != "auto":
        return requested

    configured = [
        mode
        for mode, names in SALESFORCE_AUTH_ENVIRONMENT.items()
        if any(os.environ.get(name) for name in names)
    ]
    if len(configured) == 1:
        return configured[0]
    if len(configured) > 1:
        raise UsageError(
            "Multiple Salesforce authentication methods are configured: "
            + ", ".join(configured)
            + ". Set SF_AUTH_MODE explicitly or unset the unused variables."
        )
    raise UsageError(
        "No Salesforce authentication method is configured. Copy .env.example "
        "to .env and follow the README."
    )


def required_environment(names: Sequence[str], auth_mode: str) -> dict[str, str]:
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        raise UsageError(
            f"Missing environment variables for {auth_mode}: "
            + ", ".join(missing)
            + ". Copy .env.example to .env and follow the README."
        )
    return {name: os.environ[name] for name in names}


def salesforce_domain() -> str:
    domain = os.environ.get("SF_DOMAIN", "login").strip()
    if not domain or "://" in domain or "/" in domain:
        raise UsageError(
            "SF_DOMAIN must be a host prefix such as login, test, or acme.my; "
            "do not provide a URL."
        )
    if domain.endswith(".salesforce.com"):
        raise UsageError(
            "SF_DOMAIN must omit .salesforce.com; use acme.my rather than "
            "acme.my.salesforce.com."
        )
    return domain


def build_oci_secrets_client(
    auth_mode: str,
    config_file: str,
    profile: str,
) -> tuple[Any, Any, str]:
    try:
        import oci
    except ModuleNotFoundError as exc:
        raise UsageError(
            "The OCI Python SDK is required to read SF_CLIENT_SECRET_OCID. "
            "Activate the virtual environment and run: "
            "pip install -r requirements.txt"
        ) from exc

    effective_auth = choose_oci_auth(auth_mode, config_file)
    if effective_auth == "resource-principal":
        signer = oci.auth.signers.get_resource_principals_signer()
        client = oci.secrets.SecretsClient({}, signer=signer)
    elif effective_auth == "instance-principal":
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        client = oci.secrets.SecretsClient({}, signer=signer)
    elif effective_auth == "config":
        config = oci.config.from_file(
            file_location=str(Path(config_file).expanduser()),
            profile_name=profile,
        )
        client = oci.secrets.SecretsClient(config)
    else:
        raise UsageError(f"Unsupported SF_VAULT_OCI_AUTH: {effective_auth}")
    return oci, client, effective_auth


def read_oci_vault_secret(secret_id: str) -> str:
    if not secret_id.startswith("ocid1.vaultsecret."):
        raise UsageError(
            "SF_CLIENT_SECRET_OCID must be an OCI Vault secret OCID."
        )
    auth_mode = os.environ.get("SF_VAULT_OCI_AUTH", "auto")
    config_file = os.environ.get(
        "SF_VAULT_OCI_CONFIG_FILE",
        os.environ.get("OCI_CONFIG_FILE", "~/.oci/config"),
    )
    profile = os.environ.get(
        "SF_VAULT_OCI_PROFILE",
        os.environ.get("OCI_CONFIG_PROFILE", "DEFAULT"),
    )
    oci_module, client, _ = build_oci_secrets_client(
        auth_mode,
        config_file,
        profile,
    )
    response = client.get_secret_bundle(
        secret_id=secret_id,
        stage="CURRENT",
        retry_strategy=oci_module.retry.DEFAULT_RETRY_STRATEGY,
    )
    encoded = getattr(
        getattr(response.data, "secret_bundle_content", None),
        "content",
        None,
    )
    if not encoded:
        raise UsageError(
            "The OCI Vault secret has no readable CURRENT Base64 content."
        )
    try:
        decoded = base64.b64decode(encoded, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
        raise UsageError(
            "The OCI Vault secret content is not valid Base64-encoded UTF-8."
        ) from exc
    value = decoded.rstrip("\r\n")
    if not value:
        raise UsageError("The OCI Vault client secret is empty.")
    return value


def resolve_client_secret() -> str:
    direct = os.environ.get("SF_CLIENT_SECRET")
    secret_id = os.environ.get("SF_CLIENT_SECRET_OCID")
    if direct and secret_id:
        raise UsageError(
            "Set only one of SF_CLIENT_SECRET or SF_CLIENT_SECRET_OCID."
        )
    if direct:
        return direct
    if secret_id:
        return read_oci_vault_secret(secret_id)
    raise UsageError(
        "Client Credentials authentication requires SF_CLIENT_SECRET or "
        "SF_CLIENT_SECRET_OCID."
    )


def build_salesforce_client(timeout: tuple[float, float]) -> Any:
    try:
        from simple_salesforce import Salesforce
    except ModuleNotFoundError as exc:
        raise UsageError(
            "simple-salesforce is not installed. Activate the virtual environment "
            "and run: pip install -r requirements.txt"
        ) from exc

    common: dict[str, Any] = {
        "session": build_requests_session(timeout),
        "client_id": "oci-data-flow-exporter",
    }
    if os.environ.get("SF_API_VERSION"):
        common["version"] = os.environ["SF_API_VERSION"]

    auth_mode = resolve_salesforce_auth_mode()
    if auth_mode == "client-credentials":
        values = required_environment(("SF_CLIENT_ID",), auth_mode)
        client = Salesforce(
            consumer_key=values["SF_CLIENT_ID"],
            consumer_secret=resolve_client_secret(),
            domain=salesforce_domain(),
            **common,
        )
    elif auth_mode == "access-token":
        values = required_environment(
            ("SF_ACCESS_TOKEN", "SF_INSTANCE_URL"),
            auth_mode,
        )
        client = Salesforce(
            session_id=values["SF_ACCESS_TOKEN"],
            instance_url=values["SF_INSTANCE_URL"],
            **common,
        )
    else:
        values = required_environment(
            ("SF_USERNAME", "SF_PASSWORD", "SF_SECURITY_TOKEN"),
            auth_mode,
        )
        client = Salesforce(
            username=values["SF_USERNAME"],
            password=values["SF_PASSWORD"],
            security_token=values["SF_SECURITY_TOKEN"],
            domain=salesforce_domain(),
            **common,
        )

    setattr(client, "_sample_auth_mode", auth_mode)
    return client


def describe_object(sf: Any, object_name: str) -> Mapping[str, Any]:
    object_name = checked_identifier(object_name, "object name")
    description = getattr(sf, object_name).describe()
    if not description.get("queryable", True):
        raise UsageError(f"Salesforce object {object_name!r} is not queryable.")
    return description


def discover_fields(sf: Any, object_name: str) -> list[str]:
    description = describe_object(sf, object_name)
    fields = [
        field["name"]
        for field in description.get("fields", [])
        if field.get("name") and not field.get("deprecatedAndHidden", False)
    ]
    if not fields:
        raise UsageError(
            f"No readable fields were returned for Salesforce object {object_name!r}."
        )
    return [checked_field(field) for field in fields]


def choose_oci_auth(requested: str, config_file: str) -> str:
    if requested != "auto":
        return requested
    if os.environ.get("OCI_RESOURCE_PRINCIPAL_VERSION"):
        return "resource-principal"
    if Path(config_file).expanduser().is_file():
        return "config"
    return "instance-principal"


def build_object_storage_client(
    auth_mode: str,
    config_file: str,
    profile: str,
) -> tuple[Any, Any, str]:
    try:
        import oci
    except ModuleNotFoundError as exc:
        raise UsageError(
            "The OCI Python SDK is not installed. Activate the virtual environment "
            "and run: pip install -r requirements.txt"
        ) from exc

    effective_auth = choose_oci_auth(auth_mode, config_file)
    if effective_auth == "resource-principal":
        signer = oci.auth.signers.get_resource_principals_signer()
        client = oci.object_storage.ObjectStorageClient({}, signer=signer)
    elif effective_auth == "instance-principal":
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        client = oci.object_storage.ObjectStorageClient({}, signer=signer)
    elif effective_auth == "config":
        config = oci.config.from_file(
            file_location=str(Path(config_file).expanduser()),
            profile_name=profile,
        )
        client = oci.object_storage.ObjectStorageClient(config)
    else:  # pragma: no cover - argparse controls the values
        raise UsageError(f"Unsupported OCI authentication mode: {effective_auth}")
    return oci, client, effective_auth


def resolve_namespace(
    oci_module: Any,
    client: Any,
    location: OciLocation,
) -> str:
    if location.namespace:
        return location.namespace
    return client.get_namespace(
        retry_strategy=oci_module.retry.DEFAULT_RETRY_STRATEGY
    ).data


class OciJsonlSink:
    def __init__(
        self,
        *,
        oci_module: Any,
        client: Any,
        namespace: str,
        bucket: str,
        run_prefix: str,
        partition_rows: int,
        compression: str,
        spool_memory_mib: int,
    ) -> None:
        self.oci = oci_module
        self.client = client
        self.namespace = namespace
        self.bucket = bucket
        self.run_prefix = run_prefix
        self.partition_rows = partition_rows
        self.compression = compression
        self.spool_limit = spool_memory_mib * 1024 * 1024
        self.total_rows = 0
        self.part_rows = 0
        self.parts: list[dict[str, Any]] = []
        self._spool: BinaryIO | None = None
        self._writer: BinaryIO | None = None

    def _open_part(self) -> None:
        self._spool = tempfile.SpooledTemporaryFile(
            max_size=self.spool_limit,
            mode="w+b",
        )
        if self.compression == "gzip":
            self._writer = gzip.GzipFile(
                fileobj=self._spool,
                mode="wb",
                compresslevel=6,
                mtime=0,
            )
        else:
            self._writer = self._spool

    def write(self, record: Mapping[str, Any]) -> None:
        if self._writer is None:
            self._open_part()
        line = json.dumps(
            strip_salesforce_attributes(record),
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8") + b"\n"
        self._writer.write(line)
        self.part_rows += 1
        self.total_rows += 1
        if self.part_rows >= self.partition_rows:
            self._upload_part()

    def _upload_part(self) -> None:
        if self._writer is None or self._spool is None or self.part_rows == 0:
            return
        if self._writer is not self._spool:
            self._writer.close()
        else:
            self._writer.flush()

        self._spool.seek(0, os.SEEK_END)
        size = self._spool.tell()
        self._spool.seek(0)
        digest = hashlib.sha256()
        while chunk := self._spool.read(1024 * 1024):
            digest.update(chunk)
        self._spool.seek(0)

        suffix = ".jsonl.gz" if self.compression == "gzip" else ".jsonl"
        object_name = join_object_name(
            self.run_prefix,
            f"part-{len(self.parts):05d}{suffix}",
        )
        request: dict[str, Any] = {
            "namespace_name": self.namespace,
            "bucket_name": self.bucket,
            "object_name": object_name,
            "put_object_body": self._spool,
            "content_length": size,
            "content_type": "application/x-ndjson",
            "if_none_match": "*",
            "retry_strategy": self.oci.retry.DEFAULT_RETRY_STRATEGY,
        }
        if self.compression == "gzip":
            request["content_encoding"] = "gzip"
        response = self.client.put_object(**request)
        self.parts.append(
            {
                "object_name": object_name,
                "rows": self.part_rows,
                "bytes": size,
                "sha256": digest.hexdigest(),
                "etag": response.headers.get("etag"),
            }
        )
        self._spool.close()
        self._spool = None
        self._writer = None
        self.part_rows = 0

    def finish(self, manifest: Mapping[str, Any]) -> str:
        self._upload_part()
        completed = dict(manifest)
        completed.update(
            {
                "status": "complete",
                "completed_at": iso_z(utc_now()),
                "record_count": self.total_rows,
                "part_count": len(self.parts),
                "parts": self.parts,
            }
        )
        body = (
            json.dumps(completed, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        object_name = join_object_name(self.run_prefix, "_SUCCESS.json")
        self.client.put_object(
            namespace_name=self.namespace,
            bucket_name=self.bucket,
            object_name=object_name,
            put_object_body=body,
            content_length=len(body),
            content_type="application/json",
            if_none_match="*",
            retry_strategy=self.oci.retry.DEFAULT_RETRY_STRATEGY,
        )
        return object_name

    def abort(self) -> None:
        try:
            if self._writer is not None and self._writer is not self._spool:
                self._writer.close()
        finally:
            if self._spool is not None:
                self._spool.close()
            self._writer = None
            self._spool = None


def add_salesforce_auth_arguments(command: argparse.ArgumentParser) -> None:
    group = command.add_argument_group("Salesforce authentication")
    group.add_argument(
        "--sf-auth-mode",
        choices=("auto", "client-credentials", "access-token", "password"),
        help="override SF_AUTH_MODE",
    )
    group.add_argument(
        "--sf-domain",
        help="Salesforce host prefix, such as login, test, or acme.my",
    )
    group.add_argument(
        "--sf-client-id",
        help="OAuth consumer key; this value is not the client secret",
    )
    group.add_argument(
        "--sf-client-secret-ocid",
        help="OCI Vault secret OCID containing the OAuth client secret",
    )
    group.add_argument(
        "--sf-api-version",
        help='Salesforce API version without a leading "v"',
    )
    group.add_argument(
        "--sf-vault-oci-auth",
        choices=("auto", "resource-principal", "instance-principal", "config"),
        help="OCI authentication used to read the Vault secret",
    )
    group.add_argument(
        "--sf-vault-oci-config-file",
        help="OCI SDK configuration file used to read the Vault secret",
    )
    group.add_argument(
        "--sf-vault-oci-profile",
        help="OCI SDK profile used to read the Vault secret",
    )


def configure_salesforce_environment(args: argparse.Namespace) -> None:
    mappings = {
        "sf_auth_mode": "SF_AUTH_MODE",
        "sf_domain": "SF_DOMAIN",
        "sf_client_id": "SF_CLIENT_ID",
        "sf_client_secret_ocid": "SF_CLIENT_SECRET_OCID",
        "sf_api_version": "SF_API_VERSION",
        "sf_vault_oci_auth": "SF_VAULT_OCI_AUTH",
        "sf_vault_oci_config_file": "SF_VAULT_OCI_CONFIG_FILE",
        "sf_vault_oci_profile": "SF_VAULT_OCI_PROFILE",
    }
    for argument, environment_name in mappings.items():
        value = getattr(args, argument, None)
        if value is not None:
            os.environ[environment_name] = value


def add_oci_auth_arguments(command: argparse.ArgumentParser) -> None:
    group = command.add_argument_group("OCI authentication")
    group.add_argument(
        "--oci-auth",
        choices=("auto", "resource-principal", "instance-principal", "config"),
        default="auto",
        help=(
            "authentication mode (default: auto; detects Data Flow, then an OCI "
            "config file, then instance principal)"
        ),
    )
    group.add_argument(
        "--oci-config-file",
        default=os.environ.get("OCI_CONFIG_FILE", "~/.oci/config"),
        help="OCI SDK configuration file used by --oci-auth config",
    )
    group.add_argument(
        "--oci-profile",
        default=os.environ.get("OCI_CONFIG_PROFILE", "DEFAULT"),
        help="profile in the OCI SDK configuration file",
    )


def cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Test and export Salesforce data to OCI Object Storage.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="show a traceback; it may contain query details, so use with care",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    check_sf = commands.add_parser(
        "check-salesforce",
        help="verify Salesforce authentication and object access",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    check_sf.add_argument("--object", default="Account", help="sObject API name")
    check_sf.add_argument("--timeout", type=float, default=30.0)
    check_sf.add_argument("--json", action="store_true", help="print JSON output")
    add_salesforce_auth_arguments(check_sf)

    fields = commands.add_parser(
        "list-fields",
        help="show readable fields for one Salesforce object",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    fields.add_argument("--object", required=True, help="sObject API name")
    fields.add_argument("--format", choices=("table", "json"), default="table")
    fields.add_argument("--timeout", type=float, default=30.0)
    add_salesforce_auth_arguments(fields)

    check_oci = commands.add_parser(
        "check-oci",
        help="verify OCI authentication and bucket visibility without writing data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    check_oci.add_argument(
        "--destination",
        required=True,
        type=parse_oci_uri,
        metavar="oci://BUCKET@NAMESPACE/PREFIX",
    )
    check_oci.add_argument("--json", action="store_true", help="print JSON output")
    add_oci_auth_arguments(check_oci)

    export = commands.add_parser(
        "export",
        help="export an object or SOQL query to Object Storage",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    source = export.add_mutually_exclusive_group(required=True)
    source.add_argument("--object", help="sObject API name")
    source.add_argument(
        "--query-file",
        type=Path,
        help="UTF-8 file containing a complete SOQL SELECT query",
    )
    field_choice = export.add_mutually_exclusive_group()
    field_choice.add_argument(
        "--fields",
        nargs="+",
        help="fields for --object, separated by spaces or commas",
    )
    field_choice.add_argument(
        "--all-fields",
        action="store_true",
        help="use every readable field returned by sObject describe",
    )
    export.add_argument(
        "--destination",
        required=True,
        type=parse_oci_uri,
        metavar="oci://BUCKET@NAMESPACE/PREFIX",
    )
    export.add_argument(
        "--dataset-name",
        help="output folder name; required with --query-file",
    )
    export.add_argument(
        "--watermark-field",
        default="SystemModstamp",
        help="incremental timestamp field used with --since/--until",
    )
    export.add_argument("--since", type=parse_timestamp, help="inclusive UTC lower bound")
    export.add_argument("--until", type=parse_timestamp, help="exclusive UTC upper bound")
    export.add_argument(
        "--include-deleted",
        action="store_true",
        help="use Salesforce queryAll to include deleted/archived records",
    )
    export.add_argument("--run-id", help="optional stable run identifier")
    export.add_argument(
        "--dry-run",
        action="store_true",
        help="validate and display the query and output plan without writing to OCI",
    )
    export.add_argument(
        "--log-format",
        choices=("text", "json"),
        default="text",
        help="text is friendly for manual tests; JSON is useful in Data Flow logs",
    )
    advanced = export.add_argument_group("advanced export options")
    advanced.add_argument("--partition-rows", type=int, default=50_000)
    advanced.add_argument("--compression", choices=("gzip", "none"), default="gzip")
    advanced.add_argument("--spool-memory-mib", type=int, default=16)
    advanced.add_argument("--connect-timeout", type=float, default=10.0)
    advanced.add_argument("--read-timeout", type=float, default=120.0)
    add_salesforce_auth_arguments(export)
    add_oci_auth_arguments(export)
    return parser


def command_check_salesforce(args: argparse.Namespace) -> int:
    configure_salesforce_environment(args)
    if args.timeout <= 0:
        raise UsageError("--timeout must be positive.")
    object_name = checked_identifier(args.object, "object name")
    sf = build_salesforce_client((min(10.0, args.timeout), args.timeout))
    description = describe_object(sf, object_name)
    result = sf.query(f"SELECT Id FROM {object_name} LIMIT 1")
    summary: dict[str, Any] = {
        "status": "ok",
        "authentication": getattr(sf, "_sample_auth_mode", "unknown"),
        "salesforce_instance": sf.sf_instance,
        "object": object_name,
        "readable_field_count": len(description.get("fields", [])),
        "sample_record_visible": bool(result.get("records")),
    }
    try:
        limits = sf.limits()
        daily = limits.get("DailyApiRequests", {})
        summary["daily_api_requests_remaining"] = daily.get("Remaining")
        summary["daily_api_requests_max"] = daily.get("Max")
    except Exception:
        summary["daily_api_requests_remaining"] = None
        summary["daily_api_requests_max"] = None

    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print("Salesforce connection: OK")
        print(f"  Authentication: {summary['authentication']}")
        print(f"  Instance: {summary['salesforce_instance']}")
        print(f"  Object: {object_name}")
        print(f"  Readable fields: {summary['readable_field_count']}")
        print(f"  At least one record visible: {summary['sample_record_visible']}")
        if summary["daily_api_requests_remaining"] is not None:
            print(
                "  Daily API requests remaining: "
                f"{summary['daily_api_requests_remaining']}/"
                f"{summary['daily_api_requests_max']}"
            )
    return 0


def command_list_fields(args: argparse.Namespace) -> int:
    configure_salesforce_environment(args)
    if args.timeout <= 0:
        raise UsageError("--timeout must be positive.")
    sf = build_salesforce_client((min(10.0, args.timeout), args.timeout))
    description = describe_object(sf, args.object)
    rows = [
        {
            "name": field.get("name"),
            "type": field.get("type"),
            "nillable": bool(field.get("nillable")),
        }
        for field in description.get("fields", [])
        if field.get("name") and not field.get("deprecatedAndHidden", False)
    ]
    rows.sort(key=lambda row: row["name"].lower())
    if args.format == "json":
        print(json.dumps(rows, indent=2, sort_keys=True))
        return 0
    print(f"Readable fields for {args.object} ({len(rows)}):")
    print(f"{'FIELD':40} {'TYPE':18} NULLABLE")
    print(f"{'-' * 40} {'-' * 18} {'-' * 8}")
    for row in rows:
        print(f"{row['name'][:40]:40} {row['type'][:18]:18} {str(row['nillable'])}")
    return 0


def command_check_oci(args: argparse.Namespace) -> int:
    oci_module, client, effective_auth = build_object_storage_client(
        args.oci_auth,
        args.oci_config_file,
        args.oci_profile,
    )
    location: OciLocation = args.destination
    namespace = resolve_namespace(oci_module, client, location)
    response = client.head_bucket(
        namespace_name=namespace,
        bucket_name=location.bucket,
        retry_strategy=oci_module.retry.DEFAULT_RETRY_STRATEGY,
    )
    summary = {
        "status": "ok",
        "authentication": effective_auth,
        "namespace": namespace,
        "bucket": location.bucket,
        "prefix": location.prefix,
        "etag": response.headers.get("etag"),
        "write_test_performed": False,
    }
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print("OCI Object Storage connection: OK")
        print(f"  Authentication: {effective_auth}")
        print(f"  Namespace: {namespace}")
        print(f"  Bucket: {location.bucket}")
        print(f"  Prefix: {location.prefix or '(bucket root)'}")
        print("  This check is read-only; no object was created.")
    return 0


def resolve_export_query(
    args: argparse.Namespace,
    sf: Any | None,
    started_at: dt.datetime,
) -> tuple[str, str, str | None]:
    if args.object:
        object_name = checked_identifier(args.object, "object name")
        if args.all_fields:
            if sf is None:  # Defensive; command_export connects for this mode.
                raise UsageError(
                    "Salesforce access is required to discover --all-fields."
                )
            fields = discover_fields(sf, object_name)
        elif args.fields:
            fields = split_fields(args.fields)
        else:
            raise UsageError(
                "Choose --fields or --all-fields when exporting an object. "
                "For example: --fields Id Name SystemModstamp"
            )
        if args.since is not None and args.until is None:
            args.until = started_at
        query = build_soql(
            object_name,
            fields,
            args.watermark_field,
            args.since,
            args.until,
        )
        dataset = checked_segment(args.dataset_name or object_name, "dataset name")
        return query, dataset, object_name

    if args.fields or args.all_fields:
        raise UsageError("--fields and --all-fields can only be used with --object.")
    if args.since or args.until:
        raise UsageError(
            "Put incremental filters directly in the SOQL file when using --query-file."
        )
    if not args.dataset_name:
        raise UsageError("--dataset-name is required with --query-file.")
    try:
        query = args.query_file.read_text(encoding="utf-8").strip().rstrip(";").strip()
    except OSError as exc:
        raise UsageError(f"Cannot read query file {args.query_file}: {exc}") from exc
    if not re.match(r"(?is)^SELECT\s+", query):
        raise UsageError("The query file must contain one SOQL SELECT statement.")
    return query, checked_segment(args.dataset_name, "dataset name"), None


def command_export(args: argparse.Namespace) -> int:
    configure_salesforce_environment(args)
    for name in ("partition_rows", "spool_memory_mib"):
        if getattr(args, name) <= 0:
            raise UsageError(f"--{name.replace('_', '-')} must be positive.")
    if args.connect_timeout <= 0 or args.read_timeout <= 0:
        raise UsageError("Connection and read timeouts must be positive.")

    reporter = Reporter(args.log_format)
    started_at = utc_now()
    sf = None
    if not args.dry_run or args.all_fields:
        sf = build_salesforce_client(
            (args.connect_timeout, args.read_timeout)
        )
    query, dataset, source_object = resolve_export_query(args, sf, started_at)
    query_hash = hashlib.sha256(query.encode("utf-8")).hexdigest()
    location: OciLocation = args.destination
    run_id = checked_segment(
        args.run_id
        or started_at.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:12],
        "run ID",
    )
    run_prefix = join_object_name(
        location.prefix,
        dataset,
        f"extract_date={started_at:%Y-%m-%d}",
        f"run_id={run_id}",
    )

    if args.dry_run:
        print("Export plan (no OCI writes were made)")
        print(f"  Dataset: {dataset}")
        print(f"  Destination: oci://{location.bucket}@{location.namespace or '<auto>'}")
        print(f"  Output prefix: {run_prefix}")
        print(f"  Query SHA-256: {query_hash}")
        print("  SOQL:")
        print("    " + query.replace("\n", "\n    "))
        return 0

    assert sf is not None  # Non-dry-run exports always create the client above.
    oci_module, client, effective_auth = build_object_storage_client(
        args.oci_auth,
        args.oci_config_file,
        args.oci_profile,
    )
    namespace = resolve_namespace(oci_module, client, location)
    reporter.event(
        "export_started",
        "Starting Salesforce export",
        dataset=dataset,
        run_id=run_id,
        query_sha256=query_hash,
        salesforce_auth=getattr(sf, "_sample_auth_mode", "unknown"),
        oci_auth=effective_auth,
    )
    sink = OciJsonlSink(
        oci_module=oci_module,
        client=client,
        namespace=namespace,
        bucket=location.bucket,
        run_prefix=run_prefix,
        partition_rows=args.partition_rows,
        compression=args.compression,
        spool_memory_mib=args.spool_memory_mib,
    )
    try:
        records: Iterable[Mapping[str, Any]] = sf.query_all_iter(
            query,
            include_deleted=args.include_deleted,
        )
        for record in records:
            sink.write(record)
        success_object = sink.finish(
            {
                "schema_version": 1,
                "started_at": iso_z(started_at),
                "dataset": dataset,
                "source_object": source_object,
                "query_sha256": query_hash,
                "include_deleted": args.include_deleted,
                "format": "jsonl",
                "compression": args.compression,
                "window": {
                    "field": args.watermark_field if source_object else None,
                    "since": iso_z(args.since) if args.since else None,
                    "until": iso_z(args.until) if args.until else None,
                },
            }
        )
    except Exception:
        sink.abort()
        raise

    reporter.event(
        "export_completed",
        "Salesforce export completed",
        dataset=dataset,
        records=sink.total_rows,
        parts=len(sink.parts),
        success_object=success_object,
    )
    return 0


def safe_runtime_error(exc: Exception) -> str:
    name = type(exc).__name__
    if name == "SalesforceAuthenticationFailed":
        return (
            "Salesforce authentication failed. Check SF_AUTH_MODE, SF_DOMAIN, "
            "the selected credentials or External Client App, the integration "
            "user assignment, and whether API access is enabled."
        )
    if name in {"ConnectionError", "ConnectTimeout", "ReadTimeout", "SSLError"}:
        return (
            "Network connection failed. Verify DNS, HTTPS egress on port 443, "
            "proxy settings, and the Salesforce domain."
        )
    if name.startswith("Salesforce"):
        return (
            f"Salesforce rejected the request ({name}). Verify the object, fields, "
            "SOQL, and the integration user's permissions."
        )
    if name == "ServiceError":
        status = getattr(exc, "status", "unknown")
        code = getattr(exc, "code", "unknown")
        request_id = getattr(exc, "request_id", "unknown")
        return (
            f"OCI request failed (status={status}, code={code}, "
            f"request_id={request_id}). Check IAM policy, region, and the configured "
            "Vault secret or Object Storage destination."
        )
    return f"Operation failed ({name}). Re-run with --debug for troubleshooting."


def main(argv: Sequence[str] | None = None) -> int:
    parser = cli_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "check-salesforce":
            return command_check_salesforce(args)
        if args.command == "list-fields":
            return command_list_fields(args)
        if args.command == "check-oci":
            return command_check_oci(args)
        if args.command == "export":
            return command_export(args)
        raise UsageError(f"Unsupported command: {args.command}")
    except UsageError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as exc:
        print(f"ERROR: {safe_runtime_error(exc)}", file=sys.stderr)
        if args.debug:
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
