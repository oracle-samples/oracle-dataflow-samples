import contextlib
import datetime as dt
import gzip
import io
import json
import types
import unittest
from unittest import mock

import salesforce_export as app


class FakeSObject:
    def describe(self):
        return {
            "queryable": True,
            "fields": [
                {"name": "Id", "type": "id", "nillable": False},
                {"name": "Name", "type": "string", "nillable": True},
                {
                    "name": "SystemModstamp",
                    "type": "datetime",
                    "nillable": False,
                },
            ],
        }


class FakeSalesforce:
    sf_instance = "example.my.salesforce.com"

    def __init__(self, records=None):
        self.records = records or []
        self.last_query = None
        self.include_deleted = None

    def __getattr__(self, name):
        return FakeSObject()

    def query(self, query):
        self.last_query = query
        return {"records": [{"Id": "hidden-from-output"}]}

    def limits(self):
        return {"DailyApiRequests": {"Remaining": 999, "Max": 1000}}

    def query_all_iter(self, query, include_deleted=False):
        self.last_query = query
        self.include_deleted = include_deleted
        return iter(self.records)


class FakeObjectStorage:
    def __init__(self):
        self.uploads = []

    def head_bucket(self, **kwargs):
        return types.SimpleNamespace(headers={"etag": "bucket-etag"})

    def put_object(self, **kwargs):
        body = kwargs["put_object_body"]
        payload = body if isinstance(body, bytes) else body.read()
        self.uploads.append((kwargs, payload))
        return types.SimpleNamespace(headers={"etag": f"etag-{len(self.uploads)}"})


FAKE_OCI = types.SimpleNamespace(
    retry=types.SimpleNamespace(DEFAULT_RETRY_STRATEGY=object())
)


class HelperTests(unittest.TestCase):
    def test_oci_uri_with_namespace(self):
        location = app.parse_oci_uri("oci://landing@namespace/salesforce/raw/")
        self.assertEqual(location.bucket, "landing")
        self.assertEqual(location.namespace, "namespace")
        self.assertEqual(location.prefix, "salesforce/raw")

    def test_fields_accept_spaces_and_commas(self):
        self.assertEqual(
            app.split_fields(["Id,Name", "SystemModstamp"]),
            ["Id", "Name", "SystemModstamp"],
        )

    def test_half_open_incremental_query(self):
        start = dt.datetime(2026, 9, 30, 0, 0, tzinfo=dt.timezone.utc)
        end = dt.datetime(2026, 9, 30, 0, 5, tzinfo=dt.timezone.utc)
        query = app.build_soql(
            "Account",
            ["Id", "SystemModstamp"],
            "SystemModstamp",
            start,
            end,
        )
        self.assertEqual(
            query,
            "SELECT Id, SystemModstamp FROM Account "
            "WHERE SystemModstamp >= 2026-09-30T00:00:00.000Z "
            "AND SystemModstamp < 2026-09-30T00:05:00.000Z",
        )

    def test_recursive_metadata_removal(self):
        record = {
            "attributes": {"type": "Contact"},
            "Id": "003xx",
            "Account": {
                "attributes": {"type": "Account"},
                "Name": "Example",
            },
        }
        self.assertEqual(
            app.strip_salesforce_attributes(record),
            {"Id": "003xx", "Account": {"Name": "Example"}},
        )

    def test_auto_auth_prefers_resource_principal(self):
        with mock.patch.dict(
            app.os.environ,
            {"OCI_RESOURCE_PRINCIPAL_VERSION": "2.2"},
            clear=True,
        ):
            self.assertEqual(
                app.choose_oci_auth("auto", "/missing/config"),
                "resource-principal",
            )


class CommandTests(unittest.TestCase):
    def test_salesforce_check_is_customer_readable_and_hides_record_id(self):
        args = app.cli_parser().parse_args(
            ["check-salesforce", "--object", "Account"]
        )
        output = io.StringIO()
        with mock.patch.object(
            app, "build_salesforce_client", return_value=FakeSalesforce()
        ), contextlib.redirect_stdout(output):
            self.assertEqual(app.command_check_salesforce(args), 0)
        text = output.getvalue()
        self.assertIn("Salesforce connection: OK", text)
        self.assertIn("Readable fields: 3", text)
        self.assertNotIn("hidden-from-output", text)

    def test_object_export_requires_field_choice(self):
        args = app.cli_parser().parse_args(
            [
                "export",
                "--object",
                "Account",
                "--destination",
                "oci://landing@namespace/raw",
            ]
        )
        with self.assertRaises(app.UsageError):
            app.resolve_export_query(args, FakeSalesforce(), app.utc_now())

    def test_since_without_until_is_bounded_at_start(self):
        args = app.cli_parser().parse_args(
            [
                "export",
                "--object",
                "Account",
                "--fields",
                "Id",
                "SystemModstamp",
                "--since",
                "2026-09-30T00:00:00Z",
                "--destination",
                "oci://landing@namespace/raw",
            ]
        )
        started = dt.datetime(2026, 9, 30, 0, 5, tzinfo=dt.timezone.utc)
        query, _, _ = app.resolve_export_query(args, FakeSalesforce(), started)
        self.assertIn("SystemModstamp < 2026-09-30T00:05:00.000Z", query)

    def test_explicit_field_dry_run_does_not_build_clients(self):
        args = app.cli_parser().parse_args(
            [
                "export",
                "--object",
                "Account",
                "--fields",
                "Id",
                "--destination",
                "oci://landing@namespace/raw",
                "--dry-run",
            ]
        )
        output = io.StringIO()
        with (
            mock.patch.object(app, "build_salesforce_client") as sf_builder,
            mock.patch.object(
                app, "build_object_storage_client"
            ) as oci_builder,
            contextlib.redirect_stdout(output),
        ):
            self.assertEqual(app.command_export(args), 0)
        sf_builder.assert_not_called()
        oci_builder.assert_not_called()
        self.assertIn("no OCI writes were made", output.getvalue())

    def test_export_uploads_parts_then_success_manifest(self):
        records = [
            {"attributes": {"type": "Account"}, "Id": "1"},
            {"attributes": {"type": "Account"}, "Id": "2"},
            {"attributes": {"type": "Account"}, "Id": "3"},
        ]
        sf = FakeSalesforce(records)
        storage = FakeObjectStorage()
        args = app.cli_parser().parse_args(
            [
                "export",
                "--object",
                "Account",
                "--fields",
                "Id",
                "--destination",
                "oci://landing@namespace/raw",
                "--partition-rows",
                "2",
            ]
        )
        with mock.patch.object(
            app, "build_salesforce_client", return_value=sf
        ), mock.patch.object(
            app,
            "build_object_storage_client",
            return_value=(FAKE_OCI, storage, "config"),
        ), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(app.command_export(args), 0)

        self.assertEqual(len(storage.uploads), 3)
        first_part = gzip.decompress(storage.uploads[0][1]).splitlines()
        self.assertEqual([json.loads(line) for line in first_part], [{"Id": "1"}, {"Id": "2"}])
        manifest = json.loads(storage.uploads[-1][1])
        self.assertEqual(manifest["status"], "complete")
        self.assertEqual(manifest["record_count"], 3)
        self.assertEqual(manifest["part_count"], 2)
        self.assertTrue(storage.uploads[-1][0]["object_name"].endswith("_SUCCESS.json"))


if __name__ == "__main__":
    unittest.main()
