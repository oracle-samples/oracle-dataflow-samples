#!/usr/bin/env python3

import os
import sys

from borneo import (GetIndexesRequest, GetTableRequest, ListTablesRequest,
                    NoSQLHandle, NoSQLHandleConfig, TableLimits, TableRequest,
                    TableUsageRequest)
from borneo.iam import SignatureProvider
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext


def main():
    # You can hard code your own values here if you want.
    COMPARTMENT_ID = "ocid1.compartment.oc1..aaaaaaaati55ggp45kgnterqwveayuyioyhz7hw7f46umo277mn5vecpny6q"
    ENDPOINT = "us-ashburn-1"
    TABLE_NAME = "pysparktable"
    INDEX_NAME = "pythonindex"
    try:
        COMPARTMENT_ID = sys.argv[1]
        ENDPOINT = sys.argv[2]
        TABLE_NAME = sys.argv[3]
        INDEX_NAME = sys.argv[4]
    except:
        pass

    # Set up Spark.
    spark_session = get_dataflow_spark_session()

    # Get our IAM signer.
    token_path = get_delegation_token_path(spark_session)
    signer = get_signer(token_path)

    # The Handle to our table.
    provider = SignatureProvider(provider=signer)

    # XXX: This needs to get fixed.
    provider.region = ENDPOINT.upper().replace("-", "_")

    config = NoSQLHandleConfig(ENDPOINT, provider).set_default_compartment(
        COMPARTMENT_ID
    )

    handle = NoSQLHandle(config)
    try:
        # List any existing tables for this tenant
        print("Listing tables")
        ltr = ListTablesRequest()
        lr_result = handle.list_tables(ltr)
        print("Existing tables: " + str(lr_result))

        # Create a table
        statement = (
            "Create table if not exists "
            + TABLE_NAME
            + "(id integer, \
    sid integer, name string, primary key(shard(sid), id))"
        )
        print("Creating table: " + statement)
        request = (
            TableRequest()
            .set_statement(statement)
            .set_table_limits(TableLimits(30, 10, 1))
        )
        handle.do_table_request(request, 40000, 3000)
        print("After create table")

        # Create an index
        statement = (
            "Create index if not exists " + INDEX_NAME + " on " + TABLE_NAME + "(name)"
        )
        print("Creating index: " + statement)
        request = TableRequest().set_statement(statement)
        handle.do_table_request(request, 40000, 3000)
        print("After create index")

        # Get the table
        request = GetTableRequest().set_table_name(TABLE_NAME)
        result = handle.get_table(request)
        print("After get table: " + str(result))

        # Get the indexes
        request = GetIndexesRequest().set_table_name(TABLE_NAME)
        result = handle.get_indexes(request)
        print("The indexes for: " + TABLE_NAME)
        for idx in result.get_indexes():
            print("\t" + str(idx))

        # Get the table usage information
        request = TableUsageRequest().set_table_name(TABLE_NAME)
        result = handle.get_table_usage(request)
        print("The table usage information for: " + TABLE_NAME)
        for record in result.get_usage_records():
            print("\t" + str(record))
    finally:
        if handle is not None:
            handle.close()


def get_dataflow_spark_session(
    app_name="DataFlow", file_location=None, profile_name=None, spark_config={}
):
    """
    Get a Spark session in a way that supports running locally or in Data Flow.
    """
    if in_dataflow():
        spark_builder = SparkSession.builder.appName(app_name)
    else:
        # Import OCI.
        try:
            import oci
        except:
            raise Exception(
                "You need to install the OCI python library to test locally"
            )

        # Use defaults for anything unset.
        if file_location is None:
            file_location = oci.config.DEFAULT_LOCATION
        if profile_name is None:
            profile_name = oci.config.DEFAULT_PROFILE

        # Load the config file.
        try:
            oci_config = oci.config.from_file(
                file_location=file_location, profile_name=profile_name
            )
        except Exception as e:
            print("You need to set up your OCI config properly to run locally")
            raise e
        conf = SparkConf()
        conf.set("fs.oci.client.auth.tenantId", oci_config["tenancy"])
        conf.set("fs.oci.client.auth.userId", oci_config["user"])
        conf.set("fs.oci.client.auth.fingerprint", oci_config["fingerprint"])
        conf.set("fs.oci.client.auth.pemfilepath", oci_config["key_file"])
        conf.set(
            "fs.oci.client.hostname",
            "https://objectstorage.{0}.oraclecloud.com".format(oci_config["region"]),
        )
        spark_builder = SparkSession.builder.appName(app_name).config(conf=conf)

    # Add in extra configuration.
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # Create the Spark session.
    session = spark_builder.getOrCreate()
    return session


def get_signer(token_path, file_location=None, profile_name=None):
    """
    Automatically get a local or delegation token signer.

    Example: get_signer(token_path)
    """
    import oci

    if not in_dataflow():
        # We are running locally, use our API Key.
        if file_location is None:
            file_location = oci.config.DEFAULT_LOCATION
        if profile_name is None:
            profile_name = oci.config.DEFAULT_PROFILE
        config = oci.config.from_file(
            file_location=file_location, profile_name=profile_name
        )
        signer = oci.signer.Signer(
            tenancy=config["tenancy"],
            user=config["user"],
            fingerprint=config["fingerprint"],
            private_key_file_location=config["key_file"],
            pass_phrase=config["pass_phrase"],
        )
    else:
        # We are running in Data Flow, use our Delegation Token.
        with open(token_path) as fd:
            delegation_token = fd.read()
        signer = oci.auth.signers.InstancePrincipalsDelegationTokenSigner(
            delegation_token=delegation_token
        )
    return signer


def in_dataflow():
    """
    Determine if we are running in OCI Data Flow by checking the environment.
    """
    if os.environ.get("HOME") == "/home/dataflow":
        return True
    return False


def get_delegation_token_path(spark):
    """
    Get the delegation token path when we're running in Data Flow.
    """
    if not in_dataflow():
        return None
    token_key = "spark.hadoop.fs.oci.client.auth.delegationTokenPath"
    token_path = spark.sparkContext.getConf().get(token_key)
    if not token_path:
        raise Exception(f"{token_key} is not set")
    return token_path


def get_temporary_directory():
    if in_dataflow():
        return "/opt/spark/work-dir/"
    else:
        import tempfile

        return tempfile.gettempdir()


if __name__ == "__main__":
    main()
