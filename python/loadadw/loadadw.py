#!/usr/bin/env python3

# Copyright © 2021, Oracle and/or its affiliates. 
# The Universal Permissive License (UPL), Version 1.0 as shown at https://oss.oracle.com/licenses/upl.

import os

from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext

def main():
    """
    Your application code goes here.

    Look for the sections marked TODO and customize based on your data and processing needs.
    """
    # TODO: Set all these variables.
    INPUT_PATH = "oci://<bucket>@<tenancy>/fake_data.csv"
    PASSWORD_SECRET_OCID = "ocid1.vaultsecret.oc1.iad.<secret_ocid>"
    TARGET_TABLE = "processed"
    TNSNAME = "<tnsname>"
    USER = "ADMIN"
    WALLET_PATH = "oci://<bucket>@<tenancy>/<wallet>.zip"

    # Set up Spark.
    spark_session = get_dataflow_spark_session()
    sql_context = SQLContext(spark_session)

    # Load our data.
    print("Reading data from object store")
    input_dataframe = spark_session.read.option("header", "true").csv(INPUT_PATH)

    # Download and distribute our wallet file.
    print("Downloading wallet")
    wallet_path = download_wallet(spark_session, WALLET_PATH)
    adw_url = "jdbc:oracle:thin:@{}?TNS_ADMIN={}".format(TNSNAME, wallet_path)

    # Get our password using the secret service.
    print("Getting wallet password")
    token_path = get_delegation_token_path(spark_session)
    password = get_password_from_secrets(token_path, PASSWORD_SECRET_OCID)
    print("Done getting wallet password")

    # Save the results to the database.
    print("Saving processed data to " + adw_url)
    properties = {
        "driver": "oracle.jdbc.driver.OracleDriver",
        "oracle.net.tns_admin": TNSNAME,
        "password": password,
        "user": USER,
    }
    input_dataframe.write.jdbc(
        url=adw_url, table=TARGET_TABLE, mode="Overwrite", properties=properties
    )
    print("Done saving processed data to database")

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


def get_authenticated_client(token_path, client, file_location=None, profile_name=None):
    """
    Get an an authenticated OCI client.

    Example: get_authenticated_client(token_path, oci.object_storage.ObjectStorageClient)
    """
    import oci

    if not in_dataflow():
        # We are running locally, use our API Key.
        if file_location is None:
            file_location = oci.config.DEFAULT_LOCATION
        if profile_name is None:
            profile_name = oci.config.DEFAULT_PROFILE
        config = oci.config.from_file(file_location=file_location, profile_name=profile_name)
        authenticated_client = client(config)
    else:
        # We are running in Data Flow, use our Delegation Token.
        with open(token_path) as fd:
            delegation_token = fd.read()
        signer = oci.auth.signers.InstancePrincipalsDelegationTokenSigner(
            delegation_token=delegation_token
        )
        authenticated_client = client(config={}, signer=signer)
    return authenticated_client


def get_password_from_secrets(token_path, password_ocid):
    """
    Get a password from the OCI Secrets Service.
    """
    import base64
    import oci

    secrets_client = get_authenticated_client(token_path, oci.secrets.SecretsClient)
    response = secrets_client.get_secret_bundle(password_ocid)
    base64_secret_content = response.data.secret_bundle_content.content
    base64_secret_bytes = base64_secret_content.encode("ascii")
    base64_message_bytes = base64.b64decode(base64_secret_bytes)
    secret_content = base64_message_bytes.decode("ascii")
    return secret_content


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

def download_wallet(spark, wallet_path):
    """
    Download an ADW/ATP wallet file and prepare it for use in a Data Flow
    application.
    """
    import oci
    import zipfile

    # Get an object store client.
    token_path = get_delegation_token_path(spark)
    object_store_client = get_authenticated_client(
        token_path, oci.object_storage.ObjectStorageClient
    )

    # Download the wallet file.
    from urllib.parse import urlparse
    parsed = urlparse(wallet_path)
    bucket_name, namespace = parsed.netloc.split("@")
    file_name = parsed.path[1:]
    response = object_store_client.get_object(namespace, bucket_name, file_name)
    temporary_directory = get_temporary_directory()
    zip_file_path = os.path.join(temporary_directory, "wallet.zip")
    with open(zip_file_path, "wb") as fd:
        for chunk in response.data.raw.stream(1024 * 1024, decode_content=False):
            fd.write(chunk)

    # Extract everything locally.
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(temporary_directory)

    # Distribute all wallet files.
    contents = "cwallet.sso ewallet.p12 keystore.jks ojdbc.properties sqlnet.ora tnsnames.ora truststore.jks".split()
    spark_context = spark.sparkContext
    for file in contents:
        spark_context.addFile(os.path.join(temporary_directory, file))

    return temporary_directory

if __name__ == "__main__":
    main()
