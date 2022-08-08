# Overview

This example shows you how to read/write from/to ADW/ATP/AJD using oracle datasource.
This example works only in dataflow, not from local.Please reach out to dataflow service or support, if you want to try from local.

## Prerequisites

Before you begin:

1. Ensure your tenant is configured for Data Flow by following [instructions](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin)
2. Provision an ADW/ATP/AJD.
3. Load data into source table.

## Application Setup

Customize ```loadadw_simplified_autologin``` with:
Note: no username/password required.This requires cwalet.sso to be set with credential for connection.
For more details, https://docs.oracle.com/cd/B19306_01/network.102/b14266/cnctslsh.htm#CBHBGICA

* Set WALLET_URI wallet zip file object storage or hdfs compatible location.
* Set SRC_TABLE source autonomous table name.
* Set TARGET_TABLE target autonomous table name.


Customize ```loadadw_simplified.py``` with:

* Set ADB_ID autonomous database OCID.
* Set USER to the user who generated the wallet file.
* Set PASSWORD password to the database.
* Set SRC_TABLE source autonomous table name.
* Set TARGET_TABLE target autonomous table name.

Customize ```loadadw_with_wallet_objectstorage.py``` with:

* Set WALLET_URI wallet zip file object storage or hdfs compatible location.
* Set USER to the user who generated the wallet file.
* Set PASSWORD password to the database.
* Set SRC_TABLE source autonomous table name.
* Set TARGET_TABLE target autonomous table name.

## Packaging your Application

No additional packages required for this example.

## Deploy and Run the Application

* Copy loadadw_simplified.py to object store.
* Create a Data Flow Python application. No archive required.
  * Refer [here](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_data_flow_library.htm#create_pyspark_app) for more information.
  * <img src="/Users/xinyazha/sss_github/oracle-dataflow-samples/python/loadadw_simplified/console_exmple_1.png" style="zoom:80%;" />
  * <img src="/Users/xinyazha/sss_github/oracle-dataflow-samples/python/loadadw_simplified/console_example_2.png" style="zoom:80%;" />
* Run the application.

## Run the Application using OCI Cloud Shell or OCI CLI

Create a bucket. Alternatively you can re-use an existing bucket.

```sh
spark_application = loadadw_simplified.py # replace with ```loadadw_with_wallet_objectstorage.py``` if required
oci os object put --bucket-name <bucket> --file ${spark_application}
oci data-flow application create \
    --compartment-id <compartment_ocid> \
    --display-name "PySpark Load ADW Simplified" \
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 3.0.2 \
    --file-uri oci://<bucket>@<namespace>/${spark_application} \
    --language Python
    --configuration "spark.oracle.datasource.enabled=true"
oci data-flow run create \
    --application-id <application_ocid> \
    --compartment-id <compartment_ocid> \
    --display-name 'PySpark Load ADW Simplified"
```

