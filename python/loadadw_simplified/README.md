# Overview

This example shows you how to use OCI Data Flow to process data in OCI Object Store and save the results to Oracle ADW or ATP or AJD.

## Prerequisites

Before you begin:

1. Ensure your tenant is configured for Data Flow by following [instructions](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin)
2. Provision an ADW or ATP or AJD instance.


## Application Setup

Customize ```loadadw_simplified.py``` with:

* Set ADB_ID autonomous database OCID.
* Set USER to the user who generated the wallet file.
* Set PASSWORD password to the database.
* Set SRC_TABLE source autonomous table name.
* Set TARGET_TABLE target autonomous table name.


## Packaging your Application

No additional packages required.

## Deploy and Run the Application

* Copy loadadw_simplified.py to object store.
* Create a Data Flow Python application. No archive required.
  * Refer [here](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_data_flow_library.htm#create_pyspark_app) for more information.
* Run the application.

## Run the Application using OCI Cloud Shell or OCI CLI

Create a bucket. Alternatively you can re-use an existing bucket.

```sh
oci os object put --bucket-name <bucket> --file loadadw.py
oci os object put --bucket-name <bucket> --file archive.zip
oci data-flow application create \
    --compartment-id <compartment_ocid> \
    --display-name "PySpark Load ADW Simplified" \
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 3.0.2 \
    --file-uri oci://<bucket>@<namespace>/loadadw_simplified.py \
    --language Python
oci data-flow run create \
    --application-id <application_ocid> \
    --compartment-id <compartment_ocid> \
    --application-id <application_ocid> \
    --display-name 'PySpark Load ADW"
```
