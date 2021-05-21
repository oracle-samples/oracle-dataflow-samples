# Overview

This example shows you how to use OCI Data Flow to process data in OCI Object Store and save the results to Oracle ADW or ATP.

## Prerequisites

Before you begin:

1. Ensure your tenant is configured for Data Flow by following [instructions](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin)
2. Provision an ADW or ATP instance.
3. Create a wallet for your ADW/ATP instance.
4. Store the wallet password in a secret within the OCI Secrets Service.
5. Download the Oracle JDBC driver (version 19c) from [here](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html)
   * Note: Use the Java 8 version for compatibility with the Data Flow runtime.
   * Extract the driver into a directory called ojdbc.
6. (Optional, strongly recommended): Install Spark to test your code locally before deploying to Data Flow.

## Load Required Data

Upload a sample CSV file to OCI object store.

## Application Setup

Customize ```loadadw.py``` with:

* Set INPUT_PATH to the OCI path of your CSV data.
* Set PASSWORD_SECRET_OCID to the OCID of the secret created during Required Setup.
* Set TARGET_TABLE to the table in ADW where data is to be written.
* Set TNSNAME to a TNS name valid for the database.
* Set USER to the user who generated the wallet file.
* Set WALLET_PATH to the path on object store for the wallet.

  Test the Application Locally (recommended):
  You can test the application locally using spark-submit:

  ```bash
  spark-submit --jars ojdbc/ojdbc8.jar,ojdbc/ucp.jar,ojdbc/oraclepki.jar,ojdbc/osdt_cert.jar,ojdbc/osdt_core.jar loadadw.py
  ```

## Packaging your Application

* Create the Data Flow Dependencies Archive as follows:
  
```bash
   docker pull phx.ocir.io/oracle/dataflow/dependency-packager:latest
   docker run --rm -v $(pwd):/opt/dataflow -it phx.ocir.io/oracle/dataflow/dependency-packager:latest
 ```

* Confirm you have a file named **archive.zip** with the Oracle JDBC driver in it.

## Deploy and Run the Application

* Copy loadadw.py to object store.
* Copy archive.zip to object store.
* Create a Data Flow Python application. Be sure to include archive.zip as the dependency archive.
  * Refer [here](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_data_flow_library.htm#create_pyspark_app) for more information.
* Run the application.

## Run the Application using OCI Cloud Shell or OCI CLI

Create a bucket. Alternatively you can re-use an existing bucket.

```sh
oci os object put --bucket-name <bucket> --file loadadw.py
oci os object put --bucket-name <bucket> --file archive.zip
oci data-flow application create \
    --compartment-id <compartment_ocid> \
    --display-name "PySpark Load ADW" \
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 2.4.4 \
    --file-uri oci://<bucket>@<namespace>/loadadw.py \
    --archive-uri oci://<bucket>@<namespace>/archive.zip \
    --language Python
oci data-flow run create \
    --application-id <application_ocid> \
    --compartment-id <compartment_ocid> \
    --application-id <application_ocid> \
    --display-name 'PySpark Load ADW"
```
