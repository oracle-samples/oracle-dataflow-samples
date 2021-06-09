# Overview

This example shows you how to use OCI Data Flow to interface with Oracle NoSQL Database Cloud Service.

## Prerequisites

Before you begin:

1. Ensure your tenant is configured for Data Flow by following [instructions](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin)
2. Provision an Oracle NoSQL Database cloud service table.
3. Download the Oracle NoSQL Database python sdk. The home for the project is [here](https://nosql-python-sdk.readthedocs.io/en/stable/index.html)
   The SDK can be installed using pip:

   ```bash
      pip install borneo
    ```  

   * See [the installation guide](https://nosql-python-sdk.readthedocs.io/en/stable/installation.html) for additional requirements and and alternative install methods.

4. (Optional, strongly recommended): Install Spark to test your code locally before deploying to Data Flow.

## Application Setup

Customize ```nosql_example.py``` with:

* Set COMPARTMENT_ID to the Oracle NoSQL Database Cloud service table.
* Set ENDPOINT to region that has Oracle NoSQL Database Cloud service table for e.g. ``us-ashburn-1``.
* Set TABLE_NAME to the table in Oracle NoSQL Database cloud service table.
* Set INDEX_NAME to the name of the index to create in Oracle NoSQL Database cloud service table.

## Testing Locally

Test the Application Locally (recommended):

  ```bash
  python nosql_example.py
  ```

## Packaging your Application

* Create the Data Flow Dependencies Archive as follows:

```bash
   docker pull phx.ocir.io/oracle/dataflow/dependency-packager:latest
   docker run --rm -v $(pwd):/opt/dataflow -it phx.ocir.io/oracle/dataflow/dependency-packager:latest
 ```

* Confirm you have a file named **archive.zip** with the Oracle NoSQL Database python SDK in it.

## Deploy and Run the Application

* Copy ```nosql_example.py``` to object store.
* Copy the ```archive.zip``` generated while packaging the application to object store.
* Create a Data Flow Python application. Be sure to include archive.zip as the dependency archive.
  * Refer [here](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_data_flow_library.htm#create_pyspark_app) for more information.
* Run the application.

## Run the Application using OCI Cloud Shell or OCI CLI

Create a bucket. Alternatively you can re-use an existing bucket.

```sh
oci os object put --bucket-name <bucket> --file nosql_example.py
oci os object put --bucket-name <bucket> --file archive.zip
oci data-flow application create \
    --compartment-id <compartment_ocid> \
    --display-name "Oracle NoSQL Example" \
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 2.4.4 \
    --file-uri oci://<bucket>@<namespace>/nosql_example.py \
    --archive-uri oci://<bucket>@<namespace>/archive.zip \
    --language Python
oci data-flow run create \
    --application-id <application_ocid> \
    --compartment-id <compartment_ocid> \
    --application-id <application_ocid> \
    --display-name 'Oracle NoSQL Example"
```
