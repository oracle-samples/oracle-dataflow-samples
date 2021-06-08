# Overview

Random forest is a Supervised Learning algorithm which uses ensemble learning method for classification and regression.This example shows you how to use OCI Data Flow to build a Random Forest Regression Model to predict mileage (mpg) for cars.

## Prerequisites

Before you begin:

* Ensure your tenant is configured according to the instructions to [setup admin](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin)
* Know your object store namespace.
* (Optional, strongly recommended): Install Spark to test your code locally before deploying.

## Load Required Data

Upload a sample CSV file to OCI object store.

## Application Setup

Customize ```random_forest_regression.py``` with:

1. Set INPUT_PATH to the OCI path of your CSV data.
2. Customize ```random_forest_regression.py``` with the OCI path to your CSV data. The format is ```oci://<bucket>@<namespace>/path```
  2a. Don't know what your namespace is? Run ```oci os ns get```
  2b. Don't have the OCI CLI installed? [See](https://docs.cloud.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm) to install it.
3. Customize ```random_forest_regression.py``` with the OCI path where you would like to save output data.
4. Recommended: run the sample locally to test it. Refer [Develop Oracle Cloud Infrastructure Data Flow Applications Locally, Deploy to The Cloud](https://docs.oracle.com/en-us/iaas/data-flow/data-flow-tutorial/develop-apps-locally/front.htm)
5. Upload ```random_forest_regression.py``` to an object store bucket.
6. Create a Python Data Flow application pointing to ```random_forest_regression.py```

## Testing Locally

Test the Application Locally (recommended):You can test the application locally using :

  ```bash
  python3 random_forest_regression.py
  ```

If it works you'll see output like :

```bash
 RMSE=3.888566643366089 r2=0.7532412722764463
 ```

## Deploy and Run the Application

* Copy ``random_forest_regression`` to object store.
* Create a Data Flow Python application. Refer [Create PySpark App](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_data_flow_library.htm#create_pyspark_app) for more information.
* Run the application.

## Run the Application using OCI Cloud Shell or OCI CLI

Create a bucket. Alternatively you can re-use an existing bucket.

```sh
oci os object put --bucket-name <bucket> --file random_forest_regression
oci data-flow application create \
    --compartment-id <compartment_ocid> \
    --display-name "Random Forest Regression" \
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
    --display-name "Random Forest Regression"
```
