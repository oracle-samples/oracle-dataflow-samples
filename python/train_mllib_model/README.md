# Overview

An important task in ML is model selection, or using data to find the best model or parameters for a given task. This is also called tuning. Tuning may be done for individual Estimators such as LogisticRegression, or for entire Pipelines which include multiple algorithms, featurization, and other steps. Users can tune an entire Pipeline at once, rather than tuning each element in the Pipeline separately. This example scores customer profiles using a "Recency, Frequency, Monetary Value" (RFM) metric.

## Prerequisites

Before you begin:

* Ensure your tenant is configured according to the instructions to [setup admin](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin)
* Know your object store namespace.
* (Optional, strongly recommended): Install Spark to test your code locally before deploying.

## Load Required Data

Upload a sample CSV file to OCI object store.

## Application Setup

1. Recommended: run the sample locally to test it.
2. Upload the sample CSV file to object store
3. Upload ```train_mllib_model.py``` to an object store bucket.
4. Create a Python Data Flow application pointing to ```train_mllib_model.py```
  4a. Refer [here](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_data_flow_library.htm#create_pyspark_app)

## Run the Application using OCI Cloud Shell or OCI CLI

### Deploy the application to Object Storage

Before deploying the application set the following.

BUCKET= ```Enter an OCI object storage bucket here```
COMPARTMENTID= ```Enter an OCI compartment here```
NAMESPACE=```Enter your oci namespace here```

```sh
oci os object put --bucket-name $BUCKET --file train_mllib_model.py
```

### Upload the training data

```sh
oci os object put --bucket-name $BUCKET --file moviestream_subset.csv
```

### Run the Application. Adjust shape size/count if you want

```sh
oci data-flow run submit \
    --compartment-id $COMPARTMENTID \
    --executor-shape VM.Standard2.1 \
    --num-executors 2 \
    --execute "oci://$BUCKET@$NAMESPACE/train_mllib_model.py --input oci://$BUCKET@$NAMESPACE/moviestream_subset.csv --output oci://$BUCKET@$NAMESPACE/scores.csv"
  ```

Make note of the OCID that is returned in the "id" field.

### Check the status

```sh
oci data-flow run get --run-id <ocid>
```

### When the lifecycle state of that run reaches SUCCEEDED, fetch the application output

```sh
oci data-flow run get-log --run-id <ocid> --name spark_application_stdout.log.gz --file -
```
