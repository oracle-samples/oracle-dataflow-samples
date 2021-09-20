# Convert CSV data to Parquet

The most common first step in data processing applications, is to take data from some source and get it into a format that is suitable for reporting and other forms of analytics. In a database, you would load a flat file into the database and create indexes. In Spark, your first step is usually to clean and convert data from a text format into Parquet format. Parquet is an optimized binary format supporting efficient reads, making it ideal for reporting and analytics.

![Convert CSV Data to Parquet](./images/csv_to_parquet.png)

## Prerequisites

Before you begin:

* Ensure your tenant is configured according to the instructions to [setup admin](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin)
* Know your object store namespace.
* Know the OCID of a compartment where you want to load your data and create applications.
* (Optional, strongly recommended): Install Spark to test your code locally before deploying.

## Instructions

1. Upload a sample CSV file of your choice to object store.
2. Upload ```csv_to_parquet.py``` to object store.
3. Create a Python Data Flow Application pointing to ```csv_to_parquet.py```
  3a. Refer [here](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_data_flow_library.htm#create_pyspark_app)
  3b. The Spark application requires two arguments: --input-path and --output-path. These must be OCI HDFS URIs pointing to your source CSV file and target output path. Put these in the Arguments field of the Data Flow Application.
  3c. Example Arguments field: "--input-path oci://sample@namespace/input.csv --output-path oci://sample@namespace/output.parquet"

## To use OCI CLI to run the PySpark Application

Set all these variables based on your OCI tenancy.

```sh
COMPARTMENT_ID=ocid1.compartment.oc1..<your_compartment_id>
NAMESPACE=my_object_storage_namespace
BUCKET=my_bucket
INPUT_PATH=oci://$BUCKET@$NAMESPACE/my_csv_file.csv
OUTPUT_PATH=oci://$BUCKET@$NAMESPACE/output_parquet
```

Run these commands to upload all files.

```sh
oci os bucket create --name $BUCKET --compartment-id $COMPARTMENT_ID
oci os object put --bucket-name $BUCKET --file my_csv_file.csv
oci os object put --bucket-name $BUCKET --file csv_to_parquet.py
```

Launch the Spark application to convert CSV to Parquet.

```sh
oci data-flow run submit \
    --compartment-id $COMPARTMENT_ID \
    --display-name "PySpark Convert CSV to Parquet" \
    --execute "oci://$BUCKET@$NAMESPACE/csv_to_parquet.py --input-path $INPUT_PATH --output-path $OUTPUT_PATH"
```

Make note of "id" field this command returns. When the job is finished, view its output using:

```sh
oci data-flow run get-log --run-id <run_id> --name spark_application_stdout.log.gz --file -
```
