# Convert JSON data to Parquet

The initial phase in data processing typically involves extracting data from a source and transforming it into a format conducive to reporting and various analytical tasks. For instance, in a database environment, this may entail importing a flat file and establishing indexes. In Spark, the initial step usually involves the cleansing and conversion of data from JSON to the Parquet format. Parquet, being an optimized binary format with excellent read performance, is particularly well-suited for reporting and analytics purposes.

## Prerequisites

Before you begin:

* Ensure your tenant is configured according to the instructions to [setup admin](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin)
* Know your object store namespace.
* Know the OCID of a compartment where you want to load your data and create applications.
* (Optional, but recommended): Insure you are able to run the application locally using the [Code Editor](https://docs.public.oneportal.content.oci.oraclecloud.com/en-us/iaas/data-flow/using/code-editor-using.htm).

## Instructions

1. Upload a sample JSON file of your choice to object store.
2. Upload ```json_to_parquet.py``` to object store.
3. Create a Python Data Flow Application pointing to ```json_to_parquet.py```
  3a. Refer [here](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_data_flow_library.htm#create_pyspark_app)
  3b. The Spark application requires two arguments: (-i or --input-path, and -o or --output-path. These must be OCI HDFS URIs pointing to your source JSON file and target output path. Put these in the Arguments field of the Data Flow Application.
  3c. Example Arguments field: "-i oci://sample@namespace/input.csv -o oci://sample@namespace/output.parquet"

## To use Code Editor to run the json_to_parquet.py 

Ensure you have looked at the [Running an Application with Code Editor](https://docs.public.oneportal.content.oci.oraclecloud.com/en-us/iaas/data-flow/using/code-editor-app-run.htm#code-editor-app-run)

The json_to_parquet.py can be downloaded from repository as soon as you create a project from template and choose the python template.

1. Within Code Editor, under the Dataflow OCI Plugin, navigate to the Compartment where you want to test application to reside
2. On the Projects folder, right click to Create Project
3. On the Project prompt, choose "Create from a template"
4. Choose then Python and then "json_to_parquet"
   1. This will checkout the existing code from the Github repository and make available to run locally
5. On the new Project just create, right-click to get to the pulldown menu and set it to Run Locally
   1. Provide the local python script (json_to_parquet.py) and the arguments as above example.

## To use OCI CLI to run the PySpark Application

Set all these variables based on your OCI tenancy.

```sh
COMPARTMENT_ID=ocid1.compartment.oc1..<your_compartment_id>
NAMESPACE=my_object_storage_namespace
BUCKET=my_bucket
INPUT_PATH=oci://$BUCKET@$NAMESPACE/json_file.json
OUTPUT_PATH=oci://$BUCKET@$NAMESPACE/output_parquet
```

Run these commands to upload all files.

```sh
oci os bucket create --name $BUCKET --compartment-id $COMPARTMENT_ID
oci os object put --bucket-name $BUCKET --file json_file.json
oci os object put --bucket-name $BUCKET --file json_to_parquet.py
```

Launch the Spark application to convert JSON to Parquet.

```sh
oci data-flow run submit \
    --compartment-id $COMPARTMENT_ID \
    --display-name "PySpark Convert JSON to Parquet" \
    --execute "oci://$BUCKET@$NAMESPACE/json_to_parquet.py --input-path $INPUT_PATH --output-path $OUTPUT_PATH"
```

Make note of "id" field this command returns. When the job is finished, view its output using:

```sh
oci data-flow run get-log --run-id <run_id> --name spark_application_stdout.log.gz --file -
```
