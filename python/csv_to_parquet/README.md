# Convert CSV data to Parquet.
Sample to convert CSV data to Parquet.


## Prerequisites
Before you begin:

* A - Ensure your tenant is configured according to the instructions [here](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin)
* B - Know your object store namespace.
* C - Know the OCID of a compartment where you want to load your data and create applications.
* D - (Optional, strongly recommended): Install Spark to test your code locally before deploying.



## Instructions:

1. Upload a sample CSV file to object store
2. Customize csv_to_parquet.py with the OCI path to your CSV data. The format is ```oci://<bucket>@<namespace>/path```
  2a. Don't know what your namespace is? Run ```oci os ns get```
  2b. Don't have the OCI CLI installed? [See](https://docs.cloud.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm) to install it.
3. Customize ```csv_to_parquet.py``` with your own processing logic.
   * If you don't customize the script, the application will compute the number of distinct values of each column.
4. Customize ```csv_to_parquet.py``` with the OCI path where you would like to save output data.
5. Recommended: run the sample locally to test it.
6. Upload ```csv_to_parquet.py``` to an object store bucket.
7. Create a Python Data Flow application pointing to ```csv_to_parquet.py```
  7a. Refer [here](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_data_flow_library.htm#create_pyspark_app)


## To use OCI CLI to run the PySpark Application

Create a bucket. Alternatively you can re-use an existing bucket.
```
oci os bucket create --name <bucket> --compartment-id <compartment_ocid>
oci os object put --bucket-name <bucket> --file csv_to_parquet.py
oci data-flow application create \
    --compartment-id <compartment_ocid> \
    --display-name "PySpark Object to Object" \
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 2.4.4 \
    --file-uri oci://<bucket>@<namespace>/csv_to_parquet.py \
    --language Python
```
Make note of the Application ID produced.
```
oci data-flow run create \
    --compartment-id <compartment_ocid> \
    --application-id <application_ocid> \
    --display-name 'PySpark Object to Object"
```
