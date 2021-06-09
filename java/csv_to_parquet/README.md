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

1. Upload a sample CSV file to object store
2. Customize ```src/main/java/example/Example.java``` with the OCI path to your CSV data. The format is ```oci://<bucket>@<namespace>/path```
  2a. Don't know what your namespace is? Run ```oci os ns get```
  2b. Don't have the OCI CLI installed? [See](https://docs.cloud.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm) to install it.
3. Customize ```src/main/java/example/Example.java``` with the OCI path where you would like to save output data.
4. Compile with MVN to generate the jar file ```csv_to_parquet-1.0-SNAPSHOT.jar```.
5. Recommended: run the sample locally to test it.
6. Upload the JAR file ```csv_to_parquet-1.0-SNAPSHOT.jar``` to an object store bucket.
7. Create a Java Data Flow application pointing to the JAR file ```csv_to_parquet-1.0-SNAPSHOT.jar```
  7a. Refer [Create Java App](https://docs.oracle.com/en-us/iaas/data-flow/using/dfs_data_flow_library.htm#create_java_app)

## To Compile

```sh
mvn package
```

## To Test Locally

```sh
spark-submit --class example.Example target/csv_to_parquet-1.0-SNAPSHOT.jar
```

## To use OCI CLI to run the Java Application

Create a bucket. Alternatively you can re-use an existing bucket.

```sh
oci os bucket create --name <bucket> --compartment-id <compartment_ocid>
oci os object put --bucket-name <bucket> --file csv_to_parquet.py
oci data-flow application create \
    --compartment-id <compartment_ocid> \
    --display-name "CSV to Parquet Java"
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 2.4.4 \
    --file-uri oci://<bucket>@<namespace>/csv_to_parquet.py \
    --language Java
    -class-name example.Example
```

Make note of the Application ID produced.

```sh
oci data-flow run create \
    --compartment-id <compartment_ocid> \
    --application-id <application_ocid> \
    --display-name "CSV to Parquet Java"
```
