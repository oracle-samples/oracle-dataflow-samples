# Metatsore

The given example illustrates connecting to metastore from cloud-shell. It performs the following steps:
1. Reads a csv file from object storage ans displays its contents
2. Connects to Metastore via spark-sql. It then lists all databases, creates a new database if it does not exist and write the contents of csv file into a table in p[arquet format.
3. Read the parquet data from table mentioned above and write it on object storage in a separate location.


## Prerequisites

Before you begin:

* Ensure your tenant is configured according to the instructions to [setup admin](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin)
* Know your object store namespace.
* Ensure that you have an instance of metastore up and running in your tenancy and have proper permissions. [Setup Instructions](https://docs.oracle.com/en-us/iaas/data-catalog/using/metastore.htm)
* (Optional, strongly recommended): Install Spark to test your code locally before deploying.

## Instructions

1. Upload a sample CSV file to object store
2. Update spark-properties.conf accordingly

## Compile and Package

```sh
mvn clean install
```

## To Run

```sh
spark-submit --properties-file spark-properties.conf --class example.metastore.SampleProgram metastore-1.0-SNAPSHOT.jar oci://test-bucket@<tenancy_name> <csv_file_path> <object_storage_output_path> <databses_name> <table_name>
```