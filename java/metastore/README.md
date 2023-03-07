# Read CSV data write/read from Metastore and then Write/Read from ADW
## _Note_: This example will work in Dataflow code editor plugin and OCI Dataflow


Before you begin:

* Ensure your tenant is configured according to the instructions to [setup admin](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin)
* Know your object store namespace.
* Know the OCID of a compartment where you want to load your data and create applications.
* (Optional, strongly recommended): Install Spark to test your code locally before deploying.


## Instructions

1. Upload a sample CSV file to object store
2. Customize(if required) ```src/main/java/example/adw/MetastoreToADW.java``` with the OCI path to your CSV data. The format is ```oci://<bucket>@<namespace>/path```\
  2a. Don't know what your namespace is? Run ```oci os ns get```\
  2b. Don't have the OCI CLI installed? [See](https://docs.cloud.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm) to install it.
3. Compile with MVN to generate the jar file ```metastore-1.0-SNAPSHOT.jar```.
5. Recommended: run the sample code editor to test it.
6. Upload the JAR file ```metastore-1.0-SNAPSHOT,jar``` to an object store bucket after testing with code editor dataflow plugin
7. Create a Java Data Flow application pointing to the JAR file ```metastore-1.0-SNAPSHOT.jar```
  7a. Refer [Create Java App](https://docs.oracle.com/en-us/iaas/data-flow/using/dfs_data_flow_library.htm#create_java_app)

## To Compile

```sh
mvn clean package
```

## To Test Locally

```shspark-submit  --properties-file spark-properties.conf --jars "<ojdbc8-21.7.0.0,oraclepki-21.7.0.0,osdt_cert-21.7.0.0,osdt_core-21.7.0.0,ucp-21.7.0.0>"  --conf spark.driver.extraJavaOptions="-Djava.io.tmpdir=<anyTempLocationWithReadAndWritePermission>" --conf spark.executor.extraJavaOptions="-Djava.io.tmpdir=<anyTempLocationWithReadAndWritePermission>" --conf spark.oracle.datasource.enabled=true --conf spark.sql.warehouse.dir=<warehouseDir> --conf spark.hadoop.oracle.dcat.metastore.id=<metastoreId> --conf spark.hadoop.OCI_TENANT_METADATA=<tenantId>  --conf spark.hadoop.OCI_USER_METADATA=<userId>  --conf spark.hadoop.OCI_FINGERPRINT_METADATA=<fingerPrint> --conf spark.hadoop.OCI_PVT_KEY_FILE_PATH=<privateKeyPemFile> --conf spark.hadoop.fs.oci.client.auth.tenantId=<tenantId> --conf spark.hadoop.fs.oci.client.auth.userId=<userId> --conf spark.hadoop.fs.oci.client.auth.fingerprint=<fingerPrint>  --conf spark.hadoop.fs.oci.client.auth.pemfilepath=<privateKeyPemFile>  --conf spark.hadoop.OCI_REGION_METADATA=<region>  --conf spark.hadoop.fs.oci.client.hostname=<hostName>  --conf spark.hadoop.oci.metastore.uris=<metastore_uri>  --class eexample.adw.MetastoreToADW metastore-1.0-SNAPSHOT.jar <csv_file_path> <csv_file_name> <object_storage_output_path> <databses_name> <table_name> <object_storage_adb_wallet_path/adbID> <adb_user> <adb_connection_id> <adb_password>
```

## To use OCI CLI to run the Java Application

Create a bucket. Alternatively you can re-use an existing bucket.

```sh
oci os bucket create --name <bucket> --compartment-id <compartment_ocid>
oci os object put --bucket-name <bucket> --file csv_to_parquet.py
oci data-flow application create \
    --compartment-id <compartment_ocid> \
    --display-name "Metastore to ADW"
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 3.2.1 \
    --file-uri oci://<bucket>@<namespace>/metastore-1.0-SNAPSHOT.jar \
    --arguments <csv_file_path> <csv_file_name> <object_storage_output_path> <databses_name> <table_name> <object_storage_adb_wallet_path/adbID> <adb_user> <adb_connection_id> <adb_password>
    --language Java \
    -class-name eexample.adw.MetastoreToADW
```

Make note of the Application ID produced.

```sh
oci data-flow run create \
    --compartment-id <compartment_ocid> \
    --application-id <application_ocid> \
    --display-name "Metastore to ADW"
```
