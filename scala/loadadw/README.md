# Overview

This example shows you how to use OCI Data Flow to process data in OCI Object Store and save the results to Oracle ADW or ATP.

## Prerequisites

Before you begin:

1. Ensure your tenant is configured for Data Flow by following [instructions](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin)
2. Provision an ADW or ATP instance.
3. Create a wallet for your ADW/ATP instance.
4. Store the wallet password in a secret within the OCI Secrets Service.
5. Deploy the Oracle JDBC driver to the repo subdirectory, if it does not already exist.
   * Download a Java 8 compatible (ojdbc8) [JDBC driver](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html)
   * Install the required JARs into your local environment

   On Mac/Linux:

  ```sh
   mvn deploy:deploy-file -Durl=file://$(pwd)/repo -Dfile=ojdbc8-full/ojdbc8.jar -DgroupId=com.oracle -DartifactId=$file -Dpackaging=jar -Dversion=18.3
   mvn deploy:deploy-file -Durl=file://$(pwd)/repo -Dfile=ojdbc8-full/ucp.jar -DgroupId=com.oracle -DartifactId=$file -Dpackaging=jar -Dversion=18.3
   mvn deploy:deploy-file -Durl=file://$(pwd)/repo -Dfile=ojdbc8-full/oraclepki.jar -DgroupId=com.oracle -DartifactId=$file -Dpackaging=jar -Dversion=18.3
   mvn deploy:deploy-file -Durl=file://$(pwd)/repo -Dfile=ojdbc8-full/osdt_cert.jar -DgroupId=com.oracle -DartifactId=$file -Dpackaging=jar -Dversion=18.3
   mvn deploy:deploy-file -Durl=file://$(pwd)/repo -Dfile=ojdbc8-full/osdt_core.jar -DgroupId=com.oracle -DartifactId=$file -Dpackaging=jar -Dversion=18.3
   ```

   On Windows:

  ```cmd
   mvn deploy:deploy-file -Durl=file://%cd%/repo -Dfile=ojdbc8-full/ojdbc8.jar -DgroupId=com.oracle -DartifactId=$file -Dpackaging=jar -Dversion=18.3
   mvn deploy:deploy-file -Durl=file://%cd%/repo -Dfile=ojdbc8-full/ucp.jar -DgroupId=com.oracle -DartifactId=$file -Dpackaging=jar -Dversion=18.3
   mvn deploy:deploy-file -Durl=file://%cd%/repo -Dfile=ojdbc8-full/oraclepki.jar -DgroupId=com.oracle -DartifactId=$file -Dpackaging=jar -Dversion=18.3
   mvn deploy:deploy-file -Durl=file://%cd%/repo -Dfile=ojdbc8-full/osdt_cert.jar -DgroupId=com.oracle -DartifactId=$file -Dpackaging=jar -Dversion=18.3
   mvn deploy:deploy-file -Durl=file://%cd%/repo -Dfile=ojdbc8-full/osdt_core.jar -DgroupId=com.oracle -DartifactId=$file -Dpackaging=jar -Dversion=18.3
   ```

## Load Required Data

Upload a sample CSV file to OCI object store.

## Application Setup

Customize ```src/main/java/example/Example.java``` to set values for walletPath, user, passwordOcid and tnsName.

## Compile Locally

```bash
mvn package
```

## Test Locally

  Test the Application Locally (recommended) You can test the application locally using spark-submit:

  ```bash
  spark-submit --class example.Example target/loadadw-1.0-SNAPSHOT.jar
  ```

## Deploy the Application

```sh
oci os object put --bucket-name <mybucket> --file target/loadadw-1.0-SNAPSHOT.jar
oci data-flow application create \
    --compartment-id <your_compartment> \
    --display-name "Load ADW Java"
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 2.4.4 \
    --file-uri oci://<bucket>@<namespace>/target/loadadw-1.0-SNAPSHOT.jar \
    --language Java \
    --class-name example.Example
```

Make note of the Application OCID produced by this command

## Run the Application using OCI Cloud Shell or OCI CLI

```sh
oci data-flow run create \
    --compartment-id <your_compartment> \
    --application-id <application_ocid> \
    --display-name "Load ADW Java"
```
