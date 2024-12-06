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

##Note: 
For Spark version > 3.5.0 version

      1. rm -rf src/main/java/example/OboTokenClientConfigurator.java
      2. mv src/main/java/example/OboTokenClientConfiguratorV2.java src/main/java/example/OboTokenClientConfigurator.java
      2. Update loadadw.sbt oci-java-sdk-version, oci-java-sdk-objectstorage and oci-java-sdk-secrets  properties to 3.34.1

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
## Additional Applications 

### Check Remote Bucket
This application is used to verify a remote bucket that can be accessible by a resource principal only. 
Ensure policy statement allows a client tenant is able to access the service tenancy buckets for instance:
#### Client tenancy
```sh
define tenancy <service_tenant> as ocid1.tenancy.oc1..aaaaaaa.....
admit any-user of tenancy <service_tenant> to read buckets in tenancy where ALL {request.principal.type = 'dataflowrun'}
admit any-user of tenancy <service_tenant> to read objects in tenancy where ALL {request.principal.type = 'dataflowrun'}
admit any-user of tenancy <service_tenant> to inspect compartments in tenancy where ALL {request.principal.type = 'dataflowrun'}
```
#### Service tenancy
```sh
define tenancy <client_tenant> as ocid1.tenancy.oc1..aaaaaaaavn3vhsyrj46hucx4h533lgtnitcydmwypxxsdq2g42cskqpxxdoa
Endorse any-user to manage buckets in tenancy <client_tenant> where ALL {request.principal.type = 'dataflowrun'}
Endorse any-user to inspect compartments in tenancy <client_tenant> where ALL {request.principal.type = 'dataflowrun'}
Endorse any-user to manage objects in tenancy <client_tenant> where ALL {request.principal.type = 'dataflowrun'}
```
To execute from the command line just specify the class example.checkRemoteBucket and pass the remote bucket to be tested, for instance:
```sh
oci data-flow application create \
    --compartment-id <your_compartment> \
    --display-name "Check Remote Bucket"
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 3.5.0 \
    --file-uri oci://<bucket>@<namespace>/target/loadadw-1.0-SNAPSHOT.jar \
    --language Scala \
    --class-name example.checkRemoteBucket
    --parameters oci://<remote bucket>@<remote tenancy namespace>/
```
### Private Endpoint Test
Before you run a Dataflow application using a Private Endpoint, you may wonder to quickly test the connectivity. This next example provides a simple way to validate that. 
To do so, create an application using the class ```example.PrivateEndpointTest``` and pass the FQDN and port number the application needs to reach using the Dataflow Private Endpoint.
For instance:
```sh
oci data-flow application create \
    --compartment-id <your_compartment> \
    --display-name "Test Dataflow PE"
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 3.5.0 \
    --file-uri oci://<bucket>@<namespace>/target/loadadw-1.0-SNAPSHOT.jar \
    --language Scala \
    --class-name example.PrivateEndpointTest
    --parameters p3aokzam.adb.us-phoenix-1.oraclecloud.com 1522
    --private-endpoint-id <Dataflow PrivateEndpoint OCID>
```
The application stdout will display the result of the test, for instance:
```
Error resolving FQDN 'p3aokzam.adb.us-phoenix-1.oraclecloud.com': p3aokzam.adb.us-phoenix-1.oraclecloud.com: Name or service not known
Failed to resolve FQDN 'p3aokzam.adb.us-phoenix-1.oraclecloud.com'. Skipping connectivity test.
```
Or a positive resolution and connectivity as such:
```
FQDN 'p3aokzam.adb.us-phoenix-1.oraclecloud.com' resolved to IP '255.33.36.1'. Testing connectivity...
Success: Able to connect to p3aokzam.adb.us-phoenix-1.oraclecloud.com (255.33.36.1) on port 1522.
```