Copyright © 2021, Oracle and/or its affiliates. 
The Universal Permissive License (UPL), Version 1.0

Overview:
This example shows you how to use OCI Data Flow to process data in OCI Object Store and save the results to Oracle ADW or ATP.

Required Setup:
    1. Ensure your tenant is configured for Data Flow by following: https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin
    2. Provision an ADW or ATP instance.
    3. Create a wallet for your ADW/ATP instance.
    4. Store the wallet password in a secret within the OCI Secrets Service.
    5. Download the Oracle JDBC driver (verison 19c) from https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html
      5a. Note: Use the Java 8 version for compatibility with the Data Flow runtime.
      5b. Extract the driver into a directory called ojdbc.
    6. (Optional, strongly recommended): Install Spark to test your code locally before deploying to Data Flow.

Load Required Data:
    1. Upload a sample CSV file to OCI object store.

Application Setup:
    1. Customize loadadw.py with:
      1a. Set INPUT_PATH to the OCI path of your CSV data.
      1b. Set PASSWORD_SECRET_OCID to the OCID of the secret created during Required Setup.
      1c. Set TARGET_TABLE to the table in ADW where data is to be written.
      1d. Set TNSNAME to a TNS name valid for the database.
      1e. Set USER to the user who generated the wallet file.
      1f. Set WALLET_PATH to the path on object store for the wallet.

Test the Application Locally (recommended):
You can test the application locally using spark-submit:
spark-submit --jars ojdbc/ojdbc8.jar,ojdbc/ucp.jar,ojdbc/oraclepki.jar,ojdbc/osdt_cert.jar,ojdbc/osdt_core.jar loadadw.py

Packaging your Application:
    1. Create the Data Flow Dependencies Archive as follows:
       docker pull phx.ocir.io/oracle/dataflow/dependency-packager:latest
       docker run --rm -v $(pwd):/opt/dataflow -it phx.ocir.io/oracle/dataflow/dependency-packager:latest
    2. Confirm you have a file named archive.zip with the Oracle JDBC driver in it.

Deploy and Run the Application:
    1. Copy loadadw.py to object store.
    2. Copy archive.zip to object store.
    3. Create a Data Flow Python application. Be sure to include archive.zip as the dependency archive.
      3a. Refer to https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_data_flow_library.htm#create_pyspark_app for more information.
    4. Run the application.

--==[ If you want to use the OCI Cloud Shell or OCI CLI to run the PySpark Application ]==--

# Deploy and Run the Application
oci os object put --bucket-name <bucket> --file loadadw.py
oci os object put --bucket-name <bucket> --file archive.zip
oci data-flow application create \
    --compartment-id <compartment_ocid> \
    --display-name "PySpark Load ADW" \
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
    --display-name 'PySpark Load ADW"
