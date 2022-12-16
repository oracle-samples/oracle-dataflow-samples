# Overview

This Blog will demonstrate how to connect GCP Bigquery from OCI DataFlow Spark Notebook. Following that will perform some read operation on Bigquery Table using Spark & write down the resultant spark dataframe to OCI Object Storage & also on Autonomous Datawarehouse.

## Prerequisites

Before you begin :
1. Assuming you already have active OCI & GCP Subscription &have access to portal. 

2. Setup OCI Data Flow, OCI Object Storage Bucket and OCI Data Science Notebook.

	Refer [instructions] : (https://docs.oracle.com/en-us/iaas/data-flow/using/data-flow-studio.htm#data-flow-studio)

3. Create & Download "Google API JSON Key secret OCID" for the Project where BigQuery Database is residing on GCP. 

4. Upload the "Google API JSON Key secret OCID" to OCI Object Storage 
	
		"oci://demo-bucketname@OSnamespace/gcp_utility/BigQuery/ocigcp_user_creds.json"

5. Download Spark BigQuery Jar and upload it to Object Storage:

   		Sample: spark-bigquery-with-dependencies_2.12-0.23.2.jar
    
    	[Download Spark BigQuery Jar] : https://mvnrepository.com/artifact/com.google.cloud.spark/spark-bigquery-with-dependencies_2.12/0.23.0


6. Collect below parameters for you GCP BigQuery Table.

		'project' : 'bigquery-public-data'
		'parentProject' : 'core-invention-366213'
		'table' : 'bitcoin_blockchain.transactions'
		"credentialsFile" : "./ocigcp_user_creds.json"
	
7. Download ADW Wallet from OCI Portal & keep the User & Password handy.

##

## Access GCP BigQuery Using OCI Data Science Notebook with OCI Data Flow:

1. Open OCI Data Science Session, where you have already created Conda environment for OCI Data Flow.  [Refer] : Prerequisite Point 2.

2. Open New Notebook with DataFlow as Kernel.
3. Now, Create livy session for OCI Data Flow & provide other required information including GCP BigQuery.

		spark.archives : oci://demo-ds-conda-env@OSNameSpace/conda_environments/cpu/PySpark 3.2 and Data Flow/1.0/pyspark32_p38_cpu_v1#conda
		spark.files : oci://demo-ds-conda-env@OSNameSpace/gcp_utility/BigQuery/ocigcp_user_creds.json
		spark.jars : oci://demo-ds-conda-env@OSNameSpace/gcp_utility/BigQuery/bigquery_spark-bigquery-with-dependencies_2.12-0.23.2.jar
		spark.oracle.datasource.enabled : true
		
Use [ReadBigQuery_FinalNotebook.ipynb] to Access GCP BigQuery Table from OCI Data Flow. And write it down to OCI Object Storage or ADW. 