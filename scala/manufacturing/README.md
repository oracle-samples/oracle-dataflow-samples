# Predict Remaining Useful Life (RUL)
    One of the most common uses cases of predictive manitenance in manufacturing industry is 
predicting Remaining Useful Life (RUL) of equipment.This sample application demonstrates running streaming 
application with doing machine learning on top of it to predict RUL of equipment.

This application has set of three spark applications,
1. RULSurvivalModelTrainer - Offline model trainer for predicting RUL.(Spark Batch)
2. SensorDataSimulator     - Random sensor data simulator for equipment.(Spark Streaming)
3. RealtimeRULPredictor    - Realtime RUL Predictor (Spark Streaming)
4. DeltaTable              - DeltaTable Operations on predicted dataset

## One Click Magic Button
This is a Terraform configuration that creates resources on Oracle Cloud Infrastructure for Manufacturing application.

### Prerequisites
  * Ensure you have access to MANAGE OCI Object Storage,OCI Streaming,OCI Resource Manager,OCI Autonomous Databases, 
  OCI Logging,OCI DataFlow,OCI Policies,OCI Compartments,OCI Vault.

The Terraform code is used to create a Resource Manager stack, that creates the required resources and configures the application on the created resources.
[![Deploy to Manufacturing Application to Oracle Cloud](https://oci-resourcemanager-plugin.plugins.oci.oraclecloud.com/latest/deploy-to-oracle-cloud.svg)](https://cloud.oracle.com/resourcemanager/stacks/create?zipUrl=https://github.com/oracle-samples/oracle-dataflow-samples/raw/main/scala/manufacturing/src/manufacturing.zip)

**WARNING: Ensure you destroyed the resources if you are not using.**

## Prerequisites for manual steps
Before you begin:

* Ensure your tenant is configured according to the instructions to [setup admin](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin)
* Know your object store namespace.
* Ensure you have Streampool and Streams created in oci stream [oci_stream](https://docs.oracle.com/en-us/iaas/Content/Streaming/home.htm)
* Download Turbofan Engine Degradation Simulation Data Set from Nasa prognostic data repository [nasa_prognostic](https://ti.arc.nasa.gov/tech/dash/groups/pcoe/prognostic-data-repository/)
  [turbofan_dataset](https://ti.arc.nasa.gov/c/6/)
* Create autonomous datawarehouse  [autonomous databases](https://www.oracle.com/autonomous-database/autonomous-data-warehouse/)

## Instructions
1. Setup StreamPool and Stream with OCI Streaming Service [Getting Started with Spark Streaming](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/spark-streaming.htm#streaming-get-started)
2. Upload turbofan engine degradation dataset to object storage.
3. Update STREAMPOOL_CONNECTION, ADB_ID, USERNAME, PASSWORD [scala/manufacturing/src/main/scala/com/oracle/dataflow/utils/Constants.scala]
4. Update libraries in provided scope to compile in build.sbt [scala/manufacturing/build.sbt]
5. Run RealtimeRULPredictor to train the model, trained model will be persisted in output location  provided as argument.
6. Start SensorDataSimulator which will continuously produce data to stream.
7. Start RealtimeRULPredictor which will predict and continuously output data to stream and autonomous database.  

## To Compile
```sh
sbt assembly
```

## To Test Locally

1. Run offline model trainer
```sh
spark-submit --class com.oracle.dataflow.RealtimeRULPredictor target/scala-2.12/manufacturing-assembly-0.1.jar application.conf
```

2. Run SensorDataSimulator
```sh
spark-submit --class com.oracle.dataflow.SensorDataSimulator target/scala-2.12/manufacturing-assembly-0.1.jar application.conf
```

3. Run RealtimeRULPredictor
```sh
spark-submit target/scala-2.12/manufacturing-assembly-0.1.jar <training_data> <consumer_checkpoint_location> <models_location> <stream_name> <trigger_interval>
```

4. Run RULDeltaTable
```sh
spark-submit --class com.oracle.dataflow.RealtimeRULPredictor predicted_rul_delta.py application.conf
```

More info on spark-submit [Submitting Applications](https://spark.apache.org/docs/3.0.2/submitting-applications.html) and what is supported by Data Flow [Spark-Submit Functionality in Data Flow](https://docs.oracle.com/en-us/iaas/data-flow/using/spark-submit.htm)

### To use OCI CLI to run the Scala Application

1. Create model trainer dataflow application
```sh
oci data-flow application create \
    --compartment-id <compartment_ocid> \
    --display-name "RULSurvivalModelTrainer" \
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 3.2.1 \
    --class "com.oracle.dataflow.RULSurvivalModelTrainer" \
    --type BATCH \
    --file-uri "oci://<bucket>@<namespace>/manufacturing-assembly-0.6.jar" \
    --language Scala
    --class-name com.oracle.dataflow.SensorDataSimulator
    --arguments "oci://<bucket>@<namespace>/application.conf"
```

2.Create sensor data simulator dataflow application
```sh
oci data-flow run create \
    --compartment-id <compartment_ocid> \
    --application-id <application_ocid> \
    --display-name "SensorDataSimulator" \
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 3.2.1 \
    --class "com.oracle.dataflow.SensorDataSimulator" \
    --type streaming \
    --file-uri "oci://<bucket>@<namespace>/manufacturing-assembly-0.6.jar" \
    --language Scala
    --class-name com.oracle.dataflow.SensorDataSimulator
    --arguments "oci://<bucket>@<namespace>/application.conf"
```

3.Create predictor dataflow application
```sh
oci data-flow run create \
    --compartment-id <compartment_ocid> \
    --application-id <application_ocid> \
    --display-name "RealtimeRULPredictor" \
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 3.2.1 \
    --class "com.oracle.dataflow.RealtimeRULPredictor" \
    --type streaming \
    --file-uri "oci://<bucket>@<namespace>/manufacturing-assembly-0.6.jar" \
    --language Scala
    --class-name com.oracle.dataflow.SensorDataSimulator
    --arguments "oci://<bucket>@<namespace>/application.conf"
```

4. RUL DeltaTable Operations
```sh
oci data-flow run create \
    --compartment-id <compartment_ocid> \
    --application-id <application_ocid> \
    --display-name "RealtimeRULPredictor" \
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 3.2.1 \
    --type BATCH \
    --file-uri "oci://<bucket>@<namespace>/predicted_rul_delta.py" \
    --language Python
    --arguments "oci://<bucket>@<namespace>/<delta_table>"
```

For more details on OCI CLI configuration options see [OCI CLI Command Reference ](https://docs.oracle.com/en-us/iaas/tools/oci-cli/3.4.4/oci_cli_docs/cmdref/data-flow/application/create.html)