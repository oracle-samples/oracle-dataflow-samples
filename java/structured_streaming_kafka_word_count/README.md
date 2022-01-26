# Overview
This Structured Streaming application shows how to read Kafka stream and calculate word frequencies over one-minute window intervals.

Word count problem is a classical example used in [Structured Streaming Programming Guide](https://spark.apache.org/docs/3.0.2/structured-streaming-programming-guide.html)

## Prerequisites
### For running Locally
* Refer to section on how to set the Prerequisites before you deploy the application locally [Setup Spark locally](https://docs.oracle.com/en-us/iaas/data-flow/data-flow-tutorial/develop-apps-locally/front.htm). 
### For running on Data Flow 
* Ensure your tenancy is configured according to the Data Flow onboard instructions. [Getting Started with Data Flow](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm#set_up_admin)
* Ensure that Data Flow Spark Streaming configuration is also in place. [Getting Started with Spark Streaming](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/spark-streaming.htm#streaming-get-started)

## Instructions
1. Setup OSS Kafka instance. See [Getting Started with Spark Streaming](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/spark-streaming.htm#streaming-get-started)
2. Prepared /producer/oss-producer-from-file.py for work, download source text and update constants with relevant information.
3. Prepare command line for local and Data Flow based run:
```sh
Usage: StructuredKafkaWordCount <bootstrap-servers> <subscribe-topics> <kafkaAuthentication> <checkpoint-location> <type> ...
<kafkaAuthentication>: plain <username> <password>
<kafkaAuthentication>: RP <stream-pool-id>
<type>: console
<type>: csv <output-location>
```
4. Compile Structured Streaming app (StructuredKafkaWordCount)
5. First start Structured Streaming app (StructuredKafkaWordCount) locally or in the cloud.
6. Second start data producer oss-producer-from-file.py (`python3 oss-producer-from-file.py`) locally or in the cloud.

### To Compile

```sh
mvn package
```

### To Test Locally

```sh
spark-submit --class example.StructuredKafkaWordCount target/StructuredKafkaWordCount.jar <kafka bootstrap server>:9092 <kafka topic> plain <tenancy name>/<user name>/<stream pool id> <user security token> /tmp/checkpoint csv /tmp/output
```
More info on spark-submit [Submitting Applications](https://spark.apache.org/docs/3.0.2/submitting-applications.html) and what is supported by Data Flow [Spark-Submit Functionality in Data Flow](https://docs.oracle.com/en-us/iaas/data-flow/using/spark-submit.htm)  

### To use OCI CLI to run the Java Application

```sh
oci data-flow application create \
    --compartment-id <compartment_ocid> \
    --display-name "StructuredKafkaWordCount" \
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 3.0.2 \
    --type streaming \
    --file-uri "oci://<bucket>@<namespace>/StructuredKafkaWordCount.jar" \
    --language Java
    --class-name example.StructuredKafkaWordCount
    --arguments "<kafka bootstrap server>:9092 <kafka topic> plain <tenancy name>/<user name>/<stream pool id> <user security token> oci://<bucket>@<namespace>/checkpoint csv oci://<bucket>@<namespace>/output"
```
Make note of the Application ID produced.

```sh
oci data-flow run create \
    --compartment-id <compartment_ocid> \
    --application-id <application_ocid> \
    --display-name "CSV to Parquet Java"
```
Arguments can be updated to switch from plain password authentication with Kafka to Data Flow Resource Principal which is more suitable for production scenarios

e.g.
```sh
--arguments "<kafka bootstrap server>:9092 <kafka topic> RP <stream pool id> oci://<bucket>@<namespace>/checkpoint csv oci://<bucket>@<namespace>/output"
```
For more details on OCI CLI configuration options see [OCI CLI Command Reference ](https://docs.oracle.com/en-us/iaas/tools/oci-cli/3.4.4/oci_cli_docs/cmdref/data-flow/application/create.html)