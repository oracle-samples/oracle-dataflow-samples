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
usage: streaming_aggregate.py [-h] [--auth-type AUTH_TYPE]
                              [--bootstrap-port BOOTSTRAP_PORT]
                              [--bootstrap-server BOOTSTRAP_SERVER]
                              [--checkpoint-location CHECKPOINT_LOCATION]
                              [--encryption ENCRYPTION] [--ocid OCID]
                              [--output-location OUTPUT_LOCATION]
                              [--output-mode OUTPUT_MODE]
                              [--stream-password STREAM_PASSWORD]
                              [--raw-stream RAW_STREAM]
                              [--stream-username STREAM_USERNAME]

optional arguments:
  -h, --help            show this help message and exit
  --auth-type AUTH_TYPE
  --bootstrap-port BOOTSTRAP_PORT
  --bootstrap-server BOOTSTRAP_SERVER
  --checkpoint-location CHECKPOINT_LOCATION
  --encryption ENCRYPTION
  --ocid OCID
  --output-location OUTPUT_LOCATION
  --output-mode OUTPUT_MODE
  --stream-password STREAM_PASSWORD
  --raw-stream RAW_STREAM
  --stream-username STREAM_USERNAME
```
4. Provide your dependency using any of the below suitable option.

    * Use `--packages` option or `spark.jars.packages` spark configuration. Application running in private endpoint has to allow traffic from private subnet to internet to download package (confirm with PM). 
    * Provide object storage jar location in `--jars` or `spark.jars` as comma seperated list.  
    * Use `python/structured_streaming_java_dependencies_for_python` create `archive.zip`.
5. First start Structured Streaming app (StructuredKafkaWordCount) locally or in the cloud.
6. Second start data producer oss-producer-from-file.py (`python3 oss-producer-from-file.py`) locally or in the cloud.

### To Test Locally

```sh
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2 ./StructuredKafkaWordCount.py --raw-stream <kafka topic> --bootstrap-server <kafka bootstrap server> --checkpoint-location /tmp/checkpoint --output-location /tmp/output --stream-username <tenancy name>/<user name>/<stream pool id> --stream-password <user security token> --output-mode console
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
    --file-uri "oci://<bucket>@<namespace>/StructuredKafkaWordCount.py" \
    --archive-uri "oci://<bucket>@<namespace>/archive.zip" \
    --language Python
    --class-name example.StructuredKafkaWordCount
    --arguments "--raw-stream <kafka topic> --bootstrap-server <kafka bootstrap server> --checkpoint-location oci://<bucket>@<namespace>/checkpoint --output-location oci://<bucket>@<namespace>/output --stream-username <tenancy name>/<user name>/<stream pool id> --stream-password <user security token> --output-mode csv"
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
--arguments "--raw-stream <kafka topic> --bootstrap-server <kafka bootstrap server> --checkpoint-location oci://<bucket>@<namespace>/checkpoint --output-location oci://<bucket>@<namespace>/output --ocid <stream pool id> --output-mode csv"
```
For more details on OCI CLI configuration options see [OCI CLI Command Reference ](https://docs.oracle.com/en-us/iaas/tools/oci-cli/3.4.4/oci_cli_docs/cmdref/data-flow/application/create.html)