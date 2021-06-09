Before you begin:
Customize src/main/scala/example/Example.java

To compile:
sbt assembly

Local testing:
If you need to test locally, special setup is required due to shading issues.

Your Spark environment needs to have these JARs added:
bc-fips.jar bcpkix-fips.jar hadoop-annotations-2.9.2.jar hadoop-auth-2.9.2.jar
hadoop-client-2.9.2.jar hadoop-common-2.9.2.jar hadoop-hdfs-client-2.9.2.jar
hadoop-mapreduce-client-app-2.9.2.jar hadoop-mapreduce-client-common-2.9.2.jar
hadoop-mapreduce-client-core-2.9.2.jar hadoop-mapreduce-client-jobclient-2.9.2.jar
hadoop-mapreduce-client-shuffle-2.9.2.jar hadoop-yarn-api-2.9.2.jar
hadoop-yarn-client-2.9.2.jar hadoop-yarn-common-2.9.2.jar hadoop-yarn-registry-2.9.2.jar
hadoop-yarn-server-common-2.9.2.jar hadoop-yarn-server-web-proxy-2.9.2.jar
sss_hdfs-1.0-SNAPSHOT.jar

While most of these JARs are available publicly, you will need to work out some way to
get sss_hdfs-1.0-SNAPSHOT.jar.

Once this is set up:
spark-submit --class example.Example target/scala-2.11/loadadw-assembly-1.0.jar

Deploy to Data Flow:
oci os object put --bucket-name <mybucket> --file target/scala-2.11/loadadw-assembly-1.0.jar
oci data-flow application create \
    --compartment-id <your_compartment> \
    --display-name "Load ADW Scala"
    --driver-shape VM.Standard2.1 \
    --executor-shape VM.Standard2.1 \
    --num-executors 1 \
    --spark-version 2.4.4 \
    --file-uri oci://<bucket>@<namespace>/loadadw-assembly-1.0.jar \
    --language Scala \
    --class-name example.Example
Make note of the Application OCID produced by this command.

Run in Data Flow:
oci data-flow run create \
    --compartment-id <your_compartment> \
    --application-id <application_ocid> \
    --display-name "Load ADW Scala"
