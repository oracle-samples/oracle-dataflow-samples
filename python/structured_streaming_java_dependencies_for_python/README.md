# Overview
This is a Maven project to build an uber (all in one) JAR that contains all the Java dependencies and other tweaks like shading in one place for Python Spark structured streaming applications.

See [Best Practices for Building Python Spark Streaming Applications](https://docs.cloud.oracle.com/en-us/iaas/data-flow/using/spark-streaming.htm#streaming-build-python-app-tips)
# Content
Fully includes
```sh
groupId = com.oracle.oci.sdk
artifactId = oci-java-sdk-addons-sasl
version = 1.36.1
```
```sh
groupId = com.google.protobuf
artifactId = protobuf-java
version = 3.11.1
```
```sh
groupId = org.apache.spark
artifactId = spark-sql-kafka-0-10_2.12
version = 3.0.2
```
Includes as provided
```sh
groupId = org.apache.spark
artifactId = spark-core_2.12
version = 3.0.2
```
```sh
groupId = org.apache.spark
artifactId = spark-sql_2.12
version = 3.0.2
```
Relocates namespaces
* com.google -> com.shaded.google
* com.oracle.bmc -> com.shaded.oracle.bmc (except com.oracle.bmc.auth.sasl.*)

# Instructions
1. Compile an uber jar.
2. Pack uber jar into archive.zip

## To Compile
```sh
mvn package
```

## Building archive.zip
Place SaslFat-1.0-SNAPSHOT.jar in the working directory of the dependedncy packager and execute the command:
```sh
docker run --pull always --rm -v $(pwd):/opt/dataflow -it phx.ocir.io/oracle/dataflow/dependency-packager:latest
```
SaslFat-1.0-SNAPSHOT.jar is packaged into archive.zip as a Java dependency:
```sh
adding: java/ (stored 0%)
adding: java/SaslFat-1.0-SNAPSHOT.jar (deflated 8%)
adding: version.txt (deflated 59%)
archive.zip is generated!
```
Alternatively, you can manually create such an archive.zip that contains the java folder with SaslFat-1.0-SNAPSHOT.jar in it.

More info on Dependency Packager: [Adding Third-Party Libraries to Data Flow Applications](https://docs.oracle.com/en-us/iaas/data-flow/using/third-party-libraries.htm?bundle=7861#third-party-libraries)