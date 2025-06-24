organization := "com.oracle.dataflow"
name := "delta-logging"
description := "delta-logging samples"
version := "0.1"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"
val ociSDKVersion = "3.34.1"
val ioDelta = "3.1.0"
val typesafeVersion = "1.4.2"
val protobufVersion = "3.16.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "com.oracle.oci.sdk" % "oci-java-sdk-addons-sasl" % ociSDKVersion,
  "com.oracle.oci.sdk" % "oci-java-sdk-objectstorage" % ociSDKVersion,
  "com.oracle.oci.sdk" % "oci-java-sdk-secrets" % ociSDKVersion,
  "com.oracle.oci.sdk" % "oci-java-sdk-monitoring" % ociSDKVersion,
  "com.google.protobuf" % "protobuf-java" % protobufVersion,
  "io.delta" %% "delta-core" % "2.4.0",

  // SLF4J API
  "org.slf4j" % "slf4j-api" % "2.0.13",

  // Scala Logging facade
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",

  // Log4j2 Binding for SLF4J
  "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0"
)

