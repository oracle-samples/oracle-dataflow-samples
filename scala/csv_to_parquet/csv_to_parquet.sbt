// Copyright (c) 2016, 2020 Oracle and/or its affiliates.
name := "csv_to_parquet"
version := "1.0"
scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided",
  "com.oracle.oci.sdk" % "oci-java-sdk-common" % "1.22.1",
)
