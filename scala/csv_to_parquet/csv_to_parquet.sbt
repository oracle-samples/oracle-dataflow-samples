// Copyright (c) 2016, 2020 Oracle and/or its affiliates.
name := "csv_to_parquet"
version := "1.0"
scalaVersion := "2.12.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.0.2" % "provided",
  "com.oracle.oci.sdk" % "oci-java-sdk-common" % "1.25.2",
)
