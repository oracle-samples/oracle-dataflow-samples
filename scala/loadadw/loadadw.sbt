// Copyright (c) 2016, 2020 Oracle and/or its affiliates.
name := "loadadw"
version := "1.0"
scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided",
  "com.oracle.oci.sdk" % "oci-hdfs-connector" % "2.9.2.1" % "provided",
  "com.oracle.oci.sdk" % "oci-java-sdk-core" % "1.15.4" % "provided",
  "com.oracle.oci.sdk" % "oci-java-sdk-objectstorage" % "1.15.4" % "provided",
  "com.oracle.oci.sdk" % "oci-java-sdk-secrets" % "1.15.4",
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case "module-info.class" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.apache.http.**" -> "shaded.oracle.org.apache.http.@1").inAll,
  ShadeRule.rename("org.apache.commons.**" -> "shaded.oracle.org.apache.commons.@1").inAll,
  ShadeRule.rename("com.fasterxml.**" -> "shaded.oracle.com.fasterxml.@1").inAll,
  ShadeRule.rename("com.google.**" -> "shaded.oracle.com.google.@1").inAll,
  ShadeRule.rename("javax.ws.rs.**" -> "shaded.oracle.javax.ws.rs.@1").inAll,
  ShadeRule.rename("org.glassfish.**" -> "shaded.oracle.org.glassfish.@1").inAll,
  ShadeRule.rename("org.jvnet.**" -> "shaded.oracle.org.jvnet.@1").inAll,
  ShadeRule.rename("javax.annotation.**" -> "shaded.oracle.javax.annotation.@1").inAll,
  ShadeRule.rename("javax.validation.**" -> "shaded.oracle.javax.validation.@1").inAll,
  ShadeRule.rename("com.oracle.bmc.hdfs.**" -> "com.oracle.bmc.hdfs.@1").inAll,
  ShadeRule.rename("com.oracle.bmc.**" -> "shaded.com.oracle.bmc.@1").inAll,
  ShadeRule.zap("org.bouncycastle").inAll,
)

unmanagedSources / excludeFilter := HiddenFileFilter || "OboTokenClientConfiguratorV2.java"
