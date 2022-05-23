organization := "com.oracle.dataflow"
name := "manufacturing"
description := "Trains and deploys Remaining Useful Life (RUL) of critical equipment in production line of factory floor."
version := "0.1"
scalaVersion := "2.12.12"

val sparkVersion = "3.0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case "module-info.class" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

//unmanagedJars in Compile += file("lib/spark_datasources-1.0-SNAPSHOT.jar")

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