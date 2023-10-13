organization := "com.oracle.dataflow"
name := "manufacturing"
description := "Trains and deploys Remaining Useful Life (RUL) of critical equipment in production line of factory floor."
version := "0.7"
scalaVersion := "2.12.15"

val sparkVersion = "3.2.1"
val ociSDKVersion = "2.20.0"
val typesafeVersion = "1.4.2"
val protobufVersion = "3.16.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "com.oracle.oci.sdk" % "oci-java-sdk-addons-sasl" % ociSDKVersion,
  "com.typesafe" % "config" % typesafeVersion,
  "com.oracle.oci.sdk" % "oci-java-sdk-objectstorage" % ociSDKVersion,
  "com.oracle.oci.sdk" % "oci-java-sdk-secrets" % ociSDKVersion,
  "com.google.protobuf" % "protobuf-java" % protobufVersion,
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case "module-info.class" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.zap("org.bouncycastle").inAll,
  ShadeRule.rename("com.google.protobuf.**" -> "shade.protobuf.@1").inAll,
)
