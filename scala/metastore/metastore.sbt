name := "metastore"
version := "1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
 "org.scala-lang" % "scala-library" % "2.12.15",
 "org.apache.spark" % "spark-core_2.12" % "3.2.1" % "provided",
 "org.apache.spark" % "spark-sql_2.12" % "3.2.1" % "provided",
 "com.oracle.oci.sdk" % "oci-java-sdk-core" % "2.12.0",
 "com.oracle.oci.sdk" % "oci-java-sdk-objectstorage" % "2.12.0",
 "com.oracle.oci.sdk" % "oci-java-sdk-secrets" % "2.12.0", 
)
