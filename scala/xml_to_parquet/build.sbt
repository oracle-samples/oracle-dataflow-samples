lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.10",
      version      := "0.6.0-SNAPSHOT"
    )),
    name := "IRS990",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4",
    libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.1.0"
  )
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
