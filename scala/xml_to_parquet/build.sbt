import sun.security.tools.PathList

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.10",
      version      := "0.6.0-SNAPSHOT"
    )),
    name := "IRS990",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.2",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.2",
    libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.1.0"
  )

