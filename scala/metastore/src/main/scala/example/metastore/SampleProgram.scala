package example.metastore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object SampleProgram {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    var OS_BUCKET = "oci://bhavya-bucket@paasdevssstest/"
    var relativeInputPath = "canary-assets/fake_contact_data.csv"
    var relativeOutputPath = "temp"
    var databaseName = "bhavya"
    var tableName = "test_table"

    println("Received args -> " + args.mkString(";"))
    if (args.length > 0) {
      OS_BUCKET = args(0).trim
      relativeInputPath = args(1).trim
      relativeOutputPath = args(2).trim
      if (args.length > 3) {
        databaseName = args(3).trim
        tableName = args(4).trim
      }
    }

    println("OS_BUCKET -> " + OS_BUCKET)
    if (!OS_BUCKET.endsWith("/")) {
      OS_BUCKET = OS_BUCKET + "/"
    }

    // Use Case 1: Read csv from object storage
    val df = spark.read.option("header", "true").csv(OS_BUCKET + relativeInputPath)
    println("Reading data from object storage !")
    df.show(false)
    println("================================================================================================")

    // Use Case 2: Write csv data into Metastore
    spark.sql("show databases").show(30, false)
    val databasesDf = spark.sql("show databases")

    if (databasesDf.filter(col("namespace").contains(lit(databaseName))).count() > 0) {
      println("Database: " + databaseName + " present !")
    }
    else {
      println("Database: " + databaseName + " absent, creating !")
      spark.sql("create database " + databaseName)
      println("Successfully created database: " + databaseName)
      println("List of databases -> ")
      databasesDf.show(false)
    }
    spark.sql("use " + databaseName)
    spark.sql("show tables").show(30, false)
    df.write.mode("overwrite").saveAsTable(databaseName + "." + tableName)
    println("Wrote data in Database: " + databaseName + " ; table: " + tableName)
    println("================================================================================================")

    // Use Case 3: Read data from Metastore into object storage in parquet format
    val tableDf = spark.sql("select * from " + databaseName + "." + tableName)
    println("Reading data from metastore !")
    tableDf.show(false)
    val parquetOutputPath = OS_BUCKET + relativeOutputPath + "/fake_contact_data_parquet"
    tableDf.write.mode("overwrite").parquet(parquetOutputPath)
    println("Wrote data onto object storage !")
    val parquetDf = spark.read.parquet(parquetOutputPath)
    parquetDf.show(false)
    println("================================================================================================")
  }
}
