package example.metastore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object MetastoreToADW {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    var OS_BUCKET = "oci://bhavya-bucket@paasdevssstest/"
    var relativeInputPath = "canary-assets/fake_contact_data.csv"
    // TODO: Remove this
    var relativeOutputPath = "temp"
    var databaseName = "bhavya"
    var tableName = "test_table"
    var adwDetailsObj: ADWDetails = null

    println("Received args -> " + args.mkString(";"))
    if (args.length > 0) {
      OS_BUCKET = args(0).trim
      relativeInputPath = args(1).trim
      relativeOutputPath = args(2).trim
      if (args.length > 3) {
        databaseName = args(3).trim
        tableName = args(4).trim
      }
      if (args.length > 5) {
        adwDetailsObj = ADWDetails(walletPath = args(5).trim,
          user = args(6).trim,
          tnsName = args(7).trim,
          secretValue = args(8).trim)
        println("ADW details received: " + adwDetailsObj.toString)
      }
    }

    println("OS_BUCKET -> " + OS_BUCKET)
    if (!OS_BUCKET.endsWith("/")) {
      OS_BUCKET = OS_BUCKET + "/"
    }

    // Use Case 1: Read csv from object storage
    println("Step 1: Read csv from object storage")
    val df = spark.read.option("header", "true").csv(OS_BUCKET + relativeInputPath)
    println("Reading data from object storage !")
    df.show(false)
    println("================================================================================================")

    // Use Case 2: Write csv data into Metastore
    println("Step 2: Write csv data into Metastore")
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

    // Use Case 3: Read data from Metastore and write into ADW
    println("Step 3: Read data from Metastore and write into ADW")
    val tableDf = spark.sql("select * from " + databaseName + "." + tableName)
    println("Reading data from metastore !")
    tableDf.show(false)

    if (!spark.conf.getAll.contains("spark.oracle.datasource.enabled") ||
      spark.conf.get("spark.oracle.datasource.enabled").equalsIgnoreCase("false")) {
      return
    }
    println("Writing data into ADW")
    tableDf.write.format("oracle").options(getAdwOptionsMap(adwDetailsObj)).mode("Overwrite").save
    println("Reading data from ADW -> ")
    val adwDf = spark.read.format("oracle").options(getAdwOptionsMap(adwDetailsObj)).load()
    adwDf.show(false)
  }

  def getAdwOptionsMap(adwDetailsObj: ADWDetails): Map[String, String] = {
    Seq(("walletUri", adwDetailsObj.walletPath),
      ("connectionId", adwDetailsObj.tnsName),
      ("user", adwDetailsObj.user),
      ("password", adwDetailsObj.secretValue),
      ("dbtable", "sample")).toMap
  }

  case class ADWDetails(walletPath: String, tnsName: String, user: String, secretValue: String) {
    override def toString: String = {
      "walletPath: %s, tnsName: %s, user: %s, secretValue: %s".format(walletPath, tnsName, user, secretValue)
    }
  }
}
