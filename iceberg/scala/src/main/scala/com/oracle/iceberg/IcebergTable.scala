package com.oracle.iceberg

import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object IcebergTable {

  def getSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("IcebergTable Simulation")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.dev", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.hadoop.fs.AbstractFileSystem.oci.impl", "com.oracle.bmc.hdfs.Bmc")
      .config("spark.sql.catalog.dev.type", "hadoop")
      .config("spark.sql.catalog.dev.warehouse", "oci://<bucket>@<namespace>/iceberg/")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

  def runIcebergSample(inputPath: String, outputPath: String) = {

    val spark = getSparkSession()
    import spark.implicits._

    val data = Seq(
      ("100", "2023-01-01", "2023-01-01T13:51:39.340396Z"),
      ("101", "2023-01-01", "2023-01-01T12:14:58.597216Z"),
      ("102", "2023-01-01", "2023-01-01T13:51:40.417052Z"),
      ("103", "2023-01-01", "2023-01-01T13:51:40.519832Z")).toDF("id", "creation_date", "last_update_time")

    spark.sql(
      """CREATE TABLE IF NOT EXISTS dev.db.iceberg_table (id string,
    creation_date string,
    last_update_time string)
    USING iceberg
    location 'oci://<bucket>@<namespace>/iceberg/db/iceberg_table'""")

    data.writeTo("dev.db.iceberg_table").append()

    val df1 = spark.read.format("iceberg").load("dev.db.iceberg_table")
    df1.show()

    spark.sql("SELECT * FROM dev.db.iceberg_table.snapshots").show()

    val t1 = spark.read
      .format("iceberg")
      .option("start-snapshot-id", 3633452472408602672L)
      .option("end-snapshot-id", 4166325474384420204L)
      .load("oci://<bucket>@<namespace>/iceberg/db/iceberg_table")

    t1.show()

    val t2 = spark.read
      .format("iceberg")
      .option("as-of-timestamp", "1676461234685")
      .load("dev.db.iceberg_table")

    t2.show()

    spark.sql("SELECT * FROM dev.db.iceberg_table").show()
    spark.sql("SELECT * FROM dev.db.iceberg_table.files").show()
    //spark.sql("SELECT * FROM dev.db.iceberg VERSION AS OF 142414061441660886L").show() Spark 3.3 and above
    spark.sql("SELECT * FROM dev.db.iceberg_table.history").show() // supported only in spark 3.2

    val table = spark.table("dev.db.iceberg_table")
    println ("Total Rows " + table.count())

    import org.apache.iceberg.hadoop.HadoopTables
    import org.apache.iceberg.expressions.Expressions
    import org.apache.hadoop.conf.Configuration

    val conf = new Configuration()
    val tables = new HadoopTables(conf)
    val table1 = tables.load("oci://<bucket>@<namespace>/iceberg/db/iceberg_table")

    SparkActions
      .get()
      .rewriteDataFiles(table1)
      .filter(Expressions.equal("id", "101"))
      .option("target-file-size-bytes", "536870912") // (512 MB)
      .execute()

  }

}
