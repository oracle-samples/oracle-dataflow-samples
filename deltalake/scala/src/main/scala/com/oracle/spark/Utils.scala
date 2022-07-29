package com.oracle.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp

object Utils {

  def getSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("Spark Simulation")
      .getOrCreate()
    spark
  }


  def csvToParquet(inputPath: String, outputPath: String) = {

    val spark = getSparkSession()

    val original_df = spark
      .read
      .format("csv")
      .option("header", "true")
      .load(inputPath)

      .withColumn("time_stamp", current_timestamp())

    original_df.write.partitionBy("vendor_id").mode("overwrite").parquet(outputPath)

  }
}
