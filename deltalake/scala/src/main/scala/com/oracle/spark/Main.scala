package com.oracle.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp

object Main {

  def main(args: Array[String]): Unit = {

    print("Starting delta lake sample run")

    if (args.length == 0) {
      println("I need at least input and output path")
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession
      .builder()
      .appName("Spark Simulation")
      .getOrCreate()

    val original_df = spark
      .read
      .format("csv")
      .option("header", "true")
      .load(inputPath)

    original_df
      .withColumn("time_stamp", current_timestamp())
      .write.partitionBy("vendor_id")
      .mode("overwrite")
      .parquet(outputPath)

  }
}
