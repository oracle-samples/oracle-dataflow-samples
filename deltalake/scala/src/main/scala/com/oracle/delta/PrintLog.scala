package com.oracle.delta

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

import org.slf4j.{Logger, LoggerFactory}

object SimpleSparkApp {

  protected lazy val LOG: org.slf4j.Logger = ConsoleLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val inputPath = args(0)
    val outputPath = args(1)

//    val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

    val logger: Logger = LoggerFactory.getLogger(getClass)
    logger.info("Outside Spark Context+++++++++++++++++++++++")
    logger.info("Number of executors = 2 ++++++++++++++++++++")

    logger.warn("Outside Warning+++++++++++++++++++++++++++++")
    logger.error("Outside Error++++++++++++++++++++++++++++++")

    LOG.info("Console: Outside Spark Context+++++++++++++++++++++++")
    LOG.info("Console: Number of executors = 2 ++++++++++++++++++++")

    LOG.warn("Console: Outside Warning+++++++++++++++++++++++++++++")
    LOG.error("Console: Outside Error++++++++++++++++++++++++++++++")


    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("SimpleSparkApp")
      .getOrCreate()

//    val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
    LOG.info("Console: Inside Spark Context+++++++++++++++++++++++")
    LOG.info("Console: Inside Number of executors = 2 ++++++++++++++++++++")

    println("Starting SimpleSparkApp...")
    println(s"Input path: $inputPath")
    println(s"Output path: $outputPath")

    import spark.implicits._

    println("Reading input CSV...")

    val df: DataFrame = spark.read
      .option("header", "true")
      .csv(inputPath)

    println("Showing sample data:")
    df.show(5)

    println("Compiling Vendors data...")

    val vendorsDF = df.groupBy("vendor_id").sum()
    println("Vendors Summary:")
    vendorsDF.show(10)

    println("Writing output to disk...")

    vendorsDF.write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)

    println("Completed successfully.")

    spark.stop()

  }
}
