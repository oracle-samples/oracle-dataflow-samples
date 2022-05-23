package com.oracle.dataflow.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionUtils {

  val spark = getSparkSession()

  def getSparkSession(): SparkSession = {
    val sparkBuilder = SparkSession.builder().appName(this.getClass.getName)
    if ((new SparkConf).contains("spark.master"))
      sparkBuilder.getOrCreate()
    else
      sparkBuilder.master("local[*]").getOrCreate()
  }

  def await = spark.streams.awaitAnyTermination

  def turnOffNoise = spark.sparkContext.setLogLevel("OFF")
}
