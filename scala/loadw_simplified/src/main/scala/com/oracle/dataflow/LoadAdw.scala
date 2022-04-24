package com.oracle.dataflow

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LoadAdw {
  def main(args: Array[String]): Unit = {
    val sparkBuilder = SparkSession.builder().appName(this.getClass.getName)
    var spark:SparkSession = null
    if ((new SparkConf).contains("spark.master"))
      spark=sparkBuilder.getOrCreate()
    else
      spark= sparkBuilder
        .master("local[*]")
       // .config("spark.plugins","oracle.dataflow.spark.dsv1.adb.plugin.WalletInstallerPlugin")
        .getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")

    val testData = spark.read.format("oracle")
      .option("adbId","ocid1.autonomousdatabase.oc1.iad.anuwcljsnif7xwia2yqgrs5qtyp6wf37zzw66r2igsiyui7giijhf2dctovq")
      .option("dbtable", "ADMIN.PERSONS")
      .option("user", "ADMIN")
      .option("password", "Password-123")
      .load()

    testData.printSchema()
    testData.show(10,false)
  }
}
