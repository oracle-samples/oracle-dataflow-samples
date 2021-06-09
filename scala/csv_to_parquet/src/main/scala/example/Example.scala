/**
  * Copyright (c) 2016, 2020, Oracle and/or its affiliates. All rights reserved.
  */

package example

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

object Example {
  def main(args: Array[String]) = {

    // Customize these.
    val inputPath = "oci://<bucket>@<tenancy>/fake_data.csv"
    val outputPath = "oci://<bucket>@<tenancy>/fake_data.parquet"

    // Transform CSV to Parquet.
    var spark = DataFlowSparkSession.getSparkSession("Sample App")
    val df = spark.read.option("header", "true").csv(inputPath)
    df.write.mode(SaveMode.Overwrite).format("parquet").save(outputPath)
    println(s"Conversion to Parquet complete.")
  }
}
