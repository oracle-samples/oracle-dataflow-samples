package com.oracle.dataflow.schema

import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType, TimestampType}

object EquipmentTrainingData {
  val schema = StructType(
    Array(
      StructField("asset_id", IntegerType),
      StructField("current_age", IntegerType),
      StructField("setting1", DoubleType),
      StructField("setting2", DoubleType),
      StructField("setting3", DoubleType),
      StructField("sensor1", DoubleType),
      StructField("sensor2", DoubleType),
      StructField("sensor3", DoubleType),
      StructField("sensor4", DoubleType),
      StructField("sensor5", DoubleType),
      StructField("sensor6", DoubleType),
      StructField("sensor7", DoubleType),
      StructField("sensor8", DoubleType),
      StructField("sensor9", DoubleType),
      StructField("sensor10", DoubleType),
      StructField("sensor12", DoubleType),
      StructField("sensor13", DoubleType),
      StructField("sensor14", DoubleType),
      StructField("sensor15", DoubleType),
      StructField("sensor16", DoubleType),
      StructField("sensor17", DoubleType),
      StructField("sensor18", DoubleType),
      StructField("sensor19", DoubleType),
      StructField("sensor20", DoubleType),
      StructField("sensor21", DoubleType)
    )
  )
}
