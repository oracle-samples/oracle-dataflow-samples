package com.oracle.dataflow

import com.oracle.dataflow.utils.Constants._
import com.oracle.dataflow.utils.SparkSessionUtils.spark
import com.oracle.dataflow.schema.EquipmentTrainingData
import com.oracle.dataflow.utils.Helper
import org.apache.spark.ml.feature.{MinMaxScalerModel, VectorAssembler}
import org.apache.spark.ml.regression.AFTSurvivalRegressionModel
import org.apache.spark.sql.functions.{col, current_timestamp, from_json, lit, struct, to_json, unix_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.File

object RealtimeRULPredictor {
  def main(args: Array[String]): Unit = {
    val checkpointLocation = args(0)
    val inputModelPath = args(1)
    val topics = args(2)
    val outputTopics = args(3)
    val adbId = args(4)
    val adbUserName = args(5)
    val adbPassword = args(6)
    val triggerInSeconds = args(7).toLong

    // Step 1: Read sensor data from stream
    println("Starting RealtimeRULPredictor")
    val testDataStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
      .option("subscribe", topics)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.jaas.config", STREAMPOOL_CONNECTION)
      .option("kafka.max.partition.fetch.bytes", 1024 * 1024)
      .option("startingOffsets", "latest")
      .load()

    // Step 2: Encode data to schema
    val dataSchema = EquipmentTrainingData.schema.add("eventTime", TimestampType)
    val encodedTestDataStream = testDataStream.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .select(from_json(col("value"), dataSchema).as("data"),
        col("timestamp").as("kafka_timestamp"))
      .select("data.*", "kafka_timestamp")
    println("Encoded Data")
    encodedTestDataStream.printSchema()

    // Step 3: Load Models
    println("Load models from " + inputModelPath)
    val vectorAssemblerModel = VectorAssembler.load(inputModelPath + File.separator + "features_model")
    val scalerModel = MinMaxScalerModel.load(inputModelPath + File.separator + "scaler_model")
    val survivalModel = AFTSurvivalRegressionModel.load(inputModelPath + File.separator + "survival_model")

    encodedTestDataStream.writeStream.foreachBatch((testData: DataFrame, batchId: Long) => {
      // Step 4: Predict RUL from trained model and filter out noises for demo
      val featuredData = vectorAssemblerModel.transform(testData)
      val scaledData = scalerModel.transform(featuredData)
      val predictedRULData = survivalModel.transform(scaledData)
        .withColumnRenamed("prediction","predicted_RUL")
      println(s"predicted RUL schema:")
      predictedRULData.printSchema()
      println(s"predicted RUL sample Data:")
      predictedRULData.show(5,false)

      // Step 5: Categorize records based on RUL
      val redRUL = predictedRULData
        .filter(col("predicted_RUL").between(10, 30))
        .withColumn("level", lit("RED"))
      println(s"Records with RUL less than 30 days:")
      redRUL.show(5,false)

      val yellowRUL = predictedRULData
        .filter(col("predicted_RUL").between(30, 60))
        .withColumn("level", lit("YELLOW"))
      println(s"Records with RUL between 30 to 60 days:")
      yellowRUL.show(5,false)

      val greenRUL = predictedRULData
        .filter(col("predicted_RUL") > 60)
        .withColumn("level", lit("GREEN"))
      println(s"Records with RUL greater than 60 days:")
      greenRUL.show(5,false)

      val maintenanceAlert = redRUL.unionAll(yellowRUL)
        .select(current_timestamp().as("time_created"),
        col("asset_id"),
        col("predicted_RUL"),
        col("level"))

      // Step 6: Write predicted RUL to OCI Streaming Service
      println(s"\nsending data to oci stream")
      val outputStreamData = greenRUL.withColumn("eventTime", unix_timestamp())
        .select(lit(batchId).cast(StringType).as("key"),to_json(struct("*")).as("value"))
      Helper.ociWriterPlain(outputStreamData,BOOTSTRAP_SERVER,outputTopics,STREAMPOOL_CONNECTION)

      // Step 7: Using Spark Oracle Datasource send RUL alters to Autonomous Database

      maintenanceAlert
        .write.format("oracle").mode(SaveMode.Append)
        .option("adbId", adbId)
        .option("dbtable", PREDICTED_RUL_ALERTS)
        .option("user", adbUserName)
        .option("password", adbPassword)
        .save()
    }).option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInSeconds))
      .start()

    spark.streams.awaitAnyTermination
  }
}
