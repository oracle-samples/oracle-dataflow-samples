package com.oracle.dataflow

import com.oracle.dataflow.utils.Constants._
import com.oracle.dataflow.utils.SparkSessionUtils.spark
import com.oracle.dataflow.schema.EquipmentTrainingData
import com.oracle.dataflow.utils.{ApplicationConfiguration, Helper}
import com.typesafe.config.Config
import org.apache.spark.ml.feature.{MinMaxScalerModel, VectorAssembler}
import org.apache.spark.ml.regression.AFTSurvivalRegressionModel
import org.apache.spark.sql.functions.{col, current_timestamp, from_json, lit, struct, to_json, unix_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.File

object RealtimeRULPredictor {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("Missing configuration file argument.Please provide config.")
      sys.exit(1)
    }
    val configFile = args(0)
    val appConf:Config = new ApplicationConfiguration(configFile).applicationConf
    val checkpointLocation = appConf.getString("predictor.checkpointLocation")
    val inputTopics = appConf.getString("predictor.inputTopics")
    val outputTopics = appConf.getString("predictor.outputTopics")
    val inputModelPath = appConf.getString("predictor.inputModelPath")
    val streampoolId = appConf.getString("predictor.streampoolId")
    val bootstrapServer = appConf.getString("predictor.bootstrapServer")
    val triggerIntervalInSeconds = appConf.getInt("predictor.triggerIntervalInSeconds")
    val adbId = appConf.getInt("predictor.adbId")
    val adbUserName = appConf.getInt("predictor.adbUserName")
    val adbPassword = appConf.getInt("predictor.adbPassword")
    val enableOutputStream = appConf.getBoolean("predictor.enableOutputStream")
    val enableOutputADW = appConf.getBoolean("predictor.enableOutputADW")

    // Step 1: Read sensor data from stream
    println("Starting RealtimeRULPredictor")
    val testDataStream = Helper.ociStreamReader(bootstrapServer,inputTopics,STREAMPOOL_CONNECTION.format(streampoolId))

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
      if(enableOutputStream) {
        println(s"\nsending data to oci stream")
        val outputStreamData = greenRUL.withColumn("eventTime", unix_timestamp())
          .select(lit(batchId).cast(StringType).as("key"), to_json(struct("*")).as("value"))
        Helper.ociStreamWriter(outputStreamData, bootstrapServer, outputTopics, STREAMPOOL_CONNECTION.format(streampoolId))
      }

      // Step 7: Using Spark Oracle Datasource send RUL alters to Autonomous Database
      if(enableOutputADW) {
        maintenanceAlert
          .write.format("oracle").mode(SaveMode.Append)
          .option("adbId", adbId)
          .option("dbtable", PREDICTED_RUL_ALERTS)
          .option("user", adbUserName)
          .option("password", adbPassword)
          .save()
      }
    }).option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerIntervalInSeconds))
      .start()

    spark.streams.awaitAnyTermination
  }
}
