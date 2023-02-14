package com.oracle.dataflow

import com.oracle.dataflow.utils.Constants._
import com.oracle.dataflow.utils.SparkSessionUtils.spark
import com.oracle.dataflow.schema.EquipmentTrainingData
import com.oracle.dataflow.utils.CommonUtils.{printUsage, validateArgs}
import com.oracle.dataflow.utils.VaultUtils.getSecret
import com.oracle.dataflow.utils.{ApplicationConfiguration, Helper}
import com.typesafe.config.Config
import org.apache.spark.ml.feature.{MinMaxScalerModel, VectorAssembler}
import org.apache.spark.ml.regression.AFTSurvivalRegressionModel
import org.apache.spark.sql.functions.{col, current_timestamp, from_json, lit, struct, to_json, unix_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.log4j.LogManager
import org.apache.log4j.Logger

import java.io.File

object RealtimeRULPredictor {
  val log: Logger = LogManager.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    validateArgs(args)
    val configFile = args(0)
    val appConf:Config = new ApplicationConfiguration(configFile).applicationConf
    val checkpointLocation = appConf.getString("predictor.checkpointLocation")
    val inputTopics = appConf.getString("predictor.inputTopics")
    val outputTopics = appConf.getString("predictor.outputTopics")
    val inputModelPath = appConf.getString("predictor.inputModelPath")
    val streampoolId = appConf.getString("predictor.streampoolId")
    val bootstrapServer = appConf.getString("predictor.bootstrapServer")
    val triggerIntervalInSeconds = appConf.getInt("predictor.triggerIntervalInSeconds")
    val adbId = appConf.getString("predictor.adbId")
    val adbUserName = appConf.getString("predictor.adbUserName")
    // val secretOcid = appConf.getString("predictor.secretOcid")
    val secret = appConf.getString("predictor.secret")
    val enableOutputStream = appConf.getBoolean("predictor.enableOutputStream")
    val enableOutputADW = appConf.getBoolean("predictor.enableOutputADW")
    val enableOutputParquetTable = appConf.getBoolean("predictor.enableOutputParquetTable")
    val enableOutputDeltaTable = appConf.getBoolean("predictor.enableOutputDeltaTable")
    val parquetOutputPath = appConf.getString("predictor.parquetOutputPath")
    val deltaOutputPath = appConf.getString("predictor.deltaOutputPath")
    val assetFilter = appConf.getInt("predictor.assetFilter")

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

      // Step 7: Write parquet to object storage Service
      if(enableOutputParquetTable) {
        println(s"\nwriting data to object storage")
        predictedRULData.filter(col("asset_id").leq(assetFilter)).write.mode(SaveMode.Append)
          .partitionBy("asset_id").parquet(parquetOutputPath)
      }

      // Step 8:
      if(enableOutputDeltaTable) {
        println(s"\nwriting data to object storage")
        predictedRULData.filter(col("asset_id").leq(assetFilter)).write.mode(SaveMode.Append)
          .partitionBy("asset_id").format("delta").save(deltaOutputPath)
      }

      // Step 9: Using Spark Oracle Datasource send RUL alters to Autonomous Database
      if(enableOutputADW) {
        val adbPassword  = secret
        println(s"password ${adbPassword}")
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
