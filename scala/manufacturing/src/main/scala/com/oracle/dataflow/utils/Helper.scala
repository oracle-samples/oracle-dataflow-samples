package com.oracle.dataflow.utils

import com.oracle.dataflow.schema.EquipmentTestData
import com.oracle.dataflow.utils.SparkSessionUtils.spark
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, MinMaxScalerModel, VectorAssembler}
import org.apache.spark.ml.regression.AFTSurvivalRegressionModel
import org.apache.spark.sql.functions.{col, lit, monotonicallyIncreasingId}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Dataset, Row}

import java.io.File

object Helper {

  /**
   * Label and clean up
   */
  def preProcess(inputDataset: Dataset[Row]): Dataset[Row] = {
    val ageData = inputDataset.groupBy(col("asset_id")).count()
      .select(col("asset_id"), col("count").alias("max_age"))
    val labeledData = inputDataset.join(ageData, "asset_id")
      .withColumn("rul", (col("max_age") - col("current_age") + 1).cast(IntegerType))
      .withColumn("censor", lit(1.0).cast(DoubleType))
    println(s"Labeled Data Schema:")
    labeledData.printSchema()
    println(s"Labeled Data Sample:")
    labeledData.show(5, false)
    labeledData
  }

  /**
   * Select Features : sensor readings + operational settings
   */
  def selectFeatures(dataset: Dataset[Row], ouputModelPath: String, assembler: VectorAssembler): Dataset[Row] = {
    val featureData = assembler.transform(dataset)
    assembler.write.overwrite().save(ouputModelPath + File.separator + "features_model")
    featureData
  }

  def getAssembler(dataset: Dataset[Row]): VectorAssembler = {
    val featureCols = dataset.columns.filter(cols => {
      cols.startsWith("sensor") || cols.startsWith("setting") || cols.startsWith("current_age")
    })
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
      .setHandleInvalid("skip")
    assembler
  }

  /**
   * Scale Features : Give sensor data is of different scale/units, Scale to same range.
   */
  def scaleFeatures(dataset: Dataset[Row], ouputModelPath: String, scalerModel: MinMaxScalerModel): Dataset[Row] = {
    scalerModel.write.overwrite().save(ouputModelPath + File.separator + "scaler_model")
    val scaledData = scalerModel.transform(dataset).cache()
    scaledData
  }

  def getScalerModel(dataset: Dataset[Row]): MinMaxScalerModel = {
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
    val scalerModel = scaler.fit(dataset)
    scalerModel
  }

  /**
   * Create Test Data
   */
  def createTestDataset(testDataPath: String, testRulPath: String): Dataset[Row] = {
    val rawTestDataset = spark.read.schema(EquipmentTestData.schema).option("delimiter", " ").option("header", false).option("inferSchema", true).csv(testDataPath).cache()
    val rulDataset = spark.read.option("delimiter", " ").option("header", false).option("inferSchema", true).csv(testRulPath).drop(col("_c1")).withColumn("asset_id", monotonicallyIncreasingId() + 1).withColumnRenamed("_c0", "initial_age")
    val testAgeData = rawTestDataset.groupBy(col("asset_id")).count().withColumnRenamed("count", "age").join(rulDataset, "asset_id").select(col("asset_id"), (col("age") + col("initial_age")).alias("max_age"))
    val testData = rawTestDataset.join(testAgeData, "asset_id").withColumn("current_age", (col("max_age") - col("rul")).cast(IntegerType))
    testData
  }


  /**
   * Evaluate Model
   */
  def evaluate(model: AFTSurvivalRegressionModel,
               assembler: VectorAssembler,
               scalerModel: MinMaxScalerModel,
               testData: Dataset[Row]): Unit = {
    println(s"Trained Model Evaluation:")

    val featuredTestData = assembler.transform(testData)
    val scaledTestData = scalerModel.transform(featuredTestData)
    val predictedRULData = model.transform(scaledTestData)
    predictedRULData.select(col("asset_id"), col("rul"), col("prediction").cast(IntegerType))

    val evaluator = new RegressionEvaluator().setLabelCol("rul").setPredictionCol("prediction").setMetricName("rmse")
    val rmse = evaluator.evaluate(predictedRULData)
    println(s"Root Mean Squared Error (RMSE) of RUL: $rmse")
  }


  /**
   * Write to OCI Streaming Service
   *
   * @param ds
   * @param bootStrapServer
   * @param topics
   * @param connectionString
   */
  def ociWriterPlain(ds: Dataset[Row], bootStrapServer: String,
                     topics: String, connectionString: String): Unit = {
    ds.write.format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("topic", topics)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "OCI-RSA-SHA256")
     // .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.jaas.config", connectionString)
      .save()
  }

  /**
   * Read from OCI Streaming Service
   *
   * @param bootStrapServer
   * @param topics
   * @param connectionString
   * @return
   */
  def ociStreamingReaderPlain(bootStrapServer: String,
                              topics: String, connectionString: String): Dataset[Row] = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("subscribe", topics)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "OCI-RSA-SHA256")
      //.option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.jaas.config", connectionString)
      .option("kafka.max.partition.fetch.bytes", 1024 * 1024) // limit request size to 1MB per partition
      .option("startingOffsets", "latest")
      .load()
  }
}
