package com.oracle.dataflow

import com.oracle.dataflow.utils.Constants.SURVIVAL_MODEL_DIR
import com.oracle.dataflow.utils.Helper.{createTestDataset, evaluate, getAssembler, getScalerModel, preProcess, scaleFeatures, selectFeatures}
import com.oracle.dataflow.utils.SparkSessionUtils.spark
import com.oracle.dataflow.schema.EquipmentTrainingData
import com.oracle.dataflow.utils.ApplicationConfiguration
import com.typesafe.config.Config
import org.apache.spark.ml.regression.{AFTSurvivalRegression, AFTSurvivalRegressionModel}

import java.io.File

/**
 * DESCRIPTION :
 * Predicts Remaining Useful Life (RUL) of critical equipment fleet in production line of a factory floor.
 *
 * MODEL        : Survival regression (Accelerated Failure Time (AFT))
 * TRAINING DATA: Run-To-Error data of fleet of equipment of specific type.
 * REF          : https://spark.apache.org/docs/latest/ml-classification-regression.html#survival-regression
 */

object RULSurvivalModelTrainer {
  def main(args: Array[String]): Unit = {
    println("Starting RULSurvivalModelTrainer")
    if (args.length == 0) {
      println("Missing configuration file argument.Please provide config.")
      sys.exit(1)
    }
    val configFile = args(0)
    val appConf:Config = new ApplicationConfiguration(configFile).applicationConf
    val trainingDataPath = appConf.getString("modelTrainer.trainingDataset")
    val outputModelPath = appConf.getString("modelTrainer.modelOutputPath")
    val testDataPath = appConf.getString("modelTrainer.testSensorDataset")
    val testRulPath = appConf.getString("modelTrainer.testRULDataset")
    val maxIter = appConf.getInt("modelTrainer.maxIter")

    // 1. Prepare Training Data
    val rawTrainingData = spark.read.schema(EquipmentTrainingData.schema)
      .option("delimiter"," ").option("header", false).option("inferSchema",true)
      .csv(trainingDataPath).cache()
    println("Training Data Schema:")
    rawTrainingData.printSchema()
    println("Training Data Sample:")
    rawTrainingData.show(5,false)
    println(s"Training Data Summary:")
    rawTrainingData.describe().show()
    val preProcessedData = preProcess(rawTrainingData)
    val assembler = getAssembler(preProcessedData)
    val featuredData = selectFeatures(preProcessedData, outputModelPath, assembler)
    val scalerModel = getScalerModel(featuredData)
    val trainingData = scaleFeatures(featuredData, outputModelPath, scalerModel)

    // 2. Train Model
    val survivalModel = new AFTSurvivalRegression()
      .setCensorCol("censor")
      .setFeaturesCol("scaled_features")
      .setLabelCol("rul")
      .setMaxIter(maxIter)
    val model:AFTSurvivalRegressionModel = survivalModel.fit(trainingData)

    // 3. Evaluate Model
    evaluate(model,assembler,scalerModel,createTestDataset(testDataPath,testRulPath))

    // 4. Save Model
    println(s"\nPersisting model to " + outputModelPath + File.separator + SURVIVAL_MODEL_DIR)
    model.write.overwrite().save(outputModelPath + File.separator + SURVIVAL_MODEL_DIR)
    println("\nCompleted RULSurvivalModelTrainer.\n")
  }
}
