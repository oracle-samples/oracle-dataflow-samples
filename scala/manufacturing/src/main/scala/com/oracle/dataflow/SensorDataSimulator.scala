package com.oracle.dataflow

import com.oracle.dataflow.utils.Constants.{BOOTSTRAP_SERVER, GREEN_BATCH_SIZE, MAX_AGE, NUMBER_OF_ASSETS, RED_BATCH_SIZE, STREAMPOOL_CONNECTION, YELLOW_BATCH_SIZE}
import com.oracle.dataflow.utils.Helper
import com.oracle.dataflow.utils.SparkSessionUtils.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, struct, to_json, unix_timestamp}
import org.apache.spark.sql.streaming.{GroupState, Trigger}
import org.apache.spark.sql.types.StringType

import java.util.Random
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * DESCRIPTION:
 * Sensor data simulator.
 */
object SensorDataSimulator {

  val random = new Random

  val RUL_30_60 = mutable.LinkedHashMap(
    "setting1"->(-0.0069,0.0070),
    "setting2"->(-0.014,-0.015),
    "setting3"->(100.0,100.0),
    "sensor1"->(518.0,518.0),
    "sensor2" ->(642.0,643.0),
    "sensor3"->(1580.0,1581.0),
    "sensor4"->(1395.0,1396.0),
    "sensor5"->(14.62,14.62),
    "sensor6"->(21.6,21.61),
    "sensor7"->(551.0,552.0),
    "sensor8"->(2387.89,2388.4),
    "sensor9"->(9114.0,9115.0),
    "sensor10"->(1.3,1.3),
    "sensor12"->(47.0,48.0),
    "sensor13"->(521.0,522.0),
    "sensor14"->(2387.89,2388.33),
    "sensor15"->(8209.0,8210.0),
    "sensor16"->(8.6,8.7),
    "sensor17"->(0.03,0.03),
    "sensor18"->(389.0,390.0),
    "sensor19"->(2388.0,2388.0),
    "sensor20"->(100.0,100.0),
    "sensor21"->(38.31,39.42))

  val RUL_10_30 = mutable.LinkedHashMap(
    "setting1"->(0.0057,0.0063),
    "setting2"->(-0.13,-0.11),
    "setting3"->(100.0,100.0),
    "sensor1"->(518.67,518.67),
    "sensor2" ->(642.23,642.91),
    "sensor3"->(1571.13,11581.98),
    "sensor4"->(1401.33,1409.57),
    "sensor5"->(14.62,14.62),
    "sensor6"->(21.6,21.61),
    "sensor7"->(551.26,555.34),
    "sensor8"->(2388.04,2388.08),
    "sensor9"->(9034.54,9039.45),
    "sensor10"->(1.3,1.3),
    "sensor12"->(46.80,47.33),
    "sensor13"->(520.38,521.98),
    "sensor14"->(2388.08,2388.27),
    "sensor15"->(8162.34,8170.86),
    "sensor16"->(8.39,8.62),
    "sensor17"->(0.03,0.03),
    "sensor18"->(391.49,392.04),
    "sensor19"->(2388.0,2388.0),
    "sensor20"->(100.0,100.0),
    "sensor21"->(39.30,39.42))

  val RUL_Greater_60 = mutable.LinkedHashMap(
    "setting1"->(-0.008,0.0072),
    "setting2"->(0.215,6.77),
    "setting3"->(100.0,100.0),
    "sensor1"->(641.186,644.384),
    "sensor2" ->(642.23,642.91),
    "sensor3"->(1569.11,1607.35),
    "sensor4"->(1385.34,1433.35),
    "sensor5"->(14.62,14.62),
    "sensor6"->(21.6,21.61),
    "sensor7"->(551.02,555.49),
    "sensor8"->(2387.89,2388.38),
    "sensor9"->(9025.23,9143.83),
    "sensor10"->(1.3,1.3),
    "sensor12"->(46.81,48.25),
    "sensor13"->(519.42,523.65),
    "sensor14"->(2387.89,2388.32),
    "sensor15"->(8112.11,8219.27),
    "sensor16"->(8.35,8.63),
    "sensor17"->(0.03,0.03),
    "sensor18"->(389.11,397.84),
    "sensor19"->(2388.0,2388.0),
    "sensor20"->(100.0,100.0),
    "sensor21"->(38.3,39.41))

  case class AssetSensorData(var asset_id:Int, var current_age:Int, var setting1:Double, var setting2:Double,
                             var setting3:Double, var sensor1:Double, var sensor2:Double, var sensor3:Double,
                             var sensor4:Double, var sensor5:Double, var sensor6:Double, var sensor7:Double,
                             var sensor8:Double, var sensor9:Double, var sensor10:Double, var sensor12:Double,
                             var sensor13:Double, var sensor14:Double, var sensor15:Double, var sensor16:Double,var sensor17:Double,
                             var sensor18:Double, var sensor19:Double, var sensor20:Double, var sensor21:Double)


  def main(args: Array[String]): Unit = {
    val checkpointLocation = args(0)
    val topics = args(1)
    val triggerIntervalInSeconds = args(2).toLong

    // Step 1: Create fake readstream
    val fakeInputStream = spark.readStream.format("rate")
        .option("numPartitions", 1)
        .option("rowsPerSecond", 1)
        .load()

    // Step 2: Simulate sensor data with different RULs
    fakeInputStream.writeStream.foreachBatch((batchDF: DataFrame, batchId: Long) => {
      println(s"Processing Batch Id: $batchId")
      val redRULData =  spark.createDataFrame(randomSensorReadingsGenerator(RUL_10_30,RED_BATCH_SIZE))
      val yellowRULData =  spark.createDataFrame(randomSensorReadingsGenerator(RUL_30_60,YELLOW_BATCH_SIZE))
      val greenRULData =  spark.createDataFrame(randomSensorReadingsGenerator(RUL_Greater_60,GREEN_BATCH_SIZE))

      val testData = greenRULData.unionAll(yellowRULData.unionAll(redRULData))
        .toDF().withColumn("eventTime", unix_timestamp())
        .select(lit(batchId).cast(StringType).as("key"),to_json(struct("*")).as("value"))
      testData.show(5,false)

      Helper.ociWriterPlain(testData,BOOTSTRAP_SERVER,topics,STREAMPOOL_CONNECTION)
    }).option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerIntervalInSeconds, TimeUnit.SECONDS))
      .start()

    spark.streams.awaitAnyTermination
  }

  /**
   * random sensor readings generator for each asset
   * @param data
   * @return
   */
  def randomSensorReadingsGenerator(data:mutable.LinkedHashMap[String,(Double,Double)],batchSize:Int):ListBuffer[AssetSensorData] = {
    var records = ListBuffer[AssetSensorData]()
    for (i <- 1 to batchSize) {
      var row = ListBuffer[Double]()
      data.foreach(e => {
        row += nextDoubleBetween(e._2._1, e._2._2)
      })
      val engineRecord = AssetSensorData(random.nextInt(NUMBER_OF_ASSETS),random.nextInt(MAX_AGE) ,row(0), row(1), row(2), row(3), row(4),
        row(5), row(6), row(7), row(8), row(9), row(10), row(11), row(12), row(13), row(14), row(15), row(16), row(17),
        row(18), row(19), row(20), row(21),row(22))
      records+=engineRecord
    }

    def nextDoubleBetween(min: Double, max: Double): Double = (random.nextDouble * (max - min)) + min
    records
  }
}
