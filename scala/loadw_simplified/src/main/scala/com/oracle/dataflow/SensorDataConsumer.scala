package com.oracle.dataflow

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SensorDataConsumer {

  val STREAMPOOL_CONNECTION = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"bigdatadatasciencelarge/sivanesh.selvanataraj@oracle.com/ocid1.streampool.oc1.iad.amaaaaaanif7xwiafdp6giocma6mac23gt5i25q4nfsytr25eaxbbhvmvdca\" password=\"2[5iZP.9AW65mj;P}gDo\";"
  val TOPIC = "sensor_data"
  val BOOTSTRAP_SERVER = "cell-1.streaming.us-ashburn-1.oci.oraclecloud.com:9092"

  def main(args: Array[String]): Unit = {
    val triggerInSeconds =args(0).toInt
    val checkpointLocation = args(1)

    val sparkBuilder = SparkSession.builder().appName(this.getClass.getName)
    var spark: SparkSession = null
    if ((new SparkConf).contains("spark.master"))
      spark = sparkBuilder.getOrCreate()
    else
      spark = sparkBuilder
        .master("local[*]")
        // .config("spark.plugins","oracle.dataflow.spark.dsv1.adb.plugin.WalletInstallerPlugin")
        .getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")


    val testDataStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
      .option("subscribe", TOPIC)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.jaas.config", STREAMPOOL_CONNECTION)
      .option("kafka.max.partition.fetch.bytes", 1024 * 1024)
      .option("startingOffsets", "latest")
      .load()


    testDataStream.writeStream.foreachBatch((testData: DataFrame, batchId: Long) => {
      println(s"batchId ${batchId}")
      val testData = spark.read.format("oracle")
        .option("adbId","ocid1.autonomousdatabase.oc1.iad.anuwcljsnif7xwia2yqgrs5qtyp6wf37zzw66r2igsiyui7giijhf2dctovq")
        .option("dbtable", "ADMIN.PERSONS")
        .option("user", "ADMIN")
        .option("password", "Password-123")
        .load()

      testData.printSchema()
      testData.show(10,false)

       testData.write.format("oracle")
        .option("adbId","ocid1.autonomousdatabase.oc1.iad.anuwcljsnif7xwia2yqgrs5qtyp6wf37zzw66r2igsiyui7giijhf2dctovq")
        .option("dbtable", "ADMIN.PERSONS_COPY")
        .option("user", "ADMIN")
        .option("password", "Password-123")
         .mode(SaveMode.Append)
        .save()
    }).option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInSeconds))
      .start()

    spark.streams.awaitAnyTermination
  }
}
