package com.oracle.benchmark

import com.oracle.benchmark.TPCDSBenchmarkQueries.{TPCDSQueries10TB, TPCDSQueries3TB}


class TPCDSBenchmark(scaleInGB: Int = 1000) extends Benchmark() {
  val iterations: Int = 1

  val queries: Map[String, String] = {
    if (scaleInGB <= 3000) TPCDSQueries3TB
    else if (scaleInGB == 10) TPCDSQueries10TB
    else throw new IllegalArgumentException(
      s"Unsupported scale factor of ${scaleInGB} GB")
  }

  val dbName = ""
  val extraConfs: Map[String, String] = Map(
    "spark.sql.broadcastTimeout" -> "7200",
    "spark.sql.crossJoin.enabled" -> "true"
  )

  def runInternal(): Unit = {
    for ((k, v) <- extraConfs) spark.conf.set(k, v)
    spark.sparkContext.setLogLevel("WARN")
    log("All configs:\n\t" + spark.conf.getAll.toSeq.sortBy(_._1).mkString("\n\t"))
    spark.sql(s"USE $dbName")
    for (iteration <- 1 to iterations) {
      queries.toSeq.sortBy(_._1).foreach { case (name, sql) =>
        runQuery(sql, iteration = Some(iteration), queryName = name)
      }
    }
    val results = getQueryResults().filter(_.name.startsWith("q"))
    if (results.forall(x => x.errorMsg.isEmpty && x.durationMs.nonEmpty) ) {
      val medianDurationSecPerQuery = results.groupBy(_.name).map { case (q, results) =>
        assert(results.size == iterations)
        val medianMs = results.map(_.durationMs.get).sorted
          .drop(math.floor(iterations / 2.0).toInt).head
        (q, medianMs / 1000.0)
      }
      val sumOfMedians = medianDurationSecPerQuery.map(_._2).sum
      reportExtraMetric("tpcds-result-seconds", sumOfMedians)
    }
  }
}



object TPCDSBenchmark {
  def main(args: Array[String]): Unit = {
   // new TPCDSBenchmark().run()
  }
}