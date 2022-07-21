package com.oracle.benchmark

object DeltaLakeBenchmark {
  def main(args: Array[String]): Unit = {
    print("Starting delta lake benchmark started")

    if (args.length == 0) {
      println("I need at least input and output path")
    }

    val tpcdsParquetPath = args(0)
    val deltaTablePath = args(1)

    println("\n" + tpcdsParquetPath +  ", " + deltaTablePath)

    // Load TPC-DS data as Delta Table
    new TPCDSDataLoad().createDeltaTables(tpcdsParquetPath, deltaTablePath)

    // Run Bench mark queries
  }
}