package com.oracle.iceberg

object Main {

  def main(args: Array[String]): Unit = {

    print("Starting Iceberg sample run")

    if (args.length == 0) {
      println("I need at least input and output path")
    }

    val inputPath = args(0)
    val outputPath = args(1)

    println("\n" + inputPath +  ", " + outputPath)
    IcebergTable.runIcebergSample(inputPath, outputPath)
    println("\n Iceberg Operation are done!!!" )
  }

}
