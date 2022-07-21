package com.oracle.delta

object DeltaStreamRun {

  def main(args: Array[String]): Unit = {

    print("Starting delta lake sample run")

    if (args.length == 0) {
      println("I need at least input and output path")
    }

    val inputPath = args(0)
    val deltaPath = args(1)
    val parquetPath = args(2)
    val checkpoint = args(3)

    println("\n" + inputPath +  ", " + deltaPath +  ", " + checkpoint)

    DeltaTable.StartDeltaStreamSink(inputPath, parquetPath, checkpoint)

  }

}
