package com.oracle.delta

object LongRunDelta {

  def main(args: Array[String]): Unit = {

    print("Starting delta lake sample run")

    if (args.length == 0) {
      println("I need at least input and output path")
    }

    val inputPath = args(0)
    val deltaPath = args(1)
    val parquetPath = args(2)
    val sleepTimeInSec = args(3)
    val totalRuns = args(4).toInt

    println("\n" + inputPath +  ", " + deltaPath)
    var count = 0

     while(count < totalRuns) {

       println("Wakeup " + count + " -- " + System.currentTimeMillis())
       DeltaTable.csvToDeltaAppend(inputPath, parquetPath)

       Thread.sleep(1000 * sleepTimeInSec.toInt)
       count = count + 1
     }
  }
}
