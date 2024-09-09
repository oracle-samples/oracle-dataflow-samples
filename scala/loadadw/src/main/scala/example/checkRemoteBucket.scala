/**
 * Copyright (c) 2024, Oracle and/or its affiliates. All rights reserved.
 */

package example

import com.oracle.bmc.objectstorage.ObjectStorageClient
import org.apache.hadoop.fs.Path

object checkRemoteBucket {
  def main(args: Array[String]) = {

    if (args.length == 0) {
      println("Missing configuraton files to be processes")
      sys.exit(1)
    }

    val bucketsURL = args(0)
    println("Bucket=  " + bucketsURL)
    val spark = DataFlowSparkSession.getSparkSession("Cerner Ontology List Remote Buckets")
    val bmcFS = DataFlowBmcFilesystemClient.getBmcFilesystemClient(bucketsURL, DataFlowSparkSession.getBmcConfiguration(spark))
    println("bmcFS = " + bmcFS.toString)
    val dirPath = new Path(bucketsURL)
    println("dirPath= " + dirPath.toString)
    println(System.getenv("HOME"))

    val it = bmcFS.listStatusIterator(dirPath)
    println("After Remote iterator + ******************")
    if (bmcFS.isDirectory(dirPath)) {
      println("Directory = " + dirPath)
      val it = bmcFS.listFiles(dirPath, true)
      println("Iterator = " + it.toString())
      while (it.hasNext()) {
        val fileStatus = it.next()
        println("Next = " + fileStatus.getPath())
      }
    } else if (bmcFS.isFile(dirPath)) {
      println("File = " + dirPath)
    }

  }
}

