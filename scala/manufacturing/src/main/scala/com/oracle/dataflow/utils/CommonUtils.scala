package com.oracle.dataflow.utils

import com.oracle.dataflow.RealtimeRULPredictor.log
import com.oracle.dataflow.utils.Constants.{DATAFLOW_AUTH_ENV, DELEGATION, RP, SPARK_HADOOP_FS_DELEGATION_TOKEN_PATH_ENV}

object CommonUtils {
  def isRunningInDataFlow: Boolean = {
    System.getenv("HOME") == "/home/dataflow"
  }

  def isRP: Boolean = System.getenv(DATAFLOW_AUTH_ENV) == RP
  def isDelegation: Boolean = System.getenv(DATAFLOW_AUTH_ENV) == DELEGATION

  def getDelegationTokenPath: String = {
    if (!isRunningInDataFlow || isRP) {
      println("no delegation token setup")
      return null
    }
    System.getenv(SPARK_HADOOP_FS_DELEGATION_TOKEN_PATH_ENV)
  }

  def printUsage(app:String): Unit = {
    log.info("Missing configuration file argument.Please provide config.")
    System.err.println(s"Usage: $app <oci:// config file path>")
  }

  def validateArgs(args: Array[String]): Unit = {
    if (args.length == 0) {
      printUsage(this.getClass.getName)
      sys.exit(1)
    }
  }

}
