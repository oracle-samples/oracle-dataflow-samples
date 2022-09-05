package com.oracle.dataflow.utils

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

}
