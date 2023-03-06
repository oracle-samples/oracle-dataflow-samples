package com.oracle.dataflow.utils

import java.util.regex.Pattern
import javax.ws.rs.Priorities

object Constants {
  // Model
  final val SURVIVAL_MODEL_DIR = "survival_model"
  final val SCALER_MODEL_DIR = "scaler_model"
  final  val ASSEMBLER_MODEL_DIR = "features_model"
  final val CENSOR_FIELD_NAME = "censor"
  final val FEATURES_FIELD = "features"
  final val SCALED_FEATURES_FIELD = "scaled_features"
  final val RUL_LABEL_FIELD = "rul"

  // OCI Streaming Connection Information
  final val STREAMPOOL_CONNECTION = "com.oracle.bmc.auth.sasl.ResourcePrincipalsLoginModule required intent=\"streamPoolId:%s\";"

  // Producer
  final val NUMBER_OF_ASSETS = 10000
  final val MAX_AGE = 10
  final val GREEN_BATCH_SIZE = 100
  final val RED_BATCH_SIZE = 2
  final val YELLOW_BATCH_SIZE = 5

  // ADW
  final val PREDICTED_RUL_ALERTS = "ADMIN.PREDICTED_RUL_ALERTS"

  // Auth
  final val SPARK_HADOOP_FS_DELEGATION_TOKEN_PATH_ENV = "SPARK_HADOOP_FS_DELEGATION_TOKEN_PATH"
  final val DATAFLOW_AUTH_ENV="DATAFLOW_AUTH"
  final val RP = "resource_principal"
  final val DELEGATION = "instance_delegation"
  final val URI_PATTERN = Pattern.compile("oci:\\/\\/(.*)@([^\\/]+)\\/(.*)")
}
