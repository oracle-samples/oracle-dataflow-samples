package com.oracle.dataflow.utils

object Constants {
  // Model
  val SURVIVAL_MODEL_DIR = "survival_model"
  val SCALER_MODEL_DIR = "scaler_model"
  val ASSEMBLER_MODEL_DIR = "features_model"
  val CENSOR_FIELD_NAME = "censor"
  val FEATURES_FIELD = "features"
  val SCALED_FEATURES_FIELD = "scaled_features"
  val RUL_LABEL_FIELD = "rul"

  // OCI Streaming Connection Information
  val STREAMPOOL_CONNECTION = "com.oracle.bmc.auth.sasl.ResourcePrincipalsLoginModule required intent=\"streamPoolId:%s\";"

  // Producer
  val NUMBER_OF_ASSETS = 10000
  val MAX_AGE = 10
  val GREEN_BATCH_SIZE = 100
  val RED_BATCH_SIZE = 2
  val YELLOW_BATCH_SIZE = 5

  // Consumer
  val PREDICTED_RUL_ALERTS = "ADMIN.PREDICTED_RUL_ALERTS"
}
