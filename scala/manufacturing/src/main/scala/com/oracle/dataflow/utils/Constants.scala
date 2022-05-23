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

  // Bigdatasciencelarge
  val STREAMPOOL_CONNECTION = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"bigdatadatasciencelarge/sivanesh.selvanataraj@oracle.com/ocid1.streampool.oc1.iad.amaaaaaanif7xwiafdp6giocma6mac23gt5i25q4nfsytr25eaxbbhvmvdca\" password=\"2[5iZP.9AW65mj;P}gDo\";"
  val BOOTSTRAP_SERVER = "cell-1.streaming.us-ashburn-1.oci.oraclecloud.com:9092"

  // passdevssstest
  // val BOOTSTRAP_SERVER = "cell-1.streaming.us-phoenix-1.oci.oraclecloud.com:9092"
  // val STREAMPOOL_CONNECTION = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"paasdevssstest/sivanesh.selvanataraj@oracle.com/ocid1.streampool.oc1.phx.amaaaaaal3toglqamzeu5yha54yudjasljvb3nq2htyenh63aeoangq2nn2a\" password=\"t13utBM.k.L)r.GssJE-\";"

  // Producer
  val NUMBER_OF_ASSETS = 10000
  val MAX_AGE = 10
  val GREEN_BATCH_SIZE = 100
  val RED_BATCH_SIZE = 2
  val YELLOW_BATCH_SIZE = 5

  // Consumer
  val ADB_ID = "ocid1.autonomousdatabase.oc1.iad.anuwcljrnif7xwiayhoormvqlhw33r6zrgreokmuyn4ozxjs5dci3nigbeaq"
  val PREDICTED_RUL_ALERTS = "ADMIN.PREDICTED_RUL_ALERTS"
  val SENSOR_DATA_READINGS = "ADMIN.SENSOR_DATA_READINGS"
  val USERNAME = "ADMIN"
  val PASSWORD = "Password-123"
}
