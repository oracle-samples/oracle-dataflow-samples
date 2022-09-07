provider "oci" {
  region           = var.region
  tenancy_ocid     = var.tenancy_ocid
  user_ocid        = var.user_ocid
}

locals {
  ### Identity
  namespace  = data.oci_objectstorage_namespace.ns.namespace

  ### ADW
  adw_workload = "DW"
  adb_license = "LICENSE_INCLUDED"
  adb_dedicated = false
  cpu_core_count = "1"
  data_storage_size_in_tbs = "1"
  db_version = "19c"

  ### Object Storage
  logs_bucket = "dataflow-labs-logs"
  buckets = [var.app_bucket,local.logs_bucket]

  source_bucket = "cloudworld"
  source_ns = "bigdatadatasciencelarge"
  source_region = "us-ashburn-1"
  artifacts_prefix = "demo/artifacts"
  data_prefix = "demo/data"
  streaming_artifact_name = "manufacturing-assembly-0.6.jar"
  delta_artifact_name = "predicted_rul_delta.py"
  train_sensors_name = "train_sensors.txt"
  test_sensors_name = "test_sensors.txt"
  test_rul_name = "test_RUL.txt"
  artifacts = [local.delta_artifact_name] 
  app_data = [local.train_sensors_name,local.test_sensors_name,local.test_rul_name]

  logs_bucket_uri = "oci://${local.logs_bucket}@${local.namespace}"

  ### Streaming
  auto_create_topics_enable = true
  log_retention_hours       = 24
  num_partitions            = 1
  sensor_data_simulator_stream = "sensor_data_simulator_stream"
  rul_predictor_stream = "rul_predictor_stream"
  stream_names = [local.sensor_data_simulator_stream] 

  app_conf_uri = "oci://${var.app_bucket}@${local.namespace}/${local.artifacts_prefix}/application.conf"

  ### RUL Survival model local variables.
  training_sensor_dataset = "oci://${local.source_bucket}@${local.source_ns}/${local.data_prefix}/${local.train_sensors_name}"
  test_sensor_dataset    = "oci://${local.source_bucket}@${local.source_ns}/${local.data_prefix}/${local.test_sensors_name}"
  test_rul_dataset    = "oci://${local.source_bucket}@${local.source_ns}/${local.data_prefix}/${local.test_rul_name}"
  model_location = "oci://${var.app_bucket}@${local.namespace}/demo/models/"
  model_file_uri = "oci://${local.source_bucket}@${local.source_ns}/${local.artifacts_prefix}/${local.streaming_artifact_name}"
  #model_arguments = split(",","${local.training_sensor_dataset},${local.model_location},${local.test_sensor_dataset},${local.test_rul_dataset}")
  model_arguments = [local.app_conf_uri]

  ### Sensor Data simulator local variables.
  simulator_checkpoint_location = "oci://${var.app_bucket}@${local.namespace}/demo/checkpoints/simulator/"
  simulator_file_uri = "oci://${local.source_bucket}@${local.source_ns}/${local.artifacts_prefix}/${local.streaming_artifact_name}"
  simulator_arguments = [local.app_conf_uri]

  ### RUL realtime predictor application.
  predictor_checkpoint_location = "oci://${var.app_bucket}@${local.namespace}/demo/checkpoints/predictor/"
  predictor_file_uri = "oci://${local.source_bucket}@${local.source_ns}/${local.artifacts_prefix}/${local.streaming_artifact_name}"
  predictor_arguments = [local.app_conf_uri]
  predictor_configuraton = {"spark.oracle.datasource.enabled":"true"}
  predictor_parquet_table =  "oci://${var.app_bucket}@${local.namespace}/demo/predicted_rul_ParquetTable/"
  predictor_delta_table = "oci://${var.app_bucket}@${local.namespace}/demo/predicted_rul_DeltaTable/"

  ### RUL Delta Table
  delta_file_url = "oci://${local.source_bucket}@${local.source_ns}/${local.artifacts_prefix}/${local.delta_artifact_name}"
  delta_arguments = [local.predictor_delta_table]
}