data "oci_objectstorage_namespace" "ns" {
   compartment_id = oci_identity_compartment.compartment.id
}

resource "oci_objectstorage_bucket" "df_buckets" {
  depends_on  = [oci_identity_policy.policy]
  for_each = toset(local.buckets)
  compartment_id = oci_identity_compartment.compartment.id
  namespace      = data.oci_objectstorage_namespace.ns.namespace
  name           = each.key
  access_type    = "NoPublicAccess"
  auto_tiering = "Disabled"
  delete_all_object_versions = true
}

data "template_file" "app_conf" {
  depends_on  = [oci_objectstorage_bucket.df_buckets]
  template = file("${path.module}/application.conf")
  vars = {
    trainingDataset             = local.training_sensor_dataset
    testSensorDataset           = local.test_sensor_dataset
    testRULDataset              = local.test_rul_dataset 
    modelOutputPath             = local.model_location
    simulator_checkpointLocation = local.simulator_checkpoint_location
    predictor_checkpointLocation = local.predictor_checkpoint_location
    streampoolId                 = oci_streaming_stream_pool.manufacturing_stream_pool.id
    bootstrapServer              = oci_streaming_stream_pool.manufacturing_stream_pool.kafka_settings[0].bootstrap_servers
    adbId                        = oci_database_autonomous_database.autonomous_database.id
    secretOcid                   = oci_vault_secret.manufacturing_secret.id 
    parquetOutputPath            = local.predictor_parquet_table
    deltaOutputPath              = local.predictor_delta_table 
    rul_predictor_stream         = local.rul_predictor_stream
    sensor_data_simulator_stream = local.sensor_data_simulator_stream 
  }
}

resource "oci_objectstorage_object" "upload_app_conf" {
  depends_on  = [oci_objectstorage_bucket.df_buckets]
  bucket = var.app_bucket
  namespace = data.oci_objectstorage_namespace.ns.namespace
  content = data.template_file.app_conf.rendered
  object = "${local.artifacts_prefix}/application.conf"
}
