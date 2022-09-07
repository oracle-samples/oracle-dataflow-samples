resource "oci_dataflow_application" "realtime_predictor_app" {
  depends_on  = [oci_objectstorage_object.upload_app_conf,oci_vault_secret.manufacturing_secret]
  compartment_id = oci_identity_compartment.compartment.id
  display_name   = var.rul_realtime_predictor_app["display_name"]
  driver_shape   = var.rul_realtime_predictor_app["driver_shape"]
  executor_shape = var.rul_realtime_predictor_app["executor_shape"]
  file_uri       = local.predictor_file_uri
  language       = var.rul_realtime_predictor_app["language"]
  num_executors  = var.rul_realtime_predictor_app["num_executors"]
  spark_version  = var.rul_realtime_predictor_app["spark_version"]
  arguments      = local.predictor_arguments
  class_name     = var.rul_realtime_predictor_app["class_name"]
  type           = var.rul_realtime_predictor_app["type"]
  logs_bucket_uri = local.logs_bucket_uri
  configuration  = {"spark.oracle.datasource.enabled" = "true"}
}
