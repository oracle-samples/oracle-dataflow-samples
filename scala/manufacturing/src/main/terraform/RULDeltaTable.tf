resource "oci_dataflow_application" "delta_table_app" {
  depends_on  = [oci_objectstorage_object.upload_app_conf]
  compartment_id = oci_identity_compartment.compartment.id
  display_name   = var.rul_delta_table_app["display_name"]
  driver_shape   = var.rul_delta_table_app["driver_shape"]
  executor_shape = var.rul_delta_table_app["executor_shape"]
  driver_shape_config {
    ocpus = var.rul_delta_table_app["driver_ocpus"]
    memory_in_gbs = var.rul_delta_table_app["driver_memory_in_gbs"]
  }
  executor_shape_config {
    ocpus = var.rul_delta_table_app["executor_ocpus"]
    memory_in_gbs = var.rul_delta_table_app["executor_memory_in_gbs"]
  }
  file_uri       = local.delta_file_url
  language       = var.rul_delta_table_app["language"]
  num_executors  = var.rul_delta_table_app["num_executors"] ### fix later for dynamic allocation
  spark_version  = var.rul_delta_table_app["spark_version"]
  arguments      = local.delta_arguments
  type           = var.rul_delta_table_app["type"]
  logs_bucket_uri = local.logs_bucket_uri
}
