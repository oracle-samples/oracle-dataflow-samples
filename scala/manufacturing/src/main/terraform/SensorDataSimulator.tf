resource "oci_dataflow_application" "sensor_data_simulator_app" {
  depends_on  = [oci_objectstorage_object.upload_app_conf,oci_streaming_stream.manufacturing_stream]
  compartment_id = oci_identity_compartment.compartment.id
  display_name   = var.rul_sensor_data_simulator_app["display_name"]
  driver_shape   = var.rul_sensor_data_simulator_app["driver_shape"]
  executor_shape = var.rul_sensor_data_simulator_app["executor_shape"]
  file_uri       = local.simulator_file_uri
  language       = var.rul_sensor_data_simulator_app["language"]
  num_executors  = var.rul_sensor_data_simulator_app["num_executors"]
  spark_version  = var.rul_sensor_data_simulator_app["spark_version"]
  arguments      = local.simulator_arguments
  class_name     = var.rul_sensor_data_simulator_app["class_name"]
  logs_bucket_uri = local.logs_bucket_uri
  type           = var.rul_sensor_data_simulator_app["type"]
}
