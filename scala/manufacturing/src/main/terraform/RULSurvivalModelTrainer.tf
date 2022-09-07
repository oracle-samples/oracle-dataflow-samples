resource "oci_dataflow_application" "rul_model_trainer_app" {
  depends_on  = [oci_objectstorage_object.upload_app_conf]
  compartment_id = oci_identity_compartment.compartment.id
  display_name   = var.rul_survival_model_trainer_app["display_name"]
  driver_shape   = var.rul_survival_model_trainer_app["driver_shape"]
  executor_shape = var.rul_survival_model_trainer_app["executor_shape"]
  file_uri       = local.model_file_uri 
  language       = var.rul_survival_model_trainer_app["language"]
  num_executors  = var.rul_survival_model_trainer_app["num_executors"]
  spark_version  = var.rul_survival_model_trainer_app["spark_version"]
  arguments      = local.model_arguments
  class_name     = var.rul_survival_model_trainer_app["class_name"]
  logs_bucket_uri = local.logs_bucket_uri
  type           = var.rul_survival_model_trainer_app["type"]
}
