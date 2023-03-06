resource "random_string" "autonomous_database_admin_password" {
  length      = 16
  min_numeric = 1
  min_lower   = 1
  min_upper   = 1
  min_special = 1
}

resource "oci_database_autonomous_database" "autonomous_database" {
  depends_on  = [oci_identity_policy.policy]
  admin_password           = random_string.autonomous_database_admin_password.result
  compartment_id           = oci_identity_compartment.compartment.id
  cpu_core_count           = local.cpu_core_count
  data_storage_size_in_tbs = local.data_storage_size_in_tbs
  db_name                  = var.adw_database_name
  db_workload              = local.adw_workload
  display_name             = var.adw_database_name
  license_model            = local.adb_license
}
