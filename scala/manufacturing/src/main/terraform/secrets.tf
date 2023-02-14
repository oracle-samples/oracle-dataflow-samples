/*
resource "oci_kms_vault" "manufacturing_vault" {
    depends_on  = [oci_identity_policy.policy,oci_database_autonomous_database.autonomous_database]
    compartment_id = oci_identity_compartment.compartment.id
    display_name = "manufacturing"
    vault_type = "DEFAULT"
}

resource "oci_kms_key" "master_encrypt_key" {
    depends_on = [oci_kms_vault.manufacturing_vault]
    compartment_id = oci_identity_compartment.compartment.id
    display_name = "abd"
    key_shape {
        algorithm = "AES"
        length = "32"
    }
    management_endpoint = oci_kms_vault.manufacturing_vault.management_endpoint
}

resource "oci_vault_secret" "manufacturing_secret" {
  depends_on  = [oci_kms_key.master_encrypt_key]
  compartment_id  = oci_identity_compartment.compartment.id
  vault_id    = oci_kms_vault.manufacturing_vault.id
  key_id      = oci_kms_key.master_encrypt_key.id
  secret_name = "adbPassword"
  secret_content {
        content_type = "BASE64"
        content = base64encode(random_string.autonomous_database_admin_password.result)
        name = "adbPassword"
        stage = "CURRENT"
    }
}
*/