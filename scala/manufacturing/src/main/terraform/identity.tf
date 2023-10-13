resource "oci_identity_compartment" "compartment" {
  name           = "${var.compartment-name}-${var.suffix}"
  description    = "Data Flow livelabs comparment managed by OCI Resoure Manager"
  compartment_id = var.tenancy_ocid
  enable_delete  = false
}

resource "oci_identity_group" "group" {
  depends_on = [oci_identity_compartment.compartment]
  name           = "${var.user-group-name}-${var.suffix}"
  description    = "Data Flow livelabs group managed by OCI Resoure Manager"
  compartment_id = var.tenancy_ocid
}

resource "oci_identity_user_group_membership" "user-group-mem" {
  depends_on = [oci_identity_group.group]
  compartment_id = var.tenancy_ocid
  user_id        = var.user_ocid
  group_id       = oci_identity_group.group.id
}

resource "oci_identity_policy" "policy" {
  depends_on = [oci_identity_user_group_membership.user-group-mem]
  name           = "${var.policies-name}-${var.suffix}"
  description    = "Data Flow livelabs policies managed by OCI Resoure Manager"
  compartment_id = var.tenancy_ocid

  statements = [
    "ALLOW GROUP ${oci_identity_group.group.name} TO READ buckets IN TENANCY",
    "ALLOW SERVICE dataflow TO READ objects IN COMPARTMENT ${var.compartment-name}-${var.suffix} WHERE target.bucket.name='${local.logs_bucket}'",
    "ALLOW GROUP ${oci_identity_group.group.name} TO MANAGE dataflow-family IN COMPARTMENT ${var.compartment-name}-${var.suffix}",
    "ALLOW GROUP ${oci_identity_group.group.name} TO MANAGE object-family IN COMPARTMENT ${var.compartment-name}-${var.suffix}",
    
    ### Resource Principal Policies
    "ALLOW any-user TO MANAGE object-family IN COMPARTMENT ${var.compartment-name}-${var.suffix} WHERE ALL {request.principal.type='dataflowrun'}",
    "ALLOW any-user TO MANAGE stream-family IN COMPARTMENT ${var.compartment-name}-${var.suffix} WHERE ALL {request.principal.type='dataflowrun'}",
    "ALLOW any-user TO MANAGE autonomous-database-family IN COMPARTMENT ${var.compartment-name}-${var.suffix} WHERE ALL {request.principal.type='dataflowrun'}",
    "ALLOW any-user TO MANAGE logging-family IN COMPARTMENT ${var.compartment-name}-${var.suffix} WHERE ALL {request.principal.type='dataflowrun'}",
    "ALLOW any-user TO MANAGE secret-family IN COMPARTMENT ${var.compartment-name}-${var.suffix} WHERE ALL {request.principal.type='dataflowrun'}"
  ]
}

