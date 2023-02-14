resource "oci_streaming_stream_pool" "manufacturing_stream_pool" {
  depends_on  = [oci_identity_policy.policy]
  compartment_id = oci_identity_compartment.compartment.id
  name           = "${var.stream-pool-name}-${var.suffix}"

  kafka_settings {
    auto_create_topics_enable = local.auto_create_topics_enable
    log_retention_hours       = local.log_retention_hours
    num_partitions            = local.num_partitions
  }
}


resource "oci_streaming_stream" "manufacturing_stream" {
  depends_on = [oci_streaming_stream_pool.manufacturing_stream_pool]
  for_each = toset(local.stream_names)
  name = each.key
  stream_pool_id = oci_streaming_stream_pool.manufacturing_stream_pool.id
  partitions = local.num_partitions
}