// These variables would commonly be defined as environment variables or sourced in a .env file

### Tenancy
variable "tenancy_ocid" {
}

variable "user_ocid" {
}

variable "compartment_ocid" {
}

variable "region" {
  default = "us-ashburn-1"
}

### Identity
variable "user-group-name" {
  default = "dataflow-labs"
}

variable "compartment-name" {
  default = "dataflow-labs"
}

variable "policies-name" {
  default = "dataflow-labs-policies"
}

#### ADW
variable "adw_database_name" {
  default = "manufacturing"
}

### Streaming
variable "stream-pool-name" {
  default = "manufacturing"
}

### Object Storage
variable "app_bucket" {
  default = "dataflow-labs"
}

variable "rul_survival_model_trainer_app" {
  description = "parameters for RUL survival model trainer application"
  type = map
  default = {
    display_name = "RULSurvivalModelTrainer"
    driver_shape = "VM.Standard2.1"
    executor_shape = "VM.Standard2.1"
    num_executors = 1
    spark_version = "3.2.1"
    class_name = "com.oracle.dataflow.RULSurvivalModelTrainer"
    language = "SCALA"
    type = "BATCH"
  }
}

variable "rul_sensor_data_simulator_app" {
  description = "parameters for sensor data simulator streaming applicaiton"
  type = map
  default = {
    display_name = "SensorDataSimulator"
    driver_shape = "VM.Standard2.1"
    executor_shape = "VM.Standard2.1"
    num_executors = 1
    spark_version = "3.2.1"
    class_name = "com.oracle.dataflow.SensorDataSimulator"
    language = "SCALA"
    type = "STREAMING"
    trigger_interval_sec = 10
  }
}

variable "rul_realtime_predictor_app" {
  description = "parameters for realtime rul predictor streaming applicaiton"
  type = map
  default = {
    display_name = "RealtimeRULPredictor"
    driver_shape = "VM.Standard2.1"
    executor_shape = "VM.Standard2.1"
    num_executors = 1
    spark_version = "3.2.1"
    class_name = "com.oracle.dataflow.RealtimeRULPredictor"
    language = "SCALA"
    type = "STREAMING"
    trigger_interval_sec = 120
  }
}

variable "rul_delta_table_app" {
  description = "parameters for rul delta table applicaiton"
  type = map
  default = {
    display_name = "RULDeltaTable"
    driver_shape = "VM.Standard.E4.Flex"
    executor_shape = "VM.Standard.E4.Flex"
    driver_ocpus = 1
    driver_memory_in_gbs = 16
    executor_ocpus = 1
    executor_memory_in_gbs = 16
    num_executors = "1"
    spark_version = "3.2.1"
    language = "PYTHON"
    type = "BATCH"
  }
}






