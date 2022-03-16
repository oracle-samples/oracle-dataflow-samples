"""
A simple example demonstrates the usage of oracle datasource with wallet from object storage.
"""

from pyspark.sql import SparkSession

def oracle_datasource_example(spark):
  properties = {"walletUri": WALLET_URI, "connectionId": CONNECTION_ID, "user": USER, "password": PASSWORD}

  print("Reading data from autonomous database.")
  src_df = spark.read.format("oracle") \
    .options(**properties).option("dbtable",SRC_TABLE).load()

  print("Writing data to autonomous database.")
  src_df.write.format("oracle") \
    .options(**properties).option("dbtable",TARGET_TABLE) \
    .save()

if __name__ == "__main__":
  spark = SparkSession.builder.appName("Python Spark Oracle Datasource Example").getOrCreate()

  # TODO: Set all these variables.
  WALLET_URI = "oci://<bucket>@<namespace>/wallet_database.zip"
  USER = "ADMIN"
  PASSWORD = "PASSWORD" # This is just an example.Recommend to access password secure way for example oci vault service.
  SRC_TABLE = "SCHEMA.TABLE"
  TARGET_TABLE = "SCHEMA.TABLE"
  CONNECTION_ID = "database_type" # Required with walletUri option

  oracle_datasource_example(spark)

  spark.stop()