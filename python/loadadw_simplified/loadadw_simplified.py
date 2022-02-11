"""
A simple example demonstrates the usage of oracle datasource.
"""

from pyspark.sql import SparkSession

"""
 Connect autonomous database using autonomous database ocid.
"""
def oracle_datasource_auto_connect_example(spark):
    properties = {"adbId": ADB_ID, "user": USER, "password": PASSWORD}

    # Read from autonomous database
    src_df = spark.read.format("oracle") \
        .options(**properties).option("dbtable",SRC_TABLE).load()

    # Write to autonomous database
    # Note: providing connectionId is optional with adbId
    src_df.write.format("oracle") \
        .options(**properties).option("dbtable",TARGET_TABLE) \
        .option("connectionId", CONNECTION_ID) \
        .save()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark Oracle Datasource Example").getOrCreate()

    # TODO: Set all these variables.
    ADB_ID = "ocid1.autonomousdatabase.<REALM>.[REGION][.FUTURE USE].<UNIQUE ID>"
    USER = "ADMIN"
    PASSWORD = "PASSWORD" # Recommend to use secure way of using password.This is just example
    SRC_TABLE = "SCHEMA.TABLE"
    TARGET_TABLE = "SCHEMA.TABLE"
    CONNECTION_ID = "database_type"

    oracle_datasource_auto_connect_example(spark)

    spark.stop()