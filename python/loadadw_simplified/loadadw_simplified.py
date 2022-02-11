"""
A simple example demonstrates the usage of oracle datasource.
"""

from pyspark.sql import SparkSession

def oracle_datasource_auto_connect_example(spark):
    properties = {"adbId": ADB_ID, "user": USER, "password": PASSWORD}

    # Read
    src_df = spark.read.format("oracle") \
        .options(**properties).option("dbtable",SRC_TABLE).load()

    # Write
    src_df.write.format("oracle") \
        .options(**properties).option("dbtable",TARGET_TABLE).save()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark Oracle Datasource Example").getOrCreate()

    # TODO: Set all these variables.
    ADB_ID = "ocid1.autonomousdatabase.<REALM>.[REGION][.FUTURE USE].<UNIQUE ID>"
    USER = "ADMIN"
    PASSWORD = "PASSWORD"
    SRC_TABLE = "SCHEMA.TABLE"
    TARGET_TABLE = "SCHEMA.TABLE"

    oracle_datasource_auto_connect_example(spark)

    spark.stop()