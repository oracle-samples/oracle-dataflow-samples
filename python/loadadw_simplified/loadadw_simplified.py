"""
A simple example demonstrating Oracle Spark data source.
"""

from pyspark.sql import SparkSession

def oracle_datasource_auto_connect_example(spark):
    # Autonomous database information.
    properties = {"adbId": ADB_ID,"user": USER,"password": PASSWORD}

    # Reading data from autonomous database.
    src_df = spark.read \
        .format("oracle") \
        .options(**properties) \
        .option("dbtable",SRC_TABLE) \
        .load()

    # Writing data to autonomous database.
    src_df.write \
        .format("oracle") \
        .option("dbtable",TARGET_TABLE) \
        .options(**properties) \
        .save()

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark Oracle Datasource Example") \
        .getOrCreate()

    # TODO: Set all these variables.
    ADB_ID = "ocid1.autonomousdatabase.<REALM>.[REGION][.FUTURE USE].<UNIQUE ID>"
    USER = "ADMIN"
    """
    Note: Not recommending to paste password here. This is just example. 
          Please use your own secure way.For example oci vault service.
    """
    PASSWORD = "PASSWORD"
    SRC_TABLE = "SCHEMA.TABLE"
    TARGET_TABLE = "SCHEMA.TABLE"

    oracle_datasource_auto_connect_example(spark)
    oracle_datasource_auto_connect_example(spark)

    spark.stop()