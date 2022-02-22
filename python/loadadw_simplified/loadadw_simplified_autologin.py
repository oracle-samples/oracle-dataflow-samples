"""
A more simplified example demonstrating Oracle Spark data source with AUTO-LOGIN i.e., username and password from wallet as Secure External Password Store
https://docs.oracle.com/cd/B19306_01/network.102/b14266/cnctslsh.htm#CBHEHGCE

Please keep your wallet safe in object storage.
"""

from pyspark.sql import SparkSession

def oracle_datasource_example(spark):
    properties = {"adbId": ADB_ID, "user": USER, "password": PASSWORD}

    print("Reading data from autonomous database.")
    src_df = spark.read.format("oracle") \
        .options(**properties).option("dbtable",SRC_TABLE).load()

    # Note: providing connectionId is optional with adbId
    print("Writing data to autonomous database.")
    src_df.write.format("oracle") \
        .options(**properties).option("dbtable",TARGET_TABLE) \
        .option("connectionId", CONNECTION_ID) \
        .save()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark Oracle Datasource Example").getOrCreate()

    # TODO: Set all these variables.
    ADB_ID = "ocid1.autonomousdatabase.<REALM>.[REGION][.FUTURE USE].<UNIQUE ID>"
    USER = "ADMIN"
    PASSWORD = "PASSWORD" # This is just an example.Recommend to access password secure way for example oci vault service.
    SRC_TABLE = "SCHEMA.TABLE"
    TARGET_TABLE = "SCHEMA.TABLE"
    CONNECTION_ID = "database_type"

    oracle_datasource_example(spark)

    spark.stop()