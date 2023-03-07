"""
A simple example demonstrates the usage of oci metastore and oracle datasource.
"""
import argparse
import os


from pyspark.sql import SparkSession
import pyspark.sql.functions as f
def oracle_datasource_example(spark):
    
    #properties = {"adbId": ADB_ID, "user": USER, "password": PASSWORD}
    properties = {"walletUri": walletUri, "user": USER, "password": PASSWORD}

    print("Step 1: Read csv from object storage")
    src_df = spark.read.options(delimiter=',').option("header",True).csv("INPUT_PATH")
    print("Reading data from object storage !")
    src_df.show()
    print("================================================================================================")
   
    print("Step 2: Write csv data into Metastore")
    spark.sql("create database IF NOT EXISTS " + databaseName)
    print("Successfully created database: " + databaseName)
    src_df.write.mode("overwrite").saveAsTable(databaseName + "." + tableName)
    print("Wrote data in Database: " + databaseName + " ; table: " + tableName)
    print("================================================================================================")

    print("Step 3: Read data from Metastore and write into ADW")
    tableDf = spark.sql("select * from " + databaseName + "." + tableName);
    print("Reading data from metastore !")
    tableDf.show()
    # Note: providing connectionId is optional with adbId
    print("Writing data into ADW");
    tableDf.write.format("oracle") \
        .options(**properties).option("dbtable",tableName) \
        .option("connectionId", CONNECTION_ID) \
        .mode("Overwrite") \
        .save()

    print("Reading data from ADW -> ")
    adwDf = spark.read.format("oracle").options(**properties).option("dbtable",tableName).option("connectionId", CONNECTION_ID).load();
    adwDf.show();

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Python Spark Oracle Datasource Example").enableHiveSupport().getOrCreate()

    # TODO: Set all these variables.
    INPUT_PATH = "oci://<bucket>@<tenancy>/fake_data.csv"
    walletUri = "oci://<bucket>@<tenancy>/<wallet>.zip"
    USER = "ADMIN"
    PASSWORD = "<ADW Password>>"
    CONNECTION_ID = "<tnsname>"
    databaseName = "<db_name>"
    tableName = args.table
    oracle_datasource_example(spark)
    spark.stop()


