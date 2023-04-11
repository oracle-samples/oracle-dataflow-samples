"""
A simple example demonstrates the usage of oci metastore and oracle datasource.
"""
import argparse


from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


def oracle_datasource_example(spark):

    #properties = {"adbId": ADB_ID, "user": USER, "password": PASSWORD}
    properties = {"walletUri": walletUri, "user": USER, "password": PASSWORD}

    print("Step 1: Read csv from object storage")
    src_df = spark.read.options(delimiter=',').option("header",True).csv(INPUT_PATH)
    dataList = []

    print("Reading data from object storage !")
    src_df.show()
    print("================================================================================================")

    print("Step 2: Write csv data schema into Metastore")

    spark.sql("use " + databaseName)
    spark.sql("show tables").show(30)

    spark.sql("create database IF NOT EXISTS " + databaseName)
    print("Successfully created database: " + databaseName)

    metaSchema = src_df.schema

    for i, cols in enumerate(metaSchema.fields):
        dataList.append(cols.name)
    schemaDf = spark.createDataFrame(dataList, StringType())
    schemaDf.show(truncate=False)

    schemaDf.write.mode("overwrite").saveAsTable(databaseName + "." + tableName)
    print("Wrote data schema in Database: " + databaseName + " ; table: " + tableName)

    print("Read data schema from Metastore")
    tableDf = spark.sql("select * from " + databaseName + "." + tableName)
    print("Reading data from metastore !")
    tableDf.show()
    # Note: providing connectionId is optional with adbId
    print("================================================================================================")
    print("Step 3: Writing data into ADW");
    src_df.write.format("oracle") \
        .options(**properties).option("dbtable",tableName) \
        .option("connectionId", CONNECTION_ID) \
        .mode("Overwrite") \
        .save()

    print("Reading data from ADW -> ")
    adwDf = spark.read.format("oracle").options(**properties).option("dbtable",tableName).option("connectionId", CONNECTION_ID).load();
    adwDf.show();

class customCol:
    def __init__(self, name, dataType):
        self.name = name
        self.dataType = dataType

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--walletUri", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--connection", required=True)
    parser.add_argument("--database", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Python Spark Oracle Datasource Example").enableHiveSupport().getOrCreate()


    # TODO: Set all these variables.
    INPUT_PATH = args.input
    walletUri = args.walletUri
    USER = args.user
    PASSWORD = args.password
    CONNECTION_ID = args.connection
    databaseName = args.database
    tableName = args.table
    oracle_datasource_example(spark)
    spark.stop()
