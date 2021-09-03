#!/usr/bin/env python3
# Copyright © 2021, Oracle and/or its affiliates. 
# The Universal Permissive License (UPL), Version 1.0 as shown at https://oss.oracle.com/licenses/upl.

import os
import sys
import traceback

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *


def main():

   # Read the Input JSON files
    YELP_REVIEW_INPUT_PATH = sys.argv[1]
    YELP_BUSINESS_INPUT_PATH = sys.argv[2]
    db_name = sys.argv[3]

   # Create the path for the parquet files

    YELP_REVIEW_OUTPUT_PATH = os.path.dirname(
        YELP_REVIEW_INPUT_PATH) + "/parquet"

    YELP_BUSINESS_OUTPUT_PATH = os.path.dirname(
        YELP_BUSINESS_INPUT_PATH) + "/parquet"

    # Set up Spark.
    # Set up Spark.
    spark_session = get_dataflow_spark_session()

    # Load the review data into dataframe Merging different schemas as we read.

    review_input_dataframe = spark_session.read.option("header", "true").option(
        "mergeSchema", "true").json(YELP_REVIEW_INPUT_PATH)

    # Flatten the schema
    review_input_dataframe = flatten(review_input_dataframe)

    # load the business data into dataframe. Merging different schemas as we read
    business_input_dataframe = spark_session.read.option("header", "true").option(
        "mergeSchema", "true").json(YELP_BUSINESS_INPUT_PATH)

    # Flatten the schema
    business_input_dataframe = flatten(business_input_dataframe)

    business_input_dataframe = cleanseData(business_input_dataframe)

    # Create Hive External Table
    createMetaStoreStoreTable(spark_session, review_input_dataframe,
                              business_input_dataframe, db_name)


# Generic function to flatten the JSON files
def flatten(df):
   # compute Complex Fields (Lists and Structs) in Schema
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print("Processing :"+col_name+" Type : " +
              str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if (type(complex_fields[col_name]) == StructType):
            expanded = [col(col_name+'.'+k).alias(col_name+'_'+k)
                        for k in [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif (type(complex_fields[col_name]) == ArrayType):
            df = df.withColumn(col_name, explode_outer(col_name))

        # recompute remaining Complex Fields in Schema
        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    return df


def cleanseData(df):
    print(f'Count of rows in Yelp Business {df.count()} .')
    df = df.filter(df.is_open != '1')
    print(f'Count of rows in Yelp Business after dropping the restaurants that are closed {df.count()} .')

    return df


# Read the config files


def get_dataflow_spark_session(app_name="DataFlow", file_location=None, profile_name=None, spark_config={}):
    """
    Get a Spark session in a way that supports running locally or in Data Flow.
    """
    if in_dataflow():
        spark_builder = SparkSession.builder.appName(app_name)
    else:
        # Import OCI.
        try:
            import oci
        except:
            raise Exception(
                "You need to install the OCI python library to test locally"
            )
        # Use defaults for anything unset.
        if file_location is None:
            file_location = oci.config.DEFAULT_LOCATION
        if profile_name is None:
            profile_name = oci.config.DEFAULT_PROFILE

        # Load the config file.
        try:
            oci_config = oci.config.from_file(
                file_location=file_location, profile_name=profile_name
            )
        except Exception as e:
            print("You need to set up your OCI config properly to run locally")
            raise e
        conf = SparkConf()
        conf.set("fs.oci.client.auth.tenantId", oci_config["tenancy"])
        conf.set("fs.oci.client.auth.userId", oci_config["user"])
        conf.set("fs.oci.client.auth.fingerprint", oci_config["fingerprint"])
        conf.set("fs.oci.client.auth.pemfilepath", oci_config["key_file"])
        conf.set(
            "fs.oci.client.hostname",
            "https://objectstorage.{0}.oraclecloud.com".format(
                oci_config["region"]),
        )
        spark_builder = SparkSession.builder.appName(
            app_name).config(conf=conf)

    # Add in extra configuration.
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # Create the Spark session.
    session = spark_builder.enableHiveSupport().getOrCreate()
    return session


def in_dataflow():
    """
    Determine if we are running in OCI Data Flow by checking the environment.
    """
    if os.environ.get("HOME") == "/home/dataflow":
        return True
    return False


def createMetaStoreStoreTable(spark_session, review_input_dataframe, business_input_dataframe, db_name):

    try:
        spark_session.sql("CREATE DATABASE IF NOT EXISTS " + db_name)
    except:
        print(traceback.format_exc())
        print(traceback.print_stack())

    spark_session.sql("USE " + db_name)

    try:

     # ddl = build_hive_ddl(review_input_dataframe, "yelp_review",
     # YELP_REVIEW_OUTPUT_PATH)
        # Create the yelp_review external table
     # spark_session.sql(ddl)

        # Save the dataframe to the table to table
        review_input_dataframe.write.format("parquet").mode("overwrite").saveAsTable(
            db_name + ".yelp_review")
    except Exception as err:
        print(traceback.format_exc())
        print(traceback.print_stack())

    try:

        # ddl = build_hive_ddl(
        # business_input_dataframe, "yelp_business_raw", YELP_BUSINESS_OUTPUT_PATH)

        # Create the yelp_business_raw external input table
        # spark_session.sql(ddl)
        # Save the dataframe to the table to table
        business_input_dataframe.write.mode("overwrite").saveAsTable(
            db_name + ".yelp_business_raw")
    except Exception as err:
        print(traceback.format_exc())
        print(traceback.print_stack())

    spark_session.sql("SHOW Tables").show()
    spark_session.sql("DESCRIBE TABLE yelp_review").show()
    spark_session.sql("DESCRIBE TABLE yelp_business_raw").show()

    # Create a view for Data analysis
    ddl = 'CREATE OR REPLACE VIEW yelp_data AS SELECT business.name, year(date),business.state, business.attributes_Alcohol alcohol,attributes_GoodForKids goodForKids, business.attributes_RestaurantsTakeOut takeout, avg(review.stars) average_star FROM ' \
          + db_name + '.yelp_review review,' + db_name + '.yelp_business_raw business' \
          + ' WHERE business.business_id = review.business_id GROUP BY business.name, year(date),business.state,business.attributes_Alcohol,business.attributes_GoodForKids, business.attributes_RestaurantsTakeOut ORDER BY average_star DESC '

    print("view ddl" + ddl)

    try:
        spark_session.sql(ddl)
    except Exception as err:

        print(traceback.format_exc())
        print(traceback.print_stack())


if __name__ == "__main__":
    main()
