#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SQLContext


if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession \
        .builder \
        .appName("Iceberg Simulation") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.dev", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.hadoop.fs.AbstractFileSystem.oci.impl", "com.oracle.bmc.hdfs.Bmc") \
        .config("spark.sql.catalog.dev.type", "hadoop") \
        .config("spark.sql.catalog.dev.warehouse", "oci://mk-test-dev18@paasdevssstest/iceberg/") \
        .getOrCreate()


    sqlContext = SQLContext(spark.sparkContext)

    csvFilePath = sys.argv[1]
    icebergTablePath = sys.argv[2]
    parquetTablePath = sys.argv[3]

    original_df = spark.read.format("csv").option("header", "true").load(csvFilePath).withColumn("time_stamp", current_timestamp())
    original_df.write.format("iceberg").mode("append").saveAsTable("dev.db.icebergTablePy")

    print("\nStarting Spark iceberg operations on " + icebergTablePath)
    print("\nHistory for " + icebergTablePath)
    deltaDF = spark.read.format("iceberg").load(icebergTablePath);
    deltaDF.show()

    print("\nHistory for timestampAsOf 1676563842807" + icebergTablePath)
    df1 = spark.read.format("iceberg").option("as-of-timestamp", "1676563842807").load(icebergTablePath)
    df1.show()

    print("\nHistory for snapshot-id" + icebergTablePath)
    # provide valid snapshot id from the metadata.json file
    df2 = spark.read.format("iceberg").option("snapshot-id", 3004923173007188796).load(icebergTablePath)
    df2.show()

    t2 = spark.read \
        .format("iceberg") \
        .option("as-of-timestamp", "1676563842807") \
        .load("dev.db.icebergTablePy")

    t2.show()

    spark.sql("SELECT * FROM dev.db.icebergTablePy").show()
    spark.sql("SELECT * FROM dev.db.icebergTablePy.files").show()
    #spark.sql("SELECT * FROM dev.db.iceberg VERSION AS OF 142414061441660886L").show() Spark 3.3 and above
    spark.sql("SELECT * FROM dev.db.icebergTablePy.history").show() # supported only in spark 3.2

    table = spark.table("dev.db.icebergTablePy")
    print("Total Rows " + str(table.count()))

    print("Iceberg Job Done!!!")
