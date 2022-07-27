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


if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("DeltaTable Simulation")\
        .getOrCreate()

    csvFilePath = sys.argv[1]
    deltaTablePath = sys.argv[2]
    parquetTablePath = sys.argv[3]


    original_df = spark.read.format("csv").option("header", "true").load(csvFilePath).withColumn("time_stamp", current_timestamp())
    original_df.write.partitionBy("vendor_id").format("delta").mode("overwrite").save(deltaTablePath)
    original_df.write.mode("overwrite").parquet(parquetTablePath)

    print("\nStarting Spark delta operations on " + deltaTablePath)
    print("\nHistory for " + deltaTablePath)
    deltaDF = spark.read.format("delta").load(deltaTablePath);
    deltaDF.show()

    print("\nHistory for timestampAsOf 2022-04-11 14:08:02" + deltaTablePath)
    df1 = spark.read.format("delta").option("timestampAsOf", "2022-06-21T15:57:12.000Z").load(deltaTablePath)
    df1.show()

    print("\nTime travel on SQL")
    deltaDF.createOrReplaceTempView("my_table")
    #df2 = spark.sql("SELECT count(*) FROM my_table TIMESTAMP AS OF '2022-06-21 15:57:12.000'")
    #df2.show()
    #df2_2 = spark.sql("SELECT count(*) FROM my_table@v1")
    #df2_2.show()
    # https://github.com/delta-io/delta/issues/634  https://github.com/delta-io/delta/issues/1242 https://github.com/delta-io/delta/issues/128
    # VERSION AS OF in SQL command is not supported in OSS Delta Lake because Spark's SQL parser doesn't support it yet.
    # As a workaround, you can use the DataFrame APIs to do the time travel.
    

    df2_1 = spark.sql("SELECT count(*) FROM delta.`" + deltaTablePath + "@v1`")
    df2_1.show()


    print("\nHistory for versionAsOf 3" + deltaTablePath)
    df3 = spark.read.format("delta").option("versionAsOf", 1).load(deltaTablePath)
    df3.show()

    print("\nStarting SQL delta operations on " + deltaTablePath)
    print("\nRunning VACUUM for " + deltaTablePath)
    vacuum = spark.sql("VACUUM delta.`" + deltaTablePath + "`");
    vacuum.show()

    describe = spark.sql("DESCRIBE HISTORY delta.`" + deltaTablePath + "`");
    describe.show()


    spark.sql("CONVERT TO DELTA parquet.`" + parquetTablePath + "`");



    print("\nStarting SQL DeltaTable operations on " + deltaTablePath)
    from delta.tables import *
    deltaTable = DeltaTable.forPath(spark, deltaTablePath)
    deltaTable.vacuum()

    fullHistoryDF = deltaTable.history()
    print("\nHistory for " + deltaTablePath)
    fullHistoryDF.show()

    print("\nDelta Job Done!!!")
