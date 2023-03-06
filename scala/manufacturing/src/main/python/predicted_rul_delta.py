import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":
    """
        Usage: RUL Delta Table
    """
spark = SparkSession \
    .builder \
    .appName("RUL delta table operations") \
    .getOrCreate()

deltaTablePath = sys.argv[1]
print("\nHistory for " + deltaTablePath)
deltaDF = spark.read.format("delta").load(deltaTablePath)
deltaDF.show()

print("\nTime travel on SQL")
deltaDF.createOrReplaceTempView("my_table")
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

print("\nStarting SQL DeltaTable operations on " + deltaTablePath)
from delta.tables import *
deltaTable = DeltaTable.forPath(spark, deltaTablePath)
deltaTable.vacuum()

fullHistoryDF = deltaTable.history()
print("\nHistory for " + deltaTablePath)
fullHistoryDF.show()

print("\nDelta Job Done!!!")