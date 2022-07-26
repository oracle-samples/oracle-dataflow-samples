# Delta Lake Support on OCI Data Flow

Oracle Cloud Infrastructure (OCI) [Data Flow](https://www.oracle.com/in/big-data/data-flow/) is a fully managed Apache Spark service that performs processing tasks on extremely large datasets—without infrastructure to deploy or manage. Developers can also use Spark Streaming to perform cloud ETL on their continuously produced streaming data. This enables rapid application delivery because developers can focus on app development, not infrastructure management.
To know more get started [here](https://docs.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm)

[Delta Lake](https://delta.io/)  enables building a Lakehouse architecture on top of data lakes. Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing on top of existing data lakes.

Delta Lake 1.2.1 now integrated in Data Flow Spark 3.2.1 processing engine. To use this feature please
Select your application’s Spark version to Spark 3.2.1 from console, or cli.
Use `delta`  format as mentioned in the documentations. Delta Lake release [notes](https://github.com/delta-io/delta/releases/tag/v1.2.1) and [documentation](https://docs.delta.io/latest/delta-intro.html#) for further understanding. Below are some sample usage

Running Delta Lake api is as easy as any other format. Data Flow Spark engine support `delta` by format default. Delta Lake API are available in Java/Scala/Python languages, Include delta-spark package by `pip install delta-spark` If you are using custom archive.zip dependency packager.  

Java/Scala
> spark.read().format("delta").load(deltaTablePath)
> 
> df.write().format("delta").save(deltaTablePath)
> 
> val deltaTable = io.delta.tables.DeltaTable.forPath(spark, deltaTablePath)
>
> deltaTable.vacuum()

Python
> spark.read.format("delta").option("versionAsOf", 1).load(deltaTablePath)
>
> from delta.tables import *
>
> deltaTable = DeltaTable.forPath(spark, deltaTablePath)
> 
> deltaTable.vacuum()
> 
> deltaTable.history()

SQL
> spark.sql("CONVERT TO DELTA parquet.`" + parquetTablePath + "`");
> 
> spark.sql("DESCRIBE HISTORY delta.`" + deltaTablePath + "`");


### Here are the Sample code to start using Delta Lake on Data Flow

1. Delta Lake Java/Scala sample operations [Main](https://github.com/oracle/oracle-dataflow-samplesdeltalake/deltalake/scala/src/main/scala/com/oracle/delta/Main)

```
Main class : com.oracle.delta.Main
Arguments: oci://<location>/samplecsv.csv oci://<location>/delta/spark-DeltaTable oci://<location>/delta/spark-ParquetToDeltaTable
```

2. Delta Lake Python sample operations [delta_lake_sample](https://github.com/oracle/oracle-dataflow-samplesdeltalake/deltalake/python/delta_lake_sample.py)

```
Main class : delta_lake_sample.py
Arguments: oci://<location>/samplecsv.csv oci://<location>/delta/spark-DeltaTable oci://<location>/delta/spark-ParquetToDeltaTable
```
3. Delta Lake Streaming sample operations [DeltaStreamRun](https://github.com/oracle/oracle-dataflow-samplesdeltalake/deltalake/scala/src/main/scala/com/oracle/delta/DeltaStreamRun)

```
Main class : com.oracle.delta.DeltaStreamRun
Arguments: oci://<delta-table-location> oci://<delta-table-location>/another-spark-DeltaTable oci://<location-to-checkoint-folder>/
```
4. Delta Lake sample batch write operation simulation [LongRunDelta](https://github.com/oracle/oracle-dataflow-samplesdeltalake/deltalake/scala/src/main/scala/com/oracle/delta/LongRunDelta)

```
Main class : com.oracle.delta.LongRunDelta
Arguments: oci://<location>/samplecsv.csv oci://<location>/delta/spark-DeltaTable oci://<location>/delta/spark-ParquetToDeltaTable <sleepTimeInSec> <totalRuns>
```
4. CSV sample data generation [GenerateCSVData](https://github.com/oracle/oracle-dataflow-samplesdeltalake/deltalake/scala/src/main/scala/com/oracle/delta/GenerateCSVData)

```
Main class : com.oracle.delta.GenerateCSVData
Arguments: oci://<location>/samplecsv.csv oci://<location>/delta/spark-DeltaTable oci://<location>/delta/spark-ParquetToDeltaTable <sleepTimeInSec> <totalRuns>
```
`Note: Build jar artifact from "mvn clean install`