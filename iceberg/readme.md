# Iceberg Support on OCI Data Flow

Oracle Cloud Infrastructure (OCI) [Data Flow](https://www.oracle.com/in/big-data/data-flow/) is a fully managed Apache Spark service that performs processing tasks on extremely large datasets—without infrastructure to deploy or manage. Developers can also use Spark Streaming to perform cloud ETL on their continuously produced streaming data. This enables rapid application delivery because developers can focus on app development, not infrastructure management.
To know more get started [here](https://docs.oracle.com/en-us/iaas/data-flow/using/dfs_getting_started.htm)

[Iceberg](https://iceberg.apache.org/)  Iceberg is a high-performance format for huge analytic tables. Iceberg brings the reliability and simplicity of SQL tables to big data, while making it possible for engines like Spark. 

Major features are Expressive SQL, Full Schema Evolution, Hidden Partitioning, Time Travel & Rollback, Data Compaction, ACID, optimistic concurrency, idempotent sinks and replayable sources, data format (Parquet, ORC, Avro)

Iceberg 1.1.0 can be integrated in Data Flow Spark 3.2.1 processing engine. To use this feature please
Select your application’s Spark version to Spark 3.2.1 from console, or cli.
Use `iceberg`  format as mentioned in the documentations. Iceberg release [notes](https://github.com/apache/iceberg/releases/tag/apache-iceberg-1.1.0) and [documentation](https://iceberg.apache.org/docs/latest/spark-ddl/) for further understanding.

OCI Data Flow Spark engine can be used to read/write `iceberg` format.

Java/Scala

    data.writeTo("dev.db.iceberg_table").append()
    
    val df1 = spark.read.format("iceberg").load("dev.db.iceberg_table")
    
    val df2 = spark.read
    .format("iceberg")
    .option("as-of-timestamp", "1676461234685")
    .load("dev.db.iceberg_table")

Python

    original_df.write.format("iceberg").mode("append").saveAsTable("dev.db.icebergTablePy")

    deltaDF = spark.read.format("iceberg").load(icebergTablePath);
    deltaDF.show()

    df1 = spark.read.format("iceberg").option("as-of-timestamp", "1676563842807").load(icebergTablePath)
    df1.show()

    df2 = spark.read.format("iceberg").option("snapshot-id", 3004923173007188796).load(icebergTablePath)
    df2.show()
    


SQL

    val data = Seq(
      ("100", "2023-01-01", "2023-01-01T13:51:39.340396Z"),
      ("101", "2023-01-01", "2023-01-01T12:14:58.597216Z"),
      ("102", "2023-01-01", "2023-01-01T13:51:40.417052Z"),
      ("103", "2023-01-01", "2023-01-01T13:51:40.519832Z")).toDF("id", "creation_date", "last_update_time")

    spark.sql(
      """CREATE TABLE IF NOT EXISTS dev.db.iceberg_table (id string,
    creation_date string,
    last_update_time string)
    USING iceberg
    location 'oci://<bucket>@<namespace>/iceberg/db/iceberg_table'""")

    spark.sql("SELECT * FROM dev.db.iceberg_table").show()
    spark.sql("SELECT * FROM dev.db.iceberg_table.files").show()
    spark.sql("SELECT * FROM dev.db.iceberg_table.snapshots").show()

### Here are the Sample code to start using iceberg on Data Flow

1. Iceberg Java/Scala sample operations [Main](https://github.com/oracle-samples/oracle-dataflow-samples/blob/main/iceberg/scala/src/main/scala/com/oracle/iceberg/Main.scala)

```
Main class : com.oracle.iceberg.Main
Arguments: oci://<location>/samplecsv.csv oci://<location>/iceberg/spark-IcebergTable 
```

2. Iceberg Python sample operations [Iceberg_sample](https://github.com/oracle-samples/oracle-dataflow-samples/blob/main/iceberg/python/iceberg_sample.py)

```
Main class : iceberg_sample.py
Arguments: oci://<location>/samplecsv.csv oci://<location>/iceberg/spark-IcebergTable 
```

`Note: Build jar artifact from "mvn clean install`