package example.adw;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.Read;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class MetastoreToADW {

  public static void main(String args[]) {
    SparkSession spark = SparkSession.builder().enableHiveSupport().getOrCreate();
    String OS_BUCKET = "oci://<bucket>@<namespace>/";
    String relativeInputPath = "<object storage location>";
    String relativeOutputPath = "<object storage location>";
    String databaseName = "<dbName>";
    String tableName = "<table Name>";
    //var adwDetailsObj: ADWDetails = null
    System.out.println("Received args -> ");
    System.out.println(String.join(" ", args));
    ADWDetails adwDetailsObj = null;
    if (args.length > 0) {
      OS_BUCKET = args[0].trim();
      relativeInputPath = args[1].trim();
      relativeOutputPath = args[2].trim();
      if (args.length > 3) {
        databaseName = args[3].trim();
        tableName = args[4].trim();
      }
      if (args.length > 5) {
        adwDetailsObj = new ADWDetails();
        adwDetailsObj.walletPath = args[5].trim();
        adwDetailsObj.user = args[6].trim();
        adwDetailsObj.tnsName = args[7].trim();
        adwDetailsObj.secretValue = args[8].trim();
        System.out.println("ADW details received: " + adwDetailsObj.toString());
      }
    }
    System.out.println("OS_BUCKET -> " + OS_BUCKET);
    if (!OS_BUCKET.endsWith("/")) {
      OS_BUCKET = OS_BUCKET + "/";
    }
    // Use Case 1: Read csv from object storage
    System.out.println("Step 1: Read csv from object storage");
    Dataset<Row> df = spark.read().option("header", "true").csv(OS_BUCKET + relativeInputPath);
    System.out.println("Reading data from object storage !");
    df.show(false);
    System.out.println(
        "================================================================================================");
    // Use Case 2: Write csv data into Metastore
    System.out.println("Step 2: Write csv data into Metastore");
    spark.sql("show databases").show(30, false);
    Dataset<Row> databasesDf = spark.sql("show databases");
    if (databasesDf.filter(org.apache.spark.sql.functions.col("namespace")
        .contains(org.apache.spark.sql.functions.lit(databaseName))).count() > 0) {
      System.out.println("Database: " + databaseName + " present !");
    } else {
      System.out.println("Database: " + databaseName + " absent, creating !");
      spark.sql("create database IF NOT EXISTS " + databaseName);
      System.out.println("Successfully created database: " + databaseName);
      System.out.println("List of databases -> ");
      databasesDf.show(false);
    }
    spark.sql("use " + databaseName);
    spark.sql("show tables").show(30, false);
    df.write().mode("overwrite").saveAsTable(databaseName + "." + tableName);
    System.out.println("Wrote data in Database: " + databaseName + " ; table: " + tableName);
    System.out.println(
        "================================================================================================");
    // Use Case 3: Read data from Metastore and write into ADW
    System.out.println("Step 3: Read data from Metastore and write into ADW");
    Dataset<Row> tableDf = spark.sql("select * from " + databaseName + "." + tableName);
    System.out.println("Reading data from metastore !");
    tableDf.show(false);
    if (!spark.conf().getAll().contains("spark.oracle.datasource.enabled") ||
        spark.conf().get("spark.oracle.datasource.enabled").equalsIgnoreCase("false")) {
      return;
    }
    System.out.println("Writing data into ADW");
    tableDf.write().format("oracle")
        //.option("walletUri", adwDetailsObj.getWalletPath())
        .option("adbId", adwDetailsObj.getWalletPath())
        .option("connectionId", adwDetailsObj.getTnsName())
        .option("dbtable", "sample")
        .option("user", adwDetailsObj.getUser())
        .option("password", adwDetailsObj.getSecretValue())
        .mode("Overwrite").save();
    System.out.println("Reading data from ADW -> ");
    Dataset<Row> adwDf = spark.read().format("oracle")
        //.option("walletUri", adwDetailsObj.getWalletPath())
        .option("adbId", adwDetailsObj.getWalletPath())
        .option("connectionId", adwDetailsObj.getTnsName())
        .option("dbtable", "sample")
        .option("user", adwDetailsObj.getUser())
        .option("password", adwDetailsObj.getSecretValue())
        .load();
    adwDf.show(false);
  }

  static class ADWDetails {

    public ADWDetails() {
    }

    public String getWalletPath() {
      return walletPath;
    }

    public void setWalletPath(String walletPath) {
      this.walletPath = walletPath;
    }

    public String getTnsName() {
      return tnsName;
    }

    public void setTnsName(String tnsName) {
      this.tnsName = tnsName;
    }

    public String getUser() {
      return user;
    }

    public void setUser(String user) {
      this.user = user;
    }

    public String getSecretValue() {
      return secretValue;
    }

    public void setSecretValue(String secretValue) {
      this.secretValue = secretValue;
    }

    String walletPath;
    String tnsName;
    String user;
    String secretValue;
  }
}