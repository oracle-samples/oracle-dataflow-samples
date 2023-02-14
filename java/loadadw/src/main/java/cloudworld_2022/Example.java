package cloudworld_2022;

import oracle.jdbc.driver.OracleConnection;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Example {
  public static void main(String[] args) throws Exception {
    String walletPath = args[0] + "/" + args[1]; //OCI_URI- URI of bucket where you have uploaded wallet
    String user = args[2];
    String tnsName = args[3]; // this can be found inside of the wallet.zip (unpack it)
    byte[] secretValueDecoded = args[4].getBytes();

    // Get our Spark session.
    SparkConf conf = new SparkConf();
    String master = conf.get("spark.master", "local[*]");
    SparkSession spark = SparkSession.builder().appName("Example").master(master).getOrCreate();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

    // Build a 2 row data set to save to ADW.
    // Usually you would load data from CSV/Parquet, this is to keep the example
    // simple.
    List<String[]> stringAsList = new ArrayList<>();
    stringAsList.add(new String[] { "value11", "value21" });
    stringAsList.add(new String[] { "value12", "value22" });
    JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
    JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map(RowFactory::create);
    StructType schema = DataTypes
        .createStructType(new StructField[] { DataTypes.createStructField("col1", DataTypes.StringType, false),
            DataTypes.createStructField("col2", DataTypes.StringType, false) });
    Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();

    // Save data to ADW.
    System.out.println("Saving to ADW");

    //Preparing options
    Map<String, String> options = new HashMap<String, String>();
    options.put("walletUri",walletPath);
    options.put("connectionId",tnsName);
    options.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, user);
    options.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, new String(secretValueDecoded));
    options.put("dbtable", "sample");

    //Writing data to ADW
    df.write().format("oracle").options(options).mode("Overwrite").save();
    System.out.println("Done writing to ADW");

    //Reading data from ADW
    spark.read().format("oracle").options(options).load().show();

    //Stopping spark context
    jsc.stop();
  }
}