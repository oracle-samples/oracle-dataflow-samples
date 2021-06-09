package example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/*
 * This example shows converting CSV to Parquet in Spark.
 * 
 * Before you begin, upload fake_data.csv from the sample bundle and customize the paths.
 * 
 * In OCI CLI run:
 * oci os object put --bucket-name <bucket> --file fake_data.csv
 *
 */

public class Example {
	// Customize these before you start.
	private static String inputPath = "oci://<bucket>@<tenancy>/fake_data.csv";
	private static String outputPath = "oci://<bucket>@<tenancy>/fake_data.parquet";

	public static void main(String[] args) throws Exception {
		// Get our Spark session.
		SparkSession spark = DataFlowSparkSession.getSparkSession("Sample App");

		Dataset<Row> df = spark.read().csv(inputPath);
		df.write().mode(SaveMode.Overwrite).format("parquet").save(outputPath);
		System.out.println("Conversion to Parquet complete.");

		spark.stop();
	}
}
