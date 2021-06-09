package example;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;

/*
 * Data Flow helper to create a Spark session.
 * 
 * If running locally, the Spark session is configured to access OCI using an API key.
 */
public class DataFlowSparkSession {
	public static boolean isRunningInDataFlow() {
		if (System.getenv("HOME").equals("/home/dataflow")) {
			return true;
		}
		return false;
	}

	public static SparkSession getSparkSession(String appName) throws IOException {
		return getSparkSession(appName, ConfigFileReader.DEFAULT_FILE_PATH, "DEFAULT", null);
	}

	public static SparkSession getSparkSession(String appName, Map<String, String> conf) throws IOException {
		return getSparkSession(appName, ConfigFileReader.DEFAULT_FILE_PATH, "DEFAULT", conf);
	}

	public static SparkSession getSparkSession(String appName, String localConfigurationFilePath,
			Map<String, String> conf) throws IOException {
		return getSparkSession(appName, localConfigurationFilePath, "DEFAULT", conf);
	}

	public static SparkSession getSparkSession(String appName, String localConfigurationFilePath, String localProfile,
			Map<String, String> conf) throws IOException {
		Builder builder = SparkSession.builder().appName(appName);
		if (!isRunningInDataFlow()) {
			// Set up to run locally.
			builder.master("local[*]");
			ConfigFileAuthenticationDetailsProvider authenticationDetailsProvider = new ConfigFileAuthenticationDetailsProvider(
					localConfigurationFilePath, localProfile);
			builder.config("fs.oci.client.auth.tenantId", authenticationDetailsProvider.getTenantId());
			builder.config("fs.oci.client.auth.userId", authenticationDetailsProvider.getUserId());
			builder.config("fs.oci.client.auth.fingerprint", authenticationDetailsProvider.getFingerprint());
			String guessedPath = new File(localConfigurationFilePath).getParent() + File.separator + "oci_api_key.pem";
			builder.config("fs.oci.client.auth.pemfilepath", guessedPath);

			String region = authenticationDetailsProvider.getRegion().getRegionId();
			String hostName = MessageFormat.format("https://objectstorage.{0}.oraclecloud.com",
					new Object[] { region });
			builder.config("fs.oci.client.hostname", hostName);
		}

		// Merge in additional configuration, if provided.
		if (conf != null) {
			for (Entry<String, String> x : conf.entrySet()) {
				builder.config(x.getKey(), x.getValue());
			}
		}
		return builder.getOrCreate();
	}

	/*
	 * Get configurations needed to instantiate an authenticated BMC HDFS client.
	 */
	public static Configuration getBmcConfiguration(SparkSession spark) throws RuntimeException {
		SparkConf conf = spark.sparkContext().getConf();
		Configuration config = new Configuration();
		List<String> keysToCopy;
		if (isRunningInDataFlow()) {
			keysToCopy = Arrays.asList("fs.oci.client.auth.delegationTokenPath", "fs.oci.client.custom.client",
					"fs.oci.client.custom.authenticator", "fs.oci.client.hostname");
		} else {
			keysToCopy = Arrays.asList("fs.oci.client.auth.tenantId", "fs.oci.client.auth.userId",
					"fs.oci.client.auth.fingerprint", "fs.oci.client.auth.pemfilepath", "fs.oci.client.hostname");
		}
		for (String entry : keysToCopy) {
			String configuration = conf.get(entry, "");
			if ("".equals(configuration)) {
				configuration = conf.get("spark.hadoop." + entry, "");
			}
			if ("".equals(configuration)) {
				throw new RuntimeException("Missing configuration " + entry);
			}

			config.set(entry, configuration);
		}
		return config;
	}
}
