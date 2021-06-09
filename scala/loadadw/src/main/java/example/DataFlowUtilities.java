package example;

import java.io.IOException;

import org.apache.spark.sql.SparkSession;

import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.http.ClientConfigurator;
import com.oracle.bmc.http.DefaultConfigurator;

public class DataFlowUtilities {
	public static String getTempDirectory() {
		if (DataFlowSparkSession.isRunningInDataFlow()) {
			return "/opt/spark/work-dir/";
		}
		return System.getProperty("java.io.tmpdir");
	}

	public static String getDelegationTokenPath(SparkSession spark) {
		if (!DataFlowSparkSession.isRunningInDataFlow()) {
			return null;
		}
		return spark.sparkContext().getConf().get("spark.hadoop.fs.oci.client.auth.delegationTokenPath");
	}

	public static AbstractAuthenticationDetailsProvider getAuthenticationDetailsProvider() throws IOException {
		return getAuthenticationDetailsProvider(ConfigFileReader.DEFAULT_FILE_PATH, "DEFAULT");
	}

	public static AbstractAuthenticationDetailsProvider getAuthenticationDetailsProvider(
			String localConfigurationFilePath) throws IOException {
		return getAuthenticationDetailsProvider(localConfigurationFilePath, "DEFAULT");
	}

	public static AbstractAuthenticationDetailsProvider getAuthenticationDetailsProvider(
			String localConfigurationFilePath, String localProfile) throws IOException {
		if (DataFlowSparkSession.isRunningInDataFlow()) {
			// Use our delegation token.
			return InstancePrincipalsAuthenticationDetailsProvider.builder().build();
		} else {
			// We are running outside of Data Flow, use our API key.
			return new ConfigFileAuthenticationDetailsProvider(localConfigurationFilePath, localProfile);
		}
	}

	public static ClientConfigurator getClientConfigurator(String tokenPath) {
		if (DataFlowSparkSession.isRunningInDataFlow()) {
			return new OboTokenClientConfigurator(tokenPath);
		}
		return new DefaultConfigurator();
	}

}
