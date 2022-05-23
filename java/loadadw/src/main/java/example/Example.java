package example;

import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider;
import com.oracle.bmc.http.ClientConfigurator;
import com.oracle.bmc.secrets.Secrets;
import com.oracle.bmc.secrets.SecretsClient;
import com.oracle.bmc.secrets.model.Base64SecretBundleContentDetails;
import com.oracle.bmc.secrets.requests.GetSecretBundleRequest;
import com.oracle.bmc.secrets.responses.GetSecretBundleResponse;

import oracle.jdbc.OracleConnection;

/*
 * This example shows shows a simple example writing to ADW using JDBC.
 *
 * Process:
 * * Download and extract a wallet from object store.
 * * Access a secret from OCI Vault.
 * * Create a simple dataset.
 * * Write the dataset to ADW using JDBC.
 *
 */

public class Example {
	// Customize these before you start.
	private static String walletPath = "oci://<bucket>@<tenancy>/wallet.zip";
	private static String user = "ADMIN";
	private static String passwordOcid = "ocid1.vaultsecret.oc1.iad.<secret_ocid>";
	private static String tnsName = "<tnsname>";

	public static void main(String[] args) throws Exception {
		// Get our Spark session.
		SparkSession spark = DataFlowSparkSession.getSparkSession("Sample App");

		// Build a 2 row data set to save to ADW.
		// Usually you would load data from CSV/Parquet, this is to keep the example
		// simple.
		List<String[]> stringAsList = new ArrayList<>();
		stringAsList.add(new String[] { "value11", "value21" });
		stringAsList.add(new String[] { "value12", "value22" });
		JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
		JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map((String[] row) -> RowFactory.create(row));
		StructType schema = DataTypes
				.createStructType(new StructField[] { DataTypes.createStructField("col1", DataTypes.StringType, false),
						DataTypes.createStructField("col2", DataTypes.StringType, false) });
		Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();

		// Download the wallet from object storage and distribute it.
		String tmpPath = DataFlowDeployWallet.deployWallet(spark, walletPath);

		// Configure the ADW JDBC URL.
		String jdbcUrl = MessageFormat.format("jdbc:oracle:thin:@{0}?TNS_ADMIN={1}", new Object[] { tnsName, tmpPath });
		System.out.println("JDBC URL " + jdbcUrl);

		// Get the password from the secrets service.
		String tokenPath = DataFlowUtilities.getDelegationTokenPath(spark);
		AbstractAuthenticationDetailsProvider provider = DataFlowUtilities.getAuthenticationDetailsProvider();
		ClientConfigurator configurator = DataFlowUtilities.getClientConfigurator(tokenPath);
		String password = getSecret(tokenPath, passwordOcid, provider, configurator);

		// Save data to ADW.
		System.out.println("Saving to ADW");
		Map<String, String> options = new HashMap<String, String>();
		options.put("driver", "oracle.jdbc.driver.OracleDriver");
		options.put("url", jdbcUrl);
		options.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, user);
		options.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, password);
		options.put(OracleConnection.CONNECTION_PROPERTY_TNS_ADMIN, tmpPath);
		options.put("dbtable", "sample");
		df.write().format("jdbc").options(options).mode("Overwrite").save();
		System.out.println("Done writing to ADW");

		spark.stop();
	}

	public static String getSecret(String tokenPath, String secretOcid, AbstractAuthenticationDetailsProvider provider,
			ClientConfigurator configurator) throws Exception {
		Secrets client = SecretsClient.builder().clientConfigurator(configurator).build(provider);

		GetSecretBundleRequest getSecretBundleRequest = GetSecretBundleRequest.builder().secretId(secretOcid)
				.stage(GetSecretBundleRequest.Stage.Current).build();
		GetSecretBundleResponse getSecretBundleResponse = client.getSecretBundle(getSecretBundleRequest);
		Base64SecretBundleContentDetails base64SecretBundleContentDetails = (Base64SecretBundleContentDetails) getSecretBundleResponse
				.getSecretBundle().getSecretBundleContent();
		byte[] secretValueDecoded = Base64.getDecoder().decode(base64SecretBundleContentDetails.getContent());
		String secret = new String(secretValueDecoded, StandardCharsets.UTF_8);
		client.close();
		return secret;
	}
}
