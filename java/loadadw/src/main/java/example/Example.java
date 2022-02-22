package example;



import java.util.HashMap;
import java.util.Map;
import com.oracle.bmc.ClientConfiguration;
import com.oracle.bmc.database.DatabaseClient;
import com.oracle.bmc.database.model.GenerateAutonomousDatabaseWalletDetails;
import com.oracle.bmc.database.model.GenerateAutonomousDatabaseWalletDetails.GenerateType;
import com.oracle.bmc.database.requests.GenerateAutonomousDatabaseWalletRequest;
import com.oracle.bmc.database.responses.GenerateAutonomousDatabaseWalletResponse;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.requests.GetNamespaceRequest;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.HeadObjectRequest;

import com.oracle.bmc.retrier.RetryConfiguration;
import com.oracle.bmc.waiter.MaxAttemptsTerminationStrategy;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider;
import com.oracle.bmc.http.ClientConfigurator;




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

	static final String ADB_ID =
			"ocid1.autonomousdatabase.oc1.phx.anyhqljsl3toglqajvoocrecf6ughx4al2quss3fcosh4gaqjraew5utym7a";
	static final String USER = "ADMIN";
	static final String PASSWORD = "Password-123";
	static final String SRC_TABLE = "ADMIN.MOVIES";
	static final String TARGET_TABLE = "ADMIN.MOVIES_SIVA";
	static final String WALLET_URI = "oci://siselvan@paasdevssstest/adw_datasource_test/Wallet_DB202112300839.zip";
	static final String CONNECTION_ID = "db202112300839_medium";

	public static void main(String[] args) throws Exception {
		// Get our Spark session.
		SparkSession spark = DataFlowSparkSession.getSparkSession("Sample App");
		System.out.println("Start object storage");
		String tokenPath = DataFlowUtilities.getDelegationTokenPath(spark);
		AbstractAuthenticationDetailsProvider provider = DataFlowUtilities.getAuthenticationDetailsProvider();
		System.out.println("provider created");
		ClientConfigurator configurator = DataFlowUtilities.getClientConfigurator(tokenPath);
		System.out.println("configurator created");
		downloadWalletFromObjectStorage(tokenPath,provider,configurator);
		System.out.println("End object storage");

		System.out.println("downloadWalletFromAdw");
		downloadWalletFromAdw(tokenPath,ADB_ID,provider,configurator);
		System.out.println("completed downloadWalletFromAdw");

		System.out.println("-------------------------------------------------------------------");
		System.out.println("Oracle datasource example start");
		oracleDatasourceExample(spark);
		System.out.println("Oracle datasource example end");
		System.out.println("-------------------------------------------------------------------");

		spark.stop();
	}

	public static void oracleDatasourceExample(SparkSession spark)  {
		Map<String,String> properties = new HashMap();
		properties.put("user",USER);
		properties.put("password",PASSWORD);

		System.out.println("Reading data from autonomous database.");
		Dataset<Row> src_df = spark
						.read()
						.format("oracle")
						.options(properties)
						.option("adbId",ADB_ID)
						.option("dbtable",SRC_TABLE)
						.load();


		System.out.println("Writing data to autonomous database.");
				src_df.write()
						.format("oracle")
						.options(properties)
						.option("dbtable",TARGET_TABLE)
						.option("walletUri",WALLET_URI)
						.option("connectionId",CONNECTION_ID)
						.mode(SaveMode.Overwrite)
						.save();

	}

	public static void downloadWalletFromObjectStorage(String tokenPath ,AbstractAuthenticationDetailsProvider provider,
			ClientConfigurator configurator) {
		System.out.println("downloadWalletFromObjectStorage");


		ClientConfiguration clientConfiguration =
				ClientConfiguration.builder()
						.retryConfiguration(
								RetryConfiguration.builder()
										.terminationStrategy(new MaxAttemptsTerminationStrategy(5)).build())
						.readTimeoutMillis(300000)
						.connectionTimeoutMillis(300000)
						.build();

		ObjectStorageClient objectStorageClient = ObjectStorageClient.builder()
				.clientConfigurator(configurator)
				.configuration(clientConfiguration)
				.build(provider);

		String namespace = objectStorageClient.getNamespace(GetNamespaceRequest.builder().build()).getValue();
		System.out.println("namespace: "  +namespace);

		objectStorageClient.headObject(HeadObjectRequest.builder()
				.namespaceName(namespace).bucketName("siselvan")
				.objectName("adw_datasource_test/Wallet_DB202201221739.zip").build());

		objectStorageClient.getObject(GetObjectRequest
				.builder().namespaceName(namespace).bucketName("siselvan")
				.objectName("adw_datasource_test/Wallet_DB202201221739.zip").build());
	}

	public static void downloadWalletFromAdw(String tokenPath,
			String adwOcid,
			AbstractAuthenticationDetailsProvider provider,
			ClientConfigurator configurator) {
		System.out.println("Downloading wallet from adw.");
		DatabaseClient databaseClient = DatabaseClient
				.builder()
				.clientConfigurator(configurator)
				.build(provider);

		GenerateAutonomousDatabaseWalletDetails autonomousDatabaseWalletDetails = GenerateAutonomousDatabaseWalletDetails
				.builder()
				.password("+Dc-u9F3")
				.generateType(GenerateType.create("SINGLE")) // Instance wallet
				.build();

		GenerateAutonomousDatabaseWalletRequest autonomousDatabaseWalletRequest = GenerateAutonomousDatabaseWalletRequest
				.builder()
				.autonomousDatabaseId(adwOcid)
				.generateAutonomousDatabaseWalletDetails(autonomousDatabaseWalletDetails)
				.build();

		GenerateAutonomousDatabaseWalletResponse response = databaseClient
				.generateAutonomousDatabaseWallet(autonomousDatabaseWalletRequest);

		System.out.println("response length: " + response.getContentLength());
	}
}
