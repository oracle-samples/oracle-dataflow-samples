package example;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.fs.Path;

import com.oracle.bmc.hdfs.BmcFilesystem;

import org.apache.spark.sql.SparkSession;

/*
 * Helper to deploy a wallet to the Spark cluster.
 * 
 * This only needs to be done once and should be done in the Spark driver.
 */
public class DataFlowDeployWallet {
	public static String deployWallet(SparkSession spark, String walletPath) throws IOException, URISyntaxException {
		BmcFilesystem bmcFS = DataFlowBmcFilesystemClient.getBmcFilesystemClient(walletPath,
				DataFlowSparkSession.getBmcConfiguration(spark));
		String tmpPath = downloadAndExtract(bmcFS, new Path(walletPath));

		List<String> walletContents = Arrays.asList("cwallet.sso", "ewallet.p12", "keystore.jks", "ojdbc.properties",
				"sqlnet.ora", "tnsnames.ora", "truststore.jks");
		for (String file : walletContents) {
			spark.sparkContext().addFile(tmpPath + file);
		}
		return tmpPath;
	}

	private static String downloadAndExtract(BmcFilesystem bmc, Path walletRemotePath)
			throws IllegalArgumentException, IOException {
		String tmpPath = DataFlowUtilities.getTempDirectory();
		String walletLocal = tmpPath + "wallet.zip";
		bmc.copyToLocalFile(walletRemotePath, new Path(walletLocal));
		unzip(walletLocal, tmpPath);
		return tmpPath;
	}

	private static final int BUFFER_SIZE = 4096;

	private static void unzip(String zipFilePath, String destDirectory) throws IOException {
		File destDir = new File(destDirectory);
		if (!destDir.exists()) {
			destDir.mkdir();
		}
		ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath));
		ZipEntry entry = zipIn.getNextEntry();
		// iterates over entries in the zip file
		while (entry != null) {
			String filePath = destDirectory + File.separator + entry.getName();
			if (!entry.isDirectory()) {
				// if the entry is a file, extracts it
				extractFile(zipIn, filePath);
			} else {
				// if the entry is a directory, make the directory
				File dir = new File(filePath);
				dir.mkdir();
			}
			zipIn.closeEntry();
			entry = zipIn.getNextEntry();
		}
		zipIn.close();
	}

	private static void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
		byte[] bytesIn = new byte[BUFFER_SIZE];
		int read = 0;
		while ((read = zipIn.read(bytesIn)) != -1) {
			bos.write(bytesIn, 0, read);
		}
		bos.close();
	}

}
