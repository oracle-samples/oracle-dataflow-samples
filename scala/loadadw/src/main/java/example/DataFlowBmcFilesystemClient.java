package example;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;

import com.oracle.bmc.hdfs.BmcFilesystem;

public class DataFlowBmcFilesystemClient {
	public static BmcFilesystem getBmcFilesystemClient(String path, Configuration config) throws IOException, URISyntaxException {
		BmcFilesystem bmcFS = new BmcFilesystem();
		bmcFS.initialize(new URI(path), config);
		return bmcFS;
	}
}
