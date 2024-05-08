/**
 * New version OboTokenClientConfigurator is valid for dataflow Spark version > 3.5.0 app.
 * Old OboTokenClientConfigurator has been modified to this new version
 * in order to support laests java sdk version > 3.34.1
 */
package example;

import com.oracle.bmc.http.DefaultConfigurator;
import com.oracle.bmc.http.client.HttpClientBuilder;
import com.oracle.bmc.http.client.HttpRequest;
import com.oracle.bmc.http.client.RequestInterceptor;
import com.oracle.bmc.http.signing.internal.Constants;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.ws.rs.Priorities;

/**
 * Customize the SDK underlying REST client to use the on-behalf-of token when running on
 * Data Flow.
 */
public class OboTokenClientConfiguratorV2 extends DefaultConfigurator {
  private final String delegationTokenPath;

  public OboTokenClientConfiguratorV2(String delegationTokenPath) {
    super();
    this.delegationTokenPath = delegationTokenPath;
  }

  public void customizeClient(HttpClientBuilder client) {
    super.customizeClient(client);
    client.registerRequestInterceptor(Priorities.AUTHENTICATION - 1,
        new DelegationTokenRequestInterceptor());
  }

  class DelegationTokenRequestInterceptor implements RequestInterceptor {
    @Override
    public void intercept(HttpRequest request) {
      final String token;
      try {
        token = new String(Files.readAllBytes(Paths.get(delegationTokenPath)));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      request.header(Constants.OPC_OBO_TOKEN, token);
    }
  }
}