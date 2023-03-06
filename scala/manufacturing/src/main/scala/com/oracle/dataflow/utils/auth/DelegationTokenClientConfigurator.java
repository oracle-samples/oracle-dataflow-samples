package com.oracle.dataflow.utils.auth;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import com.oracle.bmc.http.DefaultConfigurator;
import com.oracle.bmc.http.signing.internal.Constants;

public class DelegationTokenClientConfigurator extends DefaultConfigurator {
  private final String delegationTokenPath;

  public DelegationTokenClientConfigurator(String delegationTokenPath) {
    super();
    this.delegationTokenPath = delegationTokenPath;
  }

  @Override
  public void customizeClient(Client client) {
    super.customizeClient(client);
    client.register(new DelegationTokenRequestFilter());
  }

  @Priority(DelegationTokenRequestFilter.PRIORITY)
  class DelegationTokenRequestFilter implements ClientRequestFilter {
    public static final int PRIORITY = Priorities.AUTHENTICATION - 1;

    @Override
    public void filter(final ClientRequestContext requestContext) throws IOException {
      final String token = new String(Files.readAllBytes(Paths.get(delegationTokenPath)));
      requestContext.getHeaders().putSingle(Constants.OPC_OBO_TOKEN, token);
    }
  }
}
