package example;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider;
import com.oracle.bmc.http.ClientConfigurator;
import com.oracle.bmc.secrets.Secrets;
import com.oracle.bmc.secrets.SecretsClient;
import com.oracle.bmc.secrets.model.Base64SecretBundleContentDetails;
import com.oracle.bmc.secrets.requests.GetSecretBundleRequest;
import com.oracle.bmc.secrets.responses.GetSecretBundleResponse;

public class Utilities {
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
