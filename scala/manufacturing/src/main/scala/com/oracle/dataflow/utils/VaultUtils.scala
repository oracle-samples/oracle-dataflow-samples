package com.oracle.dataflow.utils
import com.oracle.bmc.secrets.SecretsClient
import com.oracle.bmc.secrets.model.Base64SecretBundleContentDetails
import com.oracle.bmc.secrets.requests.GetSecretBundleRequest
import com.oracle.dataflow.utils.auth.IdentityUtils.{getAuthenticationDetailsProvider, getClientConfigurator}

import java.nio.charset.StandardCharsets
import java.util.Base64

object VaultUtils {
  def getSecret(secretOcid:String):String = {
    val vaultClient = SecretsClient.builder.clientConfigurator(getClientConfigurator)
      .build(getAuthenticationDetailsProvider)
    val getSecretBundleResponse = vaultClient.getSecretBundle(GetSecretBundleRequest
      .builder().secretId(secretOcid).stage(GetSecretBundleRequest.Stage.Current).build())
    val base64SecretBundleContentDetails = getSecretBundleResponse.getSecretBundle()
      .getSecretBundleContent().asInstanceOf[Base64SecretBundleContentDetails]
    val secretValueDecoded = Base64.getDecoder.decode(base64SecretBundleContentDetails.getContent)
    val secret=new String(secretValueDecoded, StandardCharsets.UTF_8)
    secret
  }

}
