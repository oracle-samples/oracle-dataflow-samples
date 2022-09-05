package com.oracle.dataflow.utils

import com.typesafe.config.{Config, ConfigFactory}
import com.oracle.bmc.objectstorage.ObjectStorageClient
import com.oracle.bmc.objectstorage.requests.GetObjectRequest
import com.oracle.bmc.objectstorage.responses.GetObjectResponse
import com.oracle.dataflow.utils.auth.IdentityUtils.{getAuthenticationDetailsProvider, getClientConfigurator}

import scala.io.Source

class ApplicationConfiguration(configFilePath: String) {
  val objectStorageClient: ObjectStorageClient = ObjectStorageClient.builder.clientConfigurator(getClientConfigurator)
    .build(getAuthenticationDetailsProvider)
  ObjectStorageUriParser.parse(configFilePath)
  val osInfo = ObjectStorageUriParser.objectStorageDetails
  val resp:GetObjectResponse = objectStorageClient.getObject(GetObjectRequest.builder().namespaceName(osInfo.namespace)
    .bucketName(osInfo.bucket).objectName(osInfo.obj).build())
  var confString:String = ""
  var applicationConf: Config = _
  try {
    confString = Source.fromInputStream(resp.getInputStream).getLines().mkString("\n")
    applicationConf = ConfigFactory.parseString(confString)
  } finally {
    if (resp != null) {
      resp.getInputStream.close();
    }
  }
}
