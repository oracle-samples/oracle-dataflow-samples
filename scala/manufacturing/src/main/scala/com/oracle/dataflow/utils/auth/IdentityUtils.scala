package com.oracle.dataflow.utils.auth

import com.oracle.bmc.ConfigFileReader
import com.oracle.bmc.auth.{AbstractAuthenticationDetailsProvider, ConfigFileAuthenticationDetailsProvider, InstancePrincipalsAuthenticationDetailsProvider, ResourcePrincipalAuthenticationDetailsProvider}
import com.oracle.bmc.http.{ClientConfigurator, DefaultConfigurator}
import com.oracle.dataflow.utils.CommonUtils.{getDelegationTokenPath, isRunningInDataFlow}

import java.io.IOException

object IdentityUtils {
  @throws[IOException]
  def getAuthenticationDetailsProvider: AbstractAuthenticationDetailsProvider = {
    if (isRunningInDataFlow) {
      if (getDelegationTokenPath != null) {
        println("using delegation token with instance principal")
        InstancePrincipalsAuthenticationDetailsProvider.builder.build
      } else {
        println("resource principal authentication details")
        ResourcePrincipalAuthenticationDetailsProvider.builder.build
      }
    } else {
      println("using config file authentication")
      new ConfigFileAuthenticationDetailsProvider(ConfigFileReader.DEFAULT_FILE_PATH, "DEFAULT")
    }
  }

  def getClientConfigurator: ClientConfigurator = {
    if (isRunningInDataFlow) {
     if (getDelegationTokenPath != null) {
       println(s"delegation client configurator $getDelegationTokenPath")
       return new DelegationTokenClientConfigurator(getDelegationTokenPath)
     } else {
       println("Default configurator for RP")
       new DefaultConfigurator
     }
    }
    println("using default configurator")
    new DefaultConfigurator
  }
}
