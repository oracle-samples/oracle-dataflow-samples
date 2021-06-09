/**
  * Copyright (c) 2016, 2020, Oracle and/or its affiliates. All rights reserved.
  */

package example

import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider

import java.util.Properties

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import oracle.jdbc.OracleConnection

object Example {
  def main(args: Array[String]) = {

    // Customize these.
    val inputPath = "oci://<bucket>@<tenancy>/fake_data.csv"
    val walletPath = "oci://<bucket>@<tenancy>/wallet.zip"
    val user = "ADMIN"
    val passwordOcid = "ocid1.vaultsecret.oc1.iad.<vault_ocid>"
    val tnsName = "dataflowdemo_high";

    // Get our session.
    var spark = DataFlowSparkSession.getSparkSession("Sample App")

    // Set up access to OCI services.
    val tokenPath = DataFlowUtilities.getDelegationTokenPath(spark)
    val authenticationProvider = DataFlowUtilities.getAuthenticationDetailsProvider()
    val clientConfigurator = DataFlowUtilities.getClientConfigurator(tokenPath);

    // Deploy the wallet.
    val password = Utilities.getSecret(tokenPath, passwordOcid, authenticationProvider, clientConfigurator);
    val tmpPath = DataFlowDeployWallet.deployWallet(spark, walletPath)
    val jdbcUrl = s"jdbc:oracle:thin:@$tnsName?TNS_ADMIN=$tmpPath"
    println(s"JDBC URL " + jdbcUrl)

    // Read our data.
    val df = spark.read.option("header", "true").csv(inputPath)

    // Write data via JDBC.
    val connectionProperties = new Properties()
    var options = collection.mutable.Map[String, String]()
    options += ("driver" -> "oracle.jdbc.driver.OracleDriver")
    options += ("url" -> jdbcUrl)
    options += (OracleConnection.CONNECTION_PROPERTY_USER_NAME -> user)
    options += (OracleConnection.CONNECTION_PROPERTY_PASSWORD -> password)
    options += (OracleConnection.CONNECTION_PROPERTY_TNS_ADMIN -> tmpPath)
    options += ("dbtable" -> "sample")
    df.write.format("jdbc").options(options).mode("Overwrite").save()
    println(s"Export to ADW complete.")
  }
}
