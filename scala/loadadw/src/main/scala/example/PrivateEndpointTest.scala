package example

import org.apache.spark.sql.SparkSession

import java.net.{InetAddress, Socket}
import scala.util.{Failure, Success, Try}

object PrivateEndpointTest {
  def main(args: Array[String]): Unit = {
    // Check for correct number of arguments
    if (args.length != 2) {
      println("Usage: PrivateEndpointTest <FQDN> <port>")
      sys.exit(1)
    }

    // Parse arguments
    val privateFQDN = args(0)
    val port = args(1).toInt

    val spark = SparkSession.builder()
      .appName("Private Endpoint Test")
      .getOrCreate()

    // Function to resolve FQDN to IP
    def resolveFQDN(fqdn: String): Option[String] = {
      Try {
        val ipAddress = InetAddress.getByName(fqdn).getHostAddress
        ipAddress
      } match {
        case Success(ip) => Some(ip)
        case Failure(e) =>
          println(s"Error resolving FQDN '$fqdn': ${e.getMessage}")
          None
      }
    }

    // Function to test connectivity
    def testEndpoint(fqdn: String, port: Int): (Boolean, Option[String]) = {
      Try {
        val socket = new Socket(fqdn, port)
        socket.close()
        "Connection successful."
      } match {
        case Success(message) => (true, Some(message))
        case Failure(e) =>
          val errorMsg = s"Error connecting to $fqdn:$port - ${e.getMessage}"
          (false, Some(errorMsg))
      }
    }

    // Resolve FQDN
    val ipAddressOpt = resolveFQDN(privateFQDN)

    ipAddressOpt match {
      case Some(ipAddress) =>
        println(s"FQDN '$privateFQDN' resolved to IP '$ipAddress'. Testing connectivity...")
        // Test connectivity
        val (isAccessible, debugMessage) = testEndpoint(privateFQDN, port)

        // Output results
        if (isAccessible) {
          println(s"Success: Able to connect to $privateFQDN ($ipAddress) on port $port.")
        } else {
          println(s"Failure: Unable to connect to $privateFQDN ($ipAddress) on port $port.")
          debugMessage.foreach(println)
        }

      case None =>
        println(s"Failed to resolve FQDN '$privateFQDN'. Skipping connectivity test.")
    }

    // Stop Spark session
    spark.stop()
  }
}
