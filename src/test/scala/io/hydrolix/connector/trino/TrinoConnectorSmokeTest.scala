package io.hydrolix.connector.trino

import java.net.URI
import java.util.Optional
import scala.jdk.CollectionConverters._

import org.junit.Test

import io.hydrolix.connectors.HdxConnectionInfo

class TrinoConnectorSmokeTest {
  @Test
  def doStuff(): Unit = {
    val jdbcUrl = System.getenv("HDX_SPARK_JDBC_URL")
    val apiUrl = System.getenv("HDX_SPARK_API_URL")
    val user = System.getenv("HDX_USER")
    val pass = System.getenv("HDX_PASSWORD")
    val cloudCred1 = System.getenv("HDX_SPARK_CLOUD_CRED_1")
    val cloudCred2 = Option(System.getenv("HDX_SPARK_CLOUD_CRED_2"))

    val info = HdxConnectionInfo(jdbcUrl, user, pass, new URI(apiUrl), None, cloudCred1, cloudCred2, Some("myubuntu"))

    val session = HdxTrinoConnectorSession(info)
    val connector = HdxTrinoConnectorFactory.create("hydrolix", info.asMap.asJava, null)
    println(connector)
    val meta = connector.getMetadata(session, HdxTrinoTransactionHandle.INSTANCE)
    println(meta.listTables(session, Optional.of("hydro")))
  }
}
