package io.hydrolix.connector.trino

import java.util.UUID
import scala.jdk.CollectionConverters._

import io.trino.Session
import io.trino.metadata.SessionPropertyManager
import io.trino.spi.QueryId
import io.trino.spi.security.Identity
import io.trino.testing.{BaseConnectorTest, QueryRunner, StandaloneQueryRunner, TestingConnectorBehavior}
import org.junit.Test
import org.junit.jupiter.api.{BeforeAll, Disabled}

import io.hydrolix.connectors.HdxConnectionInfo

@Disabled("Doesn't work without an actual cluster, plus all the inherited tests fail")
class TrinoConnectorSmokeTest extends BaseConnectorTest {
  private val info = HdxConnectionInfo.fromEnv()

  @BeforeAll
  override def init(): Unit = {
    super.init()
  }

  @Test
  def doStuff(): Unit = {
    val qr = createQueryRunner()

    val res = qr.execute("select count(*), min(timestamp), max(timestamp) from hydrolix.hydro.logs where timestamp > now() - interval '5' minute")

    println(res)
  }

  override def hasBehavior(connectorBehavior: TestingConnectorBehavior): Boolean = {
    connectorBehavior match {
      case TestingConnectorBehavior.SUPPORTS_ARRAY => true
      case TestingConnectorBehavior.SUPPORTS_CANCELLATION => true
      case TestingConnectorBehavior.SUPPORTS_DYNAMIC_FILTER_PUSHDOWN => true
      case TestingConnectorBehavior.SUPPORTS_LIMIT_PUSHDOWN => true
      case TestingConnectorBehavior.SUPPORTS_NEGATIVE_DATE => true
      case TestingConnectorBehavior.SUPPORTS_NOT_NULL_CONSTRAINT => true
      case TestingConnectorBehavior.SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN => true
      case TestingConnectorBehavior.SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN_WITH_LIKE => true
      case TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN => true
      case TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY => true
      case TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY => false
      case _ => false
    }
  }

  override def createQueryRunner(): QueryRunner = {
    val session = Session.builder(new SessionPropertyManager())
      .setQueryId(QueryId.valueOf(UUID.randomUUID().toString.replace("-", "_")))
      .setIdentity(Identity.ofUser("user"))
      .setOriginalIdentity(Identity.ofUser("orig_user"))
      .build()

    val qr = new StandaloneQueryRunner(session)

    val props = (
      Map(
        HdxConnectionInfo.OPT_API_URL -> info.apiUrl.toString,
        HdxConnectionInfo.OPT_JDBC_URL -> info.jdbcUrl,
        HdxConnectionInfo.OPT_USERNAME -> info.user,
        HdxConnectionInfo.OPT_PASSWORD -> info.password,
        HdxConnectionInfo.OPT_CLOUD_CRED_1 -> info.cloudCred1
      )
        ++ info.cloudCred2.map(HdxConnectionInfo.OPT_CLOUD_CRED_2 -> _)
        ++ info.extraOpts
      ).asJava

    qr.installPlugin(new HdxTrinoPlugin)

    qr.createCatalog(
      "hydrolix",
      "hydrolix",
      props
    )

    qr
  }
}
