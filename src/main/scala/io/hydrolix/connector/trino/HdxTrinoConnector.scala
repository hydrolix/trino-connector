package io.hydrolix.connector.trino

import java.time.Instant
import java.util.{Locale, Optional, UUID}
import java.{util => ju}
import scala.annotation.unused
import scala.jdk.CollectionConverters._

import com.typesafe.scalalogging.Logger
import io.trino.spi.`type`.TimeZoneKey
import io.trino.spi.connector._
import io.trino.spi.security.ConnectorIdentity
import io.trino.spi.transaction.IsolationLevel
import io.trino.spi.{Plugin, StandardErrorCode, TrinoException}

import io.hydrolix.connectors.{HdxConnectionInfo, HdxTableCatalog}

final class HdxTrinoPlugin extends Plugin {
  override val getConnectorFactories: ju.List[ConnectorFactory] = ju.Arrays.asList(HdxTrinoConnectorFactory)
}

object HdxTrinoConnectorFactory extends ConnectorFactory {
  override def getName: String = "hydrolix"

  override def create(catalogName: String,
                           config: ju.Map[String, String],
                          context: ConnectorContext)
                                 : Connector =
  {
    val cfg = config.asScala.toMap
    val info = HdxConnectionInfo.fromOpts(cfg)
    val catalog = new HdxTableCatalog()
    catalog.initialize(catalogName, cfg)
    new HdxTrinoConnector(info, catalog)
  }
}

final class HdxTrinoConnector(val info: HdxConnectionInfo, val catalog: HdxTableCatalog) extends Connector {
  @unused private val logger = Logger(getClass)

  override def getMetadata(session: ConnectorSession, transactionHandle: ConnectorTransactionHandle): ConnectorMetadata = {
    new HdxTrinoConnectorMetadata(info, catalog)
  }

  override def getSplitManager: ConnectorSplitManager = new HdxTrinoSplitManager(info, catalog)

  override def beginTransaction(isolationLevel: IsolationLevel,
                                      readOnly: Boolean,
                                    autoCommit: Boolean) =
  {
    HdxTrinoTransactionHandle.INSTANCE
  }

  override def getPageSourceProvider: ConnectorPageSourceProvider = new HdxTrinoPageSourceProvider(info, catalog)
}

case class HdxTrinoConnectorSession(info: HdxConnectionInfo) extends ConnectorSession {
  override val getQueryId = UUID.randomUUID().toString
  override val getStart = Instant.now()

  override val getSource = Optional.empty[String]()
  override val getIdentity = ConnectorIdentity.ofUser(info.user)
  override val getTimeZoneKey = TimeZoneKey.UTC_KEY
  override val getLocale = Locale.getDefault
  override val getTraceToken = Optional.empty[String]()

  override def getProperty[T <: AnyRef](name: String, `type`: Class[T]): T = {
    throw new TrinoException(StandardErrorCode.INVALID_SESSION_PROPERTY, "Unknown session property " + name)
  }
}