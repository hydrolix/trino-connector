package io.hydrolix.connector.trino

import java.time.Instant
import java.util.{Locale, Optional, UUID}
import java.{lang => jl, util => ju}
import scala.beans.BeanProperty
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty, JsonValue}
import io.trino.spi.`type`.{TimeZoneKey, Type}
import io.trino.spi.connector._
import io.trino.spi.security.ConnectorIdentity
import io.trino.spi.transaction.IsolationLevel
import io.trino.spi.{Plugin, StandardErrorCode, TrinoException}
import org.slf4j.LoggerFactory

import io.hydrolix.connectors.{HdxConnectionInfo, HdxTableCatalog}

final class HdxTrinoPlugin extends Plugin {
  override val getConnectorFactories: ju.List[ConnectorFactory] = ju.Arrays.asList(HdxTrinoConnectorFactory)

  override def getTypes: jl.Iterable[Type] = super.getTypes
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
  private val logger = LoggerFactory.getLogger(getClass)

  override def getMetadata(session: ConnectorSession, transactionHandle: ConnectorTransactionHandle): ConnectorMetadata = {
    new HdxTrinoConnectorMetadata(catalog)
  }

  override def getSplitManager: ConnectorSplitManager = new HdxTrinoSplitManager(info, catalog)

  override def beginTransaction(isolationLevel: IsolationLevel,
                                      readOnly: Boolean,
                                    autoCommit: Boolean) =
  {
    HdxTrinoTransactionHandle.INSTANCE
  }

  override def getPageSourceProvider: ConnectorPageSourceProvider = super.getPageSourceProvider
}

case class HdxTableHandle(
  @JsonProperty @BeanProperty var    db: String,
  @JsonProperty @BeanProperty var table: String
) extends ConnectorTableHandle {
  @JsonCreator
  def this() = this(null, null)
}

case class HdxColumnHandle(
  @JsonProperty @BeanProperty var name: String
) extends ColumnHandle {
  def this() = this(null)
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