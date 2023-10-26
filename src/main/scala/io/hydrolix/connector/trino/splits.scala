package io.hydrolix.connector.trino

import java.{util => ju}
import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import io.trino.spi.HostAddress
import io.trino.spi.connector._

import io.hydrolix.connectors.{HdxConnectionInfo, HdxDbPartition, HdxJdbcSession, HdxTableCatalog}

final class HdxTrinoSplitManager(val info: HdxConnectionInfo, val catalog: HdxTableCatalog) extends ConnectorSplitManager {
  override def getSplits(transaction: ConnectorTransactionHandle,
                             session: ConnectorSession,
                               table: ConnectorTableHandle,
                       dynamicFilter: DynamicFilter,
                          constraint: Constraint)
                                    : ConnectorSplitSource =
  {
    val tbl = table.asInstanceOf[HdxTableHandle]

    // TODO should we try to do partition pruning here, and/or in ConnectorMetadata#applyFilter?
    // TODO should we try to do partition pruning here, and/or in ConnectorMetadata#applyFilter?
    // TODO should we try to do partition pruning here, and/or in ConnectorMetadata#applyFilter?
    // TODO should we try to do partition pruning here, and/or in ConnectorMetadata#applyFilter?
    val parts = HdxJdbcSession(info).collectPartitions(tbl.db, tbl.table, None, None)

    new FixedSplitSource(parts.map(HdxTrinoSplit).asJava)
  }
}

case class HdxTrinoSplit(
  @JsonProperty
  var part: HdxDbPartition
) extends ConnectorSplit {
  @JsonCreator
  def this() = this(null)

  override val isRemotelyAccessible = true

  override val getAddresses = ju.Collections.emptyList[HostAddress]()

  override val getInfo = null

  override lazy val getRetainedSizeInBytes = part.dataSize
}