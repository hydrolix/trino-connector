package io.hydrolix.connector.trino

import scala.jdk.CollectionConverters._

import io.trino.spi.connector._

import io.hydrolix.connectors.{HdxConnectionInfo, HdxJdbcSession, HdxTableCatalog}

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

    new FixedSplitSource(parts.map(new HdxTrinoSplit(_)).asJava)
  }
}
