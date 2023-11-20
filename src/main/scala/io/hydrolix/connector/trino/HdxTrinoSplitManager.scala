package io.hydrolix.connector.trino

import scala.jdk.OptionConverters.{RichOption, RichOptional}

import io.trino.spi.connector._
import org.slf4j.LoggerFactory

import io.hydrolix.connectors.{HdxConnectionInfo, HdxDbPartition, HdxTableCatalog}

final class HdxTrinoSplitManager(val info: HdxConnectionInfo, val catalog: HdxTableCatalog) extends ConnectorSplitManager {
  private val logger = LoggerFactory.getLogger(getClass)

  override def getSplits(transaction: ConnectorTransactionHandle,
                             session: ConnectorSession,
                               table: ConnectorTableHandle,
                       dynamicFilter: DynamicFilter,
                          constraint: Constraint)
                                    : ConnectorSplitSource =
  {
    val tbl = table.asInstanceOf[HdxTableHandle]

    new FixedSplitSource(tbl.splits)
  }
}

object HdxTrinoSplitManager {
  implicit class TrinoSplitOps(val split: HdxTrinoSplit) extends AnyVal {
    def toHdxPartition: HdxDbPartition = HdxDbPartition(
      split.partition,
      split.minTimestamp,
      split.maxTimestamp,
      split.manifestSize,
      split.dataSize,
      split.indexSize,
      split.rows,
      split.memSize,
      split.rootPath,
      split.shardKey,
      split.active,
      split.storageId.toScala
    )
  }

  implicit class HdxDbPartitionOps(val p: HdxDbPartition) extends AnyVal {
    def toSplit: HdxTrinoSplit = new HdxTrinoSplit(
      p.partition,
      p.minTimestamp,
      p.maxTimestamp,
      p.manifestSize,
      p.dataSize,
      p.indexSize,
      p.rows,
      p.memSize,
      p.rootPath,
      p.shardKey,
      p.active,
      p.storageId.toJava
    )
  }
}
