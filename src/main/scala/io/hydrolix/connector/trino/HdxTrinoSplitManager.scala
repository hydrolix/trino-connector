package io.hydrolix.connector.trino

import java.time.Instant
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.{RichOption, RichOptional}

import io.trino.spi.`type`.TimestampType
import io.trino.spi.block.LongArrayBlock
import io.trino.spi.connector._
import io.trino.spi.predicate.SortedRangeSet
import org.slf4j.LoggerFactory

import io.hydrolix.connectors.{HdxConnectionInfo, HdxDbPartition, HdxJdbcSession, HdxTableCatalog}

final class HdxTrinoSplitManager(val info: HdxConnectionInfo, val catalog: HdxTableCatalog) extends ConnectorSplitManager {
  private val logger = LoggerFactory.getLogger(getClass)

  import HdxTrinoSplitManager._

  override def getSplits(transaction: ConnectorTransactionHandle,
                             session: ConnectorSession,
                               table: ConnectorTableHandle,
                       dynamicFilter: DynamicFilter,
                          constraint: Constraint)
                                    : ConnectorSplitSource =
  {
    val tbl = table.asInstanceOf[HdxTableHandle]

    val (earliest, latest) = constraint.getSummary.getDomains.toScala match {
      case Some(map) if map.size() == 1 =>
        val key = map.keySet().iterator().next()
        val hdxTable = catalog.loadTable(List(tbl.db(), tbl.table()))

        key match {
          case hch: HdxColumnHandle if hch.name() == hdxTable.primaryKeyField =>
            val value = map.get(hch)
            if (value.getType.isInstanceOf[TimestampType]) {
              value.getValues match {
                case srs: SortedRangeSet =>
                  srs.getSortedRanges match {
                    case lb: LongArrayBlock =>
                      if (lb.getPositionCount == 2) {
                        if (lb.isNull(0) && lb.isNull(1)) {
                          logger.warn("Upper and earliest both null?!")
                          (None, None)
                        } else {
                          (
                            if (lb.isNull(0)) None else Some(Instant.ofEpochMilli(lb.getLong(0, 0))),
                            if (lb.isNull(1)) None else Some(Instant.ofEpochMilli(lb.getLong(1, 0)))
                          )
                        }
                      } else {
                        (None, None)
                      }
                  }
              }
            } else {
              (None, None)
            }
          case other =>
            (None, None)
        }
      case Some(otherMap) =>
        logger.warn(s"Constraint domains was a map with >1 entry: $otherMap")
        (None, None)
      case None =>
        (None, None)
    }


    // TODO should we try to do partition pruning here, and/or in ConnectorMetadata#applyFilter?
    // TODO should we try to do partition pruning here, and/or in ConnectorMetadata#applyFilter?
    // TODO should we try to do partition pruning here, and/or in ConnectorMetadata#applyFilter?
    // TODO should we try to do partition pruning here, and/or in ConnectorMetadata#applyFilter?
    val parts = HdxJdbcSession(info).collectPartitions(tbl.db, tbl.table, earliest, latest)

    new FixedSplitSource(parts.map(_.toSplit).asJava)
  }
}

object HdxTrinoSplitManager {
  implicit class TrinoSplitOps(split: HdxTrinoSplit) {
    def toHdxPartition: HdxDbPartition = HdxDbPartition(
      split.partition(),
      split.minTimestamp(),
      split.maxTimestamp(),
      split.manifestSize(),
      split.dataSize(),
      split.indexSize(),
      split.rows(),
      split.memSize(),
      split.rootPath(),
      split.shardKey(),
      split.active(),
      split.storageId().toScala
    )
  }

  implicit class HdxDbPartitionOps(p: HdxDbPartition) {
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
