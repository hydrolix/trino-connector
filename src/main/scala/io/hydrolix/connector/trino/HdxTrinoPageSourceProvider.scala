package io.hydrolix.connector.trino

import java.{util => ju}
import scala.annotation.unused
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import io.trino.spi.connector._
import org.slf4j.LoggerFactory

import io.hydrolix.connector.trino.HdxTrinoSplitManager.TrinoSplitOps
import io.hydrolix.connectors.{HdxConnectionInfo, HdxPushdown, HdxTableCatalog, types => coretypes}

class HdxTrinoPageSourceProvider(info: HdxConnectionInfo,
                              catalog: HdxTableCatalog)
  extends ConnectorPageSourceProvider
{
  @unused private val logger = LoggerFactory.getLogger(getClass)

  override def createPageSource(transaction: ConnectorTransactionHandle,
                                    session: ConnectorSession,
                                      split: ConnectorSplit,
                                      table: ConnectorTableHandle,
                                    columns: ju.List[ColumnHandle],
                              dynamicFilter: DynamicFilter)
                                           : ConnectorPageSource =
  {
    val handle = table.asInstanceOf[HdxTableHandle]
    val hsplit = split.asInstanceOf[HdxTrinoSplit]
    val hdxTable = catalog.loadTable(List(handle.db, handle.table))

    val schema = columns.asScala.map {
      case h: HdxColumnHandle =>
        hdxTable.schema.byName.getOrElse(h.name, sys.error(s"No column `${h.name}` listed in ${handle.db}.${handle.table} schema"))
    }

    val cols = coretypes.StructType(schema.toList)

    HdxPushdown.doPlan(hdxTable, info.partitionPrefix, cols, Nil, hdxTable.hdxCols, hsplit.toHdxPartition, -1) match {
      case Some(scan) =>
        val reader = new HdxTrinoColumnarReader(
          info,
          hdxTable.storages.getOrElse(scan.storageId, sys.error(s"No storage #${scan.storageId}")),
          hdxTable.primaryKeyField,
          scan,
          emptyPage,
          handle.limit.toScala
        )

        // TODO 1MB isn't really realistic, could be much smaller or much larger
        new FixedPageSource(reader.stream.iterator, 1L*1024*1024)

      case None =>
        new EmptyPageSource()
    }
  }
}
