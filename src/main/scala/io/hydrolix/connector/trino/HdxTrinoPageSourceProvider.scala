package io.hydrolix.connector.trino

import java.util
import scala.jdk.CollectionConverters._

import io.airlift.slice.Slice
import io.trino.spi.Page
import io.trino.spi.`type`.Type
import io.trino.spi.connector.{ColumnHandle, ConnectorPageSource, ConnectorPageSourceProvider, ConnectorSession, ConnectorSplit, ConnectorTableHandle, ConnectorTransactionHandle, DynamicFilter, RecordCursor, RecordPageSource}
import org.slf4j.LoggerFactory

import io.hydrolix.connectors.expr.StructLiteral
import io.hydrolix.connectors.partitionreader.{CoreRowAdapter, HdxPartitionReader, RowPartitionReader}
import io.hydrolix.connectors.{HdxApiSession, HdxApiTable, HdxConnectionInfo, HdxPartitionScanPlan, HdxStorageSettings, HdxTable, HdxTableCatalog}
import io.hydrolix.connectors.types.StructType

class HdxTrinoPageSourceProvider(info: HdxConnectionInfo, catalog: HdxTableCatalog) extends ConnectorPageSourceProvider {
  private val logger = LoggerFactory.getLogger(getClass)
  private val apiSession = new HdxApiSession(info)
  private val emptyRow = StructLiteral(Map(), StructType())

  override def createPageSource(transaction: ConnectorTransactionHandle,
                                    session: ConnectorSession,
                                      split: ConnectorSplit,
                                      table: ConnectorTableHandle,
                                    columns: util.List[ColumnHandle],
                              dynamicFilter: DynamicFilter)
                                           : ConnectorPageSource =
  {


    val handle = table.asInstanceOf[HdxTableHandle]
    val hsplit = split.asInstanceOf[HdxTrinoSplit]
    val hdxTable = catalog.loadTable(List(handle.db, handle.table))

    val (storageId, storage) = hsplit.part.storageId.flatMap(id => hdxTable.storages.get(id).map(id -> _)).getOrElse {
      val storages = apiSession.storages()
      val fromApi = storages.find(_.settings.isDefault) match {
        case Some(dflt) =>
          logger.info(s"Couldn't identify Storage for ${handle.db}.${handle.table}, partition ${hsplit.part.partition} -- using default from API")
          dflt
        case None =>
          logger.warn(s"Couldn't identify Storage for ${handle.db}.${handle.table}, partition ${hsplit.part.partition} -- using first listed in API")
          storages.head
      }
      (fromApi.uuid, fromApi.settings)
    }

    val schema = columns.asScala.map {
      case HdxColumnHandle(name) =>
        hdxTable.schema.byName.getOrElse(name, sys.error(s"No column `$name` listed in ${handle.db}.${handle.table} schema"))
    }

    val cols = StructType(schema.toList)

    val scan = HdxPartitionScanPlan(
      handle.db,
      handle.table,
      storageId,
      hsplit.part.partition,
      cols,
      Nil, // TODO!
      hdxTable.hdxCols.filter {
        case (name, _) => cols.byName.contains(name)
      })

    val reader = new RowPartitionReader[StructLiteral](info, storage, hdxTable.primaryKeyField, scan, CoreRowAdapter, emptyRow)()

    val trinoTypes = cols.fields.map(field => TrinoTypes.coreToTrino(field.`type`))

    new RecordPageSource(trinoTypes.asJava, new HdxTrinoRecordCursor(reader) {

    })
  }
}

final class HdxTrinoRecordCursor(reader: RowPartitionReader[StructLiteral]) extends RecordCursor {
  var row: StructLiteral = _

  override def getCompletedBytes: Long = ???

  override def getReadTimeNanos: Long = ???

  override def getType(field: Int): Type = ???

  override def advanceNextPosition(): Boolean = {
    if (reader.next()) {
      row = reader.get()

    }
  }

  override def getBoolean(field: Int): Boolean = ???

  override def getLong(field: Int): Long = ???

  override def getDouble(field: Int): Double = ???

  override def getSlice(field: Int): Slice = ???

  override def getObject(field: Int): AnyRef = ???

  override def isNull(field: Int): Boolean = ???

  override def close(): Unit = ???
}




