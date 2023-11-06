package io.hydrolix.connector.trino

import java.time.{Duration, Instant}
import java.{util => ju}
import scala.jdk.CollectionConverters._

import io.airlift.slice.{Slice, Slices}
import io.trino.spi.`type`.Type
import io.trino.spi.connector._
import org.slf4j.LoggerFactory

import io.hydrolix.connector.trino.HdxTrinoSplitManager.TrinoSplitOps
import io.hydrolix.connectors.expr.{ArrayLiteral, MapLiteral, StructLiteral}
import io.hydrolix.connectors.partitionreader.{CoreRowAdapter, RowPartitionReader}
import io.hydrolix.connectors.types._
import io.hydrolix.connectors.{HdxApiSession, HdxConnectionInfo, HdxPushdown, HdxTableCatalog, microsToInstant}

class HdxTrinoPageSourceProvider(info: HdxConnectionInfo,
                              catalog: HdxTableCatalog)
  extends ConnectorPageSourceProvider
{
  private val logger = LoggerFactory.getLogger(getClass)
  private val emptyRow = StructLiteral(Map(), StructType())

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

    val cols = StructType(schema.toList: _*)

    HdxPushdown.doPlan(hdxTable, info.partitionPrefix, cols, Nil, hdxTable.hdxCols, hsplit.toHdxPartition, -1) match {
      case Some(scan) =>
        val trinoTypes = cols.fields.map(field => TrinoTypes.coreToTrino(field.`type`))

        val reader = new RowPartitionReader[StructLiteral](
          info,
          hdxTable.storages.getOrElse(scan.storageId, sys.error(s"No storage #${scan.storageId}")),
          hdxTable.primaryKeyField,
          scan,
          CoreRowAdapter,
          emptyRow
        )

        new RecordPageSource(trinoTypes.asJava, new HdxTrinoRecordCursor(reader))
      case None =>
        new EmptyPageSource()
    }
  }
}

final class HdxTrinoRecordCursor(reader: RowPartitionReader[StructLiteral]) extends RecordCursor {
  private var row: StructLiteral = _
  private val schema = reader.scan.schema
  private val startTime = Instant.now
  private var completedBytes = 0L

  override def getCompletedBytes: Long = completedBytes

  override def getReadTimeNanos: Long = Duration.between(startTime, Instant.now()).toNanos

  override def getType(field: Int): Type = TrinoTypes.coreToTrino(schema.fields(field).`type`)

  override def advanceNextPosition(): Boolean = {
    if (reader.next()) {
      row = reader.get()
      true
    } else {
      false
    }
  }

  override def getBoolean(field: Int): Boolean = {
    completedBytes += 1
    row.getBoolean(field)
  }

  override def getLong(field: Int): Long = {
    completedBytes += 8
    row.getLong(field)
  }

  override def getDouble(field: Int): Double = {
    completedBytes += 8
    row.getDouble(field)
  }

  override def getSlice(field: Int): Slice = {
    schema.fields(field).`type` match {
      case StringType =>
        val bytes = row.getString(field).getBytes("UTF-8")
        completedBytes += bytes.length
        Slices.wrappedBuffer(bytes, 0, bytes.length)

      case ArrayType(Int8Type, _) =>
        // Int8 = Byte    
        val bytes = row.values(field).asInstanceOf[List[Byte]].toArray
        completedBytes += bytes.length
        Slices.wrappedBuffer(bytes, 0, bytes.length)

      case other =>
        sys.error(s"Can't get a Slice for a(n) $other -- not a string or byte array")
    }
  }

  override def getObject(field: Int): AnyRef = {
    schema.fields(field).`type` match {
      case TimestampType(_) => microsToInstant(row.getLong(field))
      case DecimalType(p, s) => row.getDecimal(field, p, s)
      case ArrayType(elt, _) =>
        val values = row.values(field).asInstanceOf[ArrayLiteral[_]].value
        ???
      case MapType(_, _, _) =>
        val values = row.values(field).asInstanceOf[MapLiteral[_,_]].value
        ???
    }
  }

  override def isNull(field: Int): Boolean = row.isNullAt(field)

  override def close(): Unit = reader.close()
}
