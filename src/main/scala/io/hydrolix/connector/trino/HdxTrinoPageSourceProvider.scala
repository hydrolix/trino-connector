package io.hydrolix.connector.trino

import java.time.{Duration, Instant}
import java.{lang => jl, util => ju}
import scala.annotation.unused
import scala.jdk.CollectionConverters._

import io.airlift.slice.Slice
import io.trino.spi.`type`.TypeUtils
import io.trino.spi.block.SqlRow
import io.trino.spi.connector._
import io.trino.spi.{`type` => ttypes}
import org.slf4j.LoggerFactory

import io.hydrolix.connector.trino.HdxTrinoSplitManager.TrinoSplitOps
import io.hydrolix.connectors.partitionreader.RowPartitionReader
import io.hydrolix.connectors.{HdxConnectionInfo, HdxPushdown, HdxTableCatalog, microsToInstant, types => coretypes}

object HdxTrinoPageSourceProvider {
  private val emptyRow = new SqlRow(-1, Array())
}

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
        val trinoTypes = cols.fields.map { field =>
          TrinoTypes.coreToTrino(field.`type`).getOrElse(sys.error(s"Can't translate ${field.name}: ${field.`type`} to Trino type"))
        }

        val reader = new RowPartitionReader[SqlRow](
          info,
          hdxTable.storages.getOrElse(scan.storageId, sys.error(s"No storage #${scan.storageId}")),
          hdxTable.primaryKeyField,
          scan,
          TrinoRowAdapter,
          HdxTrinoPageSourceProvider.emptyRow
        )

        new RecordPageSource(trinoTypes.asJava, new HdxTrinoRecordCursor(reader))
      case None =>
        new EmptyPageSource()
    }
  }
}

final class HdxTrinoRecordCursor(reader: RowPartitionReader[SqlRow]) extends RecordCursor {
  private val schema = reader.scan.schema
  private val startTime = Instant.now
  private val trinoSchema = TrinoTypes.coreToTrino(schema)
    .getOrElse(sys.error("Can't translate core schema to Trino"))
    .asInstanceOf[ttypes.RowType]

  private var row: SqlRow = _
  private var completedBytes = 0L

  override def getCompletedBytes: Long = completedBytes

  override def getReadTimeNanos: Long = Duration.between(startTime, Instant.now()).toNanos

  override def getType(field: Int): ttypes.Type = {
    trinoSchema.getFields.get(field).getType
  }

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
    // TODO check this
    row.getRawFieldBlock(field).getByte(0, 0) == 1
  }

  override def getLong(field: Int): Long = {
    completedBytes += 8
    row.getRawFieldBlock(field).getLong(0, 0)
  }

  override def getDouble(field: Int): Double = {
    completedBytes += 8
    // TODO check this
    val l = row.getRawFieldBlock(field).getLong(0, 0)
    jl.Double.longBitsToDouble(l)
  }

  override def getSlice(field: Int): Slice = {
    schema.fields(field).`type` match {
      case coretypes.StringType | coretypes.ArrayType(coretypes.Int8Type, _) =>
        val len = row.getRawFieldBlock(field).getSliceLength(0)
        completedBytes += len
        row.getRawFieldBlock(field).getSlice(0, 0, len)

      case other =>
        sys.error(s"Can't get a Slice for a(n) $other -- not a string or byte array")
    }
  }

  override def getObject(field: Int): AnyRef = {
    schema.fields(field).`type` match {
      case coretypes.TimestampType(_) =>
        // TODO this would only be called for LongTimestamp and would fail
        microsToInstant(getLong(field))
      case coretypes.UInt64Type =>
        TypeUtils.readNativeValue(TrinoUInt64Type, row.getRawFieldBlock(field), 0)
      case coretypes.ArrayType(_, _) =>
        row.getRawFieldBlock(field)
      case coretypes.MapType(_, _, _) =>
        row.getRawFieldBlock(field)
    }
  }

  override def isNull(field: Int): Boolean = {
    val block = row.getRawFieldBlock(field)
    // TODO is it count == 0 correct for empty arrays?
    block.getPositionCount == 0 || block.isNull(0)
  }

  override def close(): Unit = reader.close()
}
