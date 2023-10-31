package io.hydrolix.connector.trino

import java.time.{Duration, Instant}
import java.{util => ju}
import scala.jdk.CollectionConverters._

import io.airlift.slice.{Slice, Slices}
import io.trino.spi.`type`.Type
import io.trino.spi.connector._
import org.slf4j.LoggerFactory

import io.hydrolix.connectors.expr.{ArrayLiteral, MapLiteral, StructLiteral}
import io.hydrolix.connectors.partitionreader.{CoreRowAdapter, RowPartitionReader}
import io.hydrolix.connectors.types._
import io.hydrolix.connectors.{HdxApiSession, HdxConnectionInfo, HdxPartitionScanPlan, HdxTableCatalog, microsToInstant}

class HdxTrinoPageSourceProvider(info: HdxConnectionInfo,
                              catalog: HdxTableCatalog)
  extends ConnectorPageSourceProvider
{
  private val logger = LoggerFactory.getLogger(getClass)
  private val apiSession = new HdxApiSession(info)
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

    val cols = StructType(schema.toList: _*)

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

    val reader = new RowPartitionReader[StructLiteral](info, storage, hdxTable.primaryKeyField, scan, CoreRowAdapter, emptyRow)

    val trinoTypes = cols.fields.map(field => TrinoTypes.coreToTrino(field.`type`))

    new RecordPageSource(trinoTypes.asJava, new HdxTrinoRecordCursor(reader))
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
      case ArrayType(_, _) => row.values(field).asInstanceOf[ArrayLiteral[_]].value
      case MapType(_, _, _) => row.values(field).asInstanceOf[MapLiteral[_,_]].value
    }
  }

  override def isNull(field: Int): Boolean = row.isNullAt(field)

  override def close(): Unit = reader.close()
}
