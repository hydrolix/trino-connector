package io.hydrolix.connector.trino

import java.time.Instant
import java.{util => ju}
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.node.{BooleanNode, NumericNode, TextNode}
import io.airlift.slice.Slices
import io.trino.spi.`type`.{Int128, Type, TypeUtils}
import io.trino.spi.block.MapHashTables.HashBuildMode
import io.trino.spi.block._
import io.trino.spi.{`type` => ttypes}

import io.hydrolix.connectors.partitionreader.RowAdapter
import io.hydrolix.connectors.{instantToMicros, types => coretypes}

object TrinoRowAdapter extends RowAdapter[SqlRow, Block, SqlMap] {
  override type RB = TrinoRowBuilder
  override type AB = TrinoArrayBuilder
  override type MB = TrinoMapBuilder

  override def newRowBuilder(`type`: coretypes.StructType, rowId: Int) = new TrinoRowBuilder(`type`, rowId)
  override def newArrayBuilder(`type`: coretypes.ArrayType) = new TrinoArrayBuilder(`type`)
  override def newMapBuilder(`type`: coretypes.MapType) = new TrinoMapBuilder(`type`)

  final class TrinoRowBuilder(val `type`: coretypes.StructType, val rowId: Int) extends RowBuilder {
    private val trinoRowType = TrinoTypes
      .coreToTrino(`type`)
      .getOrElse(sys.error(s"Couldn't convert core ${`type`} to Trino RowType"))
      .asInstanceOf[ttypes.RowType]

    private val values = new ju.HashMap[String, Any]()

    override def setNull(name: String): Unit = ()

    override def setField(name: String, value: Any): Unit = values.put(name, value)

    override def build: SqlRow = {
      val valueBlocks = `type`.fields.zipWithIndex.map { case (fld, i) =>
        values.get(fld.name) match {
          case b: Block => b
          case other =>
            ttypes.TypeUtils.writeNativeValue(trinoRowType.getFields.get(i).getType, other)
        }
      }

      new SqlRow(rowId, valueBlocks.toArray)
    }
  }

  final class TrinoArrayBuilder(val `type`: coretypes.ArrayType) extends ArrayBuilder {
    private val trinoElementType = TrinoTypes
      .coreToTrino(`type`.elementType)
      .getOrElse(sys.error(s"Can't convert core element type ${`type`.elementType} to Trino"))

    private val values = new ju.ArrayList[Any]()

    override def set(pos: Int, value: Any): Unit = {
      values.ensureCapacity(pos + 1) // this seems redundant with the next line, but it avoids repeated re-allocations
      while (values.size() < pos + 1) values.add(null)
      values.set(pos, value)
    }

    override def setNull(pos: Int): Unit = ()

    override def build: Block = {
      val bb = new ArrayBlockBuilder(trinoElementType, null, 1)
      if (!values.isEmpty) {
        for (value <- values.asScala) {
          bb.buildEntry { valueBuilder =>
            // This handles nulls already
            ttypes.TypeUtils.writeNativeValue(trinoElementType, valueBuilder, value)
          }
        }
      }

      bb.build()
    }
  }

  final class TrinoMapBuilder(val `type`: coretypes.MapType) extends MapBuilder {
    private val trinoMapType = TrinoTypes
      .coreToTrino(`type`)
      .getOrElse(sys.error(s"Can't convert core ${`type`} to Trino"))
      .asInstanceOf[ttypes.MapType]
    private val trinoValueType: Type = trinoMapType.getValueType

    private val values = new ju.HashMap[Any, Any]()

    override def put(key: Any, value: Any): Unit = values.put(key, value)

    override def putNull(key: Any): Unit = ()

    override def build: SqlMap = {
      val kbb = new VariableWidthBlockBuilder(null, 1, 1)
      val vbb = new ttypes.ArrayType(trinoValueType).createBlockBuilder(null, values.size())

      for ((k, v) <- values.asScala) {
        kbb.buildEntry(bb => bb.write(k.toString.getBytes("UTF-8")))
        vbb.buildEntry(bb => ttypes.TypeUtils.writeNativeValue(trinoValueType, bb, v))
      }

      val kb = kbb.build()
      val vb = vbb.build()

      new SqlMap(trinoMapType, HashBuildMode.STRICT_EQUALS, kb, vb)
    }
  }

  override def string(value: String): Any = {
    val bytes = value.getBytes("UTF-8")
    Slices.wrappedBuffer(bytes, 0, bytes.length)
  }

  override def jsonString(value: TextNode, `type`: coretypes.ValueType): Any = {
    `type` match {
      case coretypes.StringType => string(value.textValue())
      case coretypes.BooleanType => value.textValue().toLowerCase() == "true"
      case coretypes.TimestampType(_) => instantToMicros(Instant.parse(value.textValue()))
    }
  }
  override def jsonNumber(n: NumericNode, `type`: coretypes.ValueType): Any = {
    `type` match {
      case coretypes.Float32Type => n.floatValue()
      case coretypes.Float64Type => n.doubleValue()
      case coretypes.Int8Type => n.asInt().byteValue()
      case coretypes.UInt8Type => n.asInt().shortValue()
      case coretypes.Int16Type => n.asInt().byteValue()
      case coretypes.UInt16Type => n.asInt()
      case coretypes.Int32Type => n.longValue()
      case coretypes.Int64Type => n.longValue()
      case coretypes.UInt32Type => n.longValue()
      case coretypes.UInt64Type =>
        Int128.valueOf(n.bigIntegerValue())
      case coretypes.DecimalType(p, s) =>
        val ttype = ttypes.DecimalType.createDecimalType(p, s)
        TypeUtils.writeNativeValue(ttype, n.decimalValue())
    }
  }

  override def jsonBoolean(n: BooleanNode, `type`: coretypes.ValueType): Any = {
    `type` match {
      case coretypes.BooleanType => n.booleanValue()
      case coretypes.StringType => string(if (n.booleanValue()) "true" else "false")
    }
  }
}
