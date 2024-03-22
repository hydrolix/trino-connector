package io.hydrolix.connector.trino.json.print

import java.time.Instant
import java.{lang => jl}
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import com.typesafe.scalalogging.Logger
import io.trino.spi.`type`._
import io.trino.spi.block._

import io.hydrolix.connector.trino.{ArrayOf, MapOf, fixed12ToInstant}
import io.hydrolix.connectors.{Etc, JSON, microsToInstant}

abstract class TrinoValuePrinter {
  def print(block: Block, pos: Int): JsonNode
}

object TrinoValuePrinter {
  def apply(trinoType: Type): TrinoValuePrinter = {
    trinoType match {
      case RealType.REAL                   => RealPrinter
      case SmallintType.SMALLINT           => SmallintPrinter
      case IntegerType.INTEGER             => IntegerPrinter
      case DoubleType.DOUBLE               => DoublePrinter
      case BigintType.BIGINT               => BigintPrinter
      case VarcharType.VARCHAR             => VarcharPrinter
      case TinyintType.TINYINT             => TinyintPrinter
      case BooleanType.BOOLEAN             => BooleanPrinter
      case tt: TimestampType if tt.isShort => ShortTimestampPrinter(tt)
      case tt: TimestampType               => LongTimestampPrinter(tt)
      case tt: TimestampWithTimeZoneType if tt.isShort => ShortTimestampTZPrinter(tt)
      case tt: TimestampWithTimeZoneType               => LongTimestampTZPrinter(tt)
      case dt: DecimalType if dt.isShort   => ShortDecimalPrinter(dt)
      case dt: DecimalType                 => LongDecimalPrinter(dt)

      case ArrayOf(RealType.REAL)                   => ScalarArrayPrinter(RealPrinter)
      case ArrayOf(SmallintType.SMALLINT)           => ScalarArrayPrinter(SmallintPrinter)
      case ArrayOf(IntegerType.INTEGER)             => ScalarArrayPrinter(IntegerPrinter)
      case ArrayOf(DoubleType.DOUBLE)               => ScalarArrayPrinter(DoublePrinter)
      case ArrayOf(BigintType.BIGINT)               => ScalarArrayPrinter(BigintPrinter)
      case ArrayOf(VarcharType.VARCHAR)             => ScalarArrayPrinter(VarcharPrinter)
      case ArrayOf(TinyintType.TINYINT)             => ScalarArrayPrinter(TinyintPrinter)
      case ArrayOf(BooleanType.BOOLEAN)             => ScalarArrayPrinter(BooleanPrinter)
      case ArrayOf(tt: TimestampType) if tt.isShort => ScalarArrayPrinter(ShortTimestampPrinter(tt))
      case ArrayOf(tt: TimestampType)               => ScalarArrayPrinter(LongTimestampPrinter(tt))
      case ArrayOf(dt: DecimalType) if dt.isShort   => ScalarArrayPrinter(ShortDecimalPrinter(dt))
      case ArrayOf(dt: DecimalType)                 => ScalarArrayPrinter(LongDecimalPrinter(dt))

      case ArrayOf(MapOf(kt, vt)) => ArrayOfMapsPrinter(MapPrinter(apply(kt), apply(vt)))
      case ArrayOf(elt) => NestedArrayPrinter(apply(elt))
      case MapOf(kt, vt) => MapPrinter(apply(kt), apply(vt))
      case rt: RowType => RowPrinter(rt, rt.getTypeParameters.asScala.map(apply).toList)
    }
  }
}

case class ScalarArrayPrinter(elp: TrinoValuePrinter) extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    JSON.objectMapper.createArrayNode().also { arr =>
      for (i <- 0 until block.getPositionCount) {
        arr.add(elp.print(block, i))
      }
    }
  }
}

case class NestedArrayPrinter(elp: TrinoValuePrinter) extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    val ab = block.asInstanceOf[ArrayBlock]
    val outerArray = JSON.objectMapper.createArrayNode()

    for (i <- 0 until ab.getPositionCount) {
      val innerArray = ab.getArray(i)

      val node = elp.print(innerArray, i)

      outerArray.add(node)
    }
    outerArray
  }
}

case class MapPrinter(kp: TrinoValuePrinter, vp: TrinoValuePrinter) extends TrinoValuePrinter {
  private val logger = Logger(getClass)

  override def print(block: Block, pos: Int): JsonNode = {
    val mb = block.asInstanceOf[MapBlock]
    val sqlMap = mb.getMap(pos)

    val obj = JSON.objectMapper.createObjectNode()

    for (i <- 0 until sqlMap.getSize) {
      val keyPos = sqlMap.getUnderlyingKeyPosition(i)
      val valuePos = sqlMap.getUnderlyingValuePosition(i)

      val key = kp.print(sqlMap.getRawKeyBlock, keyPos) match {
        case t: TextNode => t.textValue()
        case other =>
          logger.warn("TODO non-string map keys")
          other.textValue()
      }
      val value = vp.print(sqlMap.getRawValueBlock, valuePos)

      obj.replace(key, value)
    }

    obj
  }
}

case class ArrayOfMapsPrinter(mp: MapPrinter) extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    val mb = block.asInstanceOf[MapBlock]

    val arr = JSON.objectMapper.createArrayNode()

    for (i <- 0 until mb.getPositionCount) {
      val kid = mp.print(mb, i)
      arr.add(kid)
    }

    arr
  }
}

// TODO untested
case class RowPrinter(rt: RowType, colPrinters: Seq[TrinoValuePrinter]) extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    val rb = block.asInstanceOf[RowBlock]

    val obj = JSON.objectMapper.createObjectNode()

    for (i <- colPrinters.indices) {
      val fb = rb.getFieldBlock(i)
      obj.replace(rt.getFields.get(i).getName.get(), colPrinters(i).print(fb, i))
    }

    obj
  }
}

case object TinyintPrinter extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    ShortNode.valueOf(block.asInstanceOf[ByteArrayBlock].getByte(pos))
  }
}

case object BooleanPrinter extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    BooleanNode.valueOf(block.asInstanceOf[ByteArrayBlock].getByte(pos) != 0)
  }
}

case object SmallintPrinter extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    ShortNode.valueOf(block.asInstanceOf[ShortArrayBlock].getShort(pos))
  }
}

case object RealPrinter extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    FloatNode.valueOf(jl.Float.intBitsToFloat(block.asInstanceOf[IntArrayBlock].getInt(pos)))
  }
}

case object IntegerPrinter extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    IntNode.valueOf(block.asInstanceOf[IntArrayBlock].getInt(pos))
  }
}

case object DoublePrinter extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    DoubleNode.valueOf(jl.Double.longBitsToDouble(block.asInstanceOf[LongArrayBlock].getLong(pos)))
  }
}

case object BigintPrinter extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    LongNode.valueOf(block.asInstanceOf[LongArrayBlock].getLong(pos))
  }
}

case class ShortTimestampPrinter(tt: TimestampType) extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    val inst = microsToInstant(block.asInstanceOf[LongArrayBlock].getLong(pos))
    TextNode.valueOf(inst.toString)
  }
}

case class LongTimestampPrinter(tt: TimestampType) extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    val inst = fixed12ToInstant(block.asInstanceOf[Fixed12Block].getFixed12First(pos), block.asInstanceOf[Fixed12Block].getFixed12Second(pos))
    TextNode.valueOf(inst.toString)
  }
}

case class ShortTimestampTZPrinter(tt: TimestampWithTimeZoneType) extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    val packed = block.asInstanceOf[LongArrayBlock].getLong(pos)
    val millis = DateTimeEncoding.unpackMillisUtc(packed)
    val zone = DateTimeEncoding.unpackZoneKey(packed)
    if (zone != TimeZoneKey.UTC_KEY) sys.error(s"oh noes a non-UTC time zone: ${zone}")
    val inst = Instant.ofEpochMilli(millis)
    TextNode.valueOf(inst.toString)
  }
}

case class LongTimestampTZPrinter(tt: TimestampWithTimeZoneType) extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    val packedEpochMillis = block.asInstanceOf[Fixed12Block].getFixed12First(pos)
    val picosOfMilli = block.asInstanceOf[Fixed12Block].getFixed12Second(pos)

    val inst = LongTimestampTZPrinter.decodeInstant(packedEpochMillis, picosOfMilli)
    TextNode.valueOf(inst.toString)
  }
}

object LongTimestampTZPrinter {
  def decodeInstant(packedEpochMillis: Long, picos: Int): Instant = {
    val millis = DateTimeEncoding.unpackMillisUtc(packedEpochMillis)
    val timeZoneKey = DateTimeEncoding.unpackZoneKey(packedEpochMillis)

    if (timeZoneKey != TimeZoneKey.UTC_KEY) {
      sys.error(s"Got a non-UTC timestamp, wtf? $timeZoneKey")
    }

    val lt = LongTimestampWithTimeZone.fromEpochMillisAndFraction(millis, picos, timeZoneKey)

    // TODO we're discarding picos here
    Instant.ofEpochMilli(lt.getEpochMillis)
  }
}

case class ShortDecimalPrinter(dt: DecimalType) extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    val dec = Decimals.readBigDecimal(dt, block, pos)
    DecimalNode.valueOf(dec)
  }
}

case class LongDecimalPrinter(dt: DecimalType) extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    val dec = Decimals.readBigDecimal(dt, block, pos)
    DecimalNode.valueOf(dec)
  }
}

case object VarcharPrinter extends TrinoValuePrinter {
  override def print(block: Block, pos: Int): JsonNode = {
    TextNode.valueOf(block.asInstanceOf[VariableWidthBlock].getSlice(pos).toStringUtf8)
  }
}

