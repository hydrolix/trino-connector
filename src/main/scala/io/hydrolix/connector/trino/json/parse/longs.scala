package io.hydrolix.connector.trino.json.parse

import java.{lang => jl}
import scala.collection.mutable

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import io.trino.spi.`type`.{DecimalType, Decimals, TimestampType}
import io.trino.spi.block.{BlockBuilder, LongArrayBlockBuilder}

import io.hydrolix.connectors.{Etc, instantToMicros}

sealed abstract class LongParser extends TrinoValueParser {
  def parse(parser: JsonParser): Long

  override def makeBlockBuilder: BlockBuilder = new LongArrayBlockBuilder(null, 1)

  override def parseAndAppend(parser: JsonParser, blockBuilder: BlockBuilder): Unit = {
    if (parser.getCurrentToken == JsonToken.VALUE_NULL) {
      blockBuilder.appendNull()
    } else {
      blockBuilder.asInstanceOf[LongArrayBlockBuilder].writeLong(parse(parser))
    }
  }
}

case object BigintParser extends LongParser {
  override def parse(parser: JsonParser) = parser.getLongValue
}

case class ShortTimestampParser private(trinoType: TimestampType) extends LongParser {
  override def parse(parser: JsonParser) = {
    val inst = parseTimestamp(parser, trinoType.getPrecision)
    instantToMicros(inst)
  }
}
object ShortTimestampParser {
  private val values = Map(
    0 -> new ShortTimestampParser(TimestampType.createTimestampType(0)),
    3 -> new ShortTimestampParser(TimestampType.createTimestampType(3)),
    6 -> new ShortTimestampParser(TimestampType.createTimestampType(6)),
  )

  def apply(precision: Int): ShortTimestampParser = values.getOrElse(precision, sys.error(s"Can't handle short timestamps of precision $precision"))
}

case object DoubleParser extends LongParser {
  override def parse(parser: JsonParser) = jl.Double.doubleToLongBits(parser.getDoubleValue)
}
case class ShortDecimalParser private(trinoType: DecimalType) extends LongParser {
  override def parse(parser: JsonParser) = {
    val dec = parser.getDecimalValue
    Decimals.encodeShortScaledValue(dec, trinoType.getScale)
  }
}
object ShortDecimalParser {
  private val cache = mutable.HashMap[Int, ShortDecimalParser]()

  def apply(precision: Int): ShortDecimalParser = {
    if (cache.contains(precision)) {
      cache(precision)
    } else {
      new ShortDecimalParser(DecimalType.createDecimalType(precision)).also { th =>
        cache(precision) = th
      }
    }
  }
}
