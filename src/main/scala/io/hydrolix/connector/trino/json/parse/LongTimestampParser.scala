package io.hydrolix.connector.trino.json.parse

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import io.forktrino.`type`.DateTimes
import io.trino.spi.`type`.DateTimeEncoding.packDateTimeWithZone
import io.trino.spi.`type`.{TimeZoneKey, TimestampType, TimestampWithTimeZoneType}
import io.trino.spi.block.{BlockBuilder, Fixed12BlockBuilder}

case class LongTimestampParser private(trinoType: TimestampType) extends TrinoValueParser {
  override def makeBlockBuilder: BlockBuilder = new Fixed12BlockBuilder(null, 1)

  override def parseAndAppend(parser: JsonParser, blockBuilder: BlockBuilder): Unit = {
    if (parser.getCurrentToken == JsonToken.VALUE_NULL) {
      blockBuilder.appendNull()
    } else {
      val inst = parseTimestamp(parser, trinoType.getPrecision)

      val lt = DateTimes.longTimestamp(trinoType.getPrecision, inst)

      blockBuilder.asInstanceOf[Fixed12BlockBuilder].writeFixed12(lt.getEpochMicros, lt.getPicosOfMicro)
    }
  }
}
object LongTimestampParser {
  val values = Map(
    6 -> new LongTimestampParser(TimestampType.createTimestampType(6)),
    9 -> new LongTimestampParser(TimestampType.createTimestampType(9)),
  )
  def apply(precision: Int): LongTimestampParser = values.getOrElse(precision, sys.error(s"Can't handle long timestamps of precision $precision"))
}

case class LongTimestampTZParser private(trinoType: TimestampWithTimeZoneType) extends TrinoValueParser {
  override def makeBlockBuilder: BlockBuilder = new Fixed12BlockBuilder(null, 1)

  override def parseAndAppend(parser: JsonParser, blockBuilder: BlockBuilder): Unit = {
    if (parser.getCurrentToken == JsonToken.VALUE_NULL) {
      blockBuilder.appendNull()
    } else {
      val inst = parseTimestamp(parser, trinoType.getPrecision)

      // Hydrolix timestamps are always in UTC
      val lt = DateTimes.longTimestampWithTimeZone(trinoType.getPrecision, inst, TimeZoneKey.UTC_KEY)

      blockBuilder.asInstanceOf[Fixed12BlockBuilder].writeFixed12(
        packDateTimeWithZone(lt.getEpochMillis, lt.getTimeZoneKey),
        lt.getPicosOfMilli
      )
    }
  }
}
object LongTimestampTZParser {
  val values = Map(
    6 -> new LongTimestampTZParser(TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS),
    9 -> new LongTimestampTZParser(TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS),
  )
  def apply(precision: Int): LongTimestampTZParser = values.getOrElse(precision, sys.error(s"Can't handle long timestamps of precision $precision"))
}
