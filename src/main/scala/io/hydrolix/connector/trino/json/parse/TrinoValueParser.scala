package io.hydrolix.connector.trino.json.parse

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import io.airlift.slice.Slices
import io.trino.spi.`type`._
import io.trino.spi.block.{BlockBuilder, ShortArrayBlockBuilder, VariableWidthBlockBuilder}

import io.hydrolix.connector.trino.{ArrayOf, MapOf}

abstract class TrinoValueParser {
  def makeBlockBuilder: BlockBuilder
  def parseAndAppend(parser: JsonParser, blockBuilder: BlockBuilder): Unit
  def finish(blockBuilder: BlockBuilder) = blockBuilder.buildValueBlock()
}

object TrinoValueParser {
  def apply(trinoType: Type): TrinoValueParser = {
    trinoType match {
      case RealType.REAL                   => RealParser
      case SmallintType.SMALLINT           => SmallintParser
      case IntegerType.INTEGER             => IntegerParser
      case DoubleType.DOUBLE               => DoubleParser
      case BigintType.BIGINT               => BigintParser
      case VarcharType.VARCHAR             => VarcharParser
      case TinyintType.TINYINT             => TinyintParser
      case BooleanType.BOOLEAN             => BooleanParser
      case tt: TimestampType if tt.isShort => ShortTimestampParser(tt.getPrecision)
      case tt: TimestampType               => LongTimestampParser(tt.getPrecision)
      case tt: TimestampWithTimeZoneType if tt.isShort => ShortTimestampTZParser(tt.getPrecision)
      case tt: TimestampWithTimeZoneType               => LongTimestampTZParser(tt.getPrecision)
      case dt: DecimalType if dt.isShort   => ShortDecimalParser(dt.getPrecision)
      case dt: DecimalType                 => LongDecimalParser(dt.getPrecision)

      case ArrayOf(RealType.REAL)                   => ScalarArrayParser(RealParser)
      case ArrayOf(SmallintType.SMALLINT)           => ScalarArrayParser(SmallintParser)
      case ArrayOf(IntegerType.INTEGER)             => ScalarArrayParser(IntegerParser)
      case ArrayOf(DoubleType.DOUBLE)               => ScalarArrayParser(DoubleParser)
      case ArrayOf(BigintType.BIGINT)               => ScalarArrayParser(BigintParser)
      case ArrayOf(VarcharType.VARCHAR)             => ScalarArrayParser(VarcharParser)
      case ArrayOf(TinyintType.TINYINT)             => ScalarArrayParser(TinyintParser)
      case ArrayOf(BooleanType.BOOLEAN)             => ScalarArrayParser(BooleanParser)
      case ArrayOf(tt: TimestampType) if tt.isShort => ScalarArrayParser(ShortTimestampParser(tt.getPrecision))
      case ArrayOf(tt: TimestampType)               => ScalarArrayParser(LongTimestampParser(tt.getPrecision))
      case ArrayOf(tt: TimestampWithTimeZoneType) if tt.isShort => ScalarArrayParser(ShortTimestampTZParser(tt.getPrecision))
      case ArrayOf(tt: TimestampWithTimeZoneType)               => ScalarArrayParser(LongTimestampTZParser(tt.getPrecision))
      case ArrayOf(dt: DecimalType) if dt.isShort   => ScalarArrayParser(ShortDecimalParser(dt.getPrecision))
      case ArrayOf(dt: DecimalType)                 => ScalarArrayParser(LongDecimalParser(dt.getPrecision))

      case ArrayOf(mt @ MapOf(kt, vt)) => ArrayOfMapParser(mt, apply(kt), apply(vt))
      case ArrayOf(elt) => NestedArrayParser(apply(elt))
      case mt @ MapOf(kt, vt) => MapParser(mt, apply(kt), apply(vt))
    }
  }
}

case object SmallintParser extends TrinoValueParser {
  override def makeBlockBuilder: BlockBuilder = new ShortArrayBlockBuilder(null, 1)

  override def parseAndAppend(parser: JsonParser, blockBuilder: BlockBuilder): Unit = {
    if (parser.getCurrentToken == JsonToken.VALUE_NULL) {
      blockBuilder.appendNull()
    } else {
      blockBuilder.asInstanceOf[ShortArrayBlockBuilder].writeShort(parser.getShortValue)
    }
  }
}

case object VarcharParser extends TrinoValueParser {
  override def makeBlockBuilder: BlockBuilder = new VariableWidthBlockBuilder(null, 1, 1)

  override def parseAndAppend(parser: JsonParser, blockBuilder: BlockBuilder): Unit = {
    if (parser.getCurrentToken == JsonToken.VALUE_NULL) {
      blockBuilder.appendNull()
    } else {
      val s = parser.getText
      blockBuilder.asInstanceOf[VariableWidthBlockBuilder].writeEntry(Slices.utf8Slice(s))
    }
  }
}
