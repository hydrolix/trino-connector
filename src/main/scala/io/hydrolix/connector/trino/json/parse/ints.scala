package io.hydrolix.connector.trino.json.parse

import java.{lang => jl}

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import io.trino.spi.block.{BlockBuilder, IntArrayBlockBuilder}

abstract class IntParser extends TrinoValueParser {
  def parse(parser: JsonParser): Int

  override def makeBlockBuilder: BlockBuilder = new IntArrayBlockBuilder(null, 1)

  override def parseAndAppend(parser: JsonParser, blockBuilder: BlockBuilder): Unit = {
    if (parser.getCurrentToken == JsonToken.VALUE_NULL) {
      blockBuilder.appendNull()
    } else {
      blockBuilder.asInstanceOf[IntArrayBlockBuilder].writeInt(parse(parser))
    }
  }
}

case object RealParser extends IntParser {
  override def parse(parser: JsonParser) = jl.Float.floatToIntBits(parser.getFloatValue)
}

case object IntegerParser extends IntParser {
  override def parse(parser: JsonParser) = parser.getIntValue
}
