package io.hydrolix.connector.trino.json.parse

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import io.trino.spi.block.{BlockBuilder, ByteArrayBlockBuilder}

sealed abstract class ByteParser extends TrinoValueParser {
  def parse(parser: JsonParser): Byte

  override def makeBlockBuilder: BlockBuilder = new ByteArrayBlockBuilder(null, 1)

  override def parseAndAppend(parser: JsonParser, blockBuilder: BlockBuilder): Unit = {
    if (parser.getCurrentToken == JsonToken.VALUE_NULL) {
      blockBuilder.appendNull()
    } else {
      blockBuilder.asInstanceOf[ByteArrayBlockBuilder].writeByte(parse(parser))
    }
  }
}

case object TinyintParser extends ByteParser {
  override def parse(parser: JsonParser): Byte = parser.getByteValue
}

case object BooleanParser extends ByteParser {
  override def parse(parser: JsonParser): Byte = {
    if (parser.currentToken() == JsonToken.VALUE_NUMBER_INT) {
      if (parser.getIntValue == 0) 0 else 1
    } else if (parser.currentToken().isBoolean) {
      if (parser.getBooleanValue) 1 else 0
    } else {
      sys.error(s"Can't parse a Boolean from a ${parser.getCurrentToken}")
    }
  }
}
