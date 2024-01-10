package io.hydrolix.connector.trino.json.parse

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import io.trino.spi.`type`._
import io.trino.spi.block._

case class MapParser(trinoType: MapType, keyTypeHandler: TrinoValueParser, valueTypeHandler: TrinoValueParser) extends TrinoValueParser {
  override def makeBlockBuilder: BlockBuilder = new MapBlockBuilder(trinoType, null, 1)

  override def parseAndAppend(parser: JsonParser, blockBuilder: BlockBuilder): Unit = {
    if (parser.getCurrentToken == JsonToken.VALUE_NULL) {
      blockBuilder.appendNull()
    } else {
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        blockBuilder.asInstanceOf[MapBlockBuilder].buildEntry { (kb, vb) =>
          keyTypeHandler.parseAndAppend(parser, kb)
          parser.nextToken()
          valueTypeHandler.parseAndAppend(parser, vb)
        }
      }
    }
  }
}

case class ArrayOfMapParser(trinoType: MapType, keyTypeHandler: TrinoValueParser, valueTypeHandler: TrinoValueParser) extends TrinoValueParser {
  override def makeBlockBuilder: BlockBuilder = new MapBlockBuilder(trinoType, null, 1)

  private val mp = MapParser(trinoType, keyTypeHandler, valueTypeHandler)

  override def parseAndAppend(parser: JsonParser, blockBuilder: BlockBuilder): Unit = {
    if (parser.getCurrentToken == JsonToken.VALUE_NULL) {
      blockBuilder.appendNull()
    } else {
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        val mbb = blockBuilder.asInstanceOf[MapBlockBuilder]
        mbb.buildEntry { (kb, vb) =>
          while (parser.nextToken() != JsonToken.END_OBJECT) {
            mp.keyTypeHandler.parseAndAppend(parser, kb)
            parser.nextToken()
            mp.valueTypeHandler.parseAndAppend(parser, vb)
          }
        }
      }
    }
  }
}
