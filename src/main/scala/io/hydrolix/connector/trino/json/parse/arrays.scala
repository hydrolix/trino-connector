package io.hydrolix.connector.trino.json.parse

import scala.collection.mutable

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import io.trino.spi.block._

import io.hydrolix.connectors.Etc

case class ScalarArrayParser private(elementParser: TrinoValueParser) extends TrinoValueParser {
  override def makeBlockBuilder: BlockBuilder = elementParser.makeBlockBuilder

  override def parseAndAppend(parser: JsonParser, blockBuilder: BlockBuilder): Unit = {
    if (parser.getCurrentToken == JsonToken.VALUE_NULL) {
      blockBuilder.appendNull()
    } else {
      assert(parser.getCurrentToken == JsonToken.START_ARRAY)
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        elementParser.parseAndAppend(parser, blockBuilder)
      }
    }
  }
}

object ScalarArrayParser {
  private val cache = mutable.HashMap[TrinoValueParser, ScalarArrayParser]()

  def apply(th: TrinoValueParser): ScalarArrayParser = {
    if (cache.contains(th)) {
      cache(th)
    } else {
      new ScalarArrayParser(th).also { sth =>
        cache(th) = sth
      }
    }
  }
}

case class NestedArrayParser(elementParser: TrinoValueParser) extends TrinoValueParser {
  override def makeBlockBuilder: BlockBuilder = new ArrayBlockBuilder(elementParser.makeBlockBuilder, null, 1)

  override def parseAndAppend(parser: JsonParser, blockBuilder: BlockBuilder): Unit = {
    if (parser.getCurrentToken == JsonToken.VALUE_NULL) {
      blockBuilder.appendNull()
    } else {
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        val abb = blockBuilder.asInstanceOf[ArrayBlockBuilder]
        abb.buildEntry { b =>
          elementParser.parseAndAppend(parser, b)
        }
      }
    }
  }
}
