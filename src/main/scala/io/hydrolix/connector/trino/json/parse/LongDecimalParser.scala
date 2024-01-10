package io.hydrolix.connector.trino.json.parse

import scala.collection.mutable

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import io.trino.spi.`type`.{DecimalType, Decimals}
import io.trino.spi.block.{BlockBuilder, Int128ArrayBlockBuilder}

import io.hydrolix.connectors.Etc

case class LongDecimalParser private(trinoType: DecimalType) extends TrinoValueParser {
  override def makeBlockBuilder: BlockBuilder = new Int128ArrayBlockBuilder(null, 1)

  override def parseAndAppend(parser: JsonParser, blockBuilder: BlockBuilder): Unit = {
    if (parser.getCurrentToken == JsonToken.VALUE_NULL) {
      blockBuilder.appendNull()
    } else {
      val dec = parser.getDecimalValue
      val int128 = Decimals.encodeScaledValue(dec, trinoType.getScale)
      blockBuilder.asInstanceOf[Int128ArrayBlockBuilder].writeInt128(int128.getHigh, int128.getLow)
    }
  }
}

object LongDecimalParser {
  private val cache = mutable.HashMap[Int, LongDecimalParser]()

  def apply(precision: Int): LongDecimalParser = {
    if (cache.contains(precision)) {
      cache(precision)
    } else {
      new LongDecimalParser(DecimalType.createDecimalType(precision)).also { th =>
        cache(precision) = th
      }
    }
  }
}
