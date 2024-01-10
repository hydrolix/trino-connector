package io.hydrolix.connector.trino

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import io.trino.spi.Page
import io.trino.spi.`type`._
import io.trino.spi.block._
import org.slf4j.LoggerFactory

import io.hydrolix.connector.trino.json.parse.TrinoValueParser

/**
 * Parses the hdx_reader "jsonc" format, which looks like this:
 *
 * {{{
 * {
 *   "rows": 123,
 *   "cols": {
 *     "foo": [...],
 *     "bar": [...]
 *   }
 * }
 * }}}
 */
object TrinoColumnarJson {
  private val logger = LoggerFactory.getLogger(getClass)

  def parseStream(parser: JsonParser,
       trinoSchemaByName: Map[String, (ArrayType, Int)],
                  onDone: () => Unit,
                  onPage: Page => Unit)
                        : Unit =
  {
    parser.nextToken() // advance to first token

    if (parser.getCurrentToken == null) {
      onDone()
    } else {
      var i = 0
      while (parser.getCurrentToken != null) {
        assert(parser.currentToken() == JsonToken.START_OBJECT)
        assert(parser.nextFieldName() == "rows")
        val rows = parser.nextIntValue(-1)
        assert(rows >= 0)
        assert(parser.nextFieldName() == "cols")
        assert(parser.nextToken() == JsonToken.START_OBJECT)

        val colBlocks = Array.ofDim[Block](trinoSchemaByName.size)

        while (parser.currentToken() != JsonToken.END_OBJECT) {
          // in "cols" object
          val name = parser.nextFieldName()
          if (name != null) {
            assert(parser.nextToken() == JsonToken.START_ARRAY)

            // parser is at a column value now
            val (ttype, pos) = trinoSchemaByName(name)
            val th = TrinoValueParser(ttype)
            val bb = th.makeBlockBuilder
            th.parseAndAppend(parser, bb)
            val vb = th.finish(bb)

            colBlocks(pos) = vb.getLoadedBlock

            assert(parser.currentToken() == JsonToken.END_ARRAY)
          }
        }

        val page = new Page(rows, colBlocks: _*)

        parser.nextToken() // consume trailing '}' of "cols"
        parser.nextToken() // consume trailing '}' of root object

        onPage(page)

        i += 1
      }
    }
  }
}
