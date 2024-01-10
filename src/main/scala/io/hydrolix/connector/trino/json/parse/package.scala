package io.hydrolix.connector.trino
package json

import java.time.Instant
import java.time.format.DateTimeFormatter.ISO_INSTANT

import com.fasterxml.jackson.core.{JsonParser, JsonToken}

import io.hydrolix.connectors.{microsToInstant, nanosToInstant}

package object parse {
  def parseTimestamp(parser: JsonParser, precision: Int): Instant = {
    if (parser.getCurrentToken == JsonToken.VALUE_STRING) {
      val s = parser.getText
      if (spaceR.matches(s)) {
        Instant.from(ISO_INSTANT.parse(s.replace(" ", "T") + "Z"))
      } else if (isoR.matches(s)) {
        Instant.from(ISO_INSTANT.parse(s))
      } else {
        sys.error(s"Can't parse timestamp from string '$s'")
      }
    } else if (parser.getCurrentToken == JsonToken.VALUE_NUMBER_INT) {
      // TODO maybe decimals someday?
      val l = parser.getLongValue
      precision match {
        case 0 => Instant.ofEpochSecond(l)
        case 3 => Instant.ofEpochMilli(l)
        case 6 => microsToInstant(l)
        case 9 => nanosToInstant(l)
        case _ => sys.error(s"Can't parse timestamps with precision $precision")
      }
    } else {
      sys.error(s"Can't parse timestamp from ${parser.getCurrentToken}")
    }
  }
}
