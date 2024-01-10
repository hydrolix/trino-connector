package io.hydrolix.connector.trino

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._

import io.hydrolix.connectors.types._
import io.hydrolix.connectors.{Etc, JSON}

object TestUtil {
  implicit class NodeStuff(val node: JsonNode) extends AnyVal {
    /**
     * For testing, this recursively visits a JSON tree and upcasts all the numeric nodes to BigInteger or BigDecimal,
     * to remove spurious inequalities
     */
    def normalizeNumbers(): JsonNode = {
      recurse {
        case n: NumericNode if n.isIntegralNumber => BigIntegerNode.valueOf(n.bigIntegerValue())
        case n: NumericNode if n.isFloatingPointNumber => DecimalNode.valueOf(n.decimalValue())
      }
    }

    /**
     * For testing, this recursively visits a JSON tree and reformats timestamps that look like `spaceR` into
     * ISO8601 format, to remove spurious inequalities.
     */
    def normalizeTimestamps(): JsonNode = {
      recurse {
        case t: TextNode if isoR.matches(t.textValue()) => t
        case t: TextNode if spaceR.matches(t.textValue()) =>
          val newT = t.textValue()
            .replace(" ", "T")
            .appended('Z')

          TextNode.valueOf(newT)
      }
    }

    def recurse(f: PartialFunction[JsonNode, JsonNode]): JsonNode = {
      recurse0(node)(f)
    }

    /**
     * Recursively visit `node` and all its descendants. If `pf` accepts the node, call it to transform the value,
     * otherwise pass through. We always recurse after transformation, so anything created by `pf` will still be
     * visited.
     */
    private def recurse0(node: JsonNode)(pf: PartialFunction[JsonNode, JsonNode]): JsonNode = {
      node match {
        case obj: ObjectNode =>
          JSON.objectMapper.createObjectNode().also { out =>
            for (k <- obj.fieldNames().asScala) {
              val v = obj.get(k)
              val newValue = if (pf.isDefinedAt(v)) pf(v) else v
              val next = recurse0(newValue)(pf)
              out.replace(k, next)
            }
          }
        case arr: ArrayNode =>
          JSON.objectMapper.createArrayNode().also { out =>
            for (v <- arr.asScala) {
              val newValue = if (pf.isDefinedAt(v)) pf(v) else v
              val next = recurse0(newValue)(pf)
              out.add(next)
            }
          }
        case n if pf.isDefinedAt(n) => pf(n)
        case _ => node // Everything else falls through as-is
      }
    }
  }



  val christmasTreeStructType =
    StructType(List(
      StructField("uint32[][]",ArrayType(ArrayType(UInt32Type))),
      StructField("int8",Int8Type),
      StructField("uint64[]",ArrayType(UInt64Type)),
      StructField("datetime[][]",ArrayType(ArrayType(TimestampType(3)))),
      StructField("string",StringType),
      StructField("int32",Int32Type),
      StructField("epoch[]",ArrayType(TimestampType(3))),
      StructField("int32[][]",ArrayType(ArrayType(Int32Type))),
      StructField("boolean",Int8Type), // Note hdx has no boolean type, these are mapped to Int8.
      StructField("string[]",ArrayType(StringType)),
      StructField("double[]",ArrayType(Float64Type)),
      StructField("uint64",UInt64Type),
      StructField("uint8[][]",ArrayType(ArrayType(UInt8Type))),
      StructField("epoch[][]",ArrayType(ArrayType(TimestampType(3)))),
      StructField("epoch",TimestampType(3)),
      StructField("boolean[][]",ArrayType(ArrayType(Int8Type))),
      StructField("uint8",UInt8Type),
      StructField("int64[][]",ArrayType(ArrayType(Int64Type))),
      StructField("string[][]",ArrayType(ArrayType(StringType))),
      StructField("int8[]",ArrayType(Int8Type)),
      StructField("boolean[]",ArrayType(Int8Type)),
      StructField("datetime[]",ArrayType(TimestampType(3))),
      StructField("datetime",TimestampType(3)),
      StructField("uint32",UInt32Type),
      StructField("int32[]",ArrayType(Int32Type)),
      StructField("uint32[]",ArrayType(UInt32Type)),
      StructField("double",Float64Type),
      StructField("uint8[]",ArrayType(UInt8Type)),
      StructField("int64",Int64Type),
      StructField("double[][]",ArrayType(ArrayType(Float64Type))),
      StructField("int64[]",ArrayType(Int64Type)),
      StructField("uint64[][]",ArrayType(ArrayType(UInt64Type))),
      StructField("int8[][]",ArrayType(ArrayType(Int8Type)))
    ))
}
